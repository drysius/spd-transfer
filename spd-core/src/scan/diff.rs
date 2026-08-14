//! The receiver deciding, per entry, what it actually needs.
//!
//! Cheap evidence first: a different size settles the question without reading anything.
//! Hashes are only compared when both sides already have one, and mtime is the fallback -
//! deliberately the weakest of the three, so `--checksum` exists for when it is not enough.
//!
//! When the answer is "send it", a second question follows: is there a `.part` file here
//! from an interrupted run, and was it started for this same file? If so, the transfer
//! picks up where it stopped instead of at zero.

use crate::proto::messages::{Decision, Entry};
use crate::state::model::Partial;

/// What the receiver knows about its own copy of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalFile {
    /// Size on disk.
    pub size: u64,
    /// Modification time, seconds since the Unix epoch.
    pub mtime: u64,
    /// Its hash, when the receiver computed one. Absent means "not worth reading".
    pub hash: Option<[u8; 32]>,
}

/// Decides what to do with one offered file.
///
/// `local` is `None` when nothing is there, `partial` when no interrupted run left a `.part`
/// file for it. Uncertainty always resolves towards transferring: a needless resend costs
/// bandwidth, a wrong skip costs the user their file.
pub fn decide(entry: &Entry, local: Option<LocalFile>, partial: Option<Partial>) -> Decision {
    let need = Decision::Need {
        file_id: entry.file_id,
        from_offset: resume_offset(entry, partial),
    };

    let Some(local) = local else {
        return need;
    };

    if local.size != entry.size {
        return need;
    }

    match (entry.hash, local.hash) {
        // Both sides know the hash: this is the only comparison that actually proves the
        // files are the same.
        (Some(theirs), Some(ours)) => {
            if theirs == ours {
                Decision::Skip
            } else {
                need
            }
        }
        // No hash to compare, so fall back to the timestamp. A zero mtime means the
        // filesystem could not say, which is treated as "not equal" rather than as a match.
        _ => {
            if local.mtime != 0 && local.mtime == entry.mtime {
                Decision::Skip
            } else {
                need
            }
        }
    }
}

/// Where a transfer of this file should start.
///
/// Zero unless there is a `.part` file that was started for exactly this offer and is no
/// longer than the file it claims to be a prefix of. Everything else - a changed source, a
/// `.part` from another version, a partial file that outgrew its target - starts again.
fn resume_offset(entry: &Entry, partial: Option<Partial>) -> u64 {
    let Some(partial) = partial else {
        return 0;
    };

    if !partial.expected.still_matches(entry) || partial.bytes_on_disk > entry.size {
        return 0;
    }

    partial.bytes_on_disk
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::messages::FileId;
    use crate::state::model::Expected;

    fn offered(size: u64, mtime: u64, hash: Option<[u8; 32]>) -> Entry {
        Entry {
            file_id: FileId(3),
            path: vec!["file.bin".to_owned()],
            size,
            mtime,
            mode: 0,
            hash,
        }
    }

    fn started_for(entry: &Entry, bytes_on_disk: u64) -> Partial {
        Partial {
            bytes_on_disk,
            expected: Expected::of(entry),
        }
    }

    #[test]
    fn a_missing_file_is_always_needed() {
        assert_eq!(
            decide(&offered(10, 100, None), None, None),
            Decision::Need {
                file_id: FileId(3),
                from_offset: 0
            }
        );
    }

    #[test]
    fn a_different_size_settles_it_without_hashing() {
        let local = LocalFile {
            size: 9,
            mtime: 100,
            hash: Some([1; 32]),
        };

        assert!(matches!(
            decide(&offered(10, 100, Some([1; 32])), Some(local), None),
            Decision::Need { .. }
        ));
    }

    #[test]
    fn matching_hashes_skip_and_differing_hashes_transfer() {
        let same = LocalFile {
            size: 10,
            mtime: 7,
            hash: Some([4; 32]),
        };
        let different = LocalFile {
            hash: Some([5; 32]),
            ..same
        };

        assert_eq!(
            decide(&offered(10, 100, Some([4; 32])), Some(same), None),
            Decision::Skip
        );
        assert!(matches!(
            decide(&offered(10, 100, Some([4; 32])), Some(different), None),
            Decision::Need { .. }
        ));
    }

    #[test]
    fn without_hashes_the_timestamp_decides() {
        let local = LocalFile {
            size: 10,
            mtime: 100,
            hash: None,
        };

        assert_eq!(
            decide(&offered(10, 100, None), Some(local), None),
            Decision::Skip
        );
        assert!(matches!(
            decide(&offered(10, 101, None), Some(local), None),
            Decision::Need { .. }
        ));
    }

    #[test]
    fn an_unknown_timestamp_never_counts_as_a_match() {
        let local = LocalFile {
            size: 10,
            mtime: 0,
            hash: None,
        };

        assert!(matches!(
            decide(&offered(10, 0, None), Some(local), None),
            Decision::Need { .. }
        ));
    }

    #[test]
    fn a_partial_file_from_this_same_offer_is_resumed() {
        let entry = offered(1_000, 100, Some([2; 32]));

        assert_eq!(
            decide(&entry, None, Some(started_for(&entry, 400))),
            Decision::Need {
                file_id: FileId(3),
                from_offset: 400
            }
        );
    }

    #[test]
    fn a_partial_file_from_another_version_starts_again() {
        let older = offered(900, 100, Some([2; 32]));
        let offer_now = offered(1_000, 101, Some([3; 32]));

        assert_eq!(
            decide(&offer_now, None, Some(started_for(&older, 400))),
            Decision::Need {
                file_id: FileId(3),
                from_offset: 0
            },
            "bytes written for a different file are not a prefix of this one"
        );
    }

    #[test]
    fn a_partial_file_longer_than_its_target_starts_again() {
        let entry = offered(1_000, 100, Some([2; 32]));

        assert_eq!(
            decide(&entry, None, Some(started_for(&entry, 1_001))),
            Decision::Need {
                file_id: FileId(3),
                from_offset: 0
            }
        );
    }

    #[test]
    fn a_complete_local_copy_wins_over_a_partial_one() {
        let entry = offered(10, 100, Some([4; 32]));
        let local = LocalFile {
            size: 10,
            mtime: 100,
            hash: Some([4; 32]),
        };

        assert_eq!(
            decide(&entry, Some(local), Some(started_for(&entry, 4))),
            Decision::Skip
        );
    }
}
