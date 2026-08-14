//! The receiver deciding, per entry, what it actually needs.
//!
//! Cheap evidence first: a different size settles the question without reading anything.
//! Hashes are only compared when both sides already have one, and mtime is the fallback -
//! deliberately the weakest of the three, so `--checksum` exists for when it is not enough.

use crate::proto::messages::{Decision, Entry};

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
/// `local` is `None` when nothing is there. Uncertainty always resolves towards
/// transferring: a needless resend costs bandwidth, a wrong skip costs the user their file.
pub fn decide(entry: &Entry, local: Option<LocalFile>) -> Decision {
    let need = Decision::Need {
        file_id: entry.file_id,
        from_offset: 0,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::messages::FileId;

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

    #[test]
    fn a_missing_file_is_always_needed() {
        assert_eq!(
            decide(&offered(10, 100, None), None),
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
            decide(&offered(10, 100, Some([1; 32])), Some(local)),
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
            decide(&offered(10, 100, Some([4; 32])), Some(same)),
            Decision::Skip
        );
        assert!(matches!(
            decide(&offered(10, 100, Some([4; 32])), Some(different)),
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

        assert_eq!(decide(&offered(10, 100, None), Some(local)), Decision::Skip);
        assert!(matches!(
            decide(&offered(10, 101, None), Some(local)),
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
            decide(&offered(10, 0, None), Some(local)),
            Decision::Need { .. }
        ));
    }
}
