//! What the receiver remembers about a file it is part way through writing.
//!
//! A `.part` file on its own says nothing: it is a valid prefix of *something*, and
//! resuming it against a different offer would produce bytes that hash to nothing anyone
//! expects. [`Expected`] is the missing half - the shape of the file that `.part` was
//! started for - so the two can be compared before a single byte is reused.

use serde::{Deserialize, Serialize};

use crate::proto::messages::Entry;

/// The file a `.part` is meant to become.
///
/// Compared, never trusted: a match only means resuming is worth attempting, and the
/// per-file BLAKE3 still has the last word.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expected {
    /// Size the finished file will have.
    pub size: u64,

    /// Modification time the sender reported, seconds since the Unix epoch.
    pub mtime: u64,

    /// The sender's hash, when it offered one.
    pub hash: Option<[u8; 32]>,
}

impl Expected {
    /// Records the shape of one offered file.
    pub const fn of(entry: &Entry) -> Self {
        Self {
            size: entry.size,
            mtime: entry.mtime,
            hash: entry.hash,
        }
    }

    /// Whether this is still the same file the sender is offering.
    ///
    /// All three fields have to agree. Two offers without a hash fall back to size and
    /// mtime, which is the same evidence the diff settles for - and, like the diff, an
    /// unknown timestamp counts as "not the same" rather than as a match.
    pub fn still_matches(&self, entry: &Entry) -> bool {
        if self.hash.is_none() && self.mtime == 0 {
            return false;
        }

        self.size == entry.size && self.mtime == entry.mtime && self.hash == entry.hash
    }
}

/// A `.part` file left behind by an interrupted transfer.
///
/// `bytes_on_disk` comes from the filesystem rather than from the journal: the journal
/// records what a partial file is *for*, and only `metadata().len()` knows how far it
/// actually got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Partial {
    /// Bytes already written, straight from the filesystem.
    pub bytes_on_disk: u64,

    /// What those bytes were meant to become.
    pub expected: Expected,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::messages::FileId;

    fn offered(size: u64, mtime: u64, hash: Option<[u8; 32]>) -> Entry {
        Entry {
            file_id: FileId(1),
            path: vec!["file.bin".to_owned()],
            size,
            mtime,
            mode: 0,
            hash,
        }
    }

    #[test]
    fn the_same_offer_matches_itself() {
        let entry = offered(100, 7, Some([3; 32]));
        assert!(Expected::of(&entry).still_matches(&entry));
    }

    #[test]
    fn a_changed_size_mtime_or_hash_stops_matching() {
        let entry = offered(100, 7, Some([3; 32]));
        let expected = Expected::of(&entry);

        assert!(!expected.still_matches(&offered(101, 7, Some([3; 32]))));
        assert!(!expected.still_matches(&offered(100, 8, Some([3; 32]))));
        assert!(!expected.still_matches(&offered(100, 7, Some([4; 32]))));
        assert!(!expected.still_matches(&offered(100, 7, None)));
    }

    #[test]
    fn without_a_hash_or_a_timestamp_nothing_matches() {
        let entry = offered(100, 0, None);
        assert!(
            !Expected::of(&entry).still_matches(&entry),
            "size alone is not evidence enough to reuse bytes already on disk"
        );
    }
}
