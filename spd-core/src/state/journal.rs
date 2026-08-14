//! Which `.part` file belongs to which offer, remembered across runs.
//!
//! Two files inside the state directory: a snapshot holding what is known, and an
//! append-only journal holding what has happened since. Opening reads both; closing folds
//! one into the other and starts an empty journal, so neither grows without bound.
//!
//! Nothing here is a source of truth about *bytes*: how far a partial file got is a
//! question only `metadata().len()` can answer. The journal answers the other question -
//! what those bytes were meant to become - which no amount of looking at the file can.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::safety::limits::Limits;
use crate::safety::path::SafeRelPath;
use crate::state::STATE_DIR;
use crate::state::model::Expected;

/// Append-only log of everything that happened since the last snapshot.
const JOURNAL_FILE: &str = "journal";

/// Compacted state, rewritten whenever the journal is folded into it.
const SNAPSHOT_FILE: &str = "state";

/// Where a new snapshot is built before it replaces the old one.
const SNAPSHOT_TEMP_FILE: &str = "state.new";

/// Bytes of length prefix in front of each journal record.
const LENGTH_PREFIX_BYTES: usize = 4;

/// One thing that happened to a partial file.
///
/// Paths travel as components, exactly as they do on the wire: a journal written on one
/// platform stays readable on another, and nothing has to guess a separator.
#[derive(Debug, Serialize, Deserialize)]
enum Op {
    /// A `.part` file was opened for this offer.
    Started {
        path: Vec<String>,
        expected: Expected,
    },

    /// Its `.part` file is gone - committed, or thrown away.
    Forgot { path: Vec<String> },
}

/// Why the journal could not be read or written.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum JournalError {
    /// A file operation inside the state directory failed.
    #[error("{operation} failed on {path}")]
    Io {
        /// What was being attempted, in words a user recognises.
        operation: &'static str,
        /// The file or directory involved.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },

    /// A record could not be encoded, which means this build cannot persist its own state.
    #[error("the transfer state for {path} could not be encoded")]
    Encode {
        /// The file that was being written.
        path: PathBuf,
        /// What `postcard` reported.
        source: postcard::Error,
    },

    /// The task owning the state stopped before answering.
    #[error("the transfer state task stopped before it could answer")]
    Interrupted,
}

/// The receiver's record of every file it is part way through writing.
///
/// Blocking by design: it is a file, and it is owned by exactly one task, which runs on the
/// blocking pool. See [`crate::state`].
///
/// Nothing is written until there is something to record. A run that skips every file - or
/// a `--dry-run`, which is exactly that - leaves the destination as it found it.
#[derive(Debug)]
pub(crate) struct Journal {
    directory: PathBuf,
    snapshot: PathBuf,
    temporary: PathBuf,
    log: PathBuf,
    appender: Option<File>,
    live: HashMap<SafeRelPath, Expected>,
    unfolded: bool,
}

impl Journal {
    /// Reads the state under `root`, if any is there.
    ///
    /// Never fails: a snapshot or journal that cannot be decoded is discarded, because
    /// losing it costs a restart from offset zero, which is exactly what a first run does.
    /// A failure to *write*, on the other hand, propagates from the methods that write -
    /// silently losing every resume from there on is not something a user should have to
    /// discover later.
    pub(crate) fn open(root: &Path, limits: &Limits) -> Self {
        let directory = root.join(STATE_DIR);
        let snapshot = directory.join(SNAPSHOT_FILE);
        let temporary = directory.join(SNAPSHOT_TEMP_FILE);
        let log = directory.join(JOURNAL_FILE);

        let mut live = load_snapshot(&snapshot, limits);
        let replayed = replay(&log, limits, &mut live);

        Self {
            directory,
            snapshot,
            temporary,
            log,
            appender: None,
            live,
            // Records left by an interrupted run belong in the snapshot; folding them in is
            // what stops the journal growing across runs.
            unfolded: replayed > 0,
        }
    }

    /// What the `.part` file at this path was started for, if anything.
    pub(crate) fn partial(&self, path: &SafeRelPath) -> Option<Expected> {
        self.live.get(path).copied()
    }

    /// Records that a `.part` file is being written for this offer.
    ///
    /// Flushed to the device before returning: this is the record that makes the bytes
    /// about to be written reusable, and a record that survives only in a cache is worth
    /// nothing after the crash it exists for.
    ///
    /// # Errors
    /// [`JournalError::Io`] if the journal cannot be appended to, [`JournalError::Encode`]
    /// if the record cannot be serialised.
    pub(crate) fn started(
        &mut self,
        path: SafeRelPath,
        expected: Expected,
    ) -> Result<(), JournalError> {
        self.append(&Op::Started {
            path: path.components().to_vec(),
            expected,
        })?;

        if let Some(appender) = self.appender.as_ref() {
            appender.sync_all().map_err(|source| JournalError::Io {
                operation: "flushing the journal",
                path: self.log.clone(),
                source,
            })?;
        }

        self.live.insert(path, expected);
        Ok(())
    }

    /// Records that the `.part` file at this path is gone.
    ///
    /// Not flushed: losing this record leaves a note about a file that no longer exists,
    /// and the next run finds no `.part` to go with it and starts from zero.
    ///
    /// # Errors
    /// Same as [`Self::started`].
    pub(crate) fn forget(&mut self, path: &SafeRelPath) -> Result<(), JournalError> {
        if self.live.remove(path).is_none() {
            return Ok(());
        }

        self.append(&Op::Forgot {
            path: path.components().to_vec(),
        })
    }

    /// Writes what is known into a fresh snapshot and empties the journal.
    ///
    /// Does nothing when there is nothing to fold in, so a run that recorded nothing leaves
    /// no state directory behind at all.
    ///
    /// The snapshot lands first and the journal is emptied second. A crash between the two
    /// leaves records that are already in the snapshot, and replaying them changes nothing:
    /// the same file started twice is the same file, and forgetting one that is already
    /// gone is a no-op.
    ///
    /// # Errors
    /// [`JournalError::Io`] if either file cannot be written, [`JournalError::Encode`] if
    /// the snapshot cannot be serialised.
    pub(crate) fn compact(&mut self) -> Result<(), JournalError> {
        if !self.unfolded {
            return Ok(());
        }

        self.make_room()?;

        let entries: Vec<(&[String], &Expected)> = self
            .live
            .iter()
            .map(|(path, expected)| (path.components(), expected))
            .collect();

        let encoded = postcard::to_stdvec(&entries).map_err(|source| JournalError::Encode {
            path: self.snapshot.clone(),
            source,
        })?;

        write_atomically(&self.temporary, &self.snapshot, &encoded)?;

        self.appender = Some(File::create(&self.log).map_err(|source| JournalError::Io {
            operation: "starting a new journal",
            path: self.log.clone(),
            source,
        })?);
        self.unfolded = false;

        Ok(())
    }

    fn append(&mut self, op: &Op) -> Result<(), JournalError> {
        let encoded = postcard::to_stdvec(op).map_err(|source| JournalError::Encode {
            path: self.log.clone(),
            source,
        })?;

        // A record longer than u32 would need a path longer than any limit allows; the
        // saturating length then fails its own length check on replay instead of silently
        // shifting every record after it.
        let length = u32::try_from(encoded.len()).unwrap_or(u32::MAX);

        let mut framed = Vec::with_capacity(LENGTH_PREFIX_BYTES + encoded.len());
        framed.extend_from_slice(&length.to_le_bytes());
        framed.extend_from_slice(&encoded);

        let log = self.log.clone();
        self.appender()?
            .write_all(&framed)
            .map_err(|source| JournalError::Io {
                operation: "appending to the journal",
                path: log,
                source,
            })?;

        self.unfolded = true;
        Ok(())
    }

    /// The open journal, created on first use.
    fn appender(&mut self) -> Result<&mut File, JournalError> {
        if self.appender.is_none() {
            self.make_room()?;

            let opened = File::options()
                .create(true)
                .append(true)
                .open(&self.log)
                .map_err(|source| JournalError::Io {
                    operation: "opening the journal",
                    path: self.log.clone(),
                    source,
                })?;

            self.appender = Some(opened);
        }

        // INVARIANT: the branch above leaves `appender` filled in, and nothing between then
        // and here can empty it.
        self.appender.as_mut().ok_or(JournalError::Interrupted)
    }

    /// Creates the state directory, the first time something has to be written into it.
    fn make_room(&self) -> Result<(), JournalError> {
        std::fs::create_dir_all(&self.directory).map_err(|source| JournalError::Io {
            operation: "creating the state directory",
            path: self.directory.clone(),
            source,
        })
    }
}

/// Writes `contents` to `final_path`, via a temporary file and a rename.
///
/// The rename is the commit: a reader sees either the previous snapshot or the new one,
/// never a half-written file.
fn write_atomically(
    temporary: &Path,
    final_path: &Path,
    contents: &[u8],
) -> Result<(), JournalError> {
    let mut file = File::create(temporary).map_err(|source| JournalError::Io {
        operation: "creating",
        path: temporary.to_path_buf(),
        source,
    })?;

    file.write_all(contents)
        .map_err(|source| JournalError::Io {
            operation: "writing",
            path: temporary.to_path_buf(),
            source,
        })?;

    file.sync_all().map_err(|source| JournalError::Io {
        operation: "flushing",
        path: temporary.to_path_buf(),
        source,
    })?;

    std::fs::rename(temporary, final_path).map_err(|source| JournalError::Io {
        operation: "renaming into place",
        path: final_path.to_path_buf(),
        source,
    })
}

/// Reads the snapshot, or starts empty if there is nothing usable there.
fn load_snapshot(path: &Path, limits: &Limits) -> HashMap<SafeRelPath, Expected> {
    let Ok(bytes) = std::fs::read(path) else {
        tracing::debug!(path = %path.display(), "starting a new transfer state");
        return HashMap::new();
    };

    match postcard::from_bytes::<Vec<(Vec<String>, Expected)>>(&bytes) {
        Ok(pairs) => pairs
            .into_iter()
            .filter_map(|(components, expected)| Some((validated(&components, limits)?, expected)))
            .collect(),
        Err(error) => {
            tracing::debug!(path = %path.display(), %error, "discarding an unreadable transfer state");
            HashMap::new()
        }
    }
}

/// Applies every complete record in the journal on top of `live`, and says how many.
///
/// Stops at the first record that is truncated or too long, which is what a crash in the
/// middle of an append leaves behind. Everything before it is intact and is kept.
fn replay(path: &Path, limits: &Limits, live: &mut HashMap<SafeRelPath, Expected>) -> usize {
    let Ok(file) = File::open(path) else {
        return 0;
    };

    let mut reader = BufReader::new(file);
    let mut prefix = [0_u8; LENGTH_PREFIX_BYTES];
    let mut record = Vec::new();
    let mut applied = 0;

    loop {
        if reader.read_exact(&mut prefix).is_err() {
            return applied;
        }

        let length = u32::from_le_bytes(prefix) as usize;
        if length > limits.max_frame_len_bytes {
            tracing::debug!(path = %path.display(), length, "stopping at an oversized journal record");
            return applied;
        }

        record.clear();
        record.resize(length, 0);
        if reader.read_exact(&mut record).is_err() {
            tracing::debug!(path = %path.display(), "stopping at a truncated journal record");
            return applied;
        }

        match postcard::from_bytes::<Op>(&record) {
            Ok(Op::Started { path, expected }) => {
                if let Some(safe) = validated(&path, limits) {
                    live.insert(safe, expected);
                }
            }
            Ok(Op::Forgot { path }) => {
                if let Some(safe) = validated(&path, limits) {
                    live.remove(&safe);
                }
            }
            Err(error) => {
                tracing::debug!(path = %path.display(), %error, "stopping at an unreadable journal record");
                return applied;
            }
        }

        applied += 1;
    }
}

/// Rebuilds a path from stored components, dropping anything that would not be accepted
/// from a peer.
///
/// The journal is this process's own file, but validating on the way in is what keeps
/// `SafeRelPath` unforgeable: a serialised path is peer-supplied data the moment someone
/// edits the file.
fn validated(components: &[String], limits: &Limits) -> Option<SafeRelPath> {
    match SafeRelPath::from_components(components, limits) {
        Ok(path) => Some(path),
        Err(error) => {
            tracing::debug!(%error, "dropping an unusable path from the transfer state");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_nanos())
                .unwrap_or_default();
            let path = std::env::temp_dir().join(format!("spd-journal-{label}-{unique}"));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn path(name: &str) -> SafeRelPath {
        SafeRelPath::from_components(&[name.to_owned()], &Limits::DEFAULT).unwrap()
    }

    fn expected(size: u64) -> Expected {
        Expected {
            size,
            mtime: 42,
            hash: Some([9; 32]),
        }
    }

    #[test]
    fn a_started_file_is_still_known_after_reopening() {
        let scratch = Scratch::new("reopen");
        let limits = Limits::DEFAULT;

        let mut journal = Journal::open(&scratch.0, &limits);
        journal.started(path("big.bin"), expected(100)).unwrap();
        drop(journal);

        let reopened = Journal::open(&scratch.0, &limits);
        assert_eq!(reopened.partial(&path("big.bin")), Some(expected(100)));
    }

    #[test]
    fn a_forgotten_file_does_not_come_back() {
        let scratch = Scratch::new("forget");
        let limits = Limits::DEFAULT;

        let mut journal = Journal::open(&scratch.0, &limits);
        journal.started(path("gone.bin"), expected(10)).unwrap();
        journal.forget(&path("gone.bin")).unwrap();
        drop(journal);

        let reopened = Journal::open(&scratch.0, &limits);
        assert_eq!(reopened.partial(&path("gone.bin")), None);
    }

    #[test]
    fn compaction_empties_the_journal_without_losing_what_it_said() {
        let scratch = Scratch::new("compact");
        let limits = Limits::DEFAULT;

        let mut journal = Journal::open(&scratch.0, &limits);
        journal.started(path("kept.bin"), expected(7)).unwrap();
        journal.compact().unwrap();

        let on_disk = scratch.0.join(STATE_DIR).join(JOURNAL_FILE);
        assert_eq!(std::fs::metadata(&on_disk).unwrap().len(), 0);
        assert_eq!(journal.partial(&path("kept.bin")), Some(expected(7)));
    }

    #[test]
    fn a_record_cut_in_half_by_a_crash_is_dropped_and_the_rest_survives() {
        let scratch = Scratch::new("torn");
        let limits = Limits::DEFAULT;

        let mut journal = Journal::open(&scratch.0, &limits);
        journal.started(path("first.bin"), expected(1)).unwrap();
        journal.started(path("second.bin"), expected(2)).unwrap();
        drop(journal);

        // Chop the tail off, the way a machine losing power mid-append would.
        let on_disk = scratch.0.join(STATE_DIR).join(JOURNAL_FILE);
        let bytes = std::fs::read(&on_disk).unwrap();
        std::fs::write(&on_disk, &bytes[..bytes.len() - 3]).unwrap();

        let reopened = Journal::open(&scratch.0, &limits);
        assert_eq!(reopened.partial(&path("first.bin")), Some(expected(1)));
        assert_eq!(
            reopened.partial(&path("second.bin")),
            None,
            "a half-written record must not be believed"
        );
    }

    #[test]
    fn an_unreadable_snapshot_costs_resume_not_the_transfer() {
        let scratch = Scratch::new("corrupt");
        let limits = Limits::DEFAULT;

        std::fs::create_dir_all(scratch.0.join(STATE_DIR)).unwrap();
        std::fs::write(scratch.0.join(STATE_DIR).join(SNAPSHOT_FILE), b"nonsense").unwrap();

        let reopened = Journal::open(&scratch.0, &limits);
        assert_eq!(reopened.partial(&path("anything.bin")), None);
    }

    #[test]
    fn a_run_with_nothing_to_record_leaves_no_state_behind() {
        let scratch = Scratch::new("untouched");
        let limits = Limits::DEFAULT;

        let mut journal = Journal::open(&scratch.0, &limits);
        journal.forget(&path("never-started.bin")).unwrap();
        journal.compact().unwrap();

        assert!(
            !scratch.0.join(STATE_DIR).exists(),
            "a transfer that recorded nothing should not create a state directory"
        );
    }
}
