//! Listing a directory tree in parallel.
//!
//! Symlinks are skipped by default, in both directions: following one while reading can
//! walk out of the tree, and recreating one on the receiving side would write outside the
//! destination through a door the path validation never sees.

use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use crate::safety::limits::Limits;
use crate::safety::path::SafeRelPath;
use crate::scan::manifest::ScanError;

/// One file found under the transfer root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedFile {
    /// Where it is on this machine.
    pub absolute: PathBuf,
    /// Its path relative to the root, already validated.
    pub relative: SafeRelPath,
    /// Size in bytes.
    pub size: u64,
    /// Modification time, seconds since the Unix epoch.
    pub mtime: u64,
    /// Permission bits worth carrying.
    pub mode: u32,
}

/// What to include in a walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalkOptions {
    /// Follow symlinks when reading. Off by default: a link can point anywhere, including
    /// outside the tree the user meant to send.
    pub follow_links: bool,
}

/// Lists every file under `root`, largest first.
///
/// Largest first because the work queue hands batches to workers: starting with the big
/// files keeps the tail from being one huge file finishing alone, long after everything
/// else is done.
///
/// A single file as `root` is a tree of one, so callers do not need a separate path for it.
///
/// A file that disappears between being listed and being measured is skipped. Trees are
/// live: editors write and rename, servers churn through temporary files, and a scan that
/// refuses to send ten thousand files because one of them stopped existing is refusing the
/// one job it had.
///
/// # Errors
/// [`ScanError::Unreadable`] if the root cannot be read, or if a file under it cannot be
/// read for any reason other than no longer being there - a permission denied is a file the
/// user asked to send and will not get. [`ScanError::Path`] if a name under it cannot cross
/// safely: a file the receiver could not name is a failure to report, not something to skip
/// silently.
pub fn walk(
    root: &Path,
    options: WalkOptions,
    limits: &Limits,
) -> Result<Vec<ScannedFile>, ScanError> {
    let metadata = std::fs::metadata(root).map_err(|source| ScanError::Unreadable {
        path: root.to_path_buf(),
        source,
    })?;

    if metadata.is_file() {
        return Ok(vec![describe(root, root, &metadata, limits)?]);
    }

    let mut found = Vec::new();

    for entry in jwalk::WalkDir::new(root)
        .follow_links(options.follow_links)
        .skip_hidden(false)
    {
        let entry = match entry {
            Ok(entry) => entry,
            // A directory that went away while it was being walked is not a failure to
            // report: there is nothing under it left to send.
            Err(gone) if vanished(gone.io_error()) => {
                tracing::debug!(%gone, "a directory disappeared while the tree was listed");
                continue;
            }
            Err(source) => {
                return Err(ScanError::Unreadable {
                    path: root.to_path_buf(),
                    source: source.into(),
                });
            }
        };

        // Directories are implied by the files inside them, and an empty directory carries
        // no data worth a protocol round trip.
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();

        // spd's own state is not part of the user's data. Sending it would also make every
        // run differ from the last, since writing the hash cache changes the tree that was
        // just hashed.
        if is_state_path(&path, root) {
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            // Listed a moment ago and gone now. Live trees do this constantly - editors
            // write and rename, servers churn through temporary files - and refusing to
            // send ten thousand files because one of them was never going to be sent is
            // the wrong answer.
            Err(gone) if vanished(gone.io_error()) => {
                tracing::debug!(path = %path.display(), "skipping a file that disappeared mid-scan");
                continue;
            }
            Err(source) => {
                return Err(ScanError::Unreadable {
                    path: path.clone(),
                    source: source.into(),
                });
            }
        };

        found.push(describe(&path, root, &metadata, limits)?);
    }

    found.sort_by(|left, right| {
        right
            .size
            .cmp(&left.size)
            .then_with(|| left.relative.cmp(&right.relative))
    });

    Ok(found)
}

/// Whether an error means the thing is simply not there any more.
///
/// Only this exact case is tolerated. A permission denied, a failing disk or a path too
/// long are all things the user needs to hear about, because each of them means a file
/// they asked to send is not going to be sent.
fn vanished(error: Option<&std::io::Error>) -> bool {
    error.is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
}

/// Whether this path lives inside spd's own state directory.
fn is_state_path(path: &Path, root: &Path) -> bool {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .any(|component| component.as_os_str() == crate::state::STATE_DIR)
}

fn describe(
    path: &Path,
    root: &Path,
    metadata: &std::fs::Metadata,
    limits: &Limits,
) -> Result<ScannedFile, ScanError> {
    // A single file passed as the root keeps its own name; anything deeper keeps the part
    // of its path below the root.
    let relative_path = if path == root {
        Path::new(path.file_name().unwrap_or_default())
    } else {
        path.strip_prefix(root).unwrap_or(path)
    };

    let relative = SafeRelPath::from_relative_path(relative_path, limits)?;

    Ok(ScannedFile {
        absolute: path.to_path_buf(),
        relative,
        size: metadata.len(),
        mtime: mtime_of(metadata),
        mode: mode_of(metadata),
    })
}

/// Seconds since the Unix epoch, or zero when the filesystem cannot say.
///
/// Zero is a value the diff treats as "unknown and therefore not equal", so an unreadable
/// timestamp costs a retransfer rather than a wrong skip.
pub(crate) fn mtime_of(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |since_epoch| since_epoch.as_secs())
}

#[cfg(unix)]
pub(crate) fn mode_of(metadata: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode()
}

/// Windows has no mode bits worth carrying; the receiver applies its own default.
#[cfg(not(unix))]
pub(crate) fn mode_of(_metadata: &std::fs::Metadata) -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!("spd-walk-{label}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn a_tree_is_listed_largest_first_with_relative_names() {
        let root = scratch("tree");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("small.txt"), b"1").unwrap();
        std::fs::write(root.join("nested").join("big.bin"), vec![0_u8; 4096]).unwrap();

        let found = walk(&root, WalkOptions::default(), &Limits::DEFAULT).unwrap();

        assert_eq!(found.len(), 2);
        assert_eq!(found[0].relative.to_string(), "nested/big.bin");
        assert_eq!(found[0].size, 4096);
        assert_eq!(found[1].relative.to_string(), "small.txt");

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_single_file_is_a_tree_of_one_named_after_itself() {
        let root = scratch("single");
        let file = root.join("only.bin");
        std::fs::write(&file, b"data").unwrap();

        let found = walk(&file, WalkOptions::default(), &Limits::DEFAULT).unwrap();

        assert_eq!(found.len(), 1);
        assert_eq!(found[0].relative.to_string(), "only.bin");

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn only_a_missing_file_is_tolerated() {
        use std::io::{Error, ErrorKind};

        assert!(vanished(Some(&Error::from(ErrorKind::NotFound))));
        assert!(!vanished(Some(&Error::from(ErrorKind::PermissionDenied))));
        assert!(!vanished(Some(&Error::other("the disk gave up"))));
        assert!(!vanished(None));
    }

    /// A tree being written to while it is listed is the normal case on a server, and it
    /// used to fail the whole transfer: one temporary file that existed when the directory
    /// was read and was gone a moment later was enough.
    ///
    /// The deletions race the walk on purpose. The assertion only fails if the walk reports
    /// a file that is not there as an error, so a run where the race does not happen passes
    /// quietly rather than flaking.
    #[test]
    fn files_disappearing_mid_scan_do_not_fail_the_walk() {
        let root = scratch("vanishing");
        for index in 0..400 {
            std::fs::write(root.join(format!("file-{index}.tmp")), b"temporary").unwrap();
        }

        let deleting = root.clone();
        let deleter = std::thread::spawn(move || {
            for index in 0..400 {
                let _ = std::fs::remove_file(deleting.join(format!("file-{index}.tmp")));
            }
        });

        let walked = walk(&root, WalkOptions::default(), &Limits::DEFAULT);
        deleter.join().unwrap();

        assert!(
            walked.is_ok(),
            "a file that stopped existing is not a reason to refuse the tree: {:?}",
            walked.err()
        );

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_missing_root_is_reported_with_its_path() {
        let missing = std::env::temp_dir().join("spd-walk-absent-9999");

        assert!(matches!(
            walk(&missing, WalkOptions::default(), &Limits::DEFAULT).unwrap_err(),
            ScanError::Unreadable { .. }
        ));
    }
}
