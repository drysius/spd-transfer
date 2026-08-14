//! Turning a local listing into what goes on the wire.
//!
//! The manifest is always batched, whatever the folder size. A single message per folder
//! would mean a frame limit that has to grow with the largest tree anyone owns, and a
//! peer-controlled allocation to match.

use std::path::PathBuf;

use crate::proto::messages::{Entry, FileId};
use crate::safety::path::PathError;
use crate::scan::walk::ScannedFile;

/// One file, ready to be offered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestFile {
    /// Id used for this file for the rest of the session.
    pub id: FileId,
    /// Where it is and what it looks like locally.
    pub scanned: ScannedFile,
    /// Its hash, when it was known cheaply.
    pub hash: Option<[u8; 32]>,
}

/// Everything the sender is offering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Manifest {
    files: Vec<ManifestFile>,
}

impl Manifest {
    /// Numbers a listing, keeping the order the walk produced.
    ///
    /// Ids are positional, so they are stable within a session and meaningless outside it.
    pub fn from_scan(scanned: Vec<ScannedFile>) -> Self {
        let files = scanned
            .into_iter()
            .enumerate()
            .map(|(index, file)| ManifestFile {
                id: FileId(index as u64),
                scanned: file,
                hash: None,
            })
            .collect();

        Self { files }
    }

    /// The files, in offer order.
    pub fn files(&self) -> &[ManifestFile] {
        &self.files
    }

    /// The files, mutably, for filling in hashes.
    pub fn files_mut(&mut self) -> &mut [ManifestFile] {
        &mut self.files
    }

    /// How many files it holds.
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Whether there is nothing to offer.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Total size of everything offered, in bytes.
    pub fn total_bytes(&self) -> u64 {
        self.files
            .iter()
            .map(|file| file.scanned.size)
            .fold(0, u64::saturating_add)
    }

    /// Looks a file up by the id it was given.
    pub fn find(&self, id: FileId) -> Option<&ManifestFile> {
        self.files.iter().find(|file| file.id == id)
    }
}

/// Splits a manifest into wire batches of at most `per_batch` entries.
///
/// # Panics
/// Never: `per_batch` of zero is treated as one, since a batch of nothing would loop
/// forever rather than fail visibly.
pub fn batches(manifest: &Manifest, per_batch: usize) -> Vec<Vec<Entry>> {
    let size = per_batch.max(1);

    manifest
        .files()
        .chunks(size)
        .map(|chunk| chunk.iter().map(entry_for).collect())
        .collect()
}

fn entry_for(file: &ManifestFile) -> Entry {
    Entry {
        file_id: file.id,
        path: file.scanned.relative.components().to_vec(),
        size: file.scanned.size,
        mtime: file.scanned.mtime,
        mode: file.scanned.mode,
        hash: file.hash,
    }
}

/// Why a local tree could not be turned into a manifest.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScanError {
    /// A path could not be read.
    #[error("could not read {path}")]
    Unreadable {
        /// What was being read.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },

    /// A local name cannot cross safely.
    #[error(transparent)]
    Path(#[from] PathError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::safety::limits::Limits;
    use crate::safety::path::SafeRelPath;

    fn scanned(name: &str, size: u64) -> ScannedFile {
        ScannedFile {
            absolute: PathBuf::from(name),
            relative: SafeRelPath::from_components(&[name.to_owned()], &Limits::DEFAULT).unwrap(),
            size,
            mtime: 42,
            mode: 0,
        }
    }

    #[test]
    fn ids_follow_the_scan_order() {
        let manifest = Manifest::from_scan(vec![scanned("big", 10), scanned("small", 1)]);

        assert_eq!(manifest.files()[0].id, FileId(0));
        assert_eq!(manifest.files()[1].id, FileId(1));
        assert_eq!(manifest.find(FileId(1)).unwrap().scanned.size, 1);
        assert_eq!(manifest.total_bytes(), 11);
    }

    #[test]
    fn batching_splits_and_keeps_every_entry() {
        let files = (0..5)
            .map(|index| scanned(&format!("f{index}"), 1))
            .collect();
        let manifest = Manifest::from_scan(files);

        let batched = batches(&manifest, 2);

        assert_eq!(batched.len(), 3);
        assert_eq!(batched[0].len(), 2);
        assert_eq!(batched[2].len(), 1);
        assert_eq!(batched.iter().flatten().count(), 5);
    }

    #[test]
    fn a_zero_batch_size_still_makes_progress() {
        let manifest = Manifest::from_scan(vec![scanned("only", 1)]);

        assert_eq!(batches(&manifest, 0).len(), 1);
    }
}
