//! Remembering file hashes between runs.
//!
//! Rehashing an entire folder on every send was the single largest cost in the previous
//! project. The key is `(relative path, size, mtime)`: if any of the three changed, the
//! entry does not match and the file is hashed again. The cache is an optimisation, never
//! a source of truth - a missing or unreadable cache costs time, never correctness.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::scan::walk::ScannedFile;

/// Directory holding spd's own state inside a transfer root.
pub const STATE_DIR: &str = ".spd";

/// File the cache is written to, inside [`STATE_DIR`].
pub const CACHE_FILE: &str = "hashcache";

/// Files above this size are not hashed for the manifest.
///
/// Reading a huge file just to decide whether to send it costs about as much as sending
/// it; the receiver falls back to size and mtime. `--checksum` overrides this when the
/// user wants certainty rather than speed.
pub const HASH_SIZE_CEILING_BYTES: u64 = 1024 * 1024 * 1024;

/// What identifies a cached hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct CacheKey {
    path: String,
    size: u64,
    mtime: u64,
}

/// Hashes remembered from previous runs.
#[derive(Debug, Default)]
pub struct HashCache {
    file: Option<PathBuf>,
    entries: HashMap<CacheKey, [u8; 32]>,
    dirty: bool,
}

impl HashCache {
    /// Loads the cache stored under `root`, or starts an empty one.
    ///
    /// Never fails: a corrupt or unreadable cache is discarded with a log line, because
    /// refusing to send a folder over a stale optimisation file would be absurd.
    pub fn open(root: &Path) -> Self {
        let file = root.join(STATE_DIR).join(CACHE_FILE);

        let entries = match std::fs::read(&file) {
            Ok(bytes) => match postcard::from_bytes::<Vec<(CacheKey, [u8; 32])>>(&bytes) {
                Ok(pairs) => pairs.into_iter().collect(),
                Err(error) => {
                    tracing::debug!(path = %file.display(), %error, "discarding an unreadable hash cache");
                    HashMap::new()
                }
            },
            Err(error) => {
                tracing::debug!(path = %file.display(), %error, "starting a new hash cache");
                HashMap::new()
            }
        };

        Self {
            file: Some(file),
            entries,
            dirty: false,
        }
    }

    /// An in-memory cache that is never persisted, for callers with nowhere to write.
    pub fn ephemeral() -> Self {
        Self::default()
    }

    /// The hash remembered for this exact file, if any.
    pub fn get(&self, file: &ScannedFile) -> Option<[u8; 32]> {
        self.entries.get(&key_for(file)).copied()
    }

    /// Remembers a hash for later runs.
    pub fn insert(&mut self, file: &ScannedFile, hash: [u8; 32]) {
        self.entries.insert(key_for(file), hash);
        self.dirty = true;
    }

    /// Writes the cache back, if there is anything new and somewhere to put it.
    ///
    /// Best effort by design: a read-only source directory should still be sendable, so a
    /// failure here is logged and the transfer continues.
    pub fn save(&self) {
        let (Some(file), true) = (self.file.as_ref(), self.dirty) else {
            return;
        };

        let pairs: Vec<(&CacheKey, &[u8; 32])> = self.entries.iter().collect();
        let Ok(encoded) = postcard::to_stdvec(&pairs) else {
            tracing::debug!("could not encode the hash cache; skipping the write");
            return;
        };

        if let Some(parent) = file.parent()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            tracing::debug!(path = %parent.display(), %error, "could not create the state directory");
            return;
        }

        if let Err(error) = std::fs::write(file, encoded) {
            tracing::debug!(path = %file.display(), %error, "could not save the hash cache");
        }
    }
}

fn key_for(file: &ScannedFile) -> CacheKey {
    CacheKey {
        path: file.relative.to_string(),
        size: file.size,
        mtime: file.mtime,
    }
}

/// Hashes a file from disk, in file order.
///
/// Blocking: call it from `spawn_blocking` or a rayon task, never straight from an async
/// task, or the executor stops serving every other transfer while this one reads.
///
/// # Errors
/// The underlying I/O error, with the caller adding the path.
pub fn hash_file(path: &Path) -> std::io::Result<[u8; 32]> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::safety::limits::Limits;
    use crate::safety::path::SafeRelPath;

    fn scanned(name: &str, size: u64, mtime: u64) -> ScannedFile {
        ScannedFile {
            absolute: PathBuf::from(name),
            relative: SafeRelPath::from_components(&[name.to_owned()], &Limits::DEFAULT).unwrap(),
            size,
            mtime,
            mode: 0,
        }
    }

    #[test]
    fn a_hash_is_found_again_for_the_same_file() {
        let mut cache = HashCache::ephemeral();
        let file = scanned("a.bin", 10, 1_000);

        cache.insert(&file, [7; 32]);

        assert_eq!(cache.get(&file), Some([7; 32]));
    }

    #[test]
    fn a_changed_size_or_mtime_misses() {
        let mut cache = HashCache::ephemeral();
        cache.insert(&scanned("a.bin", 10, 1_000), [7; 32]);

        assert_eq!(cache.get(&scanned("a.bin", 11, 1_000)), None);
        assert_eq!(cache.get(&scanned("a.bin", 10, 1_001)), None);
    }

    #[test]
    fn hashing_a_file_matches_hashing_its_bytes() {
        let dir = std::env::temp_dir().join("spd-hashcache-test");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("payload.bin");
        std::fs::write(&file, b"contents").unwrap();

        assert_eq!(
            hash_file(&file).unwrap(),
            *blake3::hash(b"contents").as_bytes()
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_ephemeral_cache_never_writes() {
        let cache = HashCache::ephemeral();
        cache.save();
        assert!(cache.file.is_none());
    }
}
