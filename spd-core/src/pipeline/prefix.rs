//! Hashing the part of a file that is not going to cross the network.
//!
//! Resuming means both sides skip bytes they already agree on - but the hash that verifies
//! the file covers all of it, from byte zero. So both sides read the prefix and feed it to
//! BLAKE3 without sending or writing anything: the sender from the source file, the
//! receiver from its `.part`. One function, because the two must never drift apart.

use std::path::Path;

use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::pipeline::PipelineError;
use crate::pipeline::bufpool::BufferPool;

/// Feeds the next `length` bytes of `file` into `hasher`, starting where it is positioned.
///
/// Returns how many bytes were actually read, which is less than `length` only if the file
/// is shorter than the caller believed - a fact the caller has to act on rather than
/// discover later as a hash that does not match.
///
/// # Errors
/// [`PipelineError::Io`] if the file cannot be read.
pub(crate) async fn hash_prefix(
    file: &mut File,
    path: &Path,
    hasher: &mut blake3::Hasher,
    length: u64,
    pool: &BufferPool,
) -> Result<u64, PipelineError> {
    let mut buffer = pool.acquire().await;
    let chunk = buffer.bytes().len();
    let mut covered = 0_u64;

    while covered < length {
        let remaining = length - covered;
        // Never read past the prefix: what follows belongs to the transfer itself.
        let want = usize::try_from(remaining).unwrap_or(chunk).min(chunk);

        let read = file
            .read(&mut buffer.bytes_mut()[..want])
            .await
            .map_err(|source| PipelineError::Io {
                operation: "reading",
                path: path.to_path_buf(),
                source,
            })?;

        if read == 0 {
            break;
        }

        hasher.update(&buffer.bytes()[..read]);
        covered += read as u64;
    }

    Ok(covered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hashing_a_prefix_matches_hashing_those_bytes_alone() {
        let directory = std::env::temp_dir().join("spd-prefix-test");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("payload.bin");
        std::fs::write(&path, b"first half|second half").unwrap();

        let mut file = File::open(&path).await.unwrap();
        let mut hasher = blake3::Hasher::new();
        let pool = BufferPool::new(1, 4);

        let covered = hash_prefix(&mut file, &path, &mut hasher, 10, &pool)
            .await
            .unwrap();

        assert_eq!(covered, 10);
        assert_eq!(
            hasher.finalize().as_bytes(),
            blake3::hash(b"first half").as_bytes()
        );

        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[tokio::test]
    async fn a_file_shorter_than_the_prefix_says_how_far_it_got() {
        let directory = std::env::temp_dir().join("spd-prefix-short-test");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("short.bin");
        std::fs::write(&path, b"tiny").unwrap();

        let mut file = File::open(&path).await.unwrap();
        let mut hasher = blake3::Hasher::new();
        let pool = BufferPool::new(1, 1024);

        let covered = hash_prefix(&mut file, &path, &mut hasher, 100, &pool)
            .await
            .unwrap();

        assert_eq!(covered, 4);

        std::fs::remove_dir_all(&directory).unwrap();
    }
}
