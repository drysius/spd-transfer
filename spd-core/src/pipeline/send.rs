//! The sending side of a transfer.
//!
//! One file, one stream, in file order: the hash is fed as the bytes are read, so arrival
//! order and hashing order cannot disagree. The previous project hashed in arrival order
//! across parallel chunks and produced digests that did not match the file it had just
//! written.

use std::path::Path;
use std::time::UNIX_EPOCH;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::pipeline::{PipelineError, TransferSummary};
use crate::proto::codec::write_data_header;
use crate::proto::messages::{Control, DataHeader, Decision, Entry, FileId};
use crate::safety::limits::Limits;
use crate::safety::path::{PathError, SafeRelPath};
use crate::transport::session::Session;

/// Size of one read from disk. Replaced by the pooled buffers of F4; until then it is a
/// single allocation reused for the whole file.
const READ_CHUNK_BYTES: usize = 1024 * 1024;

/// Sends one file over an established session.
///
/// Offers the file, honours the receiver's decision, streams the bytes it asked for and
/// waits for the verdict. Returning `Ok` means the receiver confirmed the hash - not that
/// the bytes left this machine.
///
/// # Errors
/// [`PipelineError::Io`] if the file cannot be read, [`PipelineError::FileTooLarge`] if it
/// exceeds the configured ceiling, [`PipelineError::HashMismatch`] if the receiver reports
/// a mismatch, [`PipelineError::UnexpectedMessage`] if the peer answers out of order, or a
/// transport error if the connection fails.
pub async fn send_file(
    session: &mut Session,
    source: &Path,
    limits: &Limits,
) -> Result<TransferSummary, PipelineError> {
    let entry = describe(source, limits).await?;
    let file_id = entry.file_id;
    let size = entry.size;

    session
        .control()
        .send(&Control::Manifest {
            batch_seq: 0,
            last: true,
            entries: vec![entry],
        })
        .await?;

    let Some(from_offset) = await_decision(session, file_id).await? else {
        tracing::info!(%file_id, "receiver already has this file");
        return Ok(TransferSummary::default());
    };

    let hash = stream_body(session, source, file_id, from_offset).await?;

    session
        .control()
        .send(&Control::FileDone { file_id, hash })
        .await?;

    match session.control().recv().await? {
        Control::FileVerdict { ok: true, .. } => {}
        Control::FileVerdict { ok: false, .. } => {
            return Err(PipelineError::HashMismatch {
                path: source.to_path_buf(),
            });
        }
        Control::Error { msg, .. } => return Err(PipelineError::PeerFailed { msg }),
        other => {
            return Err(PipelineError::UnexpectedMessage {
                expected: "FileVerdict",
                got: other.kind(),
            });
        }
    }

    let sent = size.saturating_sub(from_offset);
    session
        .control()
        .send(&Control::Done {
            files: 1,
            bytes: sent,
        })
        .await?;

    Ok(TransferSummary {
        files: 1,
        bytes: sent,
    })
}

/// Builds the manifest entry for a local file.
async fn describe(source: &Path, limits: &Limits) -> Result<Entry, PipelineError> {
    let metadata = tokio::fs::metadata(source)
        .await
        .map_err(|source_error| PipelineError::Io {
            operation: "reading metadata",
            path: source.to_path_buf(),
            source: source_error,
        })?;

    let size = metadata.len();
    if size > limits.max_file_size_bytes {
        return Err(PipelineError::FileTooLarge {
            path: source.to_path_buf(),
            size,
            max: limits.max_file_size_bytes,
        });
    }

    // A path ending in `.` or `..` has no name to give the file on the other side.
    let name = source
        .file_name()
        .map(Path::new)
        .ok_or_else(|| PathError::NotRelative {
            path: source.display().to_string(),
        })?;
    let relative = SafeRelPath::from_relative_path(name, limits)?;

    let mtime = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |since_epoch| since_epoch.as_secs());

    Ok(Entry {
        file_id: FileId(0),
        path: relative.components().to_vec(),
        size,
        mtime,
        mode: mode_of(&metadata),
        // No hash yet: the sender would have to read the whole file to compute one, and
        // the receiver is about to receive it anyway. The hash cache of F3 is what makes
        // sending one worthwhile.
        hash: None,
    })
}

/// Waits for the receiver's decision about `file_id`.
///
/// `Ok(None)` means it already has the file. `Ok(Some(offset))` means send from there.
async fn await_decision(
    session: &mut Session,
    file_id: FileId,
) -> Result<Option<u64>, PipelineError> {
    let reply = session.control().recv().await?;

    let Control::SyncReply { decisions, .. } = reply else {
        if let Control::Error { msg, .. } = reply {
            return Err(PipelineError::PeerFailed { msg });
        }
        return Err(PipelineError::UnexpectedMessage {
            expected: "SyncReply",
            got: reply.kind(),
        });
    };

    match decisions.first() {
        Some(Decision::Skip) | None => Ok(None),
        Some(Decision::Need {
            file_id: wanted,
            from_offset,
        }) => {
            // A decision about a file that was never offered means the peer lost track of
            // the manifest; continuing would send bytes under the wrong name.
            if *wanted == file_id {
                Ok(Some(*from_offset))
            } else {
                Err(PipelineError::UnknownFile { file_id: *wanted })
            }
        }
    }
}

/// Streams the file body on its own unidirectional stream and returns its BLAKE3.
///
/// The hash covers the whole file, including the prefix the receiver already has: both
/// sides verify the same thing regardless of where the transfer resumed.
async fn stream_body(
    session: &Session,
    source: &Path,
    file_id: FileId,
    from_offset: u64,
) -> Result<[u8; 32], PipelineError> {
    let mut file = File::open(source)
        .await
        .map_err(|error| PipelineError::Io {
            operation: "opening",
            path: source.to_path_buf(),
            source: error,
        })?;

    let mut hasher = blake3::Hasher::new();

    // Everything before the resume point still has to be hashed, so read it even though
    // it is not sent. Storing hasher state in the journal (F5) is what removes this cost.
    if from_offset > 0 {
        hash_prefix(&mut file, source, &mut hasher, from_offset).await?;
        file.seek(std::io::SeekFrom::Start(from_offset))
            .await
            .map_err(|error| PipelineError::Io {
                operation: "seeking",
                path: source.to_path_buf(),
                source: error,
            })?;
    }

    let mut stream = session.open_data_stream().await?;
    write_data_header(
        &mut stream,
        &DataHeader {
            file_id,
            offset: from_offset,
            compressed: false,
        },
    )
    .await?;

    let mut buffer = vec![0_u8; READ_CHUNK_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| PipelineError::Io {
                operation: "reading",
                path: source.to_path_buf(),
                source: error,
            })?;

        if read == 0 {
            break;
        }

        hasher.update(&buffer[..read]);
        stream
            .write_all(&buffer[..read])
            .await
            .map_err(|error| PipelineError::Io {
                operation: "sending",
                path: source.to_path_buf(),
                source: error.into(),
            })?;
    }

    // Finishing the stream is how the receiver learns the file ended; without it, it waits
    // for bytes that are never coming.
    stream.finish().map_err(|error| PipelineError::Io {
        operation: "closing the stream for",
        path: source.to_path_buf(),
        source: std::io::Error::other(error),
    })?;

    Ok(*hasher.finalize().as_bytes())
}

async fn hash_prefix(
    file: &mut File,
    source: &Path,
    hasher: &mut blake3::Hasher,
    length: u64,
) -> Result<(), PipelineError> {
    let mut remaining = length;
    let mut buffer = vec![0_u8; READ_CHUNK_BYTES];

    while remaining > 0 {
        let want =
            usize::try_from(remaining.min(READ_CHUNK_BYTES as u64)).unwrap_or(READ_CHUNK_BYTES);
        let read = file
            .read(&mut buffer[..want])
            .await
            .map_err(|error| PipelineError::Io {
                operation: "reading",
                path: source.to_path_buf(),
                source: error,
            })?;

        if read == 0 {
            break;
        }

        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }

    Ok(())
}

#[cfg(unix)]
fn mode_of(metadata: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode()
}

/// Windows has no mode bits worth carrying; the receiver applies its own default.
#[cfg(not(unix))]
fn mode_of(_metadata: &std::fs::Metadata) -> u32 {
    0
}
