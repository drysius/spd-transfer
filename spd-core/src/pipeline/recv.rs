//! The receiving side of a transfer.
//!
//! Bytes land in a `.part` file, are hashed as they arrive, and only become the real file
//! after the sender's hash matches. A failed transfer therefore leaves the previous
//! version of the file untouched: the rename is the commit.

use std::path::{Path, PathBuf};

use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

use crate::pipeline::{PipelineError, TransferSummary};
use crate::proto::codec::read_data_header;
use crate::proto::messages::{Control, Decision, Entry, FileId};
use crate::safety::limits::Limits;
use crate::safety::path::SafeRelPath;
use crate::transport::session::Session;

/// Size of one read from the network.
const READ_CHUNK_BYTES: usize = 1024 * 1024;

/// Suffix for a file that is still arriving. Visible on purpose: an interrupted transfer
/// should be obvious in a directory listing, not hidden.
const PARTIAL_SUFFIX: &str = ".part";

/// Receives one file into `destination`.
///
/// # Errors
/// [`PipelineError::Path`] if the sender's path is unsafe - nothing is written in that
/// case - [`PipelineError::Io`] on a local filesystem failure,
/// [`PipelineError::HashMismatch`] if the arrived bytes do not match, or a transport error
/// if the connection fails.
pub async fn receive_file(
    session: &mut Session,
    destination: &Path,
    limits: &Limits,
) -> Result<TransferSummary, PipelineError> {
    let offered = await_manifest(session, limits).await?;
    let target = offered.relative.resolve_under(destination)?;
    let partial = with_partial_suffix(&target);

    session
        .control()
        .send(&Control::SyncReply {
            batch_seq: 0,
            decisions: vec![Decision::Need {
                file_id: offered.file_id,
                from_offset: 0,
            }],
        })
        .await?;

    let received = write_body(session, &partial, offered.file_id, limits).await?;
    let claimed = await_file_done(session, offered.file_id).await?;
    let matches = claimed == received.hash;

    session
        .control()
        .send(&Control::FileVerdict {
            file_id: offered.file_id,
            ok: matches,
        })
        .await?;

    if !matches {
        // Remove the partial file: keeping it would invite a resume that can never
        // succeed, since the bytes already on disk are the wrong ones.
        if let Err(error) = fs::remove_file(&partial).await {
            tracing::warn!(path = %partial.display(), %error, "could not remove the partial file");
        }
        return Err(PipelineError::HashMismatch { path: target });
    }

    commit(&partial, &target).await?;

    match session.control().recv().await? {
        Control::Done { .. } => {}
        Control::Error { msg, .. } => return Err(PipelineError::PeerFailed { msg }),
        other => {
            return Err(PipelineError::UnexpectedMessage {
                expected: "Done",
                got: other.kind(),
            });
        }
    }

    tracing::info!(path = %target.display(), bytes = received.bytes, "file received");

    Ok(TransferSummary {
        files: 1,
        bytes: received.bytes,
    })
}

/// One file the sender offered, with its path already validated.
struct Offered {
    file_id: FileId,
    relative: SafeRelPath,
}

async fn await_manifest(session: &mut Session, limits: &Limits) -> Result<Offered, PipelineError> {
    let message = session.control().recv().await?;

    let Control::Manifest { entries, .. } = message else {
        if let Control::Error { msg, .. } = message {
            return Err(PipelineError::PeerFailed { msg });
        }
        return Err(PipelineError::UnexpectedMessage {
            expected: "Manifest",
            got: message.kind(),
        });
    };

    if entries.len() > limits.max_manifest_entries {
        return Err(PipelineError::UnexpectedMessage {
            expected: "a manifest within the batch limit",
            got: "an oversized batch",
        });
    }

    let entry: Entry = entries
        .into_iter()
        .next()
        .ok_or(PipelineError::UnexpectedMessage {
            expected: "a manifest with at least one entry",
            got: "an empty manifest",
        })?;

    // Validate before anything touches the filesystem, so a hostile path costs nothing
    // more than a rejected session.
    let relative = SafeRelPath::from_components(&entry.path, limits)?;

    Ok(Offered {
        file_id: entry.file_id,
        relative,
    })
}

/// What arrived on the data stream.
struct Body {
    bytes: u64,
    hash: [u8; 32],
}

async fn write_body(
    session: &Session,
    partial: &Path,
    expected_id: FileId,
    limits: &Limits,
) -> Result<Body, PipelineError> {
    let mut stream = session.accept_data_stream().await?;
    let header = read_data_header(&mut stream).await?;

    // A stream may only carry a file the receiver asked for. Anything else is a peer
    // writing files nobody negotiated, which is how the previous project could be made to
    // create paths of the sender's choosing.
    if header.file_id != expected_id {
        return Err(PipelineError::UnknownFile {
            file_id: header.file_id,
        });
    }

    if let Some(parent) = partial.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|source| PipelineError::Io {
                operation: "creating the directory",
                path: parent.to_path_buf(),
                source,
            })?;
    }

    let mut file = File::create(partial)
        .await
        .map_err(|source| PipelineError::Io {
            operation: "creating",
            path: partial.to_path_buf(),
            source,
        })?;

    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; READ_CHUNK_BYTES];
    let mut total = 0_u64;

    loop {
        // `None` here is the end of the stream, which is the end of the file: the sender
        // finished it deliberately, so there is no length field to disagree with.
        let Some(read) = stream
            .read(&mut buffer)
            .await
            .map_err(|source| PipelineError::Io {
                operation: "receiving",
                path: partial.to_path_buf(),
                source: source.into(),
            })?
        else {
            break;
        };

        total += read as u64;
        if total > limits.max_file_size_bytes {
            return Err(PipelineError::FileTooLarge {
                path: partial.to_path_buf(),
                size: total,
                max: limits.max_file_size_bytes,
            });
        }

        hasher.update(&buffer[..read]);
        file.write_all(&buffer[..read])
            .await
            .map_err(|source| PipelineError::Io {
                operation: "writing",
                path: partial.to_path_buf(),
                source,
            })?;
    }

    // Flush to the device before the rename: a rename that survives a crash while its
    // contents do not is worse than no file at all.
    file.sync_all().await.map_err(|source| PipelineError::Io {
        operation: "flushing",
        path: partial.to_path_buf(),
        source,
    })?;

    Ok(Body {
        bytes: total,
        hash: *hasher.finalize().as_bytes(),
    })
}

async fn await_file_done(
    session: &mut Session,
    expected_id: FileId,
) -> Result<[u8; 32], PipelineError> {
    match session.control().recv().await? {
        Control::FileDone { file_id, hash } if file_id == expected_id => Ok(hash),
        Control::FileDone { file_id, .. } => Err(PipelineError::UnknownFile { file_id }),
        Control::Error { msg, .. } => Err(PipelineError::PeerFailed { msg }),
        other => Err(PipelineError::UnexpectedMessage {
            expected: "FileDone",
            got: other.kind(),
        }),
    }
}

/// Publishes the verified file, replacing any previous version in one step.
async fn commit(partial: &Path, target: &Path) -> Result<(), PipelineError> {
    fs::rename(partial, target)
        .await
        .map_err(|source| PipelineError::Io {
            operation: "renaming into place",
            path: target.to_path_buf(),
            source,
        })
}

fn with_partial_suffix(target: &Path) -> PathBuf {
    let mut name = target.as_os_str().to_os_string();
    name.push(PARTIAL_SUFFIX);
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_partial_file_sits_next_to_its_target() {
        let target = Path::new("out").join("photo.jpg");
        let partial = with_partial_suffix(&target);

        assert_eq!(partial.parent(), target.parent());
        assert!(partial.to_string_lossy().ends_with("photo.jpg.part"));
    }
}
