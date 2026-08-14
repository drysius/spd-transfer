//! The receiving side of a transfer.
//!
//! Bytes land in a `.part` file, are hashed as they arrive, and only become the real file
//! after the sender's hash matches. A failed transfer therefore leaves the previous
//! version of the file untouched: the rename is the commit.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

use crate::pipeline::{PipelineError, TransferSummary};
use crate::proto::codec::read_data_header;
use crate::proto::messages::{Control, Decision, Entry, FileId};
use crate::safety::limits::Limits;
use crate::safety::path::SafeRelPath;
use crate::scan::diff::{LocalFile, decide};
use crate::scan::hash_cache::hash_file;
use crate::scan::walk::mtime_of;
use crate::transport::session::Session;

/// Size of one read from the network.
const READ_CHUNK_BYTES: usize = 1024 * 1024;

/// Suffix for a file that is still arriving. Visible on purpose: an interrupted transfer
/// should be obvious in a directory listing, not hidden.
const PARTIAL_SUFFIX: &str = ".part";

/// How the receiver should behave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReceiveOptions {
    /// Hash local files even when the sender offered no hash, so a matching timestamp is
    /// never taken as proof on its own.
    pub checksum: bool,
}

/// Receives a file or a whole tree into `destination`.
///
/// # Errors
/// [`PipelineError::Path`] if a sender path is unsafe - nothing is written in that case -
/// [`PipelineError::Io`] on a local filesystem failure, [`PipelineError::HashMismatch`] if
/// arrived bytes do not match, or a transport error if the connection fails.
pub async fn receive_tree(
    session: &mut Session,
    destination: &Path,
    options: ReceiveOptions,
    limits: &Limits,
) -> Result<TransferSummary, PipelineError> {
    let wanted = negotiate(session, destination, options, limits).await?;

    let coming = match session.control().recv().await? {
        Control::Transfer { files, .. } => files,
        Control::Error { msg, .. } => return Err(PipelineError::PeerFailed { msg }),
        other => {
            return Err(PipelineError::UnexpectedMessage {
                expected: "Transfer",
                got: other.kind(),
            });
        }
    };

    // The sender may send fewer files than were asked for - a dry run sends none - but
    // never more: extra streams would be files nobody agreed to write.
    if coming > wanted.len() as u64 {
        return Err(PipelineError::UnexpectedMessage {
            expected: "at most the files that were requested",
            got: "more files than were requested",
        });
    }

    let mut summary = TransferSummary::default();
    for _ in 0..coming {
        let bytes = receive_one(session, &wanted, limits).await?;
        summary.files += 1;
        summary.bytes = summary.bytes.saturating_add(bytes);
    }

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

    Ok(summary)
}

/// A file this side agreed to receive.
#[derive(Debug, Clone)]
struct Wanted {
    relative: SafeRelPath,
    target: PathBuf,
    mode: u32,
}

/// Reads every manifest batch, answers each one, and remembers what was asked for.
async fn negotiate(
    session: &mut Session,
    destination: &Path,
    options: ReceiveOptions,
    limits: &Limits,
) -> Result<HashMap<FileId, Wanted>, PipelineError> {
    let mut wanted = HashMap::new();

    loop {
        let message = session.control().recv().await?;

        let Control::Manifest {
            batch_seq,
            last,
            entries,
        } = message
        else {
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
                expected: "a manifest batch within the limit",
                got: "an oversized batch",
            });
        }

        let mut decisions = Vec::with_capacity(entries.len());
        for entry in entries {
            // Validate before anything touches the filesystem, so a hostile path costs
            // nothing more than a rejected session.
            let relative = SafeRelPath::from_components(&entry.path, limits)?;
            let target = relative.resolve_under(destination)?;

            let local = inspect(&target, &entry, options).await;
            let decision = decide(&entry, local);

            if matches!(decision, Decision::Need { .. }) {
                wanted.insert(
                    entry.file_id,
                    Wanted {
                        relative,
                        target,
                        mode: entry.mode,
                    },
                );
            }

            decisions.push(decision);
        }

        session
            .control()
            .send(&Control::SyncReply {
                batch_seq,
                decisions,
            })
            .await?;

        if last {
            break;
        }
    }

    Ok(wanted)
}

/// Looks at the local copy, hashing it only when a hash could actually change the answer.
async fn inspect(target: &Path, entry: &Entry, options: ReceiveOptions) -> Option<LocalFile> {
    let metadata = fs::metadata(target).await.ok()?;
    if !metadata.is_file() {
        return None;
    }

    let size = metadata.len();
    let worth_hashing = size == entry.size && (entry.hash.is_some() || options.checksum);

    let hash = if worth_hashing {
        let path = target.to_path_buf();
        tokio::task::spawn_blocking(move || hash_file(&path))
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };

    Some(LocalFile {
        size,
        mtime: mtime_of(&metadata),
        hash,
    })
}

/// Receives one file: its stream, its hash, the verdict, and the rename that commits it.
async fn receive_one(
    session: &mut Session,
    wanted: &HashMap<FileId, Wanted>,
    limits: &Limits,
) -> Result<u64, PipelineError> {
    let mut stream = session.accept_data_stream().await?;
    let header = read_data_header(&mut stream).await?;

    // A stream may only carry a file the receiver asked for. Anything else is a peer
    // writing files nobody negotiated, which is how the previous project could be made to
    // create paths of the sender's choosing.
    let file = wanted
        .get(&header.file_id)
        .ok_or(PipelineError::UnknownFile {
            file_id: header.file_id,
        })?;

    let partial = with_partial_suffix(&file.target);
    let body = write_body(stream, &partial, limits).await?;
    let claimed = await_file_done(session, header.file_id).await?;
    let matches = claimed == body.hash;

    session
        .control()
        .send(&Control::FileVerdict {
            file_id: header.file_id,
            ok: matches,
        })
        .await?;

    if !matches {
        // Remove the partial file: keeping it would invite a resume that can never
        // succeed, since the bytes already on disk are the wrong ones.
        if let Err(error) = fs::remove_file(&partial).await {
            tracing::warn!(path = %partial.display(), %error, "could not remove the partial file");
        }
        return Err(PipelineError::HashMismatch {
            path: file.target.clone(),
        });
    }

    commit(&partial, &file.target).await?;
    apply_mode(&file.target, file.mode).await;
    tracing::info!(path = %file.relative, bytes = body.bytes, "file received");

    Ok(body.bytes)
}

/// What arrived on the data stream.
struct Body {
    bytes: u64,
    hash: [u8; 32],
}

async fn write_body(
    mut stream: quinn::RecvStream,
    partial: &Path,
    limits: &Limits,
) -> Result<Body, PipelineError> {
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

/// Applies the sender's mode bits.
///
/// Best effort: a file that arrived intact should not be reported as a failure because its
/// permissions could not be set.
#[cfg(unix)]
async fn apply_mode(target: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;

    if mode == 0 {
        return;
    }

    if let Err(error) = fs::set_permissions(target, std::fs::Permissions::from_mode(mode)).await {
        tracing::debug!(path = %target.display(), %error, "could not apply the sender's mode bits");
    }
}

/// Windows has no mode bits worth carrying, so the file keeps the local default.
#[cfg(not(unix))]
#[expect(
    clippy::unused_async,
    reason = "the unix arm is async; the call site must not care"
)]
async fn apply_mode(_target: &Path, _mode: u32) {}

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
