//! The receiving side of a transfer.
//!
//! Bytes land in a `.part` file, are hashed as they arrive, and only become the real file
//! after the sender's hash matches. A failed transfer therefore leaves the previous
//! version of the file untouched: the rename is the commit.
//!
//! Files arrive in parallel, so one file's hash can show up on the control stream while
//! another is still being written. One task reads that stream and hands each hash to the
//! worker waiting for it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use quinn::RecvStream;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinSet;

use crate::pipeline::budget::{DEFAULT_MEM_BUDGET_BYTES, JobLimits, TransferPlan};
use crate::pipeline::bufpool::BufferPool;
use crate::pipeline::control::{Outbox, spawn_outbox};
use crate::pipeline::{PipelineError, TransferSummary};
use crate::proto::codec::{ControlReader, ControlWriter, read_data_header};
use crate::proto::messages::{Control, Decision, Entry, FileId};
use crate::safety::limits::Limits;
use crate::safety::path::SafeRelPath;
use crate::scan::diff::{LocalFile, decide};
use crate::scan::hash_cache::hash_file;
use crate::scan::walk::mtime_of;
use crate::transport::session::{Session, Streams};

/// Suffix for a file that is still arriving. Visible on purpose: an interrupted transfer
/// should be obvious in a directory listing, not hidden.
const PARTIAL_SUFFIX: &str = ".part";

/// How the receiver should behave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveOptions {
    /// Hash local files even when the sender offered no hash, so a matching timestamp is
    /// never taken as proof on its own.
    pub checksum: bool,

    /// Memory the transfer may hold, in bytes. Concurrency follows from this.
    pub mem_budget_bytes: u64,

    /// Explicit ceilings on concurrent work.
    pub jobs: JobLimits,
}

impl Default for ReceiveOptions {
    fn default() -> Self {
        Self {
            checksum: false,
            mem_budget_bytes: DEFAULT_MEM_BUDGET_BYTES,
            jobs: JobLimits::DEFAULT,
        }
    }
}

/// Receives a file or a whole tree into `destination`, then closes the session.
///
/// Takes the session by value: the connection has to be closed in the right order once the
/// transfer ends, and leaving that to the caller is how the last message gets lost.
///
/// # Errors
/// [`PipelineError::Path`] if a sender path is unsafe - nothing is written in that case -
/// [`PipelineError::Io`] on a local filesystem failure, [`PipelineError::HashMismatch`] if
/// arrived bytes do not match, or a transport error if the connection fails.
pub async fn receive_tree(
    session: Session,
    destination: &Path,
    options: ReceiveOptions,
    limits: &Limits,
) -> Result<TransferSummary, PipelineError> {
    let mut parts = session.split();

    let wanted = negotiate(
        &mut parts.writer,
        &mut parts.reader,
        destination,
        options,
        limits,
    )
    .await?;

    let coming = match parts.reader.recv().await? {
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

    let streams = parts.streams.clone();
    let summary = receive_bodies(
        &streams,
        parts.writer,
        parts.reader,
        coming,
        wanted,
        options,
        limits,
    )
    .await?;

    // The receiver hangs up first: the sender only says `Done` once every verdict has
    // reached it, so nothing of ours is still in flight to be discarded by the close.
    streams.close("transfer complete");

    Ok(summary)
}

/// A file this side agreed to receive.
#[derive(Debug, Clone)]
struct Wanted {
    relative: SafeRelPath,
    target: PathBuf,
    mode: u32,
}

/// Accepts the streams and writes them, several at a time.
async fn receive_bodies(
    streams: &Streams,
    writer: ControlWriter,
    reader: ControlReader,
    coming: u64,
    wanted: HashMap<FileId, Wanted>,
    options: ReceiveOptions,
    limits: &Limits,
) -> Result<TransferSummary, PipelineError> {
    let plan = TransferPlan::derive(options.mem_budget_bytes, options.jobs);
    tracing::info!(
        workers = plan.workers.get(),
        reserved_bytes = plan.reserved_bytes(),
        files = coming,
        "receiving"
    );

    // Every hash has somewhere to go before the first stream is accepted, so a sender that
    // finishes a small file immediately cannot outrun its worker.
    let mut answers = HashMap::new();
    let mut waiting = HashMap::new();
    for file_id in wanted.keys().copied() {
        let (answer, wait) = oneshot::channel();
        answers.insert(file_id, answer);
        waiting.insert(file_id, wait);
    }

    let (outbox, writer_task) = spawn_outbox(writer);
    let reader_task = tokio::spawn(route_hashes(reader, answers));

    let pool = BufferPool::new(plan.buffers, plan.buffer_bytes);
    let disk_write = Arc::new(Semaphore::new(plan.disk_write_jobs.get() as usize));
    let wanted = Arc::new(wanted);
    let waiting = Arc::new(Mutex::new(waiting));

    let mut workers = JoinSet::new();
    let mut summary = TransferSummary::default();
    let mut failure = None;

    for _ in 0..coming {
        // Hold the worker count at the plan: accepting every stream at once would mean as
        // many open files, and as many buffers, as the sender feels like offering.
        while workers.len() >= plan.workers.get() as usize {
            collect(workers.join_next().await, &mut summary, &mut failure);
        }

        let stream = streams.accept().await?;
        workers.spawn(worker(
            stream,
            Arc::clone(&wanted),
            Arc::clone(&waiting),
            outbox.clone(),
            pool.clone(),
            Arc::clone(&disk_write),
            *limits,
        ));
    }

    while let Some(joined) = workers.join_next().await {
        collect(Some(joined), &mut summary, &mut failure);
    }

    if let Some(error) = failure {
        return Err(error);
    }

    // Dropping every outbox is what tells the writer task to flush and finish.
    drop(outbox);
    writer_task.await.map_err(joined_error)??;
    reader_task.await.map_err(joined_error)??;

    Ok(summary)
}

type WorkerResult = Result<Result<u64, PipelineError>, tokio::task::JoinError>;

/// Folds one finished worker into the running totals, keeping the first failure.
fn collect(
    joined: Option<WorkerResult>,
    summary: &mut TransferSummary,
    failure: &mut Option<PipelineError>,
) {
    match joined {
        Some(Ok(Ok(bytes))) => {
            summary.files += 1;
            summary.bytes = summary.bytes.saturating_add(bytes);
        }
        Some(Ok(Err(error))) => *failure = failure.take().or(Some(error)),
        Some(Err(joined)) => *failure = failure.take().or(Some(joined_error(joined))),
        None => {}
    }
}

/// Receives one file: its stream, its hash, the verdict, and the rename that commits it.
async fn worker(
    mut stream: RecvStream,
    wanted: Arc<HashMap<FileId, Wanted>>,
    waiting: Arc<Mutex<HashMap<FileId, oneshot::Receiver<[u8; 32]>>>>,
    outbox: Outbox,
    pool: BufferPool,
    disk_write: Arc<Semaphore>,
    limits: Limits,
) -> Result<u64, PipelineError> {
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

    let body = {
        // The permit is held only while the file is being written, so a worker waiting for
        // a hash is not also holding a disk slot.
        let _permit = disk_write.acquire().await;
        write_body(stream, &partial, &pool, &limits).await?
    };

    let wait = waiting
        .lock()
        .await
        .remove(&header.file_id)
        .ok_or(PipelineError::UnknownFile {
            file_id: header.file_id,
        })?;

    // A dropped sender means the reader task stopped, which it only does on a failure it
    // already reported; an all-zero hash then fails the comparison and the partial file is
    // cleaned up rather than left to look finished.
    let claimed = wait.await.unwrap_or_default();
    let matches = claimed == body.hash;

    outbox
        .send(Control::FileVerdict {
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

/// Reads the control stream, handing each hash to the worker waiting for that file.
async fn route_hashes(
    mut reader: ControlReader,
    mut answers: HashMap<FileId, oneshot::Sender<[u8; 32]>>,
) -> Result<(), PipelineError> {
    loop {
        match reader.recv().await? {
            Control::FileDone { file_id, hash } => {
                let Some(answer) = answers.remove(&file_id) else {
                    return Err(PipelineError::UnknownFile { file_id });
                };
                // The worker may already have failed; its hash then has nowhere to go.
                let _ = answer.send(hash);
            }
            // `Done` is the last thing the sender says, so this is where the conversation
            // ends - including on a dry run, where no hash ever arrives.
            Control::Done { .. } => return Ok(()),
            Control::Error { msg, .. } => return Err(PipelineError::PeerFailed { msg }),
            other => {
                return Err(PipelineError::UnexpectedMessage {
                    expected: "FileDone or Done",
                    got: other.kind(),
                });
            }
        }
    }
}

/// Reads every manifest batch, answers each one, and remembers what was asked for.
async fn negotiate(
    writer: &mut ControlWriter,
    reader: &mut ControlReader,
    destination: &Path,
    options: ReceiveOptions,
    limits: &Limits,
) -> Result<HashMap<FileId, Wanted>, PipelineError> {
    let mut wanted = HashMap::new();

    loop {
        let message = reader.recv().await?;

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

        writer
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

/// What arrived on the data stream.
struct Body {
    bytes: u64,
    hash: [u8; 32],
}

async fn write_body(
    mut stream: RecvStream,
    partial: &Path,
    pool: &BufferPool,
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
    let mut buffer = pool.acquire().await;
    let mut total = 0_u64;

    loop {
        // `None` here is the end of the stream, which is the end of the file: the sender
        // finished it deliberately, so there is no length field to disagree with.
        let Some(read) =
            stream
                .read(buffer.bytes_mut())
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

        hasher.update(&buffer.bytes()[..read]);
        file.write_all(&buffer.bytes()[..read])
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

fn joined_error(joined: tokio::task::JoinError) -> PipelineError {
    PipelineError::Io {
        operation: "running a receive task",
        path: PathBuf::new(),
        source: std::io::Error::other(joined),
    }
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
    reason = "the unix arm is async; the call site must not care which platform it runs on"
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
