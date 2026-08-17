//! The receiving side of a transfer.
//!
//! Bytes land in a `.part` file, are hashed as they arrive, and only become the real file
//! after the sender's hash matches. A failed transfer therefore leaves the previous
//! version of the file untouched: the rename is the commit.
//!
//! Files arrive in parallel, so one file's hash can show up on the control stream while
//! another is still being written. One task reads that stream and hands each hash to the
//! worker waiting for it.
//!
//! A `.part` file that survives an interrupted run is not thrown away: the state actor
//! remembers what it was started for, and if the sender offers that same file again the
//! transfer picks up where it stopped.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use quinn::RecvStream;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinSet;

use crate::compress::{CompressError, Decoder};
use crate::metrics::Progress;
use crate::pipeline::budget::{DEFAULT_MEM_BUDGET_BYTES, JobLimits, TransferPlan};
use crate::pipeline::bufpool::{BufferPool, PooledBuffer};
use crate::pipeline::control::{Outbox, spawn_outbox};
use crate::pipeline::cpu::on_cpu;
use crate::pipeline::prefix::hash_prefix;
use crate::pipeline::{PipelineError, TransferSummary};
use crate::proto::codec::{ControlReader, ControlWriter, read_data_header};
use crate::proto::messages::{Control, Decision, Entry, FileId};
use crate::safety::limits::Limits;
use crate::safety::path::SafeRelPath;
use crate::scan::diff::{LocalFile, decide};
use crate::scan::hash_cache::hash_file;
use crate::scan::walk::mtime_of;
use crate::state::model::{Expected, Partial};
use crate::state::{StateHandle, spawn_state};
use crate::transport::session::{Session, Streams};

/// Suffix for a file that is still arriving. Visible on purpose: an interrupted transfer
/// should be obvious in a directory listing, not hidden.
const PARTIAL_SUFFIX: &str = ".part";

/// How the receiver should behave.
///
/// Cloned rather than copied, for the same reason as [`crate::pipeline::send::SendOptions`]:
/// it carries the progress handle.
#[derive(Debug, Clone)]
pub struct ReceiveOptions {
    /// Hash local files even when the sender offered no hash, so a matching timestamp is
    /// never taken as proof on its own.
    pub checksum: bool,

    /// Memory the transfer may hold, in bytes. Concurrency follows from this.
    pub mem_budget_bytes: u64,

    /// Explicit ceilings on concurrent work.
    pub jobs: JobLimits,

    /// Counters for whoever is drawing progress. Ignoring them costs nothing.
    pub progress: Progress,
}

impl Default for ReceiveOptions {
    fn default() -> Self {
        Self {
            checksum: false,
            mem_budget_bytes: DEFAULT_MEM_BUDGET_BYTES,
            jobs: JobLimits::DEFAULT,
            progress: Progress::new(),
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
    let (state, state_task) = spawn_state(destination, limits).await?;
    let mut parts = session.split();

    let wanted = negotiate(
        &mut parts.writer,
        &mut parts.reader,
        destination,
        &state,
        &options,
        limits,
    )
    .await?;

    let coming = match parts.reader.recv().await? {
        Control::Transfer { files, bytes } => {
            // The sender's announcement is the only total this side can know before the
            // bytes turn up, and it is what a progress bar needs to draw anything.
            options.progress.expect(files, bytes);
            files
        }
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
        &state,
        &options,
        limits,
    )
    .await?;

    // The receiver hangs up first: the sender only says `Done` once every verdict has
    // reached it, so nothing of ours is still in flight to be discarded by the close.
    streams.close("transfer complete");

    // Dropping the last handle is what tells the state task to compact and finish; awaiting
    // it is how a caller learns the record on disk is settled.
    drop(state);
    state_task.await.map_err(joined_error)??;

    Ok(summary)
}

/// A file this side agreed to receive.
#[derive(Debug, Clone)]
struct Wanted {
    relative: SafeRelPath,
    target: PathBuf,
    mode: u32,
    /// Where the receiver asked the sender to start. Zero unless a `.part` file from an
    /// interrupted run is being continued.
    from_offset: u64,
    /// What the file is expected to be, kept so the `.part` can be recognised next time.
    expected: Expected,
}

/// Accepts the streams and writes them, several at a time.
#[expect(
    clippy::too_many_arguments,
    reason = "each one is a distinct collaborator with a single owner; bundling them would \
              only hide who holds what"
)]
async fn receive_bodies(
    streams: &Streams,
    writer: ControlWriter,
    reader: ControlReader,
    coming: u64,
    wanted: HashMap<FileId, Wanted>,
    state: &StateHandle,
    options: &ReceiveOptions,
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
    let cpu_jobs = Arc::new(Semaphore::new(plan.cpu_jobs.get() as usize));
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
            state.clone(),
            pool.clone(),
            Arc::clone(&disk_write),
            Arc::clone(&cpu_jobs),
            options.progress.clone(),
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

type WorkerResult = Result<Result<Body, PipelineError>, tokio::task::JoinError>;

/// Folds one finished worker into the running totals, keeping the first failure.
fn collect(
    joined: Option<WorkerResult>,
    summary: &mut TransferSummary,
    failure: &mut Option<PipelineError>,
) {
    match joined {
        Some(Ok(Ok(body))) => {
            summary.files += 1;
            summary.bytes = summary.bytes.saturating_add(body.bytes);
            summary.wire_bytes = summary.wire_bytes.saturating_add(body.wire_bytes);
        }
        Some(Ok(Err(error))) => *failure = failure.take().or(Some(error)),
        Some(Err(joined)) => *failure = failure.take().or(Some(joined_error(joined))),
        None => {}
    }
}

/// Receives one file: its stream, its hash, the verdict, and the rename that commits it.
#[expect(
    clippy::too_many_arguments,
    reason = "each one is a distinct collaborator with a single owner; bundling them would \
              only hide who holds what"
)]
async fn worker(
    mut stream: RecvStream,
    wanted: Arc<HashMap<FileId, Wanted>>,
    waiting: Arc<Mutex<HashMap<FileId, oneshot::Receiver<[u8; 32]>>>>,
    outbox: Outbox,
    state: StateHandle,
    pool: BufferPool,
    disk_write: Arc<Semaphore>,
    cpu_jobs: Arc<Semaphore>,
    progress: Progress,
    limits: Limits,
) -> Result<Body, PipelineError> {
    let header = read_data_header(&mut stream).await?;

    // A stream may only carry a file the receiver asked for. Anything else is a peer
    // writing files nobody negotiated, which is how the previous project could be made to
    // create paths of the sender's choosing.
    let file = wanted
        .get(&header.file_id)
        .ok_or(PipelineError::UnknownFile {
            file_id: header.file_id,
        })?;

    // Only this side decides where a file resumes. A stream starting anywhere else would
    // leave a hole in the middle of the file that no hash could later explain.
    if header.offset != file.from_offset {
        return Err(PipelineError::ResumeOffset {
            path: file.target.clone(),
            got: header.offset,
            agreed: file.from_offset,
        });
    }

    let partial = with_partial_suffix(&file.target);

    // Recorded before the first byte lands: a `.part` file nobody can identify is a `.part`
    // file that has to be thrown away.
    state.started(file.relative.clone(), file.expected).await?;

    let body = {
        // The permit is held only while the file is being written, so a worker waiting for
        // a hash is not also holding a disk slot.
        let _permit = disk_write.acquire().await;
        write_body(
            stream,
            &partial,
            file.from_offset,
            header.compressed,
            &pool,
            &cpu_jobs,
            &progress,
            &limits,
        )
        .await?
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
        state.forget(file.relative.clone()).await?;
        return Err(PipelineError::HashMismatch {
            path: file.target.clone(),
        });
    }

    commit(&partial, &file.target).await?;
    state.forget(file.relative.clone()).await?;
    apply_mode(&file.target, file.mode).await;
    apply_mtime(&file.target, file.expected.mtime).await;
    progress.finished_file();
    tracing::info!(
        path = %file.relative,
        bytes = body.bytes,
        wire_bytes = body.wire_bytes,
        resumed_from = file.from_offset,
        "file received"
    );

    Ok(body)
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
    state: &StateHandle,
    options: &ReceiveOptions,
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
            return Err(PipelineError::TooManyFiles {
                limit: "max_manifest_entries",
                max: limits.max_manifest_entries as u64,
            });
        }

        let mut decisions = Vec::with_capacity(entries.len());
        for entry in entries {
            // Validate before anything touches the filesystem, so a hostile path costs
            // nothing more than a rejected session.
            let relative = SafeRelPath::from_components(&entry.path, limits)?;
            let target = relative.resolve_under(destination)?;

            let local = inspect(&target, &entry, options.checksum).await;
            let partial = unfinished(&target, &relative, &entry, state).await?;
            let decision = decide(&entry, local, partial);

            if let Decision::Need { from_offset, .. } = decision {
                // Checked as they accumulate, not per batch: a peer can send any number of
                // batches that are individually within the limit.
                if wanted.len() as u64 >= limits.max_files {
                    return Err(PipelineError::TooManyFiles {
                        limit: "max_files",
                        max: limits.max_files,
                    });
                }

                wanted.insert(
                    entry.file_id,
                    Wanted {
                        relative,
                        target,
                        mode: entry.mode,
                        from_offset,
                        expected: Expected::of(&entry),
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

/// Looks for a `.part` file this transfer could continue.
///
/// The journal is asked first, and it is in memory: a tree with nothing left half-written -
/// which is every tree that has never been interrupted - costs no filesystem calls at all.
async fn unfinished(
    target: &Path,
    relative: &SafeRelPath,
    entry: &Entry,
    state: &StateHandle,
) -> Result<Option<Partial>, PipelineError> {
    let Some(expected) = state.partial(relative.clone()).await? else {
        return Ok(None);
    };

    // The record says what the bytes were meant to become; only the filesystem knows how
    // far they got, and a record whose `.part` file is gone means nothing.
    let Ok(metadata) = fs::metadata(with_partial_suffix(target)).await else {
        return Ok(None);
    };

    if !metadata.is_file() {
        return Ok(None);
    }

    tracing::debug!(
        path = %relative,
        bytes_on_disk = metadata.len(),
        still_offered = expected.still_matches(entry),
        "found an unfinished file"
    );

    Ok(Some(Partial {
        bytes_on_disk: metadata.len(),
        expected,
    }))
}

/// Looks at the local copy, hashing it only when a hash could actually change the answer.
async fn inspect(target: &Path, entry: &Entry, checksum: bool) -> Option<LocalFile> {
    let metadata = fs::metadata(target).await.ok()?;
    if !metadata.is_file() {
        return None;
    }

    let size = metadata.len();
    let worth_hashing = size == entry.size && (entry.hash.is_some() || checksum);

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
#[derive(Debug, Clone, Copy)]
struct Body {
    /// File bytes written in this session; a resumed file wrote fewer than it is long.
    bytes: u64,
    /// Bytes that actually arrived, before decompression.
    wire_bytes: u64,
    /// BLAKE3 of the whole file, prefix included.
    hash: [u8; 32],
}

/// Writes the body into the `.part` file, continuing from `from_offset`.
///
/// The hash covers the file from byte zero, so a resumed transfer reads the prefix already
/// on disk before it writes anything: what both sides verify is the whole file, never just
/// the piece that happened to cross this time.
///
/// `compressed` comes from the stream's own header, never from this side's configuration:
/// the sender decided per file and only it knows what it did.
#[expect(
    clippy::too_many_arguments,
    reason = "each one is a distinct collaborator with a single owner; bundling them would \
              only hide who holds what"
)]
async fn write_body(
    mut stream: RecvStream,
    partial: &Path,
    from_offset: u64,
    compressed: bool,
    pool: &BufferPool,
    cpu_jobs: &Arc<Semaphore>,
    progress: &Progress,
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

    let mut hasher = blake3::Hasher::new();
    let mut file = if from_offset == 0 {
        File::create(partial)
            .await
            .map_err(|source| PipelineError::Io {
                operation: "creating",
                path: partial.to_path_buf(),
                source,
            })?
    } else {
        open_for_resume(partial, from_offset, &mut hasher, pool).await?
    };

    let mut stage = Decoding::start(hasher, compressed, pool).await?;
    let mut written = 0_u64;
    let mut wire_bytes = 0_u64;

    loop {
        // `None` here is the end of the stream, which is the end of the file: the sender
        // finished it deliberately, so there is no length field to disagree with.
        let Some(read) =
            stream
                .read(stage.input_mut())
                .await
                .map_err(|source| PipelineError::Stream {
                    operation: "receiving",
                    path: partial.to_path_buf(),
                    source: source.into(),
                })?
        else {
            break;
        };

        wire_bytes += read as u64;
        stage.filled(read);
        let mut chunk_written = 0_u64;

        // A compressed buffer can expand into several: each one is written before the next
        // is produced, so a peer cannot make this side hold an arbitrary amount of memory
        // by sending a small stream that unpacks into a large one.
        while !stage.drained() {
            let permit = Arc::clone(cpu_jobs).acquire_owned().await.map_err(closed)?;
            let (returned, stepped) = on_cpu(permit, move || {
                let mut stage = stage;
                let stepped = stage.step();
                (stage, stepped)
            })
            .await?;

            stage = returned;
            stepped?;

            let block = stage.ready();
            written += block.len() as u64;
            chunk_written += block.len() as u64;

            let total = from_offset.saturating_add(written);
            if total > limits.max_file_size_bytes {
                return Err(PipelineError::FileTooLarge {
                    path: partial.to_path_buf(),
                    size: total,
                    max: limits.max_file_size_bytes,
                });
            }

            file.write_all(block)
                .await
                .map_err(|source| PipelineError::Io {
                    operation: "writing",
                    path: partial.to_path_buf(),
                    source,
                })?;
        }

        progress.advance(chunk_written, read as u64);
    }

    // Flush to the device before the rename: a rename that survives a crash while its
    // contents do not is worse than no file at all.
    file.sync_all().await.map_err(|source| PipelineError::Io {
        operation: "flushing",
        path: partial.to_path_buf(),
        source,
    })?;

    Ok(Body {
        bytes: written,
        wire_bytes,
        hash: stage.finish(),
    })
}

/// A closed CPU semaphore means the transfer is already shutting down.
fn closed(_closed: tokio::sync::AcquireError) -> PipelineError {
    PipelineError::Io {
        operation: "queueing CPU work for",
        path: PathBuf::new(),
        source: std::io::Error::other("the transfer is shutting down"),
    }
}

/// The CPU half of receiving one file: decompressing it when the sender compressed it, and
/// hashing what comes out.
///
/// Mirror of the sending side, and owns its buffers for the same reason: everything travels
/// to the CPU pool by value, so nothing is borrowed across a hop that may be cancelled.
/// A body that arrived as it is never leaves the input buffer.
struct Decoding {
    hasher: blake3::Hasher,
    decoder: Option<Decoder>,
    input: PooledBuffer,
    output: Option<PooledBuffer>,
    /// Bytes of `input` holding what arrived.
    filled: usize,
    /// How much of that the codec has taken.
    taken: usize,
    /// Bytes the last pass produced, in `output` when decompressing and in `input` when not.
    produced: usize,
}

impl Decoding {
    async fn start(
        hasher: blake3::Hasher,
        compressed: bool,
        pool: &BufferPool,
    ) -> Result<Self, PipelineError> {
        let (decoder, output) = if compressed {
            (Some(Decoder::new()?), Some(pool.acquire().await))
        } else {
            (None, None)
        };

        Ok(Self {
            hasher,
            decoder,
            input: pool.acquire().await,
            output,
            filled: 0,
            taken: 0,
            produced: 0,
        })
    }

    /// Where the next arriving bytes are read into.
    fn input_mut(&mut self) -> &mut [u8] {
        self.input.bytes_mut()
    }

    /// Announces how much the stream delivered.
    fn filled(&mut self, read: usize) {
        self.filled = read;
        self.taken = 0;
        self.produced = 0;
    }

    /// Whether everything that arrived has been through the codec.
    fn drained(&self) -> bool {
        self.taken >= self.filled
    }

    /// Decompresses as much as fits in the output buffer, then hashes what came out.
    ///
    /// Hashing after decoding, never before: the hash is of the file, and the compressed
    /// bytes are only how it travelled.
    ///
    /// Runs on the CPU pool.
    fn step(&mut self) -> Result<(), CompressError> {
        let (Some(decoder), Some(output)) = (self.decoder.as_mut(), self.output.as_mut()) else {
            // Arrived as it is: what came in is what goes to disk.
            self.taken = self.filled;
            self.produced = self.filled;
            self.hasher.update(&self.input.bytes()[..self.filled]);
            return Ok(());
        };

        let step = decoder.pull(
            &self.input.bytes()[self.taken..self.filled],
            output.bytes_mut(),
        )?;
        self.taken += step.taken;
        self.produced = step.produced;
        self.hasher.update(&output.bytes()[..step.produced]);

        Ok(())
    }

    /// What the last pass produced, ready for the disk.
    fn ready(&self) -> &[u8] {
        match self.output.as_ref() {
            Some(output) => &output.bytes()[..self.produced],
            None => &self.input.bytes()[..self.produced],
        }
    }

    /// The hash of the whole file.
    fn finish(self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}

/// Opens a `.part` file to continue it, leaving the cursor exactly at `from_offset`.
///
/// Truncating first is what makes the offset a fact rather than a hope: whatever is beyond
/// the agreed point was never accounted for by either side, and keeping it would put bytes
/// after the ones about to arrive.
async fn open_for_resume(
    partial: &Path,
    from_offset: u64,
    hasher: &mut blake3::Hasher,
    pool: &BufferPool,
) -> Result<File, PipelineError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(partial)
        .await
        .map_err(|source| PipelineError::Io {
            operation: "reopening",
            path: partial.to_path_buf(),
            source,
        })?;

    file.set_len(from_offset)
        .await
        .map_err(|source| PipelineError::Io {
            operation: "trimming",
            path: partial.to_path_buf(),
            source,
        })?;

    let covered = hash_prefix(&mut file, partial, hasher, from_offset, pool).await?;
    if covered != from_offset {
        return Err(PipelineError::ResumeUnavailable {
            path: partial.to_path_buf(),
            ends_at: covered,
            needed: from_offset,
        });
    }

    // Reading the prefix left the cursor at its end, which is where the arriving bytes go.
    Ok(file)
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

/// Gives the received file the modification time it had on the sender.
///
/// Without this a copied file is stamped with the moment it arrived, and the next run
/// compares that against the sender's timestamp, finds them different, and sends the whole
/// file again. Every run would move the whole tree - which is the one thing a synchronising
/// transfer must not do.
///
/// Best effort: a filesystem that will not take a timestamp costs a resend later, never a
/// wrong file now, and refusing a transfer over it would be absurd.
async fn apply_mtime(target: &Path, mtime: u64) {
    // Zero is the scan's word for "the filesystem could not say". Stamping it would claim
    // 1970 and make every later comparison disagree on purpose.
    if mtime == 0 {
        return;
    }

    let when = std::time::UNIX_EPOCH + std::time::Duration::from_secs(mtime);
    let target = target.to_path_buf();

    let applied = tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .write(true)
            .open(&target)?
            .set_modified(when)
    })
    .await;

    match applied {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::debug!(%error, "could not apply the sender's timestamp");
        }
        Err(joined) => {
            tracing::debug!(%joined, "could not apply the sender's timestamp");
        }
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
