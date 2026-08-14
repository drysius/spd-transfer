//! The sending side of a transfer.
//!
//! One file, one stream, in file order: the hash is fed as the bytes are read, so arrival
//! order and hashing order cannot disagree. The previous project hashed in arrival order
//! across parallel chunks and produced digests that did not match the file it had just
//! written.
//!
//! The conversation is always the same shape, whether it carries one file or ten thousand:
//! manifest batches, one reply each, an announcement of what is coming, then the bodies.
//! Bodies move in parallel; the control stream stays single-owner, with workers posting to
//! it through an outbox.
//!
//! A body may cross compressed. That is decided per file, before its stream is opened, and
//! announced in the stream's header - the receiver obeys the header and never its own
//! configuration, because only this side knows what it actually did.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinSet;

use crate::compress::{self, CompressError, Encoder};
use crate::pipeline::budget::{DEFAULT_MEM_BUDGET_BYTES, JobLimits, TransferPlan};
use crate::pipeline::bufpool::{BufferPool, PooledBuffer};
use crate::pipeline::control::{Outbox, spawn_outbox};
use crate::pipeline::cpu::on_cpu;
use crate::pipeline::prefix::hash_prefix;
use crate::pipeline::{PipelineError, TransferSummary};
use crate::proto::codec::{ControlReader, ControlWriter, write_data_header};
use crate::proto::messages::{Control, DataHeader, Decision, FileId};
use crate::safety::limits::Limits;
use crate::scan::hash_cache::{HASH_SIZE_CEILING_BYTES, HashCache, hash_file};
use crate::scan::manifest::{Manifest, ManifestFile, batches};
use crate::scan::walk::{WalkOptions, walk};
use crate::transport::session::{Session, Streams};

/// How the sender should behave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "these are the command-line switches; grouping them into enums would only put \
              a layer between the flag a user typed and the behaviour it names"
)]
pub struct SendOptions {
    /// Follow symlinks while scanning.
    pub follow_links: bool,

    /// Hash every file regardless of size, so the receiver decides on content rather than
    /// on timestamps. Slower, and the only way to be certain.
    pub checksum: bool,

    /// Run the whole negotiation and report what would move, without opening a single data
    /// stream.
    pub dry_run: bool,

    /// Compress a file when it looks worth compressing. Off means every body crosses as it
    /// is, which is what a fast link and a busy processor want.
    pub compress: bool,

    /// Memory the transfer may hold, in bytes. Concurrency follows from this.
    pub mem_budget_bytes: u64,

    /// Explicit ceilings on concurrent work.
    pub jobs: JobLimits,
}

impl Default for SendOptions {
    fn default() -> Self {
        Self {
            follow_links: false,
            checksum: false,
            dry_run: false,
            compress: true,
            mem_budget_bytes: DEFAULT_MEM_BUDGET_BYTES,
            jobs: JobLimits::DEFAULT,
        }
    }
}

/// What a send worked out and then did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SendReport {
    /// What actually crossed. Zero on a dry run.
    pub transferred: TransferSummary,
    /// What the receiver asked for, whether or not it was sent. Its `wire_bytes` is zero:
    /// how much would cross depends on what compresses, and that is decided file by file
    /// as each one is sent.
    pub planned: TransferSummary,
    /// Files the receiver already had.
    pub skipped: u64,
}

/// Sends a file or a whole directory tree, then closes the session.
///
/// Takes the session by value: once the transfer is over the connection has to be closed
/// in the right order, and leaving that to the caller is how the last message gets lost.
///
/// # Errors
/// [`PipelineError::Io`] if the tree cannot be read, [`PipelineError::HashMismatch`] if the
/// receiver rejects a file, [`PipelineError::UnexpectedMessage`] if the peer answers out of
/// order, or a transport error if the connection fails.
pub async fn send_tree(
    session: Session,
    root: &Path,
    options: SendOptions,
    limits: &Limits,
) -> Result<SendReport, PipelineError> {
    let manifest = build_manifest(root, options, limits).await?;
    tracing::info!(
        files = manifest.len(),
        bytes = manifest.total_bytes(),
        "offering"
    );

    let mut parts = session.split();
    let needed = negotiate(&mut parts.writer, &mut parts.reader, &manifest, limits).await?;

    let planned = TransferSummary {
        files: needed.len() as u64,
        bytes: needed
            .iter()
            .map(|need| need.remaining)
            .fold(0, u64::saturating_add),
        // Nothing has crossed yet, and how much will depends on what compresses, which is
        // not known until each file is sampled.
        wire_bytes: 0,
    };
    let skipped = manifest.len() as u64 - planned.files;

    let transferred = if options.dry_run {
        // Announce nothing, so the receiver stops waiting instead of holding a connection
        // open for streams that are not coming.
        parts
            .writer
            .send(&Control::Transfer { files: 0, bytes: 0 })
            .await?;
        parts
            .writer
            .send(&Control::Done { files: 0, bytes: 0 })
            .await?;
        parts.writer.close().await?;

        TransferSummary::default()
    } else {
        parts
            .writer
            .send(&Control::Transfer {
                files: planned.files,
                bytes: planned.bytes,
            })
            .await?;

        run_workers(&parts.streams, parts.writer, parts.reader, needed, options).await?
    };

    parts
        .streams
        .close_gracefully("transfer complete", limits.handshake_timeout)
        .await;

    Ok(SendReport {
        transferred,
        planned,
        skipped,
    })
}

/// One file the receiver asked for.
#[derive(Debug, Clone)]
struct Needed {
    file_id: FileId,
    source: PathBuf,
    from_offset: u64,
    remaining: u64,
}

/// Runs the bodies in parallel and collects the verdicts.
///
/// Three roles, three owners: workers stream files, one task owns the control writer, one
/// task reads replies and hands each verdict to whoever is waiting for that file.
async fn run_workers(
    streams: &Streams,
    writer: ControlWriter,
    reader: ControlReader,
    needed: Vec<Needed>,
    options: SendOptions,
) -> Result<TransferSummary, PipelineError> {
    let plan = TransferPlan::derive(options.mem_budget_bytes, options.jobs);
    tracing::info!(
        workers = plan.workers.get(),
        reserved_bytes = plan.reserved_bytes(),
        "starting transfer"
    );

    // Every verdict has somewhere to go before the first file is sent, so a fast peer
    // cannot answer before its worker is listening.
    let mut awaiting = HashMap::new();
    let mut queue = VecDeque::with_capacity(needed.len());
    for need in needed {
        let (answer, wait) = oneshot::channel();
        awaiting.insert(need.file_id, answer);
        queue.push_back((need, wait));
    }

    let (outbox, writer_task) = spawn_outbox(writer);
    let reader_task = tokio::spawn(route_verdicts(reader, awaiting));

    let pool = BufferPool::new(plan.buffers, plan.buffer_bytes);
    let disk_read = Arc::new(Semaphore::new(plan.disk_read_jobs.get() as usize));
    let cpu_jobs = Arc::new(Semaphore::new(plan.cpu_jobs.get() as usize));
    let queue = Arc::new(Mutex::new(queue));

    let mut workers = JoinSet::new();
    for _ in 0..plan.workers.get() {
        workers.spawn(worker(
            streams.clone(),
            outbox.clone(),
            Arc::clone(&queue),
            pool.clone(),
            Arc::clone(&disk_read),
            Arc::clone(&cpu_jobs),
            options,
        ));
    }

    let mut transferred = TransferSummary::default();
    let mut failure = None;

    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(Ok(summary)) => {
                transferred.files += summary.files;
                transferred.bytes = transferred.bytes.saturating_add(summary.bytes);
                transferred.wire_bytes = transferred.wire_bytes.saturating_add(summary.wire_bytes);
            }
            // Keep the first failure and let the others finish: aborting mid-flight would
            // leave streams half written and the peer waiting on them.
            Ok(Err(error)) => failure = failure.or(Some(error)),
            Err(joined) => {
                failure = failure.or(Some(PipelineError::Io {
                    operation: "running a transfer worker",
                    path: PathBuf::new(),
                    source: std::io::Error::other(joined),
                }));
            }
        }
    }

    if let Some(error) = failure {
        return Err(error);
    }

    outbox
        .send(Control::Done {
            files: transferred.files,
            bytes: transferred.bytes,
        })
        .await?;

    // Dropping every outbox is what tells the writer task to flush and finish.
    drop(outbox);
    join_control(writer_task, reader_task).await?;

    Ok(transferred)
}

/// A queue of files still to send, with the reply slot each one is waiting on.
type WorkQueue = Arc<Mutex<VecDeque<(Needed, oneshot::Receiver<bool>)>>>;

/// Pulls files from the shared queue until it is empty.
async fn worker(
    streams: Streams,
    outbox: Outbox,
    queue: WorkQueue,
    pool: BufferPool,
    disk_read: Arc<Semaphore>,
    cpu_jobs: Arc<Semaphore>,
    options: SendOptions,
) -> Result<TransferSummary, PipelineError> {
    let mut summary = TransferSummary::default();

    loop {
        let Some((need, verdict)) = queue.lock().await.pop_front() else {
            return Ok(summary);
        };

        let sent = {
            // The permit is held only while the file is being read, so a worker waiting
            // for a verdict is not also holding a disk slot.
            let _permit = disk_read.acquire().await;
            let compressed = worth_compressing(&need, &pool, &cpu_jobs, options).await?;
            stream_body(&streams, &need, &pool, &cpu_jobs, compressed).await?
        };

        outbox
            .send(Control::FileDone {
                file_id: need.file_id,
                hash: sent.hash,
            })
            .await?;

        // A dropped sender means the reader task stopped, which it only does on a failure
        // it already reported.
        let accepted = verdict.await.unwrap_or(false);
        if !accepted {
            return Err(PipelineError::HashMismatch { path: need.source });
        }

        summary.files += 1;
        summary.bytes = summary.bytes.saturating_add(need.remaining);
        summary.wire_bytes = summary.wire_bytes.saturating_add(sent.wire_bytes);
    }
}

/// Decides whether this file's body should cross compressed.
///
/// Cheap evidence first, as everywhere else: an extension that already means "compressed"
/// settles it without opening the file, which is what keeps a folder of video costing
/// nothing. Anything else is decided by compressing a sample of it.
///
/// A sampled file is read twice - once here, once for the body - but only its first
/// [`compress::SAMPLE_BYTES`], and the second read comes straight from the page cache.
async fn worth_compressing(
    need: &Needed,
    pool: &BufferPool,
    cpu_jobs: &Arc<Semaphore>,
    options: SendOptions,
) -> Result<bool, PipelineError> {
    if !options.compress || compress::is_already_compressed(&need.source) {
        return Ok(false);
    }

    let mut file = File::open(&need.source)
        .await
        .map_err(|source| PipelineError::Io {
            operation: "opening",
            path: need.source.clone(),
            source,
        })?;

    let mut sample = pool.acquire().await;
    let wanted = sample.bytes().len().min(compress::SAMPLE_BYTES);
    let read = file
        .read(&mut sample.bytes_mut()[..wanted])
        .await
        .map_err(|source| PipelineError::Io {
            operation: "reading",
            path: need.source.clone(),
            source,
        })?;

    let scratch = pool.acquire().await;
    let permit = Arc::clone(cpu_jobs).acquire_owned().await.map_err(closed)?;
    let (_returned, verdict) = on_cpu(permit, move || {
        let mut scratch = scratch;
        let verdict = compress::worth_compressing(&sample.bytes()[..read], scratch.bytes_mut());
        ((sample, scratch), verdict)
    })
    .await?;

    let verdict = verdict?;
    tracing::debug!(path = %need.source.display(), compress = verdict, "compression decided");

    Ok(verdict)
}

/// A closed CPU semaphore means the transfer is already shutting down.
fn closed(_closed: tokio::sync::AcquireError) -> PipelineError {
    PipelineError::Io {
        operation: "queueing CPU work for",
        path: PathBuf::new(),
        source: std::io::Error::other("the transfer is shutting down"),
    }
}

/// Reads replies and hands each verdict to the worker waiting for that file.
async fn route_verdicts(
    mut reader: ControlReader,
    mut awaiting: HashMap<FileId, oneshot::Sender<bool>>,
) -> Result<(), PipelineError> {
    while !awaiting.is_empty() {
        match reader.recv().await? {
            Control::FileVerdict { file_id, ok } => {
                let Some(answer) = awaiting.remove(&file_id) else {
                    return Err(PipelineError::UnknownFile { file_id });
                };
                // The worker may already have given up; its verdict then has nowhere to go
                // and the error it reported stands.
                let _ = answer.send(ok);
            }
            Control::Error { msg, .. } => return Err(PipelineError::PeerFailed { msg }),
            other => {
                return Err(PipelineError::UnexpectedMessage {
                    expected: "FileVerdict",
                    got: other.kind(),
                });
            }
        }
    }

    Ok(())
}

/// Waits for the writer and reader tasks, turning a panic into a reported error.
///
/// The writer goes first: its failure means the last message never left, which explains
/// anything the reader then reports.
async fn join_control(
    writer: tokio::task::JoinHandle<Result<(), crate::proto::codec::ProtoError>>,
    reader: tokio::task::JoinHandle<Result<(), PipelineError>>,
) -> Result<(), PipelineError> {
    writer.await.map_err(joined_error)??;
    reader.await.map_err(joined_error)?
}

fn joined_error(joined: tokio::task::JoinError) -> PipelineError {
    PipelineError::Io {
        operation: "running the control task",
        path: PathBuf::new(),
        source: std::io::Error::other(joined),
    }
}

/// Scans the tree and fills in the hashes that are cheap to know.
async fn build_manifest(
    root: &Path,
    options: SendOptions,
    limits: &Limits,
) -> Result<Manifest, PipelineError> {
    let root = root.to_path_buf();
    let walk_options = WalkOptions {
        follow_links: options.follow_links,
    };
    let limits = *limits;

    // Walking and hashing are both blocking disk work; keeping them off the async runtime
    // is the difference between a busy transfer and a stalled one.
    let manifest = tokio::task::spawn_blocking(move || {
        let scanned = walk(&root, walk_options, &limits)?;
        let mut manifest = Manifest::from_scan(scanned);

        // The cache lives beside the data it describes, so moving a folder takes its
        // hashes along.
        let cache_root = if root.is_dir() {
            root.clone()
        } else {
            root.parent().unwrap_or(&root).to_path_buf()
        };
        let mut cache = HashCache::open(&cache_root);

        for file in manifest.files_mut() {
            if !should_hash(file, options) {
                continue;
            }

            if let Some(known) = cache.get(&file.scanned) {
                file.hash = Some(known);
                continue;
            }

            match hash_file(&file.scanned.absolute) {
                Ok(hash) => {
                    cache.insert(&file.scanned, hash);
                    file.hash = Some(hash);
                }
                Err(error) => {
                    // A file that cannot be hashed can still be sent; the receiver falls
                    // back to size and mtime for it.
                    tracing::debug!(
                        path = %file.scanned.absolute.display(),
                        %error,
                        "could not hash before offering"
                    );
                }
            }
        }

        cache.save();
        Ok::<Manifest, crate::scan::manifest::ScanError>(manifest)
    })
    .await
    .map_err(joined_error)??;

    Ok(manifest)
}

fn should_hash(file: &ManifestFile, options: SendOptions) -> bool {
    options.checksum || file.scanned.size <= HASH_SIZE_CEILING_BYTES
}

/// Offers the manifest in batches and collects what the receiver wants.
async fn negotiate(
    writer: &mut ControlWriter,
    reader: &mut ControlReader,
    manifest: &Manifest,
    limits: &Limits,
) -> Result<Vec<Needed>, PipelineError> {
    let mut wire_batches = batches(manifest, limits.max_manifest_entries);

    // An empty tree still needs one exchange: the receiver learns there is nothing coming
    // from the batch marked last.
    if wire_batches.is_empty() {
        wire_batches.push(Vec::new());
    }

    let total_batches = wire_batches.len();
    let mut needed = Vec::new();

    for (index, entries) in wire_batches.into_iter().enumerate() {
        // A tree needing more than u32::MAX batches would hold trillions of files; the
        // saturated sequence number then fails the reply check rather than silently
        // pairing the wrong batch with the wrong answer.
        let batch_seq = u32::try_from(index).unwrap_or(u32::MAX);
        let last = index + 1 == total_batches;

        writer
            .send(&Control::Manifest {
                batch_seq,
                last,
                entries,
            })
            .await?;

        collect_decisions(reader, manifest, batch_seq, &mut needed).await?;
    }

    Ok(needed)
}

async fn collect_decisions(
    reader: &mut ControlReader,
    manifest: &Manifest,
    batch_seq: u32,
    needed: &mut Vec<Needed>,
) -> Result<(), PipelineError> {
    let reply = reader.recv().await?;

    let Control::SyncReply {
        batch_seq: answered,
        decisions,
    } = reply
    else {
        if let Control::Error { msg, .. } = reply {
            return Err(PipelineError::PeerFailed { msg });
        }
        return Err(PipelineError::UnexpectedMessage {
            expected: "SyncReply",
            got: reply.kind(),
        });
    };

    if answered != batch_seq {
        return Err(PipelineError::UnexpectedMessage {
            expected: "a reply to the batch just sent",
            got: "a reply to another batch",
        });
    }

    for decision in decisions {
        let Decision::Need {
            file_id,
            from_offset,
        } = decision
        else {
            continue;
        };

        // A decision about a file that was never offered means the peer lost track of the
        // manifest; continuing would send bytes under the wrong name.
        let file = manifest
            .find(file_id)
            .ok_or(PipelineError::UnknownFile { file_id })?;

        // A resume point past the end of the file would mean the peer has bytes this file
        // never had. Refuse before reading anything rather than send a body that cannot
        // possibly hash to what was offered.
        if from_offset > file.scanned.size {
            return Err(PipelineError::ResumeUnavailable {
                path: file.scanned.absolute.clone(),
                ends_at: file.scanned.size,
                needed: from_offset,
            });
        }

        needed.push(Needed {
            file_id,
            source: file.scanned.absolute.clone(),
            from_offset,
            remaining: file.scanned.size.saturating_sub(from_offset),
        });
    }

    Ok(())
}

/// What sending one body cost and proved.
#[derive(Debug, Clone, Copy)]
struct Sent {
    /// BLAKE3 of the whole file, compression or not.
    hash: [u8; 32],
    /// Bytes actually written to the stream.
    wire_bytes: u64,
}

/// Streams one file body on its own stream and returns its BLAKE3.
///
/// The hash covers the whole file, uncompressed, including the prefix the receiver already
/// has: both sides verify the same thing regardless of where the transfer resumed and of
/// what crossed the wire.
async fn stream_body(
    streams: &Streams,
    need: &Needed,
    pool: &BufferPool,
    cpu_jobs: &Arc<Semaphore>,
    compressed: bool,
) -> Result<Sent, PipelineError> {
    let source = need.source.as_path();
    let mut file = File::open(source)
        .await
        .map_err(|error| PipelineError::Io {
            operation: "opening",
            path: source.to_path_buf(),
            source: error,
        })?;

    let mut hasher = blake3::Hasher::new();

    // Everything before the resume point still has to be hashed, so read it even though it
    // is not sent: the hash both sides compare covers the whole file, not the part that
    // happened to cross this time.
    if need.from_offset > 0 {
        let covered = hash_prefix(&mut file, source, &mut hasher, need.from_offset, pool).await?;
        if covered != need.from_offset {
            return Err(PipelineError::ResumeUnavailable {
                path: source.to_path_buf(),
                ends_at: covered,
                needed: need.from_offset,
            });
        }

        file.seek(std::io::SeekFrom::Start(need.from_offset))
            .await
            .map_err(|error| PipelineError::Io {
                operation: "seeking",
                path: source.to_path_buf(),
                source: error,
            })?;
    }

    let mut stream = streams.open().await?;
    write_data_header(
        &mut stream,
        &DataHeader {
            file_id: need.file_id,
            offset: need.from_offset,
            compressed,
        },
    )
    .await?;

    let mut stage = Encoding::start(hasher, compressed, pool).await?;
    let mut wire_bytes = 0_u64;

    loop {
        let read = file
            .read(stage.input_mut())
            .await
            .map_err(|error| PipelineError::Io {
                operation: "reading",
                path: source.to_path_buf(),
                source: error,
            })?;

        if read == 0 {
            break;
        }

        stage.filled(read);

        // One pass is enough for a body crossing as it is; a compressed one goes round
        // until the codec has taken the whole chunk, because its output can need more room
        // than one buffer has.
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

            wire_bytes += send(&mut stream, stage.ready(), source).await?;
        }
    }

    while stage.unfinished() {
        let permit = Arc::clone(cpu_jobs).acquire_owned().await.map_err(closed)?;
        let (returned, tail) = on_cpu(permit, move || {
            let mut stage = stage;
            let tail = stage.tail();
            (stage, tail)
        })
        .await?;

        stage = returned;
        tail?;

        wire_bytes += send(&mut stream, stage.ready(), source).await?;
    }

    // Finishing the stream is how the receiver learns the file ended; without it, it waits
    // for bytes that are never coming.
    stream.finish().map_err(|error| PipelineError::Stream {
        operation: "closing the stream for",
        path: source.to_path_buf(),
        source: std::io::Error::other(error),
    })?;

    Ok(Sent {
        hash: stage.finish(),
        wire_bytes,
    })
}

/// Writes one block to the stream and says how many bytes that was.
async fn send(
    stream: &mut quinn::SendStream,
    block: &[u8],
    source: &Path,
) -> Result<u64, PipelineError> {
    if block.is_empty() {
        return Ok(0);
    }

    stream
        .write_all(block)
        .await
        .map_err(|error| PipelineError::Stream {
            operation: "sending",
            path: source.to_path_buf(),
            source: error.into(),
        })?;

    Ok(block.len() as u64)
}

/// The CPU half of sending one file: hashing it, and compressing it when that is worth
/// doing.
///
/// It owns its buffers so the whole thing can be handed to the CPU pool and taken back
/// without borrowing anything across the hop. A body crossing as it is never leaves the
/// input buffer: it is hashed where it was read and written from there, with no copy in
/// between.
struct Encoding {
    hasher: blake3::Hasher,
    encoder: Option<Encoder>,
    input: PooledBuffer,
    output: Option<PooledBuffer>,
    /// Bytes of `input` holding file data.
    filled: usize,
    /// How much of that the codec has taken.
    taken: usize,
    /// Whether this chunk has been hashed yet - once per chunk, however many codec passes
    /// it needs.
    hashed: bool,
    /// Bytes the last pass produced, in `output` when compressing and in `input` when not.
    produced: usize,
    /// Whether the compressed stream still has an ending to write.
    ending: bool,
}

impl Encoding {
    async fn start(
        hasher: blake3::Hasher,
        compressed: bool,
        pool: &BufferPool,
    ) -> Result<Self, PipelineError> {
        let (encoder, output) = if compressed {
            (Some(Encoder::new()?), Some(pool.acquire().await))
        } else {
            (None, None)
        };

        Ok(Self {
            hasher,
            encoder,
            input: pool.acquire().await,
            output,
            filled: 0,
            taken: 0,
            hashed: false,
            produced: 0,
            ending: compressed,
        })
    }

    /// Where the next chunk is read into.
    fn input_mut(&mut self) -> &mut [u8] {
        self.input.bytes_mut()
    }

    /// Announces how much of the input buffer the disk filled.
    fn filled(&mut self, read: usize) {
        self.filled = read;
        self.taken = 0;
        self.hashed = false;
        self.produced = 0;
    }

    /// Whether the current chunk has been fully taken by the codec.
    fn drained(&self) -> bool {
        self.taken >= self.filled
    }

    /// Whether the stream still has an ending to write.
    fn unfinished(&self) -> bool {
        self.ending
    }

    /// Hashes the chunk once, then compresses as much of it as fits in the output buffer.
    ///
    /// Runs on the CPU pool.
    fn step(&mut self) -> Result<(), CompressError> {
        if !self.hashed {
            self.hasher.update(&self.input.bytes()[..self.filled]);
            self.hashed = true;
        }

        let (Some(encoder), Some(output)) = (self.encoder.as_mut(), self.output.as_mut()) else {
            // Crossing as it is: the chunk is already where it needs to be.
            self.taken = self.filled;
            self.produced = self.filled;
            return Ok(());
        };

        let step = encoder.push(
            &self.input.bytes()[self.taken..self.filled],
            output.bytes_mut(),
        )?;
        self.taken += step.taken;
        self.produced = step.produced;

        Ok(())
    }

    /// Writes the next piece of the compressed stream's ending.
    ///
    /// Runs on the CPU pool.
    fn tail(&mut self) -> Result<(), CompressError> {
        let (Some(encoder), Some(output)) = (self.encoder.as_mut(), self.output.as_mut()) else {
            self.ending = false;
            self.produced = 0;
            return Ok(());
        };

        let tail = encoder.finish(output.bytes_mut())?;
        self.produced = tail.produced;
        self.ending = tail.more;

        Ok(())
    }

    /// What the last pass produced, ready to go on the wire.
    fn ready(&self) -> &[u8] {
        match self.output.as_ref() {
            Some(output) => &output.bytes()[..self.produced],
            None => &self.input.bytes()[..self.produced],
        }
    }

    /// The hash of everything that went through.
    fn finish(self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}
