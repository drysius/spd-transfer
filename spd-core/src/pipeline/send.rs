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

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinSet;

use crate::pipeline::budget::{DEFAULT_MEM_BUDGET_BYTES, JobLimits, TransferPlan};
use crate::pipeline::bufpool::BufferPool;
use crate::pipeline::control::{Outbox, spawn_outbox};
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
pub struct SendOptions {
    /// Follow symlinks while scanning.
    pub follow_links: bool,

    /// Hash every file regardless of size, so the receiver decides on content rather than
    /// on timestamps. Slower, and the only way to be certain.
    pub checksum: bool,

    /// Run the whole negotiation and report what would move, without opening a single data
    /// stream.
    pub dry_run: bool,

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
    /// What the receiver asked for, whether or not it was sent.
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
    let queue = Arc::new(Mutex::new(queue));

    let mut workers = JoinSet::new();
    for _ in 0..plan.workers.get() {
        workers.spawn(worker(
            streams.clone(),
            outbox.clone(),
            Arc::clone(&queue),
            pool.clone(),
            Arc::clone(&disk_read),
        ));
    }

    let mut transferred = TransferSummary::default();
    let mut failure = None;

    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(Ok(summary)) => {
                transferred.files += summary.files;
                transferred.bytes = transferred.bytes.saturating_add(summary.bytes);
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
) -> Result<TransferSummary, PipelineError> {
    let mut summary = TransferSummary::default();

    loop {
        let Some((need, verdict)) = queue.lock().await.pop_front() else {
            return Ok(summary);
        };

        let hash = {
            // The permit is held only while the file is being read, so a worker waiting
            // for a verdict is not also holding a disk slot.
            let _permit = disk_read.acquire().await;
            stream_body(&streams, &need, &pool).await?
        };

        outbox
            .send(Control::FileDone {
                file_id: need.file_id,
                hash,
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

        needed.push(Needed {
            file_id,
            source: file.scanned.absolute.clone(),
            from_offset,
            remaining: file.scanned.size.saturating_sub(from_offset),
        });
    }

    Ok(())
}

/// Streams one file body on its own stream and returns its BLAKE3.
///
/// The hash covers the whole file, including the prefix the receiver already has: both
/// sides verify the same thing regardless of where the transfer resumed.
async fn stream_body(
    streams: &Streams,
    need: &Needed,
    pool: &BufferPool,
) -> Result<[u8; 32], PipelineError> {
    let source = need.source.as_path();
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
    if need.from_offset > 0 {
        hash_prefix(&mut file, source, &mut hasher, need.from_offset, pool).await?;
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
            compressed: false,
        },
    )
    .await?;

    let mut buffer = pool.acquire().await;
    loop {
        let read = file
            .read(buffer.bytes_mut())
            .await
            .map_err(|error| PipelineError::Io {
                operation: "reading",
                path: source.to_path_buf(),
                source: error,
            })?;

        if read == 0 {
            break;
        }

        hasher.update(&buffer.bytes()[..read]);
        stream
            .write_all(&buffer.bytes()[..read])
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
    pool: &BufferPool,
) -> Result<(), PipelineError> {
    let mut buffer = pool.acquire().await;
    let chunk = buffer.bytes().len();
    let mut remaining = length;

    while remaining > 0 {
        let want = usize::try_from(remaining).unwrap_or(chunk).min(chunk);
        let read = file
            .read(&mut buffer.bytes_mut()[..want])
            .await
            .map_err(|error| PipelineError::Io {
                operation: "reading",
                path: source.to_path_buf(),
                source: error,
            })?;

        if read == 0 {
            break;
        }

        hasher.update(&buffer.bytes()[..read]);
        remaining -= read as u64;
    }

    Ok(())
}
