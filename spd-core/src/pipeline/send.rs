//! The sending side of a transfer.
//!
//! One file, one stream, in file order: the hash is fed as the bytes are read, so arrival
//! order and hashing order cannot disagree. The previous project hashed in arrival order
//! across parallel chunks and produced digests that did not match the file it had just
//! written.
//!
//! The conversation is always the same shape, whether it carries one file or ten thousand:
//! manifest batches, one reply each, an announcement of what is coming, then the bodies.

use std::path::{Path, PathBuf};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::pipeline::{PipelineError, TransferSummary};
use crate::proto::codec::write_data_header;
use crate::proto::messages::{Control, DataHeader, Decision, FileId};
use crate::safety::limits::Limits;
use crate::scan::hash_cache::{HASH_SIZE_CEILING_BYTES, HashCache, hash_file};
use crate::scan::manifest::{Manifest, ManifestFile, batches};
use crate::scan::walk::{WalkOptions, walk};
use crate::transport::session::Session;

/// Size of one read from disk. Replaced by the pooled buffers of F4; until then it is a
/// single allocation reused for the whole file.
const READ_CHUNK_BYTES: usize = 1024 * 1024;

/// How the sender should behave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SendOptions {
    /// Follow symlinks while scanning.
    pub follow_links: bool,

    /// Hash every file regardless of size, so the receiver decides on content rather than
    /// on timestamps. Slower, and the only way to be certain.
    pub checksum: bool,

    /// Run the whole negotiation and report what would move, without opening a single data
    /// stream.
    pub dry_run: bool,
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

/// Sends a file or a whole directory tree over an established session.
///
/// # Errors
/// [`PipelineError::Io`] if the tree cannot be read, [`PipelineError::HashMismatch`] if the
/// receiver rejects a file, [`PipelineError::UnexpectedMessage`] if the peer answers out of
/// order, or a transport error if the connection fails.
pub async fn send_tree(
    session: &mut Session,
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

    let needed = negotiate(session, &manifest, limits).await?;
    let planned = TransferSummary {
        files: needed.len() as u64,
        bytes: needed
            .iter()
            .map(|need| need.remaining)
            .fold(0, u64::saturating_add),
    };
    let skipped = manifest.len() as u64 - planned.files;

    if options.dry_run {
        // Announce nothing, so the receiver stops waiting instead of holding a connection
        // open for streams that are not coming.
        session
            .control()
            .send(&Control::Transfer { files: 0, bytes: 0 })
            .await?;
        session
            .control()
            .send(&Control::Done { files: 0, bytes: 0 })
            .await?;

        return Ok(SendReport {
            transferred: TransferSummary::default(),
            planned,
            skipped,
        });
    }

    session
        .control()
        .send(&Control::Transfer {
            files: planned.files,
            bytes: planned.bytes,
        })
        .await?;

    let mut transferred = TransferSummary::default();
    for need in &needed {
        send_one(session, need).await?;
        transferred.files += 1;
        transferred.bytes = transferred.bytes.saturating_add(need.remaining);
    }

    session
        .control()
        .send(&Control::Done {
            files: transferred.files,
            bytes: transferred.bytes,
        })
        .await?;

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
    .map_err(|joined| PipelineError::Io {
        operation: "scanning",
        path: PathBuf::new(),
        source: std::io::Error::other(joined),
    })??;

    Ok(manifest)
}

fn should_hash(file: &ManifestFile, options: SendOptions) -> bool {
    options.checksum || file.scanned.size <= HASH_SIZE_CEILING_BYTES
}

/// Offers the manifest in batches and collects what the receiver wants.
async fn negotiate(
    session: &mut Session,
    manifest: &Manifest,
    limits: &Limits,
) -> Result<Vec<Needed>, PipelineError> {
    let wire_batches = batches(manifest, limits.max_manifest_entries);
    let total_batches = wire_batches.len().max(1);
    let mut needed = Vec::new();

    for (index, entries) in wire_batches.into_iter().enumerate() {
        // A tree needing more than u32::MAX batches would hold trillions of files; the
        // saturated sequence number then fails the reply check rather than silently
        // pairing the wrong batch with the wrong answer.
        let batch_seq = u32::try_from(index).unwrap_or(u32::MAX);
        let last = index + 1 == total_batches;

        session
            .control()
            .send(&Control::Manifest {
                batch_seq,
                last,
                entries,
            })
            .await?;

        collect_decisions(session, manifest, batch_seq, &mut needed).await?;
    }

    // An empty tree still needs one exchange: the receiver has to learn there is nothing
    // coming, and it learns that from the last batch.
    if manifest.is_empty() {
        session
            .control()
            .send(&Control::Manifest {
                batch_seq: 0,
                last: true,
                entries: Vec::new(),
            })
            .await?;
        collect_decisions(session, manifest, 0, &mut needed).await?;
    }

    Ok(needed)
}

async fn collect_decisions(
    session: &mut Session,
    manifest: &Manifest,
    batch_seq: u32,
    needed: &mut Vec<Needed>,
) -> Result<(), PipelineError> {
    let reply = session.control().recv().await?;

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

/// Streams one file and waits for the receiver's verdict on it.
async fn send_one(session: &mut Session, need: &Needed) -> Result<(), PipelineError> {
    let hash = stream_body(session, &need.source, need.file_id, need.from_offset).await?;

    session
        .control()
        .send(&Control::FileDone {
            file_id: need.file_id,
            hash,
        })
        .await?;

    match session.control().recv().await? {
        Control::FileVerdict { ok: true, file_id } if file_id == need.file_id => Ok(()),
        Control::FileVerdict { ok: false, .. } => Err(PipelineError::HashMismatch {
            path: need.source.clone(),
        }),
        Control::FileVerdict { file_id, .. } => Err(PipelineError::UnknownFile { file_id }),
        Control::Error { msg, .. } => Err(PipelineError::PeerFailed { msg }),
        other => Err(PipelineError::UnexpectedMessage {
            expected: "FileVerdict",
            got: other.kind(),
        }),
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
        let want = usize::try_from(remaining)
            .unwrap_or(READ_CHUNK_BYTES)
            .min(READ_CHUNK_BYTES);
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
