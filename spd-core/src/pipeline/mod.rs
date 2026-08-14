//! Per-file transfer pipeline: disk -> CPU -> network, and back.
//!
//! [`budget`] decides how wide the pipeline may run, [`send`] drives the sending side and
//! [`recv`] the receiving side. Both agree on one rule: a file is one stream, and what is
//! on disk is always a valid prefix of it.

pub mod budget;
pub mod bufpool;
pub mod control;
pub mod recv;
pub mod send;

use std::path::PathBuf;

use crate::proto::codec::ProtoError;
use crate::proto::messages::FileId;
use crate::safety::path::PathError;
use crate::transport::endpoint::TransportError;

/// What a transfer moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransferSummary {
    /// Files actually transferred; skipped files are not counted.
    pub files: u64,
    /// File bytes moved, before any framing.
    pub bytes: u64,
}

/// Why a transfer stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PipelineError {
    /// A local file operation failed. The path is part of the error because "permission
    /// denied" without a name is not actionable.
    #[error("{operation} failed on {path}")]
    Io {
        /// What was being attempted, in words a user recognises.
        operation: &'static str,
        /// The file or directory involved.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },

    /// The received bytes do not hash to what the sender said they would.
    #[error(
        "{path} does not match the sender's hash; the partial file was removed and nothing \
         replaced the original"
    )]
    HashMismatch {
        /// The file that failed verification.
        path: PathBuf,
    },

    /// A data stream referenced a file that was never negotiated.
    #[error("the peer opened a stream for {file_id}, which was not in the agreed manifest")]
    UnknownFile {
        /// The id it claimed.
        file_id: FileId,
    },

    /// The peer sent a valid message, but not the one the protocol expects here.
    #[error("expected {expected} from the peer, got {got}")]
    UnexpectedMessage {
        /// What the protocol requires at this point.
        expected: &'static str,
        /// What arrived instead.
        got: &'static str,
    },

    /// The peer reported a failure of its own.
    #[error("the peer stopped the transfer: {msg}")]
    PeerFailed {
        /// Its wording, already meant for a user.
        msg: String,
    },

    /// A file exceeds the configured ceiling.
    #[error("{path} is {size} B, above the {max} B limit (max_file_size_bytes)")]
    FileTooLarge {
        /// The file involved.
        path: PathBuf,
        /// Its size.
        size: u64,
        /// Configured ceiling.
        max: u64,
    },

    /// A path was refused.
    #[error(transparent)]
    Path(#[from] PathError),

    /// The local tree could not be listed.
    #[error(transparent)]
    Scan(#[from] crate::scan::manifest::ScanError),

    /// Framing or serialisation failed.
    #[error(transparent)]
    Proto(#[from] ProtoError),

    /// The connection failed.
    #[error(transparent)]
    Transport(#[from] TransportError),
}
