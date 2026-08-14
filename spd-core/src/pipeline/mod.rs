//! Per-file transfer pipeline: disk -> CPU -> network, and back.
//!
//! [`budget`] decides how wide the pipeline may run, [`send`] drives the sending side and
//! [`recv`] the receiving side. Both agree on one rule: a file is one stream, and what is
//! on disk is always a valid prefix of it.

pub mod budget;
pub mod bufpool;
pub mod control;
mod prefix;
pub mod recv;
pub mod retry;
pub mod send;

use std::path::PathBuf;

use crate::proto::codec::ProtoError;
use crate::proto::messages::FileId;
use crate::safety::path::PathError;
use crate::state::journal::JournalError;
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

    /// A data stream failed part way through a file.
    ///
    /// Separate from [`Self::Io`] on purpose: a lost connection is worth another attempt,
    /// a failing disk is not, and one variant covering both would make that undecidable.
    #[error("the connection failed while {operation} {path}")]
    Stream {
        /// What was being attempted, in words a user recognises.
        operation: &'static str,
        /// The file involved.
        path: PathBuf,
        /// What the transport reported.
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

    /// A stream started somewhere other than where this side asked it to.
    ///
    /// Only the receiver decides where a file resumes; a sender starting elsewhere would
    /// leave a hole in the middle of the file, which no hash could then explain.
    #[error("the peer started {path} at byte {got}, not at the agreed byte {agreed}")]
    ResumeOffset {
        /// The file involved.
        path: PathBuf,
        /// Where the stream claims to start.
        got: u64,
        /// Where it was asked to start.
        agreed: u64,
    },

    /// A file on disk is shorter than the offset the transfer agreed to resume from.
    ///
    /// It changed between being measured and being read - the source was rewritten, or a
    /// second process is writing into the same destination.
    #[error("{path} ends at byte {ends_at}, before byte {needed} where the transfer resumes")]
    ResumeUnavailable {
        /// The file involved.
        path: PathBuf,
        /// Where it actually ends.
        ends_at: u64,
        /// Where the transfer expected to continue from.
        needed: u64,
    },

    /// A path was refused.
    #[error(transparent)]
    Path(#[from] PathError),

    /// The record of what is part way through could not be read or written.
    #[error(transparent)]
    State(#[from] JournalError),

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

impl PipelineError {
    /// Whether trying again on a new connection could succeed.
    ///
    /// True for anything that is the connection's fault: what is already on disk stays a
    /// valid prefix, so a second attempt resumes rather than starting over. False for
    /// everything a retry would only repeat - a refused path, a failing disk, a peer
    /// speaking nonsense, or bytes that arrived and did not match.
    pub const fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::Stream { .. }
                | Self::Transport(_)
                | Self::Proto(ProtoError::PeerClosed | ProtoError::Io(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lost_connection_is_worth_another_attempt() {
        let lost = PipelineError::Stream {
            operation: "receiving",
            path: PathBuf::from("photo.jpg"),
            source: std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        };

        assert!(lost.is_recoverable());
        assert!(PipelineError::Proto(ProtoError::PeerClosed).is_recoverable());
    }

    #[test]
    fn a_failing_disk_or_a_wrong_hash_is_not() {
        let disk = PipelineError::Io {
            operation: "writing",
            path: PathBuf::from("photo.jpg"),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        let mismatch = PipelineError::HashMismatch {
            path: PathBuf::from("photo.jpg"),
        };

        assert!(!disk.is_recoverable());
        assert!(!mismatch.is_recoverable());
    }
}
