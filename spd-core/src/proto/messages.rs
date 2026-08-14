//! Everything a peer can put on the wire.
//!
//! Two shapes: [`Control`] frames on the long-lived control stream, and a [`DataHeader`]
//! at the head of each unidirectional data stream, followed by raw file bytes.
//!
//! Ids are newtypes rather than bare `u64` so a file id and a byte offset cannot be
//! swapped at a call site - both would compile as `u64`, and only one of the two mistakes
//! is caught by a test.

use core::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a file within one session.
///
/// Assigned by the sender when it builds the manifest; meaningless outside that session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FileId(pub u64);

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Stable identifier of a peer installation.
///
/// Random per process in this phase. F7 persists it so a paired peer stays recognisable
/// across restarts, which is what turns it into an authentication input rather than a
/// label.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceId([u8; 32]);

impl DeviceId {
    /// Draws a fresh identifier from the operating system's randomness.
    ///
    /// # Errors
    /// [`RandomnessError`] if the OS refuses to provide entropy, which on a healthy
    /// system does not happen - it is reported rather than ignored because a predictable
    /// device id would later weaken pairing.
    pub fn random() -> Result<Self, RandomnessError> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).map_err(|source| RandomnessError {
            code: source.to_string(),
        })?;
        Ok(Self(bytes))
    }

    /// Raw bytes, for hashing or persistence.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Builds an identifier from bytes that were already generated or stored.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Shown as the first 8 hex characters: enough to tell two peers apart in a log line,
/// short enough for a human to compare on screen.
impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0[..4] {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeviceId({self})")
    }
}

/// The OS declined to provide randomness.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the operating system refused to provide randomness: {code}")]
pub struct RandomnessError {
    /// What the OS reported.
    pub code: String,
}

/// A frame on the control stream.
///
/// Both sides send and receive these for the whole session. Order matters only where the
/// protocol says so: `Hello` first, `Done` last.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Control {
    /// Opening frame, sent by the connecting side.
    Hello {
        /// Protocol version the sender speaks.
        version: u16,
        /// Capability bits the sender implements.
        features: u64,
        /// Who is calling.
        device: DeviceId,
    },

    /// Answer to [`Control::Hello`], sent by the accepting side.
    HelloAck {
        /// Protocol version the responder speaks.
        version: u16,
        /// Capability bits the responder implements.
        features: u64,
        /// Who answered.
        device: DeviceId,
    },

    /// One batch of the sender's file list. Always batched, regardless of folder size.
    Manifest {
        /// Batch number, starting at zero.
        batch_seq: u32,
        /// Whether this is the final batch.
        last: bool,
        /// Files in this batch.
        entries: Vec<Entry>,
    },

    /// The receiver's answer to one manifest batch, in the same order as its entries.
    SyncReply {
        /// Batch this answers.
        batch_seq: u32,
        /// One decision per entry of that batch.
        decisions: Vec<Decision>,
    },

    /// Sender: this file is fully written to its stream, and this is its hash.
    FileDone {
        /// Which file.
        file_id: FileId,
        /// BLAKE3 of the whole file, in file order.
        hash: [u8; 32],
    },

    /// Receiver: the file it wrote either matches that hash or does not.
    FileVerdict {
        /// Which file.
        file_id: FileId,
        /// Whether the hash matched.
        ok: bool,
    },

    /// Sender: everything is transferred.
    Done {
        /// Files sent, skipped ones excluded.
        files: u64,
        /// Bytes sent on the wire.
        bytes: u64,
    },

    /// Either side: this session is over because of an error.
    Error {
        /// Machine-readable reason.
        code: ErrorCode,
        /// Human-readable detail, already worded for a user.
        msg: String,
    },
}

impl Control {
    /// Variant name, for logs and for saying what arrived when something else was
    /// expected.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Hello { .. } => "Hello",
            Self::HelloAck { .. } => "HelloAck",
            Self::Manifest { .. } => "Manifest",
            Self::SyncReply { .. } => "SyncReply",
            Self::FileDone { .. } => "FileDone",
            Self::FileVerdict { .. } => "FileVerdict",
            Self::Done { .. } => "Done",
            Self::Error { .. } => "Error",
        }
    }
}

/// One file offered by the sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Sender-assigned id, referenced by every later message about this file.
    pub file_id: FileId,

    /// Path components relative to the transfer root - never a separator-joined string,
    /// so the receiver never has to guess which separator the sender's platform used.
    pub path: Vec<String>,

    /// File size in bytes.
    pub size: u64,

    /// Modification time, seconds since the Unix epoch.
    pub mtime: u64,

    /// Permission bits that survive the crossing; platform-dependent in meaning.
    pub mode: u32,

    /// BLAKE3 of the file, when the sender already knew it. Absent for large files and
    /// cache misses, where hashing every send would cost more than it saves.
    pub hash: Option<[u8; 32]>,
}

/// What the receiver wants done with one manifest entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    /// Already present and identical; send nothing.
    Skip,

    /// Send it, starting at this offset.
    Need {
        /// Which file.
        file_id: FileId,
        /// First byte the receiver is missing. Zero for a file it does not have.
        from_offset: u64,
    },
}

/// Head of a unidirectional data stream, followed by the file body.
///
/// End of stream marks end of file, so there is no length field to disagree with reality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataHeader {
    /// Which file this stream carries; must be a `Need` from the negotiated manifest.
    pub file_id: FileId,

    /// Offset in the file where this stream's bytes start.
    pub offset: u64,

    /// Whether the body is zstd-compressed. The receiver obeys this flag and never its
    /// own configuration - the sender decided per file, and only it knows what it did.
    pub compressed: bool,
}

/// Machine-readable reason a session ended badly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    /// A data stream referenced a file that was never negotiated.
    UnknownFile,
    /// A configured limit was exceeded.
    LimitExceeded,
    /// The peer broke the protocol - wrong message at the wrong time.
    ProtocolViolation,
    /// Local filesystem failure.
    Io,
    /// The peer is not authenticated for what it asked.
    Unauthorized,
    /// A bug on the reporting side.
    Internal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_ids_are_distinct_and_print_short() {
        let first = DeviceId::random().unwrap();
        let second = DeviceId::random().unwrap();

        assert_ne!(first, second);
        assert_eq!(first.to_string().len(), 8);
    }

    #[test]
    fn device_id_bytes_round_trip() {
        let bytes = [7_u8; 32];
        assert_eq!(DeviceId::from_bytes(bytes).as_bytes(), &bytes);
    }

    #[test]
    fn file_id_prints_for_logs() {
        assert_eq!(FileId(42).to_string(), "#42");
    }
}
