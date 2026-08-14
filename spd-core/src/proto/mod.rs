//! Wire protocol: message types, framing and version negotiation.
//!
//! Everything a peer can put on the wire is defined here, and nowhere else. Two shapes
//! exist: the control stream, which carries [`messages::Control`] frames both ways for
//! the whole session, and the data streams, each opened with a
//! [`messages::DataHeader`] followed by raw file bytes.

pub mod codec;
pub mod messages;
pub mod version;

pub use codec::{ControlChannel, ProtoError};
pub use messages::{Control, DataHeader, Decision, DeviceId, Entry, ErrorCode, FileId};
pub use version::{Features, Negotiated, PROTOCOL_VERSION, VersionError, negotiate};
