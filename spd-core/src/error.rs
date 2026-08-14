//! Typed core errors.
//!
//! One enum per layer, aggregated here. The caller decides what to do based on the
//! variant, so variants never collapse into a generic "internal error".

use crate::proto::codec::ProtoError;
use crate::proto::version::VersionError;
use crate::safety::limits::LimitsError;
use crate::transport::endpoint::TransportError;

/// Core result alias.
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Anything that can go wrong inside `spd-core`.
///
/// Marked `#[non_exhaustive]`: later phases add variants (state, scan) and downstream
/// `match`es must keep compiling.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Version or feature negotiation with the peer failed.
    #[error(transparent)]
    Version(#[from] VersionError),

    /// A configured limit is inconsistent or out of range.
    #[error(transparent)]
    Limits(#[from] LimitsError),

    /// Framing or serialisation on the control stream failed.
    #[error(transparent)]
    Proto(#[from] ProtoError),

    /// The connection could not be established or was lost.
    #[error(transparent)]
    Transport(#[from] TransportError),
}
