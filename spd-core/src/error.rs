//! Typed core errors.
//!
//! One enum per layer, aggregated here. The caller decides what to do based on the
//! variant, so variants never collapse into a generic "internal error".

use crate::safety::limits::LimitsError;
use crate::version::VersionError;

/// Core result alias.
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Anything that can go wrong inside `spd-core`.
///
/// Marked `#[non_exhaustive]`: later phases add variants (transport, protocol, state)
/// and downstream `match`es must keep compiling.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Version or feature negotiation with the peer failed.
    #[error(transparent)]
    Version(#[from] VersionError),

    /// A configured limit is inconsistent or out of range.
    #[error(transparent)]
    Limits(#[from] LimitsError),
}
