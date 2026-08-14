//! Every bound a peer can push against, in one place.
//!
//! Nothing outside this module invents a limit. A constant sitting in the middle of the
//! transfer path is a bug with a date on it: it cannot be configured, cannot be tested at
//! its edge, and nobody finds it during review.

use core::time::Duration;

/// Smallest control frame that still fits a batched manifest message.
///
/// Below this the protocol cannot make progress, so a smaller value is a configuration
/// error rather than a stricter policy.
const MIN_FRAME_LEN_BYTES: usize = 64 * 1024;

/// All configurable bounds for a session.
///
/// Defaults are conservative and hold on a hostile peer: values are bounded *before* any
/// allocation, so a peer cannot make this process reserve memory by announcing a size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest control-stream frame accepted, in bytes. Caps every allocation whose size
    /// comes from the peer.
    pub max_frame_len_bytes: usize,

    /// Largest number of entries in a single manifest message. The manifest is always
    /// batched, regardless of folder size.
    pub max_manifest_entries: usize,

    /// Largest number of path components accepted from a peer.
    pub max_path_depth: usize,

    /// Largest relative path accepted from a peer, in bytes.
    pub max_path_len_bytes: usize,

    /// Largest number of QUIC streams open at once.
    pub max_concurrent_streams: u32,

    /// How long the handshake may take before the connection is dropped.
    pub handshake_timeout: Duration,

    /// How long a session may sit idle before the connection is dropped.
    pub idle_timeout: Duration,

    /// Largest single file accepted, in bytes.
    pub max_file_size_bytes: u64,
}

impl Limits {
    /// Defaults used when nothing is configured.
    pub const DEFAULT: Self = Self {
        max_frame_len_bytes: 4 * 1024 * 1024,
        max_manifest_entries: 2_000,
        max_path_depth: 64,
        max_path_len_bytes: 4_096,
        max_concurrent_streams: 16,
        handshake_timeout: Duration::from_secs(10),
        idle_timeout: Duration::from_secs(60),
        max_file_size_bytes: 1 << 42, // 4 TiB
    };

    /// Checks that this set of limits is internally consistent.
    ///
    /// Call once at startup, after applying CLI overrides and before opening a socket: a
    /// limit only protects anything if it is checked before the first allocation.
    ///
    /// # Errors
    /// [`LimitsError::Zero`] if a field that must be positive is zero.
    /// [`LimitsError::FrameTooSmall`] if `max_frame_len_bytes` cannot hold a manifest
    /// batch.
    pub fn validate(&self) -> Result<(), LimitsError> {
        // Widened to u128 so `Duration::as_millis` joins the list without a lossy cast.
        let positive = [
            ("max_manifest_entries", self.max_manifest_entries as u128),
            ("max_path_depth", self.max_path_depth as u128),
            ("max_path_len_bytes", self.max_path_len_bytes as u128),
            (
                "max_concurrent_streams",
                u128::from(self.max_concurrent_streams),
            ),
            ("max_file_size_bytes", u128::from(self.max_file_size_bytes)),
            ("handshake_timeout", self.handshake_timeout.as_millis()),
            ("idle_timeout", self.idle_timeout.as_millis()),
        ];

        if let Some((field, _)) = positive.iter().find(|(_, value)| *value == 0) {
            return Err(LimitsError::Zero { field });
        }

        if self.max_frame_len_bytes < MIN_FRAME_LEN_BYTES {
            return Err(LimitsError::FrameTooSmall {
                got: self.max_frame_len_bytes,
                min: MIN_FRAME_LEN_BYTES,
            });
        }

        Ok(())
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Why a set of limits was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LimitsError {
    /// A field that must be positive was configured as zero.
    #[error("{field} must be greater than zero")]
    Zero {
        /// Name of the offending field, as the user typed it on the command line.
        field: &'static str,
    },

    /// The frame limit is too small for the protocol to make progress.
    #[error(
        "max_frame_len_bytes = {got} B is below the {min} B minimum needed for one manifest batch"
    )]
    FrameTooSmall {
        /// Configured value.
        got: usize,
        /// Smallest workable value.
        min: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        assert!(Limits::DEFAULT.validate().is_ok());
    }

    #[test]
    fn zero_field_names_itself() {
        let limits = Limits {
            max_path_depth: 0,
            ..Limits::DEFAULT
        };

        assert_eq!(
            limits.validate().unwrap_err(),
            LimitsError::Zero {
                field: "max_path_depth"
            }
        );
    }

    #[test]
    fn frame_below_a_manifest_batch_is_rejected() {
        let limits = Limits {
            max_frame_len_bytes: 1_024,
            ..Limits::DEFAULT
        };

        assert_eq!(
            limits.validate().unwrap_err(),
            LimitsError::FrameTooSmall {
                got: 1_024,
                min: MIN_FRAME_LEN_BYTES,
            }
        );
    }

    #[test]
    fn zero_timeout_is_rejected_not_treated_as_infinite() {
        let limits = Limits {
            idle_timeout: Duration::ZERO,
            ..Limits::DEFAULT
        };

        assert_eq!(
            limits.validate().unwrap_err(),
            LimitsError::Zero {
                field: "idle_timeout"
            }
        );
    }
}
