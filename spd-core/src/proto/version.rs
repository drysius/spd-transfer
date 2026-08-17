//! Wire protocol version and feature negotiation.
//!
//! Both sides announce a version and a feature bitset in `Hello`/`HelloAck`. The version
//! must match exactly; features are intersected, so a peer only ever uses a capability
//! both sides implement. Unknown bits are dropped on decode rather than rejected — that
//! is what lets a newer peer talk to an older one once versions become compatible.

use crate::safety::path::NamePolicy;

/// Wire protocol version this build speaks.
///
/// Bump on any incompatible change to the control stream or data headers. The golden
/// wire-format fixtures (F9) exist so an accidental bump fails a test.
pub const PROTOCOL_VERSION: u16 = 1;

/// Optional capabilities a peer may support, as a bitset.
///
/// A feature bit means "I implement this", not "use this". The effective set for a
/// session is the intersection of both sides, computed by [`negotiate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Features(u64);

impl Features {
    /// No optional capability.
    pub const NONE: Self = Self(0);

    /// Per-file zstd streaming compression (`DataHeader.compressed`).
    pub const ZSTD: Self = Self(1 << 0);

    /// Resume from a byte offset on reconnect.
    pub const RESUME: Self = Self(1 << 1);

    /// `(path, size, mtime)` hash cache, letting the manifest carry precomputed hashes.
    pub const HASH_CACHE: Self = Self(1 << 2);

    /// This side will prove it knows a pairing code before anything is transferred.
    ///
    /// Unlike the others, this bit is not announced by every build that implements it: it
    /// says what this *session* is doing, so it appears only when the side sending it was
    /// actually given a code. The intersection is then exactly "both of us are pairing".
    pub const PAIRING: Self = Self(1 << 3);

    /// This side can hold names Windows forbids: `<>:"|?*`, a trailing dot or space, and
    /// the reserved device names.
    ///
    /// Conditional like [`Self::PAIRING`], and for a stronger reason: the intersection is
    /// "both filesystems can hold such a name", which is exactly the question the sender
    /// has to answer before it offers one. A Windows peer never announces it, so a name it
    /// could not write is never sent to it.
    pub const POSIX_NAMES: Self = Self(1 << 4);

    /// Every bit this build knows how to read.
    pub const SUPPORTED: Self = Self(
        Self::ZSTD.0 | Self::RESUME.0 | Self::HASH_CACHE.0 | Self::PAIRING.0 | Self::POSIX_NAMES.0,
    );

    /// Everything this build implements and offers unconditionally.
    pub const ALWAYS: Self = Self(Self::ZSTD.0 | Self::RESUME.0 | Self::HASH_CACHE.0);

    /// What this side announces, given whether it was handed a pairing code and which
    /// names its own configuration allows.
    #[must_use]
    pub const fn announced(pairing: bool, names: NamePolicy) -> Self {
        let mut bits = Self::ALWAYS.0;

        if pairing {
            bits |= Self::PAIRING.0;
        }

        if !names.rejects_windows_traps() {
            bits |= Self::POSIX_NAMES.0;
        }

        Self(bits)
    }

    /// Raw bits, for putting on the wire.
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Reads a peer-supplied bitset, discarding bits this build does not know.
    ///
    /// Dropping unknown bits is deliberate: a future peer announcing extra capabilities
    /// must not be rejected, and must not have them silently assumed either.
    pub const fn from_bits_truncate(bits: u64) -> Self {
        Self(bits & Self::SUPPORTED.0)
    }

    /// Whether every bit of `other` is present in `self`.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Bits present on both sides.
    #[must_use]
    pub const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    /// Whether no capability is set.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// What both sides agreed on for this session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Negotiated {
    /// Protocol version in use — always [`PROTOCOL_VERSION`] while v1 requires an exact
    /// match.
    pub version: u16,
    /// Capabilities both sides implement.
    pub features: Features,
}

/// Negotiates the session against a peer's `Hello`.
///
/// v1 requires an exact version match: there is no older peer to be compatible with, and
/// pretending otherwise would mean untested downgrade paths. Features are intersected with
/// what this side announced, which for pairing is what makes the result mean "both of us".
///
/// # Errors
/// [`VersionError::Mismatch`] if the peer speaks a different protocol version.
pub fn negotiate(
    peer_version: u16,
    peer_features: u64,
    announced: Features,
) -> Result<Negotiated, VersionError> {
    if peer_version != PROTOCOL_VERSION {
        return Err(VersionError::Mismatch {
            ours: PROTOCOL_VERSION,
            peer: peer_version,
        });
    }

    let features = announced.intersection(Features::from_bits_truncate(peer_features));
    tracing::debug!(
        version = peer_version,
        features = features.bits(),
        "negotiated session"
    );

    Ok(Negotiated {
        version: peer_version,
        features,
    })
}

/// Why negotiation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum VersionError {
    /// The peer speaks a protocol version this build does not implement.
    #[error(
        "peer protocol version {peer} is incompatible with ours ({ours}); upgrade the older side"
    )]
    Mismatch {
        /// Version this build speaks.
        ours: u16,
        /// Version the peer announced.
        peer: u16,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiation_keeps_only_shared_features() {
        let peer = Features::ZSTD.bits();
        let agreed = negotiate(
            PROTOCOL_VERSION,
            peer,
            Features::announced(false, NamePolicy::Portable),
        )
        .unwrap();

        assert!(agreed.features.contains(Features::ZSTD));
        assert!(!agreed.features.contains(Features::RESUME));
    }

    #[test]
    fn unknown_feature_bits_are_dropped_not_assumed() {
        let peer_from_the_future = u64::MAX;
        let agreed =
            negotiate(PROTOCOL_VERSION, peer_from_the_future, Features::SUPPORTED).unwrap();

        assert_eq!(agreed.features, Features::SUPPORTED);
        assert_eq!(agreed.features.bits() & !Features::SUPPORTED.bits(), 0);
    }

    #[test]
    fn featureless_peer_negotiates_an_empty_set() {
        let agreed = negotiate(
            PROTOCOL_VERSION,
            Features::NONE.bits(),
            Features::announced(false, NamePolicy::Portable),
        )
        .unwrap();
        assert!(agreed.features.is_empty());
    }

    #[test]
    fn pairing_is_agreed_only_when_both_sides_were_given_a_code() {
        let both = negotiate(
            PROTOCOL_VERSION,
            Features::announced(true, NamePolicy::Portable).bits(),
            Features::announced(true, NamePolicy::Portable),
        )
        .unwrap();
        assert!(both.features.contains(Features::PAIRING));

        let only_them = negotiate(
            PROTOCOL_VERSION,
            Features::announced(true, NamePolicy::Portable).bits(),
            Features::announced(false, NamePolicy::Portable),
        )
        .unwrap();
        assert!(!only_them.features.contains(Features::PAIRING));

        let only_us = negotiate(
            PROTOCOL_VERSION,
            Features::announced(false, NamePolicy::Portable).bits(),
            Features::announced(true, NamePolicy::Portable),
        )
        .unwrap();
        assert!(!only_us.features.contains(Features::PAIRING));
    }

    /// The bit has to mean "both of us", not "one of us wants to": a sender that offered a
    /// name on its own say-so would hand a Windows receiver a file it cannot create.
    #[test]
    fn unix_only_names_are_agreed_only_when_both_filesystems_hold_them() {
        let both = negotiate(
            PROTOCOL_VERSION,
            Features::announced(false, NamePolicy::Posix).bits(),
            Features::announced(false, NamePolicy::Posix),
        )
        .unwrap();
        assert!(both.features.contains(Features::POSIX_NAMES));

        let only_the_sender = negotiate(
            PROTOCOL_VERSION,
            Features::announced(false, NamePolicy::Portable).bits(),
            Features::announced(false, NamePolicy::Posix),
        )
        .unwrap();
        assert!(!only_the_sender.features.contains(Features::POSIX_NAMES));

        let only_the_receiver = negotiate(
            PROTOCOL_VERSION,
            Features::announced(false, NamePolicy::Posix).bits(),
            Features::announced(false, NamePolicy::Portable),
        )
        .unwrap();
        assert!(!only_the_receiver.features.contains(Features::POSIX_NAMES));
    }

    #[test]
    fn differing_version_is_a_typed_error() {
        let err = negotiate(PROTOCOL_VERSION + 1, 0, Features::SUPPORTED).unwrap_err();
        assert_eq!(
            err,
            VersionError::Mismatch {
                ours: PROTOCOL_VERSION,
                peer: PROTOCOL_VERSION + 1,
            }
        );
    }
}
