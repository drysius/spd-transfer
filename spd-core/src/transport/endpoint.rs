//! Binding, connecting, and the errors either can produce.
//!
//! Both directions end in the same place: a [`Session`] whose handshake already
//! succeeded. The handshake is wrapped in [`Limits::handshake_timeout`], so a peer that
//! opens a connection and then says nothing costs one timeout instead of a stuck task.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use tokio::time::timeout;

use crate::proto::codec::ProtoError;
use crate::proto::messages::{DeviceId, RandomnessError};
use crate::proto::version::{PROTOCOL_VERSION, VersionError};
use crate::safety::limits::{Limits, LimitsError};
use crate::transport::pairing::Authentication;
use crate::transport::session::Session;
use crate::transport::tls::{self, ServerIdentity};

/// A bound listener, waiting for peers.
pub struct Listener {
    endpoint: quinn::Endpoint,
    device: DeviceId,
    auth: Authentication,
    limits: Limits,
}

/// Binds a listener on `address`.
///
/// `auth` applies to every peer this listener accepts: with a code, each of them proves it
/// knows the code before anything is written to disk.
///
/// Must be called from inside a Tokio runtime: the endpoint drives its own I/O task.
///
/// # Errors
/// [`TransportError::Limits`] if the limits are inconsistent, [`TransportError::Tls`] if
/// the identity cannot be generated, [`TransportError::Bind`] if the address is taken.
pub fn listen(
    address: SocketAddr,
    device: DeviceId,
    auth: Authentication,
    limits: Limits,
) -> Result<Listener, TransportError> {
    limits.validate()?;

    let identity = ServerIdentity::self_signed()?;
    let config = tls::server_config(identity, &limits)?;

    let endpoint = quinn::Endpoint::server(config, address)
        .map_err(|source| TransportError::Bind { address, source })?;

    Ok(Listener {
        endpoint,
        device,
        auth,
        limits,
    })
}

impl Listener {
    /// The address actually bound, which is what a caller that passed port 0 needs.
    ///
    /// # Errors
    /// [`TransportError::Bind`] if the socket cannot report its address.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.endpoint
            .local_addr()
            .map_err(|source| TransportError::Bind {
                address: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
                source,
            })
    }

    /// Accepts the next peer and completes the handshake.
    ///
    /// # Errors
    /// [`TransportError::ListenerClosed`] if the endpoint was closed,
    /// [`TransportError::HandshakeTimeout`] if the peer stalls, or whatever the handshake
    /// itself rejected.
    pub async fn accept(&self) -> Result<Session, TransportError> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or(TransportError::ListenerClosed)?;

        let connection = incoming
            .await
            .map_err(|source| TransportError::Connection { source })?;

        timeout(
            self.limits.handshake_timeout,
            Session::establish_as_server(connection, self.device, &self.auth, &self.limits),
        )
        .await
        .map_err(|_elapsed| TransportError::HandshakeTimeout {
            millis: self.limits.handshake_timeout.as_millis(),
        })?
    }

    /// Stops accepting and lets in-flight connections close.
    pub fn close(&self) {
        self.endpoint.close(0_u32.into(), b"listener closed");
    }
}

/// Connects to a listener and completes the handshake.
///
/// # Errors
/// [`TransportError::Limits`] if the limits are inconsistent, [`TransportError::Bind`] if
/// no local socket is available, [`TransportError::Connection`] if the peer is
/// unreachable, [`TransportError::HandshakeTimeout`] if it stalls,
/// [`TransportError::Pairing`] if the code does not match the receiver's.
pub async fn connect(
    address: SocketAddr,
    device: DeviceId,
    auth: &Authentication,
    limits: Limits,
) -> Result<Session, TransportError> {
    connect_announcing(address, device, auth, limits, PROTOCOL_VERSION).await
}

/// [`connect`], with the announced protocol version as a parameter.
///
/// Only the version differs, so there is still one connect path. It exists because the
/// rejection branch has to be reachable from a test: a peer speaking another version is
/// exactly the case that silently rots when nothing exercises it.
pub(crate) async fn connect_announcing(
    address: SocketAddr,
    device: DeviceId,
    auth: &Authentication,
    limits: Limits,
    announced_version: u16,
) -> Result<Session, TransportError> {
    limits.validate()?;

    // Bind on the same address family as the target: a v4 socket cannot reach a v6 peer.
    let local = if address.is_ipv6() {
        SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))
    } else {
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
    };

    let mut endpoint = quinn::Endpoint::client(local).map_err(|source| TransportError::Bind {
        address: local,
        source,
    })?;
    endpoint.set_default_client_config(tls::client_config(&limits)?);

    // The certificate is self-signed, so this name is a label the TLS layer needs rather
    // than something resolved or verified. Identity comes from the pairing proof, which is
    // bound to the session this certificate established.
    let connecting = endpoint
        .connect(address, "spd")
        .map_err(|source| TransportError::Connect { address, source })?;

    let connection = connecting
        .await
        .map_err(|source| TransportError::Connection { source })?;

    timeout(
        limits.handshake_timeout,
        Session::establish_as_client(connection, device, auth, &limits, announced_version),
    )
    .await
    .map_err(|_elapsed| TransportError::HandshakeTimeout {
        millis: limits.handshake_timeout.as_millis(),
    })?
}

/// Why a connection could not be established or kept.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// The local socket could not be bound.
    #[error("could not bind {address}")]
    Bind {
        /// Address that was attempted.
        address: SocketAddr,
        /// Underlying socket error.
        source: std::io::Error,
    },

    /// The peer address was rejected before any packet was sent.
    #[error("could not start a connection to {address}")]
    Connect {
        /// Address that was attempted.
        address: SocketAddr,
        /// What QUIC reported.
        source: quinn::ConnectError,
    },

    /// The connection failed or was lost.
    #[error("connection failed")]
    Connection {
        /// What QUIC reported.
        source: quinn::ConnectionError,
    },

    /// The listener was closed while waiting for a peer.
    #[error("the listener is closed")]
    ListenerClosed,

    /// The peer connected but did not finish the handshake in time.
    #[error("the peer did not complete the handshake within {millis} ms")]
    HandshakeTimeout {
        /// Configured handshake timeout in milliseconds.
        millis: u128,
    },

    /// The peer answered the handshake with a refusal.
    #[error("the peer refused the session ({code:?}): {msg}")]
    PeerRefused {
        /// Machine-readable reason it gave.
        code: crate::proto::messages::ErrorCode,
        /// Its own wording, already meant for a user.
        msg: String,
    },

    /// The peer sent a valid message, but not the one the protocol expects here.
    #[error("expected {expected} from the peer, got {got}")]
    UnexpectedMessage {
        /// What the protocol requires at this point.
        expected: &'static str,
        /// What arrived instead.
        got: &'static str,
    },

    /// Framing or serialisation failed.
    #[error(transparent)]
    Proto(#[from] ProtoError),

    /// Version negotiation failed.
    #[error(transparent)]
    Version(#[from] VersionError),

    /// TLS material could not be prepared.
    #[error(transparent)]
    Tls(#[from] tls::TlsError),

    /// The configured limits are not usable.
    #[error(transparent)]
    Limits(#[from] LimitsError),

    /// A device identifier could not be generated.
    #[error(transparent)]
    Randomness(#[from] RandomnessError),

    /// The peers did not prove they know the same pairing code.
    #[error(transparent)]
    Pairing(#[from] crate::transport::pairing::PairingError),
}

impl TransportError {
    /// Whether reconnecting could plausibly succeed.
    ///
    /// The connection dropping is worth another attempt. Everything a peer *decided* is
    /// not: a wrong pairing code is wrong every time, and retrying it five times only
    /// turns one clear refusal into five and hands an attacker four more guesses.
    pub const fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::Connection { .. }
                | Self::HandshakeTimeout { .. }
                | Self::Proto(ProtoError::PeerClosed | ProtoError::Io(_))
        )
    }

    /// The machine-readable code to put on the wire when refusing a peer over this.
    ///
    /// Only the reasons a peer is actually refused with need a distinct code; everything
    /// else never reaches the wire, and saying `Internal` about it is honest.
    pub(crate) const fn code(&self) -> crate::proto::messages::ErrorCode {
        use crate::proto::messages::ErrorCode;

        match self {
            Self::Pairing(_) => ErrorCode::Unauthorized,
            Self::Version(_) | Self::UnexpectedMessage { .. } => ErrorCode::ProtocolViolation,
            Self::Limits(_) => ErrorCode::LimitExceeded,
            _ => ErrorCode::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
    }

    /// A peer announcing a version this build does not speak is refused by the listener,
    /// and the refusal reaches the connecting side as its own typed error rather than as
    /// a dropped connection.
    #[tokio::test]
    async fn an_incompatible_version_is_refused_on_both_sides() {
        let listener = listen(
            loopback(),
            DeviceId::random().unwrap(),
            Authentication::Insecure,
            Limits::DEFAULT,
        )
        .unwrap();
        let address = listener.local_addr().unwrap();

        let accepting = tokio::spawn(async move { listener.accept().await.map(|_| ()) });

        let refused = connect_announcing(
            address,
            DeviceId::random().unwrap(),
            &Authentication::Insecure,
            Limits::DEFAULT,
            PROTOCOL_VERSION + 1,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(refused, TransportError::PeerRefused { .. }),
            "the connecting side should learn why, got {refused:?}"
        );

        let listener_side = accepting.await.unwrap().unwrap_err();
        assert!(
            matches!(
                listener_side,
                TransportError::Version(VersionError::Mismatch { .. })
            ),
            "the listener should reject by version, got {listener_side:?}"
        );
    }

    #[tokio::test]
    async fn connecting_to_nothing_fails_fast_and_says_where() {
        let unused = SocketAddr::from((Ipv4Addr::LOCALHOST, 1));
        let mut limits = Limits::DEFAULT;
        limits.handshake_timeout = std::time::Duration::from_millis(300);
        limits.idle_timeout = std::time::Duration::from_millis(300);

        let error = connect(
            unused,
            DeviceId::random().unwrap(),
            &Authentication::Insecure,
            limits,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            TransportError::Connection { .. } | TransportError::HandshakeTimeout { .. }
        ));
    }
}
