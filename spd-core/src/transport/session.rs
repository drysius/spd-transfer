//! A live session: one QUIC connection, one control stream, N data streams.
//!
//! A `Session` only exists after a successful handshake, so holding one is proof that
//! version and features were negotiated. Nothing downstream has to re-check that, and no
//! code path can accidentally use a connection that never agreed on anything.

use core::fmt;
use std::net::SocketAddr;

use quinn::{Connection, RecvStream, SendStream};
use tokio::time::timeout;

use crate::proto::codec::ControlChannel;
use crate::proto::messages::{Control, DeviceId, ErrorCode};
use crate::proto::version::{Features, Negotiated, PROTOCOL_VERSION, negotiate};
use crate::safety::limits::Limits;
use crate::transport::endpoint::TransportError;

/// Who is on the other end, and what was agreed with them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerInfo {
    /// The peer's device identifier.
    pub device: DeviceId,
    /// Version and features in force for this session.
    pub negotiated: Negotiated,
    /// Where the peer is, for logs and error messages.
    pub address: SocketAddr,
}

/// An established session.
pub struct Session {
    connection: Connection,
    control: ControlChannel,
    peer: PeerInfo,
}

/// Shows who the session is with. The stream machinery underneath has no readable
/// representation and printing it would bury the one fact a log line needs.
impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

impl Session {
    /// Performs the connecting side of the handshake.
    ///
    /// The connecting side speaks first: it opens the control stream, sends `Hello` and
    /// waits for `HelloAck`.
    ///
    /// # Errors
    /// [`TransportError::Connection`] if the stream cannot be opened,
    /// [`TransportError::Proto`] on a framing failure, [`TransportError::Version`] if the
    /// peer speaks another protocol version, [`TransportError::UnexpectedMessage`] if it
    /// answers with something other than `HelloAck`.
    pub(crate) async fn establish_as_client(
        connection: Connection,
        device: DeviceId,
        limits: &Limits,
        announced_version: u16,
    ) -> Result<Self, TransportError> {
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|source| TransportError::Connection { source })?;

        let mut control = ControlChannel::new(send, recv, limits);
        control.send(&hello(device, announced_version)).await?;

        match control.recv().await? {
            Control::HelloAck {
                version,
                features,
                device: peer_device,
            } => {
                let agreed = negotiate(version, features)?;
                Ok(Self::assemble(connection, control, agreed, peer_device))
            }
            // The peer refused us and said why; carry its wording out instead of
            // reporting the connection drop that follows.
            Control::Error { code, msg } => Err(TransportError::PeerRefused { code, msg }),
            other => Err(TransportError::UnexpectedMessage {
                expected: "HelloAck",
                got: other.kind(),
            }),
        }
    }

    /// Performs the accepting side of the handshake.
    ///
    /// # Errors
    /// Same as [`Self::establish_as_client`], with `Hello` as the expected message.
    pub(crate) async fn establish_as_server(
        connection: Connection,
        device: DeviceId,
        limits: &Limits,
    ) -> Result<Self, TransportError> {
        let (send, recv) = connection
            .accept_bi()
            .await
            .map_err(|source| TransportError::Connection { source })?;

        let mut control = ControlChannel::new(send, recv, limits);

        let opening = control.recv().await?;
        let Control::Hello {
            version,
            features,
            device: peer_device,
        } = opening
        else {
            return Err(TransportError::UnexpectedMessage {
                expected: "Hello",
                got: opening.kind(),
            });
        };

        // Negotiate before answering: a peer on another protocol version gets a typed
        // rejection instead of an ack it would misread.
        let agreed = match negotiate(version, features) {
            Ok(agreed) => agreed,
            Err(refusal) => {
                // Best effort courtesy: tell the peer why, so it can print something
                // better than a dropped connection. The refusal itself is what
                // propagates, so a failure to deliver it changes nothing here.
                if let Err(undeliverable) = control
                    .send(&Control::Error {
                        code: ErrorCode::ProtocolViolation,
                        msg: refusal.to_string(),
                    })
                    .await
                {
                    tracing::debug!(%undeliverable, "could not tell the peer why it was refused");
                }
                linger_until_peer_leaves(&connection, &mut control, limits).await;
                return Err(refusal.into());
            }
        };

        control
            .send(&Control::HelloAck {
                version: PROTOCOL_VERSION,
                features: Features::SUPPORTED.bits(),
                device,
            })
            .await?;

        Ok(Self::assemble(connection, control, agreed, peer_device))
    }

    /// Who this session is with.
    pub const fn peer(&self) -> &PeerInfo {
        &self.peer
    }

    /// The control stream, for the phase driving the transfer.
    pub const fn control(&mut self) -> &mut ControlChannel {
        &mut self.control
    }

    /// Opens a unidirectional stream to send one file.
    ///
    /// # Errors
    /// [`TransportError::Connection`] if the peer's stream limit is reached or the
    /// connection is gone.
    pub async fn open_data_stream(&self) -> Result<SendStream, TransportError> {
        self.connection
            .open_uni()
            .await
            .map_err(|source| TransportError::Connection { source })
    }

    /// Waits for the peer to open the next data stream.
    ///
    /// # Errors
    /// [`TransportError::Connection`] if the connection ends first.
    pub async fn accept_data_stream(&self) -> Result<RecvStream, TransportError> {
        self.connection
            .accept_uni()
            .await
            .map_err(|source| TransportError::Connection { source })
    }

    /// Closes the connection, telling the peer why.
    ///
    /// The reason travels in the QUIC close frame, so the other side can print something
    /// better than "connection reset".
    pub fn close(&self, reason: &str) {
        self.connection.close(0_u32.into(), reason.as_bytes());
    }

    fn assemble(
        connection: Connection,
        control: ControlChannel,
        negotiated: Negotiated,
        device: DeviceId,
    ) -> Self {
        let address = connection.remote_address();
        tracing::info!(%device, %address, version = negotiated.version, "session established");

        Self {
            connection,
            control,
            peer: PeerInfo {
                device,
                negotiated,
                address,
            },
        }
    }
}

/// Keeps a refused connection alive just long enough for the peer to read the refusal.
///
/// Returning immediately would drop the connection and take the explanation with it - QUIC
/// discards buffered stream data on close - leaving the peer with "connection lost" and no
/// reason. Either outcome of the wait is fine: the peer read it and left, or it stopped
/// listening; the refusal is already decided either way.
async fn linger_until_peer_leaves(
    connection: &Connection,
    control: &mut ControlChannel,
    limits: &Limits,
) {
    if let Err(unflushed) = control.close().await {
        tracing::debug!(%unflushed, "could not flush the refusal to the peer");
    }

    let linger = limits.handshake_timeout / 4;
    if timeout(linger, connection.closed()).await.is_err() {
        tracing::debug!(
            ?linger,
            "the refused peer did not close in time; dropping it"
        );
    }
}

/// The opening frame.
///
/// `version` is a parameter rather than a constant so a test can announce a version this
/// build does not speak and watch the peer reject it - the rejection path is the one that
/// must never rot, and it cannot be exercised from outside otherwise.
fn hello(device: DeviceId, version: u16) -> Control {
    Control::Hello {
        version,
        features: Features::SUPPORTED.bits(),
        device,
    }
}
