//! A live session: one QUIC connection, one control stream, N data streams.
//!
//! A `Session` only exists after a successful handshake, so holding one is proof that
//! version and features were negotiated. Nothing downstream has to re-check that, and no
//! code path can accidentally use a connection that never agreed on anything.

use core::fmt;
use std::net::SocketAddr;

use quinn::{Connection, RecvStream, SendStream};
use tokio::time::timeout;

use crate::proto::codec::{ControlChannel, ControlReader, ControlWriter};
use crate::proto::messages::{Control, DeviceId};
use crate::proto::version::{Features, Negotiated, PROTOCOL_VERSION, negotiate};
use crate::safety::limits::Limits;
use crate::transport::endpoint::TransportError;
use crate::transport::pairing::{
    Authentication, BINDING_BYTES, BINDING_LABEL, PairingError, Proof, Side,
};

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
    limits: Limits,
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
        auth: &Authentication,
        limits: &Limits,
        announced_version: u16,
    ) -> Result<Self, TransportError> {
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|source| TransportError::Connection { source })?;

        let announced = Features::announced(auth.requires_pairing(), limits.names);
        let mut control = ControlChannel::new(send, recv, limits);
        control
            .send(&hello(device, announced, announced_version))
            .await?;

        match control.recv().await? {
            Control::HelloAck {
                version,
                features,
                device: peer_device,
            } => {
                let agreed = negotiate(version, features, announced)?;
                agree_on_pairing(auth, features)?;

                if let Some(code) = auth.code() {
                    let binding = channel_binding(&connection)?;
                    let mine = Proof::compute(code, Side::Caller, &binding);
                    control
                        .send(&Control::Pair {
                            proof: mine.bytes(),
                        })
                        .await?;

                    let theirs = Proof::compute(code, Side::Answerer, &binding);
                    match control.recv().await? {
                        Control::PairAck { proof } => theirs.verify(proof)?,
                        Control::Error { code, msg } => {
                            return Err(TransportError::PeerRefused { code, msg });
                        }
                        other => {
                            return Err(TransportError::UnexpectedMessage {
                                expected: "PairAck",
                                got: other.kind(),
                            });
                        }
                    }

                    tracing::info!(%peer_device, "paired");
                }

                Ok(Self::assemble(
                    connection,
                    control,
                    agreed,
                    peer_device,
                    limits,
                ))
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
        auth: &Authentication,
        limits: &Limits,
    ) -> Result<Self, TransportError> {
        let (send, recv) = connection
            .accept_bi()
            .await
            .map_err(|source| TransportError::Connection { source })?;

        let announced = Features::announced(auth.requires_pairing(), limits.names);
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
        let agreed = match negotiate(version, features, announced) {
            Ok(agreed) => agreed,
            Err(refusal) => {
                let refusal = TransportError::from(refusal);
                refuse(&connection, &mut control, &refusal, limits).await;
                return Err(refusal);
            }
        };

        // Whether the caller is pairing is settled before the ack, so a peer that will
        // never be let in is told why instead of being invited to send a manifest first.
        if let Err(refusal) = agree_on_pairing(auth, features) {
            refuse(&connection, &mut control, &refusal, limits).await;
            return Err(refusal);
        }

        control
            .send(&Control::HelloAck {
                version: PROTOCOL_VERSION,
                features: announced.bits(),
                device,
            })
            .await?;

        if let Some(code) = auth.code() {
            if let Err(refusal) = Self::prove_to_caller(&connection, &mut control, code).await {
                refuse(&connection, &mut control, &refusal, limits).await;
                return Err(refusal);
            }

            tracing::info!(%peer_device, "paired");
        }

        Ok(Self::assemble(
            connection,
            control,
            agreed,
            peer_device,
            limits,
        ))
    }

    /// Checks the caller's proof and answers with this side's own.
    ///
    /// Both directions matter: the caller's proof says it knows the code, and the answer
    /// says the machine that showed the code is the machine that received the files.
    async fn prove_to_caller(
        connection: &Connection,
        control: &mut ControlChannel,
        code: &crate::transport::pairing::PairingCode,
    ) -> Result<(), TransportError> {
        let binding = channel_binding(connection)?;

        let theirs = Proof::compute(code, Side::Caller, &binding);
        match control.recv().await? {
            Control::Pair { proof } => theirs.verify(proof)?,
            other => {
                return Err(TransportError::UnexpectedMessage {
                    expected: "Pair",
                    got: other.kind(),
                });
            }
        }

        let mine = Proof::compute(code, Side::Answerer, &binding);
        control
            .send(&Control::PairAck {
                proof: mine.bytes(),
            })
            .await?;

        Ok(())
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

    /// Closes the connection immediately, telling the peer why.
    ///
    /// The reason travels in the QUIC close frame, so the other side can print something
    /// better than "connection reset". Anything still in flight is discarded - use
    /// [`Self::close_gracefully`] after sending a message the peer is expected to read.
    pub fn close(&self, reason: &str) {
        self.connection.close(0_u32.into(), reason.as_bytes());
    }

    /// Flushes the control stream and waits for the peer to hang up before closing.
    ///
    /// Closing straight after a final message drops it: QUIC discards buffered stream
    /// data on close, so the peer sees a connection loss where it was waiting for `Done`.
    /// Waiting for its hangup means the last message is delivered before the connection
    /// goes away.
    pub async fn close_gracefully(&mut self, reason: &str) {
        linger_until_peer_leaves(&self.connection, &mut self.control, &self.limits).await;
        self.close(reason);
    }

    /// Takes the session apart so several tasks can work on it at once.
    ///
    /// A transfer with many files in flight needs three things happening independently:
    /// workers opening streams, one task writing control messages, one task reading them.
    /// Keeping them in a single object would mean a lock across the whole conversation.
    pub fn split(self) -> SessionParts {
        let (writer, reader) = self.control.split();

        SessionParts {
            streams: Streams {
                connection: self.connection,
            },
            writer,
            reader,
            peer: self.peer,
            limits: self.limits,
        }
    }

    fn assemble(
        connection: Connection,
        control: ControlChannel,
        negotiated: Negotiated,
        device: DeviceId,
        limits: &Limits,
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
            limits: *limits,
        }
    }
}

/// Tells the peer why it is being refused, then waits for it to read that and leave.
///
/// Best effort courtesy: the refusal is already decided, and a peer that has stopped
/// listening changes nothing about it. What this buys is an error message on the other
/// machine that names the problem instead of "connection lost".
async fn refuse(
    connection: &Connection,
    control: &mut ControlChannel,
    refusal: &TransportError,
    limits: &Limits,
) {
    if let Err(undeliverable) = control
        .send(&Control::Error {
            code: refusal.code(),
            msg: refusal.to_string(),
        })
        .await
    {
        tracing::debug!(%undeliverable, "could not tell the peer why it was refused");
    }

    linger_until_peer_leaves(connection, control, limits).await;
}

/// Whether both sides are pairing, or neither is.
///
/// One of each is a misconfiguration and not something to paper over: a sender told to
/// prove a code must not silently accept a receiver that never asks for one, which is
/// exactly the downgrade an attacker in the middle would attempt.
fn agree_on_pairing(auth: &Authentication, peer_features: u64) -> Result<(), TransportError> {
    let peer_pairs = Features::from_bits_truncate(peer_features).contains(Features::PAIRING);

    match (auth.requires_pairing(), peer_pairs) {
        (true, true) | (false, false) => Ok(()),
        (true, false) => Err(PairingError::Disagreement {
            ours: "expects",
            theirs: "was not given",
        }
        .into()),
        (false, true) => Err(PairingError::Disagreement {
            ours: "was not given",
            theirs: "expects",
        }
        .into()),
    }
}

/// Bytes unique to this TLS session, which both ends can derive and nobody else can.
///
/// This is what ties a pairing proof to the connection it travels on: a peer in the middle
/// has two sessions and therefore two different bindings, so a proof from one is not the
/// proof the other expects.
fn channel_binding(connection: &Connection) -> Result<[u8; BINDING_BYTES], TransportError> {
    let mut binding = [0_u8; BINDING_BYTES];

    connection
        .export_keying_material(&mut binding, BINDING_LABEL, b"")
        .map_err(|_too_short| {
            TransportError::from(PairingError::Unbindable {
                reason: "the TLS session exported no keying material",
            })
        })?;

    Ok(binding)
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
/// A session taken apart for a parallel transfer.
pub struct SessionParts {
    /// Opens and accepts data streams; cloneable, one clone per worker.
    pub streams: Streams,
    /// The control stream's sending half.
    pub writer: ControlWriter,
    /// The control stream's receiving half.
    pub reader: ControlReader,
    /// Who the session is with.
    pub peer: PeerInfo,
    /// The limits in force.
    pub limits: Limits,
}

/// The stream-opening half of a session.
///
/// Cheap to clone - a QUIC connection is a handle - so every worker holds its own without
/// coordinating with the others.
#[derive(Debug, Clone)]
pub struct Streams {
    connection: Connection,
}

impl Streams {
    /// Opens a unidirectional stream to send one file.
    ///
    /// # Errors
    /// [`TransportError::Connection`] if the peer's stream limit is reached or the
    /// connection is gone.
    pub async fn open(&self) -> Result<SendStream, TransportError> {
        self.connection
            .open_uni()
            .await
            .map_err(|source| TransportError::Connection { source })
    }

    /// Waits for the peer to open the next data stream.
    ///
    /// # Errors
    /// [`TransportError::Connection`] if the connection ends first.
    pub async fn accept(&self) -> Result<RecvStream, TransportError> {
        self.connection
            .accept_uni()
            .await
            .map_err(|source| TransportError::Connection { source })
    }

    /// Waits for the peer to hang up, then closes.
    ///
    /// Same reasoning as [`Session::close_gracefully`]: the final message has to be read
    /// before the connection disappears.
    pub async fn close_gracefully(&self, reason: &str, linger: core::time::Duration) {
        if timeout(linger, self.connection.closed()).await.is_err() {
            tracing::debug!(
                ?linger,
                "the peer did not close in time; dropping the connection"
            );
        }
        self.connection.close(0_u32.into(), reason.as_bytes());
    }

    /// Closes the connection immediately.
    pub fn close(&self, reason: &str) {
        self.connection.close(0_u32.into(), reason.as_bytes());
    }
}

fn hello(device: DeviceId, announced: Features, version: u16) -> Control {
    Control::Hello {
        version,
        features: announced.bits(),
        device,
    }
}
