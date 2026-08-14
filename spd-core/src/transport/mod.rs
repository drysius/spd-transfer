//! QUIC transport: endpoints, TLS material and the live session.
//!
//! QUIC is doing work the previous project did by hand and got wrong: ordered delivery,
//! deduplication, retransmission and encryption. What is left here is opening the right
//! streams and refusing a peer that does not belong.

pub mod endpoint;
pub mod session;
pub mod tls;

pub use endpoint::{Listener, TransportError, connect, listen};
pub use session::{PeerInfo, Session};
pub use tls::{ALPN, TlsError, TrustPolicy};
