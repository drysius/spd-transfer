//! A sender/receiver pair over loopback QUIC, plus the scratch directories they use.
//!
//! Integration tests describe *what* should happen; the wiring to get two peers talking
//! lives here so a test that fails points at behaviour rather than at setup.

// Each test binary compiles this module separately and uses the part of it that its own
// subject needs. Unused-in-one-binary is the normal state of a shared harness, not a sign
// that something here has no callers.
#![allow(dead_code)]

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;
use spd_core::transport::{Authentication, Listener, Session, connect, listen};

/// A directory that deletes itself when the test ends.
///
/// Named after the test that made it, so a leftover directory after a crash says which
/// test to look at.
pub(crate) struct Scratch {
    path: PathBuf,
}

impl Scratch {
    /// Creates a fresh directory for `label`.
    pub(crate) fn new(label: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();

        let path = std::env::temp_dir().join(format!("spd-test-{label}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();

        Self { path }
    }

    /// The directory itself.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Writes a file inside it and returns the full path.
    pub(crate) fn write(&self, name: &str, contents: &[u8]) -> PathBuf {
        let file = self.path.join(name);
        std::fs::write(&file, contents).unwrap();
        file
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Best effort: a leftover temp directory is untidy, a panic inside Drop would
        // hide the real test failure.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Binds a listener on an ephemeral loopback port.
///
/// Unpaired: these tests are about what moves once a session exists, and pairing has its
/// own suite in `tests/pairing.rs`.
pub(crate) fn bound_listener(limits: Limits) -> (Listener, SocketAddr) {
    let listener = listen(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        DeviceId::random().unwrap(),
        Authentication::Insecure,
        limits,
    )
    .unwrap();

    let address = listener.local_addr().unwrap();
    (listener, address)
}

/// Connects to `address` as an unauthenticated peer.
pub(crate) async fn dial(address: SocketAddr, limits: Limits) -> Session {
    connect(
        address,
        DeviceId::random().unwrap(),
        &Authentication::Insecure,
        limits,
    )
    .await
    .unwrap()
}

/// Deterministic bytes, so a mismatch points at the transfer rather than at the fixture.
pub(crate) fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| u8::try_from(index % 251).unwrap_or_default())
        .collect()
}
