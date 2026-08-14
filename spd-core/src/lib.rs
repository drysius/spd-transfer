//! Core of `spd-transfer`: peer-to-peer file and folder transfer over QUIC.
//!
//! This crate holds all protocol logic and carries no user-interface dependency -
//! no `clap`, no `indicatif`, no `anyhow`. Errors are typed with `thiserror` so the
//! caller can branch on them; wording for humans happens in `spd-cli`.
//!
//! # Phase status
//!
//! The project was built in phases (see `PLAN.md §10`), all of which are done: the QUIC
//! transport, tree scanning with a hash cache, skip/need negotiation, parallel transfers,
//! interrupted files picked up where they stopped, bodies compressed when that is worth
//! doing, a pairing code that decides who is allowed to send anything at all, and counters
//! a user interface can draw without the transfer waiting for it.
//!
//! The wire format is documented in `docs/PROTOCOL.md` and frozen in
//! `spd-core/tests/golden/`.

// Panicking is a bug in the core: a failure that reaches the user must be a typed error,
// never an abort. Tests are exempt so assertions stay readable.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod compress;
pub mod error;
pub mod metrics;
pub mod pipeline;
pub mod proto;
pub mod safety;
pub mod scan;
pub mod state;
pub mod transport;

pub use error::{Error, Result};
