//! Core of `spd-transfer`: peer-to-peer file and folder transfer over QUIC.
//!
//! This crate holds all protocol logic and carries no user-interface dependency —
//! no `clap`, no `indicatif`, no `anyhow`. Errors are typed with `thiserror` so the
//! caller can branch on them; wording for humans happens in `spd-cli`.
//!
//! # Phase status
//!
//! The project is built in phases (see `PLAN.md §10`); every phase ends with green CI
//! and a usable binary. Currently at **F0 — skeleton**: errors, version negotiation,
//! safety limits and the memory-budget model. Transport, protocol and pipeline land in
//! F1 onwards.

// Panicking is a bug in the core: a failure that reaches the user must be a typed error,
// never an abort. Tests are exempt so assertions stay readable.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod error;
pub mod pipeline;
pub mod safety;
pub mod version;

pub use error::{Error, Result};
