//! Finding out what exists locally, and deciding what needs to move.
//!
//! [`walk`] lists a tree, [`hash_cache`] remembers hashes so a second send does not rehash
//! everything, [`manifest`] turns the listing into batches for the wire, and [`diff`] is
//! the receiver deciding, entry by entry, what it actually needs.

pub mod diff;
pub mod hash_cache;
pub mod manifest;
pub mod walk;

pub use diff::decide;
pub use manifest::{Manifest, ScanError, batches};
pub use walk::{ScannedFile, WalkOptions, walk};
