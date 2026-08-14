//! Everything that stands between a hostile peer and this machine.
//!
//! Two rules govern this module: a guarantee is a type, never a convention, and every
//! bound lives in [`limits`] instead of being scattered as magic constants.
//!
//! `path::SafeRelPath` lands in F7 together with its fuzz target.

pub mod limits;
