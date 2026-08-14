//! Everything that stands between a hostile peer and this machine.
//!
//! Two rules govern this module: a guarantee is a type, never a convention, and every
//! bound lives in [`limits`] instead of being scattered as magic constants.

pub mod limits;
pub mod path;

pub use limits::{Limits, LimitsError};
pub use path::{PathError, SafeRelPath};
