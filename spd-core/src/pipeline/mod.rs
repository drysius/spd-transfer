//! Per-file transfer pipeline: disk → CPU → network, and back.
//!
//! The pipeline stages are wired with bounded channels, so backpressure is a consequence
//! of the structure rather than a knob: when the channel fills, the disk reader stops and
//! memory stops growing.
//!
//! [`budget`] is the piece that decides how wide the pipeline may run. Send and receive
//! land in F2, the work queue and semaphores in F4.

pub mod budget;
