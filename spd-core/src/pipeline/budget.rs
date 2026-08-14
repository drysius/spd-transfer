//! Memory budget, and the concurrency derived from it.
//!
//! The direction matters: the user gives a memory budget and concurrency follows. The
//! opposite — pick a stream count and discover the RSS afterwards — is how a transfer
//! ends up holding gigabytes on a machine that cannot spare them.

use core::num::{NonZeroU32, NonZeroU64};

/// Default memory budget for the whole transfer, in bytes.
pub const DEFAULT_MEM_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Default size of one pipeline buffer, in bytes.
pub const DEFAULT_BUF_SIZE_BYTES: u64 = 1024 * 1024;

/// Default number of buffers in flight per stream: one being read, one in CPU, one being
/// written. Deeper hides latency; it also multiplies the memory each stream holds.
pub const DEFAULT_PIPELINE_DEPTH: u32 = 3;

/// How wide the pipeline may run under a given memory budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Concurrency {
    /// Number of files transferred at once, one QUIC stream each.
    pub streams: NonZeroU32,

    /// Memory those streams can actually hold, in bytes. Always `<= mem_budget_bytes`,
    /// except at the floor of one stream — see [`derive_concurrency`].
    pub reserved_bytes: u64,
}

/// Derives stream concurrency from a memory budget.
///
/// `streams = clamp(mem_budget / (buf_size * depth), 1, max_streams)`.
///
/// The floor of one stream is deliberate: a budget too small for a single pipeline still
/// transfers, one buffer set at a time, instead of failing at startup. That is the only
/// case where `reserved_bytes` exceeds the requested budget, and the caller can detect it
/// by comparing the two.
///
/// Takes `NonZero` arguments because a zero buffer, depth or stream cap has no meaning
/// here — the type rules them out instead of a runtime check nobody reads.
pub fn derive_concurrency(
    mem_budget_bytes: u64,
    buf_size_bytes: NonZeroU64,
    pipeline_depth: NonZeroU32,
    max_streams: NonZeroU32,
) -> Concurrency {
    let per_stream_bytes = buf_size_bytes
        .get()
        .saturating_mul(u64::from(pipeline_depth.get()));

    let affordable = mem_budget_bytes / per_stream_bytes;
    let streams = u32::try_from(affordable)
        .unwrap_or(u32::MAX)
        .clamp(1, max_streams.get());

    // INVARIANT: clamped to at least 1 just above, so the NonZero conversion holds.
    let streams = NonZeroU32::new(streams).unwrap_or(NonZeroU32::MIN);

    let concurrency = Concurrency {
        streams,
        reserved_bytes: per_stream_bytes.saturating_mul(u64::from(streams.get())),
    };

    tracing::debug!(
        mem_budget_bytes,
        per_stream_bytes,
        streams = concurrency.streams.get(),
        reserved_bytes = concurrency.reserved_bytes,
        "derived concurrency from memory budget"
    );

    concurrency
}

/// [`derive_concurrency`] with the project defaults for buffer size and pipeline depth.
///
/// `max_streams` still comes from [`crate::safety::limits::Limits`], because it is also a
/// protocol bound and not just a memory question.
pub fn derive_concurrency_with_defaults(
    mem_budget_bytes: u64,
    max_streams: NonZeroU32,
) -> Concurrency {
    // INVARIANT: both defaults are non-zero literals defined in this module.
    let buf = NonZeroU64::new(DEFAULT_BUF_SIZE_BYTES).unwrap_or(NonZeroU64::MIN);
    let depth = NonZeroU32::new(DEFAULT_PIPELINE_DEPTH).unwrap_or(NonZeroU32::MIN);

    derive_concurrency(mem_budget_bytes, buf, depth, max_streams)
}

/// Limits on concurrent work, as the user set them.
///
/// Separate knobs because the resources are separate: an HDD degrades with concurrent
/// reads while an `NVMe` benefits, and neither has anything to do with how many QUIC streams
/// the peer allows. No autodetection - an explicit flag with a conservative default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobLimits {
    /// Files being read from disk at once.
    pub disk_read_jobs: NonZeroU32,
    /// Files being written to disk at once.
    pub disk_write_jobs: NonZeroU32,
    /// Files in flight at once, one stream each.
    pub streams: NonZeroU32,
}

impl JobLimits {
    /// Conservative defaults: four concurrent operations per disk, sixteen streams.
    ///
    /// Written with `match` because `Option::unwrap_or` is not usable in a constant yet,
    /// and a constant is what keeps these defaults visible in one place.
    pub const DEFAULT: Self = Self {
        disk_read_jobs: match NonZeroU32::new(4) {
            Some(value) => value,
            None => NonZeroU32::MIN,
        },
        disk_write_jobs: match NonZeroU32::new(4) {
            Some(value) => value,
            None => NonZeroU32::MIN,
        },
        streams: match NonZeroU32::new(16) {
            Some(value) => value,
            None => NonZeroU32::MIN,
        },
    };
}

impl Default for JobLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// How a transfer will actually run: how many workers, how much memory, how many buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferPlan {
    /// Files transferred at once.
    pub workers: NonZeroU32,
    /// Size of one pipeline buffer, in bytes.
    pub buffer_bytes: usize,
    /// Buffers in the pool, shared by all workers.
    pub buffers: usize,
    /// Files read from disk at once.
    pub disk_read_jobs: NonZeroU32,
    /// Files written to disk at once.
    pub disk_write_jobs: NonZeroU32,
}

impl TransferPlan {
    /// Works out a plan from the memory budget and the user's job limits.
    ///
    /// Worker count follows the budget, never the other way around: asking for 64 streams
    /// on a 32 MiB budget gets as many workers as that memory can feed, because the
    /// alternative is discovering the real memory use only once the transfer is running.
    pub fn derive(mem_budget_bytes: u64, jobs: JobLimits) -> Self {
        let concurrency = derive_concurrency_with_defaults(mem_budget_bytes, jobs.streams);
        let buffer_bytes = usize::try_from(DEFAULT_BUF_SIZE_BYTES).unwrap_or(usize::MAX);

        // One buffer per worker per pipeline stage, which is what the budget was divided
        // into in the first place.
        let buffers = concurrency.streams.get() as usize * DEFAULT_PIPELINE_DEPTH as usize;

        Self {
            workers: concurrency.streams,
            buffer_bytes,
            buffers,
            disk_read_jobs: jobs.disk_read_jobs,
            disk_write_jobs: jobs.disk_write_jobs,
        }
    }

    /// Memory the buffer pool will hold, in bytes.
    pub fn reserved_bytes(&self) -> u64 {
        self.buffers as u64 * self.buffer_bytes as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz64(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn nz32(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).unwrap()
    }

    #[test]
    fn budget_divides_into_streams() {
        // 24 MiB budget, 1 MiB buffers, depth 3 → 8 streams.
        let derived = derive_concurrency(24 * 1024 * 1024, nz64(1024 * 1024), nz32(3), nz32(64));

        assert_eq!(derived.streams.get(), 8);
        assert_eq!(derived.reserved_bytes, 24 * 1024 * 1024);
    }

    #[test]
    fn stream_cap_wins_over_a_large_budget() {
        let derived = derive_concurrency(u64::MAX, nz64(1024 * 1024), nz32(3), nz32(16));

        assert_eq!(derived.streams.get(), 16);
    }

    #[test]
    fn tiny_budget_still_transfers_with_one_stream() {
        let derived = derive_concurrency(1, nz64(1024 * 1024), nz32(3), nz32(16));

        assert_eq!(derived.streams.get(), 1);
        assert!(
            derived.reserved_bytes > 1,
            "the floor may exceed the budget, by design"
        );
    }

    #[test]
    fn reserved_memory_never_exceeds_the_budget_above_the_floor() {
        let budget = 100 * 1024 * 1024;
        let derived = derive_concurrency(budget, nz64(3 * 1024 * 1024), nz32(4), nz32(1024));

        assert!(derived.streams.get() > 1);
        assert!(derived.reserved_bytes <= budget);
    }

    #[test]
    fn defaults_match_the_explicit_call() {
        let explicit = derive_concurrency(
            DEFAULT_MEM_BUDGET_BYTES,
            nz64(DEFAULT_BUF_SIZE_BYTES),
            nz32(DEFAULT_PIPELINE_DEPTH),
            nz32(16),
        );

        assert_eq!(
            derive_concurrency_with_defaults(DEFAULT_MEM_BUDGET_BYTES, nz32(16)),
            explicit
        );
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    #[test]
    fn the_plan_never_reserves_more_than_the_budget() {
        let budget = 32 * 1024 * 1024;
        let plan = TransferPlan::derive(budget, JobLimits::DEFAULT);

        assert!(plan.reserved_bytes() <= budget);
        assert!(plan.workers.get() >= 1);
    }

    #[test]
    fn a_stream_request_beyond_the_budget_is_cut_to_what_memory_allows() {
        let jobs = JobLimits {
            streams: NonZeroU32::new(64).unwrap(),
            ..JobLimits::DEFAULT
        };

        let plan = TransferPlan::derive(24 * 1024 * 1024, jobs);

        assert_eq!(plan.workers.get(), 8, "24 MiB / (1 MiB * depth 3) = 8");
    }

    #[test]
    fn job_limits_pass_through_untouched() {
        let jobs = JobLimits {
            disk_read_jobs: NonZeroU32::new(2).unwrap(),
            disk_write_jobs: NonZeroU32::new(3).unwrap(),
            streams: NonZeroU32::new(4).unwrap(),
        };

        let plan = TransferPlan::derive(DEFAULT_MEM_BUDGET_BYTES, jobs);

        assert_eq!(plan.disk_read_jobs.get(), 2);
        assert_eq!(plan.disk_write_jobs.get(), 3);
        assert_eq!(plan.workers.get(), 4);
    }
}
