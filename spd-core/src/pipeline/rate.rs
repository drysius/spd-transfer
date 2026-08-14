//! Holding a transfer to a bandwidth limit.
//!
//! A token bucket: tokens accrue at the configured rate, a worker takes as many as it is
//! about to send, and waits when there are not enough. The bucket has one owner and workers
//! ask it over a channel, so the arithmetic that decides how fast the transfer runs is not
//! duplicated in every worker.
//!
//! The point of a limit is to leave the link usable for everything else on it. That means
//! pacing the bytes as they go out, not sending a burst and then sleeping: the second keeps
//! the average and ruins every other connection on the way through.

use core::num::NonZeroU64;
use core::time::Duration;
use std::path::PathBuf;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

use crate::pipeline::PipelineError;

/// How many requests may queue before a worker waits.
///
/// One per worker is plenty: a worker that has asked is a worker that is not sending.
const REQUEST_QUEUE_DEPTH: usize = 32;

/// Shortest wait worth suspending a task for.
///
/// Below this the sleep costs more than the bytes it is holding back, and a timer that
/// fires every few microseconds is how a rate limiter becomes the bottleneck it was meant
/// to avoid.
const MIN_WAIT: Duration = Duration::from_millis(1);

/// How fast a transfer may put bytes on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RateLimit(Option<NonZeroU64>);

impl RateLimit {
    /// As fast as the link allows.
    pub const UNLIMITED: Self = Self(None);

    /// At most this many bytes per second.
    #[must_use]
    pub const fn bytes_per_second(rate: NonZeroU64) -> Self {
        Self(Some(rate))
    }

    /// The configured rate, if there is one.
    #[must_use]
    pub const fn get(self) -> Option<NonZeroU64> {
        self.0
    }
}

/// One worker's way of asking for permission to send.
#[derive(Debug, Clone)]
pub struct Meter {
    sender: Option<mpsc::Sender<Request>>,
}

/// The task owning the bucket, if there is a limit at all.
pub type MeterTask = Option<JoinHandle<()>>;

#[derive(Debug)]
struct Request {
    bytes: u64,
    reply: oneshot::Sender<()>,
}

impl Meter {
    /// Starts the bucket, or hands back a meter that never waits.
    ///
    /// Returns the task as well: dropping every [`Meter`] is what ends it, and awaiting it
    /// is how a caller knows nothing is still pacing.
    #[must_use]
    pub fn spawn(limit: RateLimit) -> (Self, MeterTask) {
        let Some(rate) = limit.get() else {
            return (Self { sender: None }, None);
        };

        let (sender, requests) = mpsc::channel(REQUEST_QUEUE_DEPTH);
        let task = tokio::spawn(serve(rate, requests));

        (
            Self {
                sender: Some(sender),
            },
            Some(task),
        )
    }

    /// Waits until `bytes` may be sent.
    ///
    /// Returns immediately when there is no limit, which is the common case and costs one
    /// branch on an `Option`.
    ///
    /// # Errors
    /// [`PipelineError::Io`] if the task owning the bucket has stopped, which means the
    /// transfer is already shutting down.
    pub async fn take(&self, bytes: u64) -> Result<(), PipelineError> {
        let Some(sender) = self.sender.as_ref() else {
            return Ok(());
        };

        let (reply, answer) = oneshot::channel();
        sender
            .send(Request { bytes, reply })
            .await
            .map_err(|_closed| stopped())?;

        answer.await.map_err(|_dropped| stopped())
    }
}

fn stopped() -> PipelineError {
    PipelineError::Io {
        operation: "waiting for bandwidth for",
        path: PathBuf::new(),
        source: std::io::Error::other("the rate limiter stopped"),
    }
}

/// Hands out tokens until every meter is gone.
///
/// The bucket is allowed to go into debt rather than refusing a request bigger than it is:
/// a worker asks for a whole buffer at a time, and a limit slower than a buffer per second
/// still has to work. Debt is repaid before the next request is served, so the average
/// holds and only the very first buffer goes out ahead of the rate.
async fn serve(rate: NonZeroU64, mut requests: mpsc::Receiver<Request>) {
    // One second of rate is as far ahead as the bucket may get: enough to ride out a stall
    // without releasing a minute of traffic at once.
    let capacity = i64::try_from(rate.get()).unwrap_or(i64::MAX);

    // It starts with one buffer, not one second. A bucket that starts full means a transfer
    // shorter than a second ignores the limit entirely, which is exactly the transfer
    // somebody sharing a link cares about.
    let start = i64::try_from(crate::pipeline::budget::DEFAULT_BUF_SIZE_BYTES).unwrap_or(i64::MAX);
    let mut tokens = capacity.min(start);
    let mut last = Instant::now();

    while let Some(request) = requests.recv().await {
        loop {
            let now = Instant::now();
            let refill = i64::try_from(accrued(now.duration_since(last), rate)).unwrap_or(i64::MAX);
            if refill > 0 {
                tokens = tokens.saturating_add(refill).min(capacity);
                last = now;
            }

            if tokens >= 0 {
                break;
            }

            sleep(wait_for(tokens.unsigned_abs(), rate)).await;
        }

        tokens -= i64::try_from(request.bytes).unwrap_or(i64::MAX);

        // A worker that gave up before its turn is not an error: whatever made it give up
        // is already being reported.
        let _ = request.reply.send(());
    }
}

/// Bytes worth of tokens that accrue in `elapsed` at `rate`.
fn accrued(elapsed: Duration, rate: NonZeroU64) -> u64 {
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    micros.saturating_mul(rate.get()) / 1_000_000
}

/// How long `missing` bytes take to accrue at `rate`.
fn wait_for(missing: u64, rate: NonZeroU64) -> Duration {
    let micros = missing.saturating_mul(1_000_000) / rate.get();
    Duration::from_micros(micros).max(MIN_WAIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rate(bytes: u64) -> NonZeroU64 {
        NonZeroU64::new(bytes).unwrap()
    }

    #[test]
    fn tokens_accrue_in_proportion_to_time() {
        assert_eq!(accrued(Duration::from_secs(1), rate(1_000)), 1_000);
        assert_eq!(accrued(Duration::from_millis(500), rate(1_000)), 500);
        assert_eq!(accrued(Duration::ZERO, rate(1_000)), 0);
    }

    #[test]
    fn the_wait_is_how_long_the_missing_bytes_take() {
        assert_eq!(wait_for(1_000, rate(1_000)), Duration::from_secs(1));
        assert_eq!(wait_for(500, rate(1_000)), Duration::from_millis(500));
        assert_eq!(
            wait_for(1, rate(1_000_000_000)),
            MIN_WAIT,
            "a wait too short to be worth a timer is rounded up, not skipped"
        );
    }

    #[tokio::test]
    async fn an_unlimited_meter_never_waits() {
        let (meter, task) = Meter::spawn(RateLimit::UNLIMITED);

        assert!(task.is_none(), "no limit, no task to own a bucket");
        meter.take(u64::MAX).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_limited_meter_paces_what_it_hands_out() {
        // A kibibyte a second, asked for four kibibytes one at a time. The bucket opens
        // with a kibibyte in it, so the first two go straight out and the rest accrue.
        let (meter, task) = Meter::spawn(RateLimit::bytes_per_second(rate(1024)));

        let started = Instant::now();
        for _ in 0..4 {
            meter.take(1024).await.unwrap();
        }
        let elapsed = started.elapsed();

        assert!(
            elapsed >= Duration::from_secs(2),
            "four kibibytes at a kibibyte a second cannot finish in {elapsed:?}"
        );

        drop(meter);
        task.unwrap().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_larger_than_the_bucket_is_paid_off_before_the_next_one() {
        // One buffer at a rate slower than a buffer per second: the bucket goes into debt
        // and the next request waits for it to be repaid.
        let (meter, task) = Meter::spawn(RateLimit::bytes_per_second(rate(1024)));

        meter.take(4096).await.unwrap();
        let started = Instant::now();
        meter.take(1).await.unwrap();

        assert!(
            started.elapsed() >= Duration::from_secs(3),
            "a request that overdrew the bucket must be paid for before the next one"
        );

        drop(meter);
        task.unwrap().await.unwrap();
    }
}
