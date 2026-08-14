//! Trying again when the connection, and not the data, is what failed.
//!
//! Resume is what makes a retry worth anything: the `.part` files and the journal survive a
//! lost connection, so the next attempt continues instead of starting over. Without that,
//! reconnecting would just repeat the same work more slowly.
//!
//! Only the connection's failures are retried - see [`PipelineError::is_recoverable`]. A
//! refused path or a hash that did not match would fail again identically, and hiding that
//! behind five attempts would only delay the message the user needs to read.

use core::num::NonZeroU32;
use core::time::Duration;
use std::net::SocketAddr;
use std::path::Path;

use crate::pipeline::recv::{ReceiveOptions, receive_tree};
use crate::pipeline::send::{SendOptions, SendReport, send_tree};
use crate::pipeline::{PipelineError, TransferSummary};
use crate::proto::messages::DeviceId;
use crate::safety::limits::Limits;
use crate::transport::{Listener, TrustPolicy, connect};

/// How long to wait before the first reconnection.
///
/// Short enough that a blip costs nothing noticeable, long enough that a peer restarting
/// has a moment to bind its socket again.
const FIRST_DELAY: Duration = Duration::from_millis(250);

/// Ceiling on the wait between attempts. The delay doubles until it reaches this.
const MAX_DELAY: Duration = Duration::from_secs(8);

/// Attempts made before a transfer gives up.
const DEFAULT_ATTEMPTS: u32 = 5;

/// How hard to try when a connection fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts, the first one included. One means no retry at all.
    pub attempts: NonZeroU32,

    /// Wait before the second attempt. It doubles from there.
    pub first_delay: Duration,

    /// Longest wait between two attempts.
    pub max_delay: Duration,
}

impl RetryPolicy {
    /// Five attempts, backing off from 250 ms to 8 s.
    ///
    /// Written with `match` because `Option::unwrap_or` is not usable in a constant yet,
    /// and a constant is what keeps these defaults visible in one place.
    pub const DEFAULT: Self = Self {
        attempts: match NonZeroU32::new(DEFAULT_ATTEMPTS) {
            Some(attempts) => attempts,
            None => NonZeroU32::MIN,
        },
        first_delay: FIRST_DELAY,
        max_delay: MAX_DELAY,
    };

    /// One attempt and no waiting, for a caller that wants the failure immediately.
    pub const NEVER: Self = Self {
        attempts: NonZeroU32::MIN,
        first_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    };

    /// How long to wait after `attempt` failed. Attempts are counted from one.
    fn delay_after(&self, attempt: u32) -> Duration {
        let doubling = 2_u32.saturating_pow(attempt.saturating_sub(1));
        self.first_delay
            .saturating_mul(doubling)
            .min(self.max_delay)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Sends a tree, reconnecting when the connection drops.
///
/// The report describes the attempt that succeeded: files finished by an earlier attempt
/// are counted as skipped by the last one, and a file that was half sent contributes only
/// the bytes that actually crossed this time.
///
/// # Errors
/// The last failure, once the attempts run out or the failure is one a retry could not
/// change - see [`PipelineError::is_recoverable`].
pub async fn send_tree_reconnecting(
    address: SocketAddr,
    device: DeviceId,
    policy: TrustPolicy,
    root: &Path,
    options: SendOptions,
    limits: &Limits,
    retry: RetryPolicy,
) -> Result<SendReport, PipelineError> {
    let mut attempt = 1;

    loop {
        let outcome = match connect(address, device, policy, *limits).await {
            Ok(session) => send_tree(session, root, options, limits).await,
            Err(unreachable) => Err(PipelineError::Transport(unreachable)),
        };

        match give_up_or_wait(outcome, attempt, retry).await {
            Continue::With(report) => return Ok(report),
            Continue::Failed(error) => return Err(error),
            Continue::Again => attempt += 1,
        }
    }
}

/// Receives into `destination`, accepting another session when one drops.
///
/// The listener stays bound between attempts, so a sender that reconnects finds it waiting.
/// The summary describes the attempt that succeeded, for the same reason as
/// [`send_tree_reconnecting`].
///
/// # Errors
/// The last failure, once the attempts run out or the failure is one a retry could not
/// change.
pub async fn receive_tree_resuming(
    listener: &Listener,
    destination: &Path,
    options: ReceiveOptions,
    limits: &Limits,
    retry: RetryPolicy,
) -> Result<TransferSummary, PipelineError> {
    let mut attempt = 1;

    loop {
        let outcome = match listener.accept().await {
            Ok(session) => receive_tree(session, destination, options, limits).await,
            Err(refused) => Err(PipelineError::Transport(refused)),
        };

        match give_up_or_wait(outcome, attempt, retry).await {
            Continue::With(summary) => return Ok(summary),
            Continue::Failed(error) => return Err(error),
            Continue::Again => attempt += 1,
        }
    }
}

/// What to do after one attempt.
enum Continue<T> {
    /// It worked.
    With(T),
    /// It failed for good.
    Failed(PipelineError),
    /// It failed, the wait is over, try again.
    Again,
}

/// Decides whether an attempt's outcome ends the transfer, and waits if it does not.
async fn give_up_or_wait<T>(
    outcome: Result<T, PipelineError>,
    attempt: u32,
    retry: RetryPolicy,
) -> Continue<T> {
    let error = match outcome {
        Ok(value) => return Continue::With(value),
        Err(error) => error,
    };

    if attempt >= retry.attempts.get() || !error.is_recoverable() {
        return Continue::Failed(error);
    }

    let delay = retry.delay_after(attempt);
    tracing::warn!(
        %error,
        attempt,
        attempts = retry.attempts.get(),
        ?delay,
        "the connection failed; picking the transfer up again"
    );

    tokio::time::sleep(delay).await;
    Continue::Again
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_delay_doubles_and_then_stops_growing() {
        let policy = RetryPolicy {
            attempts: NonZeroU32::MIN,
            first_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(300),
        };

        assert_eq!(policy.delay_after(1), Duration::from_millis(100));
        assert_eq!(policy.delay_after(2), Duration::from_millis(200));
        assert_eq!(policy.delay_after(3), Duration::from_millis(300));
        assert_eq!(policy.delay_after(30), Duration::from_millis(300));
    }

    #[tokio::test]
    async fn a_failure_a_retry_cannot_change_ends_it_at_once() {
        let refused = PipelineError::HashMismatch {
            path: std::path::PathBuf::from("photo.jpg"),
        };

        let decision = give_up_or_wait::<()>(Err(refused), 1, RetryPolicy::DEFAULT).await;

        assert!(matches!(decision, Continue::Failed(_)));
    }

    #[tokio::test]
    async fn the_last_attempt_reports_instead_of_waiting_again() {
        let lost = PipelineError::Proto(crate::proto::codec::ProtoError::PeerClosed);
        let policy = RetryPolicy {
            attempts: NonZeroU32::MIN,
            ..RetryPolicy::DEFAULT
        };

        let decision = give_up_or_wait::<()>(Err(lost), 1, policy).await;

        assert!(matches!(decision, Continue::Failed(_)));
    }
}
