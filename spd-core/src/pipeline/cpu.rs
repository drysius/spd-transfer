//! Getting heavy CPU work off the tasks that are meant to be moving bytes.
//!
//! Compressing a buffer takes milliseconds. An `async fn` that spends milliseconds
//! computing is an `async fn` that has stopped serving every other transfer on that worker
//! thread, so the codec runs on rayon and the network task waits for the answer.
//!
//! Work travels by value, buffers included, and comes back the same way. Nothing is
//! borrowed across the hop, so nothing has to outlive a task that may be cancelled.

use core::num::NonZeroU32;
use std::path::PathBuf;

use tokio::sync::{OwnedSemaphorePermit, oneshot};

use crate::pipeline::PipelineError;

/// Runs `work` on a pool of exactly `threads` threads and waits for it.
///
/// For the bulk work that happens before a transfer starts, where the caller is already on
/// a blocking thread and wants the machine's cores rather than one of them. The pool is
/// built for this call and dropped with it, so `--cpu-jobs` means the same thing here as it
/// does on the transfer path instead of being whatever rayon's global pool was sized to.
///
/// A pool that cannot be built is not a failure: the work runs on the default pool, which
/// is slower to control but no less correct.
pub(crate) fn on_cores<T, F>(threads: NonZeroU32, work: F) -> T
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    match rayon::ThreadPoolBuilder::new()
        .num_threads(threads.get() as usize)
        .build()
    {
        Ok(pool) => pool.install(work),
        Err(error) => {
            tracing::debug!(%error, "could not size a thread pool; using the default one");
            work()
        }
    }
}

/// Runs `work` on the CPU pool and waits for its result.
///
/// The permit is held for the length of the work and dropped with it, so the caller cannot
/// forget to release it - the type is what enforces the bound, not the discipline of the
/// call site.
///
/// # Errors
/// [`PipelineError::Io`] if the CPU task disappears without answering, which means it
/// panicked - a bug, reported rather than silently retried.
pub(crate) async fn on_cpu<F, T>(permit: OwnedSemaphorePermit, work: F) -> Result<T, PipelineError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (reply, answer) = oneshot::channel();

    rayon::spawn(move || {
        let done = work();
        drop(permit);
        // A caller that gave up is not an error here: whatever made it give up is already
        // being reported, and this result simply has nowhere to go.
        let _ = reply.send(done);
    });

    answer.await.map_err(|_gone| PipelineError::Io {
        operation: "running a compression task",
        path: PathBuf::new(),
        source: std::io::Error::other("the CPU pool task did not finish"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Semaphore;

    use super::*;

    #[tokio::test]
    async fn work_and_its_buffers_come_back() {
        let jobs = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&jobs).acquire_owned().await.unwrap();

        let carried = vec![1_u8, 2, 3];
        let (returned, sum) = on_cpu(permit, move || {
            let sum: u32 = carried.iter().map(|byte| u32::from(*byte)).sum();
            (carried, sum)
        })
        .await
        .unwrap();

        assert_eq!(sum, 6);
        assert_eq!(returned, vec![1, 2, 3]);
        assert_eq!(
            jobs.available_permits(),
            1,
            "the permit is released with the work"
        );
    }
}
