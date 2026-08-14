//! A fixed set of buffers, handed out and returned.
//!
//! The pool is what makes the memory budget real: it holds exactly as many buffers as the
//! budget allows, so a worker that wants one waits instead of allocating. Peak memory is
//! therefore a property of the pool's size, not of how many files happen to be in flight.

use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

/// A pool of equally sized byte buffers.
#[derive(Debug, Clone)]
pub struct BufferPool {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    idle: Mutex<mpsc::Receiver<Vec<u8>>>,
    returns: mpsc::Sender<Vec<u8>>,
    buffer_bytes: usize,
    buffers: usize,
}

impl BufferPool {
    /// Creates a pool of `buffers` buffers of `buffer_bytes` each.
    ///
    /// Allocates them all up front: the memory a transfer will use is committed before it
    /// starts, rather than discovered halfway through under load.
    pub fn new(buffers: usize, buffer_bytes: usize) -> Self {
        let buffers = buffers.max(1);
        let buffer_bytes = buffer_bytes.max(1);

        let (returns, idle) = mpsc::channel(buffers);
        for _ in 0..buffers {
            // The channel has room for exactly this many buffers, so the send cannot fail.
            let _ = returns.try_send(vec![0_u8; buffer_bytes]);
        }

        Self {
            inner: Arc::new(Inner {
                idle: Mutex::new(idle),
                returns,
                buffer_bytes,
                buffers,
            }),
        }
    }

    /// Takes a buffer, waiting until one is free.
    ///
    /// Waiting is the point: a full pool is backpressure reaching all the way back to
    /// whichever worker wanted to read more.
    pub async fn acquire(&self) -> PooledBuffer {
        let taken = {
            let mut idle = self.inner.idle.lock().await;
            idle.recv().await
        };

        // A `None` here means every buffer was dropped rather than returned, which cannot
        // happen while the pool itself is alive; allocating one keeps the transfer going
        // instead of deadlocking on an impossible case.
        let bytes = taken.unwrap_or_else(|| vec![0_u8; self.inner.buffer_bytes]);

        PooledBuffer {
            bytes: Some(bytes),
            returns: self.inner.returns.clone(),
        }
    }

    /// How much memory this pool holds, in bytes.
    pub fn reserved_bytes(&self) -> u64 {
        self.inner.buffers as u64 * self.inner.buffer_bytes as u64
    }
}

/// A buffer on loan from a [`BufferPool`], returned when dropped.
#[derive(Debug)]
pub struct PooledBuffer {
    bytes: Option<Vec<u8>>,
    returns: mpsc::Sender<Vec<u8>>,
}

impl PooledBuffer {
    /// The bytes, for reading into.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        self.bytes.as_mut().map_or(&mut [][..], Vec::as_mut_slice)
    }

    /// The bytes, for writing out.
    pub fn bytes(&self) -> &[u8] {
        self.bytes.as_ref().map_or(&[][..], Vec::as_slice)
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(bytes) = self.bytes.take() {
            // The channel is sized to the pool, so this only fails if the pool is gone -
            // in which case the buffer has nowhere to go and is simply freed.
            let _ = self.returns.try_send(bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_returned_buffer_can_be_taken_again() {
        let pool = BufferPool::new(1, 16);

        let first = pool.acquire().await;
        assert_eq!(first.bytes().len(), 16);
        drop(first);

        let second = pool.acquire().await;
        assert_eq!(second.bytes().len(), 16);
    }

    #[tokio::test]
    async fn acquiring_beyond_the_pool_waits_for_a_return() {
        let pool = BufferPool::new(1, 8);
        let held = pool.acquire().await;

        let waiting =
            tokio::time::timeout(std::time::Duration::from_millis(50), pool.clone().acquire())
                .await;
        assert!(
            waiting.is_err(),
            "the pool should be empty while one is held"
        );

        drop(held);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), pool.acquire())
                .await
                .is_ok(),
            "returning a buffer should unblock the waiter"
        );
    }

    #[test]
    fn the_pool_reports_what_it_holds() {
        assert_eq!(BufferPool::new(4, 1024).reserved_bytes(), 4096);
    }
}
