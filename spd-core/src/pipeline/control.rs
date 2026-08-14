//! Sharing one control stream between many workers.
//!
//! With files in flight in parallel, every worker eventually has something to say on the
//! control stream. One task owns the writer and everyone else posts to it, so the stream
//! has a single owner and no lock is held across a network write.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::pipeline::PipelineError;
use crate::proto::codec::{ControlWriter, ProtoError};
use crate::proto::messages::Control;

/// How many messages may queue up before a worker waits.
///
/// Small on purpose: a worker blocked on a full outbox is a worker that has stopped
/// reading more data, which is exactly the backpressure we want.
const OUTBOX_DEPTH: usize = 16;

/// A handle for posting control messages, cloneable per worker.
#[derive(Debug, Clone)]
pub struct Outbox {
    sender: mpsc::Sender<Control>,
}

impl Outbox {
    /// Queues one message for the control stream.
    ///
    /// # Errors
    /// [`PipelineError::Proto`] with [`ProtoError::PeerClosed`] if the writer task has
    /// stopped, which means the connection is already gone.
    pub async fn send(&self, message: Control) -> Result<(), PipelineError> {
        self.sender
            .send(message)
            .await
            .map_err(|_closed| PipelineError::Proto(ProtoError::PeerClosed))
    }
}

/// Starts the task that owns the control writer.
///
/// The returned handle finishes once every [`Outbox`] is dropped and the queue is drained,
/// which is how a caller knows its last message actually reached the wire.
pub fn spawn_outbox(mut writer: ControlWriter) -> (Outbox, JoinHandle<Result<(), ProtoError>>) {
    let (sender, mut queue) = mpsc::channel(OUTBOX_DEPTH);

    let task = tokio::spawn(async move {
        while let Some(message) = queue.recv().await {
            writer.send(&message).await?;
        }

        writer.close().await
    });

    (Outbox { sender }, task)
}
