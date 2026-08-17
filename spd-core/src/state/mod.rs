//! State that outlives a session, owned by exactly one task.
//!
//! The previous project let every connection open the same state file, and lost races on
//! it. Here nobody touches the file: one task owns the journal and everything else asks it
//! over a channel. The single owner is what makes the race impossible, rather than a rule
//! everyone has to keep remembering.
//!
//! That owner runs on the blocking pool, because a journal is a file and file work does not
//! belong on a network task.

pub mod journal;
pub mod model;

use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::safety::limits::Limits;
use crate::safety::path::SafeRelPath;
use crate::state::journal::{Journal, JournalError};
use crate::state::model::Expected;

/// Directory holding spd's own state inside a transfer root.
pub const STATE_DIR: &str = ".spd";

/// How many requests may queue before a caller waits.
///
/// Deep enough that every worker of a wide transfer can be waiting at once, because what is
/// queued when the flush comes round is exactly what that flush covers: a queue that fills
/// early splits one device round trip into two. It is not a memory concern - a request is a
/// path and a few numbers - and the backpressure that matters is still there, since a worker
/// waiting for its record is a worker that has stopped asking for bytes.
const REQUEST_QUEUE_DEPTH: usize = 256;

/// A question or an instruction for the task owning the state.
enum Request {
    /// What is the `.part` file at this path for?
    Partial {
        path: SafeRelPath,
        reply: oneshot::Sender<Option<Expected>>,
    },

    /// A `.part` file is being written for this offer.
    Started {
        path: SafeRelPath,
        expected: Expected,
        reply: oneshot::Sender<Result<(), JournalError>>,
    },

    /// That `.part` file is gone - committed, or thrown away.
    Forget {
        path: SafeRelPath,
        reply: oneshot::Sender<Result<(), JournalError>>,
    },
}

/// A way to talk to the task owning the state. Cloneable, one clone per worker.
#[derive(Debug, Clone)]
pub struct StateHandle {
    sender: mpsc::Sender<Request>,
}

/// The task owning the state.
///
/// Awaiting it once every [`StateHandle`] has been dropped is what reports whether the
/// final compaction succeeded - and is the only way to know the state on disk is settled.
pub type StateTask = JoinHandle<Result<(), JournalError>>;

/// Starts the task that owns the transfer state under `root`.
///
/// # Errors
/// [`JournalError::Io`] if the state directory cannot be created or written,
/// [`JournalError::Interrupted`] if the task fails before the state is loaded.
pub async fn spawn_state(
    root: &Path,
    limits: &Limits,
) -> Result<(StateHandle, StateTask), JournalError> {
    let root = root.to_path_buf();
    let limits = *limits;

    let journal = tokio::task::spawn_blocking(move || Journal::open(&root, &limits))
        .await
        .map_err(|_panicked| JournalError::Interrupted)?;

    let (sender, requests) = mpsc::channel(REQUEST_QUEUE_DEPTH);
    let task = tokio::task::spawn_blocking(move || serve(journal, requests));

    Ok((StateHandle { sender }, task))
}

impl StateHandle {
    /// What the `.part` file at this path was started for, if anything.
    ///
    /// # Errors
    /// [`JournalError::Interrupted`] if the task owning the state has stopped.
    pub async fn partial(&self, path: SafeRelPath) -> Result<Option<Expected>, JournalError> {
        let (reply, answer) = oneshot::channel();
        self.ask(Request::Partial { path, reply }).await?;
        answer.await.map_err(|_dropped| JournalError::Interrupted)
    }

    /// Records that a `.part` file is being written for this offer.
    ///
    /// Returns once the record is on the device, so the bytes written after it are always
    /// bytes the next run can recognise.
    ///
    /// # Errors
    /// Whatever the journal reported, or [`JournalError::Interrupted`] if the task owning
    /// it has stopped.
    pub async fn started(&self, path: SafeRelPath, expected: Expected) -> Result<(), JournalError> {
        let (reply, answer) = oneshot::channel();
        self.ask(Request::Started {
            path,
            expected,
            reply,
        })
        .await?;
        answer.await.map_err(|_dropped| JournalError::Interrupted)?
    }

    /// Records that the `.part` file at this path is gone.
    ///
    /// # Errors
    /// Same as [`Self::started`].
    pub async fn forget(&self, path: SafeRelPath) -> Result<(), JournalError> {
        let (reply, answer) = oneshot::channel();
        self.ask(Request::Forget { path, reply }).await?;
        answer.await.map_err(|_dropped| JournalError::Interrupted)?
    }

    /// Queues one request, waiting if the task is behind.
    ///
    /// `try_send` would drop a request the moment the queue filled; waiting on a full queue
    /// is the backpressure this channel exists for.
    async fn ask(&self, request: Request) -> Result<(), JournalError> {
        self.sender
            .send(request)
            .await
            .map_err(|_closed| JournalError::Interrupted)
    }
}

/// Answers requests until every handle is gone, then settles the state on disk.
///
/// Requests are taken in groups: everything already queued is recorded, then flushed once,
/// then answered. Waiting for the flush is what a caller asked for, and the flush is the
/// expensive part - one device round trip whether it covers one record or sixty-four. Doing
/// it per request made this task the ceiling on the whole transfer: sixty-four workers, all
/// of them waiting behind a single file being flushed a few hundred times a second.
///
/// Nobody is answered early. A reply is sent after the flush that covers its record, which
/// is exactly what it meant before.
fn serve(mut journal: Journal, mut requests: mpsc::Receiver<Request>) -> Result<(), JournalError> {
    let mut group = Vec::with_capacity(REQUEST_QUEUE_DEPTH);

    while let Some(first) = requests.blocking_recv() {
        group.push(first);
        // Whatever else is already waiting joins this flush. Nothing is waited *for*: a
        // group is only ever what the queue already held.
        while let Ok(next) = requests.try_recv() {
            group.push(next);
        }

        let mut answers = Vec::with_capacity(group.len());
        for request in group.drain(..) {
            match request {
                Request::Partial { path, reply } => {
                    // Answered from memory and outside the flush: reading what is known
                    // needs no device round trip, and the record it might read was made
                    // durable by the group that carried it.
                    let _ = reply.send(journal.partial(&path));
                }
                Request::Started {
                    path,
                    expected,
                    reply,
                } => answers.push((reply, journal.record_started(path, expected))),
                Request::Forget { path, reply } => answers.push((reply, journal.forget(&path))),
            }
        }

        // One flush for the group, and its outcome belongs to every record in it: a caller
        // told its record is safe when the flush failed would be a caller lied to.
        let flushed = journal.commit();
        for (reply, recorded) in answers {
            // A caller that gave up before the answer arrived is not an error: the transfer
            // it belonged to already failed for its own reason.
            let _ = reply.send(recorded.and(clone_outcome(&flushed)));
        }
    }

    journal.compact()
}

/// Repeats one flush's outcome for every caller waiting on it.
///
/// [`JournalError`] carries an [`std::io::Error`], which cannot be cloned; every caller in
/// the group still has to hear the same thing, so the failure is restated rather than
/// shared.
fn clone_outcome(flushed: &Result<(), JournalError>) -> Result<(), JournalError> {
    match flushed {
        Ok(()) => Ok(()),
        Err(failure) => Err(JournalError::Io {
            operation: "flushing the journal",
            path: PathBuf::new(),
            source: std::io::Error::other(failure.to_string()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(label: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!("spd-state-{label}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn path(name: &str) -> SafeRelPath {
        SafeRelPath::from_components(&[name.to_owned()], &Limits::DEFAULT).unwrap()
    }

    #[tokio::test]
    async fn what_one_session_started_the_next_one_finds() {
        let root = scratch("handoff");
        let limits = Limits::DEFAULT;
        let expected = Expected {
            size: 1_000,
            mtime: 5,
            hash: None,
        };

        let (state, task) = spawn_state(&root, &limits).await.unwrap();
        state.started(path("half.bin"), expected).await.unwrap();
        drop(state);
        task.await.unwrap().unwrap();

        let (state, task) = spawn_state(&root, &limits).await.unwrap();
        assert_eq!(
            state.partial(path("half.bin")).await.unwrap(),
            Some(expected)
        );
        drop(state);
        task.await.unwrap().unwrap();

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn a_finished_file_leaves_nothing_behind() {
        let root = scratch("finished");
        let limits = Limits::DEFAULT;

        let (state, task) = spawn_state(&root, &limits).await.unwrap();
        state
            .started(
                path("done.bin"),
                Expected {
                    size: 1,
                    mtime: 1,
                    hash: None,
                },
            )
            .await
            .unwrap();
        state.forget(path("done.bin")).await.unwrap();
        drop(state);
        task.await.unwrap().unwrap();

        let (state, task) = spawn_state(&root, &limits).await.unwrap();
        assert_eq!(state.partial(path("done.bin")).await.unwrap(), None);
        drop(state);
        task.await.unwrap().unwrap();

        std::fs::remove_dir_all(&root).unwrap();
    }
}
