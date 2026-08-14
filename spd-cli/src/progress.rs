//! Drawing what the core is counting.
//!
//! The bar is a reader, never a participant: it polls a [`Progress`] snapshot on a timer and
//! draws it. Nothing in the transfer waits for the terminal, and a run with the bar switched
//! off behaves identically - which is what keeps "it only fails without `--quiet`" from ever
//! being a sentence anyone has to say.

use core::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use spd_core::metrics::Progress;
use tokio::task::JoinHandle;

/// How often the bar is redrawn.
///
/// Fast enough to look live, slow enough that a transfer of small files is not spending its
/// time on escape codes.
const REDRAW: Duration = Duration::from_millis(120);

/// A bar drawing one transfer, until it is stopped.
pub(crate) struct Bar {
    task: JoinHandle<()>,
    bar: ProgressBar,
}

impl Bar {
    /// Starts drawing `progress`, or draws nothing at all.
    ///
    /// Nothing at all is the right answer more often than it looks: output that is being
    /// piped somewhere, or a run that asked for JSON logs, wants lines rather than a bar
    /// that redraws itself sixty times a second into a file.
    pub(crate) fn start(progress: &Progress, draw: bool) -> Self {
        let bar = if draw {
            let bar = ProgressBar::new(0);
            bar.set_style(style());
            bar
        } else {
            ProgressBar::hidden()
        };

        let drawn = bar.clone();
        let counters = progress.clone();

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REDRAW);
            loop {
                ticker.tick().await;
                draw_once(&drawn, &counters);
            }
        });

        Self { task, bar }
    }

    /// Stops drawing and clears the line.
    ///
    /// The summary that follows is the record; a half-finished bar above it is noise.
    pub(crate) fn stop(self) {
        self.task.abort();
        self.bar.finish_and_clear();
    }
}

fn draw_once(bar: &ProgressBar, progress: &Progress) {
    let snapshot = progress.snapshot();

    bar.set_length(snapshot.bytes_total);
    bar.set_position(snapshot.bytes_done.min(snapshot.bytes_total));
    bar.set_message(format!(
        "{}/{} files",
        snapshot.files_done, snapshot.files_total
    ));
}

/// Bytes, rate and time remaining, with the file count alongside.
///
/// Falls back to a plain template if the style is ever rejected, because a progress bar is
/// not worth failing a transfer over.
fn style() -> ProgressStyle {
    ProgressStyle::with_template(
        "  {bar:32} {bytes:>10}/{total_bytes:<10} {binary_bytes_per_sec:>12}  {msg}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("=> ")
}
