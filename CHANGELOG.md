# Changelog

Kept by hand, in the order a reader cares about rather than the order the commits landed.
Dates are the day the work was finished.

## Unreleased

### Added

- **`--names posix`, for trees that never leave Unix.** A file called `?`, one ending in a
  dot, or one called `CON` is ordinary on Linux and impossible on Windows, and until now it
  was always left behind - which on a server directory meant a backup quietly missing files.
  Pass `--names posix` on **both** sides and those names cross as they are. It stays off by
  default, so a received folder still opens on Windows, and it is refused outright on
  Windows, where such a name cannot be written at all.
  The two sides agree through a feature bit rather than the sender deciding alone: if the
  receiver did not ask for it, the sender never offers those names and reports them under
  `not sent`, exactly as before. Nothing that keeps a path inside the destination changes -
  `..`, separators, control characters and the depth and length limits are refused under
  either setting.

- **`--no-prehash`, for a first copy.** Before offering a tree, the sender hashed every file
  under 1 GiB so the receiver could skip what it already had. On a folder being synchronised
  again that is the whole point. On a first copy into an empty destination it reads and
  hashes the entire dataset to establish that the receiver has nothing - on a 140 GB tree,
  ten minutes before a single byte moved. `--no-prehash` starts sending immediately; a later
  run then compares by size and timestamp instead of by content.

- **`--max-files`, on both sides.** It was a fixed million. A tree past it can now be sent by
  raising it, and the sender refuses its own oversized tree right after the scan, naming the
  flag - instead of the receiver ending the session a thousand manifest batches later.

### Changed

- **Hashing before a transfer uses every core it was given.** It ran in one loop on one
  thread while the rest of the machine sat idle; it now runs across `--cpu-jobs` threads.
  The read buffer is also sized to the file rather than a megabyte per file, which on a tree
  of millions of small files was minutes spent allocating and zeroing memory that hashed
  nothing.

- **A large tree says what it is doing.** The scan logs the file count as soon as the walk
  ends, says how many files it is about to hash, and reports progress while the file list is
  being offered. Previously a tree of two million files showed `0 B/0 B  0/0 files` for as
  long as it took, which is indistinguishable from a hang.

### Fixed

- **A live tree no longer fails the whole transfer.** A file that existed when the directory
  was listed and was gone a moment later - a server writing temporary files, an editor
  saving - aborted everything with `No such file or directory`. The scan now skips it, and
  a file that disappears between the scan and being opened is a recoverable error: the next
  attempt rescans, does not offer it, and skips everything already transferred. Only a
  missing file is tolerated; a permission denied or a failing disk is still reported,
  because each of those is a file the user asked to send and will not get.

- **A name that cannot cross no longer fails the whole transfer.** `?`, `*`, `:`, a trailing
  dot and the Windows device names are ordinary on Linux and impossible on Windows, and one
  of them anywhere in a tree aborted everything. Those files are now left out and listed at
  the end, under `not sent`, with the path and the rule each one broke - so the transfer
  finishes and the user can still see exactly what did not go. A single file named on the
  command line is still a refusal: there the user pointed at that one file and nothing else.

### Known, not yet fixed

- **The whole file list is held in memory before anything is offered**, at roughly 1.5 KB per
  file: 2.5 million files cost about 3.8 GB on the sending side, and the memory budget does
  not cover it - `--mem-budget-mb` sizes the transfer buffers, nothing else. Splitting a tree
  that large into several transfers is the workaround; a streaming manifest is the fix, and
  it is a redesign rather than a flag.

- **The file list is offered one batch at a time**, each waiting for its answer before the
  next goes out. At 2,000 files per batch, a million files is 500 round trips before the
  first byte moves - a second on a LAN, a minute on a link with 100 ms of latency.

- **Throughput is capped by QUIC's default flow-control windows**, not by the link. quinn
  sizes them for a 100 ms round trip and 12.5 MB/s per stream: 1.25 MB per stream, 10 MB
  per connection. On a fast link that is the ceiling - roughly `streams × 1.25 MB ÷ RTT`,
  and never more than `10 MB ÷ RTT` in total. Raising `--streams` and `--disk-read-jobs`
  works around the first; the second needs the windows to be sized from `--mem-budget-mb`.
- **`--disk-read-jobs` silently caps `--streams`.** Its permit is held for a whole file, so
  the default of 4 means four files move at once however many streams were asked for. It is
  the right default for a spinning disk and the wrong one for anything else.

## 0.1.0 - 2026-08-14

First usable release. Peer-to-peer file and folder transfer over QUIC, verified end to end.

### What it does

- **Sends a folder over one QUIC connection**, a file per stream, encrypted by TLS 1.3.
  `spd send <path> <address>` and `spd recv --out <dir>`.
- **Pairs before anything moves.** The receiver shows a ten-character code; the sender types
  it. Each side proves it knows the code over material exported from the TLS session, so the
  proof is worthless in any other session and a relay in the middle satisfies neither end.
  `--insecure` skips it, warns, and has to be chosen by both sides.
- **Sends only what changed.** A parallel walk, a hash cache keyed by `(path, size, mtime)`,
  a batched manifest, and a per-file `Skip`/`Need` answer. `--dry-run` runs the whole
  dialogue and prints the outcome without opening a data stream.
- **Verifies every file.** BLAKE3 over the whole file on both sides; bytes land in a `.part`
  file and the rename is the commit, so a failed transfer leaves the previous version
  untouched.
- **Continues where it stopped.** A journal under `.spd` records what each `.part` file was
  started for, so an interrupted run resumes at a byte offset instead of starting over -
  and a `.part` from a different version of the file is thrown away rather than resumed.
  Both sides reconnect with backoff, `--attempts` times.
- **Compresses what is worth compressing**, decided per file: an extension that already
  means compressed settles it without opening the file, anything else is decided by
  compressing a 256 KiB sample. The decision travels in the stream header, so the receiver
  obeys what the sender did. `--no-compress` turns it off.
- **Moves files in parallel under a memory budget.** `--mem-budget-mb` is the input and the
  stream count follows from it; `--streams`, `--disk-read-jobs`, `--disk-write-jobs` and
  `--cpu-jobs` bound the rest.
- **Says what it is doing.** A progress bar while it runs, `--stats` when it ends, and
  `--limit-rate-mb` to leave the link usable for everything else on it.

### Measured

- 480 MiB across 60 files over loopback: 5.7 s with one stream, 2.4 s with four, flat after
  that. Peak RSS 20 MiB at `--mem-budget-mb 8`, 67 MiB at 256.
- 248 files of source, 1.8 MiB, crossed as 663 KiB. A 57 MiB mp4 crossed whole in 0.99 s.
- 220 MB with the sender killed at 17.7 MB: the next run moved exactly the missing
  192.9 MiB and the file was byte-identical.
- 19 MiB with `--limit-rate-mb 10`: 1.8 s at a reported 10.3 MiB/s.

### Known limitations

- **The pairing proof is a PSK with channel binding, not a PAKE.** Someone who records a
  session can guess codes against it offline. That is why the code carries 50 bits rather
  than the four digits a PAKE would make safe.
- **No LAN discovery.** Addresses are typed. It was the one optional item that would have
  added a listening surface rather than removed a rough edge.
- **One receiver per destination directory.** Two `spd recv` processes writing into the same
  folder would each keep their own journal, and neither would be right.
- **A dead peer is noticed on the idle timeout**, 60 seconds by default. A process killed
  mid-transfer does not tell the other side, and QUIC has nothing to notice until then.
- **No pull mode.** The sender is always the side that dials.

### Not yet

Continuous sync, bidirectional sync, intra-file delta (rsync-style rolling hash), NAT
traversal, multiple peers in one session, ACLs and extended metadata. All deliberately out
of scope for v1; see `PLAN.md §14`.
