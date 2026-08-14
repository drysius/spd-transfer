# Changelog

Kept by hand, in the order a reader cares about rather than the order the commits landed.
Dates are the day the work was finished.

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
