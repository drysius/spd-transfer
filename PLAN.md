# Project plan - `spd`

Peer-to-peer file and folder transfer, written from scratch in Rust.
Reference document for this repository; self-contained.

**Status:** F6 (compression) done. F7 (security) next.
**Last updated:** 2026-08-14

---

## 0. Decision summary

| Axis | Decision | Rejected alternatives |
|---|---|---|
| Transport | **QUIC** via `quinn`, 1 connection + N streams | TCP with hand-rolled framing; TCP+TLS, many connections |
| Transfer unit | **one stream per file**, raw byte body | application-level chunking with ACKs and a sliding window |
| Resume | **byte offset** per file | chunk bitmap |
| Hash | **BLAKE3** | SHA-256 |
| Sync model | **rsync-style push** (sender sends the delta) | bidirectional push+pull; continuous watch-based sync |
| Security | always encrypted (QUIC's TLS 1.3); pairing-based authentication, `--insecure` escape for LAN | plaintext; no LAN escape hatch |
| Interface | **CLI only** (`spd-core` library + `spd` binary) | simultaneous CLI+GUI; daemon |
| State | **single actor** owns the state, append-only persistence | JSON file shared between tasks |

### On `--insecure`

QUIC mandates TLS 1.3 by specification - there is no plaintext QUIC. So `--insecure`
means **no peer authentication** (accepts a self-signed certificate, no pairing code),
**not** "no encryption".

That is acceptable: AES-NI and ChaCha20 sustain 2-6 GB/s per core. On a 1-10 Gbps network
the bottleneck is the disk, not the cipher. Practical consequence: there is no duplicated
"encrypted/plaintext" code path - one path, with a variable certificate-verification
policy.

---

## 1. Context: what we are fixing

This plan grew out of reviewing a previous project (`P2PFileTransfer`, ~11.6k lines). The
problems found there define the design constraints here.

| Problem in the previous project | How this design prevents it |
|---|---|
| A chunk written and hashed twice, producing a divergent SHA | No application chunks; a QUIC stream is ordered and duplicate-free |
| A chunk exceeding `max_retries` disappeared silently, causing an infinite loop | Retransmission belongs to QUIC; a stream failure is a typed error that propagates |
| Partial resume never activated (function never called, dead code) | Offset resume is the only path; without it there is no resume at all |
| Race on the state file with N parallel connections | State has a single owner (actor); nobody else touches it |
| SHA-256 computed in arrival order rather than file order | Ordered stream, therefore arrival order **is** file order |
| `output_dir.join(peer_string)` - path traversal | `SafeRelPath`: constructible only through validation |
| `vec![0u8; len]` with `len` from the peer (up to 128 MB) before authentication | Small `max_frame_length` in the codec; pooled buffers |
| A GUI crate outside CI rotted until it stopped compiling | Rule: nothing outside `--all-features` in CI |
| Rehashing the whole folder on every send | Hash cache keyed by `(path, size, mtime)` |
| `send_file` and `send_file_windowed` ~90% duplicated | One single send path |

### Worth keeping from the previous project

- Work queue with batched work-stealing (largest files first) - the distribution worked well.
- Compression decision by extension (list of already-compressed formats) plus adaptive sampling.
- Write to `.part`, then atomic rename.
- Capability bits negotiated during the handshake.
- Crate separation with a UI-free core.

---

## 2. Workspace layout

```
spd-transfer/
├── Cargo.toml                  # workspace
├── PLAN.md                     # this document
├── CLAUDE.md                   # working rules for this repo
├── spd-core/                   # pure library - no clap, no indicatif, no anyhow
│   └── src/
│       ├── lib.rs
│       ├── error.rs            # thiserror, typed errors
│       ├── proto/              # wire types, codec, versioning
│       │   ├── mod.rs
│       │   ├── messages.rs     # enum Control, data stream headers
│       │   ├── codec.rs        # LengthDelimitedCodec + postcard
│       │   └── version.rs      # version and feature negotiation
│       ├── transport/          # quinn
│       │   ├── mod.rs
│       │   ├── endpoint.rs     # bind, connect, TLS config
│       │   ├── tls.rs          # rcgen, custom verifier, pairing
│       │   └── session.rs      # connection + control stream + stream opening
│       ├── safety/
│       │   ├── mod.rs
│       │   ├── path.rs         # SafeRelPath
│       │   └── limits.rs       # every limit in one place
│       ├── scan/
│       │   ├── mod.rs
│       │   ├── walk.rs         # parallel jwalk
│       │   ├── hash_cache.rs   # (path, size, mtime) -> hash
│       │   └── manifest.rs     # construction and batching
│       ├── pipeline/
│       │   ├── mod.rs
│       │   ├── send.rs
│       │   ├── recv.rs
│       │   ├── budget.rs       # semaphores, memory budget
│       │   └── bufpool.rs
│       ├── state/
│       │   ├── mod.rs          # actor
│       │   ├── journal.rs      # append-only + snapshot
│       │   └── model.rs
│       ├── compress.rs
│       └── metrics.rs          # metrics/progress actor
├── spd-cli/                    # clap + indicatif; thin, zero protocol logic
│   └── src/
│       ├── main.rs
│       ├── args.rs
│       ├── doctor.rs
│       ├── send.rs
│       ├── recv.rs
│       └── ui.rs               # bars, formatting
├── spd-fuzz/                   # cargo-fuzz (lands in F7)
│   └── fuzz_targets/
│       ├── decode_control.rs
│       └── safe_path.rs
├── xtask/                      # cargo xtask ci - runs the pipeline locally
└── tests/                      # integration + fault injection
    ├── common/
    │   ├── harness.rs          # sender/receiver pair over loopback
    │   └── faults.rs           # connection cut, corruption, slowness
    ├── roundtrip.rs
    ├── resume.rs
    ├── sync_skip.rs
    └── path_safety.rs
```

**Dependency rule (checked by CI):** `spd-core` must not depend on `clap`, `indicatif`,
`anyhow` or anything UI-related. Core errors are typed with `thiserror`; `anyhow` exists
only in `spd-cli`.

---

## 3. Threading model

Three pools with roles that never mix:

```
┌─ Tokio (multi_thread, worker_threads = n_cores) ──────────┐
│  network tasks: one per active stream, + control + actors │  never blocks
└───────────────────────────────────────────────────────────┘
┌─ Rayon ───────────────────────────────────────────────────┐
│  CPU: BLAKE3, zstd encode/decode                          │  never does I/O
└───────────────────────────────────────────────────────────┘
┌─ spawn_blocking (dedicated pool) ─────────────────────────┐
│  disk: read, write, fsync, rename                         │  never does heavy CPU
└───────────────────────────────────────────────────────────┘
```

### Per-file pipeline

```
SEND
  disk.read ──buf──▶ [zstd (rayon)] ──buf──▶ quinn.write
       └───────────────────────────▶ incremental blake3 (rayon)

RECEIVE
  quinn.read ──buf──▶ [zstd (rayon)] ──buf──▶ disk.write
                              └──────────────▶ incremental blake3 (rayon)
```

Stages are wired with `tokio::sync::mpsc` channels of **bounded capacity** (2-4 buffers).
Backpressure follows: a full channel stops the disk reader, so memory stops growing.

### Memory budget

`--mem-budget` (default ~256 MB) is the input; concurrency is derived from it, never the
other way around:

```
concurrent_streams = clamp(mem_budget / (buf_size * pipeline_depth), 1, quic_limit)
```

### Semaphores (independent, configurable)

| Semaphore | Default | Why it is separate |
|---|---|---|
| `disk_read` | 4 | an HDD degrades under high concurrency; an NVMe benefits |
| `disk_write` | 4 | writing costs more than reading |
| `cpu` | n_cores | bounds the queue handed to rayon |
| `net_streams` | 16 | QUIC negotiates a concurrent stream limit |

Exposed as `--disk-read-jobs`, `--disk-write-jobs`, `--streams`. No magic HDD-vs-SSD
autodetection - explicit flag, conservative default.

### Buffer pool

Buffers are reused (`bytes::BytesMut` with a dedicated pool or slab). Zero allocations per
block on the hot path. Data is born in a buffer, processed there, and returned to the pool.

### Work queue

Files are ordered largest-first in a shared queue; workers pull batches (bounded by bytes
**and** by file count, so nobody monopolises thousands of small files). Each worker runs
the pipeline above on its own stream.

---

## 4. Protocol

### Control stream

Bidirectional, opened right after the handshake, lives for the whole session.

```rust
enum Control {
    Hello       { version: u16, features: u64, device: [u8; 32] },
    HelloAck    { version: u16, features: u64, device: [u8; 32] },

    Manifest    { batch_seq: u32, last: bool, entries: Vec<Entry> },
    SyncReply   { batch_seq: u32, decisions: Vec<Decision> },

    FileDone    { file_id: u64, hash: [u8; 32] },   // sender: done, here is the hash
    FileVerdict { file_id: u64, ok: bool },         // receiver: matches / does not match

    Done        { files: u64, bytes: u64 },
    Error       { code: ErrorCode, msg: String },
}

struct Entry {
    file_id: u64,
    path: Vec<String>,      // components, NOT a separator-joined string
    size: u64,
    mtime: u64,
    mode: u32,              // platform-relevant bits
    hash: Option<[u8; 32]>, // absent for large files / cache misses
}

enum Decision {
    Skip,
    Need { file_id: u64, from_offset: u64 },
}
```

### Data streams

Unidirectional, one per file. Short header, raw body, end of stream = end of file.

```rust
struct DataHeader {
    file_id: u64,
    offset: u64,        // where this stream starts (resume)
    compressed: bool,
}
```

No per-block framing. No per-block CRC - QUIC's AEAD already guarantees transport
integrity, and the per-file BLAKE3 covers disk errors and application bugs, which is where
it adds value.

### Codec rules

- Serialisation: `postcard` (compact, fast, stable) over `serde`.
- Control framing: `tokio_util::codec::LengthDelimitedCodec` with a small
  `max_frame_length` (4 MB). Never allocate a peer-supplied size without a ceiling.
- Explicit `version: u16` + `features: u64` envelope. A new field arrives as a feature bit;
  an older peer ignores it safely.
- The manifest is **always** batched (e.g. 2000 entries per message), regardless of folder
  size.
- A `file_id` arriving on a data stream must exist in the negotiated manifest and be marked
  `Need`. Otherwise: error, not warning.

---

## 5. Security

### Paths - the type carries the guarantee

```rust
pub struct SafeRelPath(PathBuf);   // private constructor

impl SafeRelPath {
    pub fn from_components(parts: &[String]) -> Result<Self, PathError>;
    pub fn resolve_under(&self, root: &Path) -> Result<PathBuf, PathError>;
}
```

The constructor rejects:

- empty components, `.` and `..`
- a separator (`/` or `\`) inside a component
- an absolute prefix, a drive prefix (`C:`), UNC (`\\server\share`), `\\?\`
- Windows reserved names: `CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9`, `LPT1`-`LPT9` (with or
  without an extension)
- a trailing dot or space in any component (Windows strips them silently)
- control characters (`\0`-`\x1F`) and, on Windows, `<>:"|?*`
- depth above the limit; total length above the limit

`resolve_under` canonicalises the parent directory and asserts `starts_with(root)` before
returning. Covered by proptest and by a fuzz target.

### Symlinks

Default: ignored during the scan. `--follow-links` to follow them when reading. **Never**
create a symlink from the manifest without an explicit `--links` - otherwise it is
out-of-root writing through another door.

### Authentication

- **Default:** the receiver shows a short pairing code; the sender types it. The code
  derives the session key (PAKE, or a simple PSK in v1 with a planned upgrade). No correct
  code, no session.
- **`--insecure`:** accepts a self-signed certificate with no pairing. Prints a visible
  warning. Never the silent behaviour.
- **Optional TOFU (later phase):** persistent keypair per peer, fingerprint confirmation on
  first connection, automatic reconnection afterwards.

### Limits (all in `safety/limits.rs`, all configurable)

`max_frame_len_bytes` · `max_manifest_entries` · `max_path_depth` · `max_path_len_bytes` ·
`max_concurrent_streams` · `handshake_timeout` · `idle_timeout` · `max_file_size_bytes`

---

## 6. State and resume

### State actor

One task owns the state. Nobody else opens the file. Communication goes over a channel:

```rust
enum Request {
    Partial { path: SafeRelPath, reply: oneshot::Sender<Option<Expected>> },
    Started { path: SafeRelPath, expected: Expected, reply: oneshot::Sender<Result<()>> },
    Forget  { path: SafeRelPath, reply: oneshot::Sender<Result<()>> },
}
```

This makes the previous project's race impossible by construction rather than by
discipline.

Keyed by path, not by `file_id`: a file id is assigned by the sender and means nothing in
the next session, which is exactly the session the record exists for. And there is no
progress message - `metadata().len()` already knows how far a `.part` file got, so a second
answer to that question could only ever disagree with the first.

### Persistence

Append-only journal (`.spd/journal`) plus a periodic snapshot written to a temporary file
and moved with an atomic rename. No C dependency, no file lock, no concurrent writing. On
open: load the snapshot, replay the journal, compact.

### Offset resume

1. The receiver knows `bytes_on_disk` for the `.part` file (confirmed with
   `metadata().len()`, not just the journal) and, from the journal, what that file was
   started for. A record that no longer matches the offer means the bytes are not a prefix
   of anything being sent, and the transfer starts again at zero.
2. Answering the manifest, it replies `Need { from_offset }`.
3. The sender seeks to `offset` and opens the stream carrying that offset in the header.
   A stream arriving at any other offset is refused: only the receiver decides where a file
   resumes.
4. Both sides feed BLAKE3 in file order, each re-reading the prefix it already has.
   Storing hasher state instead was the plan; `blake3::Hasher` cannot be serialised, so
   re-reading is what there is.

Because the stream is ordered, what is on disk is always a valid prefix. No bitmap, no hole
in the middle, no divergent arrival order.

---

## 7. Scan, manifest and diff

- Parallel walk with `jwalk` (rayon underneath).
- **Local hash cache** in `.spd/hashcache`, keyed by `(path, size, mtime)` - plus
  `inode`/`file_index` where available. Avoids rehashing the whole folder on every send,
  which was the previous project's largest cost.
- Files above a threshold (e.g. 1 GB) enter the manifest without a hash (`hash: None`); the
  receiver decides on `size + mtime`. With `--checksum`, everything is hashed.
- Receiver decision, per entry:
  - different `size` -> `Need { from_offset: bytes_on_disk }`
  - same `size` and a hash on both sides -> compare hashes -> `Skip` or `Need`
  - same `size`, no hash -> compare `mtime` -> `Skip` or `Need`
  - file on disk but missing from the journal, sender has a hash -> hash locally and compare
    (recovers from a deleted journal)
- `--dry-run` runs exactly this dialogue and prints the outcome without opening a single
  data stream.

---

## 8. Compression

- zstd **streaming per file** (`zstd::stream::Encoder` over the pipeline), not per block.
- The decision is made once per file, before opening the stream:
  1. extension in the already-compressed list (`zip`, `mp4`, `jpg`, `7z`, `pdf`, `docx`, …)
     -> do not compress;
  2. otherwise sample the first ~256 KB; if the ratio is below ~1.05, do not compress.
- The decision travels in `DataHeader.compressed` - the receiver obeys the flag, never the
  global configuration. (Classic bug: receiver deciding by config while the sender decides
  per file.)
- Compression and decompression run on the CPU pool, never on a network task.

---

## 9. Observability

- `tracing` throughout the core; `tracing-subscriber` in the CLI, with
  `--log-format=text|json`.
- A metrics actor aggregates atomic counters; the UI reads snapshots. No progress bar
  wandering through transfer logic (the previous project threaded `&mut ProgressState`
  through six signatures).
- By default: one global bar. `--verbose`: one bar per stream.
- `--stats` at the end: files, bytes read/sent, compression ratio, network vs disk
  throughput, time per phase (scan, hash, transfer), streams and retries.

---

## 10. Phases

Every phase ends with green CI and a usable binary. No "I will finish it next phase".

### F0 - Skeleton — **done**
Workspace, error types, `tracing`, `xtask ci`.
Full CI **from the first commit**: `fmt --check`,
`clippy --all-targets --all-features -D warnings`, `test --all-features`, `cargo-deny`,
Linux/Windows/macOS matrix.
**Done when:** `cargo xtask ci` reproduces the pipeline locally and CI is green.
Shipped: `Limits` + validation, version/feature negotiation, memory-budget arithmetic,
`spd doctor`.

### F1 - Transport - **done**
`quinn` + `rustls` + `rcgen`. Client/server endpoint, `Hello`/`HelloAck` handshake, control
stream codec, version and feature negotiation.
**Done when:** an integration test connects over loopback, negotiates and exchanges control
messages; an incompatible version is rejected with a typed error.
Shipped: ALPN `spd/1`, TLS 1.3 with `TrustPolicy`, typed `ControlChannel`, data-stream
headers bounded before allocation, handshake timeouts, and a refusal that reaches the peer
carrying its reason.

### F2 - One file - **done**
Sending and receiving a single file. No compression, no parallelism. `.part` -> BLAKE3 ->
end-to-end verification -> atomic rename.
**Done when:** a 1 GB file crosses with a matching hash; an injected corrupt byte is
detected and reported.
Shipped: `SafeRelPath` (validated before anything touches the filesystem), `spd send` /
`spd recv`, `.part` plus atomic rename, a graceful close so the final message is not
discarded, and `--insecure` as a required, explicit choice.

### F3 - Manifest and diff - **done**
Parallel walk, hash cache, batched manifest, `Skip`/`Need`, `--dry-run`.
**Done when:** a second run over an identical folder transfers 0 bytes; changing one file
transfers only that file; `--dry-run` matches what the real run does.
Shipped: parallel walk skipping `.spd`, persistent hash cache, batched manifest with a
`Transfer` announcement so the receiver knows what is coming, `--dry-run`, `--checksum`,
`--follow-links`, and mode bits applied where the platform has them.

### F4 - Parallelism - **done**
Work queue, N streams, the four semaphores, bounded channels, buffer pool.
**Done when:** a benchmark scales with `--streams`; peak RSS respects `--mem-budget` under
load; no `unwrap` on the hot path.
Shipped: split control stream (one writer task, one reader task routing replies per file),
work queue with N workers, buffer pool sized by the budget, `disk_read`/`disk_write` job
limits, `--streams`, `--mem-budget-mb`.
Measured on 480 MiB across 60 files over loopback: 5.7 s with one stream, 2.4 s with four,
flat after that (disk bound). Peak RSS 20 MiB at `--mem-budget-mb 8` and 67 MiB at 256.
The `cpu` semaphore is not here: nothing runs on rayon yet. It arrives with zstd in F6,
where there is finally CPU work to bound.

### F5 - Resume - **done**
State actor, journal + snapshot, offset resume, reconnection with backoff.
**Done when:** the fault harness kills the connection at 100 random points and the final
tree is byte-identical all 100 times.
Shipped: a state actor owning an append-only journal plus snapshot under `.spd`, written
lazily so a run with nothing to record leaves the destination untouched; `Expected`, which
is what lets a `.part` file be recognised as belonging to this same offer rather than to an
older version of it; offset resume through `Decision::Need { from_offset }` with both sides
hashing the prefix they already share; `RetryPolicy` with reconnection and backoff on both
sides, and `--attempts`.
The acceptance run is split in two: `tests/resume.rs` cuts a live connection mid-file and
finishes the tree on the next run, then resumes the same file from a hundred different
offsets in turn - deterministic rather than random, so a failure names the offset that
broke instead of a seed.
Two things the plan expected are deliberately not here. Hasher state is not stored in the
journal: `blake3::Hasher` cannot be serialised, so both sides re-read the prefix instead.
And progress is not journalled per block - `metadata().len()` already knows how far a
`.part` file got, and a second answer to that question could only ever disagree with the
first.

### F6 - Compression - **done**
zstd streaming, extension + sample decision, header flag.
**Done when:** a text folder compresses; an mp4/zip folder passes through with no
measurable CPU cost; the receiver honours the per-file flag.
Shipped: `compress.rs` - the extension list, the sample, and thin streaming wrappers around
zstd that turn bytes into bytes and nothing else; the codec running on rayon behind the
`cpu` semaphore promised in F4, with `--cpu-jobs`; `--no-compress`; and `wire_bytes` on
`TransferSummary`, which is what makes "did it compress" a number rather than an opinion.
Measured: 248 files of source, 1.8 MiB, crossed as 663 KiB. A 57 MiB mp4 crossed whole in
0.99 s, and the same bytes named `.dat` - so the sample runs rather than the extension
list - in 0.90 s.
The uncompressed path is unchanged and stays copy-free: a body crossing as it is gets
hashed where it was read and written from there. Only compression adds a second buffer.

### F7 - Security
Pairing, `--insecure` with a warning, all limits enforced, fuzzing for the decoder and
`SafeRelPath`, path traversal suite (including the Windows cases).
**Done when:** fuzzing runs for 1 h without a crash; every traversal vector is rejected; an
unpaired peer writes nothing to disk.

### F8 - Finishing
Progress bars, `--stats`, bandwidth limit (token bucket), LAN discovery (optional), readable
error messages, complete `--help`.

### F9 - Hardening
Property tests, `criterion` benchmarks, cross-version interop test (golden wire-format
files), protocol documentation, release notes.

---

## 11. Test strategy

The previous project's weak point; here it is a per-phase requirement.

- **Unit:** path sanitiser, codec/framing, manifest diff, resume offset arithmetic,
  compression decision.
- **Property (`proptest`):** for any file set and any interruption point, resume produces a
  byte-identical tree. For any input, `SafeRelPath` either rejects or resolves inside the
  root.
- **Integration:** in-process sender/receiver pair, QUIC over loopback, with a fault
  injection layer: cut the connection after N bytes, corrupt the payload, add latency, fill
  the disk, deny permission.
- **Fuzz (`cargo-fuzz`):** control decoder and `SafeRelPath`.
- **Interop:** golden wire-format files versioned in the repo, so an accidental protocol
  change breaks a test.
- **Bench (`criterion`):** throughput by stream count, hash cost, compression cost, scan
  time with a warm and a cold cache.

---

## 12. Dependencies

`quinn` · `rustls` · `rcgen` · `tokio` · `tokio-util` · `bytes` · `serde` · `postcard` ·
`blake3` · `zstd` · `jwalk` · `rayon` · `tracing` · `tracing-subscriber` · `thiserror` ·
`clap` · `indicatif` · `proptest` · `criterion` · `cargo-deny` · `cargo-fuzz`

`anyhow` only in `spd-cli`.

---

## 13. Project rules

1. **Nothing outside CI.** Every crate is covered by `cargo check --all-features`. If it
   does not build in CI, it does not exist.
2. **No dead code.** `dead_code` is a warning treated as an error. A function with no caller
   is deleted, not commented out.
3. **Safety is a type, not a convention.** If the guarantee depends on someone remembering
   to call a function, the design is wrong.
4. **Mutable state has a single owner.** Needing a `Mutex` on the hot path signals a wrong
   design.
5. **Every phase ships a working binary.**
6. **Failure tests before optimisation.** Interruption, corruption, full disk, slow peer,
   malicious peer.
7. **No `unwrap`/`expect` in the core** outside proven invariants, commented on the spot.
8. **One way to do each thing.** If a second send path appears, one of them goes.

---

## 14. Out of scope (v1)

Written down so it does not become scope by accident:

- GUI
- Continuous sync with directory watching
- Bidirectional sync with conflict resolution
- Intra-file rsync-style delta (rolling hash) - v1 resumes by offset, not by block
- NAT traversal / relay / hole punching
- Multiple simultaneous peers in one session
- ACL and extended metadata preservation beyond `mode` and `mtime`
