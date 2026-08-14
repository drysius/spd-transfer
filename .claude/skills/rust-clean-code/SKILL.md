---
name: rust-clean-code
description: Clean Rust standards for spd-transfer — types that carry guarantees, typed errors, single-owner mutable state, zero dead code, no unwrap in the core. Use when writing, reviewing, or refactoring any Rust in this repository.
---

# Rust Clean Code (spd-transfer)

Writing and review rules. Each one exists because its absence caused a real bug in the
previous project (`P2PFileTransfer`). See `PLAN.md §1`.

## 1. The guarantee lives in the type, not in discipline

If correctness depends on someone remembering to call a function, the design is wrong.

```rust
// BAD — validation is optional, therefore forgettable
fn write_file(root: &Path, rel: &str) { root.join(rel) }   // path traversal

// GOOD — an unvalidated path cannot be constructed
pub struct SafeRelPath(PathBuf);            // private field, no `pub`
impl SafeRelPath {
    pub fn from_components(parts: &[String]) -> Result<Self, PathError> { /* validates */ }
}
fn write_file(root: &Path, rel: &SafeRelPath) { /* already safe */ }
```

Apply to: paths, offsets, negotiated ids, limits, session states.
Newtype > comment. `NonZeroU64` > a `u64` that "is never zero".
If two `u64` arguments can be swapped at a call site, they are two newtypes.

## 2. Errors are typed in the core, contextual in the CLI

- `spd-core`: `thiserror`, one enum per layer, specific variants. Never `anyhow`.
- `spd-cli`: `anyhow` with `.context()` for the message a human reads.
- `Result<T, E>` propagates. An error that "cannot happen" still propagates — it does not
  become an `unwrap`.
- An error never disappears into a `warn!` and continues. Either handle it or propagate it.

```rust
#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("frame of {got} bytes exceeds the {max} byte limit")]
    FrameTooLarge { got: usize, max: usize },
    #[error("peer protocol version {peer} is incompatible with ours ({ours})")]
    VersionMismatch { peer: u16, ours: u16 },
}
```

An error message states **what**, **how much**, and **which limit** — never "internal error".

## 3. No `unwrap` / `expect` / `panic!` in the core

`#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::panic))]`
is already in `spd-core/src/lib.rs`. Single exception: a proven invariant, with a
`// INVARIANT:` comment explaining why it cannot fail. Tests are exempt.

Arithmetic that can overflow on the hot path uses `checked_*` / `saturating_*`, never
release-mode wrapping.

## 4. Mutable state has exactly one owner

Needing a `Mutex` on the hot path means the design is wrong. Repo pattern: **actor**.
One task owns the state; everyone else talks over `mpsc`; reads reply over `oneshot`.

```rust
enum StateMsg {
    Recorded { file_id: FileId, hash: [u8; 32] },
    Query    { path: SafeRelPath, reply: oneshot::Sender<Option<Record>> },
}
```

`Arc<AtomicU64>` for metric counters is fine (no invariant between them).
`Arc<Mutex<HashMap>>` shared across N connections is not.

## 5. One way to do each thing

If a second send path appears, one of the two dies in the same commit.
Two ~90%-identical functions become one function with a parameter — or two genuinely
different functions whose names explain the difference.

## 6. No dead code

`dead_code` is an error. A function with no caller is deleted, not commented out, not
`#[allow]`ed. Code "for a future phase" lands in that future phase.
No crate outside CI — what does not build under `--all-features` rots.

## 7. Layer boundaries

```
heavy CPU  → rayon / spawn_blocking   never on a tokio task
disk I/O   → spawn_blocking           never mixed with heavy CPU
network    → tokio                    never blocks
```

A signature never carries an upper-layer dependency: no `&mut ProgressState` threaded
through six transfer functions. Progress is a message to the metrics actor.

## 8. Allocation and limits

- Never `vec![0; n]` with `n` coming from the peer. Bound first, allocate second.
- Hot path: pooled, reused buffers. Zero allocations per block.
- Every limit lives in `safety/limits.rs` and is configurable. A magic constant scattered
  through the code is a bug with a date on it.

## 9. Correct async

- An `async fn` doing heavy CPU blocks the whole executor — move it to rayon.
- Bounded channels (2–4). An unbounded channel is a memory leak with extra steps;
  backpressure is a feature, not an obstacle.
- With `select!` and cancellation, check that the cancelled future leaves consistent state
  (a half-written `.part` is fine; a half-written journal is not).
- No `std::thread::sleep` in async context.

## 10. Tests are part of "done"

A new public function without a test is not done. Preference order:
1. property test (`proptest`) when the invariant is universal;
2. unit test with a named edge case;
3. integration test with fault injection.

A test that calls the function and asserts `is_ok()` does not count.

## Review checklist

- [ ] Does any guarantee depend on remembering to call something? → make it a type
- [ ] `unwrap`/`expect`/`panic` outside tests?
- [ ] Error swallowed by a log line?
- [ ] Mutable state with more than one owner?
- [ ] A second path for the same thing?
- [ ] Allocation sized by peer-supplied data?
- [ ] Heavy CPU inside an async task?
- [ ] New public function without a test?
- [ ] Magic constant outside `limits.rs`?
