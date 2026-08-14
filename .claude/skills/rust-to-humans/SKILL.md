---
name: rust-to-humans
description: Write Rust humans can read — naming, doc comments, user-facing error messages, CLI output, and plain-English explanations of Rust code. Use when naming items, writing docs/comments, wording error messages, or explaining what a piece of Rust does.
---

# Rust to Humans (spd-transfer)

Code is read far more often than written. Two fronts: the Rust the next human reads, and
the text the end user reads.

## 1. Names state the role, not the type

| Bad | Good | Why |
|---|---|---|
| `data`, `buf2`, `tmp` | `chunk`, `staged_bytes` | says what it carries |
| `process()` | `verify_and_rename()` | says what it does, in the order it does it |
| `flag` | `compressed` | a bool is named for its true state |
| `n` | `bytes_on_disk` | unit in the name |
| `handle_file()` | `send_file()` / `receive_file()` | direction is explicit |

Never name a bool negatively: `is_valid`, not `is_not_invalid`.
Units always in the name: `timeout_ms`, `mem_budget_bytes`, `max_frame_len`.
Function = verb. Type = noun. Module = an area, not "utils".
No `utils.rs`, `helpers.rs`, `common.rs` — if you cannot tell where it goes, the design
has not decided yet.

## 2. A doc comment answers what the code cannot

```rust
/// Resolves this relative path under `root`, guaranteeing the result stays
/// inside `root`.
///
/// Canonicalizes the parent directory before comparing, so a symlink pointing
/// outside is rejected here — not later, at write time.
///
/// # Errors
/// [`PathError::Escape`] if the resolved path leaves `root`.
/// [`PathError::MissingParent`] if the parent directory does not exist.
pub fn resolve_under(&self, root: &Path) -> Result<PathBuf, PathError>
```

Rules:
- First line: one sentence, imperative, what the function delivers.
- Then: the **why** and the traps. Restating the signature in prose is noise.
- `# Errors` on every public function returning `Result` (clippy enforces the exact
  heading spelling).
- `# Panics` only when it really can panic (in the core, almost never).
- A `///` example when the API is not obvious — it runs under `cargo test`, so it is also
  a test.

A comment inside the body explains a **decision**, not mechanics:

```rust
// BAD: increment the offset
offset += n;

// GOOD: the receiver rehashes the on-disk prefix because hasher state is not
// serializable across runs; costs one read, avoids a divergent hash.
```

## 3. An error message is user interface

Formula: **what failed · with which value · what to do**.

```
BAD:   Error: InvalidInput
BAD:   thread 'main' panicked at src/main.rs:42

GOOD:  rejected path: ".." component in "docs/../../etc/passwd"
       the peer sent a path that escapes the destination folder; nothing was written

GOOD:  manifest exceeds the limit: 4.2 MB > 4.0 MB (max_frame_len)
       raise it with --max-frame-len or shrink the batch with --batch-size
```

- No internal jargon leaking untranslated (`postcard decode failed at offset 12`).
- A security error states explicitly that **nothing was written**.
- Never "unknown error". If you got there, the enum is missing a variant.
- Human-readable units in the UI (`4.2 MB`), raw numbers in JSON logs.

## 4. CLI output

- Default: quiet on success, one progress bar, a summary at the end.
- `--verbose` adds detail; nobody should have to read the source to parse the output.
- A security warning (`--insecure`) is visible and on its own line, not buried.
- Errors go to stderr; data another program consumes goes to stdout.
- `--help`: every flag gets one line stating its effect and its default.

## 5. Explaining Rust to humans (prose, PRs, reviews)

Order: **what changes for the caller → how it works → why this way**.

- Translate the type: "`Result<T, ProtoError>`" → "can fail on an incompatible version or
  an oversized frame; the caller decides".
- Translate ownership: "`&mut self` here" → "only one side can write at a time, which is
  why the state has a single owner".
- Translate lifetimes: "`'a` ties the buffer to the session" → "the buffer does not
  outlive the connection".
- One short analogy is allowed, once; after that use the precise term.
- No "simply", "just", "trivially" — if it were, nobody would be asking.

## 6. Commits and PRs

Imperative subject, ≤ 50 chars, Conventional Commits scope.
Body only when the *why* is not obvious from the diff — and then it explains the rejected
alternative, not what the diff already shows.

```
fix(proto): reject file_id outside negotiated manifest

A peer could open a stream with an arbitrary id and create a file that was never
in the agreed list. The id is now looked up in the manifest and the stream is
closed with ErrorCode::UnknownFile. Rejected alternative: accept and ignore — it
left the receiver spending disk on data nobody asked for.
```

## Checklist

- [ ] Does every name state role + unit?
- [ ] Does each public item's doc explain *why* instead of restating the signature?
- [ ] `# Errors` section present on every `pub fn -> Result`?
- [ ] Does the error message carry a concrete value and a next step?
- [ ] Does a security error say what did **not** happen?
- [ ] Do body comments explain decisions rather than mechanics?
- [ ] No module named `utils`/`helpers`/`common`?
