# spd-transfer

Peer-to-peer file and folder transfer over QUIC, in Rust. Design and phase list live in
`PLAN.md` - read it before proposing anything structural.

## Layout

| Crate | Role | Constraint |
|---|---|---|
| `spd-core` | protocol, transport, safety, pipeline | no UI dependency: never `clap`, `indicatif`, `anyhow`; errors are `thiserror` |
| `spd-cli` | binary `spd`: parsing, logging, wording | thin, zero protocol logic; `anyhow` allowed here |
| `spd-fuzz` | fuzz harnesses for the decoder and `SafeRelPath` | builds on stable so CI covers it; `spd-fuzz/fuzz` is the nightly cargo-fuzz project, outside the workspace |
| `xtask` | `cargo xtask ci` | dependency-free; the single definition of the pipeline |

## Commands

```
cargo xtask ci      # fmt check + clippy -D warnings + tests + docs + cargo-deny
cargo xtask fmt     # format in place
cargo xtask fuzz    # cargo-fuzz over the decoder (needs nightly); [target] to pick one
cargo run -p spd-cli -- doctor
```

CI runs `cargo xtask ci` on Linux, Windows and macOS, plus `cargo-deny` and a check that
`spd-core` pulled in no UI dependency.

`release.yml` runs after a green `ci` on `main` and publishes `v<DD>.<MM>.<YYYY>.<n>` -
the day it was published plus a counter that restarts each day, so the second release of
25 September 2026 is `v25.09.2026.1`. Binaries for Linux (glibc and static musl), Windows
and macOS. Several commits pushed together produce one release, for the last of them.

## Rules

Full list in `PLAN.md §13`. The ones that bite most often:

1. A guarantee is a type, not a convention. `SafeRelPath`, `NonZeroU32`, newtypes over bare
   `u64`.
2. No `unwrap`/`expect`/`panic` in `spd-core` outside tests. A proven invariant gets an
   `// INVARIANT:` comment.
3. Mutable state has one owner. The pattern is an actor plus channels, not
   `Arc<Mutex<...>>`.
4. Every limit lives in `safety/limits.rs`. No magic constants on the transfer path.
5. No dead code, no crate outside CI, one way to do each thing.
6. A new public function without a test is not done.

Two skills carry the detail and apply to every change here:
`.claude/skills/rust-clean-code` (structure) and `.claude/skills/rust-to-humans` (naming,
docs, error wording). Load them when writing or reviewing Rust.

## Where things are written down

- `PLAN.md` - the design, the phases, and what each one actually shipped.
- `docs/PROTOCOL.md` - the wire format, for someone implementing the other end.
- `CHANGELOG.md` - what a release does, measured, with its known limitations.
- `spd-core/tests/golden/` - the wire format in bytes. Regenerate on purpose only:
  `SPD_UPDATE_GOLDEN=1 cargo test -p spd-core --test interop`.

## Phase status

F0-F7 are done: limits, negotiation, memory budget, CI, `xtask`, the QUIC transport,
verified transfers (`SafeRelPath`, `.part` plus atomic rename), folder sync with a hash
cache, parallel transfers (work queue, buffer pool, `--streams`, `--mem-budget-mb`),
resume (state actor over a journal in `.spd`, offset resume, reconnection with backoff,
`--attempts`), zstd compression decided per file (`--no-compress`, `--cpu-jobs`), pairing
(`--code`, proof bound to the TLS session; `--insecure` still exists and still warns), the
finishing pass (progress bar, `--stats`, `--limit-rate-mb`) and hardening (property tests,
frozen wire format, benchmarks, protocol documentation).

That is version 0.1.0. Anything beyond it is a new phase in `PLAN.md` before it is code,
and each phase still ends with green CI and a usable binary.

## Editing note (Windows)

Do not round-trip source files through PowerShell 5.1 `Get-Content`/`Set-Content`: it
decodes UTF-8 as ANSI and writes a BOM back, corrupting non-ASCII characters. Use the
editing tools directly.
