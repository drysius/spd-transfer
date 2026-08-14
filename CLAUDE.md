# spd-transfer

Peer-to-peer file and folder transfer over QUIC, in Rust. Design and phase list live in
`PLAN.md` - read it before proposing anything structural.

## Layout

| Crate | Role | Constraint |
|---|---|---|
| `spd-core` | protocol, transport, safety, pipeline | no UI dependency: never `clap`, `indicatif`, `anyhow`; errors are `thiserror` |
| `spd-cli` | binary `spd`: parsing, logging, wording | thin, zero protocol logic; `anyhow` allowed here |
| `xtask` | `cargo xtask ci` | dependency-free; the single definition of the pipeline |

## Commands

```
cargo xtask ci      # fmt check + clippy -D warnings + tests + docs + cargo-deny
cargo xtask fmt     # format in place
cargo run -p spd-cli -- doctor
```

CI runs `cargo xtask ci` on Linux, Windows and macOS, plus `cargo-deny` and a check that
`spd-core` pulled in no UI dependency.

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

## Phase status

F0-F5 are done: limits, negotiation, memory budget, CI, `xtask`, the QUIC transport,
verified transfers (`SafeRelPath`, `.part` plus atomic rename), folder sync with a hash
cache, parallel transfers (work queue, buffer pool, `--streams`, `--mem-budget-mb`) and
resume (state actor over a journal in `.spd`, offset resume, reconnection with backoff,
`--attempts`). F6 is compression - see `PLAN.md §10`.

Nothing lands for a future phase ahead of time; each phase ends with green CI and a usable
binary.

## Editing note (Windows)

Do not round-trip source files through PowerShell 5.1 `Get-Content`/`Set-Content`: it
decodes UTF-8 as ANSI and writes a BOM back, corrupting non-ASCII characters. Use the
editing tools directly.
