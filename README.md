# spd

Peer-to-peer file and folder transfer over QUIC. One connection, one stream per file,
resume by byte offset, BLAKE3 verification end to end.

**Status: early.** Phases F0-F1 are done - limits, version negotiation, memory-budget model,
CI, and the QUIC transport with its handshake. Moving files lands in F2. See
[`PLAN.md`](PLAN.md).

## Build

```sh
cargo build --workspace
cargo xtask ci        # what CI runs: fmt, clippy -D warnings, tests, docs, cargo-deny
```

## Try what exists

```sh
cargo run -p spd-cli -- doctor
cargo run -p spd-cli -- doctor --mem-budget-mb 64 --streams 8
```

`doctor` prints the effective limits and the concurrency derived from the memory budget,
without touching the network. Concurrency always follows the budget, never the reverse.

## Design in one paragraph

QUIC gives ordered, deduplicated, retransmitted, encrypted streams, so the application does
no chunking, no ACK windows and no per-block CRC. One file is one stream; what is on disk
is therefore always a valid prefix, which makes resume a single byte offset. State has one
owner - an actor with an append-only journal - so no lock is needed on the hot path. Paths
from a peer only exist as `SafeRelPath`, validated at construction. Full rationale, and the
list of previous-project bugs each decision closes, in [`PLAN.md`](PLAN.md).

## Layout

- `spd-core` - protocol, transport, safety, pipeline. No UI dependency.
- `spd-cli` - the `spd` binary. Parsing, logging, wording.
- `xtask` - `cargo xtask ci`, the single definition of the pipeline.

## Licence

MIT OR Apache-2.0.
