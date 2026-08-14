# spd

Peer-to-peer file and folder transfer over QUIC. One connection, one stream per file,
resume by byte offset, BLAKE3 verification end to end.

**Status: early.** Phases F0-F7 are done: it syncs a folder in parallel, verified end to end,
sending only what changed, compressing what is worth compressing, resuming an interrupted
transfer where it stopped, and refusing anyone who cannot produce the pairing code. Progress
output and a bandwidth limit are still ahead. See [`PLAN.md`](PLAN.md).

## Build

```sh
cargo build --workspace
cargo xtask ci        # what CI runs: fmt, clippy -D warnings, tests, docs, cargo-deny
```

## Try what exists

Send one file between two terminals:

```sh
# receiver - shows a pairing code, then waits
spd recv --out ./inbox --listen 0.0.0.0:9432
#   pairing
#     code                     A1B2C-D3E4F

# sender - type the code the receiver is showing
spd send ./holiday.mp4 192.168.1.20:9432 --code A1B2C-D3E4F
spd send ./photos 192.168.1.20:9432 --code a1b2c-d3e4f --dry-run   # what would move
```

The code is what proves the other machine is the one showing it. The proof is tied to that
particular encrypted session, so a relay in the middle cannot pass one side's answer to the
other. Wrong code, no session, nothing written.

`--insecure` skips all of that and accepts any peer that can reach the port. Both sides have
to pass it: if one is pairing and the other is not, the session is refused rather than
quietly downgraded.

Pull the cable and both sides try again on their own, up to `--attempts` times: the files
already there are skipped and the one that was in flight continues from the byte it reached.
Run the same command again later and it does the same thing.

Bodies are compressed when that is worth doing, decided per file: a folder of source or
logs crosses at a fraction of its size, a folder of video is not even sampled. `--no-compress`
turns it off for a link fast enough that the processor is the scarce thing.

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
- `spd-fuzz` - fuzz harnesses for the two places a peer's raw bytes are parsed.
- `xtask` - `cargo xtask ci`, the single definition of the pipeline.

## Licence

MIT OR Apache-2.0.
