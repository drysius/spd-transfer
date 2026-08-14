# The spd wire protocol, version 1

What two peers say to each other, in the order they say it. Written for someone
implementing the other end, or working out why a session was refused.

The frozen encoding of every message lives in `spd-core/tests/golden/`; a change to this
document that is not also a change to those bytes is a documentation bug.

---

## 1. Transport

One QUIC connection, ALPN `spd/1`, TLS 1.3 (QUIC has no plaintext mode).

| Stream | Direction | Lifetime | Carries |
|---|---|---|---|
| control | bidirectional | the whole session | length-delimited `Control` frames |
| data | unidirectional, sender to receiver | one file | a `DataHeader`, then raw bytes |

The connecting side opens the control stream. Data streams are opened by the sender, one
per file, up to `max_concurrent_streams`.

**Framing.** Control frames are length-delimited (`tokio_util`'s `LengthDelimitedCodec`,
big-endian `u32` prefix) with a maximum frame length of `max_frame_len_bytes`, 4 MiB by
default. The body of a frame is a `Control` value encoded with
[postcard](https://docs.rs/postcard). A `DataHeader` is prefixed with its own `u16` length,
because it is read from a raw stream rather than a framed one.

**End of file is end of stream.** A data stream carries no length: the sender finishes the
stream, and that is the end of the file. There is nothing for a length field to disagree
with.

---

## 2. The conversation

```
caller                                   answerer
  |------------------ Hello ---------------->|   version, features, device
  |<---------------- HelloAck ---------------|   version, features, device
  |                                          |
  |------------------ Pair ----------------->|   only when both announced PAIRING
  |<---------------- PairAck ----------------|
  |                                          |
  |---------------- Manifest --------------->|   batch 0, entries
  |<--------------- SyncReply ---------------|   batch 0, one decision per entry
  |---------------- Manifest --------------->|   batch 1 ... last = true
  |<--------------- SyncReply ---------------|
  |                                          |
  |---------------- Transfer --------------->|   what is actually coming
  |=== data stream: DataHeader + bytes ====>|   one per needed file, in parallel
  |---------------- FileDone --------------->|   BLAKE3 of the whole file
  |<--------------- FileVerdict -------------|   matched, or did not
  |------------------ Done ----------------->|
```

Either side may send `Error` at any point; it ends the session.

The sender is always the caller in this version. There is no pull mode.

---

## 3. Messages

### Hello / HelloAck

```rust
Hello    { version: u16, features: u64, device: [u8; 32] }
HelloAck { version: u16, features: u64, device: [u8; 32] }
```

`version` must match exactly. There is no older peer to be compatible with, and pretending
otherwise would mean untested downgrade paths.

`features` is a bitset. Bits are intersected with what this side announced, so the result
means "both of us".

| Bit | Name | Meaning |
|---|---|---|
| 0 | `ZSTD` | understands `DataHeader.compressed` |
| 1 | `RESUME` | understands `Decision::Need { from_offset }` |
| 2 | `HASH_CACHE` | may put hashes in manifest entries |
| 3 | `PAIRING` | **this session** will prove a pairing code |

Bits 0-2 say what a build implements and are always announced. Bit 3 says what this session
is doing, and appears only when that side was given a code. Unknown bits are dropped on
decode rather than rejected.

`device` is a random identifier for the peer installation. In version 1 it is a label for
logs, not an authentication input.

### Pair / PairAck

```rust
Pair    { proof: [u8; 32] }
PairAck { proof: [u8; 32] }
```

Sent only when the negotiated features contain `PAIRING`. One side pairing and the other not
is a refusal with `ErrorCode::Unauthorized`, never a downgrade.

Both proofs are computed from the same code and the same session:

```
key     = BLAKE3::derive_key("spd-transfer 2026-08 pairing code", code_utf8)
binding = TLS exporter, label "spd pairing v1 channel binding", 32 bytes, empty context
proof   = BLAKE3::keyed_hash(key, side_tag || binding)     side_tag = "caller" | "answerer"
```

The binding is what stops a relay: a peer in the middle terminates two TLS sessions, so the
proof it receives from one is not the proof the other expects.

The code is ten characters of Crockford base32 without `I`, `L`, `O` and `U`, displayed in
two groups of five. On input, case is folded, dashes and spaces are ignored, and `O`, `I`
and `L` are read as `0`, `1` and `1`.

### Manifest / SyncReply

```rust
Manifest  { batch_seq: u32, last: bool, entries: Vec<Entry> }
SyncReply { batch_seq: u32, decisions: Vec<Decision> }

Entry     { file_id: u64, path: Vec<String>, size: u64, mtime: u64, mode: u32,
            hash: Option<[u8; 32]> }

Decision  = Skip | Need { file_id: u64, from_offset: u64 }
```

The manifest is **always** batched, at most `max_manifest_entries` per message, whatever the
folder size. Each batch is answered before the next is sent, and the answer carries the same
`batch_seq`.

`decisions` is in the same order as the batch's `entries`.

`path` is components, never a joined string: the receiver never has to guess which separator
the sender's platform used. Every component is validated before anything touches the
filesystem - see §5.

`hash` is absent for files above the hashing ceiling (1 GiB) and for cache misses. The
receiver then decides on size and mtime.

`from_offset` is a **file** offset, not a stream offset, and it is the receiver's decision
alone. It is non-zero only when the receiver holds a `.part` file it can identify as this
same file.

### Transfer

```rust
Transfer { files: u64, bytes: u64 }
```

What is actually coming, after the decisions are in. Without it the receiver cannot tell
"nothing to send" from "the streams have not arrived yet", and a dry run would leave it
waiting for files that were never requested.

`files` may be fewer than the receiver asked for. More is a protocol violation.

### Data streams

```rust
DataHeader { file_id: u64, offset: u64, compressed: bool }
```

`offset` must equal the `from_offset` the receiver asked for. Anything else would leave a
hole in the middle of the file that no hash could explain.

`compressed` says what the sender did with **this** file. The receiver obeys the flag and
never its own configuration. The body is then either raw bytes or a single zstd stream
covering everything from `offset` to the end of the file.

`file_id` must be one the receiver answered `Need` to.

### FileDone / FileVerdict

```rust
FileDone    { file_id: u64, hash: [u8; 32] }
FileVerdict { file_id: u64, ok: bool }
```

The hash is BLAKE3 of the **whole file**, uncompressed, from byte zero - including any
prefix the receiver already had before a resume. Both sides therefore verify the same thing
regardless of where the transfer resumed or what crossed the wire.

The receiver renames its `.part` file into place only after sending `ok: true`.

### Done / Error

```rust
Done  { files: u64, bytes: u64 }
Error { code: ErrorCode, msg: String }
```

`ErrorCode` is `UnknownFile | LimitExceeded | ProtocolViolation | Io | Unauthorized |
Internal`. `msg` is already worded for a user; a peer may print it as it is.

---

## 4. Limits

Every bound a peer can push against, with its default:

| Limit | Default | Bounds |
|---|---|---|
| `max_frame_len_bytes` | 4 MiB | one control frame, before allocation |
| `max_manifest_entries` | 2 000 | entries in one `Manifest` |
| `max_files` | 1 000 000 | files one transfer may end up wanting |
| `max_path_depth` | 64 | components in a path |
| `max_path_len_bytes` | 4 096 | a path, in bytes |
| `max_concurrent_streams` | 16 | open data streams |
| `handshake_timeout` | 10 s | from connection to established session |
| `idle_timeout` | 60 s | silence before the connection is dropped |
| `max_file_size_bytes` | 4 TiB | one file, counted as it is written |

`max_manifest_entries` bounds one message and `max_files` bounds the conversation: without
the second, a peer sends a million well-sized batches and the receiver allocates until it
dies.

A decompressed body is counted against `max_file_size_bytes` as it comes out of the codec,
not as it arrives, so a small stream that unpacks into a large one is refused at the right
size.

---

## 5. Paths

A path from a peer is a list of components, and every one of them is rejected if it:

- is empty, `.` or `..`
- contains `/` or `\`
- contains a control character, or any of `< > : " | ? *`
- ends in a dot or a space (Windows strips them, so `report.txt ` and `report.txt` would
  collide)
- is a Windows device name: `CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9`, `LPT1`-`LPT9`, with
  or without an extension

Windows rules are enforced on every platform: a folder received on Linux should still be
usable after being copied to a Windows machine.

The resolved path is then checked to start with the destination root. The components carry
no `..`, so that check is a second line of defence rather than the only one.

---

## 6. What a version 2 would have to change

The version is a `u16` and must match exactly, so any of these needs a bump and a new set of
golden files:

- a new field in an existing message, or a reordering
- a new `Control` variant that an old peer would meet mid-session
- a change to the pairing derivation or its exporter label
- a change to what `from_offset` counts

A new capability that only takes effect when both sides announce it is a feature bit, not a
version bump. That is what the bits are for.
