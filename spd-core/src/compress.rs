//! Deciding whether a file is worth compressing, and compressing it.
//!
//! Two questions, answered once per file and never again: is this the kind of file that
//! already holds compressed data, and if not, does a sample of it actually shrink? An mp4
//! is not read at all - its extension settles it - and a file that turns out to be
//! incompressible costs one sample rather than a whole pass.
//!
//! The answer travels in [`crate::proto::messages::DataHeader::compressed`], so the
//! receiver obeys what the sender did rather than its own configuration. The alternative -
//! each side deciding from its own settings - is a classic way to hand someone a file full
//! of zstd frames named `.txt`.
//!
//! Nothing here does I/O or knows about streams: it turns bytes into bytes, so it can run
//! on the CPU pool without dragging a network task along with it.

use core::fmt;
use std::path::Path;

use zstd::stream::raw::{Decoder as ZstdDecoder, Encoder as ZstdEncoder, Operation, OutBuffer};

/// Compression level used for every file.
///
/// zstd's own default. Higher levels cost several times the CPU for a few percent on a
/// local network, where the disk is the bottleneck long before the cipher or the codec is.
pub const LEVEL: i32 = 3;

/// How much of a file is compressed to find out whether the rest is worth compressing.
pub const SAMPLE_BYTES: usize = 256 * 1024;

/// How much smaller a sample has to get, as a percentage of its compressed size, before
/// the file is compressed.
///
/// Below this the saving does not pay for the CPU on either side, and both peers spend
/// time to move about as many bytes as before. Kept as a percentage rather than a ratio so
/// the comparison is integer arithmetic, exact at every size.
pub const WORTHWHILE_PERCENT: u64 = 105;

/// Extensions whose contents are already compressed.
///
/// Compressing these again reliably produces slightly *more* bytes, at full CPU cost. The
/// list is deliberately short: it holds the formats that actually turn up in a folder
/// someone is transferring, and anything missing is caught by the sample instead.
const ALREADY_COMPRESSED: [&str; 34] = [
    "7z", "avi", "avif", "br", "bz2", "docx", "flac", "gif", "gz", "heic", "jpeg", "jpg", "jxl",
    "mkv", "mov", "mp3", "mp4", "odt", "ogg", "opus", "pdf", "png", "pptx", "rar", "webm", "webp",
    "xlsx", "xz", "zip", "zst", "aac", "apk", "iso", "wasm",
];

/// Whether this name says the file already holds compressed data.
///
/// Case-insensitive: `PHOTO.JPG` is as compressed as `photo.jpg`.
pub fn is_already_compressed(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            let lowered = extension.to_ascii_lowercase();
            ALREADY_COMPRESSED.contains(&lowered.as_str())
        })
}

/// Whether a sample of a file shrinks enough to be worth compressing the whole of it.
///
/// `scratch` is where the compressed sample goes; it is never read by the caller. Passing
/// one in keeps this allocation-free on the transfer path.
///
/// # Errors
/// [`CompressError::Zstd`] if zstd refuses the sample, which on a healthy build does not
/// happen - it is reported rather than assumed either way.
pub fn worth_compressing(sample: &[u8], scratch: &mut [u8]) -> Result<bool, CompressError> {
    if sample.is_empty() {
        return Ok(false);
    }

    let mut compressor =
        zstd::bulk::Compressor::new(LEVEL).map_err(|source| CompressError::Zstd {
            operation: "start a compressor",
            source,
        })?;

    let produced = match compressor.compress_to_buffer(sample, scratch) {
        Ok(produced) => produced,
        // The compressed sample did not fit the scratch buffer, which means it did not
        // shrink at all. That is an answer, not a failure.
        Err(_full) => return Ok(false),
    };

    if produced == 0 {
        return Ok(false);
    }

    Ok(sample.len() as u64 * 100 >= produced as u64 * WORTHWHILE_PERCENT)
}

/// What one pass through the codec consumed and produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    /// Bytes of the input that were consumed.
    pub taken: usize,
    /// Bytes written into the output.
    pub produced: usize,
}

/// What ending a stream produced, and whether there is more of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tail {
    /// Bytes written into the output.
    pub produced: usize,
    /// Whether another call is needed to get the rest.
    pub more: bool,
}

/// Turns a file's bytes into a zstd stream, a buffer at a time.
///
/// Streaming rather than block by block: the window carries across the whole file, which is
/// where most of the saving on a folder of similar files comes from.
pub struct Encoder(ZstdEncoder<'static>);

/// zstd's context has no readable representation, and a log line about a compressor only
/// ever needs to say which one it is.
impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Encoder(zstd)")
    }
}

impl Encoder {
    /// Starts a stream at [`LEVEL`].
    ///
    /// # Errors
    /// [`CompressError::Zstd`] if zstd cannot allocate its context.
    pub fn new() -> Result<Self, CompressError> {
        ZstdEncoder::new(LEVEL)
            .map(Self)
            .map_err(|source| CompressError::Zstd {
                operation: "start a compressor",
                source,
            })
    }

    /// Compresses as much of `input` as fits in `output`.
    ///
    /// Producing nothing is normal: zstd holds bytes back until it has a block worth
    /// writing. Consuming nothing *and* producing nothing is not, and is reported rather
    /// than looped on.
    ///
    /// # Errors
    /// [`CompressError::Zstd`] if zstd rejects the input, [`CompressError::Stalled`] if it
    /// stops making progress.
    pub fn push(&mut self, input: &[u8], output: &mut [u8]) -> Result<Step, CompressError> {
        step(&mut self.0, input, output, "compress")
    }

    /// Ends the stream, writing its last bytes into `output`.
    ///
    /// # Errors
    /// [`CompressError::Zstd`] if zstd cannot close the frame.
    pub fn finish(&mut self, output: &mut [u8]) -> Result<Tail, CompressError> {
        let mut out = OutBuffer::around(output);
        let remaining = self
            .0
            .finish(&mut out, true)
            .map_err(|source| CompressError::Zstd {
                operation: "close the compressed stream",
                source,
            })?;

        Ok(Tail {
            produced: out.pos(),
            more: remaining > 0,
        })
    }
}

/// Turns a zstd stream back into the file's bytes, a buffer at a time.
pub struct Decoder(ZstdDecoder<'static>);

/// Same reasoning as [`Encoder`]'s.
impl fmt::Debug for Decoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Decoder(zstd)")
    }
}

impl Decoder {
    /// Starts reading a stream.
    ///
    /// # Errors
    /// [`CompressError::Zstd`] if zstd cannot allocate its context.
    pub fn new() -> Result<Self, CompressError> {
        ZstdDecoder::new()
            .map(Self)
            .map_err(|source| CompressError::Zstd {
                operation: "start a decompressor",
                source,
            })
    }

    /// Decompresses as much of `input` as fits in `output`.
    ///
    /// The caller keeps calling until `input` is consumed: one buffer of compressed bytes
    /// can expand into many buffers of file, and each of them has to be written before the
    /// next is produced.
    ///
    /// # Errors
    /// [`CompressError::Zstd`] if the stream is malformed - which, coming from a peer, is
    /// something to refuse rather than to recover from - or [`CompressError::Stalled`] if
    /// it stops making progress.
    pub fn pull(&mut self, input: &[u8], output: &mut [u8]) -> Result<Step, CompressError> {
        step(&mut self.0, input, output, "decompress")
    }
}

/// One pass of either direction, since zstd's operation is the same shape both ways.
fn step(
    operation: &mut impl Operation,
    input: &[u8],
    output: &mut [u8],
    what: &'static str,
) -> Result<Step, CompressError> {
    let status = operation
        .run_on_buffers(input, output)
        .map_err(|source| CompressError::Zstd {
            operation: what,
            source,
        })?;

    if status.bytes_read == 0 && status.bytes_written == 0 && !input.is_empty() {
        return Err(CompressError::Stalled);
    }

    Ok(Step {
        taken: status.bytes_read,
        produced: status.bytes_written,
    })
}

/// Why compression or decompression could not run.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompressError {
    /// zstd refused an operation.
    #[error("zstd could not {operation}")]
    Zstd {
        /// What was being attempted.
        operation: &'static str,
        /// What zstd reported.
        source: std::io::Error,
    },

    /// The codec consumed nothing and produced nothing, so calling it again would loop.
    #[error("the compressed stream stopped making progress before it ended")]
    Stalled,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compressible on purpose: repeated text is what a folder of source or logs looks
    /// like to zstd.
    fn text(len: usize) -> Vec<u8> {
        "the quick brown fox jumps over the lazy dog. "
            .bytes()
            .cycle()
            .take(len)
            .collect()
    }

    /// Incompressible on purpose, without needing randomness at test time.
    fn noise(len: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect()
    }

    #[test]
    fn already_compressed_names_are_recognised_whatever_their_case() {
        assert!(is_already_compressed(Path::new("holiday.MP4")));
        assert!(is_already_compressed(Path::new("archive.tar.gz")));
        assert!(!is_already_compressed(Path::new("notes.txt")));
        assert!(!is_already_compressed(Path::new("no-extension")));
    }

    #[test]
    fn a_sample_of_text_is_worth_compressing_and_a_sample_of_noise_is_not() {
        let mut scratch = vec![0_u8; SAMPLE_BYTES];

        assert!(worth_compressing(&text(SAMPLE_BYTES), &mut scratch).unwrap());
        assert!(!worth_compressing(&noise(SAMPLE_BYTES), &mut scratch).unwrap());
        assert!(!worth_compressing(&[], &mut scratch).unwrap());
    }

    #[test]
    fn a_stream_survives_the_round_trip_in_small_pieces() {
        let original = text(300_000);
        let mut encoder = Encoder::new().unwrap();
        let mut compressed = Vec::new();
        let mut block = vec![0_u8; 4096];

        let mut taken = 0;
        while taken < original.len() {
            let step = encoder.push(&original[taken..], &mut block).unwrap();
            taken += step.taken;
            compressed.extend_from_slice(&block[..step.produced]);
        }

        loop {
            let tail = encoder.finish(&mut block).unwrap();
            compressed.extend_from_slice(&block[..tail.produced]);
            if !tail.more {
                break;
            }
        }

        assert!(
            compressed.len() < original.len(),
            "repeated text should compress"
        );

        let mut decoder = Decoder::new().unwrap();
        let mut restored = Vec::new();
        let mut consumed = 0;
        while consumed < compressed.len() {
            let step = decoder.pull(&compressed[consumed..], &mut block).unwrap();
            consumed += step.taken;
            restored.extend_from_slice(&block[..step.produced]);
        }

        assert_eq!(restored, original);
    }

    #[test]
    fn a_malformed_stream_is_refused_rather_than_guessed_at() {
        let mut decoder = Decoder::new().unwrap();
        let mut output = vec![0_u8; 1024];

        assert!(matches!(
            decoder.pull(b"this is not a zstd frame at all", &mut output),
            Err(CompressError::Zstd { .. })
        ));
    }
}
