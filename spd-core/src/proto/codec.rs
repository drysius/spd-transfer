//! Framing for the control stream and the head of each data stream.
//!
//! Control frames are length-delimited and carry a [`Control`] encoded with `postcard`.
//! The length prefix is bounded by [`Limits::max_frame_len_bytes`] before a single byte is
//! buffered, so a peer cannot make this process reserve memory by announcing a size - the
//! exact mistake that let the previous project allocate 128 MB pre-authentication.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use quinn::{RecvStream, SendStream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Join, join};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::proto::messages::{Control, DataHeader};
use crate::safety::limits::Limits;

/// Largest encoded [`DataHeader`], with room to spare for future fields.
///
/// A data header is three small fields; anything larger means the peer is not sending a
/// header, so the ceiling is small on purpose.
pub const MAX_DATA_HEADER_BYTES: usize = 64;

/// The control stream, typed.
///
/// Owns both halves of the bidirectional QUIC stream opened right after the handshake and
/// lives as long as the session does.
pub struct ControlChannel {
    framed: Framed<Join<RecvStream, SendStream>, LengthDelimitedCodec>,
    max_frame_len_bytes: usize,
}

impl ControlChannel {
    /// Wraps the two halves of a bidirectional stream into a typed channel.
    pub fn new(send: SendStream, recv: RecvStream, limits: &Limits) -> Self {
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(limits.max_frame_len_bytes)
            .length_field_type::<u32>()
            .new_codec();

        Self {
            framed: Framed::new(join(recv, send), codec),
            max_frame_len_bytes: limits.max_frame_len_bytes,
        }
    }

    /// Sends one control message.
    ///
    /// # Errors
    /// [`ProtoError::Encode`] if the message cannot be serialised.
    /// [`ProtoError::FrameTooLarge`] if it would exceed the negotiated frame limit -
    /// checked here so an oversized manifest batch is a local bug report, not a peer
    /// error arriving after the fact.
    /// [`ProtoError::Io`] if the stream is gone.
    pub async fn send(&mut self, message: &Control) -> Result<(), ProtoError> {
        let encoded = postcard::to_stdvec(message).map_err(|source| ProtoError::Encode {
            kind: message.kind(),
            source,
        })?;

        if encoded.len() > self.max_frame_len_bytes {
            return Err(ProtoError::FrameTooLarge {
                got: encoded.len(),
                max: self.max_frame_len_bytes,
            });
        }

        self.framed.send(Bytes::from(encoded)).await?;
        Ok(())
    }

    /// Receives the next control message.
    ///
    /// # Errors
    /// [`ProtoError::PeerClosed`] if the peer closed the stream cleanly. That is an error
    /// here rather than `Ok(None)`: every caller is waiting for a specific reply, and a
    /// silent end is exactly what the previous project turned into a hung transfer.
    /// [`ProtoError::Decode`] if the frame is not a valid message.
    /// [`ProtoError::Io`] on transport failure, including an oversized length prefix.
    pub async fn recv(&mut self) -> Result<Control, ProtoError> {
        let frame = self.framed.next().await.ok_or(ProtoError::PeerClosed)??;
        let message =
            postcard::from_bytes(&frame).map_err(|source| ProtoError::Decode { source })?;
        Ok(message)
    }

    /// Flushes and closes the sending half, leaving the peer a clean end of stream.
    ///
    /// # Errors
    /// [`ProtoError::Io`] if the flush fails.
    pub async fn close(&mut self) -> Result<(), ProtoError> {
        self.framed.close().await?;
        Ok(())
    }
}

/// Writes the header at the head of a data stream.
///
/// Length-prefixed with a `u16` so the reader knows how much to take before the raw file
/// bytes begin.
///
/// # Errors
/// [`ProtoError::Encode`] if the header cannot be serialised, [`ProtoError::Io`] if the
/// stream fails.
pub async fn write_data_header<W>(writer: &mut W, header: &DataHeader) -> Result<(), ProtoError>
where
    W: AsyncWrite + Unpin,
{
    let encoded = postcard::to_stdvec(header).map_err(|source| ProtoError::Encode {
        kind: "DataHeader",
        source,
    })?;

    // INVARIANT: DataHeader is three fixed-width fields; postcard encodes it well under
    // the ceiling. The check keeps that true if a field is ever added.
    if encoded.len() > MAX_DATA_HEADER_BYTES {
        return Err(ProtoError::FrameTooLarge {
            got: encoded.len(),
            max: MAX_DATA_HEADER_BYTES,
        });
    }

    let len = u16::try_from(encoded.len()).unwrap_or(u16::MAX);
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(&encoded).await?;
    Ok(())
}

/// Reads the header from the head of a data stream.
///
/// # Errors
/// [`ProtoError::FrameTooLarge`] if the announced header exceeds
/// [`MAX_DATA_HEADER_BYTES`] - rejected before allocating, so the size a peer claims never
/// becomes the size this process reserves.
/// [`ProtoError::Decode`] if the bytes are not a header, [`ProtoError::Io`] if the stream
/// ends early.
pub async fn read_data_header<R>(reader: &mut R) -> Result<DataHeader, ProtoError>
where
    R: AsyncRead + Unpin,
{
    let mut len_bytes = [0_u8; 2];
    reader.read_exact(&mut len_bytes).await?;
    let len = usize::from(u16::from_le_bytes(len_bytes));

    if len > MAX_DATA_HEADER_BYTES {
        return Err(ProtoError::FrameTooLarge {
            got: len,
            max: MAX_DATA_HEADER_BYTES,
        });
    }

    let mut encoded = vec![0_u8; len];
    reader.read_exact(&mut encoded).await?;

    postcard::from_bytes(&encoded).map_err(|source| ProtoError::Decode { source })
}

/// Why a frame could not be sent or read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtoError {
    /// The peer closed the control stream while a message was expected.
    #[error("the peer closed the control stream")]
    PeerClosed,

    /// A frame is larger than the configured ceiling.
    #[error("frame of {got} B exceeds the {max} B limit (max_frame_len_bytes)")]
    FrameTooLarge {
        /// Size seen or announced.
        got: usize,
        /// Configured ceiling.
        max: usize,
    },

    /// A message could not be serialised.
    #[error("could not encode a {kind} message: {source}")]
    Encode {
        /// Which message.
        kind: &'static str,
        /// Underlying serialiser error.
        source: postcard::Error,
    },

    /// A frame could not be parsed.
    #[error("the peer sent a frame this build cannot parse: {source}")]
    Decode {
        /// Underlying deserialiser error.
        source: postcard::Error,
    },

    /// The transport failed.
    #[error("control stream I/O failed")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::messages::FileId;

    #[tokio::test]
    async fn data_header_round_trips_through_a_pipe() {
        let header = DataHeader {
            file_id: FileId(7),
            offset: 4096,
            compressed: true,
        };

        let (mut client, mut server) = tokio::io::duplex(256);
        write_data_header(&mut client, &header).await.unwrap();

        assert_eq!(read_data_header(&mut server).await.unwrap(), header);
    }

    #[tokio::test]
    async fn an_oversized_header_is_rejected_before_allocating() {
        let (mut client, mut server) = tokio::io::duplex(256);
        let announced = u16::try_from(MAX_DATA_HEADER_BYTES + 1).unwrap();
        client.write_all(&announced.to_le_bytes()).await.unwrap();

        let error = read_data_header(&mut server).await.unwrap_err();

        assert!(matches!(
            error,
            ProtoError::FrameTooLarge { got, max }
                if got == MAX_DATA_HEADER_BYTES + 1 && max == MAX_DATA_HEADER_BYTES
        ));
    }

    #[tokio::test]
    async fn a_truncated_header_fails_instead_of_hanging() {
        let (client, mut server) = tokio::io::duplex(256);
        drop(client);

        assert!(matches!(
            read_data_header(&mut server).await.unwrap_err(),
            ProtoError::Io(_)
        ));
    }
}
