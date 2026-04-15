//! msgr2 Protocol V2 implementation
//!
//! This module provides a high-level async API for the msgr2 protocol,
//! handling frame I/O, encryption, and state machine coordination.

use bytes::{Buf, Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::IoSlice;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A list of `Bytes` chunks that implements `bytes::Buf`.
///
/// `write_all_buf` uses `chunks_vectored` to feed all chunks to
/// `write_vectored` (writev) in a single syscall where possible,
/// avoiding memcpy of large payload segments.
struct BufList(VecDeque<Bytes>);

impl Buf for BufList {
    fn remaining(&self) -> usize {
        self.0.iter().map(|b| b.len()).sum()
    }

    fn chunk(&self) -> &[u8] {
        for b in &self.0 {
            if !b.is_empty() {
                return b;
            }
        }
        &[]
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let front = self.0.front_mut().expect("advance past end");
            if cnt >= front.len() {
                cnt -= front.len();
                self.0.pop_front();
            } else {
                front.advance(cnt);
                break;
            }
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut n = 0;
        for b in &self.0 {
            if n >= dst.len() {
                break;
            }
            if !b.is_empty() {
                dst[n] = IoSlice::new(b);
                n += 1;
            }
        }
        n
    }
}

use crate::Denc;
use crate::msgr2::FeatureSet;
use crate::msgr2::banner::Banner;
use crate::msgr2::error::{Msgr2Error as Error, Result};
use crate::msgr2::frames::create_frame_from_trait;
use crate::msgr2::frames::{
    CRYPTO_BLOCK_SIZE, Frame, MessageFrame, Preamble, Tag, align_to_crypto_block,
};
use crate::msgr2::message::Message;
pub use crate::msgr2::priority_queue::PriorityQueue;
use crate::msgr2::state_machine::{StateKind, StateMachine};
use std::borrow::Cow;

/// Zero padding buffer — avoids heap allocation for small alignment padding.
/// Large enough to cover INLINE_SIZE (48) padding in one slice.
const ZEROS: [u8; 64] = [0u8; 64];

/// Action to take after successful reconnection in retry_with_reconnect
enum ReconnectAction<T> {
    /// Return success immediately with the given value
    ReturnSuccess(T),
    /// Continue retrying the operation
    Retry,
}

/// Frame I/O layer - handles encryption-aware frame send/recv
pub(crate) struct FrameIO {
    stream: TcpStream,
    /// Resumable recv state.  `tokio::io::AsyncReadExt::read_exact` is NOT
    /// cancel-safe: dropping the future mid-read loses the partial buffer
    /// while the TCP stream cursor has already advanced, leaving the next
    /// `read_exact` decoding garbage from the middle of the previous frame
    /// ("Unknown tag: N").  We work around this by owning the read state
    /// (partial preamble block + partial payload) as struct fields and
    /// looping over `stream.read()` (which IS cancel-safe).  When
    /// `recv_frame` is cancelled inside `tokio::select!`, this state
    /// survives and the next call picks up exactly where we left off.
    pending_recv: Option<PendingRecv>,
}

/// Resumable receive state for a single frame.
///
/// Walks through two phases:
///  1. `Preamble` — reading the 32/96-byte preamble block off the wire.
///  2. `Payload`  — the preamble has been decoded; we're reading the
///     variable-length payload (segments + CRCs or encrypted blob).
#[derive(Debug)]
enum PendingRecv {
    Preamble {
        buf: Vec<u8>,
        filled: usize,
    },
    Payload {
        /// Decoded preamble header for the in-progress frame.
        preamble: crate::msgr2::frames::Preamble,
        /// SECURE mode only: the 48-byte inline plaintext that came
        /// embedded in the decrypted preamble block.  Empty in CRC mode.
        inline: Bytes,
        buf: Vec<u8>,
        filled: usize,
    },
}

impl FrameIO {
    const INLINE_SIZE: usize = 48;
    const GCM_TAG_SIZE: usize = 16;

    /// Create a new FrameIO from an existing TCP stream
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            pending_recv: None,
        }
    }

    /// Cancel-safe fill of `buf[*filled..]` by looping over `stream.read()`.
    ///
    /// `stream.read()` is documented as cancel-safe: if the returned future
    /// is dropped before it completes, no data has been read from the
    /// stream.  By owning `filled` and `buf` outside the future and calling
    /// `read` in a loop, we get a cancel-safe `read_exact`: dropping our
    /// caller's future preserves `filled` and `buf` for the next call.
    async fn fill_from_stream(
        stream: &mut TcpStream,
        buf: &mut [u8],
        filled: &mut usize,
    ) -> Result<()> {
        use tokio::io::AsyncReadExt;
        while *filled < buf.len() {
            let n = stream
                .read(&mut buf[*filled..])
                .await
                .map_err(Error::from)?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof while filling frame buffer",
                )));
            }
            *filled += n;
        }
        Ok(())
    }

    /// Shut down the writer half of the underlying TCP stream to
    /// trigger a clean FIN instead of a RST on drop.
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        self.stream.shutdown().await.map_err(Error::from)
    }

    fn normalized_num_segments(frame: &Frame) -> usize {
        let mut num_segments = frame.segments.len();
        while num_segments > 1 && frame.segments[num_segments - 1].is_empty() {
            num_segments -= 1;
        }
        num_segments
    }

    fn build_secure_preamble(frame: &Frame, num_segments: usize) -> Result<Bytes> {
        let mut preamble = Preamble {
            tag: frame.preamble.tag,
            num_segments: num_segments as u8,
            segments: [crate::msgr2::frames::SegmentDescriptor::default();
                crate::msgr2::frames::MAX_NUM_SEGMENTS],
            flags: frame.preamble.flags,
            reserved: 0,
            crc: 0,
        };

        for (index, segment) in frame.segments.iter().take(num_segments).enumerate() {
            preamble.segments[index] = crate::msgr2::frames::SegmentDescriptor {
                logical_len: segment.len() as u32,
                align: frame.preamble.segments[index].align,
            };
        }

        Ok(preamble.encode()?)
    }

    fn build_secure_payload(frame: &Frame, num_segments: usize) -> BytesMut {
        use crate::msgr2::frames::FRAME_LATE_STATUS_COMPLETE;

        let total_len = frame
            .segments
            .iter()
            .take(num_segments)
            .filter(|segment| !segment.is_empty())
            .map(|segment| align_to_crypto_block(segment.len()))
            .sum::<usize>()
            + if num_segments > 1 {
                crate::msgr2::frames::CRYPTO_BLOCK_SIZE
            } else {
                0
            };

        let mut payload_bytes = BytesMut::with_capacity(total_len);
        for segment in frame.segments.iter().take(num_segments) {
            let segment_len = segment.len();
            if segment_len == 0 {
                continue;
            }

            payload_bytes.extend_from_slice(segment);
            let aligned_len = align_to_crypto_block(segment_len);
            let padding_needed = aligned_len - segment_len;
            if padding_needed > 0 {
                payload_bytes.extend_from_slice(&ZEROS[..padding_needed]);
            }
        }

        if num_segments > 1 {
            payload_bytes.extend_from_slice(&[FRAME_LATE_STATUS_COMPLETE]);
            payload_bytes.extend_from_slice(&ZEROS[..crate::msgr2::frames::CRYPTO_BLOCK_SIZE - 1]);
        }

        payload_bytes
    }

    /// Send a frame with optional compression and encryption.
    ///
    /// Processing order: compression → encryption.
    /// - If compression is enabled, compress frame segments first.
    /// - If encryption is enabled, encrypt the (possibly compressed) frame.
    ///
    /// If encryption is enabled in the state machine, this implements the
    /// msgr2.1 inline optimization:
    /// - First 48 bytes of payload are encrypted together with the preamble
    ///   (32+48=80 → 96 bytes with GCM tag).
    /// - Remaining payload bytes are encrypted separately (N → N+16 bytes).
    pub async fn send_frame(
        &mut self,
        frame: &Frame,
        state_machine: &mut StateMachine,
    ) -> Result<()> {
        // Cow avoids cloning when compression is disabled or doesn't help
        let frame_to_send: Cow<Frame> = if let Some(ctx) = state_machine.compression_ctx() {
            let compressed = frame.compress(ctx);
            if compressed.preamble.is_compressed() {
                tracing::debug!(
                    "Frame compressed: tag={:?}, algorithm={:?}",
                    frame.preamble.tag,
                    ctx.algorithm()
                );
                Cow::Owned(compressed)
            } else {
                Cow::Borrowed(frame)
            }
        } else {
            Cow::Borrowed(frame)
        };

        let is_compressed = frame_to_send.preamble.is_compressed();
        if !state_machine.has_encryption() {
            // Zero-copy path: scatter-gather I/O keeps large payload segments
            // (e.g., write data) as Bytes references instead of memcpy-ing
            // them into a contiguous buffer.
            let chunks = frame_to_send.to_wire_vectored(true)?;
            let total_len: usize = chunks.iter().map(|c| c.len()).sum();
            tracing::debug!(
                "Sending frame: tag={:?}, {} segments, {} wire bytes ({} chunks), encrypted=false, compressed={}",
                frame_to_send.preamble.tag,
                frame_to_send.preamble.num_segments,
                total_len,
                chunks.len(),
                is_compressed,
            );
            for chunk in &chunks {
                state_machine.record_sent(chunk);
            }
            let mut buf = BufList(VecDeque::from(chunks));
            self.stream.write_all_buf(&mut buf).await?;
            return Ok(());
        }

        let num_segments = Self::normalized_num_segments(&frame_to_send);
        let preamble_bytes = Self::build_secure_preamble(&frame_to_send, num_segments)?;
        let payload_bytes = Self::build_secure_payload(&frame_to_send, num_segments);

        tracing::debug!(
            "Sending frame: tag={:?}, {} segments, secure payload {} bytes, compressed={}",
            frame_to_send.preamble.tag,
            num_segments,
            payload_bytes.len(),
            is_compressed,
        );

        let inline_size = payload_bytes.len().min(Self::INLINE_SIZE);
        let remaining_size = payload_bytes.len().saturating_sub(Self::INLINE_SIZE);

        let mut preamble_block =
            BytesMut::with_capacity(crate::msgr2::frames::PREAMBLE_SIZE + Self::INLINE_SIZE);
        preamble_block.extend_from_slice(&preamble_bytes);
        preamble_block.extend_from_slice(&payload_bytes[..inline_size]);

        if inline_size < Self::INLINE_SIZE {
            preamble_block.extend_from_slice(&ZEROS[..Self::INLINE_SIZE - inline_size]);
        }

        let encrypted_preamble = state_machine.encrypt_frame_data(&preamble_block)?;

        let mut result = BytesMut::with_capacity(encrypted_preamble.len() + remaining_size + 16);
        result.extend_from_slice(&encrypted_preamble);

        if remaining_size > 0 {
            let remaining_data = &payload_bytes[Self::INLINE_SIZE..];
            let encrypted_remaining = state_machine.encrypt_frame_data(remaining_data)?;
            result.extend_from_slice(&encrypted_remaining);
        }

        tracing::trace!(
            "Encrypted frame: {} bytes (preamble_block={}, remaining={})",
            result.len(),
            encrypted_preamble.len(),
            if remaining_size > 0 {
                remaining_size + 16
            } else {
                0
            }
        );

        let final_bytes = result.freeze();
        state_machine.record_sent(&final_bytes);
        self.stream.write_all(&final_bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Receive a frame, decrypting and decompressing as needed.
    ///
    /// The work splits cleanly between the two modes:
    ///
    /// 1. Read the preamble block (32 bytes plaintext, 96 bytes with the
    ///    msgr2.1 inline-encryption optimisation: preamble + 48 inline
    ///    payload bytes + GCM tag).
    /// 2. In SECURE mode decrypt that block to recover the plaintext
    ///    preamble and the 48-byte `inline_data` slice.
    /// 3. Parse the [`Preamble`] and decide whether the frame has a
    ///    payload at all — empty frames short-circuit here.
    /// 4. Hand off to [`recv_frame_secure`] or [`recv_frame_crc`], which
    ///    each own the mode-specific payload read + segment extraction
    ///    in a single linear flow.
    /// 5. If the frame advertised `FRAME_EARLY_DATA_COMPRESSED`,
    ///    decompress per-segment before returning.
    ///
    /// [`recv_frame_secure`]: Self::recv_frame_secure
    /// [`recv_frame_crc`]: Self::recv_frame_crc
    ///
    /// ## Cancel safety
    ///
    /// This function is **cancel-safe**.  If the caller drops the future
    /// mid-read (e.g. a `tokio::select!` branch fires), the partial
    /// receive state is preserved in `self.pending_recv` and the next
    /// call to `recv_frame` resumes exactly where the previous call left
    /// off.  The underlying I/O primitive is `stream.read()` (which is
    /// cancel-safe by tokio contract), not `stream.read_exact()` (which
    /// is NOT — it loses its internal loop state on cancellation while
    /// the stream cursor has already advanced).
    pub async fn recv_frame(&mut self, state_machine: &mut StateMachine) -> Result<Frame> {
        let has_encryption = state_machine.has_encryption();

        // Phase 1: Fill the preamble block (resumable via self.pending_recv).
        if self.pending_recv.is_none() {
            let size = if has_encryption {
                crate::msgr2::frames::PREAMBLE_SIZE + Self::INLINE_SIZE + Self::GCM_TAG_SIZE // 96
            } else {
                crate::msgr2::frames::PREAMBLE_SIZE // 32
            };
            self.pending_recv = Some(PendingRecv::Preamble {
                buf: vec![0u8; size],
                filled: 0,
            });
        }

        if let Some(PendingRecv::Preamble { buf, filled }) = self.pending_recv.as_mut() {
            Self::fill_from_stream(&mut self.stream, buf, filled).await?;

            // Transition: take ownership of the completed preamble block
            // and decode it.  If we're cancelled before the next `.await`,
            // `pending_recv` is left in the Payload state (or cleared for
            // empty frames) and the next call picks up from payload read.
            let Some(PendingRecv::Preamble {
                buf: preamble_block,
                ..
            }) = self.pending_recv.take()
            else {
                unreachable!()
            };

            state_machine.record_received(&preamble_block);

            // SECURE mode: the 96-byte block decrypts to a 80-byte buffer
            // holding [32 preamble | 48 inline payload]. CRC mode: the 32
            // plaintext bytes are the preamble and there is no inline data.
            let preamble_and_inline = if has_encryption {
                let decrypted = state_machine.decrypt_frame_data(&preamble_block)?;
                tracing::debug!(
                    "Decrypted preamble block: {} bytes → {} bytes",
                    preamble_block.len(),
                    decrypted.len()
                );
                decrypted
            } else {
                Bytes::copy_from_slice(&preamble_block)
            };
            let preamble =
                Preamble::decode(preamble_and_inline.slice(..crate::msgr2::frames::PREAMBLE_SIZE))?;

            tracing::debug!(
                "Received preamble: tag={:?}, num_segments={}, segments={:?}",
                preamble.tag,
                preamble.num_segments,
                &preamble.segments[0..preamble.num_segments as usize]
            );

            let total_segment_size: usize = preamble.segments[..preamble.num_segments as usize]
                .iter()
                .map(|s| s.logical_len as usize)
                .sum();

            if total_segment_size == 0 {
                tracing::debug!("Empty frame, returning immediately");
                return Ok(Frame {
                    preamble,
                    segments: vec![],
                });
            }

            // Compute payload size and allocate the resumable buffer.
            let payload_size = if has_encryption {
                Self::compute_secure_payload_size(&preamble, total_segment_size)
            } else {
                Self::compute_crc_payload_size(total_segment_size, preamble.num_segments > 1)
            };

            // Stash the decrypted inline bytes (for secure mode).  CRC mode
            // leaves this empty.
            let inline = if has_encryption {
                preamble_and_inline.slice(
                    crate::msgr2::frames::PREAMBLE_SIZE
                        ..crate::msgr2::frames::PREAMBLE_SIZE + Self::INLINE_SIZE,
                )
            } else {
                Bytes::new()
            };

            self.pending_recv = Some(PendingRecv::Payload {
                preamble,
                inline,
                buf: vec![0u8; payload_size],
                filled: 0,
            });
        }

        // Phase 2: Fill the payload buffer (resumable via self.pending_recv).
        if let Some(PendingRecv::Payload { buf, filled, .. }) = self.pending_recv.as_mut() {
            Self::fill_from_stream(&mut self.stream, buf, filled).await?;
        } else {
            unreachable!("pending_recv must be in Payload phase by now");
        }

        // Take the completed payload state.  From here on, there are no
        // `.await` points that could cancel us mid-decode — the caller
        // will get either a completed frame or an error synchronously.
        let Some(PendingRecv::Payload {
            preamble,
            inline,
            buf: payload_buf,
            ..
        }) = self.pending_recv.take()
        else {
            unreachable!()
        };

        state_machine.record_received(&payload_buf);

        let mut frame = if has_encryption {
            Self::decode_secure_payload(preamble, inline, payload_buf, state_machine)?
        } else {
            Self::decode_crc_payload(preamble, payload_buf, state_machine.is_rev1())?
        };

        if frame.preamble.is_compressed() {
            if let Some(ctx) = state_machine.compression_ctx() {
                frame = frame.decompress(ctx).map_err(|e| {
                    Error::protocol_error(&format!("Decompression failed: {:?}", e))
                })?;
                tracing::debug!(
                    "Frame decompressed: tag={:?}, algorithm={:?}",
                    frame.preamble.tag,
                    ctx.algorithm()
                );
            } else {
                tracing::warn!(
                    "Frame has FRAME_EARLY_DATA_COMPRESSED flag but no compression context"
                );
            }
        }

        Ok(frame)
    }

    fn compute_secure_payload_size(preamble: &Preamble, total_segment_size: usize) -> usize {
        if preamble.num_segments == 1 {
            let total = align_to_crypto_block(total_segment_size);
            // Only the bytes BEYOND the 48-byte inline region need to be
            // read off the wire; but when total <= INLINE_SIZE we still
            // return 0 here, meaning no additional read is needed.
            if total > Self::INLINE_SIZE {
                total - Self::INLINE_SIZE + Self::GCM_TAG_SIZE
            } else {
                0
            }
        } else {
            let padded_size: usize = preamble.segments[..preamble.num_segments as usize]
                .iter()
                .map(|s| align_to_crypto_block(s.logical_len as usize))
                .sum();
            let total = padded_size + CRYPTO_BLOCK_SIZE;
            if total > Self::INLINE_SIZE {
                total - Self::INLINE_SIZE + Self::GCM_TAG_SIZE
            } else {
                0
            }
        }
    }

    fn compute_crc_payload_size(total_segment_size: usize, is_multi: bool) -> usize {
        if !is_multi {
            total_segment_size + crate::msgr2::frames::FRAME_CRC_SIZE
        } else {
            total_segment_size
                + crate::msgr2::frames::FRAME_CRC_SIZE
                + 1
                + (crate::msgr2::frames::MAX_NUM_SEGMENTS - 1)
                    * crate::msgr2::frames::FRAME_CRC_SIZE
        }
    }

    /// Synchronous decode helper for SECURE-mode frame payload.
    ///
    /// Caller provides the already-read (possibly empty) remaining
    /// ciphertext bytes in `buf` plus the plaintext `inline` bytes that
    /// arrived in the decrypted preamble block.  Decrypts the remaining
    /// bytes, stitches the two halves, and extracts segments.  All I/O
    /// happened in `recv_frame` via `fill_from_stream` before this call.
    fn decode_secure_payload(
        preamble: Preamble,
        inline_data: Bytes,
        remaining_buf: Vec<u8>,
        state_machine: &mut StateMachine,
    ) -> Result<Frame> {
        let total_payload_size = if preamble.num_segments == 1 {
            align_to_crypto_block(
                preamble.segments[..preamble.num_segments as usize]
                    .iter()
                    .map(|s| s.logical_len as usize)
                    .sum::<usize>(),
            )
        } else {
            let padded_size: usize = preamble.segments[..preamble.num_segments as usize]
                .iter()
                .map(|s| align_to_crypto_block(s.logical_len as usize))
                .sum();
            padded_size + CRYPTO_BLOCK_SIZE
        };

        let full_payload = if !remaining_buf.is_empty() {
            let decrypted_remaining = state_machine.decrypt_frame_data(&remaining_buf)?;
            let mut combined =
                BytesMut::with_capacity(Self::INLINE_SIZE + decrypted_remaining.len());
            combined.extend_from_slice(&inline_data);
            combined.extend_from_slice(&decrypted_remaining);
            combined.freeze()
        } else {
            // Whole payload fit inside the 48-byte inline region.
            inline_data.slice(..total_payload_size)
        };

        // Walk segment descriptors with crypto-block alignment. Empty
        // segments still occupy an index position so `segments[i]` maps
        // to `preamble.segments[i]`.
        let mut segments = Vec::with_capacity(preamble.num_segments as usize);
        let mut offset = 0;
        for i in 0..preamble.num_segments as usize {
            let segment_len = preamble.segments[i].logical_len as usize;
            if segment_len == 0 {
                segments.push(Bytes::new());
                continue;
            }
            segments.push(full_payload.slice(offset..offset + segment_len));
            offset += align_to_crypto_block(segment_len);
        }

        Ok(Frame { preamble, segments })
    }

    /// Synchronous decode helper for CRC-mode frame payload.
    ///
    /// Caller provides the already-read plaintext payload bytes in
    /// `payload_buf`.  Walks the preamble's segment descriptors slicing
    /// each segment out and verifies inline + epilogue CRCs.  All I/O
    /// happened in `recv_frame` via `fill_from_stream` before this call.
    fn decode_crc_payload(
        preamble: Preamble,
        payload_buf: Vec<u8>,
        is_rev1: bool,
    ) -> Result<Frame> {
        let is_multi = preamble.num_segments > 1;
        let full_payload = Bytes::from(payload_buf);

        let mut segments = Vec::with_capacity(preamble.num_segments as usize);
        let mut offset = 0;
        for i in 0..preamble.num_segments as usize {
            let segment_len = preamble.segments[i].logical_len as usize;
            if segment_len == 0 {
                segments.push(Bytes::new());
                continue;
            }
            segments.push(full_payload.slice(offset..offset + segment_len));
            offset += segment_len;

            // Rev1: 4-byte inverted CRC32C follows segment 0 inline.
            if is_rev1 && i == 0 {
                if let Some(crc_bytes) = full_payload
                    .get(offset..offset + crate::msgr2::frames::FRAME_CRC_SIZE)
                    .and_then(|s| <[u8; 4]>::try_from(s).ok())
                {
                    let received_crc = u32::from_le_bytes(crc_bytes);
                    let expected_crc =
                        !crc32c::crc32c(segments.last().ok_or_else(|| {
                            Error::protocol_error("CRC check: empty segments list")
                        })?);
                    if received_crc != expected_crc {
                        return Err(Error::Protocol(format!(
                            "Segment 0 CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                            expected_crc, received_crc
                        )));
                    }
                }
                offset += crate::msgr2::frames::FRAME_CRC_SIZE;
            }
        }

        // Multi-segment epilogue verification.
        if is_multi && offset < full_payload.len() {
            let late_status = full_payload[offset];
            if (late_status & crate::msgr2::frames::FRAME_LATE_STATUS_ABORTED_MASK)
                == crate::msgr2::frames::FRAME_LATE_STATUS_ABORTED
            {
                return Err(Error::Protocol(
                    "Frame transmission aborted by sender".to_string(),
                ));
            }
            offset += 1;

            // Rev1: 3 CRCs for segments 1-3 (seg 0 verified inline above),
            //       stored as !crc32c(segment).
            // Rev0: 4 CRCs for segments 0-3, stored as plain crc32c(segment).
            let (crc_start_seg, num_epilogue_crcs, invert) = if is_rev1 {
                (1usize, crate::msgr2::frames::MAX_NUM_SEGMENTS - 1, true)
            } else {
                (0usize, crate::msgr2::frames::MAX_NUM_SEGMENTS, false)
            };
            for seg_idx in crc_start_seg..crc_start_seg + num_epilogue_crcs {
                let Some(crc_bytes) = full_payload
                    .get(offset..offset + crate::msgr2::frames::FRAME_CRC_SIZE)
                    .and_then(|s| <[u8; 4]>::try_from(s).ok())
                else {
                    break;
                };
                let received_crc = u32::from_le_bytes(crc_bytes);
                offset += crate::msgr2::frames::FRAME_CRC_SIZE;

                if seg_idx < segments.len() && !segments[seg_idx].is_empty() {
                    let computed = crc32c::crc32c(&segments[seg_idx]);
                    let expected_crc = if invert { !computed } else { computed };
                    if received_crc != expected_crc {
                        return Err(Error::Protocol(format!(
                            "Segment {} CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                            seg_idx, expected_crc, received_crc
                        )));
                    }
                }
            }
        }

        Ok(Frame { preamble, segments })
    }
}

/// Connection state coordination layer
pub(crate) struct ConnectionState {
    state_machine: StateMachine,
    io: FrameIO,
    /// Outgoing message sequence number (incremented for each message sent)
    out_seq: u64,
    /// Incoming message sequence number (last received from peer)
    in_seq: u64,
    /// Session cookies for reconnection
    session: SessionState,
    /// Connection policy — true means lossy (no replay queue, no reconnect).
    /// Mirrors `Policy::lossy` in the C++ messenger.  Set once from
    /// `ConnectionConfig::is_lossy` and never changes for the life of the
    /// connection object.
    is_lossy: bool,
}

/// Session state preserved across reconnections
#[derive(Debug, Clone)]
struct SessionState {
    /// Client cookie - generated on first connection, identifies this client instance
    client_cookie: u64,
    /// Server cookie - assigned by server, identifies the session at server side
    server_cookie: u64,
    /// Global sequence number - monotonically increasing across all connections
    global_seq: u64,
    /// Connection sequence number - incremented on each reconnection attempt
    connect_seq: u64,
    /// Queue of sent messages awaiting acknowledgment (for replay on reconnect)
    replay: crate::msgr2::replay_queue::ReplayQueue,
}

impl ConnectionState {
    /// Create a new connection state with the given stream and state machine.
    ///
    /// When `is_lossy` is true, client_cookie is set to 0 so the server
    /// knows not to keep session state (mirrors C++ `lossy_client` policy).
    fn new(stream: TcpStream, state_machine: StateMachine, is_lossy: bool) -> Self {
        // Lossy connections send client_cookie = 0; lossless get a random one.
        let client_cookie = if is_lossy { 0 } else { rand::random::<u64>() };

        Self {
            state_machine,
            io: FrameIO::new(stream),
            out_seq: 0, // Starts at 0, first message will get seq=1
            in_seq: 0,
            session: SessionState {
                client_cookie,
                server_cookie: 0, // Will be assigned by server
                global_seq: 0,
                connect_seq: 0,
                replay: crate::msgr2::replay_queue::ReplayQueue::new(None),
            },
            is_lossy,
        }
    }

    /// Send a frame over the wire.
    pub(crate) async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        self.io.send_frame(frame, &mut self.state_machine).await
    }

    /// Receive a frame from the wire.
    pub(crate) async fn recv_frame(&mut self) -> Result<Frame> {
        self.io.recv_frame(&mut self.state_machine).await
    }

    /// Record that a Keepalive2Ack was received.
    pub(crate) fn record_keepalive_ack(&mut self) {
        self.state_machine.record_keepalive_ack();
    }

    /// Shut down the writer half of the underlying TCP stream so the
    /// peer observes a clean FIN instead of a RST when the connection
    /// is dropped afterward.
    pub(crate) async fn close(&mut self) -> Result<()> {
        self.io.shutdown().await
    }

    /// Drive a phase to completion, handling the enter / recv / send loop.
    pub(crate) async fn drive_phase<P: crate::msgr2::phase::Phase>(
        &mut self,
        phase: P,
    ) -> Result<P::Output> {
        crate::msgr2::phase::drive(&mut self.io, &mut self.state_machine, phase).await
    }

    /// Get the current state name
    fn current_state_name(&self) -> &str {
        self.state_machine.current_state_name()
    }

    /// Get the current state kind
    fn current_state_kind(&self) -> StateKind {
        self.state_machine.current_state_kind()
    }

    // Session state management methods

    /// Get the client cookie for this connection
    pub(crate) fn client_cookie(&self) -> u64 {
        self.session.client_cookie
    }

    /// Get the server cookie (0 if not yet assigned)
    pub(crate) fn server_cookie(&self) -> u64 {
        self.session.server_cookie
    }

    /// Check if this connection has a valid session that can be reconnected
    pub(crate) fn can_reconnect(&self) -> bool {
        !self.is_lossy && self.session.server_cookie != 0
    }

    /// Set the incoming message sequence number
    pub(crate) fn set_in_seq(&mut self, seq: u64) {
        self.in_seq = seq;
    }

    /// Record a sent message for potential replay
    pub(crate) fn record_sent_message(&mut self, message: Message) -> Result<()> {
        if self.is_lossy {
            return Ok(());
        }
        self.session.replay.record(message)
    }

    /// Discard acknowledged messages up to the given sequence number
    pub(crate) fn discard_acknowledged_messages(&mut self, ack_seq: u64) {
        self.session.replay.discard_acked(ack_seq);
    }

    /// Clear the sent message queue
    pub(crate) fn clear_sent_messages(&mut self) {
        self.session.replay.clear();
    }

    /// Reset session state for a new connection
    pub(crate) fn reset_session(&mut self) {
        self.session.server_cookie = 0;
        self.session.global_seq = 0;
        self.session.connect_seq = 0;
        self.session.replay.clear();
        self.in_seq = 0;
        self.out_seq = 0;
        // Note: client_cookie is preserved across resets
    }

    /// Post-auth coordinator: compute HMAC signatures, commit auth state, and
    /// optionally install encryption. Returns `(our_signature, expected_peer_signature)`.
    fn complete_auth(
        &mut self,
        auth_out: &crate::msgr2::phase::auth::AuthOutput,
    ) -> Result<(Bytes, Bytes)> {
        // Compute HMAC-SHA256 signatures over pre-auth byte streams.
        let our_sig = StateMachine::hmac_sha256(
            auth_out.session_key.as_ref(),
            self.state_machine.get_pre_auth_rxbuf(),
        )?;
        let exp_sig = StateMachine::hmac_sha256(
            auth_out.session_key.as_ref(),
            self.state_machine.get_pre_auth_txbuf(),
        )?;

        // Commit auth state and install encryption.
        self.state_machine
            .complete_pre_auth(auth_out.session_key.clone());
        self.state_machine
            .set_connection_mode(auth_out.connection_mode);
        self.state_machine
            .set_connection_secret(auth_out.connection_secret.clone());
        if let Some(ref secret) = auth_out.connection_secret {
            self.state_machine.setup_encryption(secret)?;
        }

        Ok((our_sig, exp_sig))
    }
}

/// High-level msgr2 Protocol V2 connection
///
/// This provides an async API for establishing msgr2 connections,
/// handling authentication, and exchanging messages.
pub struct Connection {
    state: ConnectionState,
    /// Server address for reconnection
    server_addr: SocketAddr,
    /// Target entity address (for connect_with_target), if applicable
    target_entity_addr: Option<crate::EntityAddr>,
    /// Connection configuration
    config: crate::msgr2::ConnectionConfig,
    /// Optional message throttle for rate limiting
    throttle: Option<crate::msgr2::throttle::MessageThrottle>,
}

/// Snapshot of runtime connection state for diagnostics and debugging.
///
/// Obtained via [`Connection::diagnostics`]. All fields reflect the state at
/// the moment the snapshot is taken; they are not updated live.
#[derive(Debug, Clone)]
pub struct ConnectionDiagnostics {
    /// Current state-machine phase
    pub state_kind: crate::msgr2::state_machine::StateKind,
    /// Whether frames are encrypted (secure mode)
    pub encryption_enabled: bool,
    /// Active compression algorithm, or `None` if compression is not in use
    pub compression_algorithm: Option<crate::msgr2::compression::CompressionAlgorithm>,
    /// Outgoing sequence number (number of messages sent so far)
    pub out_seq: u64,
    /// Incoming sequence number (last acknowledged from peer)
    pub in_seq: u64,
    /// Number of sent messages awaiting acknowledgment (for reconnect replay)
    pub sent_messages_pending: usize,
    /// Client cookie generated at connection creation
    pub client_cookie: u64,
    /// Server cookie assigned by the peer (0 if session not yet established)
    pub server_cookie: u64,
    /// Global sequence number (monotonically increasing across reconnections)
    pub global_seq: u64,
    /// Connection sequence number (incremented on each reconnect attempt)
    pub connect_seq: u64,
    /// Global ID assigned by the monitor during CephX authentication (0 if not authenticated)
    pub global_id: u64,
    /// Timestamp of the most recent keepalive acknowledgment received
    pub last_keepalive_ack: Option<std::time::Instant>,
    /// Whether the connection policy is lossy (no reconnection support)
    pub is_lossy: bool,
    /// Whether a transparent reconnect is possible (non-lossy + server cookie present)
    pub can_reconnect: bool,
}

impl Connection {
    fn socket_to_entity_addr(addr: SocketAddr) -> crate::EntityAddr {
        crate::EntityAddr::from_socket_addr(crate::EntityAddrType::Msgr2, addr)
    }

    /// Configure a state machine with server and client addresses
    ///
    /// This helper is used by both connect() and reconnect() to set up
    /// the state machine with proper EntityAddr values.
    ///
    /// # Arguments
    /// * `state_machine` - The state machine to configure
    /// * `server_addr` - The server's TCP address
    /// * `local_addr` - The local TCP address
    /// * `target_entity_addr` - Optional target EntityAddr (for OSD connections with nonce)
    fn configure_state_machine_addresses(
        state_machine: &mut StateMachine,
        server_addr: SocketAddr,
        local_addr: SocketAddr,
        target_entity_addr: Option<&crate::EntityAddr>,
    ) {
        // Set server address
        if let Some(target_addr) = target_entity_addr {
            state_machine.set_server_addr(*target_addr);
        } else {
            state_machine.set_server_addr(Self::socket_to_entity_addr(server_addr));
        }

        // Set client address
        state_machine.set_client_addr(Self::socket_to_entity_addr(local_addr));
    }

    /// Connect to a Ceph server with a specific target EntityAddr
    ///
    /// This establishes a TCP connection and performs the msgr2 banner exchange.
    /// Use this when connecting to an OSD where you need the exact EntityAddr (with nonce)
    /// from the OSDMap for CLIENT_IDENT.
    ///
    /// # Arguments
    /// * `addr` - The TCP socket address to connect to
    /// * `target_entity_addr` - The full EntityAddr (with nonce) to use as target in CLIENT_IDENT
    /// * `config` - Connection configuration (features and connection modes)
    pub async fn connect_with_target(
        addr: SocketAddr,
        target_entity_addr: crate::EntityAddr,
        config: crate::msgr2::ConnectionConfig,
    ) -> Result<Self> {
        tracing::debug!(
            "Connection::connect_with_target() called with addr: {}, target nonce: {}",
            addr,
            target_entity_addr.nonce
        );
        Self::connect_inner(addr, Some(target_entity_addr), config).await
    }

    /// Connect to a Ceph server at the given address
    ///
    /// This establishes a TCP connection and performs the msgr2 banner exchange.
    ///
    /// # Arguments
    /// * `addr` - The server address to connect to
    /// * `config` - Connection configuration (features and connection modes)
    pub async fn connect(addr: SocketAddr, config: crate::msgr2::ConnectionConfig) -> Result<Self> {
        tracing::debug!("Connection::connect() called with addr: {}", addr);
        Self::connect_inner(addr, None, config).await
    }

    /// Shared implementation for `connect()` and `connect_with_target()`.
    async fn connect_inner(
        addr: SocketAddr,
        target_entity_addr: Option<crate::EntityAddr>,
        config: crate::msgr2::ConnectionConfig,
    ) -> Result<Self> {
        // Validate config
        if let Err(e) = config.validate() {
            return Err(Error::Protocol(format!("Invalid config: {}", e)));
        }

        // Establish TCP connection
        let mut stream = TcpStream::connect(addr).await?;
        // Ceph's msgr2 sets ms_tcp_nodelay=true by default.  Disabling
        // Nagle's algorithm is critical for latency: without it the kernel
        // may buffer small control messages for up to 40ms.
        stream.set_nodelay(true)?;
        let local_addr = stream.local_addr()?;
        tracing::info!(
            "TCP connection established to {} (local: {})",
            addr,
            local_addr
        );

        // Create state machine for client BEFORE banner exchange
        // This is important because we need to track banner bytes in pre-auth buffers
        let mut state_machine = StateMachine::new_client(config.clone());

        // Configure addresses
        Self::configure_state_machine_addresses(
            &mut state_machine,
            addr,
            local_addr,
            target_entity_addr.as_ref(),
        );

        // Perform msgr2 banner exchange and record bytes in pre-auth buffers
        Self::exchange_banner(&mut stream, &mut state_machine, &config).await?;

        let mut state = ConnectionState::new(stream, state_machine, config.is_lossy);
        state.session.replay =
            crate::msgr2::replay_queue::ReplayQueue::new(config.max_replay_queue_len);

        // Initialize throttle from config if present
        let throttle = config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::msgr2::throttle::MessageThrottle::new(cfg.clone()));

        Ok(Self {
            state,
            server_addr: addr,
            target_entity_addr,
            config,
            throttle,
        })
    }

    /// Send our msgr2 banner with the configured feature sets.
    async fn send_banner(
        stream: &mut TcpStream,
        state_machine: &mut StateMachine,
        config: &crate::msgr2::ConnectionConfig,
    ) -> Result<()> {
        let banner = Banner::new_with_features(
            FeatureSet::from(config.supported_features),
            FeatureSet::from(config.required_features),
        );
        let mut buf = BytesMut::with_capacity(64);
        banner.encode(&mut buf)?;
        state_machine.record_sent(&buf);
        stream.write_all(&buf).await?;
        stream.flush().await?;
        tracing::info!(
            "Sent msgr2 banner: supported={:x}, required={:x}",
            u64::from(banner.supported_features),
            u64::from(banner.required_features)
        );
        Ok(())
    }

    /// Receive a peer msgr2 banner, check feature compatibility, and record peer features.
    ///
    /// `peer_role` is used only in log messages ("client" or "server").
    async fn recv_banner(
        stream: &mut TcpStream,
        state_machine: &mut StateMachine,
        config: &crate::msgr2::ConnectionConfig,
        peer_role: &str,
    ) -> Result<()> {
        // Banner is always 26 bytes: 8 prefix + 2 length + 16 payload
        let mut buf = vec![0u8; 26];
        stream.read_exact(&mut buf).await?;
        state_machine.record_received(&buf);

        let mut bytes = BytesMut::from(&buf[..]);
        let peer_banner = Banner::decode(&mut bytes)?;
        tracing::info!(
            "Received {} banner: supported={:x}, required={:x}",
            peer_role,
            u64::from(peer_banner.supported_features),
            u64::from(peer_banner.required_features)
        );

        let our_features = FeatureSet::from(config.supported_features);
        let missing = peer_banner.required_features & !our_features;
        if !missing.is_empty() {
            return Err(Error::Protocol(format!(
                "Missing required features: {:x}",
                u64::from(missing)
            )));
        }
        state_machine.set_peer_supported_features(u64::from(peer_banner.supported_features));
        Ok(())
    }

    /// Perform msgr2 banner exchange (client-side): send first, then receive.
    async fn exchange_banner(
        stream: &mut TcpStream,
        state_machine: &mut StateMachine,
        config: &crate::msgr2::ConnectionConfig,
    ) -> Result<()> {
        Self::send_banner(stream, state_machine, config).await?;
        Self::recv_banner(stream, state_machine, config, "server").await
    }

    /// Perform msgr2 banner exchange (server-side): receive first, then send.
    async fn exchange_banner_server(
        stream: &mut TcpStream,
        state_machine: &mut StateMachine,
        config: &crate::msgr2::ConnectionConfig,
    ) -> Result<()> {
        Self::recv_banner(stream, state_machine, config, "client").await?;
        Self::send_banner(stream, state_machine, config).await
    }

    /// Accept an incoming msgr2 connection (server-side)
    ///
    /// This accepts a TCP connection from a client and performs the msgr2 banner exchange.
    /// After this, call `accept_session()` to complete the handshake.
    ///
    /// # Arguments
    /// * `stream` - The accepted TCP stream from TcpListener
    /// * `config` - Connection configuration (features and connection modes)
    /// * `auth_handler` - Optional server-side authentication handler
    pub async fn accept(
        mut stream: TcpStream,
        config: crate::msgr2::ConnectionConfig,
        auth_handler: Option<crate::auth::CephXServerHandler>,
    ) -> Result<Self> {
        let peer_addr = stream.peer_addr()?;
        let local_addr = stream.local_addr()?;

        tracing::info!("Accepting connection from {}", peer_addr);
        tracing::debug!("Local address: {}", local_addr);

        // Validate config
        if let Err(e) = config.validate() {
            return Err(Error::Protocol(format!("Invalid config: {}", e)));
        }

        // Create state machine for server BEFORE banner exchange
        // This is important because we need to track banner bytes in pre-auth buffers
        let mut state_machine = StateMachine::new_server_with_auth(auth_handler);

        // Set addresses using the shared conversion helper
        state_machine.set_client_addr(Self::socket_to_entity_addr(peer_addr));
        state_machine.set_server_addr(Self::socket_to_entity_addr(local_addr));

        tracing::debug!("Created server state machine");

        // Perform msgr2 banner exchange (server-side: receive first, then send)
        Self::exchange_banner_server(&mut stream, &mut state_machine, &config).await?;

        let mut state = ConnectionState::new(stream, state_machine, config.is_lossy);
        state.session.replay =
            crate::msgr2::replay_queue::ReplayQueue::new(config.max_replay_queue_len);

        // Initialize throttle from config if present
        let throttle = config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::msgr2::throttle::MessageThrottle::new(cfg.clone()));

        Ok(Self {
            state,
            server_addr: local_addr, // For server, this is our local address
            target_entity_addr: None,
            config,
            throttle,
        })
    }

    /// Accept a session by completing the full msgr2 handshake (server-side)
    ///
    /// Drives the following phase sequence:
    /// 1. `HelloServer`  — receive HELLO, send HELLO
    /// 2. `AuthServer`   — receive AUTH_REQUEST, send AUTH_DONE (with optional CephX round)
    /// 3. Coordinator    — compute HMAC-SHA256, install encryption
    /// 4. `AuthSignServer` — send server signature, verify client signature
    /// 5. `CompressionServer` (optional) — negotiate compression
    /// 6. `SessionServer` — receive CLIENT_IDENT, send SERVER_IDENT
    ///
    /// Returns when the connection is ready for message exchange.
    pub async fn accept_session(&mut self) -> Result<()> {
        use crate::msgr2::{
            compression::CompressionAlgorithm,
            phase::{
                auth::AuthServer, compression::CompressionServer, hello::HelloServer,
                session::SessionServer, sign::AuthSignServer,
            },
        };

        tracing::info!("Accepting msgr2 session...");

        // Phase 1: HELLO exchange (server receives, then sends).
        // peer_addr = client's address as seen by us → enables address
        // learning on the client side (NAT traversal).
        // This server role is test scaffolding; default to MON so the
        // reply advertises a plausible daemon type.
        let client_addr = self.state.state_machine.client_addr_clone();
        self.state
            .drive_phase(HelloServer::new(crate::EntityType::MON, client_addr))
            .await?;

        // Phase 2: Authentication
        let auth_handler = self.state.state_machine.take_server_auth_handler();
        let auth_out = self
            .state
            .drive_phase(AuthServer::new(auth_handler))
            .await?;

        // Coordinator: compute HMAC signatures, commit auth state, install encryption.
        let (our_sig, exp_sig) = self.state.complete_auth(&auth_out)?;

        // Phase 3: Auth-signature exchange (server sends first)
        self.state
            .drive_phase(AuthSignServer::new(our_sig, exp_sig))
            .await?;

        // Phase 4: Compression (only when both peers advertise it)
        if self.state.state_machine.compression_negotiation_needed() {
            let comp_out = self.state.drive_phase(CompressionServer).await?;
            if comp_out.algorithm != CompressionAlgorithm::None {
                self.state
                    .state_machine
                    .setup_compression(comp_out.algorithm);
            }
        }

        // Phase 5: Session establishment. Pass our own advertised
        // address so SessionServer can validate the client's target_addr
        // and echo a correctly-populated SERVER_IDENT addrs field —
        // without this the client's peer-address validation would reject
        // the connection because the server would previously have sent
        // EntityAddr::default() as its addrs.
        let server_cookie: u64 = rand::random();
        let server_addr = self.state.state_machine.server_addr_clone();
        self.state
            .drive_phase(SessionServer::new(server_cookie, server_addr))
            .await?;

        self.state.state_machine.transition_to_ready(0);

        tracing::info!("Session established (server-side)");
        Ok(())
    }

    /// Establish a session by completing the full msgr2 handshake (client-side)
    ///
    /// Drives the following phase sequence:
    /// 1. `HelloClient`  — send HELLO, receive HELLO
    /// 2. `AuthClient`   — AUTH_REQUEST ↔ AUTH_DONE exchange (with optional retries)
    /// 3. Coordinator    — compute HMAC-SHA256, install encryption
    /// 4. `AuthSignClient` — send client signature, verify server signature
    /// 5. `CompressionClient` (optional) — negotiate compression
    /// 6. `SessionClient` — send CLIENT_IDENT / SESSION_RECONNECT, receive SERVER_IDENT
    ///
    /// Returns when the connection is ready for message exchange.
    pub async fn establish_session(&mut self) -> Result<()> {
        use crate::msgr2::{
            compression::CompressionAlgorithm,
            phase::{
                auth::{AuthClient, AuthOutput},
                compression::CompressionClient,
                hello::HelloClient,
                session::SessionClient,
                sign::AuthSignClient,
            },
        };

        tracing::info!("Establishing msgr2 session...");

        // Snapshot config data before any mutable operations.
        let preferred_modes = self.state.state_machine.config_preferred_modes().to_vec();
        let supported_auth_methods = self
            .state
            .state_machine
            .config_supported_auth_methods()
            .to_vec();
        let auth_provider = self.state.state_machine.get_auth_provider();
        let service_id = self.state.state_machine.config_service_id();
        let entity_name = self.state.state_machine.config_entity_name().clone();
        let client_cookie = self.state.session.client_cookie;

        // Phase 1: HELLO exchange.
        // peer_addr = server's address as we dialed it → enables address
        // learning on the server side (NAT traversal).
        let server_addr = self.state.state_machine.server_addr_clone();
        self.state
            .drive_phase(HelloClient::new(server_addr))
            .await?;

        // Phase 2: Authentication
        let initial_global_id = self.state.state_machine.global_id();
        let auth_out: AuthOutput = self
            .state
            .drive_phase(AuthClient::new(
                preferred_modes,
                supported_auth_methods,
                auth_provider,
                service_id,
                entity_name,
                initial_global_id,
            ))
            .await?;

        // Coordinator: compute HMAC signatures, commit auth state, install encryption.
        let (our_sig, exp_sig) = self.state.complete_auth(&auth_out)?;
        self.state.state_machine.set_global_id(auth_out.global_id);

        // Phase 3: Auth-signature exchange (client sends first)
        let peer_features = self.state.state_machine.peer_supported_features();
        let our_features = self.state.state_machine.our_supported_features();
        let sign_out = self
            .state
            .drive_phase(AuthSignClient::new(
                our_sig,
                exp_sig,
                peer_features,
                our_features,
            ))
            .await?;

        // Phase 4: Compression (only when both peers advertise it)
        if sign_out.needs_compression {
            let comp_out = self.state.drive_phase(CompressionClient).await?;
            if comp_out.algorithm != CompressionAlgorithm::None {
                self.state
                    .state_machine
                    .setup_compression(comp_out.algorithm);
            }
        }

        // Phase 5: Session establishment (CLIENT_IDENT or SESSION_RECONNECT)
        let server_cookie = self.state.session.server_cookie;
        let global_seq = self.state.session.global_seq;
        let connect_seq = self.state.session.connect_seq;
        let in_seq = self.state.in_seq;
        let global_id = self.state.state_machine.global_id();
        let server_addr = self.state.state_machine.server_addr_clone();
        let client_addr = self.state.state_machine.client_addr_clone();

        let sess_out = self
            .state
            .drive_phase(SessionClient::new(
                global_id,
                server_addr,
                client_addr,
                client_cookie,
                server_cookie,
                global_seq,
                connect_seq,
                in_seq,
                self.state.is_lossy,
            ))
            .await?;

        // Apply session results.
        self.state.session.server_cookie = sess_out.server_cookie;

        // Adopt the server-declared lossy policy. SERVER_IDENT's flags
        // are authoritative per Ceph C++ (ProtocolV2.cc:2171) and the
        // Linux kernel (messenger_v2.c:2535-2540); whatever the server
        // echoes back in response to our CLIENT_IDENT is the policy the
        // connection actually runs with. `SessionReconnectOk` carries no
        // ident, so on reconnection we leave `is_lossy` unchanged and
        // trust the policy established during the original handshake.
        if let Some(server_is_lossy) = sess_out.server_is_lossy
            && server_is_lossy != self.state.is_lossy
        {
            tracing::info!(
                "Adopting server's lossy policy: was {}, now {}",
                self.state.is_lossy,
                server_is_lossy
            );
            self.state.is_lossy = server_is_lossy;
        }

        self.state
            .state_machine
            .transition_to_ready(sess_out.negotiated_features);

        if let Some(msg_seq) = sess_out.reconnect_msg_seq {
            tracing::info!("Session reconnected, server acked up to msg_seq={msg_seq}");
            self.state.discard_acknowledged_messages(msg_seq);
            let messages_to_replay: Vec<_> = self.state.session.replay.iter().cloned().collect();
            if !messages_to_replay.is_empty() {
                tracing::info!(
                    "Replaying {} unacknowledged messages",
                    messages_to_replay.len()
                );
                for msg in messages_to_replay {
                    let msg_frame = MessageFrame::new(msg.header, msg.front, msg.middle, msg.data);
                    let frame = create_frame_from_trait(&msg_frame, Tag::Message)?;
                    self.state.send_frame(&frame).await?;
                }
            }
        }

        tracing::info!("Session established!");
        Ok(())
    }

    /// Attempt to reconnect after TCP connection failure
    ///
    /// This preserves the session state (cookies, sequences, sent messages)
    /// and attempts to resume the session using SESSION_RECONNECT.
    async fn reconnect(&mut self) -> Result<()> {
        // Check if we can reconnect (need server_cookie and non-lossy policy)
        if !self.state.can_reconnect() {
            return Err(Error::Protocol(
                "Cannot reconnect: lossy connection or no session established".to_string(),
            ));
        }

        tracing::warn!(
            "Connection lost, attempting to reconnect to {}",
            self.server_addr
        );

        // Preserve session state from current connection
        let client_cookie = self.state.client_cookie();
        let server_cookie = self.state.server_cookie();
        let global_seq = self.state.session.global_seq;
        let connect_seq = self.state.session.connect_seq + 1; // Increment for new attempt
        let in_seq = self.state.in_seq;
        let out_seq = self.state.out_seq;
        let replay = std::mem::take(&mut self.state.session.replay);

        tracing::debug!(
            "Reconnecting with: client_cookie={}, server_cookie={}, global_seq={}, connect_seq={}, in_seq={}, out_seq={}, queued_messages={}",
            client_cookie,
            server_cookie,
            global_seq,
            connect_seq,
            in_seq,
            out_seq,
            replay.len()
        );

        // Establish new TCP connection
        let mut stream = TcpStream::connect(self.server_addr).await?;
        stream.set_nodelay(true)?;
        tracing::info!("TCP reconnection established to {}", self.server_addr);

        // Get local address for CLIENT_IDENT
        let local_addr = stream.local_addr()?;

        // Create new state machine for the new TCP connection
        let mut state_machine = StateMachine::new_client(self.config.clone());

        // Set server and client addresses
        Self::configure_state_machine_addresses(
            &mut state_machine,
            self.server_addr,
            local_addr,
            self.target_entity_addr.as_ref(),
        );

        // Perform banner exchange
        Self::exchange_banner(&mut stream, &mut state_machine, &self.config).await?;

        // Create new ConnectionState (reconnect is only reachable for lossless)
        let mut state = ConnectionState::new(stream, state_machine, false);

        // Restore session state (cookies and seqs must survive across the new TCP connection)
        state.session.client_cookie = client_cookie;
        state.session.server_cookie = server_cookie;
        state.session.global_seq = global_seq;
        state.session.connect_seq = connect_seq;
        state.out_seq = out_seq;
        state.in_seq = in_seq;
        state.session.replay = replay;

        // Replace current state with new reconnected state
        self.state = state;

        // Reinitialize throttle for new connection
        self.throttle = self
            .config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::msgr2::throttle::MessageThrottle::new(cfg.clone()));

        tracing::info!("Session state restored, initiating reconnection handshake");

        // Run the full session establishment (this will use SESSION_RECONNECT since server_cookie != 0)
        self.establish_session().await?;

        tracing::info!("Reconnection complete, session resumed");

        Ok(())
    }

    /// Execute an operation with automatic retry and reconnection on recoverable errors
    ///
    /// This generic method handles the common pattern of:
    /// 1. Try the operation
    /// 2. If it fails with a recoverable error, attempt reconnection
    /// 3. Retry the operation after successful reconnection
    /// 4. Repeat up to MAX_RECONNECT_ATTEMPTS times
    ///
    /// # Arguments
    /// * `operation` - The async operation to execute
    /// * `operation_name` - Name of the operation for logging (e.g., "send", "receive")
    /// * `on_reconnect` - Callback after successful reconnection, returns whether to continue retrying
    ///
    /// # Returns
    /// The result of the operation, or an error if all retries are exhausted
    async fn retry_with_reconnect<F, T, R>(
        &mut self,
        mut operation: F,
        operation_name: &str,
        on_reconnect: R,
    ) -> Result<T>
    where
        F: FnMut(
            &mut Self,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + '_>>,
        R: Fn() -> ReconnectAction<T>,
    {
        const MAX_RECONNECT_ATTEMPTS: usize = 3;
        const INITIAL_BACKOFF_MS: u64 = 100;
        const MAX_BACKOFF_MS: u64 = 1000;
        const JITTER_PERCENT: f64 = 0.3; // ±30% jitter

        // The loop allows 1 initial try + MAX_RECONNECT_ATTEMPTS retries after reconnect.
        for attempt in 0..=MAX_RECONNECT_ATTEMPTS {
            match operation(self).await {
                Ok(result) => return Ok(result),
                Err(e)
                    if e.is_recoverable()
                        && self.state.can_reconnect()
                        && attempt < MAX_RECONNECT_ATTEMPTS =>
                {
                    tracing::warn!(
                        "{} failed due to connection error (attempt {}/{}): {}",
                        operation_name,
                        attempt + 1,
                        MAX_RECONNECT_ATTEMPTS + 1,
                        e
                    );

                    // Calculate exponential backoff with jitter before reconnection attempt
                    // Skip backoff on first attempt (attempt == 0)
                    if attempt > 0 {
                        // Exponential backoff: 100ms, 200ms, 400ms, ...
                        let base_backoff_ms =
                            (INITIAL_BACKOFF_MS * (1_u64 << (attempt - 1))).min(MAX_BACKOFF_MS);

                        // Add jitter: ±30% randomness to prevent thundering herd
                        // Following best practices for distributed systems
                        use rand::Rng;
                        let jitter = rand::thread_rng().gen_range(-JITTER_PERCENT..=JITTER_PERCENT);
                        let jittered_backoff_ms = (base_backoff_ms as f64 * (1.0 + jitter)) as u64;

                        tracing::debug!(
                            "Backing off for {}ms (base: {}ms, jitter: {:.1}%) before attempt {}",
                            jittered_backoff_ms,
                            base_backoff_ms,
                            jitter * 100.0,
                            attempt + 1
                        );

                        tokio::time::sleep(tokio::time::Duration::from_millis(jittered_backoff_ms))
                            .await;
                    }

                    // Attempt reconnection
                    if let Err(reconnect_err) = self.reconnect().await {
                        tracing::error!("Reconnection failed: {}", reconnect_err);
                        // If this was the last reconnect attempt, return the error
                        if attempt + 1 >= MAX_RECONNECT_ATTEMPTS {
                            return Err(reconnect_err);
                        }
                        // Otherwise, continue to next attempt
                        continue;
                    }

                    tracing::info!("Reconnection successful after {}", operation_name);

                    // Let caller decide what to do after reconnection
                    match on_reconnect() {
                        ReconnectAction::ReturnSuccess(value) => return Ok(value),
                        ReconnectAction::Retry => continue,
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::Protocol(format!(
            "Failed to {} after {} attempts",
            operation_name, MAX_RECONNECT_ATTEMPTS
        )))
    }

    /// Send a Ceph message over the established session
    ///
    /// This method automatically handles connection failures by attempting
    /// reconnection and message replay according to the msgr2 protocol.
    pub async fn send_message(&mut self, msg: Message) -> Result<()> {
        if self.state.is_lossy {
            // Lossy connections never reconnect: move msg directly, no clone needed.
            return self.send_message_inner(msg).await;
        }
        self.retry_with_reconnect(
            |conn| Box::pin(conn.send_message_inner(msg.clone())),
            "send",
            || {
                // After reconnection, the message will be replayed automatically
                // from the sent_messages queue, so we can return success immediately
                ReconnectAction::ReturnSuccess(())
            },
        )
        .await
    }

    /// Internal implementation of send_message without reconnection logic
    async fn send_message_inner(&mut self, mut msg: Message) -> Result<()> {
        let msg_type = msg.msg_type();
        let msg_size = msg.total_len();

        // Wait for throttle if configured
        if let Some(throttle) = &self.throttle {
            throttle.wait_for_send(msg_size).await;
        }

        // Assign sequence numbers (pre-increment, like C++ does with ++out_seq)
        self.state.out_seq += 1;
        msg.header.set_seq(self.state.out_seq);
        msg.header.set_ack_seq(self.state.in_seq);

        let seq = msg.seq();
        let ack_seq = msg.header.get_ack_seq();
        tracing::debug!(
            "Sending message: type=0x{:04x}, seq={}, ack_seq={}, front={}, middle={}, data={}",
            msg_type,
            seq,
            ack_seq,
            msg.front.len(),
            msg.middle.len(),
            msg.data.len(),
        );

        // Clone into the replay queue before sending.  Guard avoids the clone
        // for lossy connections (record_sent_message is also safe to call, but
        // the clone itself is the expensive part we want to skip).
        if !self.state.is_lossy {
            self.state.record_sent_message(msg.clone())?;
        }

        // Convert Message to MessageFrame and send (move fields from owned msg)
        let msg_frame = MessageFrame::new(msg.header, msg.front, msg.middle, msg.data);
        let frame = create_frame_from_trait(&msg_frame, Tag::Message)?;
        self.state.send_frame(&frame).await?;

        // Record send with throttle
        if let Some(throttle) = &self.throttle {
            throttle.record_send(msg_size).await;
        }

        Ok(())
    }

    /// Receive a Ceph message from the established session
    ///
    /// This method automatically handles connection failures by attempting
    /// reconnection according to the msgr2 protocol.
    pub async fn recv_message(&mut self) -> Result<Message> {
        self.retry_with_reconnect(
            |conn| Box::pin(conn.recv_message_inner()),
            "receive",
            || {
                // After reconnection, we need to retry receiving the message
                ReconnectAction::Retry
            },
        )
        .await
    }

    /// Internal implementation of recv_message without reconnection logic
    async fn recv_message_inner(&mut self) -> Result<Message> {
        // Loop until we get a Message frame, handling ACKs along the way
        loop {
            // Receive a frame
            let frame = self.state.recv_frame().await?;

            match frame.preamble.tag {
                Tag::Ack => {
                    // Handle ACK frame - decode sequence number and continue
                    if frame.segments.is_empty() {
                        return Err(Error::protocol_error("ACK frame missing payload"));
                    }

                    // ACK frame contains a single uint64_t sequence number
                    let mut payload = frame.segments[0].as_ref();
                    let ack_seq = u64::decode(&mut payload, 0)?;

                    tracing::debug!("Received ACK frame: seq={}", ack_seq);

                    // Discard acknowledged messages from sent queue (following Ceph's practice)
                    self.state.discard_acknowledged_messages(ack_seq);

                    // Record ACK with throttle to release queue depth slot
                    if let Some(throttle) = &self.throttle {
                        throttle.record_ack().await;
                    }

                    // Continue loop to wait for Message frame
                    continue;
                }
                Tag::Message => {
                    let msg = Message::from_frame_segments(&frame.segments)?;
                    let msg_seq = msg.header.get_seq();
                    let ack_seq = msg.header.get_ack_seq();

                    tracing::debug!(
                        "Received message: type={}, seq={}, ack_seq={}",
                        msg.msg_type(),
                        msg_seq,
                        ack_seq
                    );

                    // Update in_seq to the received message's sequence (following Ceph's practice)
                    self.state.set_in_seq(msg_seq);

                    // Process piggybacked ACK in message header (following Ceph's practice)
                    if ack_seq > 0 {
                        self.state.discard_acknowledged_messages(ack_seq);
                    }

                    return Ok(msg);
                }
                Tag::Keepalive2Ack => {
                    self.state.record_keepalive_ack();
                    tracing::trace!("Received Keepalive2Ack frame, updated last_keepalive_ack");
                    continue;
                }
                _ => {
                    return Err(Error::protocol_error(&format!(
                        "Unexpected frame type in recv_message: {:?}",
                        frame.preamble.tag
                    )));
                }
            }
        }
    }

    /// Get a clone of the authenticated auth provider
    ///
    /// This should be called after `establish_session()` to get an auth provider
    /// that contains the session and service tickets obtained during authentication.
    pub fn get_auth_provider(&self) -> Option<Box<dyn crate::auth::AuthProvider>> {
        self.state.state_machine.get_auth_provider()
    }

    /// Get the global_id assigned during authentication
    /// Returns 0 if authentication hasn't completed yet
    pub fn global_id(&self) -> u64 {
        self.state.state_machine.global_id()
    }

    /// Get the negotiated Ceph session features from the ident exchange.
    pub fn negotiated_features(&self) -> u64 {
        self.state.state_machine.negotiated_features()
    }

    /// Get the client entity address (our local address as sent in CLIENT_IDENT).
    pub fn client_addr(&self) -> crate::EntityAddr {
        self.state.state_machine.client_addr_clone()
    }

    /// Send a keepalive frame to the peer
    ///
    /// This sends a Keepalive2 frame with the current timestamp.
    /// The peer should respond with a Keepalive2Ack frame.
    pub async fn send_keepalive(&mut self) -> Result<()> {
        use crate::msgr2::frames::Keepalive2Frame;

        let (timestamp_sec, timestamp_nsec) = crate::msgr2::current_keepalive_timestamp()?;

        let keepalive_frame = Keepalive2Frame::new(timestamp_sec, timestamp_nsec);
        let frame = create_frame_from_trait(&keepalive_frame, Tag::Keepalive2)?;

        tracing::trace!(
            "Sending KEEPALIVE2 with timestamp {}.{:09}",
            timestamp_sec,
            timestamp_nsec
        );

        // Send the frame
        self.state.send_frame(&frame).await
    }

    /// Get the last keepalive ACK timestamp
    ///
    /// Returns None if no keepalive ACK has been received yet.
    /// This can be used to detect connection timeouts.
    pub fn last_keepalive_ack(&self) -> Option<std::time::Instant> {
        self.state.state_machine.last_keepalive_ack()
    }

    /// Get the current state name
    pub fn current_state_name(&self) -> &str {
        self.state.current_state_name()
    }

    /// Get the current state kind
    pub fn current_state_kind(&self) -> StateKind {
        self.state.current_state_kind()
    }

    /// Close the connection and discard all pending messages
    ///
    /// This matches Ceph's `discard_out_queue()` behavior:
    /// - Shuts down the TCP write half so the peer observes a clean FIN
    ///   instead of an RST on drop
    /// - Discards all sent messages (they won't be retransmitted)
    /// - Resets the session state
    ///
    /// Errors from the shutdown are logged rather than propagated: this
    /// method runs in shutdown paths where the caller has already
    /// committed to dropping the connection, and a best-effort close is
    /// the only meaningful behaviour.
    ///
    /// Use this when permanently closing a connection (not reconnecting).
    pub async fn close(&mut self) {
        tracing::info!("Closing connection to {}", self.server_addr);

        if let Err(e) = self.state.close().await {
            tracing::warn!(
                "Graceful close of connection to {} reported an error (dropping anyway): {}",
                self.server_addr,
                e
            );
        }

        // Discard all sent messages (won't be retransmitted)
        self.state.clear_sent_messages();

        // Reset session state
        self.state.reset_session();

        tracing::debug!("Connection closed, all pending messages discarded");
    }

    /// Snapshot of runtime connection state for diagnostics and debugging.
    ///
    /// The returned [`ConnectionDiagnostics`] is a point-in-time copy; it is
    /// not updated as the connection state evolves.
    pub fn diagnostics(&self) -> ConnectionDiagnostics {
        ConnectionDiagnostics {
            state_kind: self.state.current_state_kind(),
            encryption_enabled: self.state.state_machine.has_encryption(),
            compression_algorithm: self
                .state
                .state_machine
                .compression_ctx()
                .map(|c| c.algorithm()),
            out_seq: self.state.out_seq,
            in_seq: self.state.in_seq,
            sent_messages_pending: self.state.session.replay.len(),
            client_cookie: self.state.session.client_cookie,
            server_cookie: self.state.session.server_cookie,
            global_seq: self.state.session.global_seq,
            connect_seq: self.state.session.connect_seq,
            global_id: self.state.state_machine.global_id(),
            last_keepalive_ack: self.state.state_machine.last_keepalive_ack(),
            is_lossy: self.state.is_lossy,
            can_reconnect: self.state.can_reconnect(),
        }
    }

    /// Current throttle statistics, or `None` if no throttle is configured.
    pub async fn throttle_stats(&self) -> Option<crate::msgr2::throttle::ThrottleStats> {
        let t = self.throttle.as_ref()?;
        Some(t.stats().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgr2::message::MessagePriority;
    use crate::msgr2::state_machine::StateMachine;

    /// Create a minimal Connection for unit tests using a loopback TCP socket pair.
    async fn make_test_connection() -> Connection {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let sm = StateMachine::new_client(crate::msgr2::ConnectionConfig::with_no_auth());
        let state = ConnectionState::new(stream, sm, false);
        Connection {
            state,
            server_addr: addr,
            target_entity_addr: None,
            config: crate::msgr2::ConnectionConfig::with_no_auth(),
            throttle: None,
        }
    }

    #[tokio::test]
    async fn test_diagnostics_initial_state() {
        let conn = make_test_connection().await;
        let diag = conn.diagnostics();

        assert_eq!(diag.out_seq, 0);
        assert_eq!(diag.in_seq, 0);
        assert_eq!(diag.sent_messages_pending, 0);
        assert_eq!(diag.global_id, 0);
        assert_eq!(diag.global_seq, 0);
        assert_eq!(diag.connect_seq, 0);
        assert_eq!(diag.server_cookie, 0);
        assert!(!diag.encryption_enabled);
        assert!(diag.compression_algorithm.is_none());
        assert!(!diag.can_reconnect); // no server_cookie yet
        assert!(diag.last_keepalive_ack.is_none());
        assert!(!diag.is_lossy);
    }

    #[tokio::test]
    async fn test_throttle_stats_none_without_config() {
        let conn = make_test_connection().await;
        assert!(conn.throttle_stats().await.is_none());
    }

    #[test]
    fn test_priority_queue_ordering() {
        let mut queue = PriorityQueue::new();

        let low_msg = Message::new(1, Bytes::from("low")).with_priority(MessagePriority::Low);
        let normal_msg =
            Message::new(2, Bytes::from("normal")).with_priority(MessagePriority::Normal);
        let high_msg = Message::new(3, Bytes::from("high")).with_priority(MessagePriority::High);

        // Add in random order
        queue.push_back(normal_msg);
        queue.push_back(low_msg);
        queue.push_back(high_msg);

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.pop_front().unwrap().msg_type(), 3); // High first
        assert_eq!(queue.pop_front().unwrap().msg_type(), 2); // Normal second
        assert_eq!(queue.pop_front().unwrap().msg_type(), 1); // Low last
        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_fifo_within_priority() {
        let mut queue = PriorityQueue::new();

        for i in 1u16..=3 {
            queue.push_back(
                Message::new(i, Bytes::from(format!("msg{i}")))
                    .with_priority(MessagePriority::Normal),
            );
        }

        assert_eq!(queue.pop_front().unwrap().msg_type(), 1);
        assert_eq!(queue.pop_front().unwrap().msg_type(), 2);
        assert_eq!(queue.pop_front().unwrap().msg_type(), 3);
    }

    #[test]
    fn test_priority_queue_iter() {
        let mut queue = PriorityQueue::new();

        queue.push_back(Message::new(1, Bytes::new()).with_priority(MessagePriority::Low));
        queue.push_back(Message::new(3, Bytes::new()).with_priority(MessagePriority::High));
        queue.push_back(Message::new(2, Bytes::new()).with_priority(MessagePriority::Normal));

        let types: Vec<u16> = queue.iter().map(|m| m.msg_type()).collect();
        assert_eq!(types, vec![3, 2, 1]); // High, Normal, Low
    }

    #[test]
    fn test_priority_queue_clear() {
        let mut queue = PriorityQueue::new();
        queue.push_back(Message::new(1, Bytes::new()));
        queue.push_back(Message::new(2, Bytes::new()).with_priority(MessagePriority::High));
        queue.push_back(Message::new(3, Bytes::new()).with_priority(MessagePriority::Normal));

        assert_eq!(queue.len(), 3);
        queue.clear();
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
        assert!(queue.pop_front().is_none());
    }

    #[test]
    fn test_priority_queue_front() {
        let mut queue = PriorityQueue::new();

        queue.push_back(Message::new(1, Bytes::from("low")).with_priority(MessagePriority::Low));
        queue.push_back(Message::new(2, Bytes::from("high")).with_priority(MessagePriority::High));

        assert_eq!(queue.front().unwrap().msg_type(), 2);
        assert_eq!(queue.len(), 2); // front() does not remove
    }
}
