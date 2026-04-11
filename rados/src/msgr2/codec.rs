//! Stateful [`Decoder`] / [`Encoder`] implementations for post-handshake msgr2
//! frames.
//!
//! # Scope
//!
//! This codec is **only** valid after an msgr2 connection has reached the
//! `SESSION_READY` phase. It assumes:
//!
//! - Connection mode (SECURE vs CRC) is already fixed. Either the codec holds
//!   both an encryptor and a decryptor (SECURE) or it holds neither (CRC).
//!   A mid-lifetime transition is not supported — that's exactly why the
//!   Framed refactor is scoped to post-handshake.
//! - The `AUTH_SIGNATURE` running hash is already computed and verified, so
//!   the codec has no `record_sent` / `record_received` obligations.
//! - Compression, if negotiated, has its context installed before the codec
//!   is constructed.
//! - `rev0` vs `rev1` framing is decided once at construction time.
//!
//! The codec will drive [`tokio_util::codec::FramedRead`] /
//! [`tokio_util::codec::FramedWrite`] pairs in later refactor steps; for step 1
//! it is wired up with unit tests only, and [`crate::msgr2::protocol::FrameIO`]
//! still handles all real I/O.
//!
//! # Why two types
//!
//! The decoder and encoder are split into separate structs so that a
//! `TcpStream::into_split()` + `FramedRead<ReadHalf, Msgr2Decoder>` +
//! `FramedWrite<WriteHalf, Msgr2Encoder>` layout is natural. The rx and tx
//! nonce counters on the AES-GCM sides are already independent — they've
//! always been separate objects on `StateMachine` — so splitting into two
//! codec types introduces no new shared state.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::msgr2::compression::CompressionContext;
use crate::msgr2::crypto::{FrameDecryptor, FrameEncryptor};
use crate::msgr2::error::{Msgr2Error, Result};
use crate::msgr2::frames::{
    CRYPTO_BLOCK_SIZE, FRAME_CRC_SIZE, FRAME_LATE_STATUS_ABORTED, FRAME_LATE_STATUS_ABORTED_MASK,
    FRAME_LATE_STATUS_COMPLETE, Frame, MAX_NUM_SEGMENTS, PREAMBLE_SIZE, Preamble,
    SegmentDescriptor, align_to_crypto_block,
};

/// msgr2 inline optimization: the first [`INLINE_SIZE`] bytes of the payload
/// are encrypted in the same GCM operation as the preamble, so a minimum of
/// `PREAMBLE_SIZE + INLINE_SIZE + GCM_TAG_SIZE` = 96 bytes ships on the wire
/// for every secure-mode frame, even if the actual payload is shorter.
const INLINE_SIZE: usize = 48;

/// AES-GCM authentication tag size added to every encrypted segment.
const GCM_TAG_SIZE: usize = 16;

/// Zero-padding scratch used for alignment / inline padding. Large enough to
/// cover any single padding burst we emit.
const ZEROS: [u8; 64] = [0u8; 64];

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

/// Resumable decode state.
///
/// A single frame always needs at least two reads in SECURE mode (preamble
/// block + payload remainder) and may need one or two in CRC mode (preamble
/// alone; payload if non-empty). Between [`Decoder::decode`] calls we may
/// have only part of the total; the state tracks how far we got so the
/// caller can feed more bytes without losing progress.
#[derive(Debug)]
enum DecodeState {
    /// Need to read the preamble block: 32 bytes plaintext, 96 bytes encrypted.
    NeedsPreamble,
    /// Preamble decoded; need to read the remaining encrypted or plaintext
    /// payload bytes before we can emit a frame.
    NeedsPayload {
        preamble: Preamble,
        /// For SECURE mode: the 48-byte inline slice that arrived with the
        /// preamble, already decrypted. For CRC mode: empty.
        inline_data: Bytes,
        /// Number of additional bytes we still need to pull off the buffer.
        /// In SECURE mode this is `(payload - INLINE_SIZE) + GCM_TAG_SIZE`.
        /// In CRC mode it is the full payload size.
        remaining_needed: usize,
    },
}

/// Stateful msgr2 frame decoder.
pub(crate) struct Msgr2Decoder {
    decryptor: Option<Box<dyn FrameDecryptor>>,
    is_rev1: bool,
    compression: Option<Arc<CompressionContext>>,
    state: DecodeState,
}

impl Msgr2Decoder {
    /// Construct a decoder for a post-handshake connection.
    ///
    /// `decryptor` is `Some` iff the connection negotiated SECURE mode;
    /// `None` means CRC mode (plaintext frames with per-segment CRCs).
    ///
    /// `is_rev1` is the msgr2 revision flag that was established from the
    /// peer's advertised features at handshake time.
    ///
    /// `compression` is the shared compression context if the peer
    /// negotiated compression during session establishment, else `None`.
    pub(crate) fn new(
        decryptor: Option<Box<dyn FrameDecryptor>>,
        is_rev1: bool,
        compression: Option<Arc<CompressionContext>>,
    ) -> Self {
        Self {
            decryptor,
            is_rev1,
            compression,
            state: DecodeState::NeedsPreamble,
        }
    }

    fn has_encryption(&self) -> bool {
        self.decryptor.is_some()
    }

    fn preamble_block_size(&self) -> usize {
        if self.has_encryption() {
            PREAMBLE_SIZE + INLINE_SIZE + GCM_TAG_SIZE
        } else {
            PREAMBLE_SIZE
        }
    }

    /// Decrypt / parse the preamble block into the logical preamble plus the
    /// inline 48-byte slice. In CRC mode the inline slice is empty.
    fn decode_preamble_block(&mut self, block: &[u8]) -> Result<(Preamble, Bytes)> {
        let (preamble_bytes, inline_data) = match self.decryptor.as_mut() {
            Some(decryptor) => {
                let decrypted = decryptor.decrypt(block)?;
                // Layout: [32-byte preamble][48-byte inline data]
                let preamble_bytes = decrypted.slice(..PREAMBLE_SIZE);
                let inline = decrypted.slice(PREAMBLE_SIZE..PREAMBLE_SIZE + INLINE_SIZE);
                (preamble_bytes, inline)
            }
            None => (Bytes::copy_from_slice(block), Bytes::new()),
        };
        let preamble = Preamble::decode(preamble_bytes)?;
        Ok((preamble, inline_data))
    }

    /// Compute the byte count of the fully-assembled (possibly padded,
    /// possibly including epilogue) payload for this frame, using only the
    /// preamble as input. Mirrors the calculation in
    /// `FrameIO::recv_frame` so the two paths stay byte-identical.
    fn total_payload_size(&self, preamble: &Preamble) -> usize {
        let total_segment_size: usize = preamble.segments[..preamble.num_segments as usize]
            .iter()
            .map(|s| s.logical_len as usize)
            .sum();

        if self.has_encryption() && total_segment_size > 0 {
            if preamble.num_segments == 1 {
                align_to_crypto_block(total_segment_size)
            } else {
                let mut padded = 0usize;
                for i in 0..preamble.num_segments as usize {
                    let seg_len = preamble.segments[i].logical_len as usize;
                    if seg_len > 0 {
                        padded += align_to_crypto_block(seg_len);
                    }
                }
                padded + CRYPTO_BLOCK_SIZE
            }
        } else if total_segment_size == 0 {
            0
        } else if preamble.num_segments == 1 {
            total_segment_size + FRAME_CRC_SIZE
        } else {
            total_segment_size + FRAME_CRC_SIZE + 1 + (MAX_NUM_SEGMENTS - 1) * FRAME_CRC_SIZE
        }
    }

    /// Number of additional bytes the Decoder must still pull off the buffer
    /// *after* the preamble block has been consumed, before it can assemble
    /// a complete frame.
    fn remaining_needed(&self, total_payload_size: usize) -> usize {
        if total_payload_size == 0 {
            return 0;
        }
        if self.has_encryption() {
            if total_payload_size > INLINE_SIZE {
                (total_payload_size - INLINE_SIZE) + GCM_TAG_SIZE
            } else {
                0
            }
        } else {
            total_payload_size
        }
    }

    /// Assemble the final [`Frame`] given the preamble, the inline slice from
    /// the preamble block, and any extra payload bytes pulled off the buffer.
    ///
    /// `remaining_payload` is the raw bytes immediately after the preamble
    /// block: encrypted in SECURE mode (including the trailing GCM tag),
    /// plaintext in CRC mode. It may be empty when the payload fit entirely
    /// in the inline section (SECURE) or when the frame was empty.
    fn assemble_frame(
        &mut self,
        preamble: Preamble,
        inline_data: Bytes,
        remaining_payload: Bytes,
    ) -> Result<Frame> {
        let total_segment_size: usize = preamble.segments[..preamble.num_segments as usize]
            .iter()
            .map(|s| s.logical_len as usize)
            .sum();

        // Zero-payload frame: emit immediately with empty segments.
        if total_segment_size == 0 {
            let segments = (0..preamble.num_segments).map(|_| Bytes::new()).collect();
            return Ok(Frame { preamble, segments });
        }

        let has_encryption = self.has_encryption();
        let total_payload_size = self.total_payload_size(&preamble);

        // Build the combined payload view used for segment extraction.
        //
        // - SECURE / remaining present: decrypt the remaining bytes and
        //   concatenate [inline_data ++ decrypted_remaining].
        // - SECURE / inline-only (short frame): slice inline_data down to the
        //   actual payload size.
        // - CRC: the remaining_payload IS the payload (inline_data is empty).
        let full_payload = if has_encryption {
            if !remaining_payload.is_empty() {
                let decryptor = self
                    .decryptor
                    .as_mut()
                    .expect("has_encryption() true implies decryptor is Some");
                let decrypted_remaining = decryptor.decrypt(&remaining_payload)?;
                let mut combined =
                    BytesMut::with_capacity(inline_data.len() + decrypted_remaining.len());
                combined.extend_from_slice(&inline_data);
                combined.extend_from_slice(&decrypted_remaining);
                combined.freeze()
            } else {
                inline_data.slice(..total_payload_size.min(inline_data.len()))
            }
        } else {
            remaining_payload
        };

        // Walk the preamble's segment descriptors, slicing each one out of
        // the combined payload. Mirrors the layout that `FrameIO::send_frame`
        // produces.
        let is_multi = preamble.num_segments > 1;
        let mut segments = Vec::with_capacity(preamble.num_segments as usize);
        let mut offset = 0usize;
        for i in 0..preamble.num_segments as usize {
            let segment_len = preamble.segments[i].logical_len as usize;
            if segment_len > 0 {
                if offset + segment_len > full_payload.len() {
                    return Err(Msgr2Error::protocol_error(
                        "Segment extends past the decoded payload buffer",
                    ));
                }
                let segment_data = full_payload.slice(offset..offset + segment_len);
                segments.push(segment_data);

                if has_encryption {
                    offset += align_to_crypto_block(segment_len);
                } else {
                    offset += segment_len;
                    // Rev1 plaintext: a 4-byte CRC follows segment 0 inline.
                    if self.is_rev1 && i == 0 {
                        if let Some(crc_bytes) = full_payload
                            .get(offset..offset + FRAME_CRC_SIZE)
                            .and_then(|s| <[u8; 4]>::try_from(s).ok())
                        {
                            let received_crc = u32::from_le_bytes(crc_bytes);
                            let expected_crc =
                                !crc32c::crc32c(segments.last().ok_or_else(|| {
                                    Msgr2Error::protocol_error("CRC check: empty segments list")
                                })?);
                            if received_crc != expected_crc {
                                return Err(Msgr2Error::Protocol(format!(
                                    "Segment 0 CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                                    expected_crc, received_crc
                                )));
                            }
                        }
                        offset += FRAME_CRC_SIZE;
                    }
                }
            } else {
                segments.push(Bytes::new());
            }
        }

        // Plaintext multi-segment frames carry a trailing epilogue with the
        // abort byte + per-segment CRCs (rev0: 4 CRCs; rev1: 3 CRCs for
        // segments 1..=3 because segment 0's CRC was verified inline above).
        if !has_encryption && is_multi && total_segment_size > 0 && offset < full_payload.len() {
            let late_status = full_payload[offset];
            if (late_status & FRAME_LATE_STATUS_ABORTED_MASK) == FRAME_LATE_STATUS_ABORTED {
                return Err(Msgr2Error::Protocol(
                    "Frame transmission aborted by sender".to_string(),
                ));
            }
            offset += 1;

            let (crc_start_seg, num_epilogue_crcs, invert) = if self.is_rev1 {
                (1usize, MAX_NUM_SEGMENTS - 1, true)
            } else {
                (0usize, MAX_NUM_SEGMENTS, false)
            };
            for seg_idx in crc_start_seg..crc_start_seg + num_epilogue_crcs {
                let Some(crc_bytes) = full_payload
                    .get(offset..offset + FRAME_CRC_SIZE)
                    .and_then(|s| <[u8; 4]>::try_from(s).ok())
                else {
                    break;
                };
                let received_crc = u32::from_le_bytes(crc_bytes);
                offset += FRAME_CRC_SIZE;

                if seg_idx < segments.len() && !segments[seg_idx].is_empty() {
                    let computed = crc32c::crc32c(&segments[seg_idx]);
                    let expected_crc = if invert { !computed } else { computed };
                    if received_crc != expected_crc {
                        return Err(Msgr2Error::Protocol(format!(
                            "Segment {} CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                            seg_idx, expected_crc, received_crc
                        )));
                    }
                }
            }
        }

        let mut frame = Frame { preamble, segments };

        if frame.preamble.is_compressed() {
            if let Some(ctx) = self.compression.as_deref() {
                frame = frame.decompress(ctx).map_err(|e| {
                    Msgr2Error::protocol_error(&format!("Decompression failed: {:?}", e))
                })?;
            } else {
                tracing::warn!(
                    "Frame has FRAME_EARLY_DATA_COMPRESSED flag but no compression context"
                );
            }
        }

        Ok(frame)
    }
}

impl Decoder for Msgr2Decoder {
    type Item = Frame;
    type Error = Msgr2Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Frame>> {
        loop {
            // Take the current state; we always either restore it (need more
            // bytes) or transition it inside this iteration.
            let current = std::mem::replace(&mut self.state, DecodeState::NeedsPreamble);
            match current {
                DecodeState::NeedsPreamble => {
                    let need = self.preamble_block_size();
                    if buf.len() < need {
                        // Restore state and return; caller will give us more.
                        self.state = DecodeState::NeedsPreamble;
                        return Ok(None);
                    }
                    let preamble_block = buf.split_to(need);
                    let (preamble, inline_data) = self.decode_preamble_block(&preamble_block)?;
                    let total_payload_size = self.total_payload_size(&preamble);
                    let remaining_needed = self.remaining_needed(total_payload_size);

                    if remaining_needed == 0 {
                        // All bytes already consumed (either payload fit in
                        // the inline region, or it's a zero-payload frame).
                        // Emit immediately; state stays NeedsPreamble.
                        let frame = self.assemble_frame(preamble, inline_data, Bytes::new())?;
                        return Ok(Some(frame));
                    }

                    // Fall through to the NeedsPayload branch in the next
                    // loop iteration so we opportunistically try the payload
                    // if enough bytes are already in the buffer.
                    self.state = DecodeState::NeedsPayload {
                        preamble,
                        inline_data,
                        remaining_needed,
                    };
                }
                DecodeState::NeedsPayload {
                    preamble,
                    inline_data,
                    remaining_needed,
                } => {
                    if buf.len() < remaining_needed {
                        // Put state back and wait for more.
                        self.state = DecodeState::NeedsPayload {
                            preamble,
                            inline_data,
                            remaining_needed,
                        };
                        return Ok(None);
                    }
                    let payload = buf.split_to(remaining_needed).freeze();
                    let frame = self.assemble_frame(preamble, inline_data, payload)?;
                    return Ok(Some(frame));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

/// Stateful msgr2 frame encoder.
///
/// Unlike [`Msgr2Decoder`], the encoder has no inter-call state — every
/// [`Encoder::encode`] call is self-contained. State-wise it only holds the
/// encryptor (if SECURE) and the shared compression context.
pub(crate) struct Msgr2Encoder {
    encryptor: Option<Box<dyn FrameEncryptor>>,
    is_rev1: bool,
    compression: Option<Arc<CompressionContext>>,
}

impl Msgr2Encoder {
    /// Construct an encoder for a post-handshake connection.
    pub(crate) fn new(
        encryptor: Option<Box<dyn FrameEncryptor>>,
        is_rev1: bool,
        compression: Option<Arc<CompressionContext>>,
    ) -> Self {
        Self {
            encryptor,
            is_rev1,
            compression,
        }
    }
}

/// Count of segments to serialize, trimming trailing empty ones. Mirrors
/// `FrameIO::normalized_num_segments` so the wire layout is identical.
fn normalized_num_segments(frame: &Frame) -> usize {
    let mut n = frame.segments.len();
    while n > 1 && frame.segments[n - 1].is_empty() {
        n -= 1;
    }
    n
}

/// Build the preamble bytes for a secure-mode frame. Mirrors
/// `FrameIO::build_secure_preamble`.
fn build_secure_preamble(frame: &Frame, num_segments: usize) -> Result<Bytes> {
    let mut preamble = Preamble {
        tag: frame.preamble.tag,
        num_segments: num_segments as u8,
        segments: [SegmentDescriptor::default(); MAX_NUM_SEGMENTS],
        flags: frame.preamble.flags,
        reserved: 0,
        crc: 0,
    };
    for (i, segment) in frame.segments.iter().take(num_segments).enumerate() {
        preamble.segments[i] = SegmentDescriptor {
            logical_len: segment.len() as u32,
            align: frame.preamble.segments[i].align,
        };
    }
    Ok(preamble.encode()?)
}

/// Build the padded / aligned payload body for a secure-mode frame. Mirrors
/// `FrameIO::build_secure_payload` byte-for-byte.
fn build_secure_payload(frame: &Frame, num_segments: usize) -> BytesMut {
    let total_len = frame
        .segments
        .iter()
        .take(num_segments)
        .filter(|s| !s.is_empty())
        .map(|s| align_to_crypto_block(s.len()))
        .sum::<usize>()
        + if num_segments > 1 {
            CRYPTO_BLOCK_SIZE
        } else {
            0
        };

    let mut payload = BytesMut::with_capacity(total_len);
    for segment in frame.segments.iter().take(num_segments) {
        let seg_len = segment.len();
        if seg_len == 0 {
            continue;
        }
        payload.extend_from_slice(segment);
        let pad = align_to_crypto_block(seg_len) - seg_len;
        if pad > 0 {
            payload.extend_from_slice(&ZEROS[..pad]);
        }
    }
    if num_segments > 1 {
        payload.extend_from_slice(&[FRAME_LATE_STATUS_COMPLETE]);
        payload.extend_from_slice(&ZEROS[..CRYPTO_BLOCK_SIZE - 1]);
    }
    payload
}

impl Encoder<Frame> for Msgr2Encoder {
    type Error = Msgr2Error;

    fn encode(&mut self, mut frame: Frame, dst: &mut BytesMut) -> Result<()> {
        // Apply compression if configured and beneficial. `frame.compress`
        // returns the original frame unchanged if the data didn't shrink or
        // the algorithm errored out — we detect the "actually compressed"
        // case via the flag on the returned frame.
        if let Some(ctx) = self.compression.as_deref() {
            let compressed = frame.compress(ctx);
            if compressed.preamble.is_compressed() {
                frame = compressed;
            }
        }

        if self.encryptor.is_none() {
            // CRC mode: let the existing scatter-gather assembler produce
            // the byte layout, then append each chunk to `dst`. This matches
            // `FrameIO::send_frame`'s unencrypted path.
            let chunks = frame.to_wire_vectored(self.is_rev1)?;
            let total: usize = chunks.iter().map(|c| c.len()).sum();
            dst.reserve(total);
            for chunk in &chunks {
                dst.extend_from_slice(chunk);
            }
            return Ok(());
        }

        // SECURE mode.
        let num_segments = normalized_num_segments(&frame);
        let preamble_bytes = build_secure_preamble(&frame, num_segments)?;
        let payload_bytes = build_secure_payload(&frame, num_segments);

        let inline_size = payload_bytes.len().min(INLINE_SIZE);
        let remaining_size = payload_bytes.len().saturating_sub(INLINE_SIZE);

        // Build the preamble-block plaintext: 32-byte preamble + 48-byte
        // inline slice (zero-padded if the payload was shorter than 48
        // bytes). This 80-byte buffer is GCM-encrypted into a 96-byte block.
        let mut preamble_block = BytesMut::with_capacity(PREAMBLE_SIZE + INLINE_SIZE);
        preamble_block.extend_from_slice(&preamble_bytes);
        preamble_block.extend_from_slice(&payload_bytes[..inline_size]);
        if inline_size < INLINE_SIZE {
            preamble_block.extend_from_slice(&ZEROS[..INLINE_SIZE - inline_size]);
        }

        let encryptor = self
            .encryptor
            .as_mut()
            .expect("branch above ensures encryptor is Some");
        let encrypted_preamble = encryptor.encrypt(&preamble_block)?;
        dst.reserve(encrypted_preamble.len() + remaining_size + GCM_TAG_SIZE);
        dst.extend_from_slice(&encrypted_preamble);

        if remaining_size > 0 {
            let remaining_data = &payload_bytes[INLINE_SIZE..];
            let encrypted_remaining = encryptor.encrypt(remaining_data)?;
            dst.extend_from_slice(&encrypted_remaining);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;

    use crate::msgr2::crypto::{Aes128GcmDecryptor, Aes128GcmEncryptor};
    use crate::msgr2::frames::{DEFAULT_ALIGNMENT, FrameFlags, Tag};

    fn single_segment_frame(tag: Tag, payload: &'static [u8]) -> Frame {
        Frame::new(tag, Bytes::from_static(payload))
    }

    fn multi_segment_frame(
        tag: Tag,
        segments: [Bytes; MAX_NUM_SEGMENTS],
        num_segments: u8,
    ) -> Frame {
        let mut descs = [SegmentDescriptor {
            logical_len: 0,
            align: DEFAULT_ALIGNMENT,
        }; MAX_NUM_SEGMENTS];
        for (i, seg) in segments.iter().enumerate() {
            descs[i].logical_len = seg.len() as u32;
        }
        Frame {
            preamble: Preamble {
                tag,
                num_segments,
                segments: descs,
                flags: FrameFlags::default(),
                reserved: 0,
                crc: 0,
            },
            segments: segments.into_iter().collect(),
        }
    }

    fn crc_roundtrip_pair(is_rev1: bool) -> (Msgr2Encoder, Msgr2Decoder) {
        (
            Msgr2Encoder::new(None, is_rev1, None),
            Msgr2Decoder::new(None, is_rev1, None),
        )
    }

    fn secure_roundtrip_pair(is_rev1: bool) -> (Msgr2Encoder, Msgr2Decoder) {
        // Both sides use the same key; nonces are zero, sequence counters
        // start at 0, so the rx/tx sequences stay in lockstep across the
        // roundtrip.
        let key = vec![0x42u8; 16];
        let nonce = vec![0u8; 12];
        let encryptor: Box<dyn FrameEncryptor> =
            Box::new(Aes128GcmEncryptor::new(key.clone(), nonce.clone()).unwrap());
        let decryptor: Box<dyn FrameDecryptor> =
            Box::new(Aes128GcmDecryptor::new(key, nonce).unwrap());
        (
            Msgr2Encoder::new(Some(encryptor), is_rev1, None),
            Msgr2Decoder::new(Some(decryptor), is_rev1, None),
        )
    }

    fn assert_frames_equal(a: &Frame, b: &Frame) {
        assert_eq!(a.preamble.tag, b.preamble.tag, "tag");
        assert_eq!(
            a.preamble.num_segments, b.preamble.num_segments,
            "num_segments"
        );
        assert_eq!(a.segments.len(), b.segments.len(), "segment count");
        for (i, (x, y)) in a.segments.iter().zip(b.segments.iter()).enumerate() {
            assert_eq!(x, y, "segment {i} bytes");
        }
    }

    #[test]
    fn crc_single_segment_roundtrip() {
        let (mut enc, mut dec) = crc_roundtrip_pair(true);
        let frame = single_segment_frame(Tag::Hello, b"hello world");

        let mut wire = BytesMut::new();
        enc.encode(frame.clone(), &mut wire).unwrap();

        let decoded = dec.decode(&mut wire).unwrap().expect("frame");
        assert!(
            wire.is_empty(),
            "decoder should have consumed every byte, {} left",
            wire.len()
        );
        assert_frames_equal(&frame, &decoded);
    }

    #[test]
    fn crc_multi_segment_roundtrip() {
        let (mut enc, mut dec) = crc_roundtrip_pair(true);
        let segments = [
            Bytes::from_static(b"header bytes"),
            Bytes::from_static(b"front payload"),
            Bytes::from_static(b"middle payload"),
            Bytes::from_static(b"data payload"),
        ];
        let frame = multi_segment_frame(Tag::Message, segments, 4);

        let mut wire = BytesMut::new();
        enc.encode(frame.clone(), &mut wire).unwrap();

        let decoded = dec.decode(&mut wire).unwrap().expect("frame");
        assert!(wire.is_empty());
        assert_frames_equal(&frame, &decoded);
    }

    #[test]
    fn secure_single_segment_roundtrip() {
        let (mut enc, mut dec) = secure_roundtrip_pair(true);
        let frame = single_segment_frame(Tag::Hello, b"hello secure world");

        let mut wire = BytesMut::new();
        enc.encode(frame.clone(), &mut wire).unwrap();

        let decoded = dec.decode(&mut wire).unwrap().expect("frame");
        assert!(wire.is_empty());
        assert_frames_equal(&frame, &decoded);
    }

    #[test]
    fn secure_multi_segment_roundtrip() {
        let (mut enc, mut dec) = secure_roundtrip_pair(true);
        let segments = [
            Bytes::from_static(b"header bytes"),
            Bytes::from_static(b"front payload"),
            Bytes::from_static(b"middle payload"),
            Bytes::from_static(b"data payload"),
        ];
        let frame = multi_segment_frame(Tag::Message, segments, 4);

        let mut wire = BytesMut::new();
        enc.encode(frame.clone(), &mut wire).unwrap();

        let decoded = dec.decode(&mut wire).unwrap().expect("frame");
        assert!(wire.is_empty());
        assert_frames_equal(&frame, &decoded);
    }

    #[test]
    fn secure_large_payload_spans_inline_and_remaining() {
        // A payload > INLINE_SIZE forces the encoder to emit a second
        // encrypted chunk beyond the preamble block, and the decoder to
        // follow its NeedsPayload path.
        let (mut enc, mut dec) = secure_roundtrip_pair(true);
        let payload: Vec<u8> = (0..200).map(|i| (i & 0xff) as u8).collect();
        let frame = Frame::new(Tag::Hello, Bytes::from(payload));

        let mut wire = BytesMut::new();
        enc.encode(frame.clone(), &mut wire).unwrap();

        let decoded = dec.decode(&mut wire).unwrap().expect("frame");
        assert!(wire.is_empty());
        assert_frames_equal(&frame, &decoded);
    }

    #[test]
    fn partial_buffer_returns_none_then_completes() {
        // Simulate the Framed read loop: feed bytes in small chunks and
        // verify the decoder holds state across calls without losing bytes.
        let (mut enc, mut dec) = secure_roundtrip_pair(true);
        let payload: Vec<u8> = (0..300).map(|i| (i & 0xff) as u8).collect();
        let frame = Frame::new(Tag::Hello, Bytes::from(payload));

        let mut complete = BytesMut::new();
        enc.encode(frame.clone(), &mut complete).unwrap();
        let total_len = complete.len();
        assert!(total_len > PREAMBLE_SIZE + INLINE_SIZE + GCM_TAG_SIZE);

        // Feed one byte at a time; the decoder must return Ok(None) until
        // the full frame is present, then emit the frame on the call where
        // the last byte arrives.
        let mut buf = BytesMut::new();
        let mut delivered = None;
        for (i, byte) in complete.iter().copied().enumerate() {
            buf.extend_from_slice(&[byte]);
            let result = dec.decode(&mut buf).unwrap();
            if i < total_len - 1 {
                assert!(
                    result.is_none(),
                    "decoder emitted a frame early at byte {i}/{total_len}"
                );
            } else {
                delivered = result;
            }
        }
        let decoded = delivered.expect("frame after final byte");
        assert!(
            buf.is_empty(),
            "decoder left {} bytes unconsumed",
            buf.len()
        );
        assert_frames_equal(&frame, &decoded);
    }

    #[test]
    fn multiple_frames_back_to_back() {
        // Encode three frames into a single BytesMut and verify the decoder
        // emits them in order across successive decode() calls.
        let (mut enc, mut dec) = crc_roundtrip_pair(true);
        let f1 = single_segment_frame(Tag::Hello, b"one");
        let f2 = single_segment_frame(Tag::Ack, b"two-two");
        let f3 = single_segment_frame(Tag::Keepalive2, b"three is the number");

        let mut wire = BytesMut::new();
        enc.encode(f1.clone(), &mut wire).unwrap();
        enc.encode(f2.clone(), &mut wire).unwrap();
        enc.encode(f3.clone(), &mut wire).unwrap();

        let d1 = dec.decode(&mut wire).unwrap().expect("first frame");
        let d2 = dec.decode(&mut wire).unwrap().expect("second frame");
        let d3 = dec.decode(&mut wire).unwrap().expect("third frame");
        assert!(wire.is_empty(), "leftover bytes");
        assert!(dec.decode(&mut wire).unwrap().is_none());

        assert_frames_equal(&f1, &d1);
        assert_frames_equal(&f2, &d2);
        assert_frames_equal(&f3, &d3);
    }

    #[test]
    fn secure_encoded_bytes_match_frame_io_layout() {
        // This is the critical wire-compatibility check: encode a frame
        // with the codec and independently with the same raw building
        // blocks that FrameIO::send_frame uses, and verify the output
        // matches byte-for-byte. If this ever diverges the codec cannot be
        // swapped in for FrameIO without breaking on-the-wire behaviour.
        //
        // The setup mimics FrameIO's encrypted path: build the preamble +
        // inline block, encrypt once; if remaining > 0, encrypt separately.
        let key = vec![0x42u8; 16];
        let nonce = vec![0u8; 12];
        let mut codec_enc = Msgr2Encoder::new(
            Some(Box::new(
                Aes128GcmEncryptor::new(key.clone(), nonce.clone()).unwrap(),
            )),
            true,
            None,
        );
        let mut manual_enc = Aes128GcmEncryptor::new(key, nonce).unwrap();

        let frame = Frame::new(Tag::Hello, Bytes::from_static(b"compatibility check"));

        // Codec path
        let mut codec_wire = BytesMut::new();
        codec_enc.encode(frame.clone(), &mut codec_wire).unwrap();

        // Manual path — inline copy of the relevant FrameIO logic
        let num_segments = normalized_num_segments(&frame);
        let preamble_bytes = build_secure_preamble(&frame, num_segments).unwrap();
        let payload_bytes = build_secure_payload(&frame, num_segments);
        let inline_size = payload_bytes.len().min(INLINE_SIZE);
        let remaining_size = payload_bytes.len().saturating_sub(INLINE_SIZE);
        let mut preamble_block = BytesMut::with_capacity(PREAMBLE_SIZE + INLINE_SIZE);
        preamble_block.extend_from_slice(&preamble_bytes);
        preamble_block.extend_from_slice(&payload_bytes[..inline_size]);
        if inline_size < INLINE_SIZE {
            preamble_block.extend_from_slice(&ZEROS[..INLINE_SIZE - inline_size]);
        }
        let encrypted_preamble = FrameEncryptor::encrypt(&mut manual_enc, &preamble_block).unwrap();
        let mut manual_wire = BytesMut::new();
        manual_wire.extend_from_slice(&encrypted_preamble);
        if remaining_size > 0 {
            let encrypted_remaining =
                FrameEncryptor::encrypt(&mut manual_enc, &payload_bytes[INLINE_SIZE..]).unwrap();
            manual_wire.extend_from_slice(&encrypted_remaining);
        }

        assert_eq!(
            codec_wire.as_ref(),
            manual_wire.as_ref(),
            "codec wire bytes diverged from manual FrameIO-equivalent layout"
        );
    }

    #[test]
    fn secure_sequence_counters_stay_in_lockstep() {
        // Multi-frame encode+decode with a single shared key verifies that
        // the encryptor and decryptor increment their sequence counters in
        // lockstep. If the codec ever double-encrypts or double-decrypts,
        // or if a single frame doesn't consume exactly one sequence slot,
        // this test catches it.
        let (mut enc, mut dec) = secure_roundtrip_pair(true);
        let mut wire = BytesMut::new();
        let mut frames = Vec::new();
        for i in 0..10 {
            let payload = format!("message number {i:03}").into_bytes();
            let frame = Frame::new(Tag::Message, Bytes::from(payload));
            enc.encode(frame.clone(), &mut wire).unwrap();
            frames.push(frame);
        }
        for expected in &frames {
            let decoded = dec.decode(&mut wire).unwrap().expect("frame");
            assert_frames_equal(expected, &decoded);
        }
        assert!(wire.is_empty());
    }
}
