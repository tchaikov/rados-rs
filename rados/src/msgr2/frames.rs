//! Frame definitions and assembly logic for the msgr2.1 wire protocol.
//!
//! This module contains the concrete control and message frame types, their
//! protocol tags, and the assembler logic that turns byte streams into frames
//! and frames back into wire format. It is the main surface for msgr2 framing
//! behavior above the encryption, compression, and socket layers.

use crate::Denc;
use crate::RadosError;
use crate::msgr2::header::MsgHeader;
#[cfg(test)]
use bytes::BufMut;
use bytes::{Buf, Bytes, BytesMut};

// Protocol constants
pub const MAX_NUM_SEGMENTS: usize = 4;
pub const DEFAULT_ALIGNMENT: u16 = 8;
pub const PAGE_SIZE_ALIGNMENT: u16 = 4096;
pub const PREAMBLE_SIZE: usize = 32;
pub const CRYPTO_BLOCK_SIZE: usize = 16;
pub const FRAME_CRC_SIZE: usize = 4;

// Frame flags
pub const FRAME_EARLY_DATA_COMPRESSED: u8 = 0x01;

/// Round `size` up to the nearest multiple of `CRYPTO_BLOCK_SIZE` (16).
#[inline]
pub(crate) fn align_to_crypto_block(size: usize) -> usize {
    (size + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1)
}

/// Type-safe wrapper for frame flags
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrameFlags(u8);

impl FrameFlags {
    /// Create new flags with all bits cleared
    pub fn new() -> Self {
        Self(0)
    }

    /// Create from raw u8 value
    pub fn from_raw(val: u8) -> Self {
        Self(val)
    }

    /// Get the raw u8 value
    pub fn raw(self) -> u8 {
        self.0
    }

    /// Check if compression flag is set
    pub fn is_compressed(self) -> bool {
        (self.0 & FRAME_EARLY_DATA_COMPRESSED) != 0
    }

    /// Set the compression flag
    pub fn set_compressed(&mut self) {
        self.0 |= FRAME_EARLY_DATA_COMPRESSED;
    }

    /// Clear the compression flag
    pub fn clear_compressed(&mut self) {
        self.0 &= !FRAME_EARLY_DATA_COMPRESSED;
    }

    /// Set or clear the compression flag based on boolean
    pub fn with_compressed(mut self, compressed: bool) -> Self {
        if compressed {
            self.set_compressed();
        } else {
            self.clear_compressed();
        }
        self
    }
}

impl From<u8> for FrameFlags {
    fn from(val: u8) -> Self {
        Self(val)
    }
}

impl From<FrameFlags> for u8 {
    fn from(flags: FrameFlags) -> Self {
        flags.0
    }
}

// Late status constants for msgr2.1
pub const FRAME_LATE_STATUS_ABORTED: u8 = 0x1;
pub const FRAME_LATE_STATUS_COMPLETE: u8 = 0xe;
pub const FRAME_LATE_STATUS_ABORTED_MASK: u8 = 0xf;

// Frame tags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tag {
    Hello = 1,
    AuthRequest = 2,
    AuthBadMethod = 3,
    AuthReplyMore = 4,
    AuthRequestMore = 5,
    AuthDone = 6,
    AuthSignature = 7,
    ClientIdent = 8,
    ServerIdent = 9,
    IdentMissingFeatures = 10,
    SessionReconnect = 11,
    SessionReset = 12,
    SessionRetry = 13,
    SessionRetryGlobal = 14,
    SessionReconnectOk = 15,
    Wait = 16,
    Message = 17,
    Keepalive2 = 18,
    Keepalive2Ack = 19,
    Ack = 20,
    CompressionRequest = 21,
    CompressionDone = 22,
}

impl TryFrom<u8> for Tag {
    type Error = RadosError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::AuthRequest),
            3 => Ok(Self::AuthBadMethod),
            4 => Ok(Self::AuthReplyMore),
            5 => Ok(Self::AuthRequestMore),
            6 => Ok(Self::AuthDone),
            7 => Ok(Self::AuthSignature),
            8 => Ok(Self::ClientIdent),
            9 => Ok(Self::ServerIdent),
            10 => Ok(Self::IdentMissingFeatures),
            11 => Ok(Self::SessionReconnect),
            12 => Ok(Self::SessionReset),
            13 => Ok(Self::SessionRetry),
            14 => Ok(Self::SessionRetryGlobal),
            15 => Ok(Self::SessionReconnectOk),
            16 => Ok(Self::Wait),
            17 => Ok(Self::Message),
            18 => Ok(Self::Keepalive2),
            19 => Ok(Self::Keepalive2Ack),
            20 => Ok(Self::Ack),
            21 => Ok(Self::CompressionRequest),
            22 => Ok(Self::CompressionDone),
            _ => Err(RadosError::Protocol(format!("Unknown tag: {}", value))),
        }
    }
}

// Simple Frame structure for state machine
#[derive(Debug, Clone)]
pub struct Frame {
    pub preamble: Preamble,
    pub segments: Vec<Bytes>,
}

impl Frame {
    pub fn new(tag: Tag, payload: Bytes) -> Self {
        Self {
            preamble: Preamble {
                tag,
                num_segments: 1,
                segments: [SegmentDescriptor {
                    logical_len: payload.len() as u32,
                    align: DEFAULT_ALIGNMENT,
                }; MAX_NUM_SEGMENTS],
                flags: 0,
                reserved: 0,
                crc: 0,
            },
            segments: vec![payload],
        }
    }

    /// Convert Frame to wire format with proper preamble, segments, and epilogue
    /// This should be used when sending frames over the network
    pub fn to_wire(&self, is_rev1: bool) -> Result<Bytes, RadosError> {
        let mut assembler = FrameAssembler::new(is_rev1);
        // Copy compression flag from preamble if set
        if self.preamble.is_compressed() {
            assembler.set_compression(true);
        }
        let n = self.preamble.num_segments as usize;
        let mut alignments = [0u16; MAX_NUM_SEGMENTS];
        for (a, seg) in alignments[..n].iter_mut().zip(&self.preamble.segments[..n]) {
            *a = seg.align;
        }
        assembler.assemble_frame(self.preamble.tag, &self.segments, &alignments[..n])
    }

    /// Create a compressed version of this frame.
    ///
    /// Matches Ceph's `FrameAssemblerV2::asm_compress()`: each non-empty segment
    /// is compressed independently. The preamble `FRAME_EARLY_DATA_COMPRESSED`
    /// flag is set and each segment descriptor's `logical_len` is updated to the
    /// compressed length. The `reserved` field is left as zero.
    pub fn compress(
        &self,
        ctx: &crate::msgr2::compression::CompressionContext,
    ) -> Result<Self, RadosError> {
        let total_size: usize = self.segments.iter().map(|s| s.len()).sum();

        // Check if we should compress based on threshold
        if !ctx.should_compress(total_size) {
            return Ok(self.clone());
        }

        let mut new_preamble = self.preamble.clone();
        let mut new_segments = Vec::with_capacity(self.segments.len());

        for (i, segment) in self.segments.iter().enumerate() {
            if segment.is_empty() {
                new_segments.push(Bytes::new());
            } else {
                let compressed = ctx
                    .compress(segment)
                    .map_err(|e| RadosError::Compression(e.to_string()))?;

                new_preamble.segments[i].logical_len = compressed.len() as u32;
                new_segments.push(compressed);
            }
        }

        new_preamble.set_compressed();

        tracing::debug!(
            "Compressed frame: {} bytes -> {} bytes",
            total_size,
            new_segments.iter().map(|s| s.len()).sum::<usize>()
        );

        Ok(Frame {
            preamble: new_preamble,
            segments: new_segments,
        })
    }

    /// Decompress this frame if it's compressed.
    ///
    /// Each segment is decompressed independently. Each compression format
    /// (Snappy, Zstd, LZ4, Zlib) embeds the original uncompressed size in its
    /// stream, so no external `original_size` hint is needed.
    pub fn decompress(
        &self,
        ctx: &crate::msgr2::compression::CompressionContext,
    ) -> Result<Self, RadosError> {
        if !self.preamble.is_compressed() {
            return Ok(self.clone());
        }

        let mut new_preamble = self.preamble.clone();
        let mut new_segments = Vec::with_capacity(self.segments.len());

        for (i, segment) in self.segments.iter().enumerate() {
            if segment.is_empty() {
                new_segments.push(Bytes::new());
            } else {
                let decompressed = ctx
                    .decompress(segment)
                    .map_err(|e| RadosError::Compression(e.to_string()))?;

                new_preamble.segments[i].logical_len = decompressed.len() as u32;
                new_segments.push(decompressed);
            }
        }

        new_preamble.clear_compressed();

        tracing::debug!(
            "Decompressed frame: {} bytes -> {} bytes",
            self.segments.iter().map(|s| s.len()).sum::<usize>(),
            new_segments.iter().map(|s| s.len()).sum::<usize>()
        );

        Ok(Frame {
            preamble: new_preamble,
            segments: new_segments,
        })
    }

    /// Legacy encode method - just concatenates preamble and segments without epilogue
    /// This is NOT suitable for sending over the network
    pub fn encode(&self) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.preamble.encode()?);
        for segment in &self.segments {
            buf.extend_from_slice(segment);
        }
        Ok(buf.freeze())
    }

    /// Build the secure-mode payload body for focused layout tests.
    #[cfg(test)]
    pub(crate) fn encode_secure_payload(&self) -> Bytes {
        let num_segments = self.segments.len().min(self.preamble.num_segments as usize);
        let total_len = self.segments[..num_segments]
            .iter()
            .filter(|segment| !segment.is_empty())
            .map(|segment| {
                let segment_len = segment.len();
                align_to_crypto_block(segment_len)
            })
            .sum::<usize>()
            + if self.preamble.num_segments > 1 {
                CRYPTO_BLOCK_SIZE
            } else {
                0
            };

        let mut payload = BytesMut::with_capacity(total_len);
        for segment in &self.segments[..num_segments] {
            if segment.is_empty() {
                continue;
            }

            payload.extend_from_slice(segment);

            let segment_len = segment.len();
            let aligned_len = align_to_crypto_block(segment_len);
            payload.put_bytes(0, aligned_len - segment_len);
        }

        if self.preamble.num_segments > 1 {
            payload.put_u8(FRAME_LATE_STATUS_COMPLETE);
            payload.put_bytes(0, CRYPTO_BLOCK_SIZE - 1);
        }

        payload.freeze()
    }
}

// Core FrameTrait - only provides data, no serialization logic
pub trait FrameTrait: Sized {
    const TAG: Tag;
    const NUM_SEGMENTS: usize;

    fn segment_alignments() -> &'static [u16];
    fn get_segments(&self, features: u64) -> Result<Vec<Bytes>, RadosError>;
    fn from_segments(segments: Vec<Bytes>) -> Result<Self, RadosError>;
}

// Segment descriptor
#[derive(Debug, Clone, Copy, Default)]
pub struct SegmentDescriptor {
    pub logical_len: u32,
    pub align: u16,
}

// Preamble structure
#[derive(Debug, Clone)]
pub struct Preamble {
    pub tag: Tag,
    pub num_segments: u8,
    pub segments: [SegmentDescriptor; MAX_NUM_SEGMENTS],
    pub flags: u8,
    pub reserved: u8,
    pub crc: u32,
}

impl Preamble {
    /// Check if the compression flag is set
    pub fn is_compressed(&self) -> bool {
        FrameFlags::from(self.flags).is_compressed()
    }

    /// Set the compression flag
    pub fn set_compressed(&mut self) {
        let mut flags = FrameFlags::from(self.flags);
        flags.set_compressed();
        self.flags = flags.raw();
    }

    /// Clear the compression flag
    pub fn clear_compressed(&mut self) {
        let mut flags = FrameFlags::from(self.flags);
        flags.clear_compressed();
        self.flags = flags.raw();
    }

    pub fn encode(&self) -> Result<Bytes, RadosError> {
        use crate::Denc;
        let mut buf = BytesMut::with_capacity(PREAMBLE_SIZE);

        (self.tag as u8).encode(&mut buf, 0)?;
        self.num_segments.encode(&mut buf, 0)?;

        for segment in &self.segments {
            segment.logical_len.encode(&mut buf, 0)?;
            segment.align.encode(&mut buf, 0)?;
        }

        self.flags.encode(&mut buf, 0)?;
        self.reserved.encode(&mut buf, 0)?;

        // Ceph's ceph_crc32c(): !crc32c_append(0xFFFFFFFF, data)
        let crc = !crc32c::crc32c_append(0xFFFFFFFF, &buf[..28]);

        crc.encode(&mut buf, 0)?;

        Ok(buf.freeze())
    }

    pub fn decode(buf: Bytes) -> Result<Self, RadosError> {
        use crate::Denc;
        if buf.len() < PREAMBLE_SIZE {
            return Err(RadosError::Protocol("Preamble too short".to_string()));
        }

        // Verify CRC before decoding (first 28 bytes, excluding the CRC field itself)
        let calculated_crc = !crc32c::crc32c_append(0xFFFFFFFF, &buf[..28]);

        let mut cursor = buf;
        let tag = Tag::try_from(u8::decode(&mut cursor, 0)?)?;
        let num_segments = u8::decode(&mut cursor, 0)?;

        let mut segments = [SegmentDescriptor {
            logical_len: 0,
            align: 0,
        }; MAX_NUM_SEGMENTS];
        for segment in segments.iter_mut() {
            segment.logical_len = u32::decode(&mut cursor, 0)?;
            segment.align = u16::decode(&mut cursor, 0)?;
        }

        let flags = u8::decode(&mut cursor, 0)?;
        let reserved = u8::decode(&mut cursor, 0)?;
        let received_crc = u32::decode(&mut cursor, 0)?;

        // Verify CRC matches
        if calculated_crc != received_crc {
            return Err(RadosError::Protocol(format!(
                "Preamble CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                calculated_crc, received_crc
            )));
        }

        Ok(Self {
            tag,
            num_segments,
            segments,
            flags,
            reserved,
            crc: received_crc,
        })
    }
}

// Frame epilogue structures for different revisions
#[derive(Debug, Clone, Copy)]
pub struct EpilogueCrcRev0 {
    pub late_flags: u8,
    pub crc_values: [u32; MAX_NUM_SEGMENTS],
}

#[derive(Debug, Clone, Copy)]
pub struct EpilogueCrcRev1 {
    pub late_status: u8,
    pub crc_values: [u32; MAX_NUM_SEGMENTS - 1],
}

// FrameAssembler now owns all wire serialization logic
pub struct FrameAssembler {
    descs: Vec<SegmentDescriptor>,
    flags: u8,
    is_rev1: bool,
    with_data_crc: bool,
}

impl FrameAssembler {
    pub fn new(is_rev1: bool) -> Self {
        Self {
            descs: Vec::new(),
            flags: 0,
            is_rev1,
            with_data_crc: true,
        }
    }

    pub fn set_compression(&mut self, enabled: bool) {
        if enabled {
            self.flags |= FRAME_EARLY_DATA_COMPRESSED;
        } else {
            self.flags &= !FRAME_EARLY_DATA_COMPRESSED;
        }
    }

    // Main entry point - takes any FrameTrait and serializes it to wire format
    pub fn to_wire<F: FrameTrait>(
        &mut self,
        frame: &F,
        features: u64,
    ) -> Result<Bytes, RadosError> {
        let segments = frame.get_segments(features)?;
        let alignments = F::segment_alignments();
        self.assemble_frame(F::TAG, &segments, alignments)
    }

    // Deserialize from wire format (assumes msgr2.1 / rev1 framing)
    pub fn from_wire<F: FrameTrait>(mut buf: Bytes) -> Result<F, RadosError> {
        // Decode preamble
        let preamble = Preamble::decode(buf.split_to(PREAMBLE_SIZE))?;

        if preamble.tag != F::TAG {
            return Err(RadosError::Protocol(format!(
                "Tag mismatch: expected {:?}, got {:?}",
                F::TAG,
                preamble.tag
            )));
        }

        let is_multi = preamble.num_segments > 1;

        // Extract segments, handling rev1 inter-segment CRC for multi-segment frames
        let mut segments = Vec::with_capacity(preamble.num_segments as usize);
        for i in 0..preamble.num_segments as usize {
            let len = preamble.segments[i].logical_len as usize;
            if buf.len() < len {
                return Err(RadosError::Protocol(
                    "Insufficient segment data".to_string(),
                ));
            }
            segments.push(buf.split_to(len));

            // Rev1 multi-segment: segment 0 is followed by a 4-byte CRC
            if is_multi && i == 0 {
                if buf.len() < FRAME_CRC_SIZE {
                    return Err(RadosError::Protocol(
                        "Missing CRC after first segment".to_string(),
                    ));
                }
                let mut crc_bytes = [0u8; FRAME_CRC_SIZE];
                crc_bytes.copy_from_slice(&buf[..FRAME_CRC_SIZE]);
                let received_crc = u32::from_le_bytes(crc_bytes);
                let expected_crc = !crc32c::crc32c(&segments[0]);
                if received_crc != expected_crc {
                    return Err(RadosError::Protocol(format!(
                        "Segment 0 CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                        expected_crc, received_crc
                    )));
                }
                buf.advance(FRAME_CRC_SIZE);
            }
        }

        // Verify and consume the epilogue for multi-segment frames
        if is_multi {
            // epilogue_crc_rev1: 1 byte late_status + (MAX_NUM_SEGMENTS-1) × 4-byte CRC values
            let epilogue_size = 1 + (MAX_NUM_SEGMENTS - 1) * FRAME_CRC_SIZE;
            if buf.len() < epilogue_size {
                return Err(RadosError::Protocol(format!(
                    "Epilogue too short: need {}, have {}",
                    epilogue_size,
                    buf.len()
                )));
            }
            let late_status = buf[0];
            if (late_status & FRAME_LATE_STATUS_ABORTED_MASK) == FRAME_LATE_STATUS_ABORTED {
                return Err(RadosError::Protocol(
                    "Frame transmission aborted by sender".to_string(),
                ));
            }

            let mut epilogue_offset = 1;
            for seg_idx in 1..MAX_NUM_SEGMENTS {
                let mut crc_bytes = [0u8; FRAME_CRC_SIZE];
                crc_bytes.copy_from_slice(&buf[epilogue_offset..epilogue_offset + FRAME_CRC_SIZE]);
                let received_crc = u32::from_le_bytes(crc_bytes);
                epilogue_offset += FRAME_CRC_SIZE;

                if seg_idx < segments.len() && !segments[seg_idx].is_empty() {
                    let expected_crc = !crc32c::crc32c(&segments[seg_idx]);
                    if received_crc != expected_crc {
                        return Err(RadosError::Protocol(format!(
                            "Segment {} CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                            seg_idx, expected_crc, received_crc
                        )));
                    }
                }
            }
            buf.advance(epilogue_size);
        }

        F::from_segments(segments)
    }

    // Internal assembly logic
    fn assemble_frame(
        &mut self,
        tag: Tag,
        segments: &[Bytes],
        alignments: &[u16],
    ) -> Result<Bytes, RadosError> {
        if segments.is_empty() || segments.len() > MAX_NUM_SEGMENTS {
            return Err(RadosError::Protocol(format!(
                "Invalid segment count: {}",
                segments.len()
            )));
        }

        // Calculate non-empty segments (drop trailing empty ones)
        let mut num_segments = segments.len();
        while num_segments > 1 && segments[num_segments - 1].is_empty() {
            num_segments -= 1;
        }

        // Build descriptors
        self.descs.clear();
        for i in 0..num_segments {
            self.descs.push(SegmentDescriptor {
                logical_len: segments[i].len() as u32,
                align: alignments[i],
            });
        }

        // Build preamble
        let preamble = self.fill_preamble(tag, num_segments as u8);

        // Assembly depends on revision and encryption (we don't support encryption yet)
        if self.is_rev1 {
            self.assemble_crc_rev1(preamble, segments)
        } else {
            self.assemble_crc_rev0(preamble, segments)
        }
    }

    fn fill_preamble(&self, tag: Tag, num_segments: u8) -> Preamble {
        let mut preamble = Preamble {
            tag,
            num_segments,
            segments: [SegmentDescriptor {
                logical_len: 0,
                align: 0,
            }; MAX_NUM_SEGMENTS],
            flags: self.flags,
            reserved: 0,
            crc: 0,
        };

        for (i, desc) in self.descs.iter().enumerate() {
            preamble.segments[i] = *desc;
        }

        preamble
    }

    // msgr2.0 assembly: preamble + segments + epilogue (with all segment CRCs)
    fn assemble_crc_rev0(
        &self,
        preamble: Preamble,
        segments: &[Bytes],
    ) -> Result<Bytes, RadosError> {
        // epilogue_rev0 = late_flags(1) + crc_values(4 * MAX_NUM_SEGMENTS)
        let epilogue_size = 1 + 4 * MAX_NUM_SEGMENTS;
        let segments_size: usize = segments
            .iter()
            .take(self.descs.len())
            .map(|s| s.len())
            .sum();
        let total_size = PREAMBLE_SIZE + segments_size + epilogue_size;
        let mut frame = BytesMut::with_capacity(total_size);

        frame.extend_from_slice(&preamble.encode()?);

        let mut epilogue = EpilogueCrcRev0 {
            late_flags: 0, // Not aborted
            crc_values: [0; MAX_NUM_SEGMENTS],
        };

        for (i, segment) in segments.iter().enumerate().take(self.descs.len()) {
            frame.extend_from_slice(segment);
            epilogue.crc_values[i] = if self.with_data_crc {
                crc32c::crc32c(segment)
            } else {
                0
            };
        }

        self.write_epilogue_crc_rev0(&epilogue, &mut frame);

        Ok(frame.freeze())
    }

    // msgr2.1 assembly: preamble + first_segment + first_crc + remaining_segments + epilogue (if multi-segment)
    fn assemble_crc_rev1(
        &self,
        preamble: Preamble,
        segments: &[Bytes],
    ) -> Result<Bytes, RadosError> {
        let segments_size: usize = segments
            .iter()
            .take(self.descs.len())
            .map(|s| s.len())
            .sum();
        let first_crc_size = if !segments.is_empty() && !segments[0].is_empty() {
            FRAME_CRC_SIZE
        } else {
            0
        };
        // epilogue_rev1: late_status(1) + crc_values((MAX_NUM_SEGMENTS-1) * 4)
        let epilogue_size = if self.descs.len() > 1 {
            1 + (MAX_NUM_SEGMENTS - 1) * FRAME_CRC_SIZE
        } else {
            0
        };
        let total_size = PREAMBLE_SIZE + segments_size + first_crc_size + epilogue_size;
        let mut frame = BytesMut::with_capacity(total_size);

        frame.extend_from_slice(&preamble.encode()?);

        if !segments.is_empty() && !segments[0].is_empty() {
            frame.extend_from_slice(&segments[0]);
            let first_crc = if self.with_data_crc {
                !crc32c::crc32c(&segments[0])
            } else {
                0u32
            };
            frame.extend_from_slice(&first_crc.to_le_bytes());
        }

        if self.descs.len() == 1 {
            return Ok(frame.freeze());
        }

        let mut epilogue = EpilogueCrcRev1 {
            late_status: FRAME_LATE_STATUS_COMPLETE,
            crc_values: [0; MAX_NUM_SEGMENTS - 1],
        };

        for (i, segment) in segments
            .iter()
            .enumerate()
            .skip(1)
            .take(self.descs.len() - 1)
        {
            frame.extend_from_slice(segment);
            epilogue.crc_values[i - 1] = if self.with_data_crc {
                !crc32c::crc32c(segment)
            } else {
                0
            };
        }

        self.write_epilogue_crc_rev1(&epilogue, &mut frame);

        Ok(frame.freeze())
    }

    fn write_epilogue_crc_rev0(&self, epilogue: &EpilogueCrcRev0, buf: &mut BytesMut) {
        buf.extend_from_slice(&[epilogue.late_flags]);
        for &crc in &epilogue.crc_values {
            buf.extend_from_slice(&crc.to_le_bytes());
        }
    }

    fn write_epilogue_crc_rev1(&self, epilogue: &EpilogueCrcRev1, buf: &mut BytesMut) {
        buf.extend_from_slice(&[epilogue.late_status]);
        for &crc in &epilogue.crc_values {
            buf.extend_from_slice(&crc.to_le_bytes());
        }
    }
}

// Helper trait to unify encoding with optional features support
pub trait EncodeWithFeatures {
    fn encode_with_features(&self, features: u64) -> Result<Bytes, RadosError>;
}

// Default implementation for types that implement Denc
impl<T: Denc> EncodeWithFeatures for T {
    fn encode_with_features(&self, features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(64);
        self.encode(&mut buf, features)?;
        Ok(buf.freeze())
    }
}

// Base macro for defining frames (internal implementation detail)
macro_rules! define_frame {
    (
        $name:ident,
        tag = $tag:expr,
        segments = $num_segments:literal,
        alignments = [$($align:expr),+]
    ) => {
        impl $name {
            pub const TAG: Tag = $tag;
            pub const NUM_SEGMENTS: usize = $num_segments;
            pub const SEGMENT_ALIGNMENTS: [u16; $num_segments] = [$($align),+];
        }

        impl FrameTrait for $name {
            const TAG: Tag = $tag;
            const NUM_SEGMENTS: usize = $num_segments;

            fn segment_alignments() -> &'static [u16] {
                &Self::SEGMENT_ALIGNMENTS
            }

            fn get_segments(&self, features: u64) -> Result<Vec<Bytes>, RadosError> {
                self.segments_to_bytes(features)
            }

            fn from_segments(segments: Vec<Bytes>) -> Result<Self, RadosError> {
                Self::from_bytes_segments(segments)
            }
        }
    };
}

// Macro for control frames (internal implementation detail)
macro_rules! define_control_frame {
    (
        $name:ident,
        $tag:ident,
        $($field_name:ident: $field_type:ty),* $(,)?
    ) => {
        #[derive(Debug, Clone)]
        pub struct $name {
            $(pub $field_name: $field_type,)*
        }

        impl $name {
            #[allow(clippy::too_many_arguments)]
            pub fn new($($field_name: $field_type),*) -> Self {
                Self { $($field_name),* }
            }

            // Convert to wire format using FrameAssembler
            pub fn to_wire(&self, assembler: &mut FrameAssembler, features: u64) -> Result<Bytes, RadosError> {
                assembler.to_wire(self, features)
            }

            // Decode from wire format using FrameAssembler
            pub fn from_wire(buf: Bytes) -> Result<Self, RadosError> {
                FrameAssembler::from_wire(buf)
            }

            fn segments_to_bytes(&self, _features: u64) -> Result<Vec<Bytes>, RadosError> {
                let mut buf = BytesMut::with_capacity(128);
                $(
                    // Use the unified trait to handle both Denc and FeaturedDenc
                    let encoded = self.$field_name.encode_with_features(_features)?;
                    buf.extend_from_slice(&encoded);
                )*
                Ok(vec![buf.freeze()])
            }

            fn from_bytes_segments(mut segments: Vec<Bytes>) -> Result<Self, RadosError> {
                if segments.is_empty() {
                    return Err(RadosError::Protocol("No payload segment".to_string()));
                }
                let mut payload = segments.remove(0);
                Ok(Self {
                    $($field_name: <$field_type as Denc>::decode(&mut payload, 0)?,)*
                })
            }
        }

        // Apply the base macro
        define_frame!(
            $name,
            tag = Tag::$tag,
            segments = 1,
            alignments = [DEFAULT_ALIGNMENT]
        );
    };
}

// MessageFrame struct
#[derive(Debug, Clone)]
pub struct MessageFrame {
    pub header: MsgHeader,
    pub front: Bytes,
    pub middle: Bytes,
    pub data: Bytes,
}

impl MessageFrame {
    pub fn new(header: MsgHeader, front: Bytes, middle: Bytes, data: Bytes) -> Self {
        Self {
            header,
            front,
            middle,
            data,
        }
    }

    // Convert to wire format using FrameAssembler
    pub fn to_wire(
        &self,
        assembler: &mut FrameAssembler,
        features: u64,
    ) -> Result<Bytes, RadosError> {
        assembler.to_wire(self, features)
    }

    // Decode from wire format using FrameAssembler
    pub fn from_wire(buf: Bytes) -> Result<Self, RadosError> {
        FrameAssembler::from_wire(buf)
    }

    fn segments_to_bytes(&self, _features: u64) -> Result<Vec<Bytes>, RadosError> {
        let mut buf = BytesMut::new();
        <MsgHeader as Denc>::encode(&self.header, &mut buf, 0)?;
        Ok(vec![
            buf.freeze(),
            self.front.clone(),
            self.middle.clone(),
            self.data.clone(),
        ])
    }

    fn from_bytes_segments(mut segments: Vec<Bytes>) -> Result<Self, RadosError> {
        if segments.is_empty() {
            return Err(RadosError::Protocol("No header segment".to_string()));
        }

        // Ensure we have 4 segments
        while segments.len() < 4 {
            segments.push(Bytes::new());
        }

        let header = <MsgHeader as Denc>::decode(&mut segments[0], 0)?;

        Ok(Self {
            header,
            front: std::mem::take(&mut segments[1]),
            middle: std::mem::take(&mut segments[2]),
            data: std::mem::take(&mut segments[3]),
        })
    }
}

// Apply Frame trait to MessageFrame
define_frame!(
    MessageFrame,
    tag = Tag::Message,
    segments = 4,
    alignments = [
        DEFAULT_ALIGNMENT,
        DEFAULT_ALIGNMENT,
        DEFAULT_ALIGNMENT,
        PAGE_SIZE_ALIGNMENT
    ]
);

// Example control frames
define_control_frame!(
    HelloFrame,
    Hello,
    entity_type: u8,
    peer_addr: crate::EntityAddr
);

define_control_frame!(
    AuthRequestFrame,
    AuthRequest,
    method: u32,
    preferred_modes: Vec<u32>,
    auth_payload: Bytes
);

impl AuthRequestFrame {
    /// Parse AuthRequestFrame directly from a Frame's segments
    /// This avoids the inefficiency of encoding then decoding
    pub fn from_frame(frame: &Frame) -> Result<Self, RadosError> {
        if frame.segments.is_empty() {
            return Err(RadosError::Protocol(
                "AUTH_REQUEST frame missing payload".to_string(),
            ));
        }

        let mut payload = frame.segments[0].as_ref();
        Ok(Self {
            method: u32::decode(&mut payload, 0)?,
            preferred_modes: Vec::<u32>::decode(&mut payload, 0)?,
            auth_payload: Bytes::decode(&mut payload, 0)?,
        })
    }
}

define_control_frame!(
    AuthDoneFrame,
    AuthDone,
    global_id: u64,
    con_mode: u32,
    auth_payload: Bytes
);

define_control_frame!(
    AuthBadMethodFrame,
    AuthBadMethod,
    method: u32,
    result: i32,
    allowed_methods: Vec<u32>,
    allowed_modes: Vec<u32>
);

define_control_frame!(
    AuthReplyMoreFrame,
    AuthReplyMore,
    auth_payload: Bytes
);

define_control_frame!(
    AuthRequestMoreFrame,
    AuthRequestMore,
    auth_payload: Bytes
);

// AuthSignatureFrame - special case: signature is encoded as raw bytes without length prefix
#[derive(Debug, Clone)]
pub struct AuthSignatureFrame {
    pub signature: Bytes,
}

// CompressionRequestFrame - sent by client to request compression
define_control_frame!(
    CompressionRequestFrame,
    CompressionRequest,
    is_compress: bool,
    preferred_methods: Vec<u32>,
);

// CompressionDoneFrame - sent by server in response to compression request
define_control_frame!(
    CompressionDoneFrame,
    CompressionDone,
    is_compress: bool,
    method: u32,
);

impl AuthSignatureFrame {
    pub fn new(signature: Bytes) -> Self {
        Self { signature }
    }

    fn segments_to_bytes(&self, _features: u64) -> Result<Vec<Bytes>, RadosError> {
        // Encode signature as raw bytes without length prefix (unlike default Bytes encoding)
        Ok(vec![self.signature.clone()])
    }

    fn from_bytes_segments(mut segments: Vec<Bytes>) -> Result<Self, RadosError> {
        if segments.is_empty() {
            return Err(RadosError::Protocol("No payload segment".to_string()));
        }
        let signature = segments.remove(0);
        Ok(Self { signature })
    }
}

// Apply the base frame macro
define_frame!(
    AuthSignatureFrame,
    tag = Tag::AuthSignature,
    segments = 1,
    alignments = [DEFAULT_ALIGNMENT]
);

define_control_frame!(
    ClientIdentFrame,
    ClientIdent,
    addrs: crate::EntityAddrvec,
    target_addr: crate::EntityAddr,
    gid: i64,
    global_seq: u64,
    features_supported: u64,
    features_required: u64,
    flags: u64,
    cookie: u64
);

define_control_frame!(
    ServerIdentFrame,
    ServerIdent,
    addrs: crate::EntityAddrvec,
    gid: i64,
    global_seq: u64,
    features_supported: u64,
    features_required: u64,
    flags: u64,
    cookie: u64
);

define_control_frame!(
    IdentMissingFeaturesFrame,
    IdentMissingFeatures,
    missing_features: u64
);

// Keepalive frames for connection health monitoring
define_control_frame!(
    Keepalive2Frame,
    Keepalive2,
    timestamp_sec: u32,
    timestamp_nsec: u32
);

define_control_frame!(
    Keepalive2AckFrame,
    Keepalive2Ack,
    timestamp_sec: u32,
    timestamp_nsec: u32
);

// Session reconnection frames
define_control_frame!(
    SessionReconnectFrame,
    SessionReconnect,
    addrs: crate::EntityAddrvec,
    client_cookie: u64,
    server_cookie: u64,
    global_seq: u64,
    connect_seq: u64,
    msg_seq: u64
);

define_control_frame!(
    SessionReconnectOkFrame,
    SessionReconnectOk,
    msg_seq: u64
);

define_control_frame!(
    SessionRetryFrame,
    SessionRetry,
    connect_seq: u64
);

define_control_frame!(
    SessionRetryGlobalFrame,
    SessionRetryGlobal,
    global_seq: u64
);

define_control_frame!(
    SessionResetFrame,
    SessionReset,
    full: bool
);

// WaitFrame has no payload
#[derive(Debug, Clone)]
pub struct WaitFrame;

impl Default for WaitFrame {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitFrame {
    pub fn new() -> Self {
        Self
    }

    pub fn to_wire(
        &self,
        assembler: &mut FrameAssembler,
        features: u64,
    ) -> Result<Bytes, RadosError> {
        assembler.to_wire(self, features)
    }

    pub fn from_wire(buf: Bytes) -> Result<Self, RadosError> {
        FrameAssembler::from_wire(buf)
    }

    fn segments_to_bytes(&self, _features: u64) -> Result<Vec<Bytes>, RadosError> {
        Ok(vec![Bytes::new()])
    }

    fn from_bytes_segments(_segments: Vec<Bytes>) -> Result<Self, RadosError> {
        Ok(Self)
    }
}

define_frame!(
    WaitFrame,
    tag = Tag::Wait,
    segments = 1,
    alignments = [DEFAULT_ALIGNMENT]
);

/// Helper function to create a Frame from a FrameTrait.
pub fn create_frame_from_trait<F: FrameTrait>(
    frame_trait: &F,
    tag: Tag,
) -> Result<Frame, RadosError> {
    use crate::denc::features::CephFeatures;
    const MSGR2_FRAME_ASSUMED: u64 = CephFeatures::MASK_MSG_ADDR2
        .union(CephFeatures::MASK_SERVER_NAUTILUS)
        .bits();

    let segments = frame_trait.get_segments(MSGR2_FRAME_ASSUMED)?;
    let alignments = F::segment_alignments();
    Ok(Frame {
        preamble: Preamble {
            tag,
            num_segments: segments.len() as u8,
            segments: {
                let mut descs = [SegmentDescriptor::default(); 4];
                for (i, seg) in segments.iter().enumerate() {
                    descs[i] = SegmentDescriptor {
                        logical_len: seg.len() as u32,
                        align: alignments.get(i).copied().unwrap_or(DEFAULT_ALIGNMENT),
                    };
                }
                descs
            },
            flags: 0,
            reserved: 0,
            crc: 0,
        },
        segments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_assembler_rev0() {
        let frame = HelloFrame::new(1, crate::EntityAddr::default());
        let mut assembler = FrameAssembler::new(false); // rev0
        let features = 0;
        let wire_bytes = assembler.to_wire(&frame, features).unwrap();

        // rev0 should have: preamble + segment + epilogue
        // epilogue = 1 byte (late_flags) + 4*4 bytes (crc_values) = 17 bytes
        assert!(wire_bytes.len() >= PREAMBLE_SIZE + 17);
    }

    #[test]
    fn test_frame_assembler_rev1() {
        let frame = HelloFrame::new(1, crate::EntityAddr::default());
        let mut assembler = FrameAssembler::new(true); // rev1
        let features = 0;
        let wire_bytes = assembler.to_wire(&frame, features).unwrap();

        // rev1 single segment should have: preamble + segment + crc (4 bytes)
        // No epilogue for single segment in rev1
        assert!(wire_bytes.len() >= PREAMBLE_SIZE + FRAME_CRC_SIZE);
    }

    #[test]
    fn test_message_frame_rev1_multi_segment() {
        let frame = MessageFrame::new(
            MsgHeader::new_default(1, 100),
            Bytes::from("front"),
            Bytes::from("middle"),
            Bytes::from("data"),
        );

        let mut assembler = FrameAssembler::new(true); // rev1
        let features = 0;
        let wire_bytes = assembler.to_wire(&frame, features).unwrap();

        // rev1 multi-segment should have: preamble + first_segment + first_crc + remaining_segments + epilogue
        // epilogue = 1 byte (late_status) + 3*4 bytes (crc_values) = 13 bytes
        assert!(wire_bytes.len() >= PREAMBLE_SIZE + FRAME_CRC_SIZE + 13);
    }

    #[test]
    fn test_preamble_crc_calculation() {
        let frame = HelloFrame::new(1, crate::EntityAddr::default());
        let mut assembler = FrameAssembler::new(true);
        let features = 0;
        let wire_bytes = assembler.to_wire(&frame, features).unwrap();

        // Decode and verify preamble CRC
        let preamble = Preamble::decode(wire_bytes.slice(..PREAMBLE_SIZE)).unwrap();
        assert_ne!(preamble.crc, 0); // CRC should be calculated
    }

    #[test]
    fn test_message_frame_from_wire_roundtrip() {
        // Assemble a multi-segment MessageFrame and verify from_wire can parse it back
        let header = MsgHeader::new_default(42, 100);
        let front = Bytes::from("front payload");
        let middle = Bytes::from("middle payload");
        let data = Bytes::from("data payload");

        let frame = MessageFrame::new(header, front.clone(), middle.clone(), data.clone());

        // Assemble with rev1 (multi-segment)
        let mut assembler = FrameAssembler::new(true);
        let wire_bytes = assembler.to_wire(&frame, 0).unwrap();

        // Parse back using from_wire
        let decoded = MessageFrame::from_wire(wire_bytes).unwrap();

        assert_eq!(decoded.front, front);
        assert_eq!(decoded.middle, middle);
        assert_eq!(decoded.data, data);
    }

    #[test]
    fn test_message_frame_from_wire_aborted() {
        // Build a frame and corrupt the epilogue late_status to ABORTED
        let frame = MessageFrame::new(
            MsgHeader::new_default(1, 100),
            Bytes::from("front"),
            Bytes::from("middle"),
            Bytes::from("data"),
        );

        let mut assembler = FrameAssembler::new(true);
        let wire_bytes = assembler.to_wire(&frame, 0).unwrap();

        // Locate the epilogue: it's 13 bytes from the end
        let epilogue_offset = wire_bytes.len() - 13;
        let mut corrupted = BytesMut::from(&wire_bytes[..]);
        // Set late_status to ABORTED (0x1)
        corrupted[epilogue_offset] = FRAME_LATE_STATUS_ABORTED;

        let result = MessageFrame::from_wire(corrupted.freeze());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("aborted"), "Expected 'aborted' in '{}'", err);
    }

    #[test]
    fn test_message_frame_from_wire_rejects_corrupted_epilogue_crc_for_front_segment() {
        let frame = MessageFrame::new(
            MsgHeader::new_default(1, 100),
            Bytes::from("front"),
            Bytes::from("middle"),
            Bytes::from("data"),
        );

        let mut assembler = FrameAssembler::new(true);
        let wire_bytes = assembler.to_wire(&frame, 0).unwrap();

        let epilogue_offset = wire_bytes.len() - (1 + (MAX_NUM_SEGMENTS - 1) * FRAME_CRC_SIZE);
        let mut corrupted = BytesMut::from(&wire_bytes[..]);
        corrupted[epilogue_offset + 1] ^= 0xFF;

        let result = MessageFrame::from_wire(corrupted.freeze());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Segment 1 CRC mismatch"),
            "Expected segment 1 CRC mismatch in '{err}'"
        );
    }

    #[test]
    fn test_message_frame_from_wire_rejects_corrupted_epilogue_crc_for_data_segment() {
        let frame = MessageFrame::new(
            MsgHeader::new_default(1, 100),
            Bytes::from("front"),
            Bytes::from("middle"),
            Bytes::from("data"),
        );

        let mut assembler = FrameAssembler::new(true);
        let wire_bytes = assembler.to_wire(&frame, 0).unwrap();

        let epilogue_offset = wire_bytes.len() - (1 + (MAX_NUM_SEGMENTS - 1) * FRAME_CRC_SIZE);
        let data_crc_offset = epilogue_offset + 1 + 2 * FRAME_CRC_SIZE;
        let mut corrupted = BytesMut::from(&wire_bytes[..]);
        corrupted[data_crc_offset] ^= 0xFF;

        let result = MessageFrame::from_wire(corrupted.freeze());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Segment 3 CRC mismatch"),
            "Expected segment 3 CRC mismatch in '{err}'"
        );
    }

    #[test]
    fn test_encode_secure_payload_single_segment_adds_alignment_padding_only() {
        let frame = Frame::new(Tag::Keepalive2, Bytes::from_static(b"abc"));

        let payload = frame.encode_secure_payload();

        assert_eq!(payload.len(), CRYPTO_BLOCK_SIZE);
        assert_eq!(&payload[..3], b"abc");
        assert!(payload[3..].iter().all(|&byte| byte == 0));
    }

    #[test]
    fn test_encode_secure_payload_multi_segment_includes_epilogue_block() {
        let frame = Frame {
            preamble: Preamble {
                tag: Tag::Message,
                num_segments: 3,
                segments: [
                    SegmentDescriptor {
                        logical_len: 5,
                        align: DEFAULT_ALIGNMENT,
                    },
                    SegmentDescriptor {
                        logical_len: 0,
                        align: DEFAULT_ALIGNMENT,
                    },
                    SegmentDescriptor {
                        logical_len: 2,
                        align: DEFAULT_ALIGNMENT,
                    },
                    SegmentDescriptor::default(),
                ],
                flags: 0,
                reserved: 0,
                crc: 0,
            },
            segments: vec![
                Bytes::from_static(b"front"),
                Bytes::new(),
                Bytes::from_static(b"hi"),
            ],
        };

        let payload = frame.encode_secure_payload();

        assert_eq!(payload.len(), (2 * CRYPTO_BLOCK_SIZE) + CRYPTO_BLOCK_SIZE);
        assert_eq!(&payload[..5], b"front");
        assert!(payload[5..CRYPTO_BLOCK_SIZE].iter().all(|&byte| byte == 0));
        assert_eq!(&payload[CRYPTO_BLOCK_SIZE..CRYPTO_BLOCK_SIZE + 2], b"hi");
        assert!(
            payload[CRYPTO_BLOCK_SIZE + 2..(2 * CRYPTO_BLOCK_SIZE)]
                .iter()
                .all(|&byte| byte == 0)
        );
        assert_eq!(payload[2 * CRYPTO_BLOCK_SIZE], FRAME_LATE_STATUS_COMPLETE);
        assert!(
            payload[(2 * CRYPTO_BLOCK_SIZE + 1)..]
                .iter()
                .all(|&byte| byte == 0)
        );
    }

    #[test]
    fn test_tag_try_from_covers_all_wire_values() {
        let expected_tags = [
            Tag::Hello,
            Tag::AuthRequest,
            Tag::AuthBadMethod,
            Tag::AuthReplyMore,
            Tag::AuthRequestMore,
            Tag::AuthDone,
            Tag::AuthSignature,
            Tag::ClientIdent,
            Tag::ServerIdent,
            Tag::IdentMissingFeatures,
            Tag::SessionReconnect,
            Tag::SessionReset,
            Tag::SessionRetry,
            Tag::SessionRetryGlobal,
            Tag::SessionReconnectOk,
            Tag::Wait,
            Tag::Message,
            Tag::Keepalive2,
            Tag::Keepalive2Ack,
            Tag::Ack,
            Tag::CompressionRequest,
            Tag::CompressionDone,
        ];

        for (wire_value, expected_tag) in (1u8..=22).zip(expected_tags) {
            assert_eq!(Tag::try_from(wire_value).unwrap(), expected_tag);
        }
    }

    #[test]
    fn test_tag_try_from_rejects_unknown_wire_value() {
        let err = Tag::try_from(0).unwrap_err().to_string();
        assert!(err.contains("Unknown tag: 0"), "unexpected error: {err}");
    }

    #[test]
    fn test_preamble_crc_validation_failure() {
        let frame = HelloFrame::new(1, crate::EntityAddr::default());
        let mut assembler = FrameAssembler::new(true);
        let features = 0;
        let wire_bytes = assembler.to_wire(&frame, features).unwrap();

        // Corrupt the CRC field (last 4 bytes of preamble)
        let preamble_bytes = wire_bytes.slice(..PREAMBLE_SIZE);
        let mut corrupted = BytesMut::from(&preamble_bytes[..]);
        // Flip some bits in the CRC field
        corrupted[28] ^= 0xFF;
        corrupted[29] ^= 0xFF;

        // Attempt to decode - should fail with CRC mismatch
        let result = Preamble::decode(corrupted.freeze());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Preamble CRC mismatch")
        );
    }
}
