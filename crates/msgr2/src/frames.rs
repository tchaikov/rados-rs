// This module implements the msgr2.1 frame protocol
// The macros (define_frame, define_control_frame) are internal implementation details
// Public API consists of:
//   - Frame trait
//   - FrameAssembler
//   - Concrete frame types (HelloFrame, AuthRequestFrame, MessageFrame, etc.)
//   - Tag enum and protocol constants

use crate::header::MsgHeader;
use bytes::{Bytes, BytesMut};
use denc::Denc;
use denc::RadosError;

// Protocol constants
pub const MAX_NUM_SEGMENTS: usize = 4;
pub const DEFAULT_ALIGNMENT: u16 = 8;
pub const PAGE_SIZE_ALIGNMENT: u16 = 4096;
pub const PREAMBLE_SIZE: usize = 32;
pub const CRYPTO_BLOCK_SIZE: usize = 16;
pub const FRAME_CRC_SIZE: usize = 4;

// Frame flags
pub const FRAME_EARLY_DATA_COMPRESSED: u8 = 0x01;

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
            1..=22 => unsafe { Ok(std::mem::transmute::<u8, Tag>(value)) },
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
    pub fn to_wire(&self, is_rev1: bool) -> Bytes {
        let mut assembler = FrameAssembler::new(is_rev1);
        // Copy compression flag from preamble if set
        if self.preamble.is_compressed() {
            assembler.set_compression(true);
        }
        let alignments: Vec<u16> = self.preamble.segments[..self.preamble.num_segments as usize]
            .iter()
            .map(|s| s.align)
            .collect();
        assembler
            .assemble_frame(self.preamble.tag, &self.segments, &alignments)
            .expect("Failed to assemble frame")
    }

    /// Create a compressed version of this frame
    /// Returns a new Frame with compressed segments and FRAME_EARLY_DATA_COMPRESSED flag set
    pub fn compress(
        &self,
        ctx: &crate::compression::CompressionContext,
    ) -> Result<Self, RadosError> {
        // Calculate total uncompressed size
        let total_size: usize = self.segments.iter().map(|s| s.len()).sum();

        // Check if we should compress based on threshold
        if !ctx.should_compress(total_size) {
            tracing::debug!(
                "Frame size {} below compression threshold {}, skipping compression",
                total_size,
                ctx.threshold()
            );
            return Ok(self.clone());
        }

        // Compress all segments into a single segment
        let mut uncompressed = BytesMut::with_capacity(total_size);
        for segment in &self.segments {
            uncompressed.extend_from_slice(segment);
        }

        let compressed = ctx
            .compress(&uncompressed)
            .map_err(|e| RadosError::Protocol(format!("Compression failed: {}", e)))?;

        tracing::debug!(
            "Compressed frame: {} bytes -> {} bytes (ratio: {:.2}%)",
            total_size,
            compressed.len(),
            (compressed.len() as f64 / total_size as f64) * 100.0
        );

        // Create new frame with compressed data
        let mut new_preamble = self.preamble.clone();
        new_preamble.set_compressed();
        new_preamble.num_segments = 1;
        new_preamble.segments[0] = SegmentDescriptor {
            logical_len: compressed.len() as u32,
            align: DEFAULT_ALIGNMENT,
        };
        // Store original size in reserved field for decompression
        new_preamble.reserved = (total_size & 0xFF) as u8;

        Ok(Frame {
            preamble: new_preamble,
            segments: vec![compressed],
        })
    }

    /// Decompress this frame if it's compressed
    /// Returns a new Frame with decompressed segments
    pub fn decompress(
        &self,
        ctx: &crate::compression::CompressionContext,
        original_size: usize,
    ) -> Result<Self, RadosError> {
        // Check if frame is compressed
        if !self.preamble.is_compressed() {
            // Not compressed, return as-is
            return Ok(self.clone());
        }

        // Decompress the single compressed segment
        if self.segments.len() != 1 {
            return Err(RadosError::Protocol(format!(
                "Compressed frame should have exactly 1 segment, got {}",
                self.segments.len()
            )));
        }

        let compressed = &self.segments[0];
        let decompressed = ctx
            .decompress(compressed, original_size)
            .map_err(|e| RadosError::Protocol(format!("Decompression failed: {}", e)))?;

        tracing::debug!(
            "Decompressed frame: {} bytes -> {} bytes",
            compressed.len(),
            decompressed.len()
        );

        // Create new frame with decompressed data
        let mut new_preamble = self.preamble.clone();
        new_preamble.clear_compressed();
        new_preamble.num_segments = 1;
        new_preamble.segments[0] = SegmentDescriptor {
            logical_len: decompressed.len() as u32,
            align: DEFAULT_ALIGNMENT,
        };
        new_preamble.reserved = 0;

        Ok(Frame {
            preamble: new_preamble,
            segments: vec![decompressed],
        })
    }

    /// Legacy encode method - just concatenates preamble and segments without epilogue
    /// This is NOT suitable for sending over the network
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.preamble.encode());
        for segment in &self.segments {
            buf.extend_from_slice(segment);
        }
        buf.freeze()
    }
}

// Core FrameTrait - only provides data, no serialization logic
pub trait FrameTrait: Sized {
    const TAG: Tag;
    const NUM_SEGMENTS: usize;

    fn segment_alignments() -> &'static [u16];
    fn get_segments(&self, features: u64) -> Vec<Bytes>;
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

    pub fn encode(&self) -> Bytes {
        use denc::Denc;
        let mut buf = BytesMut::with_capacity(PREAMBLE_SIZE);

        (self.tag as u8).encode(&mut buf, 0).unwrap();
        self.num_segments.encode(&mut buf, 0).unwrap();

        for segment in &self.segments {
            segment.logical_len.encode(&mut buf, 0).unwrap();
            segment.align.encode(&mut buf, 0).unwrap();
        }

        self.flags.encode(&mut buf, 0).unwrap();
        self.reserved.encode(&mut buf, 0).unwrap();

        // Calculate CRC using Ceph's non-standard CRC32C algorithm
        // Ceph's ceph_crc32c() produces different results than standard CRC32C
        // We need to match Ceph's algorithm: !crc32c_append(0xFFFFFFFF, data)
        let crc = !crc32c::crc32c_append(0xFFFFFFFF, &buf[..28]);

        crc.encode(&mut buf, 0).unwrap();

        buf.freeze()
    }

    pub fn decode(mut buf: Bytes) -> Result<Self, RadosError> {
        use denc::Denc;
        if buf.len() < PREAMBLE_SIZE {
            return Err(RadosError::Protocol("Preamble too short".to_string()));
        }

        let tag = Tag::try_from(u8::decode(&mut buf, 0)?)?;
        let num_segments = u8::decode(&mut buf, 0)?;

        let mut segments = [SegmentDescriptor {
            logical_len: 0,
            align: 0,
        }; MAX_NUM_SEGMENTS];
        for segment in segments.iter_mut() {
            segment.logical_len = u32::decode(&mut buf, 0)?;
            segment.align = u16::decode(&mut buf, 0)?;
        }

        let flags = u8::decode(&mut buf, 0)?;
        let reserved = u8::decode(&mut buf, 0)?;
        let crc = u32::decode(&mut buf, 0)?;

        // TODO: Verify CRC

        Ok(Self {
            tag,
            num_segments,
            segments,
            flags,
            reserved,
            crc,
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

    pub fn get_is_rev1(&self) -> bool {
        self.is_rev1
    }

    pub fn set_is_rev1(&mut self, is_rev1: bool) {
        self.is_rev1 = is_rev1;
        self.descs.clear();
        self.flags = 0;
    }

    // Main entry point - takes any FrameTrait and serializes it to wire format
    pub fn to_wire<F: FrameTrait>(
        &mut self,
        frame: &F,
        features: u64,
    ) -> Result<Bytes, RadosError> {
        let segments = frame.get_segments(features);
        let alignments = F::segment_alignments();
        self.assemble_frame(F::TAG, &segments, alignments)
    }

    // Deserialize from wire format
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

        // Extract segments based on preamble
        let mut segments = Vec::new();
        for i in 0..preamble.num_segments as usize {
            let len = preamble.segments[i].logical_len as usize;
            if buf.len() < len {
                return Err(RadosError::Protocol(
                    "Insufficient segment data".to_string(),
                ));
            }
            segments.push(buf.split_to(len));
        }

        // TODO: Handle epilogue in msgr2.0 or multi-segment msgr2.1

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
        let mut frame = BytesMut::new();

        // Add preamble
        frame.extend_from_slice(&preamble.encode());

        // Add segments and calculate their CRCs
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

        // Add epilogue
        frame.extend_from_slice(&self.encode_epilogue_crc_rev0(&epilogue));

        Ok(frame.freeze())
    }

    // msgr2.1 assembly: preamble + first_segment + first_crc + remaining_segments + epilogue (if multi-segment)
    fn assemble_crc_rev1(
        &self,
        preamble: Preamble,
        segments: &[Bytes],
    ) -> Result<Bytes, RadosError> {
        let mut frame = BytesMut::new();

        // Add preamble
        frame.extend_from_slice(&preamble.encode());

        // Add first segment with its CRC
        if !segments.is_empty() && !segments[0].is_empty() {
            frame.extend_from_slice(&segments[0]);
            let first_crc = if self.with_data_crc {
                // Ceph uses segment_bl.crc32c(-1) = ceph_crc32c(0xFFFFFFFF, data, len)
                // This is the raw CRC without final XOR
                !crc32c::crc32c(&segments[0])
            } else {
                0u32
            };
            frame.extend_from_slice(&first_crc.to_le_bytes());
        }

        // If only one segment, we're done (no epilogue in msgr2.1 for single segment)
        if self.descs.len() == 1 {
            return Ok(frame.freeze());
        }

        // Add remaining segments and build epilogue
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
                // Ceph uses segment_bl.crc32c(-1) = ceph_crc32c(0xFFFFFFFF, data, len)
                // This is the raw CRC without final XOR
                !crc32c::crc32c(segment)
            } else {
                0
            };
        }

        // Add epilogue
        frame.extend_from_slice(&self.encode_epilogue_crc_rev1(&epilogue));

        Ok(frame.freeze())
    }

    fn encode_epilogue_crc_rev0(&self, epilogue: &EpilogueCrcRev0) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1 + 4 * MAX_NUM_SEGMENTS);
        bytes.push(epilogue.late_flags);
        for &crc in &epilogue.crc_values {
            bytes.extend_from_slice(&crc.to_le_bytes());
        }
        bytes
    }

    fn encode_epilogue_crc_rev1(&self, epilogue: &EpilogueCrcRev1) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1 + 4 * (MAX_NUM_SEGMENTS - 1));
        bytes.push(epilogue.late_status);
        for &crc in &epilogue.crc_values {
            bytes.extend_from_slice(&crc.to_le_bytes());
        }
        bytes
    }
}

// Helper trait to unify encoding with optional features support
pub trait EncodeWithFeatures {
    fn encode_with_features(&self, features: u64) -> Result<Bytes, RadosError>;
}

// Default implementation for types that implement Denc
impl<T: Denc> EncodeWithFeatures for T {
    fn encode_with_features(&self, features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
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

            fn get_segments(&self, features: u64) -> Vec<Bytes> {
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

            fn segments_to_bytes(&self, _features: u64) -> Vec<Bytes> {
                let mut buf = BytesMut::new();
                $(
                    // Use the unified trait to handle both Denc and FeaturedDenc
                    let encoded = self.$field_name.encode_with_features(_features)
                        .expect("Failed to encode field");
                    buf.extend_from_slice(&encoded);
                )*
                vec![buf.freeze()]
            }

            fn from_bytes_segments(mut segments: Vec<Bytes>) -> Result<Self, RadosError> {
                if segments.is_empty() {
                    return Err(RadosError::Protocol("No payload segment".to_string()));
                }
                let mut payload = segments.remove(0);
                Ok(Self {
                    $($field_name: <$field_type as Denc>::decode(&mut payload, 0).map_err(|e| RadosError::Denc(e.to_string()))?,)*
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

    fn segments_to_bytes(&self, _features: u64) -> Vec<Bytes> {
        vec![
            {
                let mut buf = BytesMut::new();
                self.header.encode(&mut buf).unwrap_or(());
                buf.freeze()
            },
            self.front.clone(),
            self.middle.clone(),
            self.data.clone(),
        ]
    }

    fn from_bytes_segments(mut segments: Vec<Bytes>) -> Result<Self, RadosError> {
        if segments.is_empty() {
            return Err(RadosError::Protocol("No header segment".to_string()));
        }

        // Ensure we have 4 segments
        while segments.len() < 4 {
            segments.push(Bytes::new());
        }

        let header =
            MsgHeader::decode(&mut segments[0]).map_err(|e| RadosError::Denc(e.to_string()))?;

        Ok(Self {
            header,
            front: segments[1].clone(),
            middle: segments[2].clone(),
            data: segments[3].clone(),
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
    peer_addr: denc::EntityAddr
);

define_control_frame!(
    AuthRequestFrame,
    AuthRequest,
    method: u32,
    preferred_modes: Vec<u32>,
    auth_payload: Bytes
);

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

    fn segments_to_bytes(&self, _features: u64) -> Vec<Bytes> {
        // Encode signature as raw bytes without length prefix (unlike default Bytes encoding)
        vec![self.signature.clone()]
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
    addrs: denc::EntityAddrvec,
    target_addr: denc::EntityAddr,
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
    addrs: denc::EntityAddrvec,
    gid: i64,
    global_seq: u64,
    features_supported: u64,
    features_required: u64,
    flags: u64,
    cookie: u64
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
    addrs: denc::EntityAddrvec,
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

    fn segments_to_bytes(&self, _features: u64) -> Vec<Bytes> {
        vec![Bytes::new()]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_assembler_rev0() {
        let frame = HelloFrame::new(1, denc::EntityAddr::default());
        let mut assembler = FrameAssembler::new(false); // rev0
        let features = 0;
        let wire_bytes = assembler.to_wire(&frame, features).unwrap();

        // rev0 should have: preamble + segment + epilogue
        // epilogue = 1 byte (late_flags) + 4*4 bytes (crc_values) = 17 bytes
        assert!(wire_bytes.len() >= PREAMBLE_SIZE + 17);
    }

    #[test]
    fn test_frame_assembler_rev1() {
        let frame = HelloFrame::new(1, denc::EntityAddr::default());
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
        let frame = HelloFrame::new(1, denc::EntityAddr::default());
        let mut assembler = FrameAssembler::new(true);
        let features = 0;
        let wire_bytes = assembler.to_wire(&frame, features).unwrap();

        // Decode and verify preamble CRC
        let preamble = Preamble::decode(wire_bytes.slice(..PREAMBLE_SIZE)).unwrap();
        assert_ne!(preamble.crc, 0); // CRC should be calculated
    }
}
