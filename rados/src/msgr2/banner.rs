//! Banner exchange types for the initial msgr2 feature-negotiation handshake.
//!
//! This module models the `ceph v2` banner exchanged at the start of an msgr2
//! connection. The banner carries the supported and required feature sets that
//! peers use before entering the authenticated protocol state machine.

use crate::msgr2::FeatureSet;
use crate::msgr2::error::{Msgr2Error as Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

pub const CEPH_BANNER: &[u8] = b"ceph v2";
pub const CEPH_BANNER_LEN: usize = CEPH_BANNER.len();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Banner {
    pub banner: Bytes,
    pub supported_features: FeatureSet,
    pub required_features: FeatureSet,
}

impl Banner {
    pub fn new() -> Self {
        Self {
            banner: Bytes::from_static(CEPH_BANNER),
            supported_features: FeatureSet::ALL,
            required_features: FeatureSet::empty(),
        }
    }

    pub fn new_with_features(
        supported_features: FeatureSet,
        required_features: FeatureSet,
    ) -> Self {
        Self {
            banner: Bytes::from_static(CEPH_BANNER),
            supported_features,
            required_features,
        }
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<()> {
        use crate::Denc;
        // Correct banner format based on our discoveries:
        // 1. "ceph v2\n" (8 bytes)
        // 2. payload length (2 bytes) - always 16 for features
        // 3. supported features (8 bytes)
        // 4. required features (8 bytes)
        // Total: 26 bytes

        // Send banner prefix "ceph v2"
        dst.extend_from_slice(&self.banner);

        // Send newline
        b'\n'.encode(dst, 0)?;

        // Payload size (always 16 bytes for the two 8-byte feature fields)
        16u16.encode(dst, 0)?;

        // Supported features (8 bytes, little-endian)
        u64::from(self.supported_features).encode(dst, 0)?;

        // Required features (8 bytes, little-endian)
        u64::from(self.required_features).encode(dst, 0)?;

        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        use crate::Denc;

        if src.remaining() < CEPH_BANNER_LEN + 1 {
            // +1 for newline
            return Err(Error::invalid_data("Incomplete banner prefix"));
        }

        let mut banner_bytes = vec![0u8; CEPH_BANNER_LEN];
        src.copy_to_slice(&mut banner_bytes);

        // Check if banner starts with "ceph v"
        if !banner_bytes.starts_with(b"ceph v") {
            return Err(Error::Protocol(format!(
                "Invalid banner prefix: expected 'ceph v', got {:?}",
                String::from_utf8_lossy(&banner_bytes)
            )));
        }

        // Read newline
        let newline = u8::decode(src, 0)?;
        if newline != b'\n' {
            return Err(Error::Protocol(format!(
                "Expected newline after banner, got: {}",
                newline
            )));
        }

        // Read payload size (uint16_t)
        let payload_size = u16::decode(src, 0)? as usize;

        // Read payload
        if src.remaining() < payload_size {
            return Err(Error::invalid_data("Incomplete banner payload"));
        }

        // Banner payload: supported_features (8 bytes) + required_features (8 bytes) = 16 bytes
        if payload_size < 16 {
            return Err(Error::Protocol(format!(
                "Invalid banner payload size: expected 16 bytes, got {}",
                payload_size
            )));
        }

        let supported_features = FeatureSet::from(u64::decode(src, 0)?);
        let required_features = FeatureSet::from(u64::decode(src, 0)?);

        if payload_size > 16 {
            tracing::warn!(
                "Unexpected banner payload size: {} (expected 16), skipping extra bytes",
                payload_size
            );
            src.advance(payload_size - 16);
        }
        Ok(Self {
            banner: Bytes::from(banner_bytes),
            supported_features,
            required_features,
        })
    }
}

impl Default for Banner {
    fn default() -> Self {
        Self::new()
    }
}

#[repr(C)]
#[derive(
    Debug,
    Copy,
    crate::ZeroCopyDencode,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
#[denc(crate = "crate")]
pub struct ConnectMessage {
    pub features: crate::denc::zerocopy::little_endian::U64,
    pub host_type: crate::denc::zerocopy::little_endian::U32,
    pub global_seq: crate::denc::zerocopy::little_endian::U32,
    pub connect_seq: crate::denc::zerocopy::little_endian::U32,
    pub protocol_version: crate::denc::zerocopy::little_endian::U32,
    pub authorizer_protocol: crate::denc::zerocopy::little_endian::U32,
    pub authorizer_len: crate::denc::zerocopy::little_endian::U32,
    pub flags: u8,
    padding: [u8; 3], // Explicit padding to match wire format
}

impl ConnectMessage {
    pub const LENGTH: usize = 36; // 8+4+4+4+4+4+4+1+3 = 36 bytes
    pub const PROTOCOL_VERSION: u32 = 2; // msgr2

    pub fn new(features: FeatureSet, host_type: u32) -> Self {
        use crate::denc::zerocopy::little_endian::{U32, U64};
        Self {
            features: U64::new(features.bits()),
            host_type: U32::new(host_type),
            global_seq: U32::new(0),
            connect_seq: U32::new(0),
            protocol_version: U32::new(Self::PROTOCOL_VERSION),
            authorizer_protocol: U32::new(0),
            authorizer_len: U32::new(0),
            flags: 0,
            padding: [0; 3],
        }
    }

    /// Get features as FeatureSet
    pub fn get_features(&self) -> FeatureSet {
        FeatureSet::from(self.features.get())
    }

    /// Set features from FeatureSet
    pub fn set_features(&mut self, features: FeatureSet) {
        self.features = crate::denc::zerocopy::little_endian::U64::new(features.bits());
    }

    pub fn with_auth(mut self, auth_protocol: u32, authorizer_len: u32) -> Self {
        self.authorizer_protocol = crate::denc::zerocopy::little_endian::U32::new(auth_protocol);
        self.authorizer_len = crate::denc::zerocopy::little_endian::U32::new(authorizer_len);
        self
    }

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        <Self as crate::Denc>::encode(self, dst, 0)?;
        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        Ok(<Self as crate::Denc>::decode(src, 0)?)
    }
}

impl Clone for ConnectMessage {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectReplyMessage {
    pub tag: u8,
    pub features: FeatureSet,
    pub global_seq: u32,
    pub connect_seq: u32,
    pub protocol_version: u32,
    pub authorizer_len: u32,
    pub flags: u8,
}

impl ConnectReplyMessage {
    pub const LENGTH: usize = 31;
    pub const REPLY_TAG_READY: u8 = 1;
    pub const REPLY_TAG_RESETSESSION: u8 = 2;
    pub const REPLY_TAG_WAIT: u8 = 3;
    pub const REPLY_TAG_RETRY_SESSION: u8 = 4;
    pub const REPLY_TAG_RETRY_GLOBAL: u8 = 5;
    pub const REPLY_TAG_BADPROTOVER: u8 = 6;
    pub const REPLY_TAG_BADAUTHORIZER: u8 = 7;
    pub const REPLY_TAG_FEATURES: u8 = 8;
    pub const REPLY_TAG_SEQ: u8 = 9;

    pub fn ready(features: FeatureSet, global_seq: u32, connect_seq: u32) -> Self {
        Self {
            tag: Self::REPLY_TAG_READY,
            features,
            global_seq,
            connect_seq,
            protocol_version: ConnectMessage::PROTOCOL_VERSION,
            authorizer_len: 0,
            flags: 0,
        }
    }

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        use crate::Denc;
        self.tag.encode(dst, 0)?;
        u64::from(self.features).encode(dst, 0)?;
        self.global_seq.encode(dst, 0)?;
        self.connect_seq.encode(dst, 0)?;
        self.protocol_version.encode(dst, 0)?;
        self.authorizer_len.encode(dst, 0)?;
        self.flags.encode(dst, 0)?;
        // padding
        0u8.encode(dst, 0)?;
        0u8.encode(dst, 0)?;

        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        use crate::Denc;
        let tag = u8::decode(src, 0)?;
        let features = FeatureSet::from(u64::decode(src, 0)?);
        let global_seq = u32::decode(src, 0)?;
        let connect_seq = u32::decode(src, 0)?;
        let protocol_version = u32::decode(src, 0)?;
        let authorizer_len = u32::decode(src, 0)?;
        let flags = u8::decode(src, 0)?;

        // Skip padding
        let _pad1 = u8::decode(src, 0)?;
        let _pad2 = u8::decode(src, 0)?;

        Ok(Self {
            tag,
            features,
            global_seq,
            connect_seq,
            protocol_version,
            authorizer_len,
            flags,
        })
    }

    pub fn is_ready(&self) -> bool {
        self.tag == Self::REPLY_TAG_READY
    }

    pub fn is_retry(&self) -> bool {
        matches!(
            self.tag,
            Self::REPLY_TAG_RETRY_SESSION | Self::REPLY_TAG_RETRY_GLOBAL
        )
    }

    pub fn is_error(&self) -> bool {
        matches!(
            self.tag,
            Self::REPLY_TAG_BADPROTOVER | Self::REPLY_TAG_BADAUTHORIZER | Self::REPLY_TAG_FEATURES
        )
    }
}

impl fmt::Display for ConnectReplyMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tag_name = match self.tag {
            Self::REPLY_TAG_READY => "READY",
            Self::REPLY_TAG_RESETSESSION => "RESETSESSION",
            Self::REPLY_TAG_WAIT => "WAIT",
            Self::REPLY_TAG_RETRY_SESSION => "RETRY_SESSION",
            Self::REPLY_TAG_RETRY_GLOBAL => "RETRY_GLOBAL",
            Self::REPLY_TAG_BADPROTOVER => "BADPROTOVER",
            Self::REPLY_TAG_BADAUTHORIZER => "BADAUTHORIZER",
            Self::REPLY_TAG_FEATURES => "FEATURES",
            Self::REPLY_TAG_SEQ => "SEQ",
            _ => "UNKNOWN",
        };

        write!(
            f,
            "ConnectReply(tag={}, features={:x})",
            tag_name,
            u64::from(self.features)
        )
    }
}
