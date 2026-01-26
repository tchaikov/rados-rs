//! PaxosServiceMessage base trait and utilities
//!
//! This module provides a unified encoding/decoding framework for messages
//! that inherit from PaxosServiceMessage in Ceph. These messages include
//! common paxos fields that must be encoded/decoded consistently.

use crate::error::{MonClientError, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Common fields for all PaxosServiceMessage types
///
/// These fields are encoded at the beginning of every PaxosServiceMessage
/// payload using the paxos_encode() method in C++.
#[derive(Debug, Clone, Default)]
pub struct PaxosFields {
    /// Version number for the paxos service
    pub version: u64,
    /// Deprecated session monitor ID (always -1)
    pub deprecated_session_mon: i16,
    /// Deprecated session monitor transaction ID (always 0)
    pub deprecated_session_mon_tid: u64,
}

impl PaxosFields {
    /// Create new PaxosFields with default values
    pub fn new() -> Self {
        Self {
            version: 0,
            deprecated_session_mon: -1,
            deprecated_session_mon_tid: 0,
        }
    }

    /// Create PaxosFields with a specific version
    pub fn with_version(version: u64) -> Self {
        Self {
            version,
            deprecated_session_mon: -1,
            deprecated_session_mon_tid: 0,
        }
    }

    /// Encode paxos fields to buffer (paxos_encode in C++)
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.version);
        buf.put_i16_le(self.deprecated_session_mon);
        buf.put_u64_le(self.deprecated_session_mon_tid);
    }

    /// Decode paxos fields from buffer (paxos_decode in C++)
    pub fn decode(data: &mut &[u8]) -> Result<Self> {
        if data.remaining() < 18 {
            // 8 + 2 + 8 = 18 bytes
            return Err(MonClientError::DecodingError(format!(
                "Incomplete PaxosFields: need 18 bytes, got {}",
                data.remaining()
            )));
        }

        let version = data.get_u64_le();
        let deprecated_session_mon = data.get_i16_le();
        let deprecated_session_mon_tid = data.get_u64_le();

        Ok(Self {
            version,
            deprecated_session_mon,
            deprecated_session_mon_tid,
        })
    }

    /// Size of encoded paxos fields in bytes
    pub const fn encoded_size() -> usize {
        18 // 8 + 2 + 8
    }
}

/// Trait for messages that inherit from PaxosServiceMessage
///
/// This trait provides a unified interface for encoding/decoding messages
/// that include the common paxos fields. Implementations should:
/// 1. Encode paxos fields first using encode_paxos()
/// 2. Encode message-specific fields
/// 3. Decode paxos fields first using decode_paxos()
/// 4. Decode message-specific fields
pub trait PaxosServiceMessage: Sized {
    /// Get the paxos fields for this message
    fn paxos_fields(&self) -> &PaxosFields;

    /// Get mutable paxos fields for this message
    fn paxos_fields_mut(&mut self) -> &mut PaxosFields;

    /// Encode the message-specific fields (after paxos fields)
    fn encode_message(&self, buf: &mut BytesMut) -> Result<()>;

    /// Decode the message-specific fields (after paxos fields)
    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self>;

    /// Encode the complete message (paxos fields + message fields)
    fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        // Encode paxos fields first
        self.paxos_fields().encode(&mut buf);

        // Encode message-specific fields
        self.encode_message(&mut buf)?;

        Ok(buf.freeze())
    }

    /// Decode the complete message (paxos fields + message fields)
    fn decode(mut data: &[u8]) -> Result<Self> {
        // Decode paxos fields first
        let paxos = PaxosFields::decode(&mut data)?;

        // Decode message-specific fields
        Self::decode_message(paxos, &mut data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paxos_fields_encode_decode() {
        let fields = PaxosFields {
            version: 42,
            deprecated_session_mon: -1,
            deprecated_session_mon_tid: 0,
        };

        let mut buf = BytesMut::new();
        fields.encode(&mut buf);

        assert_eq!(buf.len(), PaxosFields::encoded_size());

        let mut data = buf.as_ref();
        let decoded = PaxosFields::decode(&mut data).unwrap();

        assert_eq!(decoded.version, 42);
        assert_eq!(decoded.deprecated_session_mon, -1);
        assert_eq!(decoded.deprecated_session_mon_tid, 0);
    }

    #[test]
    fn test_paxos_fields_default() {
        let fields = PaxosFields::new();
        assert_eq!(fields.version, 0);
        assert_eq!(fields.deprecated_session_mon, -1);
        assert_eq!(fields.deprecated_session_mon_tid, 0);
    }
}
