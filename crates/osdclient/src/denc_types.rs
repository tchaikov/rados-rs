//! Denc implementations for OSD message types
//!
//! This module provides Denc trait implementations for structures used in
//! MOSDOp encoding, following Ceph's encoding patterns.

use bytes::{Buf, BufMut};
use denc::denc::{Denc, VersionedEncode};
use denc::error::RadosError;

use crate::types::{
    BlkinTraceInfo, EntityName, JaegerSpanContext, OSDOp, ObjectLocator, OpCode, OpData, PgId,
    RequestRedirect, StripedPgId,
};

// Re-export types for convenience
pub use crate::types::{
    BlkinTraceInfo as BlkinTraceInfoType, EntityName as EntityNameType,
    JaegerSpanContext as JaegerSpanContextType, PgId as PgIdType, StripedPgId as StripedPgIdType,
};

// Re-export entity type constants for convenience
pub use crate::types::{
    CEPH_ENTITY_TYPE_AUTH, CEPH_ENTITY_TYPE_CLIENT, CEPH_ENTITY_TYPE_MDS, CEPH_ENTITY_TYPE_MGR,
    CEPH_ENTITY_TYPE_MON, CEPH_ENTITY_TYPE_OSD,
};

// ============= PgId (pg_t) =============

impl Denc for PgId {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // pg_t encoding: version byte + pool (i64) + seed (u32) + preferred (i32, always -1)
        1u8.encode(buf, 0)?; // version
        self.pool.encode(buf, 0)?;
        self.seed.encode(buf, 0)?;
        (-1i32).encode(buf, 0)?; // preferred (deprecated, always -1)
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        let version = u8::decode(buf, 0)?;
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unknown pg_t version: {}",
                version
            )));
        }

        let pool = i64::decode(buf, 0)?;
        let seed = u32::decode(buf, 0)?;
        let _preferred = i32::decode(buf, 0)?; // deprecated

        Ok(PgId { pool, seed })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1 + 8 + 4 + 4) // version + pool + seed + preferred = 17 bytes
    }
}

// ============= StripedPgId (spg_t) =============

impl VersionedEncode for StripedPgId {
    fn encoding_version(&self, _features: u64) -> u8 {
        1
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Encode pgid (pg_t)
        let pgid = PgId {
            pool: self.pool,
            seed: self.seed,
        };
        pgid.encode(buf, features)?;

        // Encode shard (i8)
        self.shard.encode(buf, 0)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Decode pgid (pg_t)
        let pgid = PgId::decode(buf, features)?;

        // Decode shard (i8)
        let shard = i8::decode(buf, 0)?;

        Ok(StripedPgId {
            pool: pgid.pool,
            seed: pgid.seed,
            shard,
        })
    }
}

impl Denc for StripedPgId {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // Version header: struct_v (1) + struct_compat (1) + len (4) = 6 bytes
        // Content: pgid (17) + shard (1) = 18 bytes
        // Total: 6 + 18 = 24 bytes
        Some(6 + 17 + 1)
    }
}

// ============= EntityName (entity_name_t) =============

impl Denc for EntityName {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // entity_name_t encoding: type (u8) + num (u64)
        self.entity_type.encode(buf, 0)?;
        self.num.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        let entity_type = u8::decode(buf, 0)?;
        let num = u64::decode(buf, 0)?;

        Ok(EntityName { entity_type, num })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1 + 8) // type + num = 9 bytes
    }
}

// ============= OsdReqId (osd_reqid_t) =============

/// OSD request ID with versioned encoding
///
/// This structure uses a special versioning scheme (DENC_START_OSD_REQID)
/// that enforces version 2 and compat 2.
pub struct OsdReqId {
    pub name: EntityName,
    pub tid: u64,
    pub inc: i32,
}

impl VersionedEncode for OsdReqId {
    fn encoding_version(&self, _features: u64) -> u8 {
        2
    }

    fn compat_version(&self, _features: u64) -> u8 {
        2
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Encode entity_name_t
        self.name.encode(buf, features)?;

        // Encode tid (u64)
        self.tid.encode(buf, 0)?;

        // Encode inc (i32)
        self.inc.encode(buf, 0)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Decode entity_name_t
        let name = EntityName::decode(buf, features)?;

        // Decode tid (u64)
        let tid = u64::decode(buf, 0)?;

        // Decode inc (i32)
        let inc = i32::decode(buf, 0)?;

        Ok(OsdReqId { name, tid, inc })
    }
}

impl Denc for OsdReqId {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // Version header: struct_v (1) + struct_compat (1) + len (4) = 6 bytes
        // Content: entity_name (9) + tid (8) + inc (4) = 21 bytes
        // Total: 6 + 21 = 27 bytes
        Some(6 + 9 + 8 + 4)
    }
}

// ============= BlkinTraceInfo =============

impl Denc for BlkinTraceInfo {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // blkin_trace_info encoding: 3 x u64
        self.trace_id.encode(buf, 0)?;
        self.span_id.encode(buf, 0)?;
        self.parent_span_id.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        let trace_id = u64::decode(buf, 0)?;
        let span_id = u64::decode(buf, 0)?;
        let parent_span_id = u64::decode(buf, 0)?;

        Ok(BlkinTraceInfo {
            trace_id,
            span_id,
            parent_span_id,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(24) // 3 x u64 = 24 bytes
    }
}

// ============= JaegerSpanContext (jspan_context) =============

impl VersionedEncode for JaegerSpanContext {
    fn encoding_version(&self, _features: u64) -> u8 {
        1
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // When Jaeger is not enabled, just encode is_valid flag
        (if self.is_valid { 1u8 } else { 0u8 }).encode(buf, 0)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        _features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        let is_valid = u8::decode(buf, 0)? != 0;

        Ok(JaegerSpanContext { is_valid })
    }
}

impl Denc for JaegerSpanContext {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // Version header: struct_v (1) + struct_compat (1) + len (4) = 6 bytes
        // Content: is_valid (1) = 1 byte
        // Total: 6 + 1 = 7 bytes
        Some(6 + 1)
    }
}

// ============= ObjectLocator (object_locator_t) =============

impl VersionedEncode for ObjectLocator {
    fn encoding_version(&self, _features: u64) -> u8 {
        6
    }

    fn compat_version(&self, _features: u64) -> u8 {
        3
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Encode pool (i64)
        self.pool.encode(buf, 0)?;

        // Encode preferred (i32, always -1, deprecated field)
        (-1i32).encode(buf, 0)?;

        // Encode key (String)
        self.key.encode(buf, features)?;

        // Encode nspace (String)
        self.nspace.encode(buf, features)?;

        // Encode hash (i64)
        self.hash.encode(buf, 0)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Decode pool (i64)
        let pool = i64::decode(buf, 0)?;

        // Decode preferred (i32, deprecated)
        let _preferred = i32::decode(buf, 0)?;

        // Decode key (String)
        let key = String::decode(buf, features)?;

        // Decode nspace (String)
        let nspace = String::decode(buf, features)?;

        // Decode hash (i64)
        let hash = i64::decode(buf, 0)?;

        Ok(ObjectLocator {
            pool,
            key,
            nspace,
            hash,
        })
    }
}

impl Denc for ObjectLocator {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Version header: struct_v (1) + struct_compat (1) + len (4) = 6 bytes
        // Content: pool (8) + preferred (4) + key + nspace + hash (8)
        // Key and nspace are variable-length strings
        let key_size = self.key.encoded_size(features)?;
        let nspace_size = self.nspace.encoded_size(features)?;
        Some(6 + 8 + 4 + key_size + nspace_size + 8)
    }
}

// ============= RequestRedirect (request_redirect_t) =============

impl VersionedEncode for RequestRedirect {
    fn encoding_version(&self, _features: u64) -> u8 {
        1
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Encode redirect_locator (object_locator_t)
        self.redirect_locator.encode(buf, features)?;

        // Encode redirect_object (String)
        self.redirect_object.encode(buf, features)?;

        // Encode legacy field (u32, always 0)
        0u32.encode(buf, 0)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Decode redirect_locator (object_locator_t)
        let redirect_locator = ObjectLocator::decode(buf, features)?;

        // Decode redirect_object (String)
        let redirect_object = String::decode(buf, features)?;

        // Decode legacy field (u32, ignore)
        let _legacy = u32::decode(buf, 0)?;

        Ok(RequestRedirect {
            redirect_locator,
            redirect_object,
        })
    }
}

impl Denc for RequestRedirect {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Version header: struct_v (1) + struct_compat (1) + len (4) = 6 bytes
        // Content: redirect_locator + redirect_object + legacy (4)
        let locator_size = self.redirect_locator.encoded_size(features)?;
        let object_size = self.redirect_object.encoded_size(features)?;
        Some(6 + locator_size + object_size + 4)
    }
}

// ============= OSDOp (ceph_osd_op) =============

/// Denc implementation for OSDOp
///
/// This implements encoding/decoding for the 38-byte ceph_osd_op structure:
/// - op (u16) - 2 bytes
/// - flags (u32) - 4 bytes
/// - union (28 bytes) - OpData variant based on op
/// - payload_len (u32) - 4 bytes
///
/// Note: The actual payload data (indata) is stored separately in the message
/// data section and is not part of this 38-byte structure.
impl Denc for OSDOp {
    const USES_VERSIONING: bool = false;

    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // 1. op (u16)
        buf.put_u16_le(self.op.as_u16());

        // 2. flags (u32)
        buf.put_u32_le(self.flags);

        // 3. Union (28 bytes) - OpData variant based on op
        match &self.op_data {
            OpData::Extent {
                offset,
                length,
                truncate_size,
                truncate_seq,
            } => {
                buf.put_u64_le(*offset);
                buf.put_u64_le(*length);
                buf.put_u64_le(*truncate_size);
                buf.put_u32_le(*truncate_seq);
                // 8 + 8 + 8 + 4 = 28 bytes ✓
            }
            OpData::Pgls {
                max_entries,
                start_epoch,
            } => {
                buf.put_u64_le(*max_entries);
                buf.put_u32_le(*start_epoch);
                // Pad to 28 bytes: 8 + 4 = 12, need 16 more
                buf.put_u64_le(0);
                buf.put_u64_le(0);
            }
            OpData::Xattr {
                name_len,
                value_len,
                cmp_op,
                cmp_mode,
            } => {
                buf.put_u32_le(*name_len);
                buf.put_u32_le(*value_len);
                buf.put_u8(*cmp_op);
                buf.put_u8(*cmp_mode);
                // Pad to 28 bytes: 4 + 4 + 1 + 1 = 10, need 18 more
                buf.put_u64_le(0);
                buf.put_u64_le(0);
                buf.put_u16_le(0);
            }
            OpData::None => {
                // Empty union - 28 bytes of zeros
                buf.put_u64_le(0);
                buf.put_u64_le(0);
                buf.put_u64_le(0);
                buf.put_u32_le(0);
            }
        }

        // 4. payload_len (u32)
        buf.put_u32_le(self.indata.len() as u32);

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        // 1. op (u16)
        let op_code = buf.get_u16_le();
        let op = OpCode::from_u16(op_code).ok_or_else(|| {
            RadosError::Protocol(format!("Unknown operation code: 0x{:04x}", op_code))
        })?;

        // 2. flags (u32)
        let flags = buf.get_u32_le();

        // 3. Union (28 bytes) - decode based on op
        let op_data = match op {
            OpCode::Read | OpCode::Write | OpCode::WriteFull | OpCode::Truncate | OpCode::Stat => {
                // Extent-based operations
                let offset = buf.get_u64_le();
                let length = buf.get_u64_le();
                let truncate_size = buf.get_u64_le();
                let truncate_seq = buf.get_u32_le();
                OpData::Extent {
                    offset,
                    length,
                    truncate_size,
                    truncate_seq,
                }
            }
            OpCode::Pgls => {
                // PG list operation
                let max_entries = buf.get_u64_le();
                let start_epoch = buf.get_u32_le();
                // Skip padding (16 bytes)
                buf.advance(16);
                OpData::Pgls {
                    max_entries,
                    start_epoch,
                }
            }
            OpCode::GetXattr | OpCode::SetXattr => {
                // Extended attribute operations
                let name_len = buf.get_u32_le();
                let value_len = buf.get_u32_le();
                let cmp_op = buf.get_u8();
                let cmp_mode = buf.get_u8();
                // Skip padding (18 bytes)
                buf.advance(18);
                OpData::Xattr {
                    name_len,
                    value_len,
                    cmp_op,
                    cmp_mode,
                }
            }
            _ => {
                // Other operations - skip 28 bytes
                buf.advance(28);
                OpData::None
            }
        };

        // 4. payload_len (u32)
        let _payload_len = buf.get_u32_le();

        // Note: We don't decode the actual payload data here - that comes from
        // the message data section. We just store the length for now.
        // The caller will need to extract the data separately.

        Ok(Self {
            op,
            flags,
            op_data,
            indata: bytes::Bytes::new(), // Will be filled from data section
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // Fixed size: 38 bytes
        // op (2) + flags (4) + union (28) + payload_len (4) = 38
        Some(38)
    }
}

// ============= OsdStatData =============

/// Result data from CEPH_OSD_OP_STAT operation
///
/// This contains the object size and modification time returned by the OSD.
/// Format: u64 size + SystemTime (u32 sec + u32 nsec as utime_t)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OsdStatData {
    pub size: u64,
    pub mtime: std::time::SystemTime,
}

impl Denc for OsdStatData {
    const USES_VERSIONING: bool = false;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        buf.put_u64_le(self.size);
        self.mtime.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let size = buf.get_u64_le();
        let mtime = std::time::SystemTime::decode(buf, features)?;

        Ok(Self { size, mtime })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(16) // u64 + u32 + u32
    }
}

// ============= UTime (utime_t/timespec) =============

/// Ceph utime_t structure (timespec)
///
/// Represents time with second and nanosecond precision.
/// Wire format: u32 tv_sec + u32 tv_nsec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UTime {
    pub tv_sec: u32,
    pub tv_nsec: u32,
}

impl UTime {
    /// Create a new UTime with zero time
    pub const fn zero() -> Self {
        Self {
            tv_sec: 0,
            tv_nsec: 0,
        }
    }

    /// Create a new UTime from seconds and nanoseconds
    pub const fn new(tv_sec: u32, tv_nsec: u32) -> Self {
        Self { tv_sec, tv_nsec }
    }
}

impl Denc for UTime {
    const USES_VERSIONING: bool = false;

    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        buf.put_u32_le(self.tv_sec);
        buf.put_u32_le(self.tv_nsec);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        Ok(Self {
            tv_sec: buf.get_u32_le(),
            tv_nsec: buf.get_u32_le(),
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8) // u32 + u32
    }
}

// ============= Size Constants =============

/// Size of spg_t encoding (with version header)
pub const SPG_T_ENCODED_SIZE: usize = 24; // 6 (header) + 17 (pgid) + 1 (shard)

/// Size of osd_reqid_t encoding (with version header)
pub const OSD_REQID_T_ENCODED_SIZE: usize = 27; // 6 (header) + 9 (name) + 8 (tid) + 4 (inc)

/// Size of blkin_trace_info encoding
pub const BLKIN_TRACE_INFO_SIZE: usize = 24; // 3 x u64

/// Size of jspan_context encoding (with version header, when invalid)
pub const JSPAN_CONTEXT_ENCODED_SIZE: usize = 7; // 6 (header) + 1 (is_valid)

/// Size of ceph_osd_op structure
pub const OSD_OP_ENCODED_SIZE: usize = 38; // 2 (op) + 4 (flags) + 28 (union) + 4 (payload_len)

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_pgid_roundtrip() {
        let pgid = PgId::new(3, 0);
        let mut buf = BytesMut::new();

        pgid.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 17);

        let decoded = PgId::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.pool, 3);
        assert_eq!(decoded.seed, 0);
    }

    #[test]
    fn test_spgid_roundtrip() {
        let spgid = StripedPgId::new(3, 0, -1);
        let mut buf = BytesMut::new();

        spgid.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 24);

        let decoded = StripedPgId::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.pool, 3);
        assert_eq!(decoded.seed, 0);
        assert_eq!(decoded.shard, -1);
    }

    #[test]
    fn test_entity_name_roundtrip() {
        let name = EntityName::new(0x08, 0); // CEPH_ENTITY_TYPE_CLIENT
        let mut buf = BytesMut::new();

        name.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 9);

        let decoded = EntityName::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.entity_type, 0x08);
        assert_eq!(decoded.num, 0);
    }

    #[test]
    fn test_osd_reqid_roundtrip() {
        let reqid = OsdReqId {
            name: EntityName::new(0x08, 0),
            tid: 1,
            inc: 1,
        };
        let mut buf = BytesMut::new();

        reqid.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 27);

        let decoded = OsdReqId::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.name.entity_type, 0x08);
        assert_eq!(decoded.name.num, 0);
        assert_eq!(decoded.tid, 1);
        assert_eq!(decoded.inc, 1);
    }

    #[test]
    fn test_blkin_trace_info_roundtrip() {
        let trace = BlkinTraceInfo::empty();
        let mut buf = BytesMut::new();

        trace.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 24);

        let decoded = BlkinTraceInfo::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.trace_id, 0);
        assert_eq!(decoded.span_id, 0);
        assert_eq!(decoded.parent_span_id, 0);
    }

    #[test]
    fn test_jspan_context_roundtrip() {
        let ctx = JaegerSpanContext::invalid();
        let mut buf = BytesMut::new();

        ctx.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 7);

        let decoded = JaegerSpanContext::decode(&mut buf, 0).unwrap();
        assert!(!decoded.is_valid);
    }

    #[test]
    fn test_size_constants() {
        let spgid = StripedPgId::new(3, 0, -1);
        assert_eq!(spgid.encoded_size(0), Some(SPG_T_ENCODED_SIZE));

        let reqid = OsdReqId {
            name: EntityName::new(0x08, 0),
            tid: 1,
            inc: 1,
        };
        assert_eq!(reqid.encoded_size(0), Some(OSD_REQID_T_ENCODED_SIZE));

        let trace = BlkinTraceInfo::empty();
        assert_eq!(trace.encoded_size(0), Some(BLKIN_TRACE_INFO_SIZE));

        let ctx = JaegerSpanContext::invalid();
        assert_eq!(ctx.encoded_size(0), Some(JSPAN_CONTEXT_ENCODED_SIZE));
    }

    #[test]
    fn test_object_locator_roundtrip() {
        let locator = ObjectLocator {
            pool: 3,
            key: "test_key".to_string(),
            nspace: "test_nspace".to_string(),
            hash: -1,
        };
        let mut buf = BytesMut::new();

        locator.encode(&mut buf, 0).unwrap();

        let decoded = ObjectLocator::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.pool, 3);
        assert_eq!(decoded.key, "test_key");
        assert_eq!(decoded.nspace, "test_nspace");
        assert_eq!(decoded.hash, -1);
    }

    #[test]
    fn test_object_locator_empty() {
        let locator = ObjectLocator::new();
        let mut buf = BytesMut::new();

        locator.encode(&mut buf, 0).unwrap();

        let decoded = ObjectLocator::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.pool, -1);
        assert_eq!(decoded.key, "");
        assert_eq!(decoded.nspace, "");
        assert_eq!(decoded.hash, -1);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_request_redirect_roundtrip() {
        let redirect = RequestRedirect {
            redirect_locator: ObjectLocator {
                pool: 5,
                key: "redirect_key".to_string(),
                nspace: "redirect_nspace".to_string(),
                hash: 42,
            },
            redirect_object: "redirect_obj".to_string(),
        };
        let mut buf = BytesMut::new();

        redirect.encode(&mut buf, 0).unwrap();

        let decoded = RequestRedirect::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.redirect_locator.pool, 5);
        assert_eq!(decoded.redirect_locator.key, "redirect_key");
        assert_eq!(decoded.redirect_locator.nspace, "redirect_nspace");
        assert_eq!(decoded.redirect_locator.hash, 42);
        assert_eq!(decoded.redirect_object, "redirect_obj");
    }

    #[test]
    fn test_request_redirect_empty() {
        let redirect = RequestRedirect::new();
        let mut buf = BytesMut::new();

        redirect.encode(&mut buf, 0).unwrap();

        let decoded = RequestRedirect::decode(&mut buf, 0).unwrap();
        assert!(decoded.is_empty());
        assert!(decoded.redirect_locator.is_empty());
        assert_eq!(decoded.redirect_object, "");
    }

    #[test]
    fn test_osd_op_extent_roundtrip() {
        use bytes::Bytes;

        let op = OSDOp {
            op: OpCode::Read,
            flags: 0,
            op_data: OpData::Extent {
                offset: 0,
                length: 4096,
                truncate_size: 0,
                truncate_seq: 0,
            },
            indata: Bytes::new(),
        };

        let mut buf = BytesMut::new();

        op.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 38);

        let decoded = OSDOp::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.op.as_u16(), OpCode::Read.as_u16());
        assert_eq!(decoded.flags, 0);
        match decoded.op_data {
            OpData::Extent {
                offset,
                length,
                truncate_size,
                truncate_seq,
            } => {
                assert_eq!(offset, 0);
                assert_eq!(length, 4096);
                assert_eq!(truncate_size, 0);
                assert_eq!(truncate_seq, 0);
            }
            _ => panic!("Expected Extent variant"),
        }
    }

    #[test]
    fn test_osd_op_pgls_roundtrip() {
        use bytes::Bytes;

        let op = OSDOp {
            op: OpCode::Pgls,
            flags: 0,
            op_data: OpData::Pgls {
                max_entries: 100,
                start_epoch: 42,
            },
            indata: Bytes::new(),
        };
        let mut buf = BytesMut::new();

        op.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 38);

        let decoded = OSDOp::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.op.as_u16(), OpCode::Pgls.as_u16());
        match decoded.op_data {
            OpData::Pgls {
                max_entries,
                start_epoch,
            } => {
                assert_eq!(max_entries, 100);
                assert_eq!(start_epoch, 42);
            }
            _ => panic!("Expected Pgls variant"),
        }
    }

    #[test]
    fn test_osd_op_xattr_roundtrip() {
        use bytes::Bytes;

        let op = OSDOp {
            op: OpCode::GetXattr,
            flags: 0,
            op_data: OpData::Xattr {
                name_len: 10,
                value_len: 20,
                cmp_op: 1,
                cmp_mode: 2,
            },
            indata: Bytes::new(),
        };
        let mut buf = BytesMut::new();

        op.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 38);

        let decoded = OSDOp::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.op.as_u16(), OpCode::GetXattr.as_u16());
        match decoded.op_data {
            OpData::Xattr {
                name_len,
                value_len,
                cmp_op,
                cmp_mode,
            } => {
                assert_eq!(name_len, 10);
                assert_eq!(value_len, 20);
                assert_eq!(cmp_op, 1);
                assert_eq!(cmp_mode, 2);
            }
            _ => panic!("Expected Xattr variant"),
        }
    }

    #[test]
    fn test_osd_op_with_indata() {
        use bytes::Bytes;

        let write_data = Bytes::from(vec![0x42; 1024]);
        let op = OSDOp {
            op: OpCode::Write,
            flags: 0,
            op_data: OpData::Extent {
                offset: 0,
                length: 1024,
                truncate_size: 0,
                truncate_seq: 0,
            },
            indata: write_data.clone(),
        };
        let mut buf = BytesMut::new();

        op.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 38);

        // The last 4 bytes should be the payload_len
        let payload_len_bytes = &buf[34..38];
        let payload_len = u32::from_le_bytes([
            payload_len_bytes[0],
            payload_len_bytes[1],
            payload_len_bytes[2],
            payload_len_bytes[3],
        ]);
        assert_eq!(payload_len, 1024);
    }

    #[test]
    fn test_osd_op_size_constant() {
        use bytes::Bytes;

        let op = OSDOp {
            op: OpCode::Read,
            flags: 0,
            op_data: OpData::None,
            indata: Bytes::new(),
        };
        assert_eq!(op.encoded_size(0), Some(OSD_OP_ENCODED_SIZE));
    }

    #[test]
    fn test_osd_stat_data_roundtrip() {
        use bytes::BytesMut;
        use std::time::{Duration, UNIX_EPOCH};

        let mtime = UNIX_EPOCH + Duration::new(1234567890, 123456789);
        let original = OsdStatData { size: 12345, mtime };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 16);

        let decoded = OsdStatData::decode(&mut &buf[..], 0).unwrap();
        assert_eq!(decoded.size, original.size);
        assert_eq!(decoded.mtime, original.mtime);
    }

    #[test]
    fn test_utime_roundtrip() {
        use bytes::BytesMut;

        let original = UTime::new(1234567890, 123456789);

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);

        let decoded = UTime::decode(&mut &buf[..], 0).unwrap();
        assert_eq!(decoded.tv_sec, original.tv_sec);
        assert_eq!(decoded.tv_nsec, original.tv_nsec);

        // Test zero
        let zero = UTime::zero();
        assert_eq!(zero.tv_sec, 0);
        assert_eq!(zero.tv_nsec, 0);
    }
}
