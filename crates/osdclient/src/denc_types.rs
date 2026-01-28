//! Denc implementations for OSD message types
//!
//! This module provides Denc trait implementations for structures used in
//! MOSDOp encoding, following Ceph's encoding patterns.

use bytes::{Buf, BufMut};
use denc::denc::{Denc, VersionedEncode};
use denc::error::RadosError;

use crate::types::{
    BlkinTraceInfo, EntityName, JaegerSpanContext, ObjectLocator, PgId, RequestRedirect,
    StripedPgId,
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
        buf.put_u8(1); // version
        buf.put_i64_le(self.pool);
        buf.put_u32_le(self.seed);
        buf.put_i32_le(-1); // preferred (deprecated, always -1)
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 17 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for pg_t".to_string(),
            ));
        }

        let version = buf.get_u8();
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unknown pg_t version: {}",
                version
            )));
        }

        let pool = buf.get_i64_le();
        let seed = buf.get_u32_le();
        let _preferred = buf.get_i32_le(); // deprecated

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
        buf.put_i8(self.shard);

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
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for spg_t shard".to_string(),
            ));
        }
        let shard = buf.get_i8();

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
        buf.put_u8(self.entity_type);
        buf.put_u64_le(self.num);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 9 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for entity_name_t".to_string(),
            ));
        }

        let entity_type = buf.get_u8();
        let num = buf.get_u64_le();

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
        buf.put_u64_le(self.tid);

        // Encode inc (i32)
        buf.put_i32_le(self.inc);

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
        if buf.remaining() < 12 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for osd_reqid_t tid and inc".to_string(),
            ));
        }
        let tid = buf.get_u64_le();

        // Decode inc (i32)
        let inc = buf.get_i32_le();

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
        buf.put_u64_le(self.trace_id);
        buf.put_u64_le(self.span_id);
        buf.put_u64_le(self.parent_span_id);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 24 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for blkin_trace_info".to_string(),
            ));
        }

        let trace_id = buf.get_u64_le();
        let span_id = buf.get_u64_le();
        let parent_span_id = buf.get_u64_le();

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
        buf.put_u8(if self.is_valid { 1 } else { 0 });
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        _features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for jspan_context".to_string(),
            ));
        }

        let is_valid = buf.get_u8() != 0;

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
        buf.put_i64_le(self.pool);

        // Encode preferred (i32, always -1, deprecated field)
        buf.put_i32_le(-1);

        // Encode key (String)
        self.key.encode(buf, features)?;

        // Encode nspace (String)
        self.nspace.encode(buf, features)?;

        // Encode hash (i64)
        buf.put_i64_le(self.hash);

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Decode pool (i64)
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for object_locator_t pool".to_string(),
            ));
        }
        let pool = buf.get_i64_le();

        // Decode preferred (i32, deprecated)
        if buf.remaining() < 4 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for object_locator_t preferred".to_string(),
            ));
        }
        let _preferred = buf.get_i32_le();

        // Decode key (String)
        let key = String::decode(buf, features)?;

        // Decode nspace (String)
        let nspace = String::decode(buf, features)?;

        // Decode hash (i64)
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for object_locator_t hash".to_string(),
            ));
        }
        let hash = buf.get_i64_le();

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
        buf.put_u32_le(0);

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
        if buf.remaining() < 4 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for request_redirect_t legacy field".to_string(),
            ));
        }
        let _legacy = buf.get_u32_le();

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

// ============= Size Constants =============

/// Size of spg_t encoding (with version header)
pub const SPG_T_ENCODED_SIZE: usize = 24; // 6 (header) + 17 (pgid) + 1 (shard)

/// Size of osd_reqid_t encoding (with version header)
pub const OSD_REQID_T_ENCODED_SIZE: usize = 27; // 6 (header) + 9 (name) + 8 (tid) + 4 (inc)

/// Size of blkin_trace_info encoding
pub const BLKIN_TRACE_INFO_SIZE: usize = 24; // 3 x u64

/// Size of jspan_context encoding (with version header, when invalid)
pub const JSPAN_CONTEXT_ENCODED_SIZE: usize = 7; // 6 (header) + 1 (is_valid)

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
        assert_eq!(decoded.is_valid, false);
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
}
