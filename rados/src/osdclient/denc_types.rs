//! Denc implementations for OSD message types
//!
//! This module provides Denc trait implementations for structures used in
//! MOSDOp encoding, following Ceph's encoding patterns.

use crate::denc::error::RadosError;
use crate::denc::{Denc, VersionedEncode};
use bytes::{Buf, BufMut};

use crate::osdclient::osdmap::PgId;
use crate::osdclient::types::{
    OSDOp, ObjectLocator, OpCode, OpData, PackedEntityName, RequestRedirect, StripedPgId,
};

#[cfg(test)]
use crate::osdclient::types::{BlkinTraceInfo, JaegerSpanContext};

// PgId Denc implementation is now in the rados-denc crate (osdmap.rs)
// We just re-export and use it here

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

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        // pg_t + shard
        let pgid = PgId {
            pool: self.pool,
            seed: self.seed,
        };
        Some(pgid.encoded_size(features)? + self.shard.encoded_size(0)?)
    }
}

crate::denc::impl_denc_for_versioned!(StripedPgId);

// ============= OsdReqId (osd_reqid_t) =============

/// OSD request ID with versioned encoding
///
/// This structure uses a special versioning scheme (DENC_START_OSD_REQID)
/// that enforces version 2 and compat 2.
#[derive(Debug, Clone, PartialEq, Eq, crate::VersionedDenc)]
#[denc(crate = "crate", version = 2, compat = 2)]
pub struct OsdReqId {
    pub name: PackedEntityName,
    pub tid: u64,
    pub inc: i32,
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

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        // redirect_locator + redirect_object + legacy
        Some(
            self.redirect_locator.encoded_size(features)?
                + self.redirect_object.encoded_size(features)?
                + 0u32.encoded_size(0)?, // legacy (always 0)
        )
    }
}

crate::denc::impl_denc_for_versioned!(RequestRedirect);

// ============= OSDOp (ceph_osd_op) =============

use crate::osdclient::messages::{CEPH_OSD_OP_SIZE, CEPH_OSD_OP_UNION_SIZE};

// Compile-time verification matching rados.h static_assert
const _: () = assert!(CEPH_OSD_OP_UNION_SIZE == 28);
const _: () = assert!(CEPH_OSD_OP_SIZE == 38);

/// Denc implementation for OSDOp
///
/// This implements encoding/decoding for the CEPH_OSD_OP_SIZE-byte ceph_osd_op structure:
/// - op (u16) - 2 bytes
/// - flags (u32) - 4 bytes
/// - union (CEPH_OSD_OP_UNION_SIZE bytes) - OpData variant based on op
/// - payload_len (u32) - 4 bytes
///
/// Note: The actual payload data (indata) is stored separately in the message
/// data section and is not part of this structure.
impl Denc for OSDOp {
    const USES_VERSIONING: bool = false;

    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // 1. op (u16)
        buf.put_u16_le(self.op.as_u16());

        // 2. flags (u32)
        buf.put_u32_le(self.flags);

        // 3. Union (CEPH_OSD_OP_UNION_SIZE bytes) - OpData variant based on op
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
                // 8 + 8 + 8 + 4 = CEPH_OSD_OP_UNION_SIZE ✓
            }
            OpData::Pgls {
                max_entries,
                start_epoch,
            } => {
                buf.put_u64_le(*max_entries);
                buf.put_u32_le(*start_epoch);
                // Pad to CEPH_OSD_OP_UNION_SIZE: 8 + 4 = 12, need 16 more
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
                // Pad to CEPH_OSD_OP_UNION_SIZE: 4 + 4 + 1 + 1 = 10, need 18 more
                buf.put_u64_le(0);
                buf.put_u64_le(0);
                buf.put_u16_le(0);
            }
            OpData::Call {
                class_len,
                method_len,
                indata_len,
            } => {
                // C++ rados.h cls union: u8 class_len + u8 method_len + u8 argc + u32 indata_len
                buf.put_u8(*class_len);
                buf.put_u8(*method_len);
                buf.put_u8(0); // argc (always 0)
                buf.put_u32_le(*indata_len);
                // Pad to CEPH_OSD_OP_UNION_SIZE: 1 + 1 + 1 + 4 = 7, need 21 more
                buf.put_u64_le(0);
                buf.put_u64_le(0);
                buf.put_u32_le(0);
                buf.put_u8(0);
            }
            OpData::Snap { snapid } => {
                // ceph_osd_op.snap.snapid (u64) + 20 bytes padding = CEPH_OSD_OP_UNION_SIZE
                buf.put_u64_le(*snapid);
                buf.put_u64_le(0);
                buf.put_u64_le(0);
                buf.put_u32_le(0);
            }
            OpData::AssertVer { ver } => {
                // assert_ver union: u64 unused + u64 ver + 12 bytes padding = 28 bytes
                buf.put_u64_le(0); // unused
                buf.put_u64_le(*ver);
                buf.put_u64_le(0); // padding
                buf.put_u32_le(0); // padding
            }
            OpData::None => {
                // Empty union - CEPH_OSD_OP_UNION_SIZE bytes of zeros
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

        // 3. Union (CEPH_OSD_OP_UNION_SIZE bytes) - decode based on op
        let op_data = match op {
            OpCode::Read
            | OpCode::Write
            | OpCode::WriteFull
            | OpCode::Truncate
            | OpCode::Append
            | OpCode::Stat => {
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
            OpCode::Rollback => {
                // snap.snapid (u64) + 20 bytes padding = CEPH_OSD_OP_UNION_SIZE
                let snapid = buf.get_u64_le();
                buf.advance(20);
                OpData::Snap { snapid }
            }
            OpCode::AssertVer => {
                // assert_ver: u64 unused + u64 ver + 12 bytes padding
                let _unused = buf.get_u64_le();
                let ver = buf.get_u64_le();
                buf.advance(12);
                OpData::AssertVer { ver }
            }
            _ => {
                // Other operations (including ListSnaps) - skip CEPH_OSD_OP_UNION_SIZE bytes
                buf.advance(CEPH_OSD_OP_UNION_SIZE);
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
        // Fixed size: CEPH_OSD_OP_SIZE bytes
        Some(
            std::mem::size_of::<u16>() // op
                + std::mem::size_of::<u32>() // flags
                + CEPH_OSD_OP_UNION_SIZE // union (fixed size regardless of variant)
                + std::mem::size_of::<u32>(), // payload_len
        )
    }
}

// ============= OsdStatData =============

/// Result data from CEPH_OSD_OP_STAT operation
///
/// This contains the object size and modification time returned by the OSD.
/// Format: u64 size + SystemTime (u32 sec + u32 nsec as utime_t)
#[derive(Debug, Clone, Copy, PartialEq, Eq, crate::Denc)]
#[denc(crate = "crate")]
pub struct OsdStatData {
    pub size: u64,
    pub mtime: std::time::SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UTime;
    use bytes::BytesMut;

    #[test]
    fn test_pgid_roundtrip() {
        let pgid = PgId { pool: 3, seed: 0 };
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
        let name = PackedEntityName::new(0x08, 0); // CEPH_ENTITY_TYPE_CLIENT
        let mut buf = BytesMut::new();

        name.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 9);

        let decoded = PackedEntityName::decode(&mut buf, 0).unwrap();
        // Copy values to avoid taking references to packed struct fields
        let entity_type = decoded.entity_type;
        let num = decoded.num;
        assert_eq!(entity_type, 0x08);
        assert_eq!(num, 0);
    }

    #[test]
    fn test_osd_reqid_roundtrip() {
        let reqid = OsdReqId {
            name: PackedEntityName::new(0x08, 0),
            tid: 1,
            inc: 1,
        };
        let mut buf = BytesMut::new();

        reqid.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 27);

        let decoded = OsdReqId::decode(&mut buf, 0).unwrap();
        // Copy EntityName to avoid taking references to packed struct fields
        let name = decoded.name;
        let entity_type = name.entity_type;
        let num = name.num;
        assert_eq!(entity_type, 0x08);
        assert_eq!(num, 0);
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
        // Copy values to avoid taking references to packed struct fields
        let trace_id = decoded.trace_id;
        let span_id = decoded.span_id;
        let parent_span_id = decoded.parent_span_id;
        assert_eq!(trace_id, 0);
        assert_eq!(span_id, 0);
        assert_eq!(parent_span_id, 0);
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
        assert_eq!(spgid.encoded_size(0), Some(24)); // 6 (header) + 17 (pgid) + 1 (shard)

        let reqid = OsdReqId {
            name: PackedEntityName::new(0x08, 0),
            tid: 1,
            inc: 1,
        };
        assert_eq!(reqid.encoded_size(0), Some(27)); // 6 (header) + 9 (name) + 8 (tid) + 4 (inc)

        let trace = BlkinTraceInfo::empty();
        assert_eq!(trace.encoded_size(0), Some(24)); // 3 x u64

        let ctx = JaegerSpanContext::invalid();
        assert_eq!(ctx.encoded_size(0), Some(7)); // 6 (header) + 1 (is_valid)
    }

    #[test]
    fn test_object_locator_roundtrip() {
        let locator = ObjectLocator {
            pool_id: 3,
            key: "test_key".to_string(),
            namespace: "test_nspace".to_string(),
            hash: -1,
        };
        let mut buf = BytesMut::new();

        locator.encode(&mut buf, 0).unwrap();

        let decoded = ObjectLocator::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.pool_id, 3);
        assert_eq!(decoded.key, "test_key");
        assert_eq!(decoded.namespace, "test_nspace");
        assert_eq!(decoded.hash, -1);
    }

    #[test]
    fn test_object_locator_empty() {
        let locator = ObjectLocator::default();
        let mut buf = BytesMut::new();

        locator.encode(&mut buf, 0).unwrap();

        let decoded = ObjectLocator::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.pool_id, u64::MAX);
        assert_eq!(decoded.key, "");
        assert_eq!(decoded.namespace, "");
        assert_eq!(decoded.hash, -1);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_request_redirect_roundtrip() {
        let redirect = RequestRedirect {
            redirect_locator: ObjectLocator {
                pool_id: 5,
                key: String::new(),
                namespace: "redirect_nspace".to_string(),
                hash: 42,
            },
            redirect_object: "redirect_obj".to_string(),
        };
        let mut buf = BytesMut::new();

        redirect.encode(&mut buf, 0).unwrap();

        let decoded = RequestRedirect::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.redirect_locator.pool_id, 5);
        assert_eq!(decoded.redirect_locator.key, "");
        assert_eq!(decoded.redirect_locator.namespace, "redirect_nspace");
        assert_eq!(decoded.redirect_locator.hash, 42);
        assert_eq!(decoded.redirect_object, "redirect_obj");
    }

    #[test]
    fn test_request_redirect_empty() {
        let redirect = RequestRedirect::default();
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
        assert_eq!(op.encoded_size(0), Some(CEPH_OSD_OP_SIZE));
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
        // Copy values to avoid taking references to packed struct fields
        let decoded_sec = decoded.sec;
        let decoded_nsec = decoded.nsec;
        let original_sec = original.sec;
        let original_nsec = original.nsec;
        assert_eq!(decoded_sec, original_sec);
        assert_eq!(decoded_nsec, original_nsec);

        // Test zero
        let zero = UTime::zero();
        let zero_sec = zero.sec;
        let zero_nsec = zero.nsec;
        assert_eq!(zero_sec, 0);
        assert_eq!(zero_nsec, 0);
    }
}
