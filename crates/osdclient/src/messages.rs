//! Message encoding and decoding for OSD operations
//!
//! This module implements encoding/decoding for MOSDOp and MOSDOpReply messages.

use crate::osdmap::PgId;
use crate::types::{OSDOp, ObjectId, OpReply, OpResult, RequestId, RequestRedirect, StripedPgId};
use denc::UTime;

#[cfg(test)]
use crate::types::OpData;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
use msgr2::impl_denc_ceph_message;
use tracing::debug;

/// Message type for MOSDOp (Client to OSD)
pub const CEPH_MSG_OSD_OP: u16 = 42;

/// Message type for MOSDOpReply (OSD to Client)
pub const CEPH_MSG_OSD_OPREPLY: u16 = 43;

/// Message type for MOSDBackoff (OSD to Client)
pub const CEPH_MSG_OSD_BACKOFF: u16 = 61;

/// Backoff operation codes
pub const CEPH_OSD_BACKOFF_OP_BLOCK: u8 = 1;
pub const CEPH_OSD_BACKOFF_OP_ACK_BLOCK: u8 = 2;
pub const CEPH_OSD_BACKOFF_OP_UNBLOCK: u8 = 3;

/// Message priority levels (from include/msgr.h)
/// Priority is stored in the message header, not the message payload
pub const CEPH_MSG_PRIO_LOW: i32 = 64;
pub const CEPH_MSG_PRIO_DEFAULT: i32 = 127;
pub const CEPH_MSG_PRIO_HIGH: i32 = 196;
pub const CEPH_MSG_PRIO_HIGHEST: i32 = 255;

/// Size of the ceph_osd_op union field (largest variant: extent = 3×u64 + u32)
pub const CEPH_OSD_OP_UNION_SIZE: usize =
    3 * std::mem::size_of::<u64>() + std::mem::size_of::<u32>();
/// Size of ceph_osd_op on the wire: op(u16) + flags(u32) + union + payload_len(u32)
/// Verified by static_assert in rados.h: (2+4+(2*8+8+4)+4) = 38
pub const CEPH_OSD_OP_SIZE: usize = std::mem::size_of::<u16>()
    + std::mem::size_of::<u32>()
    + CEPH_OSD_OP_UNION_SIZE
    + std::mem::size_of::<u32>();
/// Size of BlkinTraceInfo on the wire: 3 × u64
const BLKIN_TRACE_INFO_SIZE: usize = std::mem::size_of::<crate::types::BlkinTraceInfo>();
/// Sentinel value meaning "calculate hash from object name"
/// Reference: linux/net/ceph/osd_client.c encode_request_partial()
pub(crate) const HASH_CALCULATE_FROM_NAME: i64 = -1;

/// MOSDOp message - Client to OSD (message type 42)
#[derive(Debug, Clone)]
pub struct MOSDOp {
    pub client_inc: u32,
    pub osdmap_epoch: u32,
    pub flags: u32,
    pub mtime: UTime,
    pub retry_attempt: i32,
    pub object: ObjectId,
    pub pgid: StripedPgId,
    pub ops: Vec<OSDOp>,
    pub snapid: u64,
    pub snap_seq: u64,
    pub snaps: Vec<u64>,
    pub reqid: RequestId,
    /// Global ID from monitor authentication (used in entity_name)
    pub global_id: u64,
}

impl MOSDOp {
    /// Message version (from MOSDOp.h HEAD_VERSION)
    pub const VERSION: u16 = 9;

    /// Message compat version (from MOSDOp.h COMPAT_VERSION)
    pub const COMPAT_VERSION: u16 = 3;

    /// Create a new MOSDOp message
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_inc: u32,
        osdmap_epoch: u32,
        flags: u32,
        object: ObjectId,
        pgid: StripedPgId,
        ops: Vec<OSDOp>,
        reqid: RequestId,
        global_id: u64,
    ) -> Self {
        Self {
            client_inc,
            osdmap_epoch,
            flags,
            mtime: UTime::zero(),
            retry_attempt: -1,
            object,
            pgid,
            ops,
            snapid: crate::types::SNAP_HEAD,
            snap_seq: 0,
            snaps: Vec::new(),
            reqid,
            global_id,
        }
    }

    /// Calculate appropriate flags for the operations
    ///
    /// This determines the READ/WRITE flags based on the operation types,
    /// matching the behavior of the Linux kernel and librados.
    pub fn calculate_flags(ops: &[OSDOp]) -> u32 {
        use crate::types::OsdOpFlags;

        let mut flags = OsdOpFlags::ACK; // Always want acknowledgment

        // Scan ops once to determine read/write/pgop flags
        let (has_read, has_write, has_pgop) =
            ops.iter().fold((false, false, false), |(r, w, pg), op| {
                (
                    r || op.op.is_read(),
                    w || op.op.is_write(),
                    pg || op.op.is_pg_op(),
                )
            });
        if has_read {
            flags |= OsdOpFlags::READ;
        }
        if has_write {
            flags |= OsdOpFlags::WRITE;
        }
        if has_pgop {
            flags |= OsdOpFlags::PGOP;
        }

        // Add compatibility flags required by OSDs
        // ONDISK: Required for compatibility with pre-luminous OSDs
        flags |= OsdOpFlags::ONDISK;
        // KNOWN_REDIR: Indicates client understands redirects
        flags |= OsdOpFlags::KNOWN_REDIR;
        // SUPPORTSPOOLEIO: Indicates client understands pool EIO behavior
        flags |= OsdOpFlags::SUPPORTSPOOLEIO;

        flags.bits()
    }

    /// Get the expected front section size for a PGLS operation
    ///
    /// This is useful for verifying the encoding is correct.
    /// - v8: 209 bytes (without OpenTelemetry trace)
    /// - v9: 216 bytes (with OpenTelemetry trace, 7 bytes for JaegerSpanContext)
    ///
    /// Returns `None` for unsupported versions.
    pub fn expected_front_size_pgls(version: u16) -> Option<usize> {
        match version {
            8 => Some(209), // v8 format (Ceph v18)
            9 => Some(216), // v9 format (Ceph v19+)
            _ => None,
        }
    }
}

/// MOSDOpReply message - OSD to Client (message type 43)
#[derive(Debug, Clone)]
pub struct MOSDOpReply {
    pub object: ObjectId,
    pub pgid: StripedPgId,
    pub flags: u32,
    pub result: i32,
    pub epoch: u32,
    pub version: u64,
    pub user_version: u64,
    pub retry_attempt: i32,
    pub redirect: Option<RequestRedirect>,
    pub ops: Vec<OpReply>,
}

impl MOSDOpReply {
    /// Message version (from MOSDOpReply.h HEAD_VERSION)
    pub const VERSION: u16 = 8;
    /// Compatibility version (from MOSDOpReply.h COMPAT_VERSION)
    pub const COMPAT_VERSION: u16 = 2;

    /// Convert to OpResult
    pub fn to_op_result(self) -> OpResult {
        OpResult {
            result: self.result,
            // Use user_version as the primary version - this is the object version
            // that clients should see. The replay_version is used internally by OSDs.
            version: self.user_version,
            user_version: self.user_version,
            ops: self.ops,
            redirect: self.redirect,
        }
    }
}

/// MOSDBackoff message - OSD to Client (message type 61)
///
/// Used by OSDs to request clients pause operations on specific object ranges
/// during recovery, backfill, or when the OSD is overloaded.
///
/// Reference: ~/dev/ceph/src/messages/MOSDBackoff.h
#[derive(Debug, Clone, denc::Denc)]
pub struct MOSDBackoff {
    /// Placement group ID
    pub pgid: StripedPgId,
    /// OSDMap epoch
    pub map_epoch: u32,
    /// Operation code (CEPH_OSD_BACKOFF_OP_*)
    pub op: u8,
    /// Unique backoff ID within this session
    pub id: u64,
    /// Start of object range (inclusive)
    pub begin: denc::HObject,
    /// End of object range (exclusive)
    /// If begin == end, blocks a single object
    pub end: denc::HObject,
}

impl MOSDBackoff {
    /// Message version (from MOSDBackoff.h HEAD_VERSION)
    pub const VERSION: u16 = 1;

    /// Message compat version (from MOSDBackoff.h COMPAT_VERSION)
    pub const COMPAT_VERSION: u16 = 1;

    /// Create a new MOSDBackoff message
    pub fn new(
        pgid: StripedPgId,
        map_epoch: u32,
        op: u8,
        id: u64,
        begin: denc::HObject,
        end: denc::HObject,
    ) -> Self {
        Self {
            pgid,
            map_epoch,
            op,
            id,
            begin,
            end,
        }
    }
}

// ============================================================================
// CephMessagePayload trait implementations
// ============================================================================

impl CephMessagePayload for MOSDOp {
    fn msg_type() -> u16 {
        CEPH_MSG_OSD_OP
    }

    fn msg_version(features: u64) -> u16 {
        // Return v9 if SERVER_SQUID feature is present (Ceph v19+)
        // Return v8 otherwise for backward compatibility with Ceph v18
        use denc::features::CephFeatures;
        if features & CephFeatures::MASK_SERVER_SQUID.bits() != 0 {
            9
        } else {
            8
        }
    }

    fn msg_compat_version(_features: u64) -> u16 {
        Self::COMPAT_VERSION
    }

    fn encode_payload(&self, features: u64) -> std::result::Result<Bytes, msgr2::Msgr2Error> {
        use crate::denc_types::OsdReqId;
        use crate::types::{
            BlkinTraceInfo, CEPH_ENTITY_TYPE_CLIENT, JaegerSpanContext, PackedEntityName,
        };
        use denc::Denc;

        // Pre-allocate buffer with estimated size to avoid reallocations
        // Estimate: fixed fields + ops + variable data
        let estimated_size = 256 + (self.ops.len() * 64);
        let mut buf = BytesMut::with_capacity(estimated_size);

        // Debug logging for MOSDOp message

        // 1. spgid (spg_t) - with version header (1,1)
        self.pgid.encode(&mut buf, 0)?;

        // 2. hash (raw pg hash)
        self.object.hash.encode(&mut buf, 0)?;

        // 3. osdmap_epoch
        self.osdmap_epoch.encode(&mut buf, 0)?;

        // 4. flags
        self.flags.encode(&mut buf, 0)?;

        // 5. reqid (osd_reqid_t) - with version header (2,2)
        let entity_name = PackedEntityName::new(CEPH_ENTITY_TYPE_CLIENT, self.global_id);
        let reqid = OsdReqId {
            name: entity_name,
            tid: self.reqid.tid,
            inc: self.reqid.inc,
        };
        reqid.encode(&mut buf, 0)?;

        // 6. trace (blkin_trace_info) - 3 x u64 = 24 bytes
        let trace = BlkinTraceInfo::empty();
        trace.encode(&mut buf, 0)?;

        // 6b. otel_trace (jspan_context) - added in v9, only encode if SERVER_SQUID feature is present
        use denc::features::CephFeatures;
        if features & CephFeatures::MASK_SERVER_SQUID.bits() != 0 {
            let otel_trace = JaegerSpanContext::invalid();
            otel_trace.encode(&mut buf, 0)?;
        }

        // --- Above decoded up front; below decoded post-dispatch ---

        // 7. client_inc
        self.client_inc.encode(&mut buf, 0)?;

        // 8. mtime (utime_t: timespec with sec as u32, nsec as u32)
        self.mtime.encode(&mut buf, 0)?;

        // 9. object_locator_t (using Denc encoding)
        // Note: hash=-1 means "calculate from object name" which is the normal case
        let locator = crush::ObjectLocator {
            pool_id: self.object.pool,
            key: self.object.key.clone(),
            namespace: self.object.namespace.clone(),
            hash: HASH_CALCULATE_FROM_NAME,
        };

        locator.encode(&mut buf, 0)?;

        // 10. object name (object_t)
        self.object.oid.encode(&mut buf, 0)?;

        // 11. operations
        (self.ops.len() as u16).encode(&mut buf, 0)?;
        for op in &self.ops {
            op.encode(&mut buf, 0)?;
        }

        // 12. snapid
        self.snapid.encode(&mut buf, 0)?;

        // 13. snap_seq
        self.snap_seq.encode(&mut buf, 0)?;

        // 14. snaps vector
        self.snaps.encode(&mut buf, 0)?;

        // 15. retry_attempt
        self.retry_attempt.encode(&mut buf, 0)?;

        // 16. features (encode actual connection features)
        features.encode(&mut buf, 0)?;

        Ok(buf.freeze())
    }

    fn encode_data(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Msgr2Error> {
        // Collect all indata from operations into a single buffer for the message data section.
        // This follows the Ceph pattern of OSDOp::merge_osd_op_vector_in_data()

        // Pre-calculate total size to avoid reallocations
        let total_size: usize = self.ops.iter().map(|op| op.indata.len()).sum();
        let mut buf = BytesMut::with_capacity(total_size);

        for op in &self.ops {
            if !op.indata.is_empty() {
                buf.put_slice(&op.indata);
            }
        }
        Ok(buf.freeze())
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        data: &[u8],
    ) -> std::result::Result<Self, msgr2::Msgr2Error> {
        // MOSDOp decoding not implemented yet - this is typically only needed on the server side
        let _ = (front, data);
        Err(msgr2::Msgr2Error::Deserialization(
            "MOSDOp decoding not implemented".into(),
        ))
    }
}

impl CephMessagePayload for MOSDOpReply {
    fn msg_type() -> u16 {
        CEPH_MSG_OSD_OPREPLY
    }

    fn msg_version(_features: u64) -> u16 {
        Self::VERSION
    }

    fn msg_compat_version(_features: u64) -> u16 {
        Self::COMPAT_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Msgr2Error> {
        // MOSDOpReply encoding not implemented yet - this is typically only needed on the server side
        Err(msgr2::Msgr2Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        data: &[u8],
    ) -> std::result::Result<Self, msgr2::Msgr2Error> {
        use denc::Denc;

        let mut cursor = front;
        // According to MOSDOpReply.h line 200, the encoding is:
        // encode(oid, payload);
        // encode(pgid, payload);
        // encode(flags, payload);
        // encode(result, payload);
        // encode(bad_replay_version, payload);
        // encode(osdmap_epoch, payload);
        // encode(num_ops, payload);
        // for each op: encode(ops[i].op, payload);
        // encode(retry_attempt, payload);
        // for each op: encode(ops[i].rval, payload);
        // encode(replay_version, payload);
        // encode(user_version, payload);
        // encode(do_redirect, payload);
        // if do_redirect: encode(redirect, payload);
        // encode_trace(payload, features);

        // 1. oid (object_t) - just the name as a string
        let oid = String::decode(&mut cursor, 0)?;

        // 2. pgid (pg_t) - use Denc infrastructure
        let pgid_raw = PgId::decode(&mut cursor, 0)?;

        let pgid = StripedPgId {
            pool: pgid_raw.pool,
            seed: pgid_raw.seed,
            shard: -1, // Not in pg_t, only in spg_t
        };

        // 3. flags (int64_t)
        let flags = i64::decode(&mut cursor, 0)? as u32;

        // 4. result (errorcode32_t = int32_t)
        let result = i32::decode(&mut cursor, 0)?;

        // 5. bad_replay_version (eversion_t = epoch + version)
        // This is for backwards compatibility with old clients.
        // Modern clients should use replay_version (our 'version' field) and user_version instead.
        // See: ~/dev/ceph/src/messages/MOSDOpReply.h set_reply_versions()
        let _bad_replay_epoch = u32::decode(&mut cursor, 0)?;
        let _bad_replay_version = u64::decode(&mut cursor, 0)?;

        // 6. osdmap_epoch (epoch_t = u32)
        let epoch = u32::decode(&mut cursor, 0)?;

        // 7. num_ops (u32)
        let num_ops = u32::decode(&mut cursor, 0)? as usize;

        // 8. For each op: osd_op structure
        // osd_op is defined in rados.h and has a fixed size
        // struct ceph_osd_op {
        //   __le16 op;           /* CEPH_OSD_OP_* */
        //   __le32 flags;        /* CEPH_OSD_OP_FLAG_* */
        //   union {
        //     ... various 28-byte unions ...
        //   } __attribute__ ((packed));
        //   __le32 payload_len;
        // } __attribute__ ((packed));
        // Total size: 2 + 4 + 28 + 4 = 38 bytes
        // Verified by static_assert in rados.h: (2+4+(2*8+8+4)+4) = 38

        // Parse osd_op structures to get payload lengths
        let mut payload_lens = Vec::with_capacity(num_ops);
        for i in 0..num_ops {
            if cursor.remaining() < CEPH_OSD_OP_SIZE {
                return Err(msgr2::Msgr2Error::Deserialization(format!(
                    "Incomplete osd_op {}: need {} bytes, have {}",
                    i,
                    CEPH_OSD_OP_SIZE,
                    cursor.remaining()
                )));
            }
            // Skip to payload_len field (at offset 34)
            cursor.advance(34);
            let payload_len = u32::decode(&mut cursor, 0)?;

            payload_lens.push(payload_len as usize);
        }

        // 9. retry_attempt (int32_t)
        // Used to validate that the reply matches the request attempt
        // See: ~/dev/linux/net/ceph/osd_client.c handle_reply()
        let retry_attempt = i32::decode(&mut cursor, 0)?;

        // 10. For each op: rval (int32_t)
        let mut ops = Vec::with_capacity(num_ops);
        for _i in 0..num_ops {
            let return_code = i32::decode(&mut cursor, 0)?;

            ops.push(OpReply {
                return_code,
                outdata: Bytes::new(), // Will be filled from data section below
            });
        }

        // 11. replay_version (eversion_t = epoch + version)
        // The epoch part is not currently used since we track OSDMap epoch separately
        let _replay_epoch = u32::decode(&mut cursor, 0)?;
        let version = u64::decode(&mut cursor, 0)?;

        // 12. user_version (version_t = u64)
        let user_version = u64::decode(&mut cursor, 0)?;

        // 13. do_redirect (bool)
        let do_redirect = u8::decode(&mut cursor, 0)? != 0;

        // 14. If do_redirect: redirect structure (request_redirect_t)
        let redirect = if do_redirect {
            let r = RequestRedirect::decode(&mut cursor, 0)?;
            debug!(
                "Received redirect: pool={}, key={}, namespace={}, object={}",
                r.redirect_locator.pool_id,
                r.redirect_locator.key,
                r.redirect_locator.namespace,
                r.redirect_object
            );
            Some(r)
        } else {
            None
        };

        // 15. trace (blkin_trace_info: 3 x u64)
        // The trace is used for distributed tracing (Zipkin/Jaeger)
        // These fields could be exposed in the future for observability/debugging
        // See: ~/dev/ceph/src/include/encoding.h encode(blkin_trace_info)
        if cursor.remaining() >= BLKIN_TRACE_INFO_SIZE {
            let _trace = crate::types::BlkinTraceInfo::decode(&mut cursor, 0)?;
        }

        // 16. Distribute data section to operations
        // The data section contains concatenated output data for all operations

        let mut data_offset = 0;
        for (i, op) in ops.iter_mut().enumerate() {
            let len = payload_lens[i];
            if len > 0 {
                if data_offset + len > data.len() {
                    return Err(msgr2::Msgr2Error::Deserialization(format!(
                        "Insufficient data for op {}: need {} bytes at offset {}, have {} total",
                        i,
                        len,
                        data_offset,
                        data.len()
                    )));
                }
                op.outdata = Bytes::copy_from_slice(&data[data_offset..data_offset + len]);
                data_offset += len;
            }
        }

        let object = ObjectId {
            pool: pgid_raw.pool,
            oid,
            snap: 0,
            hash: 0,
            namespace: String::new(),
            key: String::new(),
        };

        Ok(Self {
            object,
            pgid,
            flags,
            result,
            epoch,
            version,
            user_version,
            retry_attempt,
            redirect,
            ops,
        })
    }
}

impl_denc_ceph_message!(MOSDBackoff, CEPH_MSG_OSD_BACKOFF, MOSDBackoff::VERSION);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mosdop_encoding_v9() {
        use crate::types::{OSDOp, ObjectId, RequestId, StripedPgId};
        use msgr2::ceph_message::{CephMessage, CrcFlags};

        // Create a PGLS operation
        let object = ObjectId::new(3, "");
        let pgid = StripedPgId::from_pg(3, 0);
        let ops = vec![OSDOp::pgls(100, denc::HObject::empty_cursor(3), 20).unwrap()];
        let reqid = RequestId::new("client.0", 1, 1);

        let mosdop = MOSDOp::new(
            1,
            20,
            MOSDOp::calculate_flags(&ops),
            object,
            pgid,
            ops,
            reqid,
            0,
        );

        // Encode using CephMessage framework with features=0 (v8 format)
        let msg = CephMessage::from_payload(&mosdop, 0, CrcFlags::ALL).unwrap();

        // Verify front section size
        // Expected: 209 bytes for v8 (without OpenTelemetry trace)

        assert_eq!(
            msg.front.len(),
            MOSDOp::expected_front_size_pgls(8).unwrap(),
            "Front section should be 209 bytes for v8"
        );

        // Verify data section (should contain the 39-byte HObject cursor)

        assert_eq!(
            msg.data.len(),
            39,
            "Data section should contain 39-byte HObject cursor"
        );
    }

    #[test]
    fn test_mosdop_message_encoding() {
        use msgr2::ceph_message::{CephMessage, CrcFlags};

        let object = ObjectId {
            pool: 1,
            oid: "test_object".to_string(),
            snap: 0,
            hash: 0x12345678,
            namespace: String::new(),
            key: String::new(),
        };

        let pgid = StripedPgId {
            pool: 1,
            seed: 0x12345678,
            shard: -1,
        };

        let ops = vec![OSDOp {
            op: crate::types::OpCode::Read,
            flags: 0,
            op_data: OpData::Extent {
                offset: 0,
                length: 4096,
                truncate_size: 0,
                truncate_seq: 0,
            },
            indata: Bytes::new(),
        }];

        let reqid = RequestId {
            entity_name: "client.admin".to_string(),
            tid: 1,
            inc: 1,
        };

        let mosdop = MOSDOp::new(1, 1, 0, object, pgid, ops, reqid, 0);

        // Create a complete message using CephMessage framework with features=0 (v8)
        let msg = CephMessage::from_payload(&mosdop, 0, CrcFlags::ALL).unwrap();

        // Verify message structure
        // Copy packed struct fields to local variables to avoid unaligned references
        let msg_type = msg.header.msg_type;
        let version = msg.header.version;
        assert_eq!(msg_type, CEPH_MSG_OSD_OP);
        assert_eq!(version, 8, "Version should be 8 with features=0");
        assert!(!msg.front.is_empty());
        assert_eq!(msg.middle.len(), 0);
        assert_eq!(msg.data.len(), 0); // No data for read operation

        // Encode to bytes
        let encoded = msg.encode().unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_mosdop_with_write_data() {
        use msgr2::ceph_message::{CephMessage, CrcFlags};

        let object = ObjectId {
            pool: 1,
            oid: "test_object".to_string(),
            snap: 0,
            hash: 0x12345678,
            namespace: String::new(),
            key: String::new(),
        };

        let pgid = StripedPgId {
            pool: 1,
            seed: 0x12345678,
            shard: -1,
        };

        let write_data = Bytes::from(vec![0x42; 1024]);
        let ops = vec![OSDOp {
            op: crate::types::OpCode::Write,
            flags: 0,
            op_data: OpData::Extent {
                offset: 0,
                length: 1024,
                truncate_size: 0,
                truncate_seq: 0,
            },
            indata: write_data.clone(),
        }];

        let reqid = RequestId {
            entity_name: "client.admin".to_string(),
            tid: 1,
            inc: 1,
        };

        let mosdop = MOSDOp::new(1, 1, 0, object, pgid, ops, reqid, 0);

        // Create a complete message using CephMessage framework
        let msg = CephMessage::from_payload(&mosdop, 0, CrcFlags::ALL).unwrap();

        // Verify data section contains write data
        assert_eq!(msg.data.len(), 1024);
        assert_eq!(msg.data, write_data);
    }

    #[test]
    fn test_mosdop_encoding_v9_with_squid_features() {
        use denc::features::CephFeatures;
        use msgr2::ceph_message::{CephMessage, CrcFlags};

        // Create a PGLS operation using the helper function
        let object = ObjectId::new(3, "");
        let pgid = StripedPgId::from_pg(3, 0);
        let ops = vec![OSDOp::pgls(100, denc::HObject::empty_cursor(3), 20).unwrap()];
        let reqid = RequestId::new("client.0", 1, 1);

        let mosdop = MOSDOp::new(
            1,
            20,
            MOSDOp::calculate_flags(&ops),
            object,
            pgid,
            ops,
            reqid,
            0,
        );

        // Encode with SERVER_SQUID features (v9 format)
        let msg = CephMessage::from_payload(
            &mosdop,
            CephFeatures::MASK_SERVER_SQUID.bits(),
            CrcFlags::ALL,
        )
        .unwrap();

        // Verify message version is 9
        let version = msg.header.version;
        assert_eq!(
            version, 9,
            "Message version should be 9 with SERVER_SQUID features"
        );

        // Verify front section size is 216 bytes (v9 with OpenTelemetry trace)
        assert_eq!(
            msg.front.len(),
            MOSDOp::expected_front_size_pgls(9).unwrap(),
            "Front section should be 216 bytes for v9"
        );

        // Verify data section (should contain the 39-byte HObject cursor)
        assert_eq!(
            msg.data.len(),
            39,
            "Data section should contain 39-byte HObject cursor"
        );
    }
}
