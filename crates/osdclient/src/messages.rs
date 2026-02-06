//! Message encoding and decoding for OSD operations
//!
//! This module implements encoding/decoding for MOSDOp and MOSDOpReply messages.

use crate::error::{OSDClientError, Result};
use crate::types::{
    OSDOp, ObjectId, OpReply, OpResult, PgId, RequestId, RequestRedirect, StripedPgId,
};

#[cfg(test)]
use crate::types::OpData;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
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

/// MOSDOp message - Client to OSD (message type 42)
#[derive(Debug, Clone)]
pub struct MOSDOp {
    pub client_inc: u32,
    pub osdmap_epoch: u32,
    pub flags: u32,
    pub mtime: crate::denc_types::UTime,
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
            mtime: crate::denc_types::UTime::zero(),
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

        // Check if we have any read or write operations
        let has_read = ops.iter().any(|op| op.op.is_read());
        let has_write = ops.iter().any(|op| op.op.is_write());

        if has_read {
            flags |= OsdOpFlags::READ;
        }
        if has_write {
            flags |= OsdOpFlags::WRITE;
        }

        // Check if any operation is a PG operation (based on opcode type)
        let has_pgop = ops.iter().any(|op| op.op.is_pg_op());
        if has_pgop {
            flags |= OsdOpFlags::PGOP;
        }

        flags.bits()
    }

    /// Get the expected front section size for a PGLS operation
    ///
    /// This is useful for verifying the encoding is correct.
    /// The size should be 216 bytes for v9 (based on actual encoding)
    pub fn expected_front_size_pgls() -> usize {
        // Calculated from actual v9 encoding
        216
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

    /// Decode MOSDOpReply from front and data sections
    ///
    /// This implements v8 decoding format for MOSDOpReply.
    /// Reference: ~/dev/ceph/src/messages/MOSDOpReply.h lines 199-230
    ///
    /// # Arguments
    /// * `front` - The front (payload) section of the message
    /// * `data` - The data section of the message (contains operation output data)
    pub fn decode(front: &[u8], data: &[u8]) -> Result<Self> {
        use denc::Denc;

        let mut cursor = front;
        if cursor.remaining() < 16 {
            return Err(OSDClientError::Decoding("Incomplete MOSDOpReply".into()));
        }

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
        let oid = String::decode(&mut cursor, 0)
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode oid: {}", e)))?;

        // 2. pgid (pg_t) - use Denc infrastructure
        let pgid_raw = PgId::decode(&mut cursor, 0)
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode pgid: {}", e)))?;

        let pgid = StripedPgId {
            pool: pgid_raw.pool,
            seed: pgid_raw.seed,
            shard: -1, // Not in pg_t, only in spg_t
        };

        // 3. flags (int64_t)
        let flags = i64::decode(&mut cursor, 0)
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode flags: {}", e)))?
            as u32;

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
            if cursor.remaining() < 38 {
                return Err(OSDClientError::Decoding(format!(
                    "Incomplete osd_op {}: need 38 bytes, have {}",
                    i,
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
            let r = RequestRedirect::decode(&mut cursor, 0).map_err(|e| {
                OSDClientError::Decoding(format!("Failed to decode redirect: {}", e))
            })?;
            debug!(
                "Received redirect: pool={}, key={}, nspace={}, object={}",
                r.redirect_locator.pool,
                r.redirect_locator.key,
                r.redirect_locator.nspace,
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
        if cursor.remaining() >= 24 {
            let _trace = crate::types::BlkinTraceInfo::decode(&mut cursor, 0).map_err(|e| {
                OSDClientError::Decoding(format!("Failed to decode blkin_trace_info: {}", e))
            })?;
        }

        // 16. Distribute data section to operations
        // The data section contains concatenated output data for all operations

        let mut data_offset = 0;
        for (i, op) in ops.iter_mut().enumerate() {
            let len = payload_lens[i];
            if len > 0 {
                if data_offset + len > data.len() {
                    return Err(OSDClientError::Decoding(format!(
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
#[derive(Debug, Clone)]
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

    /// Decode MOSDBackoff from front section
    ///
    /// This implements v1 decoding format for MOSDBackoff.
    /// Reference: ~/dev/ceph/src/messages/MOSDBackoff.h lines 63-72
    ///
    /// # Arguments
    /// * `front` - The front (payload) section of the message
    pub fn decode(front: &[u8]) -> Result<Self> {
        use denc::Denc;

        let mut cursor = front;

        // 1. pgid (spg_t)
        let pgid = StripedPgId::decode(&mut cursor, 0)
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode pgid: {}", e)))?;

        // 2. map_epoch (epoch_t = u32)
        let map_epoch = u32::decode(&mut cursor, 0)?;

        // 3. op (uint8_t)
        let op = u8::decode(&mut cursor, 0)?;

        // 4. id (uint64_t)
        let id = u64::decode(&mut cursor, 0)?;

        // 5. begin (hobject_t)
        let begin = denc::HObject::decode(&mut cursor, 0)
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode begin: {}", e)))?;

        // 6. end (hobject_t)
        let end = denc::HObject::decode(&mut cursor, 0)
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode end: {}", e)))?;

        Ok(Self {
            pgid,
            map_epoch,
            op,
            id,
            begin,
            end,
        })
    }

    /// Encode MOSDBackoff to bytes
    ///
    /// This implements v1 encoding format for MOSDBackoff.
    /// Reference: ~/dev/ceph/src/messages/MOSDBackoff.h lines 53-61
    pub fn encode(&self) -> Result<Bytes> {
        use denc::Denc;

        let mut buf = BytesMut::new();

        // 1. pgid (spg_t)
        self.pgid.encode(&mut buf, 0)?;

        // 2. map_epoch (epoch_t = u32)
        self.map_epoch.encode(&mut buf, 0)?;

        // 3. op (uint8_t)
        self.op.encode(&mut buf, 0)?;

        // 4. id (uint64_t)
        self.id.encode(&mut buf, 0)?;

        // 5. begin (hobject_t)
        self.begin.encode(&mut buf, 0)?;

        // 6. end (hobject_t)
        self.end.encode(&mut buf, 0)?;

        Ok(buf.freeze())
    }
}

// ============================================================================
// CephMessagePayload trait implementations
// ============================================================================

impl CephMessagePayload for MOSDOp {
    fn msg_type() -> u16 {
        CEPH_MSG_OSD_OP
    }

    fn msg_version() -> u16 {
        Self::VERSION
    }

    fn msg_compat_version() -> u16 {
        Self::COMPAT_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        use crate::denc_types::OsdReqId;
        use crate::types::{
            BlkinTraceInfo, EntityName, JaegerSpanContext, CEPH_ENTITY_TYPE_CLIENT,
        };
        use denc::denc::Denc;

        let mut buf = BytesMut::new();

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
        let entity_name = EntityName::new(CEPH_ENTITY_TYPE_CLIENT, self.global_id);
        let reqid = OsdReqId {
            name: entity_name,
            tid: self.reqid.tid,
            inc: self.reqid.inc,
        };
        reqid.encode(&mut buf, 0)?;

        // 6. trace (blkin_trace_info) - 3 x u64 = 24 bytes
        let trace = BlkinTraceInfo::empty();
        trace.encode(&mut buf, 0)?;

        // 6b. otel_trace (jspan_context) - added in v9
        let otel_trace = JaegerSpanContext::invalid();
        otel_trace.encode(&mut buf, 0)?;

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
            hash: -1,
        };

        let before_len = buf.len();
        locator.encode(&mut buf, 0)?;
        let after_len = buf.len();
        let encoded_bytes = after_len - before_len;

        if encoded_bytes < 50 {
            let start = before_len;
            let locator_bytes: Vec<u8> = buf[start..after_len].to_vec();
            let _hex_str: String = locator_bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("");
        }

        // 10. object name (object_t)
        self.object.oid.encode(&mut buf, 0)?;

        // 11. operations
        (self.ops.len() as u16).encode(&mut buf, 0)?;
        for op in &self.ops {
            use denc::Denc;

            // Debug logging for PGLS operations

            // Encode ceph_osd_op structure (38 bytes) using Denc
            let op_start = buf.len();
            op.encode(&mut buf, 0)?;

            // Debug: print hex dump of this operation
            let op_end = buf.len();
            let op_bytes = &buf[op_start..op_end];
            let _hex_str: String = op_bytes.iter().map(|b| format!("{:02x}", b)).collect();
        }

        // Debug: Check operation bytes are still intact before continuing
        let first_op_start = buf.len() - (38 * self.ops.len());

        if !self.ops.is_empty() {
            let _op_check: String = buf[first_op_start..first_op_start + 38]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect();
        }

        // 12. snapid
        self.snapid.encode(&mut buf, 0)?;

        // 13. snap_seq
        self.snap_seq.encode(&mut buf, 0)?;

        // 14. snaps vector
        self.snaps.encode(&mut buf, 0)?;

        // 15. retry_attempt
        self.retry_attempt.encode(&mut buf, 0)?;

        // 16. features (set to 0 for now)
        0u64.encode(&mut buf, 0)?;

        Ok(buf.freeze())
    }

    fn encode_data(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        // Collect all indata from operations into a single buffer for the message data section.
        // This follows the Ceph pattern of OSDOp::merge_osd_op_vector_in_data()
        let mut buf = BytesMut::new();
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
    ) -> std::result::Result<Self, msgr2::Error> {
        // MOSDOp decoding not implemented yet - this is typically only needed on the server side
        let _ = (front, data);
        Err(msgr2::Error::Deserialization(
            "MOSDOp decoding not implemented".into(),
        ))
    }
}

impl CephMessagePayload for MOSDOpReply {
    fn msg_type() -> u16 {
        CEPH_MSG_OSD_OPREPLY
    }

    fn msg_version() -> u16 {
        Self::VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        // MOSDOpReply encoding not implemented yet - this is typically only needed on the server side
        Err(msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        Self::decode(front, data)
            .map_err(|_e| msgr2::Error::Deserialization("MOSDOpReply decode failed".into()))
    }
}

impl CephMessagePayload for MOSDBackoff {
    fn msg_type() -> u16 {
        CEPH_MSG_OSD_BACKOFF
    }

    fn msg_version() -> u16 {
        Self::VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        self.encode().map_err(|_| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        Self::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MOSDBackoff decode failed".into()))
    }
}

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
        let ops = vec![OSDOp::pgls(100, denc::HObject::empty_cursor(3), 20)];
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

        // Encode using CephMessage framework
        let msg = CephMessage::from_payload(&mosdop, 0, CrcFlags::ALL).unwrap();

        // Verify front section size
        // Expected: 216 bytes for v9 (based on actual encoding)

        assert_eq!(
            msg.front.len(),
            MOSDOp::expected_front_size_pgls(),
            "Front section should be 216 bytes for v9"
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

        // Create a complete message using CephMessage framework
        let msg = CephMessage::from_payload(&mosdop, 0, CrcFlags::ALL).unwrap();

        // Verify message structure
        // Copy packed struct fields to local variables to avoid unaligned references
        let msg_type = msg.header.msg_type;
        let version = msg.header.version;
        assert_eq!(msg_type, CEPH_MSG_OSD_OP);
        assert_eq!(version, MOSDOp::VERSION);
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
}
