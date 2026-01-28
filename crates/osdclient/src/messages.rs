//! Message encoding and decoding for OSD operations
//!
//! This module implements encoding/decoding for MOSDOp and MOSDOpReply messages.

use crate::error::{OSDClientError, Result};
use crate::types::{OSDOp, ObjectId, OpData, OpReply, OpResult, PgId, RequestId, StripedPgId};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
use tracing::debug;

/// Message type for MOSDOp (Client to OSD)
pub const CEPH_MSG_OSD_OP: u16 = 42;

/// Message type for MOSDOpReply (OSD to Client)
pub const CEPH_MSG_OSD_OPREPLY: u16 = 43;

/// MOSDOp message - Client to OSD (message type 42)
#[derive(Debug, Clone)]
pub struct MOSDOp {
    pub client_inc: u32,
    pub osdmap_epoch: u32,
    pub flags: u32,
    pub mtime: u64, // Simplified: using u64 instead of UTime for now
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
            mtime: 0, // Current time - simplified for now
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
        use crate::types::flags::*;

        let mut flags = CEPH_OSD_FLAG_ACK; // Always want acknowledgment

        // Check if we have any read or write operations
        let has_read = ops.iter().any(|op| op.op.is_read());
        let has_write = ops.iter().any(|op| op.op.is_write());

        if has_read {
            flags |= CEPH_OSD_FLAG_READ;
        }
        if has_write {
            flags |= CEPH_OSD_FLAG_WRITE;
        }

        // Check if any operation is a PG operation (based on opcode type)
        let has_pgop = ops.iter().any(|op| op.op.is_pg_op());
        if has_pgop {
            flags |= CEPH_OSD_FLAG_PGOP;
        }

        flags
    }

    /// Encode the message payload (front section) to bytes (v9 format)
    ///
    /// This implements v9 encoding format for MOSDOp.
    /// Note: This is a basic implementation that may need refinement for
    /// full Ceph protocol compatibility.
    pub(crate) fn encode_payload_internal(&self, _features: u64) -> Result<Bytes> {
        use crate::denc_types::OsdReqId;
        use crate::types::{
            BlkinTraceInfo, EntityName, JaegerSpanContext, CEPH_ENTITY_TYPE_CLIENT,
        };
        use denc::denc::Denc;

        let mut buf = BytesMut::new();

        // Debug logging for MOSDOp message
        eprintln!(
            "DEBUG encode MOSDOp: pgid={}:{}, hash={:#x}, snapid={:#x}, flags={:#x}",
            self.pgid.pool, self.pgid.seed, self.object.hash, self.snapid, self.flags
        );

        // 1. spgid (spg_t) - with version header (1,1)
        self.pgid
            .encode(&mut buf, 0)
            .map_err(|e| OSDClientError::Encoding(format!("Failed to encode spg_t: {}", e)))?;

        // 2. hash (raw pg hash)
        buf.put_u32_le(self.object.hash);

        // 3. osdmap_epoch
        buf.put_u32_le(self.osdmap_epoch);

        // 4. flags
        buf.put_u32_le(self.flags);

        // 5. reqid (osd_reqid_t) - with version header (2,2)
        let entity_name = EntityName::new(CEPH_ENTITY_TYPE_CLIENT, self.global_id);
        let reqid = OsdReqId {
            name: entity_name,
            tid: self.reqid.tid,
            inc: self.reqid.inc,
        };
        reqid.encode(&mut buf, 0).map_err(|e| {
            OSDClientError::Encoding(format!("Failed to encode osd_reqid_t: {}", e))
        })?;

        // 6. trace (blkin_trace_info) - 3 x u64 = 24 bytes
        let trace = BlkinTraceInfo::empty();
        trace.encode(&mut buf, 0).map_err(|e| {
            OSDClientError::Encoding(format!("Failed to encode blkin_trace_info: {}", e))
        })?;
        eprintln!(
            "DEBUG: After blkin_trace encoding, buf.len() = {}",
            buf.len()
        );

        // 6b. otel_trace (jspan_context) - added in v9
        let otel_trace = JaegerSpanContext::invalid();
        otel_trace.encode(&mut buf, 0).map_err(|e| {
            OSDClientError::Encoding(format!("Failed to encode jspan_context: {}", e))
        })?;
        eprintln!(
            "DEBUG: After otel_trace encoding, buf.len() = {}",
            buf.len()
        );

        // --- Above decoded up front; below decoded post-dispatch ---

        // 7. client_inc
        buf.put_u32_le(self.client_inc);
        eprintln!("DEBUG: After client_inc, buf.len() = {}", buf.len());

        // 8. mtime (timespec: sec as u32, nsec as u32)
        buf.put_u32_le(self.mtime as u32); // sec
        buf.put_u32_le(0); // nsec
        eprintln!("DEBUG: After mtime, buf.len() = {}", buf.len());

        // 9. object_locator_t (using Denc encoding)
        // Note: hash=-1 means "calculate from object name" which is the normal case
        let locator = crush::ObjectLocator {
            pool_id: self.object.pool,
            key: self.object.key.clone(),
            namespace: self.object.namespace.clone(),
            hash: -1,
        };

        let before_len = buf.len();
        locator.encode(&mut buf, 0).map_err(|e| {
            OSDClientError::Encoding(format!("Failed to encode ObjectLocator: {}", e))
        })?;
        let after_len = buf.len();
        let encoded_bytes = after_len - before_len;
        eprintln!(
            "DEBUG: Encoded ObjectLocator: pool={}, key='{}', namespace='{}', hash={}, encoded {} bytes",
            locator.pool_id, locator.key, locator.namespace, locator.hash, encoded_bytes
        );
        if encoded_bytes < 50 {
            let start = before_len;
            let locator_bytes: Vec<u8> = buf[start..after_len].to_vec();
            let hex_str: String = locator_bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("");
            eprintln!("DEBUG: ObjectLocator bytes (hex): {}", hex_str);
        }

        // 10. object name (object_t)
        buf.put_u32_le(self.object.oid.len() as u32);
        buf.put_slice(self.object.oid.as_bytes());

        // 11. operations
        buf.put_u16_le(self.ops.len() as u16);
        for op in &self.ops {
            // Debug logging for PGLS operations
            eprintln!(
                "DEBUG encode: op={:#x} ({:?}), flags={:#x}, indata_len={}",
                op.op.as_u16(),
                op.op,
                op.flags,
                op.indata.len()
            );

            // Encode ceph_osd_op structure (38 bytes total)
            // 2 (op) + 4 (flags) + 28 (union) + 4 (payload_len) = 38
            let op_start = buf.len();
            buf.put_u16_le(op.op.as_u16()); // op code
            buf.put_u32_le(op.flags); // flags

            // Encode operation data union (28 bytes total)
            match &op.op_data {
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
                    eprintln!(
                        "DEBUG encode Pgls: max_entries={}, start_epoch={}",
                        max_entries, start_epoch
                    );
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

            // payload_len - data length for this op (stored separately from ops array)
            buf.put_u32_le(op.indata.len() as u32);

            // Debug: print hex dump of this operation
            let op_end = buf.len();
            let op_bytes = &buf[op_start..op_end];
            let hex_str: String = op_bytes.iter().map(|b| format!("{:02x}", b)).collect();
            eprintln!("DEBUG op bytes (38 bytes): {}", hex_str);
            eprintln!(
                "DEBUG: Operation at buffer offset {}-{}",
                op_start,
                op_end - 1
            );
        }

        // Debug: Check operation bytes are still intact before continuing
        let first_op_start = buf.len() - (38 * self.ops.len());
        eprintln!(
            "DEBUG: Before adding snapid, checking operation bytes at offset {}...",
            first_op_start
        );
        if !self.ops.is_empty() {
            let op_check: String = buf[first_op_start..first_op_start + 38]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect();
            eprintln!("DEBUG: Op bytes check: {}", op_check);
        }

        // 12. snapid
        buf.put_u64_le(self.snapid);

        // 13. snap_seq
        buf.put_u64_le(self.snap_seq);

        // 14. snaps vector
        buf.put_u32_le(self.snaps.len() as u32);
        for snap in &self.snaps {
            buf.put_u64_le(*snap);
        }

        // 15. retry_attempt
        buf.put_i32_le(self.retry_attempt);

        // 16. features (set to 0 for now)
        buf.put_u64_le(0);

        eprintln!("DEBUG: Final buf.len() before freeze = {}", buf.len());

        // Debug: dump key sections of payload
        let dump_len = std::cmp::min(buf.len(), 250);
        let hex_str: String = buf[..dump_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        eprintln!("DEBUG: First {} bytes of payload (hex):", dump_len);
        for (i, chunk) in hex_str.as_bytes().chunks(64).enumerate() {
            eprintln!("  {:04x}: {}", i * 32, std::str::from_utf8(chunk).unwrap());
        }

        Ok(buf.freeze())
    }

    /// Get the expected front section size for a PGLS operation
    ///
    /// This is useful for verifying the encoding is correct.
    /// The size should be 216 bytes for v9 (based on actual encoding)
    pub fn expected_front_size_pgls() -> usize {
        // Calculated from actual v9 encoding
        216
    }

    /// Extract operation data for message data section
    ///
    /// Collects all indata from operations into a single buffer for the message data section.
    /// This follows the Ceph pattern of OSDOp::merge_osd_op_vector_in_data()
    pub(crate) fn get_data_section_internal(&self) -> Bytes {
        let mut buf = BytesMut::new();
        for op in &self.ops {
            if !op.indata.is_empty() {
                buf.put_slice(&op.indata);
            }
        }
        buf.freeze()
    }

    /// Legacy encode method for backwards compatibility
    #[deprecated(note = "Use CephMessage::from_payload instead")]
    pub fn encode(&self) -> Result<Bytes> {
        self.encode_payload_internal(0)
    }

    /// Legacy get_data_section method for backwards compatibility
    #[deprecated(note = "Use encode_data instead")]
    pub fn get_data_section(&self) -> Bytes {
        self.get_data_section_internal()
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

        eprintln!(
            "DEBUG: MOSDOpReply::decode called: front.len()={}, data.len()={}",
            front.len(),
            data.len()
        );
        eprintln!(
            "DEBUG: First 64 bytes of front: {:02x?}",
            &front[..std::cmp::min(64, front.len())]
        );

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
        let flags = cursor.get_i64_le() as u32;

        // 4. result (errorcode32_t = int32_t)
        let result = cursor.get_i32_le();
        eprintln!("DEBUG: Overall result code: {}", result);

        // 5. bad_replay_version (eversion_t = epoch + version)
        // This is for backwards compatibility with old clients.
        // Modern clients should use replay_version (our 'version' field) and user_version instead.
        // See: ~/dev/ceph/src/messages/MOSDOpReply.h set_reply_versions()
        let _bad_replay_epoch = cursor.get_u32_le();
        let _bad_replay_version = cursor.get_u64_le();

        // 6. osdmap_epoch (epoch_t = u32)
        let epoch = cursor.get_u32_le();

        // 7. num_ops (u32)
        let num_ops = cursor.get_u32_le() as usize;

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
            let payload_len = cursor.get_u32_le();
            eprintln!(
                "DEBUG: Op {} payload_len from osd_op structure: {}",
                i, payload_len
            );
            payload_lens.push(payload_len as usize);
        }

        // 9. retry_attempt (int32_t)
        // Used to validate that the reply matches the request attempt
        // See: ~/dev/linux/net/ceph/osd_client.c handle_reply()
        let retry_attempt = cursor.get_i32_le();

        // 10. For each op: rval (int32_t)
        let mut ops = Vec::with_capacity(num_ops);
        for i in 0..num_ops {
            if cursor.remaining() < 4 {
                return Err(OSDClientError::Decoding(format!(
                    "Incomplete rval {}: need 4 bytes, have {}",
                    i,
                    cursor.remaining()
                )));
            }
            let return_code = cursor.get_i32_le();
            eprintln!("DEBUG: Op {} return_code (rval): {}", i, return_code);

            ops.push(OpReply {
                return_code,
                outdata: Bytes::new(), // Will be filled from data section below
            });
        }

        // 11. replay_version (eversion_t = epoch + version)
        // The epoch part is not currently used since we track OSDMap epoch separately
        let _replay_epoch = cursor.get_u32_le();
        let version = cursor.get_u64_le();
        eprintln!(
            "DEBUG: Decoded version from MOSDOpReply: epoch={}, version={}",
            _replay_epoch, version
        );

        // 12. user_version (version_t = u64)
        let user_version = cursor.get_u64_le();
        eprintln!(
            "DEBUG: Decoded user_version from MOSDOpReply: {}",
            user_version
        );

        // 13. do_redirect (bool)
        let do_redirect = cursor.get_u8() != 0;

        // 14. If do_redirect: redirect structure
        if do_redirect {
            // request_redirect_t encoding (v1):
            // - struct_v (u8), struct_compat (u8), len (u32)
            // - object_locator_t (redirect_locator)
            // - string (redirect_object)
            // - u32 (legacy field, always 0)

            if cursor.remaining() < 6 {
                return Err(OSDClientError::Decoding(
                    "Incomplete redirect header".into(),
                ));
            }

            // Version fields from ENCODE_START macro
            // TODO: In production, should validate struct_v is compatible
            let _struct_v = cursor.get_u8();
            let _struct_compat = cursor.get_u8();
            let redirect_len = cursor.get_u32_le();

            if cursor.remaining() < redirect_len as usize {
                return Err(OSDClientError::Decoding(format!(
                    "Incomplete redirect data: expected {} bytes, got {}",
                    redirect_len,
                    cursor.remaining()
                )));
            }

            // Skip the redirect data for now - we don't handle redirects yet
            // In the future, we could parse and follow the redirect
            cursor.advance(redirect_len as usize);

            debug!("Received redirect response (not following redirect)");
        }

        // 15. trace (blkin_trace_info: 3 x i64)
        // The trace is used for distributed tracing (Zipkin/Jaeger)
        // These fields could be exposed in the future for observability/debugging
        // See: ~/dev/ceph/src/include/encoding.h encode(blkin_trace_info)
        if cursor.remaining() >= 24 {
            let _trace_id = cursor.get_i64_le();
            let _span_id = cursor.get_i64_le();
            let _parent_span_id = cursor.get_i64_le();
        }

        // 16. Distribute data section to operations
        // The data section contains concatenated output data for all operations
        eprintln!(
            "DEBUG: Distributing data section: {} ops, data.len()={}, payload_lens={:?}",
            ops.len(),
            data.len(),
            payload_lens
        );
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
                eprintln!("DEBUG: Op {}: assigned {} bytes of outdata", i, len);
            } else {
                eprintln!("DEBUG: Op {}: no outdata (payload_len=0)", i);
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

    fn msg_version() -> u16 {
        Self::VERSION
    }

    fn msg_compat_version() -> u16 {
        Self::COMPAT_VERSION
    }

    fn encode_payload(&self, features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        self.encode_payload_internal(features)
            .map_err(|_e| msgr2::Error::Serialization)
    }

    fn encode_data(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        Ok(self.get_data_section_internal())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mosdop_encoding_v9() {
        use crate::types::{OSDOp, ObjectId, RequestId, StripedPgId};

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

        // Encode
        let encoded = mosdop.encode().expect("Failed to encode");

        // Verify size
        // Expected: 216 bytes for v9 (based on actual encoding)
        eprintln!("Encoded front size: {} bytes", encoded.len());
        eprintln!(
            "Expected front size: {} bytes",
            MOSDOp::expected_front_size_pgls()
        );
        assert_eq!(
            encoded.len(),
            MOSDOp::expected_front_size_pgls(),
            "Front section should be 216 bytes for v9"
        );

        // Verify data section (should contain the 39-byte HObject cursor)
        let data = mosdop.get_data_section();
        eprintln!("Data section size: {} bytes", data.len());
        assert_eq!(
            data.len(),
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
        assert_eq!(msg.header.msg_type, CEPH_MSG_OSD_OP);
        assert_eq!(msg.header.version, MOSDOp::VERSION);
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
