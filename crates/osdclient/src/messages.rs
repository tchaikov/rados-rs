//! Message encoding and decoding for OSD operations
//!
//! This module implements encoding/decoding for MOSDOp and MOSDOpReply messages.

use crate::error::{OSDClientError, Result};
use crate::types::{OSDOp, ObjectId, OpReply, OpResult, RequestId, StripedPgId};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
}

impl MOSDOp {
    /// Create a new MOSDOp message
    pub fn new(
        client_inc: u32,
        osdmap_epoch: u32,
        flags: u32,
        object: ObjectId,
        pgid: StripedPgId,
        ops: Vec<OSDOp>,
        reqid: RequestId,
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
        }
    }

    /// Encode the message to bytes (v8 format)
    ///
    /// This implements a simplified v8 encoding format for MOSDOp.
    /// Note: This is a basic implementation that may need refinement for
    /// full Ceph protocol compatibility.
    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        // V8 encoding format:
        // 1. spgid (spg_t) - with version header (1,1)
        // Version header: struct_v, struct_compat, len
        buf.put_u8(1); // struct_v
        buf.put_u8(1); // struct_compat
        buf.put_u32_le(18); // len: 17 bytes for pgid + 1 byte for shard

        // pgid encoding (with its own version byte)
        buf.put_u8(1); // pgid version
        buf.put_i64_le(self.pgid.pool);
        buf.put_u32_le(self.pgid.seed);
        buf.put_i32_le(-1); // preferred (always -1)

        // shard
        buf.put_i8(self.pgid.shard);

        // 2. hash (raw pg hash)
        buf.put_u32_le(self.object.hash);

        // 3. osdmap_epoch
        buf.put_u32_le(self.osdmap_epoch);

        // 4. flags
        buf.put_u32_le(self.flags);

        // 5. reqid (osd_reqid_t) - with version header (2,2)
        buf.put_u8(2); // struct_v
        buf.put_u8(2); // struct_compat
        buf.put_u32_le(21); // len: 9 bytes entity_name + 8 bytes tid + 4 bytes inc

        // entity_name_t: type (u8) + num (u64)
        buf.put_u8(8); // CEPH_ENTITY_TYPE_CLIENT
        buf.put_u64_le(0); // client num (extracted from entity_name)

        // reqid fields
        buf.put_u64_le(self.reqid.tid);
        buf.put_i32_le(self.reqid.inc);

        // 6. trace (blkin_trace_info) - 3 x u64 = 24 bytes
        buf.put_u64_le(0); // trace_id
        buf.put_u64_le(0); // span_id
        buf.put_u64_le(0); // parent_span_id

        // --- Above decoded up front; below decoded post-dispatch ---

        // 7. client_inc
        buf.put_u32_le(self.client_inc);

        // 8. mtime (timespec: sec as u32, nsec as u32)
        buf.put_u32_le(self.mtime as u32); // sec
        buf.put_u32_le(0); // nsec

        // 9. object_locator_t (using Denc encoding)
        let locator = crush::ObjectLocator {
            pool_id: self.object.pool,
            key: self.object.key.clone(),
            namespace: self.object.namespace.clone(),
            hash: -1,
        };
        use denc::denc::Denc;

        // Debug: encode to temporary buffer to see what we're encoding
        let mut locator_buf = BytesMut::new();
        locator.encode(&mut locator_buf, 0).map_err(|e| {
            OSDClientError::Encoding(format!("Failed to encode ObjectLocator: {}", e))
        })?;
        eprintln!(
            "DEBUG: ObjectLocator encoded {} bytes, hex: {:02x?}",
            locator_buf.len(),
            &locator_buf[..locator_buf.len().min(40)]
        );
        buf.put_slice(&locator_buf);

        // 10. object name (object_t)
        buf.put_u32_le(self.object.oid.len() as u32);
        buf.put_slice(self.object.oid.as_bytes());

        // 11. operations
        buf.put_u16_le(self.ops.len() as u16);
        for op in &self.ops {
            // Encode ceph_osd_op structure (42 bytes total)
            buf.put_u16_le(op.op.as_u16()); // op code
            buf.put_u32_le(op.flags); // flags

            // Encode extent union (28 bytes)
            if let Some(extent) = op.extent {
                buf.put_u64_le(extent.offset);
                buf.put_u64_le(extent.length);
                buf.put_u64_le(extent.truncate_size);
                buf.put_u32_le(extent.truncate_seq);
            } else {
                // Empty extent
                buf.put_u64_le(0);
                buf.put_u64_le(0);
                buf.put_u64_le(0);
                buf.put_u32_le(0);
            }

            // payload_len - data length for this op (stored separately from ops array)
            buf.put_u32_le(op.indata.len() as u32);
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

        eprintln!(
            "DEBUG: Full MOSDOp message {} bytes, first 128 bytes hex: {:02x?}",
            buf.len(),
            &buf[..buf.len().min(128)]
        );

        Ok(buf.freeze())
    }

    /// Extract operation data for message data section
    ///
    /// Collects all indata from operations into a single buffer for the message data section.
    /// This follows the Ceph pattern of OSDOp::merge_osd_op_vector_in_data()
    pub fn get_data_section(&self) -> Bytes {
        let mut buf = BytesMut::new();
        for op in &self.ops {
            if !op.indata.is_empty() {
                buf.put_slice(&op.indata);
            }
        }
        buf.freeze()
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
    pub ops: Vec<OpReply>,
    pub reqid: RequestId,
}

impl MOSDOpReply {
    /// Decode the message from bytes (v8 format)
    ///
    /// This implements a simplified v8 decoding format for MOSDOpReply.
    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 16 {
            return Err(OSDClientError::Decoding("Incomplete MOSDOpReply".into()));
        }

        // 1. object (hobject_t) - simplified
        let oid_len = data.get_u32_le() as usize;
        if data.remaining() < oid_len {
            return Err(OSDClientError::Decoding("Incomplete object name".into()));
        }
        let oid_bytes = &data[..oid_len];
        let oid = String::from_utf8(oid_bytes.to_vec())
            .map_err(|e| OSDClientError::Decoding(format!("Invalid UTF-8: {}", e)))?;
        data.advance(oid_len);

        let pool = data.get_i64_le();
        let hash = data.get_u32_le();
        let snap = data.get_u64_le();

        // namespace and key (simplified - assume empty for now)
        let namespace = String::new();
        let key = String::new();

        let object = ObjectId {
            pool,
            oid,
            snap,
            hash,
            namespace,
            key,
        };

        // 2. pgid (spg_t)
        let pg_pool = data.get_i64_le();
        let pg_seed = data.get_u32_le();
        let pg_shard = data.get_i8();
        let pgid = StripedPgId {
            pool: pg_pool,
            seed: pg_seed,
            shard: pg_shard,
        };

        // 3. flags
        let flags = data.get_u32_le();

        // 4. result
        let result = data.get_i32_le();

        // 5. epoch
        let epoch = data.get_u32_le();

        // 6. version
        let version = data.get_u64_le();

        // 7. user_version
        let user_version = data.get_u64_le();

        // 8. reqid
        let entity_len = data.get_u32_le() as usize;
        if data.remaining() < entity_len {
            return Err(OSDClientError::Decoding("Incomplete entity name".into()));
        }
        let entity_bytes = &data[..entity_len];
        let entity_name = String::from_utf8(entity_bytes.to_vec())
            .map_err(|e| OSDClientError::Decoding(format!("Invalid UTF-8: {}", e)))?;
        data.advance(entity_len);

        let inc = data.get_i32_le();
        let tid = data.get_u64_le();
        let reqid = RequestId {
            entity_name,
            tid,
            inc,
        };

        // 9. operations
        let num_ops = data.get_u16_le() as usize;
        let mut ops = Vec::with_capacity(num_ops);

        for _ in 0..num_ops {
            let return_code = data.get_i32_le();
            let outdata_len = data.get_u32_le() as usize;

            if data.remaining() < outdata_len {
                return Err(OSDClientError::Decoding("Incomplete outdata".into()));
            }

            let outdata = Bytes::copy_from_slice(&data[..outdata_len]);
            data.advance(outdata_len);

            ops.push(OpReply {
                return_code,
                outdata,
            });
        }

        Ok(Self {
            object,
            pgid,
            flags,
            result,
            epoch,
            version,
            user_version,
            ops,
            reqid,
        })
    }

    /// Convert to OpResult
    pub fn to_op_result(self) -> OpResult {
        OpResult {
            result: self.result,
            version: self.version,
            user_version: self.user_version,
            ops: self.ops,
        }
    }
}
