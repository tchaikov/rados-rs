//! Monitor protocol messages
//!
//! Message types and encoding/decoding for monitor communication.

use crate::error::{MonClientError, Result};
use crate::paxos_service_message::{PaxosFields, PaxosServiceMessage};
use crate::subscription::SubscribeItem;
use bytes::{Buf, Bytes, BytesMut};
use denc::UuidD;
use std::collections::HashMap;
use uuid::Uuid;

// Message type constants (add to msgr2::message)
pub const CEPH_MSG_MON_SUBSCRIBE: u16 = 0x000f;
pub const CEPH_MSG_MON_SUBSCRIBE_ACK: u16 = 0x0010;
pub const CEPH_MSG_OSD_MAP: u16 = 0x0029;
pub const CEPH_MSG_MON_GET_VERSION: u16 = 19;
pub const CEPH_MSG_MON_GET_VERSION_REPLY: u16 = 20;

// Pool operation constants
pub const POOL_OP_CREATE: u32 = 0x01;
pub const POOL_OP_DELETE: u32 = 0x02;
pub const POOL_OP_AUID_CHANGE: u32 = 0x03;
pub const POOL_OP_CREATE_SNAP: u32 = 0x11;
pub const POOL_OP_DELETE_SNAP: u32 = 0x12;
pub const POOL_OP_CREATE_UNMANAGED_SNAP: u32 = 0x21;
pub const POOL_OP_DELETE_UNMANAGED_SNAP: u32 = 0x22;

/// MMonSubscribe - Subscribe to cluster maps
#[derive(Debug, Clone)]
pub struct MMonSubscribe {
    pub what: HashMap<String, SubscribeItem>,
    pub hostname: String,
}

impl MMonSubscribe {
    pub fn new() -> Self {
        Self {
            what: HashMap::new(),
            hostname: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
        }
    }

    pub fn add(&mut self, name: String, item: SubscribeItem) {
        self.what.insert(name, item);
    }

    /// Encode to bytes for message payload
    pub fn encode(&self) -> Result<Bytes> {
        use denc::Denc;
        let mut buf = BytesMut::new();

        // Encode map size using Denc
        (self.what.len() as u32).encode(&mut buf, 0)?;

        // Encode each subscription
        for (name, item) in &self.what {
            // Encode name using Denc
            name.encode(&mut buf, 0)?;
            tracing::debug!(
                "  📝 Subscription: '{}' start={} flags={}",
                name,
                item.start,
                item.flags
            );

            // Encode subscribe item using Denc
            item.start.encode(&mut buf, 0)?;
            item.flags.encode(&mut buf, 0)?;
        }

        // Encode hostname (version 3) using Denc
        self.hostname.encode(&mut buf, 0)?;
        tracing::debug!("  🖥️  Hostname: '{}'", self.hostname);

        let bytes = buf.freeze();

        Ok(bytes)
    }

    /// Decode from message payload
    pub fn decode(mut data: &[u8]) -> Result<Self> {
        use denc::Denc;

        let count = u32::decode(&mut data, 0)
            .map_err(|e| MonClientError::DecodingError(format!("Failed to decode count: {}", e)))?
            as usize;
        let mut what = HashMap::new();

        for _ in 0..count {
            // Decode name using Denc
            let name = String::decode(&mut data, 0).map_err(|e| {
                MonClientError::DecodingError(format!("Failed to decode name: {}", e))
            })?;

            // Decode subscribe item using Denc
            let start = u64::decode(&mut data, 0)?;
            let flags = u8::decode(&mut data, 0)?;

            what.insert(name, SubscribeItem { start, flags });
        }

        // Decode hostname (version 3) using Denc
        let hostname = if data.remaining() >= 4 {
            String::decode(&mut data, 0).unwrap_or_else(|_| "unknown".to_string())
        } else {
            "unknown".to_string()
        };

        Ok(Self { what, hostname })
    }
}

impl Default for MMonSubscribe {
    fn default() -> Self {
        Self::new()
    }
}

/// MMonSubscribeAck - Acknowledgment of subscription
#[derive(Debug, Clone)]
pub struct MMonSubscribeAck {
    pub interval: u32,
    pub fsid: Uuid,
}

impl MMonSubscribeAck {
    pub fn new(interval: u32, fsid: Uuid) -> Self {
        Self { interval, fsid }
    }

    pub fn encode(&self) -> Result<Bytes> {
        use denc::Denc;
        let mut buf = BytesMut::new();
        self.interval.encode(&mut buf, 0)?;

        // Encode UUID using Denc
        let uuid_denc = UuidD::from_bytes(*self.fsid.as_bytes());
        uuid_denc.encode(&mut buf, 0)?;
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        use denc::Denc;

        let interval = u32::decode(&mut data, 0)?;

        // Decode UUID using Denc
        let uuid_denc = UuidD::decode(&mut data, 0)?;
        let fsid = Uuid::from_bytes(uuid_denc.bytes);

        Ok(Self { interval, fsid })
    }
}

/// MMonGetVersion - Query map version
#[derive(Debug, Clone)]
pub struct MMonGetVersion {
    pub tid: u64,
    pub what: String,
}

impl MMonGetVersion {
    pub fn new(tid: u64, what: String) -> Self {
        Self { tid, what }
    }

    pub fn encode(&self) -> Result<Bytes> {
        use denc::Denc;
        let mut buf = BytesMut::new();
        self.tid.encode(&mut buf, 0)?;
        self.what.encode(&mut buf, 0)?;
        let result = buf.freeze();
        eprintln!(
            "DEBUG: MMonGetVersion::encode() tid={}, what='{}', payload={} bytes: {:02x?}",
            self.tid,
            self.what,
            result.len(),
            &result[..]
        );
        Ok(result)
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        use denc::Denc;

        let tid = u64::decode(&mut data, 0)?;
        let what = String::decode(&mut data, 0).map_err(|e| {
            MonClientError::DecodingError(format!("Failed to decode what string: {}", e))
        })?;

        Ok(Self { tid, what })
    }
}

/// MMonGetVersionReply - Version query response
#[derive(Debug, Clone)]
pub struct MMonGetVersionReply {
    pub tid: u64,
    pub version: u64,
    pub oldest_version: u64,
}

impl MMonGetVersionReply {
    pub fn new(tid: u64, version: u64, oldest_version: u64) -> Self {
        Self {
            tid,
            version,
            oldest_version,
        }
    }

    pub fn encode(&self) -> Result<Bytes> {
        use denc::Denc;
        let mut buf = BytesMut::new();
        self.tid.encode(&mut buf, 0)?;
        self.version.encode(&mut buf, 0)?;
        self.oldest_version.encode(&mut buf, 0)?;
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        use denc::Denc;

        let tid = u64::decode(&mut data, 0)?;
        let version = u64::decode(&mut data, 0)?;
        let oldest_version = u64::decode(&mut data, 0)?;

        Ok(Self {
            tid,
            version,
            oldest_version,
        })
    }
}

/// MMonMap - Monitor map update
#[derive(Debug, Clone)]
pub struct MMonMap {
    pub monmap_bl: Bytes,
}

impl MMonMap {
    pub fn new(monmap_bl: Bytes) -> Self {
        Self { monmap_bl }
    }

    pub fn encode(&self) -> Result<Bytes> {
        use denc::Denc;
        let mut buf = BytesMut::new();
        // Use Denc for Bytes which handles length prefix automatically
        self.monmap_bl.encode(&mut buf, 0)?;
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        use denc::Denc;

        // Use Denc for Bytes which handles length prefix automatically
        let monmap_bl = Bytes::decode(&mut data, 0)?;
        Ok(Self { monmap_bl })
    }
}

/// MOSDMap - OSD map message
#[derive(Debug, Clone)]
pub struct MOSDMap {
    pub fsid: [u8; 16],
    pub incremental_maps: HashMap<u32, Bytes>,
    pub maps: HashMap<u32, Bytes>,
    pub cluster_osdmap_trim_lower_bound: u32,
    pub newest_map: u32,
}

impl MOSDMap {
    pub fn decode(mut data: &[u8]) -> Result<Self> {
        use denc::Denc;

        // Decode fsid using Denc (16 bytes)
        let fsid_denc = UuidD::decode(&mut data, 0)?;
        let fsid = fsid_denc.bytes;

        // Decode incremental_maps (map<epoch_t, buffer::list>)
        let inc_count = u32::decode(&mut data, 0)? as usize;
        let mut incremental_maps = HashMap::new();
        for _ in 0..inc_count {
            let epoch = u32::decode(&mut data, 0)?;
            let map_data = Bytes::decode(&mut data, 0)?;
            incremental_maps.insert(epoch, map_data);
        }

        // Decode maps (map<epoch_t, buffer::list>)
        let maps_count = u32::decode(&mut data, 0)? as usize;
        let mut maps = HashMap::new();
        for _ in 0..maps_count {
            let epoch = u32::decode(&mut data, 0)?;
            let map_data = Bytes::decode(&mut data, 0)?;
            maps.insert(epoch, map_data);
        }

        // Decode cluster_osdmap_trim_lower_bound and newest_map (version >= 2)
        let cluster_osdmap_trim_lower_bound = if data.remaining() >= 4 {
            u32::decode(&mut data, 0)?
        } else {
            0
        };

        let newest_map = if data.remaining() >= 4 {
            u32::decode(&mut data, 0)?
        } else {
            0
        };

        Ok(Self {
            fsid,
            incremental_maps,
            maps,
            cluster_osdmap_trim_lower_bound,
            newest_map,
        })
    }

    /// Get the first (oldest) epoch in this message
    pub fn get_first(&self) -> u32 {
        let mut first = u32::MAX;
        for &epoch in self.incremental_maps.keys() {
            if epoch < first {
                first = epoch;
            }
        }
        for &epoch in self.maps.keys() {
            if epoch < first {
                first = epoch;
            }
        }
        if first == u32::MAX {
            0
        } else {
            first
        }
    }

    /// Get the last (newest) epoch in this message
    pub fn get_last(&self) -> u32 {
        self.newest_map
    }
}

/// MMonCommand - Execute command on monitor
#[derive(Debug, Clone)]
pub struct MMonCommand {
    // PaxosServiceMessage fields
    pub paxos: PaxosFields,

    // MMonCommand fields
    pub fsid: UuidD,
    pub cmd: Vec<String>,
    pub inbl: Bytes, // Input buffer (data segment)
}

impl MMonCommand {
    pub fn new(_tid: u64, cmd: Vec<String>, inbl: Bytes, fsid: UuidD) -> Self {
        Self {
            paxos: PaxosFields::new(),
            fsid,
            cmd,
            inbl,
        }
    }
}

impl PaxosServiceMessage for MMonCommand {
    fn paxos_fields(&self) -> &PaxosFields {
        &self.paxos
    }

    fn paxos_fields_mut(&mut self) -> &mut PaxosFields {
        &mut self.paxos
    }

    fn encode_message(&self, buf: &mut BytesMut) -> Result<()> {
        use denc::Denc;

        // Encode fsid (UUID) using Denc
        tracing::debug!(
            "MMonCommand::encode_message: fsid={}, bytes={:02x?}",
            self.fsid,
            self.fsid.bytes
        );
        self.fsid.encode(buf, 0)?;
        tracing::debug!("MMonCommand::encode: fsid={}", self.fsid);

        // Encode command array using Denc
        (self.cmd.len() as u32).encode(buf, 0)?;
        for s in &self.cmd {
            s.encode(buf, 0)?;
        }
        tracing::debug!("MMonCommand::encode: cmd={:?}", self.cmd);

        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        use denc::Denc;

        // Decode fsid (UUID) using Denc
        let fsid = UuidD::decode(data, 0)?;

        // Decode command array using Denc
        let cmd_count = u32::decode(data, 0)? as usize;
        let mut cmd = Vec::with_capacity(cmd_count);

        for _ in 0..cmd_count {
            let s = String::decode(data, 0)?;
            cmd.push(s);
        }

        Ok(Self {
            paxos,
            fsid,
            cmd,
            inbl: Bytes::new(), // Will be filled by CephMessagePayload::decode_payload
        })
    }
}

/// MMonCommandAck - Command execution result
#[derive(Debug, Clone)]
pub struct MMonCommandAck {
    // PaxosServiceMessage fields
    pub paxos: PaxosFields,

    // MMonCommandAck fields
    pub r: i32, // errorcode32_t
    pub rs: String,
    pub cmd: Vec<String>,
}

impl MMonCommandAck {
    pub fn new(_tid: u64, retval: i32, outs: String, _outbl: Bytes) -> Self {
        Self {
            paxos: PaxosFields::new(),
            r: retval,
            rs: outs,
            cmd: Vec::new(),
        }
    }
}

impl PaxosServiceMessage for MMonCommandAck {
    fn paxos_fields(&self) -> &PaxosFields {
        &self.paxos
    }

    fn paxos_fields_mut(&mut self) -> &mut PaxosFields {
        &mut self.paxos
    }

    fn encode_message(&self, buf: &mut BytesMut) -> Result<()> {
        use denc::Denc;

        // Encode r (errorcode32_t)
        self.r.encode(buf, 0)?;

        // Encode rs string using Denc
        self.rs.encode(buf, 0)?;

        // Encode cmd array using Denc
        (self.cmd.len() as u32).encode(buf, 0)?;
        for s in &self.cmd {
            s.encode(buf, 0)?;
        }

        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        use denc::Denc;

        tracing::debug!(
            "MMonCommandAck::decode: data.len()={}, first 32 bytes: {:02x?}",
            data.len(),
            &data[..data.len().min(32)]
        );

        // Decode r (errorcode32_t)
        let r = i32::decode(data, 0)?;

        tracing::debug!("Decoded r={}, remaining={}", r, data.remaining());

        // Decode rs string using Denc
        let rs = String::decode(data, 0)?;

        // Decode cmd array using Denc
        let cmd_count = u32::decode(data, 0)? as usize;
        tracing::debug!("cmd_count={}, remaining={}", cmd_count, data.remaining());

        let mut cmd = Vec::with_capacity(cmd_count);
        for i in 0..cmd_count {
            let s = String::decode(data, 0).map_err(|e| {
                MonClientError::DecodingError(format!("Failed to decode cmd[{}]: {}", i, e))
            })?;
            cmd.push(s);
        }

        Ok(Self { paxos, r, rs, cmd })
    }
}

/// MPoolOp - Pool operation message
///
/// This message is used to create, delete, or modify pools.
/// It inherits from PaxosServiceMessage and follows the encoding
/// defined in ~/dev/ceph/src/messages/MPoolOp.h
#[derive(Debug, Clone)]
pub struct MPoolOp {
    // PaxosServiceMessage fields
    pub paxos: PaxosFields,

    // MPoolOp fields
    pub fsid: UuidD,
    pub pool: u32,
    pub name: String,
    pub op: u32,
    pub snapid: u64,
    pub crush_rule: i16,
}

impl MPoolOp {
    /// Create a new pool operation message
    pub fn new(fsid: [u8; 16], pool: u32, name: String, op: u32, version: u64) -> Self {
        Self {
            paxos: PaxosFields::with_version(version),
            fsid: UuidD::from_bytes(fsid),
            pool,
            name,
            op,
            snapid: 0,
            crush_rule: 0,
        }
    }

    /// Create a pool creation message
    ///
    /// # Arguments
    /// * `fsid` - Cluster FSID
    /// * `name` - Pool name
    /// * `crush_rule` - Optional CRUSH rule ID
    /// * `version` - Current OSDMap epoch (used by paxos versioning)
    pub fn create_pool(
        fsid: [u8; 16],
        name: String,
        crush_rule: Option<i16>,
        version: u64,
    ) -> Self {
        Self {
            paxos: PaxosFields::with_version(version),
            fsid: UuidD::from_bytes(fsid),
            pool: 0,
            name,
            op: POOL_OP_CREATE,
            snapid: 0,
            crush_rule: crush_rule.unwrap_or(0),
        }
    }

    /// Create a pool deletion message
    ///
    /// # Arguments
    /// * `fsid` - Cluster FSID
    /// * `pool` - Pool ID to delete
    /// * `_pool_name` - Pool name (unused, kept for API compatibility)
    /// * `version` - Current OSDMap epoch (used by paxos versioning)
    ///
    /// # Note
    /// The `name` field is set to "delete" as a confirmation mechanism.
    /// This prevents accidental deletions - the monitor will only proceed
    /// with deletion if the name field is NOT a valid pool name.
    pub fn delete_pool(fsid: [u8; 16], pool: u32, _pool_name: String, version: u64) -> Self {
        Self {
            paxos: PaxosFields::with_version(version),
            fsid: UuidD::from_bytes(fsid),
            pool,
            name: "delete".to_string(), // Confirmation string, NOT the pool name!
            op: POOL_OP_DELETE,
            snapid: 0,
            crush_rule: 0,
        }
    }
}

impl PaxosServiceMessage for MPoolOp {
    fn paxos_fields(&self) -> &PaxosFields {
        &self.paxos
    }

    fn paxos_fields_mut(&mut self) -> &mut PaxosFields {
        &mut self.paxos
    }

    fn encode_message(&self, buf: &mut BytesMut) -> Result<()> {
        use denc::Denc;

        // Encode fsid (UUID) using Denc
        self.fsid.encode(buf, 0)?;

        // Encode pool
        self.pool.encode(buf, 0)?;

        // Encode op
        self.op.encode(buf, 0)?;

        // Encode old_auid (obsolete, always 0)
        0u64.encode(buf, 0)?;

        // Encode snapid
        self.snapid.encode(buf, 0)?;

        // Encode name using Denc
        self.name.encode(buf, 0)?;

        // Encode pad (for v3->v4 encoding change)
        0u8.encode(buf, 0)?;

        // Encode crush_rule
        self.crush_rule.encode(buf, 0)?;

        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        use denc::Denc;

        // Decode fsid (UUID) using Denc
        let fsid = UuidD::decode(data, 0)?;

        // Decode pool
        let pool = u32::decode(data, 0)?;

        // Decode op
        let op = u32::decode(data, 0)?;

        // Decode old_auid (obsolete)
        let _old_auid = u64::decode(data, 0)?;

        // Decode snapid
        let snapid = u64::decode(data, 0)?;

        // Decode name using Denc
        let name = String::decode(data, 0)?;

        // Decode pad (for v3->v4 encoding change)
        let _pad = u8::decode(data, 0)?;

        // Decode crush_rule
        let crush_rule = i16::decode(data, 0)?;

        Ok(Self {
            paxos,
            fsid,
            pool,
            name,
            op,
            snapid,
            crush_rule,
        })
    }
}

/// MPoolOpReply - Pool operation reply message
///
/// This message is sent in response to MPoolOp.
/// It inherits from PaxosServiceMessage and follows the encoding
/// defined in ~/dev/ceph/src/messages/MPoolOpReply.h
#[derive(Debug, Clone)]
pub struct MPoolOpReply {
    // PaxosServiceMessage fields
    pub paxos: PaxosFields,

    // MPoolOpReply fields
    pub fsid: UuidD,
    pub reply_code: u32,
    pub epoch: u32,
    pub response_data: Bytes,
}

impl MPoolOpReply {
    pub fn new(fsid: [u8; 16], reply_code: u32, epoch: u32, version: u64) -> Self {
        Self {
            paxos: PaxosFields::with_version(version),
            fsid: UuidD::from_bytes(fsid),
            reply_code,
            epoch,
            response_data: Bytes::new(),
        }
    }
}

impl PaxosServiceMessage for MPoolOpReply {
    fn paxos_fields(&self) -> &PaxosFields {
        &self.paxos
    }

    fn paxos_fields_mut(&mut self) -> &mut PaxosFields {
        &mut self.paxos
    }

    fn encode_message(&self, buf: &mut BytesMut) -> Result<()> {
        use denc::Denc;

        // Encode fsid (UUID) using Denc
        self.fsid.encode(buf, 0)?;

        // Encode reply_code
        self.reply_code.encode(buf, 0)?;

        // Encode epoch
        self.epoch.encode(buf, 0)?;

        // Encode response_data (optional)
        if !self.response_data.is_empty() {
            1u8.encode(buf, 0)?; // has_data = true
            self.response_data.encode(buf, 0)?;
        } else {
            0u8.encode(buf, 0)?; // has_data = false
        }

        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        use denc::Denc;

        // Decode fsid (UUID) using Denc
        let fsid = UuidD::decode(data, 0)?;

        // Decode reply_code
        let reply_code = u32::decode(data, 0)?;

        // Decode epoch
        let epoch = u32::decode(data, 0)?;

        // Decode response_data (optional)
        let has_data = u8::decode(data, 0)?;

        let response_data = if has_data != 0 {
            Bytes::decode(data, 0)?
        } else {
            Bytes::new()
        };

        Ok(Self {
            paxos,
            fsid,
            reply_code,
            epoch,
            response_data,
        })
    }
}

/// MAuth - Authentication request message
///
/// This message is used for authentication with the monitor, including:
/// - Initial authentication during connection setup
/// - Ticket renewal requests
///
/// This is a PaxosServiceMessage, so it includes paxos fields.
#[derive(Debug, Clone)]
pub struct MAuth {
    // PaxosServiceMessage fields
    pub paxos: PaxosFields,

    // MAuth-specific fields
    pub protocol: u32,
    pub auth_payload: Bytes,
    pub monmap_epoch: u32,
}

impl MAuth {
    pub fn new(protocol: u32, auth_payload: Bytes) -> Self {
        Self {
            paxos: PaxosFields::new(),
            protocol,
            auth_payload,
            monmap_epoch: 0,
        }
    }
}

impl PaxosServiceMessage for MAuth {
    fn paxos_fields(&self) -> &PaxosFields {
        &self.paxos
    }

    fn paxos_fields_mut(&mut self) -> &mut PaxosFields {
        &mut self.paxos
    }

    fn encode_message(&self, buf: &mut BytesMut) -> Result<()> {
        use denc::Denc;
        self.protocol.encode(buf, 0)?;
        self.auth_payload.encode(buf, 0)?;
        self.monmap_epoch.encode(buf, 0)?;
        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        use denc::Denc;

        let protocol = u32::decode(data, 0)?;
        let auth_payload = Bytes::decode(data, 0)?;

        // monmap_epoch is optional (for backwards compatibility)
        let monmap_epoch = if !data.is_empty() {
            u32::decode(data, 0)?
        } else {
            0
        };

        Ok(Self {
            paxos,
            protocol,
            auth_payload,
            monmap_epoch,
        })
    }
}

/// MAuthReply - Authentication reply message
///
/// This message is sent by the monitor in response to MAuth.
/// Note: This is NOT a PaxosServiceMessage (it inherits from Message in C++).
#[derive(Debug, Clone)]
pub struct MAuthReply {
    pub protocol: u32,
    pub result: i32,
    pub global_id: u64,
    pub result_msg: String,
    pub auth_payload: Bytes,
}

impl MAuthReply {
    pub fn new(
        protocol: u32,
        result: i32,
        global_id: u64,
        result_msg: String,
        auth_payload: Bytes,
    ) -> Self {
        Self {
            protocol,
            result,
            global_id,
            result_msg,
            auth_payload,
        }
    }

    /// Encode the message payload
    pub fn encode(&self) -> Result<Bytes> {
        use denc::Denc;
        let mut buf = BytesMut::new();

        self.protocol.encode(&mut buf, 0)?;
        self.result.encode(&mut buf, 0)?;
        self.global_id.encode(&mut buf, 0)?;

        // Encode result_msg as length-prefixed string
        let msg_bytes = Bytes::from(self.result_msg.as_bytes().to_vec());
        msg_bytes.encode(&mut buf, 0)?;

        self.auth_payload.encode(&mut buf, 0)?;

        Ok(buf.freeze())
    }

    /// Decode the message payload
    pub fn decode(data: &mut &[u8]) -> Result<Self> {
        use denc::Denc;

        let protocol = u32::decode(data, 0)?;
        let result = i32::decode(data, 0)?;
        let global_id = u64::decode(data, 0)?;

        // Decode result_msg as string
        let result_msg_bytes = Bytes::decode(data, 0)?;
        let result_msg = String::from_utf8(result_msg_bytes.to_vec())
            .map_err(|e| MonClientError::Other(format!("Invalid UTF-8 in result_msg: {}", e)))?;

        let auth_payload = Bytes::decode(data, 0)?;

        Ok(Self {
            protocol,
            result,
            global_id,
            result_msg,
            auth_payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_encode_decode() {
        let mut msg = MMonSubscribe::new();
        msg.add(
            "osdmap".to_string(),
            SubscribeItem {
                start: 10,
                flags: 0,
            },
        );
        msg.add("monmap".to_string(), SubscribeItem { start: 5, flags: 1 });

        let encoded = msg.encode().unwrap();
        let decoded = MMonSubscribe::decode(&encoded).unwrap();

        assert_eq!(decoded.what.len(), 2);
        assert_eq!(decoded.what.get("osdmap").unwrap().start, 10);
        assert_eq!(decoded.what.get("monmap").unwrap().flags, 1);
    }

    #[test]
    fn test_version_reply_encode_decode() {
        let msg = MMonGetVersionReply::new(42, 100, 50);
        let encoded = msg.encode().unwrap();
        let decoded = MMonGetVersionReply::decode(&encoded).unwrap();

        assert_eq!(decoded.tid, 42);
        assert_eq!(decoded.version, 100);
        assert_eq!(decoded.oldest_version, 50);
    }
}
