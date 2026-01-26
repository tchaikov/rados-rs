//! Monitor protocol messages
//!
//! Message types and encoding/decoding for monitor communication.

use crate::error::{MonClientError, Result};
use crate::paxos_service_message::{PaxosFields, PaxosServiceMessage};
use crate::subscription::SubscribeItem;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use denc::{Denc, UuidD};
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
        let mut buf = BytesMut::new();

        // Encode map size
        buf.put_u32_le(self.what.len() as u32);

        // Encode each subscription
        for (name, item) in &self.what {
            // Encode name length and name
            buf.put_u32_le(name.len() as u32);
            buf.put_slice(name.as_bytes());
            tracing::info!(
                "  📝 Subscription: '{}' start={} flags={}",
                name,
                item.start,
                item.flags
            );

            // Encode subscribe item
            buf.put_u64_le(item.start);
            buf.put_u8(item.flags);
        }

        // Encode hostname (version 3)
        buf.put_u32_le(self.hostname.len() as u32);
        buf.put_slice(self.hostname.as_bytes());
        tracing::info!("  🖥️  Hostname: '{}'", self.hostname);

        let bytes = buf.freeze();

        Ok(bytes)
    }

    /// Decode from message payload
    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(
                "Incomplete MMonSubscribe".into(),
            ));
        }

        let count = data.get_u32_le() as usize;
        let mut what = HashMap::new();

        for _ in 0..count {
            if data.remaining() < 4 {
                return Err(MonClientError::DecodingError(
                    "Incomplete subscription entry".into(),
                ));
            }

            // Decode name
            let name_len = data.get_u32_le() as usize;
            if data.remaining() < name_len {
                return Err(MonClientError::DecodingError("Incomplete name".into()));
            }
            let name_bytes = &data[..name_len];
            let name = String::from_utf8(name_bytes.to_vec())
                .map_err(|e| MonClientError::DecodingError(format!("Invalid UTF-8: {}", e)))?;
            data.advance(name_len);

            // Decode subscribe item
            if data.remaining() < 9 {
                return Err(MonClientError::DecodingError(
                    "Incomplete subscribe item".into(),
                ));
            }
            let start = data.get_u64_le();
            let flags = data.get_u8();

            what.insert(name, SubscribeItem { start, flags });
        }

        // Decode hostname (version 3)
        let hostname = if data.remaining() >= 4 {
            let hostname_len = data.get_u32_le() as usize;
            if data.remaining() >= hostname_len {
                let hostname_bytes = &data[..hostname_len];
                String::from_utf8(hostname_bytes.to_vec())
                    .map_err(|e| MonClientError::DecodingError(format!("Invalid UTF-8: {}", e)))?
            } else {
                "unknown".to_string()
            }
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
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.interval);
        buf.put_slice(self.fsid.as_bytes());
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 20 {
            return Err(MonClientError::DecodingError(
                "Incomplete MMonSubscribeAck".into(),
            ));
        }

        let interval = data.get_u32_le();

        let mut fsid_bytes = [0u8; 16];
        data.copy_to_slice(&mut fsid_bytes);
        let fsid = Uuid::from_bytes(fsid_bytes);

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
        let mut buf = BytesMut::new();
        buf.put_u64_le(self.tid);
        buf.put_u32_le(self.what.len() as u32);
        buf.put_slice(self.what.as_bytes());
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
        if data.remaining() < 12 {
            return Err(MonClientError::DecodingError(
                "Incomplete MMonGetVersion".into(),
            ));
        }

        let tid = data.get_u64_le();
        let what_len = data.get_u32_le() as usize;

        if data.remaining() < what_len {
            return Err(MonClientError::DecodingError(
                "Incomplete what string".into(),
            ));
        }

        let what_bytes = &data[..what_len];
        let what = String::from_utf8(what_bytes.to_vec())
            .map_err(|e| MonClientError::DecodingError(format!("Invalid UTF-8: {}", e)))?;

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
        let mut buf = BytesMut::new();
        buf.put_u64_le(self.tid);
        buf.put_u64_le(self.version);
        buf.put_u64_le(self.oldest_version);
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 24 {
            return Err(MonClientError::DecodingError(
                "Incomplete MMonGetVersionReply".into(),
            ));
        }

        let tid = data.get_u64_le();
        let version = data.get_u64_le();
        let oldest_version = data.get_u64_le();

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
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.monmap_bl.len() as u32);
        buf.put_slice(&self.monmap_bl);
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError("Incomplete MMonMap".into()));
        }

        let len = data.get_u32_le() as usize;
        if data.remaining() < len {
            return Err(MonClientError::DecodingError(
                "Incomplete monmap data".into(),
            ));
        }

        let monmap_bl = Bytes::copy_from_slice(&data[..len]);
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
        // Decode fsid (16 bytes)
        if data.remaining() < 16 {
            return Err(MonClientError::DecodingError("Incomplete fsid".into()));
        }
        let mut fsid = [0u8; 16];
        data.copy_to_slice(&mut fsid);

        // Decode incremental_maps (map<epoch_t, buffer::list>)
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(
                "Incomplete incremental_maps count".into(),
            ));
        }
        let inc_count = data.get_u32_le();
        let mut incremental_maps = HashMap::new();
        for _ in 0..inc_count {
            if data.remaining() < 8 {
                return Err(MonClientError::DecodingError(
                    "Incomplete incremental_maps entry".into(),
                ));
            }
            let epoch = data.get_u32_le();
            let len = data.get_u32_le() as usize;
            if data.remaining() < len {
                return Err(MonClientError::DecodingError(
                    "Incomplete incremental_maps data".into(),
                ));
            }
            let map_data = Bytes::copy_from_slice(&data[..len]);
            data.advance(len);
            incremental_maps.insert(epoch, map_data);
        }

        // Decode maps (map<epoch_t, buffer::list>)
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(
                "Incomplete maps count".into(),
            ));
        }
        let maps_count = data.get_u32_le();
        let mut maps = HashMap::new();
        for _ in 0..maps_count {
            if data.remaining() < 8 {
                return Err(MonClientError::DecodingError(
                    "Incomplete maps entry".into(),
                ));
            }
            let epoch = data.get_u32_le();
            let len = data.get_u32_le() as usize;
            if data.remaining() < len {
                return Err(MonClientError::DecodingError("Incomplete maps data".into()));
            }
            let map_data = Bytes::copy_from_slice(&data[..len]);
            data.advance(len);
            maps.insert(epoch, map_data);
        }

        // Decode cluster_osdmap_trim_lower_bound and newest_map (version >= 2)
        let cluster_osdmap_trim_lower_bound = if data.remaining() >= 4 {
            data.get_u32_le()
        } else {
            0
        };

        let newest_map = if data.remaining() >= 4 {
            data.get_u32_le()
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
}

impl MMonCommand {
    pub fn new(_tid: u64, cmd: Vec<String>, _inbl: Bytes, fsid: UuidD) -> Self {
        Self {
            paxos: PaxosFields::new(),
            fsid,
            cmd,
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
        // Encode fsid (UUID) using Denc
        tracing::debug!(
            "MMonCommand::encode_message: fsid={}, bytes={:02x?}",
            self.fsid,
            self.fsid.bytes
        );
        self.fsid
            .encode(buf, 0)
            .map_err(|e| MonClientError::DecodingError(format!("Failed to encode fsid: {}", e)))?;
        tracing::debug!("MMonCommand::encode: fsid={}", self.fsid);

        // Encode command array
        buf.put_u32_le(self.cmd.len() as u32);
        for s in &self.cmd {
            buf.put_u32_le(s.len() as u32);
            buf.put_slice(s.as_bytes());
        }
        tracing::debug!("MMonCommand::encode: cmd={:?}", self.cmd);

        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        // Decode fsid (UUID) using Denc
        let fsid = UuidD::decode(data, 0)
            .map_err(|e| MonClientError::DecodingError(format!("Failed to decode fsid: {}", e)))?;

        // Decode command array
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(
                "Incomplete command count".into(),
            ));
        }
        let cmd_count = data.get_u32_le() as usize;
        let mut cmd = Vec::with_capacity(cmd_count);

        for _ in 0..cmd_count {
            if data.remaining() < 4 {
                return Err(MonClientError::DecodingError("Incomplete command".into()));
            }
            let len = data.get_u32_le() as usize;
            if data.remaining() < len {
                return Err(MonClientError::DecodingError(
                    "Incomplete command string".into(),
                ));
            }
            let s = String::from_utf8(data[..len].to_vec())
                .map_err(|e| MonClientError::DecodingError(format!("Invalid UTF-8: {}", e)))?;
            data.advance(len);
            cmd.push(s);
        }

        Ok(Self { paxos, fsid, cmd })
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
        // Encode r (errorcode32_t)
        buf.put_i32_le(self.r);

        // Encode rs string
        buf.put_u32_le(self.rs.len() as u32);
        buf.put_slice(self.rs.as_bytes());

        // Encode cmd array
        buf.put_u32_le(self.cmd.len() as u32);
        for s in &self.cmd {
            buf.put_u32_le(s.len() as u32);
            buf.put_slice(s.as_bytes());
        }

        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        tracing::debug!(
            "MMonCommandAck::decode: data.len()={}, first 32 bytes: {:02x?}",
            data.len(),
            &data[..data.len().min(32)]
        );

        // Decode r (errorcode32_t)
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(format!(
                "Incomplete r field: need 4 bytes, got {}",
                data.remaining()
            )));
        }
        let r = data.get_i32_le();

        tracing::debug!("Decoded r={}, remaining={}", r, data.remaining());

        // Decode rs string
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(format!(
                "Incomplete rs length: need 4 bytes, got {}",
                data.remaining()
            )));
        }
        let rs_len = data.get_u32_le() as usize;
        tracing::debug!("rs_len={}, remaining={}", rs_len, data.remaining());

        if data.remaining() < rs_len {
            return Err(MonClientError::DecodingError(format!(
                "Incomplete rs: need {} bytes, got {}",
                rs_len,
                data.remaining()
            )));
        }
        let rs = String::from_utf8(data[..rs_len].to_vec())
            .map_err(|e| MonClientError::DecodingError(format!("Invalid UTF-8: {}", e)))?;
        data.advance(rs_len);

        // Decode cmd array
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(format!(
                "Incomplete cmd count: need 4 bytes, got {}",
                data.remaining()
            )));
        }
        let cmd_count = data.get_u32_le() as usize;
        tracing::debug!("cmd_count={}, remaining={}", cmd_count, data.remaining());

        let mut cmd = Vec::with_capacity(cmd_count);
        for i in 0..cmd_count {
            if data.remaining() < 4 {
                return Err(MonClientError::DecodingError(format!(
                    "Incomplete cmd[{}] length",
                    i
                )));
            }
            let len = data.get_u32_le() as usize;
            if data.remaining() < len {
                return Err(MonClientError::DecodingError(format!(
                    "Incomplete cmd[{}] data",
                    i
                )));
            }
            let s = String::from_utf8(data[..len].to_vec()).map_err(|e| {
                MonClientError::DecodingError(format!("Invalid UTF-8 in cmd[{}]: {}", i, e))
            })?;
            data.advance(len);
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
    pub fn create_pool(fsid: [u8; 16], name: String, crush_rule: Option<i16>) -> Self {
        Self {
            paxos: PaxosFields::new(),
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
    ///
    /// # Note
    /// The `name` field is set to "delete" as a confirmation mechanism.
    /// This prevents accidental deletions - the monitor will only proceed
    /// with deletion if the name field is NOT a valid pool name.
    pub fn delete_pool(fsid: [u8; 16], pool: u32, _pool_name: String) -> Self {
        Self {
            paxos: PaxosFields::new(),
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
        // Encode fsid (UUID) using Denc
        self.fsid
            .encode(buf, 0)
            .map_err(|e| MonClientError::DecodingError(format!("Failed to encode fsid: {}", e)))?;

        // Encode pool
        buf.put_u32_le(self.pool);

        // Encode op
        buf.put_u32_le(self.op);

        // Encode old_auid (obsolete, always 0)
        buf.put_u64_le(0);

        // Encode snapid
        buf.put_u64_le(self.snapid);

        // Encode name
        buf.put_u32_le(self.name.len() as u32);
        buf.put_slice(self.name.as_bytes());

        // Encode pad (for v3->v4 encoding change)
        buf.put_u8(0);

        // Encode crush_rule
        buf.put_i16_le(self.crush_rule);

        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        // Decode fsid (UUID) using Denc
        let fsid = UuidD::decode(data, 0)
            .map_err(|e| MonClientError::DecodingError(format!("Failed to decode fsid: {}", e)))?;

        // Decode pool
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError("Incomplete pool".into()));
        }
        let pool = data.get_u32_le();

        // Decode op
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError("Incomplete op".into()));
        }
        let op = data.get_u32_le();

        // Decode old_auid (obsolete)
        if data.remaining() < 8 {
            return Err(MonClientError::DecodingError("Incomplete old_auid".into()));
        }
        let _old_auid = data.get_u64_le();

        // Decode snapid
        if data.remaining() < 8 {
            return Err(MonClientError::DecodingError("Incomplete snapid".into()));
        }
        let snapid = data.get_u64_le();

        // Decode name
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(
                "Incomplete name length".into(),
            ));
        }
        let name_len = data.get_u32_le() as usize;
        if data.remaining() < name_len {
            return Err(MonClientError::DecodingError("Incomplete name".into()));
        }
        let name = String::from_utf8(data[..name_len].to_vec())
            .map_err(|e| MonClientError::DecodingError(format!("Invalid UTF-8: {}", e)))?;
        data.advance(name_len);

        // Decode pad (for v3->v4 encoding change)
        if data.remaining() < 1 {
            return Err(MonClientError::DecodingError("Incomplete pad".into()));
        }
        let _pad = data.get_u8();

        // Decode crush_rule
        if data.remaining() < 2 {
            return Err(MonClientError::DecodingError(
                "Incomplete crush_rule".into(),
            ));
        }
        let crush_rule = data.get_i16_le();

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
        // Encode fsid (UUID) using Denc
        self.fsid
            .encode(buf, 0)
            .map_err(|e| MonClientError::DecodingError(format!("Failed to encode fsid: {}", e)))?;

        // Encode reply_code
        buf.put_u32_le(self.reply_code);

        // Encode epoch
        buf.put_u32_le(self.epoch);

        // Encode response_data (optional)
        if !self.response_data.is_empty() {
            buf.put_u8(1); // has_data = true
            buf.put_u32_le(self.response_data.len() as u32);
            buf.put_slice(&self.response_data);
        } else {
            buf.put_u8(0); // has_data = false
        }

        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        // Decode fsid (UUID) using Denc
        let fsid = UuidD::decode(data, 0)
            .map_err(|e| MonClientError::DecodingError(format!("Failed to decode fsid: {}", e)))?;

        // Decode reply_code
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(
                "Incomplete reply_code".into(),
            ));
        }
        let reply_code = data.get_u32_le();

        // Decode epoch
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError("Incomplete epoch".into()));
        }
        let epoch = data.get_u32_le();

        // Decode response_data (optional)
        if data.remaining() < 1 {
            return Err(MonClientError::DecodingError("Incomplete has_data".into()));
        }
        let has_data = data.get_u8();

        let response_data = if has_data != 0 {
            if data.remaining() < 4 {
                return Err(MonClientError::DecodingError(
                    "Incomplete response_data length".into(),
                ));
            }
            let len = data.get_u32_le() as usize;
            if data.remaining() < len {
                return Err(MonClientError::DecodingError(
                    "Incomplete response_data".into(),
                ));
            }
            let bytes = Bytes::copy_from_slice(&data[..len]);
            data.advance(len);
            bytes
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
