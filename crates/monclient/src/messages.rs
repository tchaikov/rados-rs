//! Monitor protocol messages
//!
//! Message types and encoding/decoding for monitor communication.

use crate::error::{MonClientError, Result};
use crate::paxos_service_message::{PaxosFields, PaxosServiceMessage};
use crate::subscription::{MonService, SubscribeItem};
use bytes::{Buf, Bytes, BytesMut};
use denc::{Denc, UuidD};
use std::collections::HashMap;

// Message type constants
pub const CEPH_MSG_MON_SUBSCRIBE: u16 = 0x000f;
pub const CEPH_MSG_MON_SUBSCRIBE_ACK: u16 = 0x0010;
pub const CEPH_MSG_OSD_MAP: u16 = msgr2::message::CEPH_MSG_OSD_MAP;
pub const CEPH_MSG_MON_GET_VERSION: u16 = 0x0013;
pub const CEPH_MSG_MON_GET_VERSION_REPLY: u16 = 0x0014;

// Pool operation constants
pub const POOL_OP_CREATE: u32 = 0x01;
pub const POOL_OP_DELETE: u32 = 0x02;
pub const POOL_OP_AUID_CHANGE: u32 = 0x03;
pub const POOL_OP_CREATE_SNAP: u32 = 0x11;
pub const POOL_OP_DELETE_SNAP: u32 = 0x12;
pub const POOL_OP_CREATE_UNMANAGED_SNAP: u32 = 0x21;
pub const POOL_OP_DELETE_UNMANAGED_SNAP: u32 = 0x22;

/// Implement CephMessagePayload for a type that derives Denc.
///
/// This macro eliminates the boilerplate encode_payload/decode_payload
/// implementations that are identical across all simple Denc-derived messages.
///
/// # Variants
///
/// - `($type:ty, $msg_type:expr, $version:expr)` - version == compat_version
/// - `($type:ty, $msg_type:expr, $version:expr, $compat:expr)` - explicit compat_version
macro_rules! impl_denc_ceph_message {
    ($type:ty, $msg_type:expr, $version:expr) => {
        impl msgr2::ceph_message::CephMessagePayload for $type {
            fn msg_type() -> u16 {
                $msg_type
            }

            fn msg_version(_features: u64) -> u16 {
                $version
            }

            fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
                let size = self.encoded_size(0).unwrap_or(256);
                let mut buf = BytesMut::with_capacity(size);
                self.encode(&mut buf, 0)?;
                Ok(buf.freeze())
            }

            fn decode_payload(
                _header: &msgr2::ceph_message::CephMsgHeader,
                front: &[u8],
                _middle: &[u8],
                _data: &[u8],
            ) -> std::result::Result<Self, msgr2::Error> {
                let mut data = front;
                Self::decode(&mut data, 0).map_err(|_| {
                    msgr2::Error::Deserialization(
                        concat!(stringify!($type), " decode failed").into(),
                    )
                })
            }
        }
    };
    ($type:ty, $msg_type:expr, $version:expr, $compat:expr) => {
        impl msgr2::ceph_message::CephMessagePayload for $type {
            fn msg_type() -> u16 {
                $msg_type
            }

            fn msg_version(_features: u64) -> u16 {
                $version
            }

            fn msg_compat_version(_features: u64) -> u16 {
                $compat
            }

            fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
                let size = self.encoded_size(0).unwrap_or(256);
                let mut buf = BytesMut::with_capacity(size);
                self.encode(&mut buf, 0)?;
                Ok(buf.freeze())
            }

            fn decode_payload(
                _header: &msgr2::ceph_message::CephMsgHeader,
                front: &[u8],
                _middle: &[u8],
                _data: &[u8],
            ) -> std::result::Result<Self, msgr2::Error> {
                let mut data = front;
                Self::decode(&mut data, 0).map_err(|_| {
                    msgr2::Error::Deserialization(
                        concat!(stringify!($type), " decode failed").into(),
                    )
                })
            }
        }
    };
}

/// Implement CephMessagePayload for a PaxosServiceMessage type.
///
/// These messages delegate encoding/decoding to the PaxosServiceMessage trait,
/// which handles the common paxos fields prefix.
///
/// # Variants
///
/// - `($type:ty, $msg_type:expr, $version:expr)` - version == compat_version
/// - `($type:ty, $msg_type:expr, $version:expr, $compat:expr)` - explicit compat_version
macro_rules! impl_paxos_ceph_message {
    ($type:ty, $msg_type:expr, $version:expr) => {
        impl msgr2::ceph_message::CephMessagePayload for $type {
            fn msg_type() -> u16 {
                $msg_type
            }

            fn msg_version(_features: u64) -> u16 {
                $version
            }

            fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
                PaxosServiceMessage::encode(self).map_err(|_| msgr2::Error::Serialization)
            }

            fn decode_payload(
                _header: &msgr2::ceph_message::CephMsgHeader,
                front: &[u8],
                _middle: &[u8],
                _data: &[u8],
            ) -> std::result::Result<Self, msgr2::Error> {
                PaxosServiceMessage::decode(front).map_err(|_| {
                    msgr2::Error::Deserialization(
                        concat!(stringify!($type), " decode failed").into(),
                    )
                })
            }
        }
    };
    ($type:ty, $msg_type:expr, $version:expr, $compat:expr) => {
        impl msgr2::ceph_message::CephMessagePayload for $type {
            fn msg_type() -> u16 {
                $msg_type
            }

            fn msg_version(_features: u64) -> u16 {
                $version
            }

            fn msg_compat_version(_features: u64) -> u16 {
                $compat
            }

            fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
                PaxosServiceMessage::encode(self).map_err(|_| msgr2::Error::Serialization)
            }

            fn decode_payload(
                _header: &msgr2::ceph_message::CephMsgHeader,
                front: &[u8],
                _middle: &[u8],
                _data: &[u8],
            ) -> std::result::Result<Self, msgr2::Error> {
                PaxosServiceMessage::decode(front).map_err(|_| {
                    msgr2::Error::Deserialization(
                        concat!(stringify!($type), " decode failed").into(),
                    )
                })
            }
        }
    };
}

/// MMonSubscribe - Subscribe to cluster maps
#[derive(Debug, Clone, denc::Denc)]
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

    pub fn add(&mut self, service: MonService, item: SubscribeItem) {
        self.what.insert(service.as_str().to_string(), item);
    }
}

impl Default for MMonSubscribe {
    fn default() -> Self {
        Self::new()
    }
}

impl_denc_ceph_message!(MMonSubscribe, CEPH_MSG_MON_SUBSCRIBE, 3, 1);

/// MMonSubscribeAck - Acknowledgment of subscription
#[derive(Debug, Clone, denc::Denc)]
pub struct MMonSubscribeAck {
    pub interval: u32,
    pub fsid: UuidD,
}

impl MMonSubscribeAck {
    pub fn new(interval: u32, fsid: UuidD) -> Self {
        Self { interval, fsid }
    }
}

impl_denc_ceph_message!(MMonSubscribeAck, CEPH_MSG_MON_SUBSCRIBE_ACK, 1);

/// MMonGetVersion - Query map version
#[derive(Debug, Clone, denc::Denc)]
pub struct MMonGetVersion {
    pub tid: u64,
    pub what: String,
}

impl MMonGetVersion {
    pub fn new(tid: u64, service: MonService) -> Self {
        Self {
            tid,
            what: service.as_str().to_string(),
        }
    }
}

impl_denc_ceph_message!(MMonGetVersion, CEPH_MSG_MON_GET_VERSION, 1);

/// MMonGetVersionReply - Version query response
#[derive(Debug, Clone, denc::Denc)]
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
}

impl_denc_ceph_message!(MMonGetVersionReply, CEPH_MSG_MON_GET_VERSION_REPLY, 1);

/// MMonMap - Monitor map update
#[derive(Debug, Clone, denc::Denc)]
pub struct MMonMap {
    pub monmap_bl: Bytes,
}

impl MMonMap {
    pub fn new(monmap_bl: Bytes) -> Self {
        Self { monmap_bl }
    }
}

impl_denc_ceph_message!(MMonMap, msgr2::message::CEPH_MSG_MON_MAP, 1);

/// MConfig - Runtime configuration update
#[derive(Debug, Clone, denc::Denc)]
pub struct MConfig {
    pub config: HashMap<String, String>,
}

impl MConfig {
    pub fn new(config: HashMap<String, String>) -> Self {
        Self { config }
    }
}

impl_denc_ceph_message!(MConfig, msgr2::message::CEPH_MSG_CONFIG, 1);

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
    /// Message version (from MOSDMap.h HEAD_VERSION)
    const VERSION: u16 = 4;
    /// Compatibility version (from MOSDMap.h COMPAT_VERSION)
    const COMPAT_VERSION: u16 = 3;

    /// Get the first (oldest) epoch in this message
    pub fn get_first(&self) -> u32 {
        self.incremental_maps
            .keys()
            .chain(self.maps.keys())
            .copied()
            .min()
            .unwrap_or(0)
    }

    /// Get the last (newest) epoch in this message
    pub fn get_last(&self) -> u32 {
        self.newest_map
    }
}

/// Implement MapMessage for MOSDMap to enable type-safe routing
impl msgr2::MapMessage for MOSDMap {
    const MSG_TYPE: u16 = CEPH_MSG_OSD_MAP;
    const NAME: &'static str = "MOSDMap";
}

impl msgr2::ceph_message::CephMessagePayload for MOSDMap {
    fn msg_type() -> u16 {
        msgr2::message::CEPH_MSG_OSD_MAP
    }

    fn msg_version(_features: u64) -> u16 {
        Self::VERSION
    }

    fn msg_compat_version(_features: u64) -> u16 {
        Self::COMPAT_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        // MOSDMap encoding not implemented - typically only needed on server side
        Err(msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &msgr2::ceph_message::CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        let mut data = front;
        let fsid_denc = UuidD::decode(&mut data, 0)?;
        let fsid = fsid_denc.bytes;

        let inc_count = u32::decode(&mut data, 0)? as usize;
        let mut incremental_maps = HashMap::new();
        for _ in 0..inc_count {
            let epoch = u32::decode(&mut data, 0)?;
            let map_data = Bytes::decode(&mut data, 0)?;
            incremental_maps.insert(epoch, map_data);
        }

        let maps_count = u32::decode(&mut data, 0)? as usize;
        let mut maps = HashMap::new();
        for _ in 0..maps_count {
            let epoch = u32::decode(&mut data, 0)?;
            let map_data = Bytes::decode(&mut data, 0)?;
            maps.insert(epoch, map_data);
        }

        // Optional fields (version >= 2)
        let cluster_osdmap_trim_lower_bound = if data.remaining() >= std::mem::size_of::<u32>() {
            u32::decode(&mut data, 0)?
        } else {
            0
        };

        let newest_map = if data.remaining() >= std::mem::size_of::<u32>() {
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
    pub fn new(cmd: Vec<String>, inbl: Bytes, fsid: UuidD) -> Self {
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
        self.fsid.encode(buf, 0)?;
        (self.cmd.len() as u32).encode(buf, 0)?;
        for s in &self.cmd {
            s.encode(buf, 0)?;
        }
        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        let fsid = UuidD::decode(data, 0)?;
        let cmd_count = u32::decode(data, 0)? as usize;
        let cmd = (0..cmd_count)
            .map(|_| String::decode(data, 0).map_err(MonClientError::from))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            paxos,
            fsid,
            cmd,
            inbl: Bytes::new(), // Filled by CephMessagePayload::decode_payload
        })
    }
}

/// MMonCommand has a custom CephMessagePayload because it uses the data segment
/// for `inbl` (via encode_data/decode with data slice).
impl msgr2::ceph_message::CephMessagePayload for MMonCommand {
    fn msg_type() -> u16 {
        msgr2::message::CEPH_MSG_MON_COMMAND
    }

    fn msg_version(_features: u64) -> u16 {
        1
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        PaxosServiceMessage::encode(self).map_err(|_| msgr2::Error::Serialization)
    }

    fn encode_data(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        Ok(self.inbl.clone())
    }

    fn decode_payload(
        _header: &msgr2::ceph_message::CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        let mut cmd: MMonCommand = PaxosServiceMessage::decode(front)
            .map_err(|_| msgr2::Error::Deserialization("MMonCommand decode failed".into()))?;
        cmd.inbl = Bytes::copy_from_slice(data);
        Ok(cmd)
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
    pub fn new(retval: i32, outs: String) -> Self {
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
        self.r.encode(buf, 0)?;
        self.rs.encode(buf, 0)?;
        (self.cmd.len() as u32).encode(buf, 0)?;
        for s in &self.cmd {
            s.encode(buf, 0)?;
        }
        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        let r = i32::decode(data, 0)?;
        let rs = String::decode(data, 0)?;
        let cmd_count = u32::decode(data, 0)? as usize;
        let cmd = (0..cmd_count)
            .map(|_| String::decode(data, 0).map_err(MonClientError::from))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { paxos, r, rs, cmd })
    }
}

impl_paxos_ceph_message!(MMonCommandAck, msgr2::message::CEPH_MSG_MON_COMMAND_ACK, 1);

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
    /// Obsolete auid field, always encoded as 0 (removed in Ceph v12)
    const OLD_AUID_OBSOLETE: u64 = 0;
    /// Padding byte for v3→v4 encoding compatibility
    const V3_V4_PAD: u8 = 0;

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

    /// Create a pool snapshot creation message
    ///
    /// # Arguments
    /// * `fsid` - Cluster FSID
    /// * `pool` - Pool ID to snapshot
    /// * `name` - Snapshot name
    /// * `version` - Current OSDMap epoch (used by paxos versioning)
    pub fn create_snap(fsid: [u8; 16], pool: u32, name: String, version: u64) -> Self {
        Self {
            paxos: PaxosFields::with_version(version),
            fsid: UuidD::from_bytes(fsid),
            pool,
            name,
            op: POOL_OP_CREATE_SNAP,
            snapid: 0,
            crush_rule: 0,
        }
    }

    /// Create a pool snapshot deletion message
    ///
    /// # Arguments
    /// * `fsid` - Cluster FSID
    /// * `pool` - Pool ID
    /// * `snapid` - Snapshot ID to delete
    /// * `name` - Snapshot name
    /// * `version` - Current OSDMap epoch (used by paxos versioning)
    pub fn delete_snap(fsid: [u8; 16], pool: u32, snapid: u64, name: String, version: u64) -> Self {
        Self {
            paxos: PaxosFields::with_version(version),
            fsid: UuidD::from_bytes(fsid),
            pool,
            name,
            op: POOL_OP_DELETE_SNAP,
            snapid,
            crush_rule: 0,
        }
    }

    /// Create a pool deletion message
    ///
    /// # Arguments
    /// * `fsid` - Cluster FSID
    /// * `pool` - Pool ID to delete
    /// * `version` - Current OSDMap epoch (used by paxos versioning)
    ///
    /// # Note
    /// The `name` field is set to "delete" as a confirmation mechanism.
    /// This prevents accidental deletions - the monitor will only proceed
    /// with deletion if the name field is NOT a valid pool name.
    pub fn delete_pool(fsid: [u8; 16], pool: u32, version: u64) -> Self {
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
        self.fsid.encode(buf, 0)?;
        self.pool.encode(buf, 0)?;
        self.op.encode(buf, 0)?;
        Self::OLD_AUID_OBSOLETE.encode(buf, 0)?;
        self.snapid.encode(buf, 0)?;
        self.name.encode(buf, 0)?;
        Self::V3_V4_PAD.encode(buf, 0)?;
        self.crush_rule.encode(buf, 0)?;
        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        let fsid = UuidD::decode(data, 0)?;
        let pool = u32::decode(data, 0)?;
        let op = u32::decode(data, 0)?;
        let _old_auid = u64::decode(data, 0)?; // obsolete field
        let snapid = u64::decode(data, 0)?;
        let name = String::decode(data, 0)?;
        let _pad = u8::decode(data, 0)?; // v3->v4 pad
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

impl_paxos_ceph_message!(MPoolOp, msgr2::message::CEPH_MSG_POOLOP, 4, 2);

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
        self.fsid.encode(buf, 0)?;
        self.reply_code.encode(buf, 0)?;
        self.epoch.encode(buf, 0)?;
        if !self.response_data.is_empty() {
            1u8.encode(buf, 0)?;
            self.response_data.encode(buf, 0)?;
        } else {
            0u8.encode(buf, 0)?;
        }
        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
        let fsid = UuidD::decode(data, 0)?;
        let reply_code = u32::decode(data, 0)?;
        let epoch = u32::decode(data, 0)?;
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

impl_paxos_ceph_message!(MPoolOpReply, msgr2::message::CEPH_MSG_POOLOP_REPLY, 1);

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
        self.protocol.encode(buf, 0)?;
        self.auth_payload.encode(buf, 0)?;
        self.monmap_epoch.encode(buf, 0)?;
        Ok(())
    }

    fn decode_message(paxos: PaxosFields, data: &mut &[u8]) -> Result<Self> {
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

impl_paxos_ceph_message!(MAuth, msgr2::message::CEPH_MSG_AUTH, 1);

/// MAuthReply - Authentication reply message
///
/// This message is sent by the monitor in response to MAuth.
/// Note: This is NOT a PaxosServiceMessage (it inherits from Message in C++).
/// Field order matches wire encoding: auth_payload is encoded before result_msg
/// (see C++ MAuthReply.h:50-57).
#[derive(Debug, Clone, denc::Denc)]
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
}

impl_denc_ceph_message!(MAuthReply, msgr2::message::CEPH_MSG_AUTH_REPLY, 1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_encode_decode() {
        use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};

        let mut msg = MMonSubscribe::new();
        msg.add(
            MonService::OsdMap,
            SubscribeItem {
                start: 10,
                flags: 0,
            },
        );
        msg.add(MonService::MonMap, SubscribeItem { start: 5, flags: 1 });

        let encoded = msg.encode_payload(0).unwrap();
        let header = CephMsgHeader::new(MMonSubscribe::msg_type(), MMonSubscribe::msg_version(0));
        let decoded = MMonSubscribe::decode_payload(&header, &encoded, &[], &[]).unwrap();

        assert_eq!(decoded.what.len(), 2);
        assert_eq!(decoded.what.get("osdmap").unwrap().start, 10);
        assert_eq!(decoded.what.get("monmap").unwrap().flags, 1);
    }

    #[test]
    fn test_version_reply_encode_decode() {
        use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};

        let msg = MMonGetVersionReply::new(42, 100, 50);
        let encoded = msg.encode_payload(0).unwrap();
        let header = CephMsgHeader::new(
            MMonGetVersionReply::msg_type(),
            MMonGetVersionReply::msg_version(0),
        );
        let decoded = MMonGetVersionReply::decode_payload(&header, &encoded, &[], &[]).unwrap();

        assert_eq!(decoded.tid, 42);
        assert_eq!(decoded.version, 100);
        assert_eq!(decoded.oldest_version, 50);
    }

    #[test]
    fn test_config_encode_decode() {
        use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};

        let mut config = HashMap::new();
        config.insert("mon_client_hunt_interval".to_string(), "3".to_string());
        config.insert("rados_mon_op_timeout".to_string(), "60".to_string());
        let msg = MConfig::new(config);

        let encoded = msg.encode_payload(0).unwrap();
        let header = CephMsgHeader::new(MConfig::msg_type(), MConfig::msg_version(0));
        let decoded = MConfig::decode_payload(&header, &encoded, &[], &[]).unwrap();

        assert_eq!(decoded.config.len(), 2);
        assert_eq!(
            decoded.config.get("mon_client_hunt_interval"),
            Some(&"3".to_string())
        );
        assert_eq!(
            decoded.config.get("rados_mon_op_timeout"),
            Some(&"60".to_string())
        );
    }

    #[test]
    fn test_mconfig_message_encoding() {
        use msgr2::ceph_message::{CephMessage, CephMessagePayload, CrcFlags};

        let mut config = HashMap::new();
        config.insert("mon_client_hunt_interval".to_string(), "3".to_string());
        let msg = MConfig::new(config);

        // Create a complete message
        let ceph_msg = CephMessage::from_payload(&msg, 0, CrcFlags::ALL).unwrap();

        // Verify message structure
        let msg_type = ceph_msg.header.msg_type;
        let version = ceph_msg.header.version;
        assert_eq!(msg_type, msgr2::message::CEPH_MSG_CONFIG);
        assert_eq!(version, MConfig::msg_version(0));
        assert!(!ceph_msg.front.is_empty());
        assert_eq!(ceph_msg.middle.len(), 0);
        assert_eq!(ceph_msg.data.len(), 0);

        // Round-trip: decode the payload back and verify
        let decoded = MConfig::decode_payload(
            &ceph_msg.header,
            &ceph_msg.front,
            &ceph_msg.middle,
            &ceph_msg.data,
        )
        .unwrap();
        assert_eq!(decoded.config.len(), 1);
        assert_eq!(
            decoded.config.get("mon_client_hunt_interval"),
            Some(&"3".to_string())
        );
    }

    #[test]
    fn test_mmon_subscribe_message_encoding() {
        use msgr2::ceph_message::{CephMessage, CephMessagePayload, CrcFlags};

        let mut subscribe = MMonSubscribe::new();
        subscribe.add(MonService::OsdMap, SubscribeItem { start: 0, flags: 0 });

        // Create a complete message
        let msg = CephMessage::from_payload(&subscribe, 0, CrcFlags::ALL).unwrap();

        // Verify message structure
        // Copy packed struct fields to local variables to avoid unaligned references
        let msg_type = msg.header.msg_type;
        let version = msg.header.version;
        assert_eq!(msg_type, CEPH_MSG_MON_SUBSCRIBE);
        assert_eq!(version, MMonSubscribe::msg_version(0));
        assert!(!msg.front.is_empty());
        assert_eq!(msg.middle.len(), 0);
        assert_eq!(msg.data.len(), 0);

        // Encode to bytes
        let encoded = msg.encode().unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_mmon_get_version_message_encoding() {
        use msgr2::ceph_message::{CephMessage, CephMessagePayload, CrcFlags};

        let get_version = MMonGetVersion::new(1, MonService::OsdMap);

        // Create a complete message
        let msg = CephMessage::from_payload(&get_version, 0, CrcFlags::ALL).unwrap();

        // Verify message structure
        // Copy packed struct fields to local variables to avoid unaligned references
        let msg_type = msg.header.msg_type;
        let version = msg.header.version;
        assert_eq!(msg_type, CEPH_MSG_MON_GET_VERSION);
        assert_eq!(version, MMonGetVersion::msg_version(0));
        assert!(!msg.front.is_empty());
    }
}
