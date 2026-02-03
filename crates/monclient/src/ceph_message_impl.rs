//! Implementation of CephMessagePayload trait for Monitor message types
//!
//! This module implements the unified message encoding/decoding framework
//! for monitor protocol messages.

use crate::messages::*;
use crate::paxos_service_message::PaxosServiceMessage;
use bytes::Bytes;
use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
use msgr2::message::{CEPH_MSG_AUTH, CEPH_MSG_AUTH_REPLY};

/// MMonSubscribe message version
const MMON_SUBSCRIBE_VERSION: u16 = 3;

/// MMonSubscribeAck message version
const MMON_SUBSCRIBE_ACK_VERSION: u16 = 1;

/// MMonGetVersion message version
const MMON_GET_VERSION_VERSION: u16 = 1;

/// MMonGetVersionReply message version
const MMON_GET_VERSION_REPLY_VERSION: u16 = 1;

/// MMonMap message version
const MMON_MAP_VERSION: u16 = 1;

/// MOSDMap message version
const MOSD_MAP_VERSION: u16 = 1;

/// MMonCommand message version
const MMON_COMMAND_VERSION: u16 = 1;

/// MMonCommandAck message version
const MMON_COMMAND_ACK_VERSION: u16 = 1;

impl CephMessagePayload for MMonSubscribe {
    fn msg_type() -> u16 {
        CEPH_MSG_MON_SUBSCRIBE
    }

    fn msg_version() -> u16 {
        MMON_SUBSCRIBE_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        self.encode().map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        Self::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MMonSubscribe decode failed".into()))
    }
}

impl CephMessagePayload for MMonSubscribeAck {
    fn msg_type() -> u16 {
        CEPH_MSG_MON_SUBSCRIBE_ACK
    }

    fn msg_version() -> u16 {
        MMON_SUBSCRIBE_ACK_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        self.encode().map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        Self::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MMonSubscribeAck decode failed".into()))
    }
}

impl CephMessagePayload for MMonGetVersion {
    fn msg_type() -> u16 {
        CEPH_MSG_MON_GET_VERSION
    }

    fn msg_version() -> u16 {
        MMON_GET_VERSION_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        self.encode().map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        Self::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MMonGetVersion decode failed".into()))
    }
}

impl CephMessagePayload for MMonGetVersionReply {
    fn msg_type() -> u16 {
        CEPH_MSG_MON_GET_VERSION_REPLY
    }

    fn msg_version() -> u16 {
        MMON_GET_VERSION_REPLY_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        self.encode().map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        Self::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MMonGetVersionReply decode failed".into()))
    }
}

impl CephMessagePayload for MMonMap {
    fn msg_type() -> u16 {
        msgr2::message::CEPH_MSG_MON_MAP
    }

    fn msg_version() -> u16 {
        MMON_MAP_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        self.encode().map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        Self::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MMonMap decode failed".into()))
    }
}

impl CephMessagePayload for MOSDMap {
    fn msg_type() -> u16 {
        msgr2::message::CEPH_MSG_OSD_MAP
    }

    fn msg_version() -> u16 {
        MOSD_MAP_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        // MOSDMap encoding not implemented - typically only needed on server side
        Err(msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        Self::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MOSDMap decode failed".into()))
    }
}

impl CephMessagePayload for MMonCommand {
    fn msg_type() -> u16 {
        msgr2::message::CEPH_MSG_MON_COMMAND
    }

    fn msg_version() -> u16 {
        MMON_COMMAND_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        PaxosServiceMessage::encode(self).map_err(|_e| msgr2::Error::Serialization)
    }

    fn encode_data(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        Ok(self.inbl.clone())
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        let mut cmd: MMonCommand = PaxosServiceMessage::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MMonCommand decode failed".into()))?;
        cmd.inbl = Bytes::copy_from_slice(data);
        Ok(cmd)
    }
}

impl CephMessagePayload for MMonCommandAck {
    fn msg_type() -> u16 {
        msgr2::message::CEPH_MSG_MON_COMMAND_ACK
    }

    fn msg_version() -> u16 {
        MMON_COMMAND_ACK_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        PaxosServiceMessage::encode(self).map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        PaxosServiceMessage::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MMonCommandAck decode failed".into()))
    }
}

/// MPoolOp message version (HEAD_VERSION = 4, COMPAT_VERSION = 2)
const MPOOL_OP_VERSION: u16 = 4;

/// MPoolOpReply message version
const MPOOL_OP_REPLY_VERSION: u16 = 1;

impl CephMessagePayload for MPoolOp {
    fn msg_type() -> u16 {
        msgr2::message::CEPH_MSG_POOLOP
    }

    fn msg_version() -> u16 {
        MPOOL_OP_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        PaxosServiceMessage::encode(self).map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        PaxosServiceMessage::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MPoolOp decode failed".into()))
    }
}

impl CephMessagePayload for MPoolOpReply {
    fn msg_type() -> u16 {
        msgr2::message::CEPH_MSG_POOLOP_REPLY
    }

    fn msg_version() -> u16 {
        MPOOL_OP_REPLY_VERSION
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        PaxosServiceMessage::encode(self).map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        PaxosServiceMessage::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MPoolOpReply decode failed".into()))
    }
}

// MAuth implementation
impl CephMessagePayload for MAuth {
    fn msg_type() -> u16 {
        CEPH_MSG_AUTH
    }

    fn msg_version() -> u16 {
        1
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        PaxosServiceMessage::encode(self).map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        PaxosServiceMessage::decode(front)
            .map_err(|_e| msgr2::Error::Deserialization("MAuth decode failed".into()))
    }
}

// MAuthReply implementation
impl CephMessagePayload for MAuthReply {
    fn msg_type() -> u16 {
        CEPH_MSG_AUTH_REPLY
    }

    fn msg_version() -> u16 {
        1
    }

    fn encode_payload(&self, _features: u64) -> std::result::Result<Bytes, msgr2::Error> {
        self.encode().map_err(|_e| msgr2::Error::Serialization)
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> std::result::Result<Self, msgr2::Error> {
        let mut data = front;
        Self::decode(&mut data)
            .map_err(|_e| msgr2::Error::Deserialization("MAuthReply decode failed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::SubscribeItem;
    use msgr2::ceph_message::{CephMessage, CrcFlags};

    #[test]
    fn test_mmon_subscribe_message_encoding() {
        let mut subscribe = MMonSubscribe::new();
        subscribe.add("osdmap".to_string(), SubscribeItem { start: 0, flags: 0 });

        // Create a complete message
        let msg = CephMessage::from_payload(&subscribe, 0, CrcFlags::ALL).unwrap();

        // Verify message structure
        // Copy packed struct fields to local variables to avoid unaligned references
        let msg_type = msg.header.msg_type;
        let version = msg.header.version;
        assert_eq!(msg_type, CEPH_MSG_MON_SUBSCRIBE);
        assert_eq!(version, MMON_SUBSCRIBE_VERSION);
        assert!(!msg.front.is_empty());
        assert_eq!(msg.middle.len(), 0);
        assert_eq!(msg.data.len(), 0);

        // Encode to bytes
        let encoded = msg.encode().unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_mmon_get_version_message_encoding() {
        let get_version = MMonGetVersion::new(1, "osdmap".to_string());

        // Create a complete message
        let msg = CephMessage::from_payload(&get_version, 0, CrcFlags::ALL).unwrap();

        // Verify message structure
        // Copy packed struct fields to local variables to avoid unaligned references
        let msg_type = msg.header.msg_type;
        let version = msg.header.version;
        assert_eq!(msg_type, CEPH_MSG_MON_GET_VERSION);
        assert_eq!(version, MMON_GET_VERSION_VERSION);
        assert!(!msg.front.is_empty());
    }
}
