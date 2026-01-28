//! Implementation of CephMessagePayload trait for OSD message types
//!
//! This module implements the unified message encoding/decoding framework
//! for MOSDOp and MOSDOpReply messages.

use crate::messages::{MOSDOp, MOSDOpReply, CEPH_MSG_OSD_OP, CEPH_MSG_OSD_OPREPLY};
use bytes::Bytes;
use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};

#[cfg(test)]
use crate::types::OpData;

/// MOSDOp message version (from MOSDOp.h HEAD_VERSION)
const MOSDOP_VERSION: u16 = 9;

/// MOSDOp compat version (from MOSDOp.h COMPAT_VERSION)
const MOSDOP_COMPAT_VERSION: u16 = 3;

/// MOSDOpReply message version (from MOSDOpReply.h HEAD_VERSION)
const MOSDOPREPLY_VERSION: u16 = 8;

impl CephMessagePayload for MOSDOp {
    fn msg_type() -> u16 {
        CEPH_MSG_OSD_OP
    }

    fn msg_version() -> u16 {
        MOSDOP_VERSION
    }

    fn msg_compat_version() -> u16 {
        MOSDOP_COMPAT_VERSION
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
        MOSDOPREPLY_VERSION
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
        Self::decode_internal(front, data)
            .map_err(|_e| msgr2::Error::Deserialization("MOSDOpReply decode failed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OSDOp, ObjectId, OpCode, RequestId, StripedPgId};
    use msgr2::ceph_message::{CephMessage, CrcFlags};

    #[test]
    fn test_mosdop_message_encoding() {
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
            op: OpCode::Read,
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

        // Create a complete message
        let msg = CephMessage::from_payload(&mosdop, 0, CrcFlags::ALL).unwrap();

        // Verify message structure
        assert_eq!(msg.header.msg_type, CEPH_MSG_OSD_OP);
        assert_eq!(msg.header.version, MOSDOP_VERSION);
        assert!(!msg.front.is_empty());
        assert_eq!(msg.middle.len(), 0);
        assert_eq!(msg.data.len(), 0); // No data for read operation

        // Encode to bytes
        let encoded = msg.encode().unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_mosdop_with_write_data() {
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
            op: OpCode::Write,
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

        // Create a complete message
        let msg = CephMessage::from_payload(&mosdop, 0, CrcFlags::ALL).unwrap();

        // Verify data section contains write data
        assert_eq!(msg.data.len(), 1024);
        assert_eq!(msg.data, write_data);
    }
}
