//! Monitor protocol messages
//!
//! Message types and encoding/decoding for monitor communication.

use crate::error::{MonClientError, Result};
use crate::subscription::SubscribeItem;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use uuid::Uuid;

// Message type constants (add to msgr2::message)
pub const CEPH_MSG_MON_SUBSCRIBE: u16 = 0x000f;
pub const CEPH_MSG_MON_SUBSCRIBE_ACK: u16 = 0x0010;
pub const CEPH_MSG_OSD_MAP: u16 = 0x0029;
pub const CEPH_MSG_MON_GET_VERSION: u16 = 0x0052;
pub const CEPH_MSG_MON_GET_VERSION_REPLY: u16 = 0x0053;

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
        Ok(buf.freeze())
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
}

/// MMonCommand - Execute command on monitor
#[derive(Debug, Clone)]
pub struct MMonCommand {
    pub tid: u64,
    pub cmd: Vec<String>,
    pub inbl: Bytes,
}

impl MMonCommand {
    pub fn new(tid: u64, cmd: Vec<String>, inbl: Bytes) -> Self {
        Self { tid, cmd, inbl }
    }

    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        // Encode tid
        buf.put_u64_le(self.tid);

        // Encode command array
        buf.put_u32_le(self.cmd.len() as u32);
        for s in &self.cmd {
            buf.put_u32_le(s.len() as u32);
            buf.put_slice(s.as_bytes());
        }

        // Encode input buffer
        buf.put_u32_le(self.inbl.len() as u32);
        buf.put_slice(&self.inbl);

        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 12 {
            return Err(MonClientError::DecodingError(
                "Incomplete MMonCommand".into(),
            ));
        }

        let tid = data.get_u64_le();

        // Decode command array
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

        // Decode input buffer
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError("Incomplete inbl".into()));
        }
        let inbl_len = data.get_u32_le() as usize;
        if data.remaining() < inbl_len {
            return Err(MonClientError::DecodingError("Incomplete inbl data".into()));
        }
        let inbl = Bytes::copy_from_slice(&data[..inbl_len]);

        Ok(Self { tid, cmd, inbl })
    }
}

/// MMonCommandAck - Command execution result
#[derive(Debug, Clone)]
pub struct MMonCommandAck {
    pub tid: u64,
    pub retval: i32,
    pub outs: String,
    pub outbl: Bytes,
}

impl MMonCommandAck {
    pub fn new(tid: u64, retval: i32, outs: String, outbl: Bytes) -> Self {
        Self {
            tid,
            retval,
            outs,
            outbl,
        }
    }

    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_u64_le(self.tid);
        buf.put_i32_le(self.retval);
        buf.put_u32_le(self.outs.len() as u32);
        buf.put_slice(self.outs.as_bytes());
        buf.put_u32_le(self.outbl.len() as u32);
        buf.put_slice(&self.outbl);
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 16 {
            return Err(MonClientError::DecodingError(
                "Incomplete MMonCommandAck".into(),
            ));
        }

        let tid = data.get_u64_le();
        let retval = data.get_i32_le();

        // Decode outs
        let outs_len = data.get_u32_le() as usize;
        if data.remaining() < outs_len {
            return Err(MonClientError::DecodingError("Incomplete outs".into()));
        }
        let outs = String::from_utf8(data[..outs_len].to_vec())
            .map_err(|e| MonClientError::DecodingError(format!("Invalid UTF-8: {}", e)))?;
        data.advance(outs_len);

        // Decode outbl
        if data.remaining() < 4 {
            return Err(MonClientError::DecodingError(
                "Incomplete outbl length".into(),
            ));
        }
        let outbl_len = data.get_u32_le() as usize;
        if data.remaining() < outbl_len {
            return Err(MonClientError::DecodingError(
                "Incomplete outbl data".into(),
            ));
        }
        let outbl = Bytes::copy_from_slice(&data[..outbl_len]);

        Ok(Self {
            tid,
            retval,
            outs,
            outbl,
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
