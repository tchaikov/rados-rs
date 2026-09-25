//! `LIST_WATCHERS`: the clients watching an object.
//!
//! The OSD answers with `obj_list_watch_response_t` from
//! `src/osd/osd_types.h`: a versioned struct (v1) holding a list of
//! `watch_item_t` (v2, compat 1), each the watcher's packed
//! `entity_name_t`, the watch cookie, the timeout in seconds and the
//! watcher's `entity_addr_t`.

use bytes::{Buf, BufMut};
use serde::Serialize;

use crate::denc::{Denc, EntityAddr, RadosError, VersionedEncode};
use crate::osdclient::error::Result;
use crate::osdclient::types::{OpReply, PackedEntityName};

/// One watcher of an object: `watch_item_t`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchItem {
    /// The watching client (`client.<gid>`).
    pub name: PackedEntityName,
    /// The cookie the watcher registered with.
    pub cookie: u64,
    /// The watch timeout the OSD applies, in seconds.
    pub timeout_seconds: u32,
    /// The watcher's address.
    pub addr: EntityAddr,
}

impl WatchItem {
    /// The watcher as `entity_name_t` prints it: `client.4242`, or
    /// `client.?` when the number is negative as an `int64_t`.
    pub fn watcher_name(&self) -> String {
        let entity_type = crate::EntityType::from_bits_truncate(u32::from(self.name.entity_type));
        let num = self.name.num.get();
        if (num as i64) < 0 {
            format!("{entity_type}.?")
        } else {
            format!("{entity_type}.{num}")
        }
    }
}

/// Matches `watch_item_t::dump` for the corpus harness: the field names
/// differ from the struct's, and the name is a streamed string.
impl Serialize for WatchItem {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("WatchItem", 4)?;
        state.serialize_field("watcher", &self.watcher_name())?;
        state.serialize_field("cookie", &self.cookie)?;
        state.serialize_field("timeout", &self.timeout_seconds)?;
        state.serialize_field("addr", &self.addr)?;
        state.end()
    }
}

/// `watch_item_t` is `ENCODE_START(2, 1)`: name, cookie, timeout, and
/// from v2 the address. Squid emits v2.
impl VersionedEncode for WatchItem {
    const MAX_DECODE_VERSION: u8 = 2;

    fn encoding_version(&self, _features: u64) -> u8 {
        2
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.name.encode(buf, features)?;
        self.cookie.encode(buf, features)?;
        self.timeout_seconds.encode(buf, features)?;
        self.addr.encode(buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        crate::denc::check_min_version!(struct_v, 2, "WatchItem", "Squid v19+");

        let name = PackedEntityName::decode(buf, features)?;
        let cookie = u64::decode(buf, features)?;
        let timeout_seconds = u32::decode(buf, features)?;
        let addr = EntityAddr::decode(buf, features)?;

        Ok(Self {
            name,
            cookie,
            timeout_seconds,
            addr,
        })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        Some(self.name.encoded_size(features)? + 8 + 4 + self.addr.encoded_size(features)?)
    }
}

crate::denc::impl_denc_for_versioned!(WatchItem);

/// The reply to `LIST_WATCHERS`: `obj_list_watch_response_t`. Its
/// `dump` is an `entries` array of the items, which the derive matches.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct ListWatchersReply {
    /// Every current watcher of the object.
    pub entries: Vec<WatchItem>,
}

/// `obj_list_watch_response_t` is `ENCODE_START(1, 1)` around a
/// `std::list<watch_item_t>`, which encodes as a u32 count then the items.
impl VersionedEncode for ListWatchersReply {
    const MAX_DECODE_VERSION: u8 = 1;

    fn encoding_version(&self, _features: u64) -> u8 {
        1
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.entries.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        crate::denc::check_min_version!(struct_v, 1, "ListWatchersReply", "Squid v19+");

        Ok(Self {
            entries: Vec::<WatchItem>::decode(buf, features)?,
        })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        self.entries.encoded_size(features)
    }
}

crate::denc::impl_denc_for_versioned!(ListWatchersReply);

/// Decode the reply to `list_watchers`.
pub fn decode_list_watchers(reply: &OpReply) -> Result<Vec<WatchItem>> {
    let mut buf = reply.outdata.clone();
    Ok(ListWatchersReply::decode(&mut buf, 0)?.entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::denc::{EntityAddrType, encode_with_capacity};
    use crate::osdclient::types::{OpReply, PackedEntityName};
    use bytes::Bytes;

    fn addr() -> EntityAddr {
        EntityAddr::from_socket_addr(
            EntityAddrType::Msgr2,
            "127.0.0.1:6800".parse().expect("addr"),
        )
    }

    fn sample_item() -> WatchItem {
        WatchItem {
            name: PackedEntityName::new(0x08, 4242), // client.4242
            cookie: 7,
            timeout_seconds: 30,
            addr: addr(),
        }
    }

    /// `obj_list_watch_response_t` holding `sample_item`, built by hand
    /// from osd_types.h: each ENCODE_START is `struct_v, struct_compat,
    /// u32 length`; the addr bytes come from the crate's own EntityAddr
    /// encoding, which has its own tests.
    fn one_watcher_wire() -> Vec<u8> {
        let addr = encode_with_capacity(&addr(), 0).expect("addr");
        let mut item = vec![0x08]; // entity_name_t: type CLIENT
        item.extend_from_slice(&4242u64.to_le_bytes()); // num
        item.extend_from_slice(&7u64.to_le_bytes()); // cookie
        item.extend_from_slice(&30u32.to_le_bytes()); // timeout_seconds
        item.extend_from_slice(&addr);
        let mut entries = 1u32.to_le_bytes().to_vec(); // list length
        entries.extend_from_slice(&[2, 1]); // watch_item_t v2, compat 1
        entries.extend_from_slice(&(item.len() as u32).to_le_bytes());
        entries.extend_from_slice(&item);
        let mut wire = vec![1, 1]; // obj_list_watch_response_t v1, compat 1
        wire.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        wire.extend_from_slice(&entries);
        wire
    }

    fn reply(outdata: Vec<u8>) -> OpReply {
        OpReply {
            return_code: 0,
            outdata: Bytes::from(outdata),
        }
    }

    #[test]
    fn decodes_one_watcher() {
        let items = decode_list_watchers(&reply(one_watcher_wire())).expect("decode");
        assert_eq!(items, vec![sample_item()]);
    }

    #[test]
    fn decodes_no_watchers() {
        // v1, compat 1, length 4: an empty list.
        let items =
            decode_list_watchers(&reply(vec![1, 1, 4, 0, 0, 0, 0, 0, 0, 0])).expect("decode");
        assert!(items.is_empty());
    }

    #[test]
    fn encodes_the_same_bytes() {
        let reply = ListWatchersReply {
            entries: vec![sample_item()],
        };
        let bytes = encode_with_capacity(&reply, 0).expect("encode");
        assert_eq!(bytes.as_ref(), &one_watcher_wire()[..]);
        let back = ListWatchersReply::decode(&mut bytes.clone(), 0).expect("decode");
        assert_eq!(back, reply);
    }

    #[test]
    fn rejects_a_pre_squid_watch_item() {
        // watch_item_t v1 has no addr; no supported OSD emits it.
        let mut item = vec![0x08];
        item.extend_from_slice(&1u64.to_le_bytes());
        item.extend_from_slice(&2u64.to_le_bytes());
        item.extend_from_slice(&3u32.to_le_bytes());
        let mut entries = 1u32.to_le_bytes().to_vec();
        entries.extend_from_slice(&[1, 1]);
        entries.extend_from_slice(&(item.len() as u32).to_le_bytes());
        entries.extend_from_slice(&item);
        let mut wire = vec![1, 1];
        wire.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        wire.extend_from_slice(&entries);
        assert!(decode_list_watchers(&reply(wire)).is_err());
    }

    #[test]
    fn serializes_like_ceph_dencoder() {
        // watch_item_t::dump in osd_types.h: "watcher" streams the
        // entity_name_t, "cookie" and "timeout" are ints, "addr" is the
        // entity_addr_t dump; obj_list_watch_response_t::dump wraps the
        // items in an "entries" array.
        let item = sample_item();
        let json = serde_json::to_value(&item).expect("json");
        assert_eq!(json["watcher"], serde_json::json!("client.4242"));
        assert_eq!(json["cookie"], serde_json::json!(7));
        assert_eq!(json["timeout"], serde_json::json!(30));
        assert_eq!(
            json["addr"],
            serde_json::to_value(addr()).expect("addr json")
        );
        assert_eq!(json.as_object().expect("object").len(), 4);

        let reply = ListWatchersReply {
            entries: vec![item],
        };
        let json = serde_json::to_value(&reply).expect("json");
        assert_eq!(json["entries"].as_array().expect("array").len(), 1);

        // entity_name_t prints a negative number as "?".
        let negative = WatchItem {
            name: PackedEntityName::new(0x08, u64::MAX),
            ..sample_item()
        };
        assert_eq!(negative.watcher_name(), "client.?");
    }
}
