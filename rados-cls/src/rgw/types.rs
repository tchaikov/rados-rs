//! `cls_rgw_types.h`: the pieces every rgw-side class shares. This file
//! holds what the GC classes need; the bucket index, usage, lifecycle
//! and OLH types join it with their classes.

use bytes::{Buf, BufMut};
use rados::{Denc, RadosError, UTime, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::SerializeStruct;

/// `cls_rgw_obj_key` (`rgw_obj_index_key`): an object's name and, when it
/// is a version, its instance.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ObjKey {
    pub name: String,
    pub instance: String,
}

/// `cls_rgw_obj`: one object of a GC chain. Version 2 appended the full
/// key after the three strings version 1 wrote, so `key.name` is on the
/// wire twice; the decoder floors at version 2. The dump calls `key.name`
/// "oid" and `loc` "key".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Obj {
    pub pool: String,
    pub key: ObjKey,
    pub loc: String,
}

impl VersionedEncode for Obj {
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
        self.pool.encode(buf, features)?;
        self.key.name.encode(buf, features)?;
        self.loc.encode(buf, features)?;
        self.key.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 2, "Obj", "Hammer v0.94+");

        let pool = String::decode(buf, features)?;
        let _name = String::decode(buf, features)?;
        let loc = String::decode(buf, features)?;
        let key = ObjKey::decode(buf, features)?;
        Ok(Self { pool, key, loc })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(Obj);

impl Serialize for Obj {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Obj", 4)?;
        state.serialize_field("pool", &self.pool)?;
        state.serialize_field("oid", &self.key.name)?;
        state.serialize_field("key", &self.loc)?;
        state.serialize_field("instance", &self.key.instance)?;
        state.end()
    }
}

/// `cls_rgw_obj_chain`: the objects one GC entry deletes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ObjChain {
    pub objs: Vec<Obj>,
}

/// `cls_rgw_gc_obj_info`: a GC entry: the tag naming it, the chain to
/// delete, and when it falls due. This is the payload of every `rgw_gc`
/// queue entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GcObjInfo {
    pub tag: String,
    pub chain: ObjChain,
    #[serde(serialize_with = "crate::dump::real_time")]
    pub time: UTime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::encode_with_capacity;

    fn bytes<T: Denc>(v: &T) -> Vec<u8> {
        encode_with_capacity(v, 0).expect("encode").to_vec()
    }

    fn json<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("json")
    }

    #[test]
    fn obj_key_is_two_strings() {
        // Corpus cls_rgw_obj_key/eedab0b3...
        let k = ObjKey {
            name: "name".to_owned(),
            instance: "instance".to_owned(),
        };
        assert_eq!(
            bytes(&k),
            b"\x01\x01\x14\x00\x00\x00\x04\x00\x00\x00name\x08\x00\x00\x00instance"
        );
        assert_eq!(json(&k), r#"{"name":"name","instance":"instance"}"#);
    }

    #[test]
    fn obj_writes_the_name_twice_and_dumps_loc_as_key() {
        // Corpus cls_rgw_obj/bfcd58c2...: pool "mypool", key.name "myoid",
        // loc "mykey", no instance; 53 bytes, version 2 over compat 1.
        let o = Obj {
            pool: "mypool".to_owned(),
            key: ObjKey {
                name: "myoid".to_owned(),
                instance: String::new(),
            },
            loc: "mykey".to_owned(),
        };
        let wire = b"\x02\x01\x2f\x00\x00\x00\x06\x00\x00\x00mypool\x05\x00\x00\x00myoid\x05\x00\x00\x00mykey\x01\x01\x0d\x00\x00\x00\x05\x00\x00\x00myoid\x00\x00\x00\x00";
        assert_eq!(bytes(&o), wire);
        assert_eq!(Obj::decode(&mut &wire[..], 0).expect("decode"), o);
        assert_eq!(
            json(&o),
            r#"{"pool":"mypool","oid":"myoid","key":"mykey","instance":""}"#
        );

        // A version-1 writer sent only the three strings; the decoder
        // floors at version 2 and rejects it.
        let v1 = b"\x01\x01\x0f\x00\x00\x00\x01\x00\x00\x00p\x01\x00\x00\x00n\x01\x00\x00\x00l";
        assert!(Obj::decode(&mut &v1[..], 0).is_err());
    }

    #[test]
    fn gc_obj_info_matches_the_corpus_instance() {
        // Corpus cls_rgw_gc_obj_info/fd3d3891...: tag "footag", empty chain,
        // time {21, 32}; 34 bytes.
        let info = GcObjInfo {
            tag: "footag".to_owned(),
            chain: ObjChain::default(),
            time: UTime { sec: 21, nsec: 32 },
        };
        assert_eq!(
            bytes(&info),
            b"\x01\x01\x1c\x00\x00\x00\x06\x00\x00\x00footag\x01\x01\x04\x00\x00\x00\x00\x00\x00\x00\x15\x00\x00\x00\x20\x00\x00\x00"
        );
        assert_eq!(
            json(&info),
            r#"{"tag":"footag","chain":{"objs":[]},"time":"1970-01-01T00:00:21.000000+0000"}"#
        );
        assert_eq!(
            GcObjInfo::decode(&mut &bytes(&info)[..], 0).expect("decode"),
            info
        );
    }

    #[test]
    fn json_keeps_a_nul_in_a_tag() {
        // cls_rgw_gc_obj_info::dump uses the whole std::string, unlike
        // obj_refcount; the corpus tags end in a NUL and dencoder prints it.
        let info = GcObjInfo {
            tag: "t\0".to_owned(),
            ..GcObjInfo::default()
        };
        assert!(json(&info).starts_with(r#"{"tag":"t\u0000""#));
    }
}
