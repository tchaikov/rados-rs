//! The `refcount` class: a set of reference tags an object carries in its
//! `refcount` xattr; RGW uses it for tail objects shared between copies.
//! Mirrors `cls_refcount_client.h`.
//!
//! Server facts the API rests on (`src/cls/refcount/cls_refcount.cc`):
//! `get` adds a tag (a repeat is a no-op); `put` retires a tag, is a silent
//! success for an unknown or already-retired tag, is `EINVAL` when the
//! object holds no references at all, and removes the object when it
//! drops the last one; `set` replaces the whole set and removes the object
//! when given none; the implicit reference an unrefcounted object is
//! assumed to hold is the empty-string tag, honoured only when
//! `implicit_ref` is set.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut};
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, RadosError, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::{SerializeSeq, SerializeStruct};

use crate::call;

/// The class name.
pub const CLASS: &str = "refcount";

/// The tag every unrefcounted object is assumed to hold when
/// `implicit_ref` is set: `cls_refcount.cc`'s `wildcard_tag`.
pub const WILDCARD_TAG: &str = "";

/// `cls_refcount_get_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetOp {
    pub tag: String,
    #[serde(serialize_with = "crate::dump::bool_as_int")]
    pub implicit_ref: bool,
}

/// `cls_refcount_put_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct PutOp {
    pub tag: String,
    #[serde(serialize_with = "crate::dump::bool_as_int")]
    pub implicit_ref: bool,
}

/// `cls_refcount_set_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct SetOp {
    pub refs: Vec<String>,
}

/// `cls_refcount_read_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ReadOp {
    #[serde(serialize_with = "crate::dump::bool_as_int")]
    pub implicit_ref: bool,
}

/// `cls_refcount_read_ret`: the active tags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ReadRet {
    pub refs: Vec<String>,
}

/// `obj_refcount`: what the class stores in the `refcount` xattr.
/// `ENCODE_START(2, 1)`: `refs`, then from v2 `retired_refs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjRefcount {
    /// Active tags; the class only ever stores `true`.
    pub refs: BTreeMap<String, bool>,
    /// Tags a `put` already retired, so a repeated `put` is a no-op.
    pub retired_refs: BTreeSet<String>,
}

impl VersionedEncode for ObjRefcount {
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
        self.refs.encode(buf, features)?;
        self.retired_refs.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 2, "ObjRefcount", "Luminous v12+");

        let refs = BTreeMap::<String, bool>::decode(buf, features)?;
        let retired_refs = BTreeSet::<String>::decode(buf, features)?;
        Ok(Self { refs, retired_refs })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        Some(self.refs.encoded_size(features)? + self.retired_refs.encoded_size(features)?)
    }
}

rados::impl_denc_for_versioned!(ObjRefcount);

/// `obj_refcount::dump` prints each tag with `c_str()`, so a tag holding
/// a NUL, as RGW once wrote them, dumps only up to it.
fn c_str(s: &str) -> &str {
    s.find('\0').map_or(s, |i| &s[..i])
}

/// `obj_refcount::dump`: `refs` as `{"oid", "active"}` objects,
/// `retired_refs` as strings, each tag truncated at its first NUL via
/// `c_str()`, matching what `ceph-dencoder` prints.
impl Serialize for ObjRefcount {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        struct Ref<'a>(&'a str, bool);
        impl Serialize for Ref<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                let mut s = serializer.serialize_struct("ref", 2)?;
                s.serialize_field("oid", self.0)?;
                s.serialize_field("active", &self.1)?;
                s.end()
            }
        }
        struct Refs<'a>(&'a BTreeMap<String, bool>);
        impl Serialize for Refs<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for (oid, active) in self.0 {
                    seq.serialize_element(&Ref(c_str(oid), *active))?;
                }
                seq.end()
            }
        }
        struct Retired<'a>(&'a BTreeSet<String>);
        impl Serialize for Retired<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for item in self.0 {
                    seq.serialize_element(c_str(item))?;
                }
                seq.end()
            }
        }
        let mut state = serializer.serialize_struct("ObjRefcount", 2)?;
        state.serialize_field("refs", &Refs(&self.refs))?;
        state.serialize_field("retired_refs", &Retired(&self.retired_refs))?;
        state.end()
    }
}

/// `cls_refcount_get`: add `tag` to the object's references.
pub fn get_op(tag: &str, implicit_ref: bool) -> Result<OSDOp> {
    call::op(
        CLASS,
        "get",
        &GetOp {
            tag: tag.to_owned(),
            implicit_ref,
        },
    )
}

/// `cls_refcount_put`: retire `tag`; removes the object when it was the
/// last reference.
pub fn put_op(tag: &str, implicit_ref: bool) -> Result<OSDOp> {
    call::op(
        CLASS,
        "put",
        &PutOp {
            tag: tag.to_owned(),
            implicit_ref,
        },
    )
}

/// `cls_refcount_set`: replace the references; removes the object when
/// `refs` is empty.
pub fn set_op(refs: &[String]) -> Result<OSDOp> {
    call::op(
        CLASS,
        "set",
        &SetOp {
            refs: refs.to_vec(),
        },
    )
}

/// `cls_refcount_read`: the op; decode its reply with [`decode_read`].
pub fn read_op(implicit_ref: bool) -> Result<OSDOp> {
    call::op(CLASS, "read", &ReadOp { implicit_ref })
}

/// Decode the reply to [`read_op`]: the active tags.
pub fn decode_read(reply: &OpReply) -> Result<Vec<String>> {
    Ok(call::decode::<ReadRet>(reply)?.refs)
}

/// Add `tag` to `oid`'s references.
pub async fn get(ioctx: &IoCtx, oid: &str, tag: &str, implicit_ref: bool) -> Result<()> {
    let op = GetOp {
        tag: tag.to_owned(),
        implicit_ref,
    };
    call::exec(ioctx, oid, CLASS, "get", &op).await.map(drop)
}

/// Retire `tag` on `oid`, removing the object if it was the last reference.
pub async fn put(ioctx: &IoCtx, oid: &str, tag: &str, implicit_ref: bool) -> Result<()> {
    let op = PutOp {
        tag: tag.to_owned(),
        implicit_ref,
    };
    call::exec(ioctx, oid, CLASS, "put", &op).await.map(drop)
}

/// Replace `oid`'s references with `refs`, removing the object if empty.
pub async fn set(ioctx: &IoCtx, oid: &str, refs: &[String]) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "set",
        &SetOp {
            refs: refs.to_vec(),
        },
    )
    .await
    .map(drop)
}

/// Read `oid`'s active tags.
pub async fn read(ioctx: &IoCtx, oid: &str, implicit_ref: bool) -> Result<Vec<String>> {
    let out = call::exec(ioctx, oid, CLASS, "read", &ReadOp { implicit_ref }).await?;
    Ok(call::decode_bytes::<ReadRet>(out)?.refs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::encode_with_capacity;
    use rados::osdclient::types::OpData;

    #[test]
    fn get_op_encodes_tag_then_bool() {
        let op = GetOp {
            tag: "foo".to_owned(),
            implicit_ref: true,
        };
        let bytes = encode_with_capacity(&op, 0).expect("encode");
        assert_eq!(
            bytes.as_ref(),
            &[1, 1, 8, 0, 0, 0, 3, 0, 0, 0, b'f', b'o', b'o', 1][..]
        );
        assert_eq!(GetOp::decode(&mut bytes.clone(), 0).expect("decode"), op);
    }

    #[test]
    fn set_op_encodes_a_list_of_strings() {
        let op = SetOp {
            refs: vec!["foo".to_owned(), "bar".to_owned()],
        };
        let bytes = encode_with_capacity(&op, 0).expect("encode");
        assert_eq!(
            bytes.as_ref(),
            &[
                1, 1, 18, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, b'f', b'o', b'o', 3, 0, 0, 0, b'b',
                b'a', b'r'
            ][..]
        );
    }

    #[test]
    fn read_op_is_one_bool() {
        let bytes = encode_with_capacity(
            &ReadOp {
                implicit_ref: false,
            },
            0,
        )
        .expect("encode");
        assert_eq!(bytes.as_ref(), &[1, 1, 1, 0, 0, 0, 0][..]);
    }

    #[test]
    fn obj_refcount_is_v2_with_retired_refs() {
        // obj_refcount::generate_test_instances(): refs {"foo": true},
        // retired_refs {"bar"}; ENCODE_START(2, 1).
        let mut refs = BTreeMap::new();
        refs.insert("foo".to_owned(), true);
        let mut retired_refs = BTreeSet::new();
        retired_refs.insert("bar".to_owned());
        let obj = ObjRefcount { refs, retired_refs };
        let bytes = encode_with_capacity(&obj, 0).expect("encode");
        assert_eq!(
            bytes.as_ref(),
            &[
                2, 1, 23, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, b'f', b'o', b'o', 1, 1, 0, 0, 0, 3, 0,
                0, 0, b'b', b'a', b'r'
            ][..]
        );
        assert_eq!(
            ObjRefcount::decode(&mut bytes.clone(), 0).expect("decode"),
            obj
        );
    }

    #[test]
    fn obj_refcount_rejects_v1() {
        // Every release since Luminous writes v2; a v1 blob (no retired_refs)
        // is below the floor.
        let wire = [
            1u8, 1, 12, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, b'f', b'o', b'o', 1,
        ];
        assert!(ObjRefcount::decode(&mut &wire[..], 0).is_err());
    }

    #[test]
    fn ops_name_the_class_and_method() {
        let op = put_op("t", true).expect("op");
        assert!(op.indata.starts_with(b"refcountput"));
        assert!(matches!(
            op.op_data,
            OpData::Call {
                class_len: 8,
                method_len: 3,
                ..
            }
        ));
    }

    #[test]
    fn decode_read_unwraps_the_reply() {
        let ret = ReadRet {
            refs: vec!["a".to_owned()],
        };
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&ret, 0).expect("encode"),
        };
        assert_eq!(decode_read(&reply).expect("decode"), vec!["a".to_owned()]);
    }

    #[test]
    fn json_matches_ceph_dencoder_dump() {
        // cls_refcount_get_op::dump: dump_string("tag"), dump_int("implicit_ref", (int)b).
        assert_eq!(
            serde_json::to_value(GetOp {
                tag: "foo".to_owned(),
                implicit_ref: true
            })
            .expect("json"),
            serde_json::json!({"tag": "foo", "implicit_ref": 1})
        );
        // cls_refcount_read_ret::dump: an array section of strings.
        assert_eq!(
            serde_json::to_value(ReadRet {
                refs: vec!["foo".to_owned(), "bar".to_owned()]
            })
            .expect("json"),
            serde_json::json!({"refs": ["foo", "bar"]})
        );
        // obj_refcount::dump: refs as {"oid", "active"} objects, retired_refs as strings.
        let mut refs = BTreeMap::new();
        refs.insert("foo".to_owned(), true);
        let mut retired_refs = BTreeSet::new();
        retired_refs.insert("bar".to_owned());
        assert_eq!(
            serde_json::to_value(ObjRefcount { refs, retired_refs }).expect("json"),
            serde_json::json!({
                "refs": [{"oid": "foo", "active": true}],
                "retired_refs": ["bar"]
            })
        );
    }

    #[test]
    fn json_truncates_tags_at_a_nul() {
        let mut refs = BTreeMap::new();
        refs.insert("a\u{0}b".to_owned(), true);
        let mut retired_refs = BTreeSet::new();
        retired_refs.insert("c\u{0}".to_owned());
        assert_eq!(
            serde_json::to_value(ObjRefcount { refs, retired_refs }).expect("json"),
            serde_json::json!({
                "refs": [{"oid": "a", "active": true}],
                "retired_refs": ["c"]
            })
        );
    }
}
