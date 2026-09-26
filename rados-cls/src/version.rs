//! The `version` class: a `(ver, tag)` pair an object carries in its
//! `ceph.objclass.version` xattr, used by RGW as an optimistic-concurrency
//! guard on its metadata objects. Mirrors `cls_version_client.h`.
//!
//! Server facts the API rests on (`src/cls/version/cls_version.cc`): a
//! failed condition fails the request with `ECANCELED`; an object that was
//! never versioned reads as `{ver: 0, tag: ""}`; `inc` on such an object
//! initialises the version to 1 with a random 24-character tag and then
//! increments it, so the first `inc` reads back as 2, and later `inc`s
//! keep the tag; `set` stores its argument without any check.

use bytes::{Buf, BufMut, Bytes};
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, RadosError, VersionedDenc};
use serde::Serialize;

use crate::call;

/// The class name.
pub const CLASS: &str = "version";

/// `obj_version`: a counter and a tag that changes when the counter is
/// reset. `ENCODE_START(1, 1)`: `ver`, then `tag`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ObjVersion {
    pub ver: u64,
    pub tag: String,
}

impl ObjVersion {
    /// What the class does on `inc`: the counter goes up, the tag stays.
    pub fn inc(&mut self) {
        self.ver += 1;
    }

    /// `obj_version::empty`: no tag means the object was never versioned.
    pub fn is_empty(&self) -> bool {
        self.tag.is_empty()
    }
}

/// `VersionCond`: how the class compares the object's version with the
/// one supplied. The ordered forms compare `ver` only; the tag forms
/// compare `tag` only; `Eq` compares both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum VersionCond {
    #[default]
    None = 0,
    Eq = 1,
    Gt = 2,
    Ge = 3,
    Lt = 4,
    Le = 5,
    TagEq = 6,
    TagNe = 7,
}

impl TryFrom<u32> for VersionCond {
    type Error = RadosError;

    fn try_from(value: u32) -> std::result::Result<Self, RadosError> {
        Ok(match value {
            0 => Self::None,
            1 => Self::Eq,
            2 => Self::Gt,
            3 => Self::Ge,
            4 => Self::Lt,
            5 => Self::Le,
            6 => Self::TagEq,
            7 => Self::TagNe,
            other => {
                return Err(RadosError::Protocol(format!(
                    "invalid version condition {other}"
                )));
            }
        })
    }
}

/// `obj_version_cond` encodes the condition as a `uint32_t`.
impl Denc for VersionCond {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        (*self as u32).encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        Self::try_from(u32::decode(buf, features)?)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4)
    }
}

/// `obj_version_cond::dump` uses `dump_unsigned`.
impl Serialize for VersionCond {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_u32(*self as u32)
    }
}

/// `obj_version_cond`: a version to compare against and how.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ObjVersionCond {
    pub ver: ObjVersion,
    pub cond: VersionCond,
}

/// `cls_version_set_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct SetOp {
    pub objv: ObjVersion,
}

/// `cls_version_inc_op`: `objv` is ignored by the server; only `conds`
/// matter, and they may be empty.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct IncOp {
    pub objv: ObjVersion,
    pub conds: Vec<ObjVersionCond>,
}

/// `cls_version_check_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct CheckOp {
    pub objv: ObjVersion,
    pub conds: Vec<ObjVersionCond>,
}

/// `cls_version_read_ret`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ReadRet {
    pub objv: ObjVersion,
}

fn one_cond(objv: &ObjVersion, cond: VersionCond) -> Vec<ObjVersionCond> {
    vec![ObjVersionCond {
        ver: objv.clone(),
        cond,
    }]
}

/// `cls_version_set`: store `objv` unconditionally.
pub fn set_op(objv: &ObjVersion) -> Result<OSDOp> {
    call::op(CLASS, "set", &SetOp { objv: objv.clone() })
}

/// `cls_version_inc(op)`: bump `ver` by one; a missing version is
/// initialised to 1 first, so the first `inc` reads back as 2.
pub fn inc_op() -> Result<OSDOp> {
    call::op(CLASS, "inc", &IncOp::default())
}

/// `cls_version_inc(op, objv, cond)`: bump `ver` only if `cond` holds for
/// `objv` against the stored version; otherwise the request fails with
/// `ECANCELED` and nothing in it applies.
pub fn inc_conds_op(objv: &ObjVersion, cond: VersionCond) -> Result<OSDOp> {
    call::op(
        CLASS,
        "inc_conds",
        &IncOp {
            objv: objv.clone(),
            conds: one_cond(objv, cond),
        },
    )
}

/// `cls_version_check`: a read op that fails the request with `ECANCELED`
/// unless `cond` holds; RGW puts one ahead of the writes it guards.
pub fn check_op(objv: &ObjVersion, cond: VersionCond) -> Result<OSDOp> {
    call::op(
        CLASS,
        "check_conds",
        &CheckOp {
            objv: objv.clone(),
            conds: one_cond(objv, cond),
        },
    )
}

/// `cls_version_read`: the op; decode its reply with [`decode_read`].
pub fn read_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "read", Bytes::new())
}

/// Decode the reply to [`read_op`]. An object that was never versioned
/// reads as `ObjVersion::default()`.
pub fn decode_read(reply: &OpReply) -> Result<ObjVersion> {
    Ok(call::decode::<ReadRet>(reply)?.objv)
}

/// Store `objv` on `oid`.
pub async fn set(ioctx: &IoCtx, oid: &str, objv: &ObjVersion) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "set", &SetOp { objv: objv.clone() })
        .await
        .map(drop)
}

/// Bump `oid`'s version; a missing one is initialised first, so the first
/// call reads back as 2.
pub async fn inc(ioctx: &IoCtx, oid: &str) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "inc", &IncOp::default())
        .await
        .map(drop)
}

/// Bump `oid`'s version if `cond` holds for `objv`; `ECANCELED` otherwise.
pub async fn inc_conds(
    ioctx: &IoCtx,
    oid: &str,
    objv: &ObjVersion,
    cond: VersionCond,
) -> Result<()> {
    let op = IncOp {
        objv: objv.clone(),
        conds: one_cond(objv, cond),
    };
    call::exec(ioctx, oid, CLASS, "inc_conds", &op)
        .await
        .map(drop)
}

/// Succeed if `cond` holds for `objv` against `oid`'s version; `ECANCELED`
/// otherwise.
pub async fn check(ioctx: &IoCtx, oid: &str, objv: &ObjVersion, cond: VersionCond) -> Result<()> {
    let op = CheckOp {
        objv: objv.clone(),
        conds: one_cond(objv, cond),
    };
    call::exec(ioctx, oid, CLASS, "check_conds", &op)
        .await
        .map(drop)
}

/// Read `oid`'s version; `ObjVersion::default()` if it was never versioned.
pub async fn read(ioctx: &IoCtx, oid: &str) -> Result<ObjVersion> {
    let out = call::exec_raw(ioctx, oid, CLASS, "read", Bytes::new()).await?;
    Ok(call::decode_bytes::<ReadRet>(out)?.objv)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::encode_with_capacity;
    use rados::osdclient::types::OpData;

    fn v(ver: u64, tag: &str) -> ObjVersion {
        ObjVersion {
            ver,
            tag: tag.to_owned(),
        }
    }

    /// `obj_version::encode`: ENCODE_START(1, 1), u64 ver, string tag.
    fn objv_wire(ver: u64, tag: &str) -> Vec<u8> {
        let mut content = ver.to_le_bytes().to_vec();
        content.extend_from_slice(&(tag.len() as u32).to_le_bytes());
        content.extend_from_slice(tag.as_bytes());
        let mut wire = vec![1, 1];
        wire.extend_from_slice(&(content.len() as u32).to_le_bytes());
        wire.extend_from_slice(&content);
        wire
    }

    #[test]
    fn obj_version_encodes_as_ceph() {
        let bytes = encode_with_capacity(&v(123, "foo"), 0).expect("encode");
        assert_eq!(
            bytes.as_ref(),
            &[
                1, 1, 15, 0, 0, 0, 123, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, b'f', b'o', b'o'
            ][..]
        );
        assert_eq!(bytes.as_ref(), &objv_wire(123, "foo")[..]);
        let back = ObjVersion::decode(&mut bytes.clone(), 0).expect("decode");
        assert_eq!(back, v(123, "foo"));
    }

    #[test]
    fn cond_encodes_as_u32_after_the_version() {
        let cond = ObjVersionCond {
            ver: v(1, "foo"),
            cond: VersionCond::Eq,
        };
        let bytes = encode_with_capacity(&cond, 0).expect("encode");
        let mut want = vec![1, 1, 25, 0, 0, 0];
        want.extend_from_slice(&objv_wire(1, "foo"));
        want.extend_from_slice(&[1, 0, 0, 0]);
        assert_eq!(bytes.as_ref(), &want[..]);
        let back = ObjVersionCond::decode(&mut bytes.clone(), 0).expect("decode");
        assert_eq!(back, cond);
    }

    #[test]
    fn inc_op_with_one_condition_matches_the_corpus_instance() {
        // cls_version_inc_op::generate_test_instances(): objv {123, "foo"},
        // one cond {ver {123, "foo"}, VER_COND_GE}.
        let op = IncOp {
            objv: v(123, "foo"),
            conds: vec![ObjVersionCond {
                ver: v(123, "foo"),
                cond: VersionCond::Ge,
            }],
        };
        let bytes = encode_with_capacity(&op, 0).expect("encode");
        let mut cond = vec![1, 1, 25, 0, 0, 0];
        cond.extend_from_slice(&objv_wire(123, "foo"));
        cond.extend_from_slice(&[3, 0, 0, 0]);
        let mut content = objv_wire(123, "foo");
        content.extend_from_slice(&[1, 0, 0, 0]);
        content.extend_from_slice(&cond);
        let mut want = vec![1, 1, content.len() as u8, 0, 0, 0];
        want.extend_from_slice(&content);
        assert_eq!(content.len(), 56);
        assert_eq!(bytes.as_ref(), &want[..]);
        assert_eq!(IncOp::decode(&mut bytes.clone(), 0).expect("decode"), op);
    }

    #[test]
    fn ops_name_the_class_and_method() {
        let op = inc_conds_op(&v(5, "t"), VersionCond::Eq).expect("op");
        assert!(op.indata.starts_with(b"versioninc_conds"));
        assert!(matches!(
            op.op_data,
            OpData::Call {
                class_len: 7,
                method_len: 9,
                ..
            }
        ));
        let op = read_op().expect("op");
        assert_eq!(&op.indata[..], b"versionread");
        assert!(matches!(op.op_data, OpData::Call { indata_len: 0, .. }));
    }

    #[test]
    fn decode_read_unwraps_the_reply() {
        let ret = ReadRet { objv: v(7, "x") };
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&ret, 0).expect("encode"),
        };
        assert_eq!(decode_read(&reply).expect("decode"), v(7, "x"));
    }

    #[test]
    fn json_matches_ceph_dencoder_dump() {
        // obj_version::dump: dump_int("ver"), dump_string("tag");
        // obj_version_cond::dump: dump_object("ver"), dump_unsigned("cond");
        // cls_version_inc_op::dump: dump_object("objv"), encode_json("conds").
        let op = IncOp {
            objv: v(123, "foo"),
            conds: vec![ObjVersionCond {
                ver: v(123, "foo"),
                cond: VersionCond::Ge,
            }],
        };
        assert_eq!(
            serde_json::to_value(&op).expect("json"),
            serde_json::json!({
                "objv": {"ver": 123, "tag": "foo"},
                "conds": [{"ver": {"ver": 123, "tag": "foo"}, "cond": 3}]
            })
        );
        assert_eq!(
            serde_json::to_value(ReadRet { objv: v(1, "a") }).expect("json"),
            serde_json::json!({"objv": {"ver": 1, "tag": "a"}})
        );
    }
}
