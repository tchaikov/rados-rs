//! omap operations: the key/value store every RADOS object carries.
//!
//! Wire formats follow `ObjectOperation` in `src/osdc/Objecter.h`: every op
//! that carries `indata` sets an extent union of `(0, indata.len())`,
//! matching `add_data` — this includes the read ops that take arguments
//! (`OMAPGETKEYS`, `OMAPGETVALS`, `OMAPGETVALSBYKEYS`, `OMAP_CMP`), not only
//! the writes. Only `omap_get_header` and `omap_clear` carry no `indata`
//! and send the empty union `add_op` builds. Replies for the listing ops
//! carry the entries and then a `more` flag.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::denc::{Denc, RadosError, encode_with_capacity};
use crate::osdclient::error::Result;
use crate::osdclient::types::{OSDOp, OpCode, OpData, OpReply};

/// An omap key. Ceph treats keys as byte strings and RGW's bucket index
/// uses bytes outside UTF-8, so keys are not `String`.
pub type OmapKey = Bytes;
/// An omap key/value listing, as returned by `omap_get_vals`.
pub type OmapMap = BTreeMap<OmapKey, Bytes>;
/// A set of omap keys, as passed to `omap_get_vals_by_keys`/`omap_rm_keys`.
pub type OmapKeySet = BTreeSet<OmapKey>;

/// Reply to `omap_get_keys`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OmapKeys {
    /// Keys returned by this call.
    pub keys: OmapKeySet,
    /// More keys remain after the last one returned.
    pub more: bool,
}

/// Reply to `omap_get_vals`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OmapVals {
    /// Key/value entries returned by this call.
    pub vals: OmapMap,
    /// More entries remain after the last one returned.
    pub more: bool,
}

/// `CEPH_OSD_CMPXATTR_OP_*`, shared by `omap_cmp` and `cmpxattr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CmpOp {
    /// Equal.
    Eq = 1,
    /// Not equal.
    Ne = 2,
    /// Greater than.
    Gt = 3,
    /// Greater than or equal.
    Gte = 4,
    /// Less than.
    Lt = 5,
    /// Less than or equal.
    Lte = 6,
}

impl TryFrom<i32> for CmpOp {
    type Error = RadosError;

    fn try_from(value: i32) -> std::result::Result<Self, RadosError> {
        Ok(match value {
            1 => Self::Eq,
            2 => Self::Ne,
            3 => Self::Gt,
            4 => Self::Gte,
            5 => Self::Lt,
            6 => Self::Lte,
            other => return Err(RadosError::Protocol(format!("invalid cmp op {other}"))),
        })
    }
}

/// One entry of an `omap_cmp` assertion map: `std::pair<bufferlist, int>`.
///
/// The OSD evaluates `current <op> value`, where `current` is the key's
/// stored value — for [`CmpOp::Lt`], the assertion holds only when the
/// stored value is less than `value`. A key absent from the object's omap
/// compares as if its value were empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OmapAssertion {
    /// The value to compare the key's current value against.
    pub value: Bytes,
    /// The comparison to apply.
    pub op: CmpOp,
}

impl Denc for OmapAssertion {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        self.value.encode(buf, features)?;
        (self.op as i32).encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        let value = Bytes::decode(buf, features)?;
        let op = CmpOp::try_from(i32::decode(buf, features)?)?;
        Ok(Self { value, op })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(self.value.encoded_size(features)? + 4)
    }
}

/// An op with an empty union and no `indata` (`add_op`): `omap_get_header`
/// and `omap_clear` are the only omap ops shaped this way.
fn bare_op(op: OpCode) -> OSDOp {
    OSDOp {
        op,
        flags: 0,
        op_data: OpData::None,
        indata: Bytes::new(),
    }
}

/// An op that carries `indata`, with an extent union of `(0, indata.len())`
/// (`add_data`) — every other omap op is shaped this way, including the
/// read ops that take arguments, not only the writes.
fn extent_op(op: OpCode, indata: Bytes) -> OSDOp {
    let length = indata.len() as u64;
    OSDOp {
        op,
        flags: 0,
        op_data: OpData::Extent {
            offset: 0,
            length,
            truncate_size: 0,
            truncate_seq: 0,
        },
        indata,
    }
}

impl OSDOp {
    /// Keys after `start_after`. `max_return` should be positive — the OSD
    /// caps it at `osd_max_omap_entries_per_request` and sets `more` when
    /// it truncates the listing. A `max_return` of 0 returns zero entries
    /// (with `more` staying true whenever the object has keys), which
    /// loops forever if used to page by `start_after`.
    pub fn omap_get_keys(start_after: &[u8], max_return: u64) -> Result<Self> {
        let mut buf = BytesMut::new();
        Bytes::copy_from_slice(start_after).encode(&mut buf, 0)?;
        max_return.encode(&mut buf, 0)?;
        Ok(extent_op(OpCode::OmapGetKeys, buf.freeze()))
    }

    /// Entries after `start_after` whose key starts with `filter_prefix`.
    /// `max_return` should be positive, for the same reason as
    /// [`omap_get_keys`](Self::omap_get_keys): 0 returns no entries and
    /// never stops paging.
    pub fn omap_get_vals(
        start_after: &[u8],
        max_return: u64,
        filter_prefix: &[u8],
    ) -> Result<Self> {
        let mut buf = BytesMut::new();
        Bytes::copy_from_slice(start_after).encode(&mut buf, 0)?;
        max_return.encode(&mut buf, 0)?;
        Bytes::copy_from_slice(filter_prefix).encode(&mut buf, 0)?;
        Ok(extent_op(OpCode::OmapGetVals, buf.freeze()))
    }

    /// Values for exactly the given `keys` (missing keys are simply absent).
    pub fn omap_get_vals_by_keys(keys: &OmapKeySet) -> Result<Self> {
        Ok(extent_op(
            OpCode::OmapGetValsByKeys,
            encode_with_capacity(keys, 0)?,
        ))
    }

    /// The object's omap header (separate from its key/value entries).
    pub fn omap_get_header() -> Self {
        bare_op(OpCode::OmapGetHeader)
    }

    /// Set (insert or overwrite) omap entries.
    pub fn omap_set(vals: &OmapMap) -> Result<Self> {
        Ok(extent_op(
            OpCode::OmapSetVals,
            encode_with_capacity(vals, 0)?,
        ))
    }

    /// The header is raw bytes, not length-prefixed.
    pub fn omap_set_header(header: Bytes) -> Self {
        extent_op(OpCode::OmapSetHeader, header)
    }

    /// Remove all omap entries (but not the header).
    pub fn omap_clear() -> Self {
        bare_op(OpCode::OmapClear)
    }

    /// Remove the given `keys`.
    pub fn omap_rm_keys(keys: &OmapKeySet) -> Result<Self> {
        Ok(extent_op(
            OpCode::OmapRmKeys,
            encode_with_capacity(keys, 0)?,
        ))
    }

    /// Removes keys in `[begin, end)`.
    pub fn omap_rm_range(begin: &[u8], end: &[u8]) -> Result<Self> {
        let mut buf = BytesMut::new();
        Bytes::copy_from_slice(begin).encode(&mut buf, 0)?;
        Bytes::copy_from_slice(end).encode(&mut buf, 0)?;
        Ok(extent_op(OpCode::OmapRmKeyRange, buf.freeze()))
    }

    /// Fails the whole transaction with `ECANCELED` when any assertion fails.
    pub fn omap_cmp(assertions: &BTreeMap<OmapKey, OmapAssertion>) -> Result<Self> {
        Ok(extent_op(
            OpCode::OmapCmp,
            encode_with_capacity(assertions, 0)?,
        ))
    }
}

/// Decode the reply to `omap_get_keys`.
pub fn decode_omap_keys(reply: &OpReply) -> Result<OmapKeys> {
    let mut buf = reply.outdata.clone();
    let keys = OmapKeySet::decode(&mut buf, 0)?;
    let more = bool::decode(&mut buf, 0)?;
    Ok(OmapKeys { keys, more })
}

/// Decode the reply to `omap_get_vals`.
pub fn decode_omap_vals(reply: &OpReply) -> Result<OmapVals> {
    let mut buf = reply.outdata.clone();
    let vals = OmapMap::decode(&mut buf, 0)?;
    let more = bool::decode(&mut buf, 0)?;
    Ok(OmapVals { vals, more })
}

/// Decode the reply to `omap_get_vals_by_keys` (no `more` flag: the OSD
/// returns exactly the requested keys that exist).
pub fn decode_omap_vals_by_keys(reply: &OpReply) -> Result<OmapMap> {
    let mut buf = reply.outdata.clone();
    Ok(OmapMap::decode(&mut buf, 0)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_vals_encodes_start_after_max_then_prefix() {
        let op = OSDOp::omap_get_vals(b"k", 7, b"p").expect("op");
        assert_eq!(op.op, OpCode::OmapGetVals);
        assert_eq!(
            op.indata.as_ref(),
            &[1, 0, 0, 0, b'k', 7, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, b'p'][..]
        );
        assert!(matches!(
            op.op_data,
            OpData::Extent {
                offset: 0,
                length: 18,
                truncate_size: 0,
                truncate_seq: 0
            }
        ));
    }

    #[test]
    fn get_keys_encodes_start_after_then_max() {
        let op = OSDOp::omap_get_keys(b"k", 7).expect("op");
        assert_eq!(op.op, OpCode::OmapGetKeys);
        assert_eq!(
            op.indata.as_ref(),
            &[1, 0, 0, 0, b'k', 7, 0, 0, 0, 0, 0, 0, 0][..]
        );
        assert!(matches!(
            op.op_data,
            OpData::Extent {
                offset: 0,
                length: 13,
                truncate_size: 0,
                truncate_seq: 0
            }
        ));
    }

    #[test]
    fn set_carries_extent_of_indata_length() {
        let mut vals = OmapMap::new();
        vals.insert(Bytes::from_static(b"a"), Bytes::from_static(b"xy"));
        let op = OSDOp::omap_set(&vals).expect("op");
        assert_eq!(op.op, OpCode::OmapSetVals);
        assert_eq!(
            op.indata.as_ref(),
            &[1, 0, 0, 0, 1, 0, 0, 0, b'a', 2, 0, 0, 0, b'x', b'y'][..]
        );
        assert!(matches!(
            op.op_data,
            OpData::Extent {
                offset: 0,
                length: 15,
                truncate_size: 0,
                truncate_seq: 0
            }
        ));
    }

    #[test]
    fn set_header_is_raw_bytes() {
        let op = OSDOp::omap_set_header(Bytes::from_static(b"hdr"));
        assert_eq!(op.op, OpCode::OmapSetHeader);
        assert_eq!(op.indata.as_ref(), b"hdr");
        assert!(matches!(
            op.op_data,
            OpData::Extent {
                offset: 0,
                length: 3,
                truncate_size: 0,
                truncate_seq: 0
            }
        ));
    }

    #[test]
    fn rm_range_encodes_begin_then_end() {
        let op = OSDOp::omap_rm_range(b"a", b"c").expect("op");
        assert_eq!(op.op, OpCode::OmapRmKeyRange);
        assert_eq!(
            op.indata.as_ref(),
            &[1, 0, 0, 0, b'a', 1, 0, 0, 0, b'c'][..]
        );
        assert!(matches!(
            op.op_data,
            OpData::Extent {
                offset: 0,
                length: 10,
                truncate_size: 0,
                truncate_seq: 0
            }
        ));
    }

    #[test]
    fn rm_keys_carries_extent() {
        let mut keys = OmapKeySet::new();
        keys.insert(Bytes::from_static(b"a"));
        let op = OSDOp::omap_rm_keys(&keys).expect("op");
        assert_eq!(op.op, OpCode::OmapRmKeys);
        assert_eq!(op.indata.as_ref(), &[1, 0, 0, 0, 1, 0, 0, 0, b'a'][..]);
        assert!(matches!(
            op.op_data,
            OpData::Extent {
                offset: 0,
                length: 9,
                truncate_size: 0,
                truncate_seq: 0
            }
        ));
    }

    #[test]
    fn cmp_encodes_assertion_map() {
        let mut assertions = BTreeMap::new();
        assertions.insert(
            Bytes::from_static(b"a"),
            OmapAssertion {
                value: Bytes::from_static(b"v"),
                op: CmpOp::Eq,
            },
        );
        let op = OSDOp::omap_cmp(&assertions).expect("op");
        assert_eq!(op.op, OpCode::OmapCmp);
        assert_eq!(
            op.indata.as_ref(),
            &[1, 0, 0, 0, 1, 0, 0, 0, b'a', 1, 0, 0, 0, b'v', 1, 0, 0, 0][..]
        );
        assert!(matches!(
            op.op_data,
            OpData::Extent {
                offset: 0,
                length: 18,
                truncate_size: 0,
                truncate_seq: 0
            }
        ));
    }

    #[test]
    fn header_and_clear_have_empty_union() {
        let header = OSDOp::omap_get_header();
        assert_eq!(header.op, OpCode::OmapGetHeader);
        assert!(header.indata.is_empty());
        assert!(matches!(header.op_data, OpData::None));

        let clear = OSDOp::omap_clear();
        assert_eq!(clear.op, OpCode::OmapClear);
        assert!(clear.indata.is_empty());
        assert!(matches!(clear.op_data, OpData::None));
    }

    #[test]
    fn keys_are_bytes_and_survive_non_utf8() {
        let key = Bytes::from_static(&[0x80, b'v', b'1']);
        let mut keys = OmapKeySet::new();
        keys.insert(key.clone());
        let mut buf = BytesMut::new();
        keys.encode(&mut buf, 0).expect("encode");
        let reply = OpReply {
            return_code: 0,
            outdata: {
                let mut b = BytesMut::from(buf.as_ref());
                b.extend_from_slice(&[1]); // more = true
                b.freeze()
            },
        };
        let decoded = decode_omap_keys(&reply).expect("decode");
        assert!(decoded.more);
        assert!(decoded.keys.contains(&key));
    }

    #[test]
    fn assertion_encodes_value_then_op() {
        let a = OmapAssertion {
            value: Bytes::from_static(b"v"),
            op: CmpOp::Gte,
        };
        let mut buf = BytesMut::new();
        a.encode(&mut buf, 0).expect("encode");
        assert_eq!(buf.as_ref(), &[1, 0, 0, 0, b'v', 4, 0, 0, 0][..]);
        let back = OmapAssertion::decode(&mut buf.freeze(), 0).expect("decode");
        assert_eq!(back, a);
    }

    #[test]
    fn decode_vals_reads_map_then_more() {
        let reply = OpReply {
            return_code: 0,
            outdata: Bytes::from_static(&[1, 0, 0, 0, 1, 0, 0, 0, b'a', 1, 0, 0, 0, b'x', 0]),
        };
        let vals = decode_omap_vals(&reply).expect("decode");
        assert!(!vals.more);
        assert_eq!(
            vals.vals.get(&Bytes::from_static(b"a")),
            Some(&Bytes::from_static(b"x"))
        );
    }

    #[test]
    fn decode_vals_by_keys_reads_map_only() {
        let reply = OpReply {
            return_code: 0,
            outdata: Bytes::from_static(&[1, 0, 0, 0, 1, 0, 0, 0, b'a', 1, 0, 0, 0, b'x']),
        };
        let vals = decode_omap_vals_by_keys(&reply).expect("decode");
        assert_eq!(vals.len(), 1);
        assert_eq!(
            vals.get(&Bytes::from_static(b"a")),
            Some(&Bytes::from_static(b"x"))
        );
    }

    #[test]
    fn decode_without_more_byte_is_an_error() {
        let reply = OpReply {
            return_code: 0,
            outdata: Bytes::from_static(&[0, 0, 0, 0]),
        };
        assert!(decode_omap_keys(&reply).is_err());
    }
}
