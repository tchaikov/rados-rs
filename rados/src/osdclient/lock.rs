//! Object locking support via cls_lock
//!
//! Implements distributed object locking using Ceph's cls_lock object class.

use crate::denc::{Denc, RadosError};
use bytes::{Buf, BufMut};
use std::time::Duration;

/// Lock type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockType {
    None = 0,
    Exclusive = 1,
    Shared = 2,
    ExclusiveEphemeral = 3,
}

bitflags::bitflags! {
    /// Lock flags
    pub struct LockFlags: u8 {
        const MAY_RENEW = 0x01;
        const MUST_RENEW = 0x02;
    }
}

/// Lock request structure
pub struct LockRequest {
    pub name: String,
    pub lock_type: LockType,
    pub cookie: String,
    pub tag: String,
    pub description: String,
    pub duration: Duration,
    pub flags: LockFlags,
}

impl Denc for LockRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        use bytes::BytesMut;

        // Matches cls_lock_lock_op::encode()'s ENCODE_START(1, 1, bl): a
        // struct_v(u8) + struct_compat(u8) + content_len(u32 LE) header in
        // front of the fields, which cls_lock's decode requires to parse
        // the op at all.
        let mut content = BytesMut::with_capacity(64);
        self.name.encode(&mut content, features)?;
        (self.lock_type as u8).encode(&mut content, features)?;
        self.cookie.encode(&mut content, features)?;
        self.tag.encode(&mut content, features)?;
        self.description.encode(&mut content, features)?;
        (self.duration.as_secs() as u32).encode(&mut content, features)?;
        self.duration
            .subsec_nanos()
            .encode(&mut content, features)?;
        self.flags.bits().encode(&mut content, features)?;

        buf.put_u8(1); // struct_v
        buf.put_u8(1); // struct_compat
        buf.put_u32_le(content.len() as u32);
        buf.put_slice(&content);
        Ok(())
    }

    fn decode<B: Buf>(_buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        Err(RadosError::Protocol(
            "LockRequest decode is not supported".into(),
        ))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        None
    }
}

/// Unlock request structure
///
/// Matches `cls_lock_unlock_op`, whose `encode()` uses `ENCODE_START(1, 1,
/// bl)` — hence `VersionedDenc` (struct_v/struct_compat/len header) rather
/// than the plain `Denc` derive.
#[derive(crate::VersionedDenc)]
#[denc(crate = "crate", version = 1, compat = 1)]
pub struct UnlockRequest {
    pub name: String,
    pub cookie: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_unlock_request_encodes_ceph_version_header() {
        // Matches cls_lock_unlock_op::encode()'s ENCODE_START(1, 1, bl):
        // struct_v(1) + struct_compat(1) + content_len(u32 LE) + content,
        // where content is `name` then `cookie`, each a denc string (u32 LE
        // length + bytes).
        let req = UnlockRequest {
            name: "n".to_string(),
            cookie: "c".to_string(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 0).expect("encode");

        #[rustfmt::skip]
        let expected: &[u8] = &[
            1, 1,             // struct_v, struct_compat
            10, 0, 0, 0,      // content_len (u32 LE): 5 (name) + 5 (cookie)
            1, 0, 0, 0, b'n', // name: u32 LE len + bytes
            1, 0, 0, 0, b'c', // cookie: u32 LE len + bytes
        ];
        assert_eq!(&buf[..], expected);
    }

    #[test]
    fn test_lock_request_encodes_ceph_version_header() {
        // Matches cls_lock_lock_op::encode()'s ENCODE_START(1, 1, bl), with
        // fields in order: name, type (u8), cookie, tag, description,
        // duration (u32 secs + u32 nanos), flags (u8).
        let req = LockRequest {
            name: "n".to_string(),
            lock_type: LockType::Exclusive,
            cookie: "c".to_string(),
            tag: String::new(),
            description: String::new(),
            duration: Duration::ZERO,
            flags: LockFlags::empty(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 0).expect("encode");

        #[rustfmt::skip]
        let expected: &[u8] = &[
            1, 1,             // struct_v, struct_compat
            28, 0, 0, 0,      // content_len (u32 LE)
            1, 0, 0, 0, b'n', // name
            1,                // lock_type = Exclusive
            1, 0, 0, 0, b'c', // cookie
            0, 0, 0, 0,       // tag (empty)
            0, 0, 0, 0,       // description (empty)
            0, 0, 0, 0,       // duration secs
            0, 0, 0, 0,       // duration nanos
            0,                // flags (empty)
        ];
        assert_eq!(&buf[..], expected);
    }
}
