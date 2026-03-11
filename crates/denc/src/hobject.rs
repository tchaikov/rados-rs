//! hobject_t - Hash Object identifier
//!
//! From ~/dev/ceph/src/common/hobject.h
//!
//! hobject_t is used to identify objects in OSD and contains:
//! - oid: object name
//! - key: locator key
//! - snapid: snapshot ID
//! - hash: hash for CRUSH placement
//! - max: special flag for maximum object
//! - nspace: namespace
//! - pool: pool ID

use crate::codec::{Denc, VersionedEncode};
use crate::error::RadosError;
use bytes::{Buf, BufMut};
use serde::Serialize;

/// hobject_t - Hashed Object ID
///
/// Corresponds to `hobject_t` in Ceph C++ code.
/// Uses ENCODE_START(4, 3, bl) versioned encoding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize)]
pub struct HObject {
    /// Object locator key (usually empty)
    pub key: String,
    /// Object name/ID
    pub oid: String,
    /// Snapshot ID (SNAP_HEAD = -2 for live objects)
    pub snapid: u64,
    /// Hash for CRUSH placement
    pub hash: u32,
    /// Maximum object flag
    pub max: bool,
    /// Namespace (usually empty)
    pub nspace: String,
    /// Pool ID (u64::MAX for meta pool, >= 0 for data pools)
    pub pool: u64,
}

impl PartialOrd for HObject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HObject {
    /// Compare hobject_t objects following Ceph's ordering.
    ///
    /// Reference: ~/dev/ceph/src/common/hobject.h operator<=>
    ///
    /// Ordering: max > pool > hash/key > nspace > oid > snap
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.max
            .cmp(&other.max)
            .then_with(|| self.pool.cmp(&other.pool))
            .then_with(|| {
                // If key is non-empty, compare keys; otherwise compare hashes.
                // Non-empty key sorts after empty key.
                match (self.key.is_empty(), other.key.is_empty()) {
                    (false, false) => self.key.cmp(&other.key),
                    (false, true) => std::cmp::Ordering::Greater,
                    (true, false) => std::cmp::Ordering::Less,
                    (true, true) => self.hash.cmp(&other.hash),
                }
            })
            .then_with(|| self.nspace.cmp(&other.nspace))
            .then_with(|| self.oid.cmp(&other.oid))
            .then_with(|| self.snapid.cmp(&other.snapid))
    }
}

/// Special snapid values
pub const SNAP_HEAD: u64 = u64::MAX - 1; // -2 in two's complement
pub const SNAP_DIR: u64 = u64::MAX; // -1 in two's complement

impl HObject {
    /// Create an empty hobject_t for use as initial cursor
    pub fn empty_cursor(pool: u64) -> Self {
        Self {
            key: String::new(),
            oid: String::new(),
            snapid: SNAP_HEAD,
            hash: 0,
            max: false,
            nspace: String::new(),
            pool,
        }
    }

    /// Create a new hobject_t for a specific object
    pub fn new(pool: u64, oid: String, hash: u32) -> Self {
        Self {
            key: String::new(),
            oid,
            snapid: SNAP_HEAD,
            hash,
            max: false,
            nspace: String::new(),
            pool,
        }
    }
}

impl VersionedEncode for HObject {
    const FEATURE_DEPENDENT: bool = false;

    fn encoding_version(&self, _features: u64) -> u8 {
        4
    }

    fn compat_version(&self, _features: u64) -> u8 {
        3
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Field order from hobject.cc: key, oid, snap, hash, max, nspace, pool
        Denc::encode(&self.key, buf, features)?;
        Denc::encode(&self.oid, buf, features)?;
        Denc::encode(&self.snapid, buf, features)?;
        Denc::encode(&self.hash, buf, features)?;
        Denc::encode(&self.max, buf, features)?;
        Denc::encode(&self.nspace, buf, features)?;
        // pool is u64 internally but i64 on the wire
        Denc::encode(&(self.pool as i64), buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Minimum supported project release boundary (Octopus v15+)
        crate::check_min_version!(version, 4, "HObject", "Octopus v15+");

        // All fields are present in v4+ (which we now require)
        let key = <String as Denc>::decode(buf, features)?;
        let oid = <String as Denc>::decode(buf, features)?;
        let snapid = <u64 as Denc>::decode(buf, features)?;
        let hash = <u32 as Denc>::decode(buf, features)?;
        let max = <bool as Denc>::decode(buf, features)?;
        let nspace = <String as Denc>::decode(buf, features)?;
        let mut pool = <i64 as Denc>::decode(buf, features)?;

        // Compatibility fix for hammer (see hobject.cc)
        if pool == -1 && snapid == 0 && hash == 0 && !max && oid.is_empty() {
            pool = i64::MIN;
        }

        // Convert i64 to u64 (wire format uses i64, internal representation uses u64)
        // Note: Negative values are reinterpreted as large u64 values
        let pool = pool as u64;

        Ok(Self {
            key,
            oid,
            snapid,
            hash,
            max,
            nspace,
            pool,
        })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        // Fixed-size fields: snapid(u64) + hash(u32) + max(bool) + pool(i64) = 21
        const FIXED: usize = 8 + 4 + 1 + 8;
        let key_size = self.key.encoded_size(features)?;
        let oid_size = self.oid.encoded_size(features)?;
        let nspace_size = self.nspace.encoded_size(features)?;
        Some(key_size + oid_size + nspace_size + FIXED)
    }
}

crate::impl_denc_for_versioned!(HObject);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_empty_cursor_encoding() {
        // Create empty cursor for pool 3
        let cursor = HObject::empty_cursor(3);

        // Encode
        let mut buf = BytesMut::new();
        cursor.encode(&mut buf, 0).unwrap();

        // Should be 39 bytes total
        assert_eq!(buf.len(), 39);

        // Verify header
        assert_eq!(buf[0], 4); // version = 4
        assert_eq!(buf[1], 3); // compat_version = 3
                               // bytes 2-5 are struct_len (should be 33 = 0x21)

        println!("Encoded {} bytes", buf.len());
        println!(
            "Hex: {}",
            buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
        );
    }

    #[test]
    fn test_decode_empty_cursor() {
        // Encode then decode
        let original = HObject::empty_cursor(3);
        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        let decoded = HObject::decode(&mut buf.as_ref(), 0).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_corpus_file() {
        // Test against known corpus file (if available)
        // f71a1728c8ccda576480d07b2f3fb310: hash=4, pool=296
        let expected_hex =
            "0403210000000000000000000000feffffffffffffff0400000000000000002801000000000000";
        let expected_bytes = hex::decode(expected_hex).unwrap();

        let hobject = HObject {
            key: String::new(),
            oid: String::new(),
            snapid: SNAP_HEAD,
            hash: 4,
            max: false,
            nspace: String::new(),
            pool: 296,
        };

        let mut buf = BytesMut::new();
        hobject.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.as_ref(), expected_bytes.as_slice());
    }
}
