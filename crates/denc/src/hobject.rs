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

use crate::denc::{Denc, VersionedEncode};
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
    /// Compare hobject_t objects following Ceph's ordering
    ///
    /// Reference: ~/dev/ceph/src/common/hobject.h operator<=>
    ///
    /// Ordering: max > pool > hash/key > nspace > oid > snap
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        // 1. max field (reversed: max objects come last)
        match self.max.cmp(&other.max) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // 2. pool
        match self.pool.cmp(&other.pool) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // 3. bitwise key (hash or key)
        // If key is empty, use hash; otherwise use key string
        let self_bitwise = if self.key.is_empty() {
            None
        } else {
            Some(&self.key)
        };
        let other_bitwise = if other.key.is_empty() {
            None
        } else {
            Some(&other.key)
        };

        match (self_bitwise, other_bitwise) {
            (Some(k1), Some(k2)) => match k1.cmp(k2) {
                Ordering::Equal => {}
                ord => return ord,
            },
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => {
                // Both use hash
                match self.hash.cmp(&other.hash) {
                    Ordering::Equal => {}
                    ord => return ord,
                }
            }
        }

        // 4. nspace
        match self.nspace.cmp(&other.nspace) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // 5. oid
        match self.oid.cmp(&other.oid) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // 6. snap
        self.snapid.cmp(&other.snapid)
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
        // Encoding order from hobject.cc:
        // 1. key (string)
        // 2. oid (string)
        // 3. snap (uint64)
        // 4. hash (uint32)
        // 5. max (bool)
        // 6. nspace (string)
        // 7. pool (int64)

        // encode(key, bl)
        Denc::encode(&self.key, buf, features)?;

        // encode(oid, bl)
        Denc::encode(&self.oid, buf, features)?;

        // encode(snap, bl)
        Denc::encode(&self.snapid, buf, features)?;

        // encode(hash, bl)
        Denc::encode(&self.hash, buf, features)?;

        // encode(max, bl)
        Denc::encode(&self.max, buf, features)?;

        // encode(nspace, bl)
        Denc::encode(&self.nspace, buf, features)?;

        // encode(pool, bl) - convert u64 to i64 for wire format
        // Note: Large u64 values are reinterpreted as negative i64 values
        Denc::encode(&(self.pool as i64), buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Version 1: added key field
        // Version 2: added max field
        // Version 4: added nspace and pool fields

        let key = if version >= 1 {
            <String as Denc>::decode(buf, features)?
        } else {
            String::new()
        };

        let oid = <String as Denc>::decode(buf, features)?;
        let snapid = <u64 as Denc>::decode(buf, features)?;
        let hash = <u32 as Denc>::decode(buf, features)?;

        let max = if version >= 2 {
            <bool as Denc>::decode(buf, features)?
        } else {
            false
        };

        let (nspace, pool) = if version >= 4 {
            let ns = <String as Denc>::decode(buf, features)?;
            let mut p = <i64 as Denc>::decode(buf, features)?;

            // Compatibility fix for hammer (see hobject.cc)
            if p == -1 && snapid == 0 && hash == 0 && !max && oid.is_empty() {
                p = i64::MIN;
            }

            // Convert i64 to u64 (wire format uses i64, internal representation uses u64)
            // Note: Negative values are reinterpreted as large u64 values
            (ns, p as u64)
        } else {
            // Default to u64::MAX (equivalent to -1 in i64)
            (String::new(), u64::MAX)
        };

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
        // Content size calculation (without VERSION_HEADER_SIZE)
        let key_size = self.key.encoded_size(features)?;
        let oid_size = self.oid.encoded_size(features)?;
        let snap_size = 8; // u64
        let hash_size = 4; // u32
        let max_size = 1; // bool
        let nspace_size = self.nspace.encoded_size(features)?;
        let pool_size = 8; // i64

        Some(key_size + oid_size + snap_size + hash_size + max_size + nspace_size + pool_size)
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
