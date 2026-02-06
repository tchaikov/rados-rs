//! Denc implementations for crush crate types
//!
//! This module implements Denc encoding/decoding for types defined in the crush crate.

use crate::denc::{Denc, VersionedEncode};
use crate::error::RadosError;
use bytes::{Buf, BufMut};

// ============= PgId (pg_t) =============

/// Denc implementation for crush::PgId
/// Encoding format matches pg_t::encode() in ~/dev/ceph/src/osd/osd_types.h
impl Denc for crush::PgId {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // Encode version byte
        1u8.encode(buf, 0)?;

        // Encode pool (u64)
        self.pool.encode(buf, 0)?;

        // Encode seed (u32)
        self.seed.encode(buf, 0)?;

        // Encode deprecated preferred field (i32, always -1)
        (-1i32).encode(buf, 0)?;

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        // Decode version byte
        let version = u8::decode(buf, 0)?;
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported PgId version: {}",
                version
            )));
        }

        // Decode pool (u64)
        let pool = u64::decode(buf, 0)?;

        // Decode seed (u32)
        let seed = u32::decode(buf, 0)?;

        // Decode and discard deprecated preferred field (i32)
        let _preferred = i32::decode(buf, 0)?;

        Ok(crush::PgId { pool, seed })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // version (u8) + pool (u64) + seed (u32) + preferred (i32) = 1 + 8 + 4 + 4 = 17
        Some(
            1u8.encoded_size(0)?
                + self.pool.encoded_size(0)?
                + self.seed.encoded_size(0)?
                + (-1i32).encoded_size(0)?,
        )
    }
}

impl crate::denc::FixedSize for crush::PgId {
    const SIZE: usize = 17;
}

// ============= ObjectLocator (object_locator_t) =============

/// Implement VersionedEncode for crush::ObjectLocator
/// Matches C++ object_locator_t encoding from ~/dev/ceph/src/osd/osd_types.cc
/// Uses version 6, compat 3 (or 6 if hash != -1), includes pool, preferred, key, nspace, and hash
impl VersionedEncode for crush::ObjectLocator {
    fn encoding_version(&self, _features: u64) -> u8 {
        6 // Match C++ - version 6 includes pool, preferred, key, nspace, hash
    }

    fn compat_version(&self, _features: u64) -> u8 {
        // If hash != -1, we need version 6 to decode it, otherwise version 3 is sufficient
        if self.hash != -1 {
            6
        } else {
            3
        }
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Verify nobody's corrupted the locator (hash == -1 OR key is empty)
        if self.hash != -1 && !self.key.is_empty() {
            return Err(RadosError::Protocol(
                "ObjectLocator: cannot have both hash and key set".into(),
            ));
        }

        // Encode fields (version 6 format - includes hash)
        self.pool_id.encode(buf, features)?;

        // Encode preferred (always -1 for compatibility with old code)
        let preferred: i32 = -1;
        preferred.encode(buf, features)?;

        self.key.encode(buf, features)?;
        self.namespace.encode(buf, features)?;
        self.hash.encode(buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Decode pool and preferred fields (format changed in v2)
        let pool_id = if struct_v < 2 {
            // Old format: int32_t pool, int16_t preferred
            let op = i32::decode(buf, features)?;
            let _pref = i16::decode(buf, features)?;
            op as u64
        } else {
            // New format: int64_t pool, int32_t preferred
            // Note: Ceph wire format uses i64, but we convert to u64 internally
            let pool = i64::decode(buf, features)?;
            let _preferred = i32::decode(buf, features)?;
            pool as u64
        };

        // Decode key (present in all versions)
        let key = String::decode(buf, features)?;

        // Decode namespace (added in v5)
        let namespace = if struct_v >= 5 {
            String::decode(buf, features)?
        } else {
            String::new()
        };

        // Decode hash (added in v6)
        let hash = if struct_v >= 6 {
            i64::decode(buf, features)?
        } else {
            -1i64
        };

        // Verify that nobody's corrupted the locator (hash == -1 OR key is empty)
        if hash != -1 && !key.is_empty() {
            return Err(RadosError::Protocol(
                "ObjectLocator: cannot have both hash and key set".into(),
            ));
        }

        Ok(Self {
            pool_id,
            key,
            namespace,
            hash,
        })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        // pool_id (i64) + preferred (i32) + key + namespace + hash (i64)
        let pool_size = 8;
        let preferred_size = 4;
        let key_size = self.key.encoded_size(features)?;
        let namespace_size = self.namespace.encoded_size(features)?;
        let hash_size = 8;

        Some(pool_size + preferred_size + key_size + namespace_size + hash_size)
    }
}

crate::impl_denc_for_versioned!(crush::ObjectLocator);
