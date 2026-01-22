//! Denc implementations for crush crate types
//!
//! This module implements Denc encoding/decoding for types defined in the crush crate.

use crate::denc::{Denc, VersionedEncode};
use crate::error::RadosError;
use bytes::{Buf, BufMut};

/// Implement VersionedEncode for crush::ObjectLocator
/// Matches C++ object_locator_t encoding from src/osd/osd_types.cc
impl VersionedEncode for crush::ObjectLocator {
    fn encoding_version(&self, _features: u64) -> u8 {
        6
    }

    fn compat_version(&self, _features: u64) -> u8 {
        // Compat version depends on whether hash is set
        // If hash != -1, we need version 6 to decode it
        // Otherwise, version 3 is sufficient
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

        // Encode fields
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
            op as i64
        } else {
            // New format: int64_t pool, int32_t preferred
            let pool = i64::decode(buf, features)?;
            let _preferred = i32::decode(buf, features)?;
            pool
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
            -1
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
}

/// Implement Denc for crush::ObjectLocator by delegating to VersionedEncode
impl Denc for crush::ObjectLocator {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // Variable size due to strings
        None
    }
}
