//! Denc implementations for crush crate types
//!
//! This module implements Denc encoding/decoding for types defined in the crush crate.

use crate::denc::{Denc, VersionedEncode};
use crate::error::RadosError;
use bytes::{Buf, BufMut};

/// Implement VersionedEncode for crush::ObjectLocator
/// Matches Linux kernel encoding (version 5) from ~/dev/linux/net/ceph/osd_client.c
/// The Linux kernel uses version 5, compat 4, which includes pool and namespace but NOT hash
impl VersionedEncode for crush::ObjectLocator {
    fn encoding_version(&self, _features: u64) -> u8 {
        5 // Match Linux kernel - version 5 includes pool, preferred, key, namespace (NO hash)
    }

    fn compat_version(&self, _features: u64) -> u8 {
        4 // Match Linux kernel - version 4 is minimum to decode namespace
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

        // Encode fields (version 5 format - NO hash field)
        self.pool_id.encode(buf, features)?;

        // Encode preferred (always -1 for compatibility with old code)
        let preferred: i32 = -1;
        preferred.encode(buf, features)?;

        self.key.encode(buf, features)?;
        self.namespace.encode(buf, features)?;
        // DO NOT encode hash - version 5 doesn't include it

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

        // hash field added in v6, but we don't use it (matching Linux kernel which uses v5)
        // Always set hash to -1
        let hash = -1i64;

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
