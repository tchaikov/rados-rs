//! pg_nls_response_t - PG namespace list response
//!
//! From ~/dev/ceph/src/osd/osd_types.h
//!
//! This is the response type for PGLS (PG List) operations.
//! In C++, it's defined as pg_nls_response_template<librados::ListObjectImpl>

use crate::denc::{Denc, VersionedEncode};
use crate::error::RadosError;
use crate::hobject::HObject;
use bytes::{Buf, BufMut};
use serde::Serialize;

/// ListObjectImpl - Object entry in a list response
///
/// Corresponds to `librados::ListObjectImpl` in Ceph C++ code.
/// Contains namespace, object ID, and locator key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ListObjectImpl {
    /// Namespace (usually empty)
    pub nspace: String,
    /// Object name/ID
    pub oid: String,
    /// Object locator key (usually empty)
    pub locator: String,
}

impl ListObjectImpl {
    /// Create a new ListObjectImpl
    pub fn new(nspace: String, oid: String, locator: String) -> Self {
        Self {
            nspace,
            oid,
            locator,
        }
    }
}

/// PgNlsResponse - PG namespace list response
///
/// Corresponds to `pg_nls_response_template<librados::ListObjectImpl>` in Ceph C++ code.
/// Uses ENCODE_START(1, 1, bl) versioned encoding.
///
/// This type is used as the response for PGLS (PG List) operations.
/// The handle is a cursor (hobject_t) for pagination, and entries contains
/// the list of objects returned.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PgNlsResponse {
    /// Cursor for next iteration (hobject_t)
    /// When handle.hash == u32::MAX, it indicates end of PG
    pub handle: HObject,
    /// List of object entries
    pub entries: Vec<ListObjectImpl>,
}

impl PgNlsResponse {
    /// Create a new empty response
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a response with handle and entries
    pub fn with_entries(handle: HObject, entries: Vec<ListObjectImpl>) -> Self {
        Self { handle, entries }
    }
}

impl VersionedEncode for PgNlsResponse {
    const FEATURE_DEPENDENT: bool = false;

    fn encoding_version(&self, _features: u64) -> u8 {
        1
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // From pg_nls_response_template::encode():
        // 1. encode(handle, bl)
        // 2. encode(n, bl) where n = entries.size()
        // 3. for each entry:
        //    - encode(i->nspace, bl)
        //    - encode(i->oid, bl)
        //    - encode(i->locator, bl)

        // Encode handle (hobject_t)
        Denc::encode(&self.handle, buf, features)?;

        // Encode entry count
        let n = self.entries.len() as u32;
        Denc::encode(&n, buf, features)?;

        // Encode each entry's fields
        for entry in &self.entries {
            Denc::encode(&entry.nspace, buf, features)?;
            Denc::encode(&entry.oid, buf, features)?;
            Denc::encode(&entry.locator, buf, features)?;
        }

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Decode handle
        let handle = <HObject as Denc>::decode(buf, features)?;

        // Decode entry count
        let n = <u32 as Denc>::decode(buf, features)?;

        // Decode entries
        let mut entries = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let nspace = <String as Denc>::decode(buf, features)?;
            let oid = <String as Denc>::decode(buf, features)?;
            let locator = <String as Denc>::decode(buf, features)?;
            entries.push(ListObjectImpl {
                nspace,
                oid,
                locator,
            });
        }

        Ok(Self { handle, entries })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        // Handle size
        let handle_size = self.handle.encoded_size(features)?;

        // Entry count: u32 = 4 bytes
        let count_size = 4;

        // Entries size
        let mut entries_size = 0;
        for entry in &self.entries {
            entries_size += entry.nspace.encoded_size(features)?;
            entries_size += entry.oid.encoded_size(features)?;
            entries_size += entry.locator.encoded_size(features)?;
        }

        Some(handle_size + count_size + entries_size)
    }
}

crate::impl_denc_for_versioned!(PgNlsResponse);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hobject::SNAP_HEAD;
    use bytes::BytesMut;

    #[test]
    fn test_empty_response() {
        let response = PgNlsResponse::new();
        let mut buf = BytesMut::new();
        response.encode(&mut buf, 0).unwrap();

        let decoded = PgNlsResponse::decode(&mut buf.as_ref(), 0).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn test_response_with_entries() {
        let handle = HObject {
            key: String::new(),
            oid: String::new(),
            snapid: SNAP_HEAD,
            hash: 4,
            max: false,
            nspace: String::new(),
            pool: 3,
        };

        let entries = vec![
            ListObjectImpl::new(String::new(), "obj1".to_string(), String::new()),
            ListObjectImpl::new(String::new(), "obj2".to_string(), "key2".to_string()),
            ListObjectImpl::new("ns3".to_string(), "obj3".to_string(), String::new()),
        ];

        let response = PgNlsResponse::with_entries(handle.clone(), entries.clone());

        let mut buf = BytesMut::new();
        response.encode(&mut buf, 0).unwrap();

        let decoded = PgNlsResponse::decode(&mut buf.as_ref(), 0).unwrap();
        assert_eq!(response, decoded);
        assert_eq!(decoded.handle, handle);
        assert_eq!(decoded.entries.len(), 3);
        assert_eq!(decoded.entries[0].oid, "obj1");
        assert_eq!(decoded.entries[1].locator, "key2");
        assert_eq!(decoded.entries[2].nspace, "ns3");
    }

    #[test]
    fn test_encoded_size() {
        let handle = HObject::empty_cursor(3);
        let entries = vec![ListObjectImpl::new(
            String::new(),
            "test".to_string(),
            String::new(),
        )];
        let response = PgNlsResponse::with_entries(handle, entries);

        let mut buf = BytesMut::new();
        response.encode(&mut buf, 0).unwrap();

        let calculated_size = response.encoded_size(0).unwrap();
        assert_eq!(buf.len(), calculated_size);
    }
}
