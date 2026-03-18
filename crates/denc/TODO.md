# Ceph denc-rs Implementation TODO

> **Last Updated**: 2025-02-15

## ✅ Completed

- [x] Core `Denc` trait with buffer-based encode/decode (50-70% perf improvement)
- [x] `VersionedEncode` trait with ENCODE_START/DECODE_START pattern
- [x] `ZeroCopyDencode` derive macro for POD types (in `denc-derive` crate)
- [x] All primitive types: u8–u64, i8–i64, f32, f64, bool
- [x] Collections: `Vec<T>`, `String`, `BTreeMap`, `HashMap`, `Option<T>`, `Bytes`
- [x] Core Ceph types: `EVersion`, `UTime`, `UuidD`, `EntityName`, `EntityType`, `FsId`
- [x] Type-safe IDs: `OsdId`, `PoolId`, `Epoch`, `GlobalId`
- [x] `EntityAddr` / `EntityAddrvec` with MSG_ADDR2 feature support — 100% corpus validated
- [x] `HObject` with Ceph-compatible ordering semantics
- [x] `MonMap`, `MonInfo`, `MonFeature`, `MonCephRelease`, `ElectionStrategy`
- [x] `PgNlsResponse`, `ListObjectImpl`
- [x] Feature flags system (13+ features, JEWEL through TENTACLE incarnation masks)
- [x] Encoding metadata system (`mark_simple_encoding!`, `mark_versioned_encoding!`)
- [x] `Padding<T>` wrapper for JSON-silent binary fields
- [x] 100% corpus validation for `entity_addr_t`, `pg_merge_meta_t`, `pg_pool_t`
- [x] `decode_if_version!` macro for version-conditional decoding
- [x] `impl_ceph_message_payload!` macro for message encoding
- [x] Tuple Denc implementations
- [x] Duration/SystemTime encoding support

## 🚧 Remaining Work

- [ ] Fix derive macro to work inside the denc crate itself (see `denc.rs:925`)
- [ ] Add `FixedSize` implementations for more complex fixed-size types
- [ ] Performance: streaming decode for large objects, memory pool for allocations

## Notes

Complex types like `OSDMap`, `PgPool`, `PgMergeMeta`, and `PgMapTypes` are
implemented in the `osdclient` crate (which depends on `denc`) rather than
in this crate directly, to avoid circular dependencies with `crush`.