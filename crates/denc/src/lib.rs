pub mod constants;
pub mod denc;
pub mod encoding_metadata;
pub mod entity_addr;
pub mod error;
pub mod features;
pub mod hobject;
pub mod ids;
pub mod macros;
pub mod monmap;
pub mod padding;
pub mod pg_nls_response;
pub mod types;
pub mod zerocopy;

/// Ceph-compatible CRC32C with explicit initial seed.
///
/// Cross-checked against Ceph (`src/include/crc32c.h`: `ceph_crc32c(crc, data, len)`)
/// and Linux Ceph client (`net/ceph/messenger_v2.c`: `crc32c(seed, buf, len)`).
pub use crc32c::crc32c_append as ceph_crc32c_append;
pub use denc::*;
pub use encoding_metadata::*;
pub use entity_addr::*;
pub use error::*;
pub use features::*;
pub use hobject::*;
pub use monmap::*;
pub use padding::*;
pub use pg_nls_response::*;
pub use types::*;
pub use zerocopy::*;

// Re-export derive macros
pub use denc_derive::{DencMut, ZeroCopyDencode};
