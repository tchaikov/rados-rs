pub mod denc;
pub mod encoding_metadata;
pub mod entity_addr;
pub mod entity_addr_dencmut;
pub mod error;
pub mod features;
pub mod monmap;
pub mod osdmap;
pub mod padding;
pub mod pgmap_types;
pub mod types;
pub mod zerocopy;

pub use denc::*;
pub use entity_addr::{EntityAddr, EntityAddrType, EntityAddrvec};
pub use error::*;
pub use monmap::{ElectionStrategy, MonCephRelease, MonFeature, MonInfo, MonMap};
pub use osdmap::{OSDMap, PgId};
pub use padding::Padding;
pub use zerocopy::*;

// Re-export types for convenience (use the denc versions, not osdmap duplicates)
pub use denc::{UTime, UuidD};

// Re-export derive macros
pub use denc_derive::{DencMut, ZeroCopyDecode, ZeroCopyDencode, ZeroCopyEncode};
