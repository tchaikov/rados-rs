//! CRUSH (Controlled Replication Under Scalable Hashing) algorithm implementation
//!
//! This crate provides a Rust implementation of the CRUSH algorithm used by Ceph
//! for data placement and replication. It includes support for:
//!
//! - CRUSH map decoding and manipulation
//! - All bucket selection algorithms (Uniform, List, Tree, Straw, Straw2)
//! - CRUSH rule execution for object placement
//! - Device classes for tiered storage (SSD, HDD, NVMe, etc.)
//! - Object to PG and PG to OSD mapping
//!
//! ## Device Classes
//!
//! Device classes allow OSDs to be organized by storage media type. Common classes include:
//! - `ssd` - Solid State Drives
//! - `hdd` - Hard Disk Drives
//! - `nvme` - NVMe SSDs
//!
//! CRUSH rules can be configured to place data on specific device classes, enabling
//! tiered storage strategies (e.g., hot data on SSDs, cold data on HDDs).
//!
//! ## Example
//!
//! ```rust
//! use crush::{CrushMap, PgId};
//! use bytes::Bytes;
//!
//! // Decode a CRUSH map from bytes
//! let data = /* binary CRUSH map data */;
//! # let data = Bytes::new();
//! # let mut data_copy = data.clone();
//! # if false {
//! let crush_map = CrushMap::decode(&mut data)?;
//!
//! // Check device class for an OSD
//! if let Some(class) = crush_map.get_device_class(0) {
//!     println!("OSD 0 is in class: {}", class);
//! }
//!
//! // Map a PG to OSDs using CRUSH rules
//! let pg = PgId { pool: 1, seed: 0 };
//! let osds = crush::pg_to_osds(&crush_map, &pg, 0, &[])?;
//! # }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod bucket;
pub mod crush_ln_table;
pub mod decode;
pub mod error;
pub mod hash;
pub mod mapper;
pub mod placement;
pub mod types;

pub use error::{CrushError, Result};
pub use placement::{object_to_osds, object_to_pg, pg_to_osds, ObjectLocator, PgId};
pub use types::{
    BucketAlgorithm, BucketData, CrushBucket, CrushMap, CrushRule, CrushRuleStep, RuleOp, RuleType,
};
