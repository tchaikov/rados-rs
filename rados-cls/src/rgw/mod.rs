//! Types the `rgw` object class and the classes RGW layers on it share
//! (`cls_rgw_types.h`, `cls_rgw_ops.h`), and the class's methods:
//! [`index`] has the bucket-index, resharding and head-object ones,
//! [`gc`] the omap-era GC ones, [`usage`] the usage-log ones, [`lc`] the
//! lifecycle ones and [`olh`] the object-versioning ones.

/// The class name.
pub const CLASS: &str = "rgw";

pub mod gc;
pub mod index;
pub mod lc;
pub mod olh;
mod packed;
pub mod types;
pub mod usage;
