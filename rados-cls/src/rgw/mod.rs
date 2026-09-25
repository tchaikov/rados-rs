//! Types the `rgw` object class and the classes RGW layers on it share
//! (`cls_rgw_types.h`, `cls_rgw_ops.h`). The class's own methods come
//! with the bucket-index, GC, usage, lifecycle and OLH work.

pub mod gc;
pub mod types;
