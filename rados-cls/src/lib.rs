//! Client-side encodings for Ceph object classes.
//!
//! One module per class, each behind a Cargo feature of the same name. A
//! module mirrors the class's `cls_*_client.h`: request and reply structs
//! encoded as Ceph does, `OSDOp` constructors for use inside a compound
//! operation, and async functions over [`rados::IoCtx`] for the single-op
//! case. Errors are the OSD's errno for the call, as
//! [`rados::OSDClientError::OSDError`]; a reply that does not decode is
//! [`rados::OSDClientError::Denc`].

// A build with no class has no caller for `call`, so it stays out.
#[cfg(any(feature = "version", feature = "refcount"))]
mod call;

#[cfg(feature = "refcount")]
pub mod refcount;
#[cfg(feature = "version")]
pub mod version;
