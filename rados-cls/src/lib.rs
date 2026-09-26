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
#[cfg(any(
    feature = "version",
    feature = "refcount",
    feature = "user",
    feature = "queue",
    feature = "rgw",
    feature = "rgw_gc"
))]
mod call;
#[cfg(any(feature = "refcount", feature = "rgw", feature = "user"))]
pub(crate) mod dump;

#[cfg(feature = "queue")]
pub mod queue;
#[cfg(feature = "refcount")]
pub mod refcount;
#[cfg(feature = "rgw")]
pub mod rgw;
#[cfg(feature = "rgw_gc")]
pub mod rgw_gc;
#[cfg(feature = "user")]
pub mod user;
#[cfg(feature = "version")]
pub mod version;
