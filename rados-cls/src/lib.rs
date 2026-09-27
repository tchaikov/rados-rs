//! Client-side encodings for Ceph object classes.
//!
//! One module per class, each behind a Cargo feature of the same name. A
//! module mirrors the class's `cls_*_client.h`: request and reply structs
//! encoded as Ceph does, `OSDOp` constructors for use inside a compound
//! operation, and async functions over [`rados::IoCtx`] for the single-op
//! case. Errors are the OSD's errno for the call, as
//! [`rados::OSDClientError::OSDError`]; a reply that does not decode is
//! [`rados::OSDClientError::Denc`].

/// A one-byte Ceph enum. Ceph decodes any byte and later releases add
/// values, so the newtype keeps the byte; the constants name the values
/// v19 knows. Defined ahead of the `mod` declarations so every module
/// sees it without an import.
#[cfg(any(feature = "rgw", feature = "otp"))]
macro_rules! byte_enum {
    ($(#[$doc:meta])* $name:ident { $($k:ident = $v:expr),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, ::serde::Serialize)]
        #[serde(transparent)]
        pub struct $name(pub u8);

        impl $name {
            $(pub const $k: Self = Self($v);)*
        }

        impl ::rados::Denc for $name {
            fn encode<B: ::bytes::BufMut>(&self, buf: &mut B, features: u64) -> ::std::result::Result<(), ::rados::RadosError> {
                ::rados::Denc::encode(&self.0, buf, features)
            }

            fn decode<B: ::bytes::Buf>(buf: &mut B, features: u64) -> ::std::result::Result<Self, ::rados::RadosError> {
                Ok(Self(<u8 as ::rados::Denc>::decode(buf, features)?))
            }

            fn encoded_size(&self, _features: u64) -> Option<usize> {
                Some(1)
            }
        }
    };
}

// A build with no class has no caller for `call`, so it stays out.
#[cfg(any(
    feature = "version",
    feature = "refcount",
    feature = "user",
    feature = "queue",
    feature = "rgw",
    feature = "rgw_gc",
    feature = "lock",
    feature = "otp"
))]
mod call;
#[cfg(any(
    feature = "refcount",
    feature = "rgw",
    feature = "user",
    feature = "lock",
    feature = "two_pc_queue"
))]
pub(crate) mod dump;

#[cfg(feature = "lock")]
pub mod lock;
#[cfg(feature = "otp")]
pub mod otp;
#[cfg(feature = "queue")]
pub mod queue;
#[cfg(feature = "refcount")]
pub mod refcount;
#[cfg(feature = "rgw")]
pub mod rgw;
#[cfg(feature = "rgw_gc")]
pub mod rgw_gc;
#[cfg(feature = "two_pc_queue")]
pub mod two_pc_queue;
#[cfg(feature = "user")]
pub mod user;
#[cfg(feature = "version")]
pub mod version;
