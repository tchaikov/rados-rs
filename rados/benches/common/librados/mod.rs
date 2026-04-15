//! Minimal librados FFI binding.
//!
//! Only the symbols the A/B benchmark actually calls are declared here;
//! this is deliberately not a full librados binding and should not grow
//! past what `adapter_librados.rs` needs.

mod completion;
mod error;
mod ffi;
mod io_ctx;
mod rados;

pub use io_ctx::IoCtx;
pub use rados::Rados;
