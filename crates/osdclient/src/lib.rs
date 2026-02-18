//! RADOS OSD Client
//!
//! This crate provides a Rust implementation of a RADOS OSD client for performing
//! object operations (read, write, stat, delete) against a Ceph cluster.
//!
//! # Architecture
//!
//! - `OSDClient`: Main entry point for performing object operations
//! - `OSDSession`: Per-OSD connection manager
//! - Message encoding/decoding for MOSDOp and MOSDOpReply
//! - Integration with MonClient for OSDMap updates and CRUSH placement

pub(crate) mod backoff;
pub mod client;
pub(crate) mod denc_types;
pub mod error;
pub mod ioctx;
pub mod lock;
pub mod messages;
pub mod operation;
pub mod osdmap;
pub(crate) mod pgmap_types;
pub mod session;
pub(crate) mod throttle;
pub(crate) mod tracker;
pub mod types;

// Re-export commonly used types
pub use client::{OSDClient, OSDClientConfig};
pub use error::OSDClientError;
pub use ioctx::IoCtx;
pub use lock::{LockFlags, LockRequest, LockType, UnlockRequest};
pub use operation::{BuiltOp, OpBuilder};
pub use osdmap::{OSDMap, OSDMapIncremental, PgMergeMeta, PgPool, UuidD};
pub use types::{
    OSDOp, ObjectId, OpCode, OpState, OpTarget, OsdOpFlags, PoolInfo, ReadResult, SparseExtent,
    SparseReadResult, StatResult, StripedPgId, WriteResult,
};

pub type Result<T> = std::result::Result<T, OSDClientError>;
