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

pub mod client;
pub mod denc_types;
pub mod error;
pub mod ioctx;
pub mod messages;
pub mod operation;
pub mod osdmap;
pub mod pgmap_types;
pub mod session;
pub mod throttle;
pub mod tracker;
pub mod types;

// Re-export commonly used types
pub use client::{OSDClient, OSDClientConfig};
pub use error::OSDClientError;
pub use ioctx::IoCtx;
pub use osdmap::{OSDMap, OSDMapIncremental, PgMergeMeta, PgPool, UuidD};
pub use types::{
    OSDOp, ObjectId, OpCode, PoolInfo, ReadResult, SparseExtent, SparseReadResult, StatResult,
    StripedPgId, WriteResult,
};

pub type Result<T> = std::result::Result<T, OSDClientError>;
