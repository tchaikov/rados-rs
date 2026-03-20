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
pub mod list_stream;
pub mod lock;
pub mod messages;
pub mod object_io;
pub mod operation;
pub mod osdmap;
pub mod pg_nls_response;
pub mod pgmap_types;
pub(crate) mod session;
pub mod snapshot;
pub(crate) mod throttle;
pub(crate) mod tracker;
pub mod types;

// Re-export commonly used types
pub use client::{OSDClient, OSDClientConfig};
pub use error::OSDClientError;
pub use ioctx::IoCtx;
pub use list_stream::list_objects_stream;
pub use lock::{LockFlags, LockRequest, LockType, UnlockRequest};
pub use object_io::RadosObject;
pub use operation::{BuiltOp, OpBuilder};
pub use osdmap::{OSDMap, OSDMapIncremental, PgMergeMeta, PgPool, PoolSnapInfo, UuidD};
pub use pg_nls_response::{ListObjectImpl, PgNlsResponse};
pub use pgmap_types::{ObjectstorePerfStat, PoolStat};
pub use snapshot::SnapId;
pub use types::{
    OSDOp, ObjectId, ObjectLocator, OpCode, OpState, OpTarget, OsdOpFlags, PoolInfo, ReadResult,
    SparseExtent, SparseReadResult, StatResult, StripedPgId, WriteResult,
};

// Re-export Result type alias from error module
pub use error::Result;
