//! Core types for OSD client operations

use bytes::Bytes;
use std::time::SystemTime;

/// Object identification (corresponds to hobject_t in Ceph)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectId {
    /// Pool ID
    pub pool: i64,
    /// Object name
    pub oid: String,
    /// Snapshot ID (SNAP_HEAD for current version)
    pub snap: u64,
    /// Hash for CRUSH placement
    pub hash: u32,
    /// Namespace (usually empty)
    pub namespace: String,
    /// Object locator key (usually empty)
    pub key: String,
}

/// Special snapshot ID values
pub const SNAP_HEAD: u64 = u64::MAX;
pub const SNAP_DIR: u64 = u64::MAX - 1;

impl ObjectId {
    /// Create a new ObjectId for the current version of an object
    pub fn new(pool: i64, oid: impl Into<String>) -> Self {
        Self {
            pool,
            oid: oid.into(),
            snap: SNAP_HEAD,
            hash: 0, // Will be calculated from oid
            namespace: String::new(),
            key: String::new(),
        }
    }

    /// Create ObjectId with explicit namespace
    pub fn with_namespace(pool: i64, oid: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            pool,
            oid: oid.into(),
            snap: SNAP_HEAD,
            hash: 0,
            namespace: namespace.into(),
            key: String::new(),
        }
    }

    /// Calculate the hash for CRUSH placement using Ceph's rjenkins hash
    pub fn calculate_hash(&mut self) {
        use crush::hash::ceph_str_hash_rjenkins;

        // Hash the object name using Ceph's rjenkins hash function
        // This matches the behavior of ceph_str_hash_rjenkins() in Ceph
        self.hash = ceph_str_hash_rjenkins(self.oid.as_bytes());
    }
}

/// Striped placement group ID (corresponds to spg_t in Ceph)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StripedPgId {
    /// Placement group ID
    pub pool: i64,
    pub seed: u32,
    /// OSD shard ID (-1 for replicated pools)
    pub shard: i8,
}

impl StripedPgId {
    pub fn new(pool: i64, seed: u32, shard: i8) -> Self {
        Self { pool, seed, shard }
    }

    /// Create from PgId (for replicated pools)
    pub fn from_pg(pool: i64, seed: u32) -> Self {
        Self {
            pool,
            seed,
            shard: -1,
        }
    }
}

/// Request identifier for tracking operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestId {
    /// Client entity name
    pub entity_name: String,
    /// Transaction ID (unique per client)
    pub tid: u64,
    /// Client incarnation
    pub inc: i32,
}

impl RequestId {
    pub fn new(entity_name: impl Into<String>, tid: u64, inc: i32) -> Self {
        Self {
            entity_name: entity_name.into(),
            tid,
            inc,
        }
    }
}

/// OSD operation codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum OpCode {
    /// Read operation
    Read = 1,
    /// Write operation
    Write = 2,
    /// Truncate operation
    Truncate = 3,
    /// Stat operation
    Stat = 4,
    /// Delete operation
    Delete = 8,
    /// Get extended attribute
    GetXattr = 9,
    /// Set extended attribute
    SetXattr = 10,
    /// Write full object
    WriteFull = 17,
    /// Create object
    Create = 21,
}

impl OpCode {
    pub fn as_u16(self) -> u16 {
        self as u16
    }
}

/// Extent for read/write operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    /// Offset in bytes
    pub offset: u64,
    /// Length in bytes
    pub length: u64,
    /// Truncate size (0 = no truncate)
    pub truncate_size: u64,
    /// Truncate sequence number
    pub truncate_seq: u32,
}

impl Extent {
    pub fn new(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length,
            truncate_size: 0,
            truncate_seq: 0,
        }
    }
}

/// Single OSD operation
#[derive(Debug, Clone)]
pub struct OSDOp {
    /// Operation code
    pub op: OpCode,
    /// Operation flags
    pub flags: u32,
    /// Extent for read/write operations
    pub extent: Option<Extent>,
    /// Input data payload
    pub indata: Bytes,
}

impl OSDOp {
    /// Create a read operation
    pub fn read(offset: u64, length: u64) -> Self {
        Self {
            op: OpCode::Read,
            flags: 0,
            extent: Some(Extent::new(offset, length)),
            indata: Bytes::new(),
        }
    }

    /// Create a write operation
    pub fn write(offset: u64, data: Bytes) -> Self {
        Self {
            op: OpCode::Write,
            flags: 0,
            extent: Some(Extent::new(offset, data.len() as u64)),
            indata: data,
        }
    }

    /// Create a write_full operation
    pub fn write_full(data: Bytes) -> Self {
        Self {
            op: OpCode::WriteFull,
            flags: 0,
            extent: Some(Extent::new(0, data.len() as u64)),
            indata: data,
        }
    }

    /// Create a stat operation
    pub fn stat() -> Self {
        Self {
            op: OpCode::Stat,
            flags: 0,
            extent: None,
            indata: Bytes::new(),
        }
    }

    /// Create a delete operation
    pub fn delete() -> Self {
        Self {
            op: OpCode::Delete,
            flags: 0,
            extent: None,
            indata: Bytes::new(),
        }
    }
}

/// Result of a read operation
#[derive(Debug, Clone)]
pub struct ReadResult {
    /// Data read from the object
    pub data: Bytes,
    /// Object version
    pub version: u64,
}

/// Result of a write operation
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Object version after write
    pub version: u64,
}

/// Result of a stat operation
#[derive(Debug, Clone)]
pub struct StatResult {
    /// Object size in bytes
    pub size: u64,
    /// Modification time
    pub mtime: SystemTime,
}

/// Generic operation result
#[derive(Debug, Clone)]
pub struct OpResult {
    /// Overall result code (0 = success)
    pub result: i32,
    /// Object version
    pub version: u64,
    /// User version
    pub user_version: u64,
    /// Per-operation results
    pub ops: Vec<OpReply>,
}

/// Single operation reply
#[derive(Debug, Clone)]
pub struct OpReply {
    /// Return code for this operation (0 = success)
    pub return_code: i32,
    /// Output data
    pub outdata: Bytes,
}
