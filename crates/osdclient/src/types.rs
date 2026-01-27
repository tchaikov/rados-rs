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

/// Special snapshot ID values (from Ceph's rados.h)
/// CEPH_SNAPDIR: reserved for hidden .snap dir
pub const SNAP_DIR: u64 = u64::MAX; // -1 in two's complement
/// CEPH_NOSNAP: "head", "live" revision (normal object)
pub const SNAP_HEAD: u64 = u64::MAX - 1; // -2 in two's complement

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

/// OSD request flags (from Ceph's rados.h)
pub mod flags {
    /// Want (or is) "ack" acknowledgment
    pub const CEPH_OSD_FLAG_ACK: u32 = 0x0001;
    /// Want (or is) "ondisk" acknowledgment
    pub const CEPH_OSD_FLAG_ONDISK: u32 = 0x0004;
    /// Op may read
    pub const CEPH_OSD_FLAG_READ: u32 = 0x0010;
    /// Op may write
    pub const CEPH_OSD_FLAG_WRITE: u32 = 0x0020;
    /// PG operation, no object
    pub const CEPH_OSD_FLAG_PGOP: u32 = 0x0400;

    // Internal OSD op flags (RMW flags) - set based on op types
    // These are per-operation flags, not message-level flags
    pub const CEPH_OSD_RMW_FLAG_READ: u32 = 1 << 1;
    pub const CEPH_OSD_RMW_FLAG_WRITE: u32 = 1 << 2;
    pub const CEPH_OSD_RMW_FLAG_CLASS_READ: u32 = 1 << 3;
    pub const CEPH_OSD_RMW_FLAG_CLASS_WRITE: u32 = 1 << 4;
    pub const CEPH_OSD_RMW_FLAG_PGOP: u32 = 1 << 5;
    pub const CEPH_OSD_RMW_FLAG_CACHE: u32 = 1 << 6;
    pub const CEPH_OSD_RMW_FLAG_FORCE_PROMOTE: u32 = 1 << 7;
    pub const CEPH_OSD_RMW_FLAG_SKIP_HANDLE_CACHE: u32 = 1 << 8;
    pub const CEPH_OSD_RMW_FLAG_SKIP_PROMOTE: u32 = 1 << 9;
    pub const CEPH_OSD_RMW_FLAG_RWORDERED: u32 = 1 << 10;
    pub const CEPH_OSD_RMW_FLAG_RETURNVEC: u32 = 1 << 11;
}

// OSD operation modes (from Ceph's rados.h)
const CEPH_OSD_OP_MODE_RD: u16 = 0x1000; // Read mode
const CEPH_OSD_OP_MODE_WR: u16 = 0x2000; // Write mode
#[allow(dead_code)]
const CEPH_OSD_OP_MODE_RMW: u16 = 0x3000; // Read-modify-write mode

// OSD operation types (from Ceph's rados.h)
const CEPH_OSD_OP_TYPE_DATA: u16 = 0x0200; // Data operations
const CEPH_OSD_OP_TYPE_ATTR: u16 = 0x0300; // Attribute operations
const CEPH_OSD_OP_TYPE_PG: u16 = 0x0500; // PG operations

/// Helper macro to construct operation codes using Ceph's encoding scheme
/// Matches __CEPH_OSD_OP(mode, type, nr) macro from rados.h
macro_rules! osd_op {
    (RD, DATA, $nr:expr) => {
        CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | $nr
    };
    (WR, DATA, $nr:expr) => {
        CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | $nr
    };
    (RMW, DATA, $nr:expr) => {
        CEPH_OSD_OP_MODE_RMW | CEPH_OSD_OP_TYPE_DATA | $nr
    };
    (RD, ATTR, $nr:expr) => {
        CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_ATTR | $nr
    };
    (WR, ATTR, $nr:expr) => {
        CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | $nr
    };
    (RD, PG, $nr:expr) => {
        CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_PG | $nr
    };
}

/// OSD operation codes
///
/// These values are calculated using Ceph's macro system to compose:
/// - MODE (read/write/rmw) - bits 12-15
/// - TYPE (data/attr/exec/pg) - bits 8-11
/// - Operation number - bits 0-7
///
/// This matches the C++ __CEPH_OSD_OP macro from include/rados.h
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum OpCode {
    /// Read operation: __CEPH_OSD_OP(RD, DATA, 1)
    Read = osd_op!(RD, DATA, 1),
    /// Stat operation: __CEPH_OSD_OP(RD, DATA, 2)
    Stat = osd_op!(RD, DATA, 2),
    /// Write operation: __CEPH_OSD_OP(WR, DATA, 1)
    Write = osd_op!(WR, DATA, 1),
    /// Write full object: __CEPH_OSD_OP(WR, DATA, 2)
    WriteFull = osd_op!(WR, DATA, 2),
    /// Truncate operation: __CEPH_OSD_OP(WR, DATA, 3)
    Truncate = osd_op!(WR, DATA, 3),
    /// Delete operation: __CEPH_OSD_OP(WR, DATA, 5)
    Delete = osd_op!(WR, DATA, 5),
    /// Create object: __CEPH_OSD_OP(WR, DATA, 13)
    Create = osd_op!(WR, DATA, 13),
    /// Get extended attribute: __CEPH_OSD_OP(RD, ATTR, 1)
    GetXattr = osd_op!(RD, ATTR, 1),
    /// Set extended attribute: __CEPH_OSD_OP(WR, ATTR, 1)
    SetXattr = osd_op!(WR, ATTR, 1),
    /// PG list operation: __CEPH_OSD_OP(RD, PG, 1) = PGLS
    Pgls = osd_op!(RD, PG, 1),
}

impl OpCode {
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    /// Check if this operation is a read operation
    pub fn is_read(self) -> bool {
        (self as u16) & CEPH_OSD_OP_MODE_RD != 0
    }

    /// Check if this operation is a write operation
    pub fn is_write(self) -> bool {
        (self as u16) & CEPH_OSD_OP_MODE_WR != 0
    }

    /// Check if this is a PG operation (operates on placement group, not object)
    pub fn is_pg_op(self) -> bool {
        (self as u16) & CEPH_OSD_OP_TYPE_PG != 0
    }
}

/// Operation-specific data for different OSD operations
///
/// Each OSD operation type has its own specific fields as a variant.
/// This enforces type safety and makes it impossible to accidentally
/// use the wrong fields for an operation.
#[derive(Debug, Clone)]
pub enum OpData {
    /// Extent-based operations (read, write, etc.)
    Extent {
        offset: u64,
        length: u64,
        truncate_size: u64,
        truncate_seq: u32,
    },
    /// PG list operation (for object listing)
    Pgls { max_entries: u64, start_epoch: u32 },
    /// Extended attribute operations
    Xattr {
        name_len: u32,
        value_len: u32,
        cmp_op: u8,
        cmp_mode: u8,
    },
    /// Operations with no specific data
    None,
}

/// Single OSD operation
#[derive(Debug, Clone)]
pub struct OSDOp {
    /// Operation code
    pub op: OpCode,
    /// Operation flags
    pub flags: u32,
    /// Operation-specific data
    pub op_data: OpData,
    /// Input data payload
    pub indata: Bytes,
}

impl OSDOp {
    /// Create a read operation
    pub fn read(offset: u64, length: u64) -> Self {
        Self {
            op: OpCode::Read,
            flags: 0,
            op_data: OpData::Extent {
                offset,
                length,
                truncate_size: 0,
                truncate_seq: 0,
            },
            indata: Bytes::new(),
        }
    }

    /// Create a write operation
    pub fn write(offset: u64, data: Bytes) -> Self {
        Self {
            op: OpCode::Write,
            flags: 0,
            op_data: OpData::Extent {
                offset,
                length: data.len() as u64,
                truncate_size: 0,
                truncate_seq: 0,
            },
            indata: data,
        }
    }

    /// Create a write_full operation
    pub fn write_full(data: Bytes) -> Self {
        Self {
            op: OpCode::WriteFull,
            flags: 0,
            op_data: OpData::Extent {
                offset: 0,
                length: data.len() as u64,
                truncate_size: 0,
                truncate_seq: 0,
            },
            indata: data,
        }
    }

    /// Create a stat operation
    pub fn stat() -> Self {
        Self {
            op: OpCode::Stat,
            flags: 0,
            op_data: OpData::None,
            indata: Bytes::new(),
        }
    }

    /// Create a delete operation
    pub fn delete() -> Self {
        Self {
            op: OpCode::Delete,
            flags: 0,
            op_data: OpData::None,
            indata: Bytes::new(),
        }
    }

    /// Create a pgls (PG list) operation
    ///
    /// # Arguments
    /// * `max_entries` - Maximum number of entries to return
    /// * `cursor` - Continuation cursor (HObject for pagination)
    /// * `start_epoch` - OSD map epoch for consistency
    pub fn pgls(max_entries: u64, cursor: denc::HObject, start_epoch: u32) -> Self {
        use bytes::BytesMut;
        use denc::denc::Denc;

        // Encode cursor (hobject_t) into indata
        let mut indata = BytesMut::new();
        cursor
            .encode(&mut indata, 0)
            .expect("Failed to encode HObject cursor");

        Self {
            op: OpCode::Pgls,
            flags: 0, // Operation-specific flags (CEPH_OSD_OP_FLAG_*), not RMW flags
            op_data: OpData::Pgls {
                max_entries,
                start_epoch,
            },
            indata: indata.freeze(),
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

/// Pool information
#[derive(Debug, Clone)]
pub struct PoolInfo {
    /// Pool ID
    pub pool_id: i64,
    /// Pool name
    pub pool_name: String,
}

/// Single object entry from listing
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectEntry {
    /// Namespace (usually empty)
    pub nspace: String,
    /// Object name
    pub oid: String,
    /// Object locator key (usually empty)
    pub locator: String,
}

impl ListObjectEntry {
    pub fn new(nspace: String, oid: String, locator: String) -> Self {
        Self {
            nspace,
            oid,
            locator,
        }
    }
}

/// Result of a list operation
#[derive(Debug, Clone)]
pub struct ListResult {
    /// Listed objects
    pub entries: Vec<ListObjectEntry>,
    /// Continuation cursor for pagination (None if at end)
    pub cursor: Option<String>,
}
