//! Core types for OSD client operations

use bytes::Bytes;
use std::time::SystemTime;

// ============= Pool Flags =============

bitflags::bitflags! {
    /// Pool flags (from ~/dev/ceph/src/osd/osd_types.h)
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct PoolFlags: u64 {
        /// Hash pg seed and pool together (instead of adding)
        const HASHPSPOOL = 1 << 0;
        /// Pool is full
        const FULL = 1 << 1;
        /// May have incomplete clones (bc we are/were an overlay)
        const INCOMPLETE_CLONES = 1 << 3;
        /// Pool is currently running out of quota, will set FULL too
        const FULL_QUOTA = 1 << 10;
    }
}

// ============= OSD Operation Flags =============

bitflags::bitflags! {
    /// OSD operation flags (from ~/dev/ceph/src/include/rados.h)
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OsdOpFlags: u32 {
        /// Request acknowledgement
        const ACK = 0x0001;
        /// Request commit confirmation
        const ONDISK = 0x0004;
        /// Read operation
        const READ = 0x0010;
        /// Write operation
        const WRITE = 0x0020;
        /// Balance reads among replicas
        const BALANCE_READS = 0x0100;
        /// Read from nearby replica, if any
        const LOCALIZE_READS = 0x2000;
        /// PG operation
        const PGOP = 0x0400;
        /// Ignore cache logic (used for redirects)
        const IGNORE_CACHE = 0x8000;
        /// Ignore pool overlay (used for redirects)
        const IGNORE_OVERLAY = 0x20000;
        /// Operation has been redirected (for EC pools)
        const REDIRECTED = 0x200000;
    }
}

bitflags::bitflags! {
    /// OSD Read-Modify-Write operation flags (from ~/dev/ceph/src/osd/osd_types.h)
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OsdRmwFlags: u32 {
        /// Read operation
        const READ = 1 << 1;
        /// Write operation
        const WRITE = 1 << 2;
        /// Class read operation
        const CLASS_READ = 1 << 3;
        /// Class write operation
        const CLASS_WRITE = 1 << 4;
        /// PG operation
        const PGOP = 1 << 5;
        /// Cache operation
        const CACHE = 1 << 6;
        /// Force promote
        const FORCE_PROMOTE = 1 << 7;
        /// Skip handle cache
        const SKIP_HANDLE_CACHE = 1 << 8;
        /// Skip promote
        const SKIP_PROMOTE = 1 << 9;
        /// RW ordered
        const RWORDERED = 1 << 10;
        /// Return vector
        const RETURNVEC = 1 << 11;
    }
}

/// Object identification (corresponds to hobject_t in Ceph)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectId {
    /// Pool ID (u64::MAX for sentinel/invalid pool)
    pub pool: u64,
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
    pub fn new(pool: u64, oid: impl Into<String>) -> Self {
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
    pub fn with_namespace(pool: u64, oid: impl Into<String>, namespace: impl Into<String>) -> Self {
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

// Re-export PgId from crush
pub use crush::PgId;

/// Striped placement group ID (corresponds to spg_t in Ceph)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StripedPgId {
    /// Placement group ID
    pub pool: u64,
    pub seed: u32,
    /// OSD shard ID (-1 for replicated pools)
    pub shard: i8,
}

impl StripedPgId {
    pub fn new(pool: u64, seed: u32, shard: i8) -> Self {
        Self { pool, seed, shard }
    }

    /// Create from PgId (for replicated pools)
    pub fn from_pg(pool: u64, seed: u32) -> Self {
        Self {
            pool,
            seed,
            shard: -1,
        }
    }
}

/// Entity name (corresponds to entity_name_t in Ceph)
///
/// Represents a Ceph entity like "client.0", "osd.1", etc.
#[derive(Debug, Clone, PartialEq, Eq, denc::ZeroCopyDencode)]
#[repr(C, packed)]
pub struct EntityName {
    /// Entity type (CEPH_ENTITY_TYPE_*)
    pub entity_type: u8,
    /// Entity number
    pub num: u64,
}

/// Entity type constants (from Ceph's msgr.h)
pub const CEPH_ENTITY_TYPE_MON: u8 = 0x01;
pub const CEPH_ENTITY_TYPE_MDS: u8 = 0x02;
pub const CEPH_ENTITY_TYPE_OSD: u8 = 0x04;
pub const CEPH_ENTITY_TYPE_CLIENT: u8 = 0x08;
pub const CEPH_ENTITY_TYPE_MGR: u8 = 0x10;
pub const CEPH_ENTITY_TYPE_AUTH: u8 = 0x20;

impl EntityName {
    pub fn new(entity_type: u8, num: u64) -> Self {
        Self { entity_type, num }
    }
}

impl std::str::FromStr for EntityName {
    type Err = String;

    /// Parse from string like "client.0"
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid entity name format: {}", s));
        }

        let entity_type = match parts[0] {
            "mon" => CEPH_ENTITY_TYPE_MON,
            "mds" => CEPH_ENTITY_TYPE_MDS,
            "osd" => CEPH_ENTITY_TYPE_OSD,
            "client" => CEPH_ENTITY_TYPE_CLIENT,
            "mgr" => CEPH_ENTITY_TYPE_MGR,
            "auth" => CEPH_ENTITY_TYPE_AUTH,
            _ => return Err(format!("Unknown entity type: {}", parts[0])),
        };

        let num = parts[1]
            .parse()
            .map_err(|e| format!("Invalid entity number: {}", e))?;
        Ok(Self { entity_type, num })
    }
}

/// Request identifier for tracking operations (corresponds to osd_reqid_t in Ceph)
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

/// Blkin trace info (corresponds to blkin_trace_info in Ceph)
///
/// Used for distributed tracing with Zipkin/Jaeger.
/// In most cases, all fields are 0 (tracing disabled).
#[derive(Debug, Clone, Copy, PartialEq, Eq, denc::ZeroCopyDencode)]
#[repr(C, packed)]
pub struct BlkinTraceInfo {
    pub trace_id: u64,
    pub span_id: u64,
    pub parent_span_id: u64,
}

impl BlkinTraceInfo {
    /// Create empty trace info (tracing disabled)
    pub const fn empty() -> Self {
        Self {
            trace_id: 0,
            span_id: 0,
            parent_span_id: 0,
        }
    }
}

/// Object locator (corresponds to object_locator_t in Ceph)
///
/// A locator constrains the placement of an object. Mainly specifies which pool
/// the object goes in, and optionally namespace, key, or hash position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLocator {
    /// Pool ID (u64::MAX for invalid/default)
    pub pool: u64,
    /// Key string (if non-empty) - specify either hash or key, not both
    pub key: String,
    /// Namespace
    pub nspace: String,
    /// Hash position (if >= 0) - specify either hash or key, not both
    pub hash: i64,
}

impl ObjectLocator {
    /// Create an empty object locator (with sentinel value for pool)
    pub fn new() -> Self {
        Self {
            pool: u64::MAX,
            key: String::new(),
            nspace: String::new(),
            hash: -1,
        }
    }

    /// Create an object locator for a specific pool
    pub fn with_pool(pool: u64) -> Self {
        Self {
            pool,
            key: String::new(),
            nspace: String::new(),
            hash: -1,
        }
    }

    /// Check if the locator is empty
    pub fn is_empty(&self) -> bool {
        self.pool == u64::MAX && self.key.is_empty() && self.nspace.is_empty() && self.hash == -1
    }
}

impl Default for ObjectLocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Request redirect (corresponds to request_redirect_t in Ceph)
///
/// Used in MOSDOpReply to indicate that the request should be redirected
/// to a different object or pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestRedirect {
    /// Authoritative redirect locator
    pub redirect_locator: ObjectLocator,
    /// If non-empty, the request goes to this object name
    pub redirect_object: String,
}

impl RequestRedirect {
    /// Create an empty redirect
    pub fn new() -> Self {
        Self {
            redirect_locator: ObjectLocator::new(),
            redirect_object: String::new(),
        }
    }

    /// Check if the redirect is empty
    pub fn is_empty(&self) -> bool {
        self.redirect_locator.is_empty() && self.redirect_object.is_empty()
    }
}

impl Default for RequestRedirect {
    fn default() -> Self {
        Self::new()
    }
}

/// OpenTelemetry trace context (corresponds to jspan_context in Ceph)
///
/// Used for OpenTelemetry distributed tracing.
/// When Jaeger is not enabled, this is just a flag indicating validity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JaegerSpanContext {
    pub is_valid: bool,
    // Additional fields would go here if tracing is enabled
}

impl JaegerSpanContext {
    /// Create invalid context (tracing disabled)
    pub const fn invalid() -> Self {
        Self { is_valid: false }
    }
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
    /// Sparse read operation: __CEPH_OSD_OP(RD, DATA, 5)
    SparseRead = osd_op!(RD, DATA, 5),
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

    /// Try to convert a u16 to an OpCode
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0x1201 => Some(OpCode::Read),       // RD | DATA | 1
            0x1202 => Some(OpCode::Stat),       // RD | DATA | 2
            0x1205 => Some(OpCode::SparseRead), // RD | DATA | 5
            0x2201 => Some(OpCode::Write),      // WR | DATA | 1
            0x2202 => Some(OpCode::WriteFull),  // WR | DATA | 2
            0x2203 => Some(OpCode::Truncate),   // WR | DATA | 3
            0x2205 => Some(OpCode::Delete),     // WR | DATA | 5
            0x220D => Some(OpCode::Create),     // WR | DATA | 13
            0x1301 => Some(OpCode::GetXattr),   // RD | ATTR | 1
            0x2301 => Some(OpCode::SetXattr),   // WR | ATTR | 1
            0x1501 => Some(OpCode::Pgls),       // RD | PG | 1
            _ => None,
        }
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

    /// Create a sparse_read operation
    ///
    /// Sparse read returns a map of extents (offset -> length) indicating
    /// which regions of the object contain data, along with the actual data.
    /// This is useful for reading sparse objects efficiently.
    ///
    /// # Arguments
    /// * `offset` - Starting offset to read from
    /// * `length` - Maximum length to read
    pub fn sparse_read(offset: u64, length: u64) -> Self {
        Self {
            op: OpCode::SparseRead,
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

/// Calculate operation budget (bytes consumed by operations)
///
/// This matches C++ Objecter::calc_op_budget() from ~/dev/ceph/src/osdc/Objecter.cc:3527-3543
///
/// Budget calculation rules:
/// - Write operations: sum of indata lengths (actual data being written)
/// - Read operations with extent: extent length (expected response size)
/// - Attribute operations: name_len + value_len
/// - Other operations: 0 bytes
///
/// # Arguments
/// * `ops` - List of OSD operations
///
/// # Returns
/// Total bytes that this operation set will consume in-flight
pub fn calc_op_budget(ops: &[OSDOp]) -> usize {
    ops.iter()
        .map(|op| {
            // Write operations contribute indata size
            if op.op.is_write() {
                return op.indata.len();
            }

            // Read operations contribute expected response size
            if op.op.is_read() {
                if let OpData::Extent { length, .. } = op.op_data {
                    if length > 0 {
                        return length as usize;
                    }
                }
                // Attribute operations (getxattr, etc.)
                // In C++: name_len + value_len from op.xattr
                // For now we'll estimate conservatively
                // TODO: Add xattr fields to OpData::Xattr variant if needed
            }

            0
        })
        .sum()
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

/// Extent information for sparse read
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseExtent {
    /// Offset in the object where data exists
    pub offset: u64,
    /// Length of data at this offset
    pub length: u64,
}

impl SparseExtent {
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }
}

// Implement Denc for SparseExtent to enable direct decoding
impl denc::denc::Denc for SparseExtent {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
        features: u64,
    ) -> std::result::Result<(), denc::RadosError> {
        // Encode as tuple (offset, length)
        denc::denc::Denc::encode(&(self.offset, self.length), buf, features)
    }

    fn decode<B: bytes::Buf>(
        buf: &mut B,
        features: u64,
    ) -> std::result::Result<Self, denc::RadosError> {
        // Decode as tuple (offset, length)
        let (offset, length) = <(u64, u64) as denc::denc::Denc>::decode(buf, features)?;
        Ok(Self::new(offset, length))
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Size is same as tuple (offset, length)
        denc::denc::Denc::encoded_size(&(self.offset, self.length), features)
    }
}

/// Result of a sparse read operation
///
/// Sparse read returns a map of extents indicating which regions of the object
/// contain data, along with the actual data. This is useful for efficiently
/// reading sparse objects (e.g., VM disk images with holes).
///
/// # Example
/// ```ignore
/// // Read 1MB starting at offset 0
/// let result = client.sparse_read("myobject", 0, 1024*1024).await?;
///
/// // Check which regions have data
/// for extent in &result.extents {
///     println!("Data at offset  for {} bytes", extent.offset, extent.length);
/// }
///
/// // Access the actual data
/// println!("Total data bytes: {}", result.data.len());
/// ```
#[derive(Debug, Clone)]
pub struct SparseReadResult {
    /// Map of extents (offset -> length) indicating data regions
    pub extents: Vec<SparseExtent>,
    /// Actual data bytes (concatenated from all extents)
    pub data: Bytes,
    /// Object version
    pub version: u64,
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
    /// Redirect information (for EC pools)
    pub redirect: Option<RequestRedirect>,
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
    pub pool_id: u64,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opcode_sparse_read_encoding() {
        // Verify SparseRead opcode matches Ceph's encoding
        // __CEPH_OSD_OP(RD, DATA, 5) = 0x1000 | 0x0200 | 5 = 0x1205
        assert_eq!(OpCode::SparseRead as u16, 0x1205);
        assert!(OpCode::SparseRead.is_read());
        assert!(!OpCode::SparseRead.is_write());
    }

    #[test]
    fn test_opcode_write_full_encoding() {
        // Verify WriteFull opcode matches Ceph's encoding
        // __CEPH_OSD_OP(WR, DATA, 2) = 0x2000 | 0x0200 | 2 = 0x2202
        assert_eq!(OpCode::WriteFull as u16, 0x2202);
        assert!(!OpCode::WriteFull.is_read());
        assert!(OpCode::WriteFull.is_write());
    }

    #[test]
    fn test_opcode_from_u16_sparse_read() {
        // Test conversion from u16 to OpCode for SparseRead
        assert_eq!(OpCode::from_u16(0x1205), Some(OpCode::SparseRead));
    }

    #[test]
    fn test_opcode_from_u16_write_full() {
        // Test conversion from u16 to OpCode for WriteFull
        assert_eq!(OpCode::from_u16(0x2202), Some(OpCode::WriteFull));
    }

    #[test]
    fn test_osdop_sparse_read() {
        // Test creating a sparse read operation
        let op = OSDOp::sparse_read(1024, 4096);

        assert_eq!(op.op, OpCode::SparseRead);
        assert_eq!(op.flags, 0);
        assert!(op.indata.is_empty());

        match op.op_data {
            OpData::Extent {
                offset,
                length,
                truncate_size,
                truncate_seq,
            } => {
                assert_eq!(offset, 1024);
                assert_eq!(length, 4096);
                assert_eq!(truncate_size, 0);
                assert_eq!(truncate_seq, 0);
            }
            _ => panic!("Expected OpData::Extent"),
        }
    }

    #[test]
    fn test_osdop_write_full() {
        // Test creating a write_full operation
        let data = Bytes::from(vec![1, 2, 3, 4, 5]);
        let op = OSDOp::write_full(data.clone());

        assert_eq!(op.op, OpCode::WriteFull);
        assert_eq!(op.flags, 0);
        assert_eq!(op.indata, data);

        match op.op_data {
            OpData::Extent {
                offset,
                length,
                truncate_size,
                truncate_seq,
            } => {
                assert_eq!(offset, 0);
                assert_eq!(length, 5);
                assert_eq!(truncate_size, 0);
                assert_eq!(truncate_seq, 0);
            }
            _ => panic!("Expected OpData::Extent"),
        }
    }

    #[test]
    fn test_sparse_extent_creation() {
        // Test creating a sparse extent
        let extent = SparseExtent::new(1024, 4096);
        assert_eq!(extent.offset, 1024);
        assert_eq!(extent.length, 4096);
    }

    #[test]
    fn test_sparse_extent_equality() {
        // Test sparse extent equality
        let extent1 = SparseExtent::new(1024, 4096);
        let extent2 = SparseExtent::new(1024, 4096);
        let extent3 = SparseExtent::new(2048, 4096);

        assert_eq!(extent1, extent2);
        assert_ne!(extent1, extent3);
    }

    #[test]
    fn test_sparse_read_result() {
        // Test creating a sparse read result
        let extents = vec![SparseExtent::new(0, 1024), SparseExtent::new(4096, 2048)];
        let data = Bytes::from(vec![0u8; 3072]); // 1024 + 2048
        let version = 42;

        let result = SparseReadResult {
            extents: extents.clone(),
            data: data.clone(),
            version,
        };

        assert_eq!(result.extents.len(), 2);
        assert_eq!(result.extents[0].offset, 0);
        assert_eq!(result.extents[0].length, 1024);
        assert_eq!(result.extents[1].offset, 4096);
        assert_eq!(result.extents[1].length, 2048);
        assert_eq!(result.data.len(), 3072);
        assert_eq!(result.version, 42);
    }

    #[test]
    fn test_calc_op_budget_sparse_read() {
        // Test budget calculation for sparse read
        let op = OSDOp::sparse_read(0, 4096);
        let budget = calc_op_budget(&[op]);

        // Sparse read is a read operation, so budget should be the expected response size
        assert_eq!(budget, 4096);
    }

    #[test]
    fn test_calc_op_budget_write_full() {
        // Test budget calculation for write_full
        let data = Bytes::from(vec![0u8; 1024]);
        let op = OSDOp::write_full(data);
        let budget = calc_op_budget(&[op]);

        // Write operations contribute indata size
        assert_eq!(budget, 1024);
    }

    #[test]
    fn test_object_id_creation() {
        // Test creating an ObjectId
        let obj = ObjectId::new(1, "test-object");
        assert_eq!(obj.pool, 1);
        assert_eq!(obj.oid, "test-object");
        assert_eq!(obj.snap, SNAP_HEAD);
        assert_eq!(obj.hash, 0);
        assert!(obj.namespace.is_empty());
        assert!(obj.key.is_empty());
    }

    #[test]
    fn test_object_id_with_namespace() {
        // Test creating an ObjectId with namespace
        let obj = ObjectId::with_namespace(1, "test-object", "test-ns");
        assert_eq!(obj.pool, 1);
        assert_eq!(obj.oid, "test-object");
        assert_eq!(obj.namespace, "test-ns");
        assert_eq!(obj.snap, SNAP_HEAD);
    }

    #[test]
    fn test_list_object_entry() {
        // Test creating a ListObjectEntry
        let entry = ListObjectEntry::new(
            "namespace".to_string(),
            "object-name".to_string(),
            "locator".to_string(),
        );

        assert_eq!(entry.nspace, "namespace");
        assert_eq!(entry.oid, "object-name");
        assert_eq!(entry.locator, "locator");
    }
}
