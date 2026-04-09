//! Core types for OSD client operations

use bytes::Bytes;
use std::time::SystemTime;

// ============= Operation State Machine =============

/// Operation state machine matching Ceph Objecter's implicit states
///
/// State transitions:
/// - `Created` → `Queued`: Operation submitted to session
/// - `Queued` → `Sent`: Operation sent over the wire
/// - `Sent` → `Completed`: Successful reply received
/// - `Sent` → `Failed`: Error reply received
/// - `Sent` → `NeedsResend`: OSDMap changed, target may have moved
/// - `Sent` → `Blocked`: Backoff received from OSD
/// - `NeedsResend` → `Queued`: Operation resubmitted to new target
/// - `Blocked` → `Queued`: Backoff lifted, operation resubmitted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpState {
    /// Operation created, not submitted
    #[default]
    Created,
    /// Submitted to session, awaiting send
    Queued,
    /// Sent to OSD, awaiting reply
    Sent,
    /// Needs resend due to OSDMap change
    NeedsResend,
    /// Blocked by backoff
    Blocked,
    /// Successfully completed
    Completed,
    /// Failed with error
    Failed,
}

impl OpState {
    /// Check if the operation is in a terminal state
    pub fn is_terminal(self) -> bool {
        matches!(self, OpState::Completed | OpState::Failed)
    }

    /// Check if the operation is in-flight (sent but not completed)
    pub fn is_in_flight(self) -> bool {
        matches!(
            self,
            OpState::Sent | OpState::NeedsResend | OpState::Blocked
        )
    }

    /// Check if the operation can be resent
    pub fn can_resend(self) -> bool {
        matches!(self, OpState::NeedsResend | OpState::Blocked)
    }
}

/// Target tracking (inspired by Ceph's op_target_t)
///
/// Tracks where an operation is targeted and the OSDMap epoch
/// used to calculate that target. When the OSDMap changes,
/// we can compare epochs to determine if retargeting is needed.
#[derive(Debug, Clone)]
pub struct OpTarget {
    /// OSDMap epoch when calculated
    pub epoch: u32,
    /// Calculated PG
    pub pgid: StripedPgId,
    /// Target OSD
    pub osd: i32,
    /// Acting set
    pub acting: Vec<i32>,
    /// Whether replica was used
    pub used_replica: bool,
}

impl OpTarget {
    /// Create a new OpTarget
    pub fn new(epoch: u32, pgid: StripedPgId, osd: i32, acting: Vec<i32>) -> Self {
        Self {
            epoch,
            pgid,
            osd,
            acting,
            used_replica: false,
        }
    }

    /// Check if the target needs updating based on a new OSDMap epoch
    pub fn needs_update(&self, new_epoch: u32) -> bool {
        new_epoch > self.epoch
    }

    /// Update the target with new placement information
    pub fn update(&mut self, epoch: u32, osd: i32, acting: Vec<i32>) {
        self.epoch = epoch;
        self.osd = osd;
        self.acting = acting;
    }
}

impl Default for OpTarget {
    fn default() -> Self {
        Self {
            epoch: 0,
            pgid: StripedPgId::new(0, 0, -1),
            osd: -1,
            acting: Vec::new(),
            used_replica: false,
        }
    }
}

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
        /// All client ops fail immediately with -EIO
        const EIO = 1 << 16;
    }
}

// ============= OSD Operation Flags =============

bitflags::bitflags! {
    /// OSD operation flags (from ~/dev/ceph/src/include/rados.h)
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
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
        /// Redirect bit is authoritative
        const KNOWN_REDIR = 0x400000;
        /// Client understands pool EIO flag
        const SUPPORTSPOOLEIO = 0x8000000;
        /// Attempt op even if pool is full (OSD still rejects if cluster is full).
        /// Mirrors `CEPH_OSD_FLAG_FULL_TRY`.
        const FULL_TRY = 0x800000;
        /// Force op even if cluster is full.
        /// Mirrors `CEPH_OSD_FLAG_FULL_FORCE`.
        const FULL_FORCE = 0x1000000;
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
pub use crate::denc::{SNAP_DIR, SNAP_HEAD};

impl ObjectId {
    /// Create a new ObjectId for the current version of an object
    pub fn new(pool: u64, oid: &str) -> Self {
        Self {
            pool,
            oid: oid.to_string(),
            snap: SNAP_HEAD,
            hash: 0, // Will be calculated from oid
            namespace: String::new(),
            key: String::new(),
        }
    }

    /// Create ObjectId with explicit namespace
    pub fn with_namespace(pool: u64, oid: &str, namespace: &str) -> Self {
        Self {
            pool,
            oid: oid.to_string(),
            snap: SNAP_HEAD,
            hash: 0,
            namespace: namespace.to_string(),
            key: String::new(),
        }
    }

    /// Calculate the hash for CRUSH placement using Ceph's rjenkins hash.
    ///
    /// Mirrors `OSDMap::map_to_pg` / `pg_pool_t::hash_key`:
    /// - If a locator `key` is set, hash the key instead of the oid.
    /// - If a `namespace` is set, prepend `ns + '\x1f'` before hashing.
    pub fn calculate_hash(&mut self) {
        use crate::crush::hash::ceph_str_hash_rjenkins;

        let target = if self.key.is_empty() {
            &self.oid
        } else {
            &self.key
        };

        self.hash = if self.namespace.is_empty() {
            ceph_str_hash_rjenkins(target.as_bytes())
        } else {
            // Matches pg_pool_t::hash_key: ns + '\x1f' + key_or_oid
            let ns = self.namespace.as_bytes();
            let tgt = target.as_bytes();
            let mut buf = Vec::with_capacity(ns.len() + 1 + tgt.len());
            buf.extend_from_slice(ns);
            buf.push(0x1f);
            buf.extend_from_slice(tgt);
            ceph_str_hash_rjenkins(&buf)
        };
    }
}

/// Striped placement group ID (corresponds to spg_t in Ceph)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

/// Packed entity name for zero-copy wire protocol encoding (9 bytes: u8 type + u64 num)
///
/// This is the most efficient representation for OSD network communication, using a packed
/// struct with numeric fields for zero-copy encoding/decoding in message headers.
///
/// **When to use:** For encoding/decoding entity names in OSD network messages where
/// zero-copy operations are important (MOSDOp, OsdReqId).
///
/// **See also:** `crate::EntityName` — the canonical entity name type used everywhere else.
///
/// Represents a Ceph entity like "client.0", "osd.1", etc.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    crate::ZeroCopyDencode,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
#[denc(crate = "crate")]
#[repr(C)]
pub struct PackedEntityName {
    /// Entity type (CEPH_ENTITY_TYPE_*)
    pub entity_type: u8,
    /// Entity number
    pub num: crate::denc::zerocopy::little_endian::U64,
}

// Use crate::EntityType for CEPH_ENTITY_TYPE_* constants.
use crate::EntityType;

impl PackedEntityName {
    pub fn new(entity_type: u8, num: u64) -> Self {
        Self {
            entity_type,
            num: crate::denc::zerocopy::little_endian::U64::new(num),
        }
    }
}

impl std::str::FromStr for PackedEntityName {
    type Err = String;

    /// Parse from string like "client.0"
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid entity name format: {}", s));
        }

        let entity_type = match parts[0] {
            "mon" => EntityType::MON,
            "mds" => EntityType::MDS,
            "osd" => EntityType::OSD,
            "client" => EntityType::CLIENT,
            "mgr" => EntityType::MGR,
            "auth" => EntityType::AUTH,
            _ => return Err(format!("Unknown entity type: {}", parts[0])),
        };

        let num: u64 = parts[1]
            .parse()
            .map_err(|e| format!("Invalid entity number: {}", e))?;
        Ok(Self::new(entity_type.bits() as u8, num))
    }
}

/// Request identifier for tracking operations (corresponds to osd_reqid_t in Ceph)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestId {
    /// Client entity name (shared — same string for the lifetime of a client)
    pub entity_name: std::sync::Arc<str>,
    /// Transaction ID (unique per client)
    pub tid: u64,
    /// Client incarnation
    pub inc: i32,
}

impl RequestId {
    pub fn new(entity_name: &std::sync::Arc<str>, tid: u64, inc: i32) -> Self {
        Self {
            entity_name: std::sync::Arc::clone(entity_name),
            tid,
            inc,
        }
    }

    /// Convenience constructor that allocates a new Arc from a string slice.
    /// Use `new()` with a pre-allocated `Arc<str>` on the hot path.
    pub fn from_str_name(entity_name: &str, tid: u64, inc: i32) -> Self {
        Self {
            entity_name: std::sync::Arc::from(entity_name),
            tid,
            inc,
        }
    }
}

/// Blkin trace info (corresponds to blkin_trace_info in Ceph)
///
/// Used for distributed tracing with Zipkin/Jaeger.
/// In most cases, all fields are 0 (tracing disabled).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    crate::ZeroCopyDencode,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
#[denc(crate = "crate")]
#[repr(C)]
pub struct BlkinTraceInfo {
    pub trace_id: crate::denc::zerocopy::little_endian::U64,
    pub span_id: crate::denc::zerocopy::little_endian::U64,
    pub parent_span_id: crate::denc::zerocopy::little_endian::U64,
}

impl BlkinTraceInfo {
    /// Create empty trace info (tracing disabled)
    pub const fn empty() -> Self {
        use crate::denc::zerocopy::little_endian::U64;
        Self {
            trace_id: U64::ZERO,
            span_id: U64::ZERO,
            parent_span_id: U64::ZERO,
        }
    }
}

/// Object locator (corresponds to object_locator_t in Ceph)
///
// Re-export ObjectLocator from rados-crush (canonical definition with Denc implementation)
pub use crate::crush::placement::ObjectLocator;

impl From<&ObjectId> for ObjectLocator {
    fn from(id: &ObjectId) -> Self {
        ObjectLocator {
            pool_id: id.pool,
            key: id.key.clone(),
            namespace: id.namespace.clone(),
            hash: crate::crush::placement::HASH_CALCULATE_FROM_NAME,
        }
    }
}

/// Request redirect (corresponds to request_redirect_t in Ceph)
///
/// Used in MOSDOpReply to indicate that the request should be redirected
/// to a different object or pool.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestRedirect {
    /// Authoritative redirect locator
    pub redirect_locator: ObjectLocator,
    /// If non-empty, the request goes to this object name
    pub redirect_object: String,
}

impl RequestRedirect {
    /// Check if the redirect is empty
    pub fn is_empty(&self) -> bool {
        self.redirect_locator.is_empty() && self.redirect_object.is_empty()
    }
}

/// OpenTelemetry trace context (corresponds to jspan_context in Ceph)
///
/// Used for OpenTelemetry distributed tracing.
/// When Jaeger is not enabled, this is just a flag indicating validity.
#[derive(Debug, Clone, PartialEq, Eq, crate::VersionedDenc)]
#[denc(crate = "crate", version = 1, compat = 1)]
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
const CEPH_OSD_OP_MODE_RMW: u16 = 0x3000; // Read-modify-write mode (used via osd_op! macro)

// OSD operation types (from Ceph's rados.h)
const CEPH_OSD_OP_TYPE_DATA: u16 = 0x0200; // Data operations
const CEPH_OSD_OP_TYPE_ATTR: u16 = 0x0300; // Attribute operations
const CEPH_OSD_OP_TYPE_EXEC: u16 = 0x0400; // Exec/CLS operations (object class methods)
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
    (RMW, CLS, $nr:expr) => {
        CEPH_OSD_OP_MODE_RMW | CEPH_OSD_OP_TYPE_EXEC | $nr
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
    /// Append data to the end of an object: __CEPH_OSD_OP(WR, DATA, 6)
    Append = osd_op!(WR, DATA, 6),
    /// Assert object version matches before executing other ops: __CEPH_OSD_OP(RD, DATA, 8)
    AssertVer = osd_op!(RD, DATA, 8),
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
    /// Remove extended attribute: __CEPH_OSD_OP(WR, ATTR, 2)
    RemoveXattr = osd_op!(WR, ATTR, 2),
    /// List extended attributes: __CEPH_OSD_OP(RD, ATTR, 3)
    ListXattrs = osd_op!(RD, ATTR, 3),
    /// Call object class method: __CEPH_OSD_OP(RMW, CLS, 1)
    Call = osd_op!(RMW, CLS, 1),
    /// PG list operation (legacy): __CEPH_OSD_OP(RD, PG, 1) = PGLS
    /// Returns pg_ls_response_t (no namespace support). Kept for completeness.
    Pgls = osd_op!(RD, PG, 1),
    /// PG namespace list operation: __CEPH_OSD_OP(RD, PG, 5) = PGNLS
    /// Returns pg_nls_response_t (includes namespace and locator). Used for
    /// all modern object listing; supersedes PGLS.
    Pgnls = osd_op!(RD, PG, 5),
    /// List object snapshots/clones: __CEPH_OSD_OP(RD, DATA, 10)
    ListSnaps = osd_op!(RD, DATA, 10),
    /// Roll back object HEAD to a prior snapshot: __CEPH_OSD_OP(WR, DATA, 14)
    Rollback = osd_op!(WR, DATA, 14),
}

impl OpCode {
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    /// Try to convert a u16 to an OpCode
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0x1201 => Some(OpCode::Read),        // RD | DATA | 1
            0x1202 => Some(OpCode::Stat),        // RD | DATA | 2
            0x1205 => Some(OpCode::SparseRead),  // RD | DATA | 5
            0x2201 => Some(OpCode::Write),       // WR | DATA | 1
            0x2202 => Some(OpCode::WriteFull),   // WR | DATA | 2
            0x2203 => Some(OpCode::Truncate),    // WR | DATA | 3
            0x2206 => Some(OpCode::Append),      // WR | DATA | 6
            0x1208 => Some(OpCode::AssertVer),   // RD | DATA | 8
            0x2205 => Some(OpCode::Delete),      // WR | DATA | 5
            0x220D => Some(OpCode::Create),      // WR | DATA | 13
            0x1301 => Some(OpCode::GetXattr),    // RD | ATTR | 1
            0x2301 => Some(OpCode::SetXattr),    // WR | ATTR | 1
            0x2302 => Some(OpCode::RemoveXattr), // WR | ATTR | 2
            0x1303 => Some(OpCode::ListXattrs),  // RD | ATTR | 3
            0x3501 => Some(OpCode::Call),        // RMW | CLS | 1
            0x1501 => Some(OpCode::Pgls),        // RD | PG | 1
            0x1505 => Some(OpCode::Pgnls),       // RD | PG | 5
            0x120A => Some(OpCode::ListSnaps),   // RD | DATA | 10
            0x220E => Some(OpCode::Rollback),    // WR | DATA | 14
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
    /// Object class method call
    Call {
        class_len: u8,
        method_len: u8,
        indata_len: u32,
    },
    /// Snapshot rollback target (`ceph_osd_op.snap.snapid`)
    Snap { snapid: u64 },
    /// Version assertion (`ceph_osd_op.assert_ver.ver`)
    AssertVer { ver: u64 },
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

    /// Create an append operation.
    ///
    /// Atomically appends `data` to the end of the object without specifying
    /// an explicit offset.  The OSD determines the actual write position.
    /// Matches `CEPH_OSD_OP_APPEND` (`__CEPH_OSD_OP(WR, DATA, 6)`).
    pub fn append(data: Bytes) -> Self {
        Self {
            op: OpCode::Append,
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

    /// Create a create operation.
    ///
    /// When `exclusive` is true the OSD sets `CEPH_OSD_OP_FLAG_EXCL` (0x1)
    /// in the per-op flags field and returns `-EEXIST` if the object already
    /// exists, matching `ObjectOperation::create(bool excl)` in Objecter.h.
    pub fn create(exclusive: bool) -> Self {
        const CEPH_OSD_OP_FLAG_EXCL: u32 = 0x1;
        Self {
            op: OpCode::Create,
            flags: if exclusive { CEPH_OSD_OP_FLAG_EXCL } else { 0 },
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
    pub fn pgls(
        max_entries: u64,
        cursor: crate::HObject,
        start_epoch: u32,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        use crate::Denc;
        use bytes::BytesMut;

        // Encode cursor (hobject_t) into indata
        let mut indata = BytesMut::with_capacity(cursor.encoded_size(0).unwrap_or(64));
        cursor.encode(&mut indata, 0)?;

        Ok(Self {
            op: OpCode::Pgnls,
            flags: 0, // Operation-specific flags (CEPH_OSD_OP_FLAG_*), not RMW flags
            op_data: OpData::Pgls {
                max_entries,
                start_epoch,
            },
            indata: indata.freeze(),
        })
    }

    /// Create a CALL operation for object class method invocation
    ///
    /// # Arguments
    /// * `class` - Object class name (e.g., "lock", "rbd")
    /// * `method` - Method name (e.g., "lock", "unlock")
    /// * `indata` - Method-specific input data
    pub fn call(
        class: impl Into<String>,
        method: impl Into<String>,
        indata: Bytes,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        use crate::Denc;
        use bytes::BytesMut;

        let class = class.into();
        let method = method.into();

        // Encode class and method into indata buffer (prepended to actual indata)
        // 4-byte len prefix + content for each string
        let mut buf = BytesMut::with_capacity(4 + class.len() + 4 + method.len());
        class.encode(&mut buf, 0)?;
        method.encode(&mut buf, 0)?;
        buf.extend_from_slice(&indata);

        Ok(Self {
            op: OpCode::Call,
            flags: 0,
            op_data: OpData::Call {
                class_len: class.len() as u8,
                method_len: method.len() as u8,
                indata_len: indata.len() as u32,
            },
            indata: buf.freeze(),
        })
    }

    /// Create a version assertion operation.
    ///
    /// When included in a compound operation, the OSD rejects the entire batch
    /// with `-ERANGE` if the object's current version differs from `ver`.
    /// Enables optimistic CAS semantics: read version, modify, submit with assertion.
    ///
    /// Matches `ObjectOperation::assert_version()` in C++ librados.
    pub fn assert_version(ver: u64) -> Self {
        Self {
            op: OpCode::AssertVer,
            flags: 0,
            op_data: OpData::AssertVer { ver },
            indata: Bytes::new(),
        }
    }

    /// Create a lock operation (shared helper for exclusive and shared locks)
    fn lock_op(
        name: &str,
        lock_type: crate::osdclient::lock::LockType,
        cookie: &str,
        tag: &str,
        description: &str,
        duration: Option<std::time::Duration>,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        use crate::Denc;
        use crate::osdclient::lock::{LockFlags, LockRequest};

        let request = LockRequest {
            name: name.to_string(),
            lock_type,
            cookie: cookie.to_string(),
            tag: tag.to_string(),
            description: description.to_string(),
            duration: duration.unwrap_or(std::time::Duration::ZERO),
            flags: LockFlags::empty(),
        };

        let mut buf = bytes::BytesMut::with_capacity(request.encoded_size(0).unwrap_or(128));
        Denc::encode(&request, &mut buf, 0)?;
        Self::call("lock", "lock", buf.freeze())
    }

    /// Create an exclusive lock operation
    ///
    /// # Arguments
    /// * `name` - Lock name (can have multiple named locks per object)
    /// * `cookie` - Unique lock identifier for this client
    /// * `description` - Human-readable description
    /// * `duration` - Lock duration (None = infinite)
    pub fn lock_exclusive(
        name: &str,
        cookie: &str,
        description: &str,
        duration: Option<std::time::Duration>,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        Self::lock_op(
            name,
            crate::osdclient::lock::LockType::Exclusive,
            cookie,
            "",
            description,
            duration,
        )
    }

    /// Create a shared lock operation
    ///
    /// # Arguments
    /// * `name` - Lock name
    /// * `cookie` - Unique lock identifier for this client
    /// * `tag` - Shared lock tag (all shared locks with same tag are compatible)
    /// * `description` - Human-readable description
    /// * `duration` - Lock duration (None = infinite)
    pub fn lock_shared(
        name: &str,
        cookie: &str,
        tag: &str,
        description: &str,
        duration: Option<std::time::Duration>,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        Self::lock_op(
            name,
            crate::osdclient::lock::LockType::Shared,
            cookie,
            tag,
            description,
            duration,
        )
    }

    /// Create an unlock operation
    ///
    /// # Arguments
    /// * `name` - Lock name to unlock
    /// * `cookie` - Lock identifier that was used when acquiring the lock
    pub fn unlock(
        name: &str,
        cookie: &str,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        use crate::Denc;
        use crate::osdclient::lock::UnlockRequest;

        let request = UnlockRequest {
            name: name.to_string(),
            cookie: cookie.to_string(),
        };

        let mut buf = bytes::BytesMut::with_capacity(request.encoded_size(0).unwrap_or(32));
        Denc::encode(&request, &mut buf, 0)?;
        Self::call("lock", "unlock", buf.freeze())
    }

    /// Build an xattr operation with encoded name and optional value
    fn xattr_op(
        op: OpCode,
        name: String,
        value: Option<Bytes>,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        use crate::Denc;
        use bytes::BytesMut;

        let mut buf =
            BytesMut::with_capacity(4 + name.len() + value.as_ref().map_or(0, |v| v.len()));
        name.encode(&mut buf, 0)?;

        let value_len = value.as_ref().map_or(0, |v| v.len() as u32);

        if let Some(ref v) = value {
            buf.extend_from_slice(v);
        }

        Ok(Self {
            op,
            flags: 0,
            op_data: OpData::Xattr {
                name_len: name.len() as u32,
                value_len,
                cmp_op: 0,
                cmp_mode: 0,
            },
            indata: buf.freeze(),
        })
    }

    /// Get an extended attribute
    ///
    /// # Arguments
    /// * `name` - Attribute name
    pub fn get_xattr(
        name: impl Into<String>,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        Self::xattr_op(OpCode::GetXattr, name.into(), None)
    }

    /// Set an extended attribute
    ///
    /// # Arguments
    /// * `name` - Attribute name
    /// * `value` - Attribute value
    pub fn set_xattr(
        name: impl Into<String>,
        value: Bytes,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        Self::xattr_op(OpCode::SetXattr, name.into(), Some(value))
    }

    /// Remove an extended attribute
    ///
    /// # Arguments
    /// * `name` - Attribute name to remove
    pub fn remove_xattr(
        name: impl Into<String>,
    ) -> Result<Self, crate::osdclient::error::OSDClientError> {
        Self::xattr_op(OpCode::RemoveXattr, name.into(), None)
    }

    /// List all extended attributes
    pub fn list_xattrs() -> Self {
        Self {
            op: OpCode::ListXattrs,
            flags: 0,
            op_data: OpData::Xattr {
                name_len: 0,
                value_len: 0,
                cmp_op: 0,
                cmp_mode: 0,
            },
            indata: Bytes::new(),
        }
    }

    /// List snapshots/clones of an object (LIST_SNAPS)
    pub fn list_snaps() -> Self {
        Self {
            op: OpCode::ListSnaps,
            flags: 0,
            op_data: OpData::None,
            indata: Bytes::new(),
        }
    }

    /// Roll back object HEAD to a prior pool snapshot (ROLLBACK)
    ///
    /// # Arguments
    /// * `snap_id` - Snapshot ID to roll back to
    pub fn rollback(snap_id: impl Into<crate::osdclient::snapshot::SnapId>) -> Self {
        let snap_id = snap_id.into();
        Self {
            op: OpCode::Rollback,
            flags: 0,
            op_data: OpData::Snap {
                snapid: snap_id.as_u64(),
            },
            indata: Bytes::new(),
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
                match op.op_data {
                    OpData::Extent { length, .. } if length > 0 => return length as usize,
                    // Attribute operations (getxattr): budget = name_len + value_len
                    // Matches C++ Objecter::calc_op_budget() (Objecter.cc:3535-3538)
                    OpData::Xattr {
                        name_len,
                        value_len,
                        ..
                    } => return (name_len + value_len) as usize,
                    _ => {}
                }
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

impl StatResult {
    /// Parse a `StatResult` from the first op's outdata in a completed `OpResult`.
    pub(crate) fn from_op_result(result: &OpResult) -> crate::osdclient::error::Result<Self> {
        use crate::denc::Denc;
        let outdata = result.ops.first().map(|op| &op.outdata[..]).unwrap_or(&[]);
        let stat_data = crate::osdclient::denc_types::OsdStatData::decode(&mut &outdata[..], 0)?;
        Ok(StatResult {
            size: stat_data.size,
            mtime: stat_data.mtime,
        })
    }
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
impl crate::Denc for SparseExtent {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
        features: u64,
    ) -> std::result::Result<(), crate::RadosError> {
        // Encode as tuple (offset, length)
        crate::Denc::encode(&(self.offset, self.length), buf, features)
    }

    fn decode<B: bytes::Buf>(
        buf: &mut B,
        features: u64,
    ) -> std::result::Result<Self, crate::RadosError> {
        // Decode as tuple (offset, length)
        let (offset, length) = <(u64, u64) as crate::Denc>::decode(buf, features)?;
        Ok(Self::new(offset, length))
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Size is same as tuple (offset, length)
        crate::Denc::encoded_size(&(self.offset, self.length), features)
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

impl SparseReadResult {
    /// Parse a `SparseReadResult` from a completed `OpResult`.
    ///
    /// Decodes the extent map and data bufferlist from the first op's outdata.
    pub(crate) fn from_op_result(result: &OpResult) -> Result<Self, crate::RadosError> {
        use crate::Denc;

        let outdata = result
            .ops
            .first()
            .map(|op| op.outdata.clone())
            .unwrap_or_default();

        if outdata.is_empty() {
            return Ok(SparseReadResult {
                extents: vec![],
                data: bytes::Bytes::new(),
                version: result.version,
            });
        }

        let mut buf = outdata;
        let extents = Vec::<SparseExtent>::decode(&mut buf, 0)?;
        let data = bytes::Bytes::decode(&mut buf, 0)?;

        Ok(SparseReadResult {
            extents,
            data,
            version: result.version,
        })
    }
}

/// Generic operation result
#[derive(Debug, Clone)]
pub struct OpResult {
    /// Overall result code (0 = success)
    pub result: i32,
    /// Object version (user_version from OSD reply)
    pub version: u64,
    /// Per-operation results
    pub ops: Vec<OpReply>,
    /// Redirect information (for EC pools)
    pub redirect: Option<RequestRedirect>,
}

impl OpResult {
    /// Return the outdata of the first operation reply, or an error if the reply list is empty.
    pub fn first_outdata(&self) -> crate::osdclient::error::Result<&Bytes> {
        self.ops
            .first()
            .map(|op| &op.outdata)
            .ok_or_else(|| crate::osdclient::error::OSDClientError::Other("No op result".into()))
    }
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

// Re-export ListObjectImpl as ListObjectEntry
pub use crate::osdclient::pg_nls_response::ListObjectImpl as ListObjectEntry;

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
        let entry = ListObjectEntry::new("namespace", "object-name", "locator");

        assert_eq!(entry.nspace, "namespace");
        assert_eq!(entry.oid, "object-name");
        assert_eq!(entry.locator, "locator");
    }

    #[test]
    fn test_op_state_default() {
        let state = OpState::default();
        assert_eq!(state, OpState::Created);
    }

    #[test]
    fn test_op_state_is_terminal() {
        assert!(!OpState::Created.is_terminal());
        assert!(!OpState::Queued.is_terminal());
        assert!(!OpState::Sent.is_terminal());
        assert!(!OpState::NeedsResend.is_terminal());
        assert!(!OpState::Blocked.is_terminal());
        assert!(OpState::Completed.is_terminal());
        assert!(OpState::Failed.is_terminal());
    }

    #[test]
    fn test_op_state_is_in_flight() {
        assert!(!OpState::Created.is_in_flight());
        assert!(!OpState::Queued.is_in_flight());
        assert!(OpState::Sent.is_in_flight());
        assert!(OpState::NeedsResend.is_in_flight());
        assert!(OpState::Blocked.is_in_flight());
        assert!(!OpState::Completed.is_in_flight());
        assert!(!OpState::Failed.is_in_flight());
    }

    #[test]
    fn test_op_state_can_resend() {
        assert!(!OpState::Created.can_resend());
        assert!(!OpState::Queued.can_resend());
        assert!(!OpState::Sent.can_resend());
        assert!(OpState::NeedsResend.can_resend());
        assert!(OpState::Blocked.can_resend());
        assert!(!OpState::Completed.can_resend());
        assert!(!OpState::Failed.can_resend());
    }

    #[test]
    fn test_op_target_new() {
        let pgid = StripedPgId::new(1, 42, -1);
        let target = OpTarget::new(100, pgid, 5, vec![5, 6, 7]);

        assert_eq!(target.epoch, 100);
        assert_eq!(target.pgid.pool, 1);
        assert_eq!(target.pgid.seed, 42);
        assert_eq!(target.osd, 5);
        assert_eq!(target.acting, vec![5, 6, 7]);
        assert!(!target.used_replica);
    }

    #[test]
    fn test_op_target_default() {
        let target = OpTarget::default();

        assert_eq!(target.epoch, 0);
        assert_eq!(target.osd, -1);
        assert!(target.acting.is_empty());
        assert!(!target.used_replica);
    }

    #[test]
    fn test_op_target_needs_update() {
        let target = OpTarget::new(100, StripedPgId::new(1, 0, -1), 5, vec![5]);

        assert!(!target.needs_update(99));
        assert!(!target.needs_update(100));
        assert!(target.needs_update(101));
    }

    #[test]
    fn test_op_target_update() {
        let mut target = OpTarget::new(100, StripedPgId::new(1, 0, -1), 5, vec![5]);

        target.update(200, 10, vec![10, 11]);

        assert_eq!(target.epoch, 200);
        assert_eq!(target.osd, 10);
        assert_eq!(target.acting, vec![10, 11]);
    }
}
