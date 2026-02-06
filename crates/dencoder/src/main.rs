//! Rust implementation of ceph-dencoder
//!
//! This tool can decode, encode, and inspect Ceph data structures from binary corpus files.
//!
//! Usage:
//!   dencoder type <typename> [command] [command] ...
//!   dencoder list_types
//!
//! Commands:
//!   type <name>        - Select type to work with
//!   import <file>      - Read binary file (use "-" for stdin)
//!   decode             - Decode binary data to object
//!   encode             - Encode object to binary
//!   dump_json          - Output object as JSON
//!   export <file>      - Write binary data to file
//!   hexdump            - Show hex representation of binary data
//!   set_features <hex> - Set feature flags (hex or decimal)
//!   get_features       - Show current feature flags
//!   list_types         - List all available types

use bytes::Bytes;
use denc::denc::{Denc, VersionedEncode};
use denc::entity_addr::EntityAddr;
use denc::error::RadosError;
use denc::hobject::HObject;
use denc::monmap::*;
use denc::pg_nls_response::PgNlsResponse;
use denc::{EVersion, UTime, UuidD};
use osdclient::osdmap::*;
use osdclient::{OSDMap, PgMergeMeta, PgPool};
use serde::Serialize;
use std::any::Any;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::process;

// Re-export auth types (may not compile if auth crate is not available)
// use auth::types::{CephXTicketBlob, CryptoKey};
// use auth::protocol::{CephXAuthenticate, CephXRequestHeader};

/// Trait for type-erased serializable objects
///
/// This bridges the gap between type erasure (Box<dyn SerializableType>)
/// and JSON serialization, allowing us to store different types in a single variable
/// while still being able to serialize them to JSON.
trait SerializableType: Any {
    /// Convert this object to a JSON value
    fn to_json(&self) -> std::result::Result<serde_json::Value, Box<dyn std::error::Error>>;

    /// Get reference to Any for downcasting
    fn as_any(&self) -> &dyn Any;
}

/// Blanket implementation for all types that implement Serialize + 'static
impl<T: Serialize + 'static> SerializableType for T {
    fn to_json(&self) -> std::result::Result<serde_json::Value, Box<dyn std::error::Error>> {
        Ok(serde_json::to_value(self)?)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Type information for the registry
///
/// Each type has functions for decoding and encoding.
struct TypeInfo {
    decode_fn: fn(&mut Bytes, u64) -> Result<Box<dyn SerializableType>>,
    encode_fn: fn(&dyn SerializableType, u64) -> Result<Bytes>,
}

/// Generic decode function for types implementing Denc
fn decode_denc<T>(bytes: &mut Bytes, features: u64) -> Result<Box<dyn SerializableType>>
where
    T: Denc + Serialize + 'static,
{
    let obj = T::decode(bytes, features).map_err(DencoderError::DecodeError)?;
    Ok(Box::new(obj))
}

/// Generic decode function for types implementing VersionedEncode
fn decode_versioned<T>(bytes: &mut Bytes, features: u64) -> Result<Box<dyn SerializableType>>
where
    T: VersionedEncode + Serialize + 'static,
{
    let obj = T::decode_versioned(bytes, features).map_err(DencoderError::DecodeError)?;
    Ok(Box::new(obj))
}

/// Generic encode function for types implementing Denc
fn encode_denc<T>(obj: &dyn SerializableType, features: u64) -> Result<Bytes>
where
    T: Denc + 'static,
{
    let typed = obj.as_any().downcast_ref::<T>().ok_or_else(|| {
        DencoderError::EncodeError(RadosError::Protocol("Type mismatch".to_string()))
    })?;
    let mut buf = bytes::BytesMut::new();
    typed
        .encode(&mut buf, features)
        .map_err(DencoderError::EncodeError)?;
    Ok(buf.freeze())
}

/// Generic encode function for types implementing VersionedEncode
fn encode_versioned<T>(obj: &dyn SerializableType, features: u64) -> Result<Bytes>
where
    T: VersionedEncode + 'static,
{
    let typed = obj.as_any().downcast_ref::<T>().ok_or_else(|| {
        DencoderError::EncodeError(RadosError::Protocol("Type mismatch".to_string()))
    })?;
    let mut buf = bytes::BytesMut::new();
    typed
        .encode_versioned(&mut buf, features)
        .map_err(DencoderError::EncodeError)?;
    Ok(buf.freeze())
}

/// Helper to create TypeInfo for types using regular Denc
fn type_info_denc<T>() -> TypeInfo
where
    T: Denc + Serialize + 'static,
{
    TypeInfo {
        decode_fn: decode_denc::<T>,
        encode_fn: encode_denc::<T>,
    }
}

/// Helper to create TypeInfo for types using VersionedEncode
fn type_info_versioned<T>() -> TypeInfo
where
    T: VersionedEncode + Serialize + 'static,
{
    TypeInfo {
        decode_fn: decode_versioned::<T>,
        encode_fn: encode_versioned::<T>,
    }
}

/// Get type information by name
///
/// This is the type registry - a simple match statement mapping type names
/// to their decode/encode functions.
///
/// Types are organized by dependency level for systematic testing:
/// - Level 1: Primitive types (no Denc dependencies)
/// - Level 2: Types depending on Level 1
/// - Level 3: Complex types depending on Level 2
fn get_type_info(name: &str) -> Option<TypeInfo> {
    match name {
        // ========================================
        // LEVEL 1: PRIMITIVE TYPES
        // No Denc dependencies - only use built-in Rust types
        // These must be tested and validated FIRST
        // ========================================

        // pg_t (PgId) - Dependencies: u64, u32
        "pg_t" | "pg_id" => Some(type_info_denc::<PgId>()),

        // eversion_t (EVersion) - Dependencies: u64, u32
        "eversion_t" => Some(type_info_denc::<EVersion>()),

        // utime_t (UTime) - Dependencies: u32, u32
        "utime_t" => Some(type_info_denc::<UTime>()),

        // uuid_d (UuidD) - Dependencies: [u8; 16]
        "uuid_d" => Some(type_info_denc::<UuidD>()),

        // osd_info_t (OsdInfo) - Dependencies: multiple u32 fields
        "osd_info_t" => Some(type_info_denc::<OsdInfo>()),

        // ========================================
        // LEVEL 2: TYPES DEPENDING ON LEVEL 1
        // These types contain Level 1 types as fields
        // Test these ONLY after Level 1 is validated
        // ========================================

        // entity_addr_t (EntityAddr) - Dependencies: EntityAddrType (Level 1), u32, Vec<u8>
        "entity_addr_t" => Some(type_info_denc::<EntityAddr>()),

        // pool_snap_info_t (PoolSnapInfo) - Dependencies: u64, UTime (Level 1), String
        "pool_snap_info_t" => Some(type_info_versioned::<PoolSnapInfo>()),

        // osd_xinfo_t (OsdXInfo) - Dependencies: UTime×2 (Level 1), f32, u32, u64
        "osd_xinfo_t" => Some(type_info_denc::<OsdXInfo>()),

        // ========================================
        // LEVEL 3: COMPLEX TYPES
        // These depend on Level 2 or contain multiple nested dependencies
        // Test these ONLY after Level 1 & 2 are validated
        // ========================================

        // pg_merge_meta_t (PgMergeMeta) - Dependencies: PgId (L1), EVersion×2 (L1), u32
        "pg_merge_meta_t" => Some(type_info_denc::<PgMergeMeta>()),

        // object_locator_t (ObjectLocator) - Object placement information
        // Dependencies: i64, String
        "object_locator_t" => Some(type_info_denc::<crush::ObjectLocator>()),

        // hobject_t (HObject) - Hashed object identifier
        // Dependencies: String, u64, u32, bool, i64
        "hobject_t" => Some(type_info_denc::<HObject>()),

        // pg_nls_response_t (PgNlsResponse) - PG namespace list response
        // Dependencies: HObject (L3), Vec<ListObjectImpl>
        "pg_nls_response_t" => Some(type_info_denc::<PgNlsResponse>()),

        // pg_pool_t (PgPool) - Dependencies: UTime (L1), PoolSnapInfo (L2), SnapInterval (L1), HitSetParams (L2), PgMergeMeta (L3)
        // This is the most complex type - test LAST
        "pg_pool_t" => Some(type_info_denc::<PgPool>()),

        // OSDMap - The main OSD cluster map
        // Dependencies: All of the above types plus many more
        "OSDMap" => Some(type_info_versioned::<OSDMap>()),

        // mon_info_t (MonInfo) - Monitor information
        // Dependencies: String, EntityAddrvec (Level 2), u16, BTreeMap, UTime (Level 1)
        "mon_info_t" => Some(type_info_denc::<MonInfo>()),

        // MonMap - Monitor cluster map
        // Dependencies: FsId, UTime×2 (Level 1), MonFeature×2, MonInfo (Level 3), MonCephRelease, ElectionStrategy
        "MonMap" => Some(type_info_denc::<MonMap>()),

        // Auth types (commented out if auth crate not available)
        // "CryptoKey" => Some(type_info_denc::<CryptoKey>()),
        _ => None,
    }
}

/// List all available types
///
/// Types are grouped by dependency level for systematic testing.
/// Always test Level 1 completely before moving to Level 2, etc.
fn list_types() {
    println!("Available types (ordered by dependency level):");
    println!();
    println!("LEVEL 1: Primitive Types (no Denc dependencies)");
    println!("  Test these FIRST - they are the foundation");
    println!("  pg_t              - Placement group ID [simple]");
    println!("  eversion_t        - Event version [simple]");
    println!("  utime_t           - Unix timestamp [simple]");
    println!("  uuid_d            - UUID [simple]");
    println!("  osd_info_t        - OSD information [simple]");
    println!();
    println!("LEVEL 2: Types depending on Level 1");
    println!("  Test these ONLY after Level 1 is 100% validated");
    println!("  entity_addr_t     - Entity address [versioned, feature-dependent: MSG_ADDR2]");
    println!("  pool_snap_info_t  - Pool snapshot info [versioned]");
    println!("  osd_xinfo_t       - Extended OSD info [versioned, feature-dependent: OCTOPUS]");
    println!();
    println!("LEVEL 3: Complex types");
    println!("  Test these ONLY after Level 1 & 2 are validated");
    println!("  pg_merge_meta_t   - PG merge metadata [versioned]");
    println!("  object_locator_t  - Object placement information [versioned]");
    println!("  hobject_t         - Hashed object identifier [versioned]");
    println!("  pg_nls_response_t - PG namespace list response [versioned]");
    println!("  pg_pool_t         - Pool configuration [versioned, feature-dependent: multiple]");
    println!();
    println!("LEVEL 4: Top-level cluster structures");
    println!("  Test these ONLY after all lower levels are validated");
    println!("  OSDMap            - OSD cluster map [versioned, feature-dependent: multiple]");
    println!(
        "  mon_info_t        - Monitor information [versioned, feature-dependent: SERVER_NAUTILUS]"
    );
    println!("  MonMap            - Monitor cluster map [versioned, feature-dependent: MONENC, SERVER_NAUTILUS]");
    println!();
    println!("Encoding Properties:");
    println!("  [simple]              - No versioning, no feature dependency");
    println!("  [versioned]           - Uses ENCODE_START/DECODE_START");
    println!("  [feature-dependent]   - Encoding changes based on feature flags");
    println!();
    println!("CRITICAL: All decodes must show '0 bytes remaining' for validation");
    println!("Testing order: Level 1 → Level 2 → Level 3");
}

/// State maintained across command execution
struct DencoderState {
    current_type: Option<TypeInfo>,
    features: u64,
    raw_data: Option<Bytes>,
    decoded: Option<Box<dyn SerializableType>>,
}

impl DencoderState {
    fn new() -> Self {
        Self {
            current_type: None,
            features: 0,
            raw_data: None,
            decoded: None,
        }
    }
}

/// Error type for dencoder operations
#[derive(Debug)]
enum DencoderError {
    NoTypeSelected,
    UnknownType(String),
    NoDataImported,
    NothingDecoded,
    DecodeError(RadosError),
    EncodeError(RadosError),
    IoError(io::Error),
    InvalidFeatures(String),
    JsonError(serde_json::Error),
    MissingArgument(String),
}

impl fmt::Display for DencoderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NoTypeSelected => {
                write!(f, "Error: No type selected. Use 'type <typename>' first.")
            }
            Self::UnknownType(t) => write!(
                f,
                "Error: Unknown type '{}'. Use 'list_types' to see available types.",
                t
            ),
            Self::NoDataImported => {
                write!(f, "Error: No data loaded. Use 'import <file>' first.")
            }
            Self::NothingDecoded => {
                write!(f, "Error: Nothing decoded yet. Use 'decode' first.")
            }
            Self::DecodeError(e) => write!(f, "Decode error: {}", e),
            Self::EncodeError(e) => write!(f, "Encode error: {}", e),
            Self::IoError(e) => write!(f, "I/O error: {}", e),
            Self::InvalidFeatures(s) => write!(f, "Invalid features: {}", s),
            Self::JsonError(e) => write!(f, "JSON error: {}", e),
            Self::MissingArgument(cmd) => write!(f, "Error: Missing argument for '{}'", cmd),
        }
    }
}

impl From<io::Error> for DencoderError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e)
    }
}

impl From<RadosError> for DencoderError {
    fn from(e: RadosError) -> Self {
        Self::DecodeError(e)
    }
}

impl From<serde_json::Error> for DencoderError {
    fn from(e: serde_json::Error) -> Self {
        Self::JsonError(e)
    }
}

type Result<T> = std::result::Result<T, DencoderError>;

/// Command: Select type to work with
fn cmd_type(state: &mut DencoderState, typename: &str) -> Result<()> {
    let type_info =
        get_type_info(typename).ok_or_else(|| DencoderError::UnknownType(typename.to_string()))?;

    state.current_type = Some(type_info);
    println!("Selected type: {}", typename);
    Ok(())
}

/// Command: Import binary data from file
fn cmd_import(state: &mut DencoderState, filename: &str) -> Result<()> {
    let data = if filename == "-" {
        // Read from stdin
        let mut buffer = Vec::new();
        io::stdin().read_to_end(&mut buffer)?;
        buffer
    } else {
        fs::read(filename)?
    };

    println!("Imported {} bytes from {}", data.len(), filename);
    state.raw_data = Some(Bytes::from(data));
    Ok(())
}

/// Command: Decode binary data to object
fn cmd_decode(state: &mut DencoderState) -> Result<()> {
    let type_info = state
        .current_type
        .as_ref()
        .ok_or(DencoderError::NoTypeSelected)?;

    let mut data = state
        .raw_data
        .clone()
        .ok_or(DencoderError::NoDataImported)?;

    let original_len = data.len();
    state.decoded = Some((type_info.decode_fn)(&mut data, state.features)?);

    let consumed = original_len - data.len();
    println!(
        "Decoded successfully ({} bytes consumed, {} bytes remaining)",
        consumed,
        data.len()
    );

    Ok(())
}

/// Command: Encode object to binary
fn cmd_encode(state: &mut DencoderState) -> Result<()> {
    let type_info = state
        .current_type
        .as_ref()
        .ok_or(DencoderError::NoTypeSelected)?;

    let decoded = state
        .decoded
        .as_ref()
        .ok_or(DencoderError::NothingDecoded)?;

    state.raw_data = Some((type_info.encode_fn)(decoded.as_ref(), state.features)?);

    println!(
        "Encoded successfully ({} bytes)",
        state.raw_data.as_ref().unwrap().len()
    );
    Ok(())
}

/// Command: Dump object as JSON
fn cmd_dump_json(state: &DencoderState) -> Result<()> {
    let decoded = state
        .decoded
        .as_ref()
        .ok_or(DencoderError::NothingDecoded)?;

    let json = decoded.to_json().map_err(|e| {
        DencoderError::JsonError(serde_json::Error::io(io::Error::other(e.to_string())))
    })?;

    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}

/// Command: Export binary data to file
fn cmd_export(state: &DencoderState, filename: &str) -> Result<()> {
    let data = state
        .raw_data
        .as_ref()
        .ok_or(DencoderError::NoDataImported)?;

    fs::write(filename, data)?;
    println!("Exported {} bytes to {}", data.len(), filename);
    Ok(())
}

/// Command: Show hex dump of binary data
fn cmd_hexdump(state: &DencoderState) -> Result<()> {
    let data = state
        .raw_data
        .as_ref()
        .ok_or(DencoderError::NoDataImported)?;

    println!("Hex dump ({} bytes):", data.len());
    for (i, chunk) in data.chunks(16).enumerate() {
        print!("{:08x}  ", i * 16);

        // Hex bytes
        for (j, byte) in chunk.iter().enumerate() {
            if j == 8 {
                print!(" ");
            }
            print!("{:02x} ", byte);
        }

        // Padding
        for _ in chunk.len()..16 {
            print!("   ");
        }
        if chunk.len() <= 8 {
            print!(" ");
        }

        // ASCII representation
        print!(" |");
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }

    Ok(())
}

/// Command: Set feature flags
fn cmd_set_features(state: &mut DencoderState, features_str: &str) -> Result<()> {
    let features = if features_str.starts_with("0x") || features_str.starts_with("0X") {
        u64::from_str_radix(&features_str[2..], 16)
    } else {
        features_str.parse::<u64>()
    }
    .map_err(|_| DencoderError::InvalidFeatures(features_str.to_string()))?;

    state.features = features;
    println!("Set features to 0x{:x}", features);
    Ok(())
}

/// Command: Get current feature flags
fn cmd_get_features(state: &DencoderState) -> Result<()> {
    println!(
        "Current features: 0x{:x} ({})",
        state.features, state.features
    );
    Ok(())
}

/// Process a single command
fn process_command(state: &mut DencoderState, cmd: &str, args: &[String]) -> Result<()> {
    match cmd {
        "type" => {
            let typename = args
                .first()
                .ok_or_else(|| DencoderError::MissingArgument("type".to_string()))?;
            cmd_type(state, typename)
        }
        "import" => {
            let filename = args
                .first()
                .ok_or_else(|| DencoderError::MissingArgument("import".to_string()))?;
            cmd_import(state, filename)
        }
        "decode" => cmd_decode(state),
        "encode" => cmd_encode(state),
        "dump_json" => cmd_dump_json(state),
        "export" => {
            let filename = args
                .first()
                .ok_or_else(|| DencoderError::MissingArgument("export".to_string()))?;
            cmd_export(state, filename)
        }
        "hexdump" => cmd_hexdump(state),
        "set_features" => {
            let features_str = args
                .first()
                .ok_or_else(|| DencoderError::MissingArgument("set_features".to_string()))?;
            cmd_set_features(state, features_str)
        }
        "get_features" => cmd_get_features(state),
        "list_types" => {
            list_types();
            Ok(())
        }
        _ => {
            eprintln!("Unknown command: {}", cmd);
            print_usage();
            process::exit(1);
        }
    }
}

/// Print usage information
fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  dencoder type <typename> [command] [command] ...");
    eprintln!("  dencoder list_types");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  type <name>        - Select type to work with");
    eprintln!("  import <file>      - Read binary file (use \"-\" for stdin)");
    eprintln!("  decode             - Decode binary data to object");
    eprintln!("  encode             - Encode object to binary");
    eprintln!("  dump_json          - Output object as JSON");
    eprintln!("  export <file>      - Write binary data to file");
    eprintln!("  hexdump            - Show hex representation of binary data");
    eprintln!("  set_features <hex> - Set feature flags (hex or decimal)");
    eprintln!("  get_features       - Show current feature flags");
    eprintln!("  list_types         - List all available types");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  dencoder type pg_pool_t import pool.bin decode dump_json");
    eprintln!("  dencoder type entity_addr_t set_features 0x40000000000000 import addr.bin decode dump_json");
    eprintln!("  dencoder list_types");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        process::exit(1);
    }

    // Special case: list_types can be called without other commands
    if args[1] == "list_types" {
        list_types();
        return;
    }

    let mut state = DencoderState::new();
    let mut i = 1;

    while i < args.len() {
        let cmd = &args[i];

        // Collect arguments for this command
        let mut cmd_args = Vec::new();
        let mut j = i + 1;

        // Determine how many arguments this command needs
        let arg_count = match cmd.as_str() {
            "type" | "import" | "export" | "set_features" => 1,
            "decode" | "encode" | "dump_json" | "hexdump" | "get_features" | "list_types" => 0,
            _ => 0,
        };

        // Collect the arguments
        for _ in 0..arg_count {
            if j < args.len() {
                cmd_args.push(args[j].clone());
                j += 1;
            }
        }

        // Process the command
        if let Err(e) = process_command(&mut state, cmd, &cmd_args) {
            eprintln!("{}", e);
            process::exit(1);
        }

        i = j;
    }
}
