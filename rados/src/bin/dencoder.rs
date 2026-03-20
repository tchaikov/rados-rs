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
use rados::osdclient::osdmap::{OsdInfo, OsdXInfo, PgId};
use rados::osdclient::{OSDMap, ObjectLocator, ObjectstorePerfStat, PgMergeMeta, PgPool, PoolStat};
use rados::{
    Denc, EVersion, EntityAddr, HObject, MonInfo, MonMap, PgNlsResponse, PoolSnapInfo, RadosError,
    UTime, UuidD, VersionedEncode,
};
use serde::Serialize;
use std::any::Any;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::process;

/// Type-erased serializable object supporting JSON output and downcasting.
trait SerializableType: Any {
    fn to_json(&self) -> std::result::Result<serde_json::Value, serde_json::Error>;
    fn as_any(&self) -> &dyn Any;
}

impl<T: Serialize + 'static> SerializableType for T {
    fn to_json(&self) -> std::result::Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct TypeInfo {
    decode_fn: fn(&mut Bytes, u64) -> Result<Box<dyn SerializableType>>,
    encode_fn: fn(&dyn SerializableType, u64) -> Result<Bytes>,
}

fn decode_denc<T>(bytes: &mut Bytes, features: u64) -> Result<Box<dyn SerializableType>>
where
    T: Denc + Serialize + 'static,
{
    let obj = T::decode(bytes, features)?;
    Ok(Box::new(obj))
}

fn decode_versioned<T>(bytes: &mut Bytes, features: u64) -> Result<Box<dyn SerializableType>>
where
    T: VersionedEncode + Serialize + 'static,
{
    let obj = T::decode_versioned(bytes, features)?;
    Ok(Box::new(obj))
}

fn downcast_and_encode<T>(
    obj: &dyn SerializableType,
    features: u64,
    encode: fn(&T, &mut bytes::BytesMut, u64) -> std::result::Result<(), RadosError>,
) -> Result<Bytes>
where
    T: 'static,
{
    let typed = obj
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| RadosError::Protocol("Type mismatch".to_string()))?;
    let mut buf = bytes::BytesMut::with_capacity(256);
    encode(typed, &mut buf, features)?;
    Ok(buf.freeze())
}

fn encode_denc<T>(obj: &dyn SerializableType, features: u64) -> Result<Bytes>
where
    T: Denc + 'static,
{
    downcast_and_encode::<T>(obj, features, T::encode)
}

fn encode_versioned<T>(obj: &dyn SerializableType, features: u64) -> Result<Bytes>
where
    T: VersionedEncode + 'static,
{
    downcast_and_encode::<T>(obj, features, T::encode_versioned)
}

fn type_info_denc<T>() -> TypeInfo
where
    T: Denc + Serialize + 'static,
{
    TypeInfo {
        decode_fn: decode_denc::<T>,
        encode_fn: encode_denc::<T>,
    }
}

fn type_info_versioned<T>() -> TypeInfo
where
    T: VersionedEncode + Serialize + 'static,
{
    TypeInfo {
        decode_fn: decode_versioned::<T>,
        encode_fn: encode_versioned::<T>,
    }
}

fn get_type_info(name: &str) -> Option<TypeInfo> {
    match name {
        // Level 1: Primitive types
        "pg_t" | "pg_id" => Some(type_info_denc::<PgId>()),
        "eversion_t" => Some(type_info_denc::<EVersion>()),
        "utime_t" => Some(type_info_denc::<UTime>()),
        "uuid_d" => Some(type_info_denc::<UuidD>()),
        "osd_info_t" => Some(type_info_denc::<OsdInfo>()),

        // Level 2: Types depending on Level 1
        "entity_addr_t" => Some(type_info_denc::<EntityAddr>()),
        "pool_snap_info_t" => Some(type_info_versioned::<PoolSnapInfo>()),
        "osd_xinfo_t" => Some(type_info_denc::<OsdXInfo>()),

        // Level 3: Complex types
        "pg_merge_meta_t" => Some(type_info_denc::<PgMergeMeta>()),
        "object_locator_t" => Some(type_info_denc::<ObjectLocator>()),
        "objectstore_perf_stat_t" => Some(type_info_denc::<ObjectstorePerfStat>()),
        "pool_stat_t" => Some(type_info_denc::<PoolStat>()),
        "hobject_t" => Some(type_info_denc::<HObject>()),
        "pg_nls_response_t" => Some(type_info_denc::<PgNlsResponse>()),
        "pg_pool_t" => Some(type_info_denc::<PgPool>()),

        // Level 4: Top-level cluster structures
        "OSDMap" => Some(type_info_versioned::<OSDMap>()),
        "mon_info_t" => Some(type_info_denc::<MonInfo>()),
        "MonMap" => Some(type_info_denc::<MonMap>()),

        _ => None,
    }
}

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
    println!(
        "  entity_addr_t     - Entity address [versioned, modern encode with legacy decode compatibility]"
    );
    println!("  pool_snap_info_t  - Pool snapshot info [versioned]");
    println!("  osd_xinfo_t       - Extended OSD info [versioned, Octopus+ encode contract]");
    println!();
    println!("LEVEL 3: Complex types");
    println!("  Test these ONLY after Level 1 & 2 are validated");
    println!("  pg_merge_meta_t   - PG merge metadata [versioned]");
    println!("  object_locator_t  - Object placement information [versioned]");
    println!(
        "  objectstore_perf_stat_t - Objectstore latency stats [versioned, Octopus+ encode contract]"
    );
    println!(
        "  pool_stat_t       - Aggregate per-pool stats [versioned, Octopus+ encode contract]"
    );
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
    println!(
        "  MonMap            - Monitor cluster map [versioned, feature-dependent: MONENC, SERVER_NAUTILUS]"
    );
    println!();
    println!("Encoding Properties:");
    println!("  [simple]              - No versioning, no feature dependency");
    println!("  [versioned]           - Uses ENCODE_START/DECODE_START");
    println!("  [feature-dependent]   - Encoding changes based on feature flags");
    println!();
    println!("CRITICAL: All decodes must show '0 bytes remaining' for validation");
    println!("Testing order: Level 1 → Level 2 → Level 3");
}

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

#[derive(Debug)]
enum DencoderError {
    NoTypeSelected,
    UnknownType(String),
    NoDataImported,
    NothingDecoded,
    Rados(RadosError),
    Io(io::Error),
    InvalidFeatures(String),
    Json(serde_json::Error),
    MissingArgument(String),
    UnknownCommand(String),
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
            Self::Rados(e) => write!(f, "{}", e),
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::InvalidFeatures(s) => write!(f, "Invalid features: {}", s),
            Self::Json(e) => write!(f, "JSON error: {}", e),
            Self::MissingArgument(cmd) => write!(f, "Error: Missing argument for '{}'", cmd),
            Self::UnknownCommand(cmd) => write!(
                f,
                "Error: Unknown command '{}'. Use 'list_types' to see available commands.",
                cmd
            ),
        }
    }
}

impl From<io::Error> for DencoderError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<RadosError> for DencoderError {
    fn from(e: RadosError) -> Self {
        Self::Rados(e)
    }
}

impl From<serde_json::Error> for DencoderError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

type Result<T> = std::result::Result<T, DencoderError>;

fn cmd_type(state: &mut DencoderState, typename: &str) -> Result<()> {
    let type_info =
        get_type_info(typename).ok_or_else(|| DencoderError::UnknownType(typename.to_string()))?;

    state.current_type = Some(type_info);
    println!("Selected type: {}", typename);
    Ok(())
}

fn cmd_import(state: &mut DencoderState, filename: &str) -> Result<()> {
    let data = if filename == "-" {
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

fn cmd_encode(state: &mut DencoderState) -> Result<()> {
    let type_info = state
        .current_type
        .as_ref()
        .ok_or(DencoderError::NoTypeSelected)?;

    let decoded = state
        .decoded
        .as_ref()
        .ok_or(DencoderError::NothingDecoded)?;

    let encoded = (type_info.encode_fn)(decoded.as_ref(), state.features)?;
    println!("Encoded successfully ({} bytes)", encoded.len());
    state.raw_data = Some(encoded);
    Ok(())
}

fn cmd_dump_json(state: &DencoderState) -> Result<()> {
    let decoded = state
        .decoded
        .as_ref()
        .ok_or(DencoderError::NothingDecoded)?;

    let json = decoded.to_json()?;

    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}

fn cmd_export(state: &DencoderState, filename: &str) -> Result<()> {
    let data = state
        .raw_data
        .as_ref()
        .ok_or(DencoderError::NoDataImported)?;

    fs::write(filename, data)?;
    println!("Exported {} bytes to {}", data.len(), filename);
    Ok(())
}

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

fn cmd_set_features(state: &mut DencoderState, features_str: &str) -> Result<()> {
    let features = if features_str.starts_with("0x") || features_str.starts_with("0X") {
        u64::from_str_radix(&features_str[2..], 16)
    } else {
        features_str.parse::<u64>()
    }
    .map_err(|e| DencoderError::InvalidFeatures(format!("{features_str}: {e}")))?;

    state.features = features;
    println!("Set features to 0x{:x}", features);
    Ok(())
}

fn cmd_get_features(state: &DencoderState) -> Result<()> {
    println!(
        "Current features: 0x{:x} ({})",
        state.features, state.features
    );
    Ok(())
}

fn require_arg<'a>(args: &'a [String], cmd: &str) -> Result<&'a str> {
    args.first()
        .map(String::as_str)
        .ok_or_else(|| DencoderError::MissingArgument(cmd.to_string()))
}

fn process_command(state: &mut DencoderState, cmd: &str, args: &[String]) -> Result<()> {
    match cmd {
        "type" => cmd_type(state, require_arg(args, cmd)?),
        "import" => cmd_import(state, require_arg(args, cmd)?),
        "decode" => cmd_decode(state),
        "encode" => cmd_encode(state),
        "dump_json" => cmd_dump_json(state),
        "export" => cmd_export(state, require_arg(args, cmd)?),
        "hexdump" => cmd_hexdump(state),
        "set_features" => cmd_set_features(state, require_arg(args, cmd)?),
        "get_features" => cmd_get_features(state),
        "list_types" => {
            list_types();
            Ok(())
        }
        _ => {
            print_usage();
            Err(DencoderError::UnknownCommand(cmd.to_string()))
        }
    }
}

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
    eprintln!(
        "  dencoder type entity_addr_t set_features 0x40000000000000 import addr.bin decode dump_json"
    );
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
