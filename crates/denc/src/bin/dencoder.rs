//! Rust implementation of ceph-dencoder
//!
//! This tool can decode, encode, and inspect Ceph data structures from binary corpus files.
//!
//! Usage:
//!   dencoder list_types
//!   dencoder type <typename> import <file> decode dump_json
//!
//! Commands:
//!   list_types         - List all available types
//!   type <name>        - Select type to work with
//!   import <file>      - Read binary file (use "-" for stdin)
//!   decode             - Decode binary data to object
//!   dump_json          - Output object as JSON
//!   hexdump            - Show hex representation of binary data

use std::process;

/// Print usage information
fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  dencoder list_types");
    eprintln!("  dencoder type <typename> import <file> decode dump_json");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  list_types         - List all available types");
    eprintln!("  type <name>        - Select type to work with");
    eprintln!("  import <file>      - Read binary file (use \"-\" for stdin)");
    eprintln!("  decode             - Decode binary data to object");
    eprintln!("  dump_json          - Output object as JSON");
    eprintln!("  hexdump            - Show hex representation of binary data");
}

/// List all available types
fn list_types() {
    println!("Available types:");
    println!("  (No types registered yet - types will be added in subsequent commits)");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        process::exit(1);
    }

    // For now, only support list_types
    if args[1] == "list_types" {
        list_types();
        return;
    }

    eprintln!("Error: Type encoding/decoding not yet implemented.");
    eprintln!("This will be added in subsequent commits as types are implemented.");
    process::exit(1);
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_dencoder_exists() {
        // Test that the dencoder binary can be compiled
        // Actual functionality will be tested as types are added
        assert!(true);
    }
}
