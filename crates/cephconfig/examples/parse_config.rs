//! Example: Parse a Ceph configuration file
//!
//! This example demonstrates how to parse a ceph.conf file and extract
//! common configuration options.
//!
//! Usage:
//!   cargo run --example parse_config /path/to/ceph.conf

use cephconfig::CephConfig;
use std::env;

fn main() {
    // Get config file path from command line or use default
    let args: Vec<String> = env::args().collect();
    let config_path = if args.len() > 1 {
        &args[1]
    } else {
        "/home/kefu/dev/ceph/build/ceph.conf"
    };

    println!("Parsing Ceph configuration from: {}", config_path);
    println!();

    // Parse the configuration file
    let config = match CephConfig::from_file(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error parsing config file: {}", e);
            std::process::exit(1);
        }
    };

    // Display sections
    println!("📋 Configuration sections:");
    for section in config.sections() {
        println!("  - [{}]", section);
    }
    println!();

    // Get monitor addresses
    match config.mon_addrs() {
        Ok(addrs) => {
            println!("🖥️  Monitor addresses ({} total):", addrs.len());
            for addr in &addrs {
                let protocol = if addr.starts_with("v2:") {
                    "v2"
                } else if addr.starts_with("v1:") {
                    "v1"
                } else {
                    "??"
                };
                println!("  - {} ({})", addr, protocol);
            }
            println!();

            // Get first v2 address
            if let Ok(v2_addr) = config.first_v2_mon_addr() {
                println!("✓ First v2 monitor: {}", v2_addr);
                println!();
            }
        }
        Err(e) => {
            eprintln!("⚠️  Could not get monitor addresses: {}", e);
            println!();
        }
    }

    // Get keyring path
    match config.keyring() {
        Ok(keyring) => {
            println!("🔑 Keyring path: {}", keyring);
        }
        Err(e) => {
            eprintln!("⚠️  Could not get keyring path: {}", e);
        }
    }
    println!();

    // Get entity name
    let entity_name = config.entity_name();
    println!("👤 Entity name: {}", entity_name);
    println!();

    // Display some global settings
    println!("⚙️  Global settings:");
    if let Some(fsid) = config.get("global", "fsid") {
        println!("  - FSID: {}", fsid);
    }
    if let Some(auth_cluster) = config.get("global", "auth cluster required") {
        println!("  - Auth cluster required: {}", auth_cluster);
    }
    if let Some(auth_service) = config.get("global", "auth service required") {
        println!("  - Auth service required: {}", auth_service);
    }
    if let Some(auth_client) = config.get("global", "auth client required") {
        println!("  - Auth client required: {}", auth_client);
    }
    println!();

    // Display client settings
    println!("👥 Client settings:");
    for key in config.keys("client") {
        if let Some(value) = config.get("client", key) {
            println!("  - {}: {}", key, value);
        }
    }
}
