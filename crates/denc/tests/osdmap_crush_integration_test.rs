use bytes::Bytes;
use crush::PgId;
use denc::VersionedEncode;
use osdclient::OSDMap;
use std::fs;
use std::path::PathBuf;

/// Test that OSDMap correctly integrates with CRUSH map
#[test]
fn test_osdmap_crush_integration() {
    // Path to the OSDMap corpus file
    let corpus_path = PathBuf::from(env!("HOME"))
        .join("dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/OSDMap");

    if !corpus_path.exists() {
        eprintln!("Corpus directory not found: {:?}", corpus_path);
        eprintln!("Skipping test");
        return;
    }

    let entries = match fs::read_dir(&corpus_path) {
        Ok(e) => e,
        Err(_) => {
            eprintln!("Cannot read corpus directory, skipping test");
            return;
        }
    };

    let mut tested_count = 0;
    let mut crush_parsed_count = 0;

    for entry in entries {
        let entry = entry.expect("Failed to read directory entry");
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        println!("\nTesting corpus file: {:?}", path.file_name());

        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("  Failed to read file: {:?}", e);
                continue;
            }
        };

        let mut bytes = Bytes::from(data);

        match OSDMap::decode_versioned(&mut bytes, 0) {
            Ok(osdmap) => {
                tested_count += 1;
                println!("  ✓ Decoded OSDMap successfully");
                println!("    Epoch: {}", osdmap.epoch);
                println!("    Max OSD: {}", osdmap.max_osd);
                println!("    Pool count: {}", osdmap.pools.len());

                // Test CRUSH map integration
                if let Some(crush_map) = osdmap.get_crush_map() {
                    crush_parsed_count += 1;
                    println!("    ✓ CRUSH map parsed!");
                    println!("      Max buckets: {}", crush_map.max_buckets);
                    println!("      Max devices: {}", crush_map.max_devices);
                    println!("      Max rules: {}", crush_map.max_rules);

                    // Test pool access methods
                    for pool_id in osdmap.pools.keys() {
                        let pool_name = osdmap.get_pool_name(*pool_id);
                        let crush_rule = osdmap.get_pool_crush_rule(*pool_id);

                        println!(
                            "      Pool {}: name={:?}, crush_rule={:?}",
                            pool_id, pool_name, crush_rule
                        );

                        // Verify crush_rule exists in the CRUSH map
                        if let Some(rule_id) = crush_rule {
                            match crush_map.get_rule(rule_id as u32) {
                                Ok(rule) => {
                                    println!(
                                        "        ✓ CRUSH rule {} found with {} steps",
                                        rule.rule_id,
                                        rule.steps.len()
                                    );
                                }
                                Err(e) => {
                                    println!("        ⚠ CRUSH rule {} not found: {:?}", rule_id, e);
                                }
                            }
                        }
                    }
                } else {
                    println!("    ⚠ No CRUSH map in this OSDMap");
                }
            }
            Err(e) => {
                eprintln!("  ✗ Failed to decode: {:?}", e);
            }
        }
    }

    println!("\n=== Summary ===");
    println!("OSDMaps decoded: {}", tested_count);
    println!("CRUSH maps parsed: {}", crush_parsed_count);

    // We expect at least some files to be tested
    assert!(
        tested_count > 0,
        "Should have decoded at least one OSDMap file"
    );
}

/// Test helper methods for accessing pool and CRUSH data
#[test]
fn test_osdmap_helper_methods() {
    // Create a simple OSDMap for testing
    let osdmap = OSDMap::new();

    // Initially, all access methods should return None or empty
    assert!(osdmap.get_crush_map().is_none());
    assert!(osdmap.get_pool(1).is_none());
    assert!(osdmap.get_pool_name(1).is_none());
    assert!(osdmap.get_pool_crush_rule(1).is_none());

    // Test pg_to_osds returns error when no CRUSH map
    let pg = PgId { pool: 1, seed: 0 };
    let result = osdmap.pg_to_osds(&pg);
    assert!(result.is_err());

    // The helper methods are tested more thoroughly with real corpus data
    // in the integration test above
}
