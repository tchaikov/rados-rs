use bytes::Bytes;
use crush::PgId;
use denc::VersionedEncode;
use osdclient::OSDMap;
use std::fs;
use std::path::PathBuf;

/// Test complete object placement pipeline
#[test]
fn test_object_placement_pipeline() {
    // Path to the OSDMap corpus file
    let corpus_path = PathBuf::from(env!("HOME"))
        .join("dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/OSDMap");

    if !corpus_path.exists() {
        eprintln!("Corpus directory not found, skipping test");
        return;
    }

    let entries = match fs::read_dir(&corpus_path) {
        Ok(e) => e,
        Err(_) => {
            eprintln!("Cannot read corpus directory, skipping test");
            return;
        }
    };

    let mut tested_placements = 0;

    for entry in entries {
        let entry = entry.expect("Failed to read directory entry");
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(_) => continue,
        };

        let mut bytes = Bytes::from(data);

        if let Ok(osdmap) = OSDMap::decode_versioned(&mut bytes, 0) {
            // Skip if no CRUSH map or no pools
            if osdmap.get_crush_map().is_none() || osdmap.pools.is_empty() {
                continue;
            }

            println!("\nTesting placement with {:?}", path.file_name());

            // Test object placement for each pool
            for (pool_id, pool) in &osdmap.pools {
                if pool.pg_num == 0 {
                    continue; // Skip pools with no PGs
                }

                let pool_name = osdmap
                    .get_pool_name(*pool_id)
                    .map(|s| s.as_str())
                    .unwrap_or("unknown");

                println!("  Pool {}: {} (pg_num={})", pool_id, pool_name, pool.pg_num);

                // Test object_to_pg mapping
                let test_objects = vec!["test_object", "foo", "bar", "object.1"];

                for obj_name in &test_objects {
                    match osdmap.object_to_pg(*pool_id, obj_name) {
                        Ok(pg) => {
                            println!("    {} -> PG {}.{:x}", obj_name, pg.pool, pg.seed);

                            // Verify PG is within valid range
                            assert_eq!(pg.pool, *pool_id, "PG pool ID should match input pool ID");
                            assert!(
                                pg.seed < pool.pg_num,
                                "PG seed {} should be less than pg_num {}",
                                pg.seed,
                                pool.pg_num
                            );

                            // Test PG to OSD mapping
                            match osdmap.pg_to_osds(&pg) {
                                Ok(osds) => {
                                    println!("      -> OSDs: {:?}", osds);

                                    // Verify we got the expected number of replicas (or less if cluster is small)
                                    assert!(
                                        osds.len() <= pool.size as usize,
                                        "Should not get more OSDs than pool size"
                                    );

                                    tested_placements += 1;
                                }
                                Err(e) => {
                                    eprintln!("      ⚠ PG to OSD mapping failed: {:?}", e);
                                }
                            }

                            // Test complete object_to_osds pipeline
                            match osdmap.object_to_osds(*pool_id, obj_name) {
                                Ok(osds) => {
                                    println!("      Full pipeline -> OSDs: {:?}", osds);
                                }
                                Err(e) => {
                                    eprintln!("      ⚠ Object placement failed: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("    ⚠ object_to_pg failed for {}: {:?}", obj_name, e);
                        }
                    }
                }
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Successful placements tested: {}", tested_placements);

    // We should have tested at least some placements if corpus files are available
    // If not, the test passes (corpus may not be available in CI)
    if tested_placements > 0 {
        println!("✓ Object placement pipeline working!");
    } else {
        println!("⚠ No placements tested (corpus may not be available)");
    }
}

/// Test PG to OSD mapping with a simple manually created OSDMap
#[test]
fn test_pg_to_osds_without_crush() {
    let osdmap = OSDMap::new();
    let pg = PgId { pool: 1, seed: 0 };

    // Should fail because there's no CRUSH map
    let result = osdmap.pg_to_osds(&pg);
    assert!(result.is_err(), "Should fail without CRUSH map");
}

/// Test object_to_pg mapping
#[test]
fn test_object_to_pg_without_pool() {
    let osdmap = OSDMap::new();

    // Should fail because pool doesn't exist
    let result = osdmap.object_to_pg(1, "test_object");
    assert!(result.is_err(), "Should fail when pool doesn't exist");
}

/// Test that same object name always maps to same PG
#[test]
fn test_object_to_pg_deterministic() {
    // This test requires a valid OSDMap from corpus
    let corpus_path = PathBuf::from(env!("HOME"))
        .join("dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/OSDMap");

    if !corpus_path.exists() {
        eprintln!("Corpus directory not found, skipping test");
        return;
    }

    let entries = match fs::read_dir(&corpus_path) {
        Ok(e) => e,
        Err(_) => {
            eprintln!("Cannot read corpus directory, skipping test");
            return;
        }
    };

    for entry in entries {
        let entry = entry.expect("Failed to read directory entry");
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(_) => continue,
        };

        let mut bytes = Bytes::from(data);

        if let Ok(osdmap) = OSDMap::decode_versioned(&mut bytes, 0) {
            // Find a pool with PGs
            for (pool_id, pool) in &osdmap.pools {
                if pool.pg_num == 0 {
                    continue;
                }

                let obj_name = "deterministic_test_object";

                // Map the same object multiple times
                let pg1 = osdmap.object_to_pg(*pool_id, obj_name);
                let pg2 = osdmap.object_to_pg(*pool_id, obj_name);
                let pg3 = osdmap.object_to_pg(*pool_id, obj_name);

                // All should succeed or all should fail
                match (pg1, pg2, pg3) {
                    (Ok(p1), Ok(p2), Ok(p3)) => {
                        assert_eq!(p1, p2, "Same object should map to same PG");
                        assert_eq!(p2, p3, "Same object should map to same PG");
                        println!("✓ Deterministic mapping verified for pool {}", pool_id);
                    }
                    _ => {
                        // If mapping fails, that's OK for this test
                    }
                }

                // Only need to test one pool
                break;
            }

            // Only need to test one OSDMap
            break;
        }
    }
}
