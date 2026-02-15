use bytes::BytesMut;
use denc::features::*;
use denc::Denc;
use osdclient::PgPool;
use std::fs;
use std::path::PathBuf;

/// Get the path to the external ceph-object-corpus
fn get_corpus_dir() -> PathBuf {
    // Try multiple locations for the corpus
    let possible_paths = [
        PathBuf::from(std::env::var("HOME").unwrap_or_default())
            .join("dev/ceph/ceph-object-corpus"),
        PathBuf::from("/tmp/ceph-object-corpus"),
        PathBuf::from(std::env::var("CORPUS_DIR").unwrap_or_default()),
    ];

    for path in &possible_paths {
        if path.exists() {
            return path.clone();
        }
    }

    // Default to ~/dev/ceph/ceph-object-corpus
    PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("dev/ceph/ceph-object-corpus")
}

const CORPUS_VERSION: &str = "19.2.0-404-g78ddc7f9027";

/// Determine the features needed to encode a pg_pool_t with the given version
/// Based on the encoding_version logic in PgPool::encoding_version
fn features_for_version(version: u8, _is_stretch_pool: bool) -> u64 {
    // Start with all significant features
    let mut features = SIGNIFICANT_FEATURES;

    // Remove features based on version to match the encoding_version logic
    match version {
        ..=21 => {
            // Version 21 or below: no NEW_OSDOP_ENCODING
            features &= !CEPH_FEATUREMASK_NEW_OSDOP_ENCODING;
            features &= !CEPH_FEATUREMASK_SERVER_LUMINOUS;
            features &= !CEPH_FEATUREMASK_SERVER_MIMIC;
            features &= !CEPH_FEATUREMASK_SERVER_NAUTILUS;
            features &= !CEPH_FEATUREMASK_SERVER_TENTACLE;
        }
        22..=24 => {
            // Version 24: has NEW_OSDOP_ENCODING, no SERVER_LUMINOUS
            features &= !CEPH_FEATUREMASK_SERVER_LUMINOUS;
            features &= !CEPH_FEATUREMASK_SERVER_MIMIC;
            features &= !CEPH_FEATUREMASK_SERVER_NAUTILUS;
            features &= !CEPH_FEATUREMASK_SERVER_TENTACLE;
        }
        25..=26 => {
            // Version 26: has SERVER_LUMINOUS, no SERVER_MIMIC
            features &= !CEPH_FEATUREMASK_SERVER_MIMIC;
            features &= !CEPH_FEATUREMASK_SERVER_NAUTILUS;
            features &= !CEPH_FEATUREMASK_SERVER_TENTACLE;
        }
        27..=28 => {
            // Version 27: has SERVER_MIMIC, no SERVER_NAUTILUS
            features &= !CEPH_FEATUREMASK_SERVER_NAUTILUS;
            features &= !CEPH_FEATUREMASK_SERVER_TENTACLE;
        }
        29 => {
            // Version 29: has SERVER_NAUTILUS, no SERVER_TENTACLE, not stretch pool
            features &= !CEPH_FEATUREMASK_SERVER_TENTACLE;
        }
        30 => {
            // Version 30: has SERVER_NAUTILUS, no SERVER_TENTACLE, is stretch pool
            features &= !CEPH_FEATUREMASK_SERVER_TENTACLE;
        }
        31 => {
            // Version 31: has SERVER_TENTACLE
            // All features enabled (note: version 31 uses optional encoding for stretch pool)
        }
        _ => {
            // Version 32+: has SERVER_TENTACLE and all features
            // All features enabled
        }
    }

    features
}

#[test]
#[ignore] // Requires external ceph-object-corpus
fn test_pg_pool_t_decode_encode_roundtrip() {
    let corpus_dir = get_corpus_dir();
    let test_dir = corpus_dir
        .join("archive")
        .join(CORPUS_VERSION)
        .join("objects")
        .join("pg_pool_t");

    if !test_dir.exists() {
        panic!(
            "Corpus directory not found: {}\nPlease ensure ceph-object-corpus is available.",
            test_dir.display()
        );
    }

    let mut success_count = 0;
    let mut total_count = 0;
    let mut mismatch_count = 0;

    for entry in fs::read_dir(test_dir).expect("Failed to read test directory") {
        let entry = entry.expect("Failed to read directory entry");
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        total_count += 1;
        let filename = path.file_name().unwrap().to_string_lossy();

        println!("Testing pg_pool_t file: {}", filename);

        // Read original data
        let original_data = fs::read(&path).expect("Failed to read test file");
        
        // Extract version from the original data (first byte of versioned encoding)
        if original_data.len() < 6 {
            println!("  ✗ File too small for versioned encoding");
            continue;
        }
        let original_version = original_data[0];
        let original_compat = original_data[1];
        
        println!("  Original encoding: version={}, compat={}", original_version, original_compat);
        
        let mut bytes = bytes::Bytes::from(original_data.clone());

        // Try to decode
        match PgPool::decode(&mut bytes, 0) {
            Ok(pg_pool) => {
                println!(
                    "  Decoded successfully: type={}, size={}, pg_num={}, pgp_num={}",
                    pg_pool.pool_type, pg_pool.size, pg_pool.pg_num, pg_pool.pgp_num
                );

                // Determine features to use for encoding based on the original version
                let encode_features = features_for_version(original_version, pg_pool.is_stretch_pool());
                println!("  Using features: 0x{:x} for re-encoding", encode_features);

                // Try to encode back with the same features
                let mut encoded_buf = BytesMut::new();
                match pg_pool.encode(&mut encoded_buf, encode_features) {
                    Ok(()) => {
                        let encoded_bytes = encoded_buf.freeze();

                        if encoded_bytes.as_ref() == original_data.as_slice() {
                            println!("  ✓ Perfect roundtrip");
                            success_count += 1;
                        } else {
                            println!("  ✗ Roundtrip mismatch");
                            mismatch_count += 1;
                            println!(
                                "    Original len: {}, Encoded len: {}",
                                original_data.len(),
                                encoded_bytes.len()
                            );

                            // Show first few bytes for debugging
                            let orig_hex = hex::encode(
                                &original_data[..std::cmp::min(32, original_data.len())],
                            );
                            let enc_hex = hex::encode(
                                &encoded_bytes[..std::cmp::min(32, encoded_bytes.len())],
                            );
                            println!("    Original: {}", orig_hex);
                            println!("    Encoded:  {}", enc_hex);
                        }
                    }
                    Err(e) => {
                        println!("  ✗ Failed to encode: {}", e);
                        mismatch_count += 1;
                    }
                }
            }
            Err(e) => {
                println!("  ✗ Failed to decode: {}", e);

                // Show first few bytes for debugging decode failures
                let hex_data =
                    hex::encode(&original_data[..std::cmp::min(16, original_data.len())]);
                println!("    Data: {}", hex_data);
            }
        }

        println!();
    }

    println!(
        "pg_pool_t Results: {}/{} files processed successfully",
        success_count, total_count
    );
    
    if mismatch_count > 0 {
        println!("⚠ {} files had roundtrip mismatches", mismatch_count);
    }

    // Assert that all files roundtrip correctly
    assert_eq!(
        mismatch_count, 0,
        "Roundtrip test failed: {} files had mismatches",
        mismatch_count
    );
    
    // We should have processed at least some files
    assert!(
        total_count > 0,
        "No test files were found in the corpus directory"
    );
}
