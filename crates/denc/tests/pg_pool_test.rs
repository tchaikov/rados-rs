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
    // Build features progressively based on version
    // Start with base features that don't affect version selection
    let mut features = CEPH_FEATUREMASK_PGID64
        | CEPH_FEATUREMASK_PGPOOL3
        | CEPH_FEATUREMASK_OSDENC
        | CEPH_FEATUREMASK_OSD_POOLRESEND
        | CEPH_FEATUREMASK_MSG_ADDR2;

    // Add features based on version
    // The encoding_version logic checks these features in order:
    // SERVER_TENTACLE, NEW_OSDOP_ENCODING, SERVER_LUMINOUS, SERVER_MIMIC, SERVER_NAUTILUS
    match version {
        ..=21 => {
            // Version 21 or below: no NEW_OSDOP_ENCODING
            // Keep only base features
        }
        22..=24 => {
            // Version 24: has NEW_OSDOP_ENCODING, no SERVER_LUMINOUS
            features |= CEPH_FEATUREMASK_NEW_OSDOP_ENCODING;
        }
        25..=26 => {
            // Version 26: has SERVER_LUMINOUS, no SERVER_MIMIC
            features |= CEPH_FEATUREMASK_NEW_OSDOP_ENCODING;
            features |= CEPH_FEATUREMASK_SERVER_LUMINOUS;
        }
        27..=28 => {
            // Version 27: has SERVER_MIMIC, no SERVER_NAUTILUS
            features |= CEPH_FEATUREMASK_NEW_OSDOP_ENCODING;
            features |= CEPH_FEATUREMASK_SERVER_LUMINOUS;
            features |= CEPH_FEATUREMASK_SERVER_MIMIC;
        }
        29..=30 => {
            // Version 29-30: has SERVER_NAUTILUS, no SERVER_TENTACLE
            // Also include OCTOPUS and SQUID since they're in SIGNIFICANT_FEATURES
            // but don't affect version selection
            features |= CEPH_FEATUREMASK_NEW_OSDOP_ENCODING;
            features |= CEPH_FEATUREMASK_SERVER_LUMINOUS;
            features |= CEPH_FEATUREMASK_SERVER_MIMIC;
            features |= CEPH_FEATUREMASK_SERVER_NAUTILUS;
            features |= CEPH_FEATUREMASK_SERVER_OCTOPUS;
            features |= CEPH_FEATUREMASK_SERVER_SQUID;
        }
        _ => {
            // Version 31+: has SERVER_TENTACLE and all features
            features = SIGNIFICANT_FEATURES;
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

        println!(
            "  Original encoding: version={}, compat={}",
            original_version, original_compat
        );

        let mut bytes = bytes::Bytes::from(original_data.clone());

        // Try to decode
        match PgPool::decode(&mut bytes, 0) {
            Ok(pg_pool) => {
                println!(
                    "  Decoded successfully: type={}, size={}, pg_num={}, pgp_num={}",
                    pg_pool.pool_type, pg_pool.size, pg_pool.pg_num, pg_pool.pgp_num
                );

                // Determine features to use for encoding based on the original version
                let encode_features =
                    features_for_version(original_version, pg_pool.is_stretch_pool());
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

#[test]
fn test_features_for_version() {
    use denc::VersionedEncode;
    use osdclient::PgPool;

    // Test that features_for_version produces the expected version
    // when used with PgPool::encoding_version

    // Version 21: no NEW_OSDOP_ENCODING
    let features_21 = features_for_version(21, false);
    let pool = PgPool::new();
    assert_eq!(
        pool.encoding_version(features_21),
        21,
        "Features for v21 should produce v21, got features=0x{:x}",
        features_21
    );

    // Version 24: has NEW_OSDOP_ENCODING, no SERVER_LUMINOUS
    let features_24 = features_for_version(24, false);
    assert_eq!(
        pool.encoding_version(features_24),
        24,
        "Features for v24 should produce v24, got features=0x{:x}",
        features_24
    );

    // Version 26: has SERVER_LUMINOUS, no SERVER_MIMIC
    let features_26 = features_for_version(26, false);
    assert_eq!(
        pool.encoding_version(features_26),
        26,
        "Features for v26 should produce v26, got features=0x{:x}",
        features_26
    );

    // Version 27: has SERVER_MIMIC, no SERVER_NAUTILUS
    let features_27 = features_for_version(27, false);
    assert_eq!(
        pool.encoding_version(features_27),
        27,
        "Features for v27 should produce v27, got features=0x{:x}",
        features_27
    );

    // Version 29: has SERVER_NAUTILUS, no SERVER_TENTACLE, not stretch
    let features_29 = features_for_version(29, false);
    assert_eq!(
        pool.encoding_version(features_29),
        29,
        "Features for v29 should produce v29, got features=0x{:x}",
        features_29
    );

    // Version 32: all features
    let features_32 = features_for_version(32, false);
    assert_eq!(
        pool.encoding_version(features_32),
        32,
        "Features for v32 should produce v32, got features=0x{:x}",
        features_32
    );
}
