use denc::Denc;
use osdclient::PgMergeMeta;
use std::fs;
use std::path::Path;

#[test]
fn test_pg_merge_meta_decode_encode_roundtrip() {
    let test_dir = Path::new("/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/pg_merge_meta_t");

    if !test_dir.exists() {
        eprintln!("Test corpus directory not found: {}", test_dir.display());
        eprintln!("Skipping test");
        return;
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

        println!("Testing pg_merge_meta_t file: {}", filename);

        // Read original data
        let original_data = fs::read(&path).expect("Failed to read test file");
        
        // Extract version from the original data (first byte of versioned encoding)
        if original_data.len() < 6 {
            println!("  ✗ File too small for versioned encoding");
            continue;
        }
        let original_version = original_data[0];
        let original_compat = original_data[1];

        println!("  File size: {} bytes", original_data.len());
        println!("  Original encoding: version={}, compat={}", original_version, original_compat);
        println!(
            "  Data: {}",
            hex::encode(&original_data[..std::cmp::min(32, original_data.len())])
        );
        
        let mut bytes = bytes::Bytes::from(original_data.clone());

        // Try to decode
        match PgMergeMeta::decode(&mut bytes, 0) {
            Ok(merge_meta) => {
                println!("  ✓ Decoded successfully:");
                println!(
                    "    source_pgid: pool={}, seed={}",
                    merge_meta.source_pgid.pool, merge_meta.source_pgid.seed
                );
                println!("    ready_epoch: {}", merge_meta.ready_epoch);
                println!("    last_epoch_started: {}", merge_meta.last_epoch_started);
                println!("    last_epoch_clean: {}", merge_meta.last_epoch_clean);
                println!(
                    "    source_version: epoch={}, version={}",
                    merge_meta.source_version.epoch, merge_meta.source_version.version
                );
                println!(
                    "    target_version: epoch={}, version={}",
                    merge_meta.target_version.epoch, merge_meta.target_version.version
                );

                // Try to encode back (PgMergeMeta always uses version 1, so features don't matter)
                let mut encoded_buf = bytes::BytesMut::new();
                match merge_meta.encode(&mut encoded_buf, 0) {
                    Ok(()) => {
                        let encoded_bytes = encoded_buf.to_vec();

                        println!("  Original bytes: {}", hex::encode(&original_data));
                        println!("  Encoded bytes:  {}", hex::encode(&encoded_bytes));

                        if encoded_bytes == original_data {
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
            }
        }

        println!();
    }

    println!(
        "pg_merge_meta_t Results: {}/{} files processed successfully",
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
    if total_count > 0 {
        assert!(
            success_count > 0,
            "No test files roundtripped successfully"
        );
    }
}
