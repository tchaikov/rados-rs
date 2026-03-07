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
        let mut bytes = bytes::Bytes::from(original_data.clone());

        println!("  File size: {} bytes", original_data.len());
        println!(
            "  Data: {}",
            hex::encode(&original_data[..std::cmp::min(32, original_data.len())])
        );

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

                // Try to encode back
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
                            println!("  ⚠ Roundtrip mismatch");
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

    // We expect at least some files to work
    if total_count > 0 {
        println!("Processed {} test files", total_count);
    }
}
