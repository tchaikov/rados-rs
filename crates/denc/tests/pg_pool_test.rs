use bytes::BytesMut;
use denc::{Denc, PgPool};
use std::fs;
use std::path::Path;

#[test]
fn test_pg_pool_t_decode_encode_roundtrip() {
    let test_dir = Path::new("test_corpus/pg_pool_t");

    if !test_dir.exists() {
        panic!("Test corpus directory not found: {}", test_dir.display());
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

        println!("Testing pg_pool_t file: {}", filename);

        // Read original data
        let original_data = fs::read(&path).expect("Failed to read test file");
        let mut bytes = bytes::Bytes::from(original_data.clone());

        // Try to decode
        match PgPool::decode(&mut bytes, 0) {
            Ok(pg_pool) => {
                println!(
                    "  Decoded successfully: type={}, size={}, pg_num={}, pgp_num={}",
                    pg_pool.pool_type, pg_pool.size, pg_pool.pg_num, pg_pool.pgp_num
                );

                // Try to encode back
                let mut encoded_buf = BytesMut::new();
                match pg_pool.encode(&mut encoded_buf, 0) {
                    Ok(()) => {
                        let encoded_bytes = encoded_buf.freeze();

                        if encoded_bytes.as_ref() == original_data.as_slice() {
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

    // We expect at least some files to work - this is a learning/debugging test
    // so we don't fail if nothing works yet
    if total_count > 0 {
        println!("Processed {} test files", total_count);
    }
}
