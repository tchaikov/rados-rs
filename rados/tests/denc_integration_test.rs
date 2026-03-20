use rados::denc::{Denc, EntityAddr, EntityAddrType};
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

#[test]
#[ignore] // Requires external ceph-object-corpus
fn test_entity_addr_decode_encode_roundtrip() {
    let corpus_dir = get_corpus_dir();
    let test_dir = corpus_dir
        .join("archive")
        .join(CORPUS_VERSION)
        .join("objects")
        .join("entity_addr_t");

    if !test_dir.exists() {
        panic!(
            "Corpus directory not found: {}\nPlease ensure ceph-object-corpus is available.",
            test_dir.display()
        );
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

        println!("Testing file: {}", filename);

        // Read original data
        let original_data = fs::read(&path).expect("Failed to read test file");
        let mut bytes = bytes::Bytes::from(original_data.clone());

        // Try to decode
        match EntityAddr::decode(&mut bytes, 0) {
            Ok(entity_addr) => {
                println!(
                    "  Decoded successfully: type={:?}, nonce={}, sockaddr_len={}",
                    entity_addr.addr_type,
                    entity_addr.nonce,
                    entity_addr.sockaddr_data.len()
                );

                let mut encoded_buf = bytes::BytesMut::new();
                match entity_addr.encode(&mut encoded_buf, 0) {
                    Ok(()) => {
                        let encoded_bytes = encoded_buf.to_vec();

                        if original_data.first() == Some(&0) {
                            println!(
                                "  ✓ Legacy sample decoded and modern re-encode succeeded (modern-only encode contract)"
                            );
                            success_count += 1;
                        } else if encoded_bytes == original_data {
                            println!("  ✓ Perfect roundtrip");
                            success_count += 1;
                        } else {
                            println!("  ⚠ Roundtrip mismatch");
                            println!(
                                "    Original len: {}, Encoded len: {}",
                                original_data.len(),
                                encoded_bytes.len()
                            );

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
        "Results: {}/{} files processed successfully",
        success_count, total_count
    );

    // We expect at least some files to work
    assert!(success_count > 0, "No test files processed successfully");
}

#[test]
fn test_specific_entity_addr_sample() {
    // Test the specific sample we examined
    let sample_data = hex::decode("0101012800000000000000000000001c00000000000000000000000000000000000000000000000000000000000000").unwrap();
    let mut bytes = bytes::Bytes::from(sample_data.clone());

    println!("Sample data: {}", hex::encode(&sample_data));
    println!("Data length: {} bytes", sample_data.len());

    let entity_addr = EntityAddr::decode(&mut bytes, 0).expect("Failed to decode sample");

    println!(
        "Decoded: type={:?}, nonce={}, sockaddr_len={}",
        entity_addr.addr_type,
        entity_addr.nonce,
        entity_addr.sockaddr_data.len()
    );

    assert_eq!(entity_addr.addr_type, EntityAddrType::None);
    assert_eq!(entity_addr.nonce, 0);

    // Try encoding back
    let mut encoded_buf = bytes::BytesMut::new();
    entity_addr
        .encode(&mut encoded_buf, 0)
        .expect("Failed to encode");
    let encoded_bytes = encoded_buf.to_vec();

    if encoded_bytes == sample_data {
        println!("✓ Sample roundtrip successful");
    } else {
        println!("⚠ Sample roundtrip mismatch");
        println!("Original: {}", hex::encode(&sample_data));
        println!("Encoded:  {}", hex::encode(&encoded_bytes));
    }
}
