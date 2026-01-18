use bytes::Bytes;
use denc::{Denc, OSDMap};
use std::fs;
use std::path::PathBuf;

#[test]
fn test_osdmap_decode() {
    // Path to the OSDMap corpus file
    let corpus_path = PathBuf::from(env!("HOME"))
        .join("dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/OSDMap/303e0d4679afb7b809fd924c7825eecd");

    if !corpus_path.exists() {
        eprintln!("Corpus file not found: {:?}", corpus_path);
        eprintln!("Skipping test");
        return;
    }

    // Read the corpus file
    let data = fs::read(&corpus_path).expect("Failed to read corpus file");
    let mut bytes = Bytes::from(data);

    println!("Corpus file size: {} bytes", bytes.len());
    println!("First 32 bytes: {:02x?}", &bytes[..32.min(bytes.len())]);

    // Decode the OSDMap
    match OSDMap::decode_versioned(&mut bytes, 0) {
        Ok(osdmap) => {
            println!("Successfully decoded OSDMap!");
            println!("  Epoch: {}", osdmap.epoch);
            println!("  FSID: {:?}", osdmap.fsid);
            println!("  Max OSD: {}", osdmap.max_osd);
            println!("  Pool count: {}", osdmap.pools.len());
            println!("  Flags: 0x{:x}", osdmap.flags);
            println!("  Remaining bytes: {}", bytes.len());

            // Verify against expected values from C++ dencoder output
            assert_eq!(osdmap.epoch, 0, "Epoch should be 0");
            assert_eq!(osdmap.max_osd, 0, "Max OSD should be 0");
            assert_eq!(osdmap.pools.len(), 0, "Should have 0 pools");
            assert_eq!(osdmap.flags, 0, "Flags should be 0");
        }
        Err(e) => {
            panic!("Failed to decode OSDMap: {:?}", e);
        }
    }
}

#[test]
fn test_all_osdmap_corpus_files() {
    let corpus_dir = PathBuf::from(env!("HOME"))
        .join("dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/OSDMap");

    if !corpus_dir.exists() {
        eprintln!("Corpus directory not found: {:?}", corpus_dir);
        eprintln!("Skipping test");
        return;
    }

    let entries = fs::read_dir(&corpus_dir).expect("Failed to read corpus directory");

    let mut success_count = 0;
    let mut failure_count = 0;

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
                failure_count += 1;
                continue;
            }
        };

        let mut bytes = Bytes::from(data);
        let original_len = bytes.len();

        match OSDMap::decode_versioned(&mut bytes, 0) {
            Ok(osdmap) => {
                println!(
                    "  ✓ Success! Epoch: {}, Max OSD: {}, Pools: {}, Remaining: {} bytes",
                    osdmap.epoch,
                    osdmap.max_osd,
                    osdmap.pools.len(),
                    bytes.len()
                );
                success_count += 1;
            }
            Err(e) => {
                eprintln!("  ✗ Failed: {:?}", e);
                eprintln!("    File size: {} bytes", original_len);
                failure_count += 1;
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Success: {}", success_count);
    println!("Failure: {}", failure_count);
    println!("Total: {}", success_count + failure_count);

    // We expect at least some files to decode successfully
    assert!(
        success_count > 0,
        "At least one corpus file should decode successfully"
    );
}
