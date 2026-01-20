//! Test to verify MonMap corpus decoding
//!
//! This test verifies that all three MonMap corpus files can be properly decoded.

use bytes::Bytes;
use denc::{Denc, MonMap};
use std::fs;
use std::path::PathBuf;

#[test]
#[ignore] // Requires corpus to be available
fn test_monmap_corpus_decoding() {
    let corpus_dir = PathBuf::from("/tmp/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/MonMap");
    
    if !corpus_dir.exists() {
        eprintln!("Corpus not available at {:?}, skipping test", corpus_dir);
        return;
    }
    
    let files = fs::read_dir(&corpus_dir)
        .expect("Failed to read corpus directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .collect::<Vec<_>>();
    
    assert_eq!(files.len(), 3, "Expected 3 MonMap corpus files");
    
    let mut success_count = 0;
    
    for entry in files {
        let file_path = entry.path();
        let file_name = file_path.file_name().unwrap().to_string_lossy();
        
        println!("Testing {}", file_name);
        
        let data = fs::read(&file_path).expect("Failed to read corpus file");
        let mut bytes = Bytes::from(data);
        let original_len = bytes.len();
        
        // Decode with all features enabled
        match MonMap::decode(&mut bytes, u64::MAX) {
            Ok(monmap) => {
                let consumed = original_len - bytes.len();
                println!("  ✓ Decoded successfully: {} bytes consumed, {} bytes remaining", consumed, bytes.len());
                println!("    Epoch: {}", monmap.epoch);
                println!("    Monitors: {}", monmap.mon_info.len());
                
                // Verify all bytes were consumed
                assert_eq!(bytes.len(), 0, "Expected 0 bytes remaining for {}", file_name);
                
                success_count += 1;
            }
            Err(e) => {
                panic!("Failed to decode {}: {:?}", file_name, e);
            }
        }
    }
    
    assert_eq!(success_count, 3, "All 3 MonMap corpus files should decode successfully");
    println!("\n✓ All {} MonMap corpus files decoded successfully!", success_count);
}
