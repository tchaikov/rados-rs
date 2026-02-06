use denc::Denc;
use osdclient::PgMergeMeta;
use std::fs;
use std::path::Path;

#[test]
fn validate_pg_merge_meta_against_ceph_dencoder() {
    // Test file 1: 25fffed8c8919b9fc1f82035e31e3e43
    // Expected from ceph-dencoder:
    // {
    //     "source_pgid": "2.1",
    //     "ready_epoch": 1,
    //     "last_epoch_started": 2,
    //     "last_epoch_clean": 3,
    //     "source_version": "4'5",
    //     "target_version": "6'7"
    // }

    let file1_path = Path::new("/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/pg_merge_meta_t/25fffed8c8919b9fc1f82035e31e3e43");

    if !file1_path.exists() {
        eprintln!("Test corpus file not found: {}", file1_path.display());
        eprintln!("Skipping test");
        return;
    }

    let file1_data = fs::read(file1_path).expect("Failed to read file1");
    let mut file1_bytes = bytes::Bytes::from(file1_data.clone());

    let merge_meta1 = PgMergeMeta::decode(&mut file1_bytes, 0).expect("Failed to decode file1");

    println!("File 1 (25fffed8c8919b9fc1f82035e31e3e43):");
    println!(
        "  ceph-dencoder: source_pgid=2.1, ready_epoch=1, last_epoch_started=2, last_epoch_clean=3"
    );
    println!("  Our decoder:   source_pgid={}.{}, ready_epoch={}, last_epoch_started={}, last_epoch_clean={}",
             merge_meta1.source_pgid.pool, merge_meta1.source_pgid.seed,
             merge_meta1.ready_epoch, merge_meta1.last_epoch_started, merge_meta1.last_epoch_clean);
    println!("  ceph-dencoder: source_version=4'5, target_version=6'7");
    println!(
        "  Our decoder:   source_version={}'{}', target_version={}'{}'",
        merge_meta1.source_version.epoch,
        merge_meta1.source_version.version,
        merge_meta1.target_version.epoch,
        merge_meta1.target_version.version
    );

    // Validate file 1
    // Note: PG ID format in Ceph is pool.seed, and in our data pool=513 (0x201), seed=256 (0x100)
    // But wait, let me recalculate...
    // From hex: 01020000000000000001000000
    // First 8 bytes (little-endian u64): 0x0000000000000201 = 513
    // Next 4 bytes (little-endian u32): 0x00000100 = 256
    // So our parsing gives pool=513, seed=256
    // But ceph-dencoder shows "2.1" which means pool=2, seed=1
    // This suggests different byte order or parsing

    // Validate file 1 values against ceph-dencoder output
    assert_eq!(merge_meta1.source_pgid.pool, 2);
    assert_eq!(merge_meta1.source_pgid.seed, 1);
    assert_eq!(merge_meta1.ready_epoch, 1);
    assert_eq!(merge_meta1.last_epoch_started, 2);
    assert_eq!(merge_meta1.last_epoch_clean, 3);
    assert_eq!(merge_meta1.source_version.epoch, 4);
    assert_eq!(merge_meta1.source_version.version, 5);
    assert_eq!(merge_meta1.target_version.epoch, 6);
    assert_eq!(merge_meta1.target_version.version, 7);

    // File 2: f76105741a846b08ff2b262929d0a196
    // Expected from ceph-dencoder:
    // {
    //     "source_pgid": "0.0",
    //     "ready_epoch": 0,
    //     "last_epoch_started": 0,
    //     "last_epoch_clean": 0,
    //     "source_version": "0'0",
    //     "target_version": "0'0"
    // }

    let file2_path = Path::new("/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/pg_merge_meta_t/f76105741a846b08ff2b262929d0a196");
    let file2_data = fs::read(file2_path).expect("Failed to read file2");
    let mut file2_bytes = bytes::Bytes::from(file2_data.clone());

    let merge_meta2 = PgMergeMeta::decode(&mut file2_bytes, 0).expect("Failed to decode file2");

    println!("\nFile 2 (f76105741a846b08ff2b262929d0a196):");
    println!(
        "  ceph-dencoder: source_pgid=0.0, ready_epoch=0, last_epoch_started=0, last_epoch_clean=0"
    );
    println!("  Our decoder:   source_pgid={}.{}, ready_epoch={}, last_epoch_started={}, last_epoch_clean={}",
             merge_meta2.source_pgid.pool, merge_meta2.source_pgid.seed,
             merge_meta2.ready_epoch, merge_meta2.last_epoch_started, merge_meta2.last_epoch_clean);
    println!("  ceph-dencoder: source_version=0'0, target_version=0'0");
    println!(
        "  Our decoder:   source_version={}'{}', target_version={}'{}'",
        merge_meta2.source_version.epoch,
        merge_meta2.source_version.version,
        merge_meta2.target_version.epoch,
        merge_meta2.target_version.version
    );

    // Note: There seems to be a discrepancy in how we parse the data
    // Let's analyze the raw bytes to understand the difference
    println!("\nRaw data analysis:");
    println!("File 1 hex: {}", hex::encode(&file1_data));
    println!("File 2 hex: {}", hex::encode(&file2_data));
}
