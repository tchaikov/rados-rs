use denc::{Denc, EntityAddr};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing EntityAddr encoding");

    // Test default EntityAddr
    let default_addr = EntityAddr::default();
    println!("Default EntityAddr: {:?}", default_addr);

    // Encode with different features
    let encoded_no_features = default_addr.encode(0)?;
    println!("Encoded (no features): {} bytes", encoded_no_features.len());
    println!("  Hex: {:02x?}", &encoded_no_features[..encoded_no_features.len().min(32)]);

    let encoded_with_addr2 = default_addr.encode(denc::CEPH_FEATURE_MSG_ADDR2)?;
    println!("Encoded (MSG_ADDR2): {} bytes", encoded_with_addr2.len());
    println!("  Hex: {:02x?}", &encoded_with_addr2[..encoded_with_addr2.len().min(32)]);

    // Test what the official client expects - let's decode some corpus data
    if std::path::Path::new("/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/entity_addr_t").exists() {
        // Read first corpus file
        let corpus_files = std::fs::read_dir("/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/entity_addr_t")?;
        for entry in corpus_files.take(2) {
            let entry = entry?;
            let data = std::fs::read(entry.path())?;
            println!("\nCorpus file: {:?}", entry.file_name());
            println!("  Size: {} bytes", data.len());
            println!("  Hex: {:02x?}", &data[..data.len().min(32)]);

            // Try to decode
            let mut cursor = bytes::Bytes::from(data);
            match EntityAddr::decode(&mut cursor) {
                Ok(addr) => println!("  Decoded: {:?}", addr),
                Err(e) => println!("  Decode error: {}", e),
            }
        }
    }

    Ok(())
}