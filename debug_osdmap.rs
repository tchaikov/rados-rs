use bytes::Bytes;
use denc::{Denc, OSDMap};
use std::fs;

fn main() {
    let corpus_path = std::path::PathBuf::from(std::env::var("HOME").unwrap())
        .join("dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/OSDMap/303e0d4679afb7b809fd924c7825eecd");

    let data = fs::read(&corpus_path).expect("Failed to read corpus file");
    let mut bytes = Bytes::from(data);

    println!("Total file size: {} bytes", bytes.len());
    println!("First 64 bytes:");
    for (i, chunk) in bytes[..64.min(bytes.len())].chunks(16).enumerate() {
        print!("{:04x}: ", i * 16);
        for b in chunk {
            print!("{:02x} ", b);
        }
        println!();
    }

    match OSDMap::decode(&mut bytes) {
        Ok(osdmap) => {
            println!("\nSuccessfully decoded OSDMap!");
            println!("  Epoch: {}", osdmap.epoch);
            println!("  Max OSD: {}", osdmap.max_osd);
            println!("  Remaining bytes: {}", bytes.len());
        }
        Err(e) => {
            eprintln!("\nFailed to decode OSDMap: {:?}", e);
            eprintln!("Remaining bytes: {}", bytes.len());
        }
    }
}
