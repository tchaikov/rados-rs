use bytes::Bytes;
use denc::VersionedEncode;
use osdclient::OSDMap;
use std::fs;

fn main() {
    let corpus_path = "/tmp/osdmap-epoch-12.bin";

    let data = fs::read(corpus_path).expect("Failed to read corpus file");
    println!("Read {} bytes from corpus file", data.len());

    // Show first 32 bytes
    println!("First 32 bytes:");
    for (i, chunk) in data.chunks(16).take(2).enumerate() {
        print!("{:04x}: ", i * 16);
        for byte in chunk {
            print!("{:02x} ", byte);
        }
        println!();
    }

    let mut bytes = Bytes::from(data);
    println!("\nAttempting to decode OSDMap...");

    match OSDMap::decode_versioned(&mut bytes, 0) {
        Ok(map) => {
            println!("Successfully decoded OSDMap!");
            println!("  fsid: {:?}", map.fsid);
            println!("  epoch: {}", map.epoch);
            println!("  max_osd: {}", map.max_osd);
            println!("  pools: {} pools", map.pools.len());
            println!("  Remaining bytes: {}", bytes.len());
        }
        Err(e) => {
            println!("Failed to decode: {}", e);
            println!("Remaining bytes: {}", bytes.len());
        }
    }
}
