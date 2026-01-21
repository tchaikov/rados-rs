use bytes::{Buf, Bytes};
use crush::CrushMap;
use std::fs;

fn main() {
    let data = fs::read("/tmp/crushmap.bin").expect("Failed to read CRUSH map");
    println!("Total bytes: {}", data.len());

    let mut bytes = Bytes::from(data);

    match CrushMap::decode(&mut bytes) {
        Ok(map) => {
            println!("Successfully decoded CRUSH map!");
            println!("  max_buckets: {}", map.max_buckets);
            println!("  max_rules: {}", map.max_rules);
            println!("  max_devices: {}", map.max_devices);
            println!(
                "  buckets: {}",
                map.buckets.iter().filter(|b| b.is_some()).count()
            );
            println!(
                "  rules: {}",
                map.rules.iter().filter(|r| r.is_some()).count()
            );
            println!("  Remaining bytes: {}", bytes.remaining());
        }
        Err(e) => {
            println!("Failed to decode: {:?}", e);
            println!("Remaining bytes: {}", bytes.remaining());
        }
    }
}
