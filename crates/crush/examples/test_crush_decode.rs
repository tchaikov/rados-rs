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

            // Display device class information
            if !map.class_name.is_empty() {
                println!("\nDevice Classes:");
                for (class_id, class_name) in &map.class_name {
                    println!("  Class {}: {}", class_id, class_name);

                    // Count devices in this class
                    let device_count = map
                        .class_map
                        .iter()
                        .filter(|(_, &cid)| cid == *class_id)
                        .count();
                    println!("    Devices: {}", device_count);
                }
            }

            // Display some device class assignments
            if !map.class_map.is_empty() {
                println!("\nSample Device Class Assignments:");
                for (device_id, class_id) in map.class_map.iter().take(10) {
                    if let Some(class_name) = map.class_name.get(class_id) {
                        println!("  OSD {}: {}", device_id, class_name);
                    }
                }
            }

            println!("\n  Remaining bytes: {}", bytes.remaining());
        }
        Err(e) => {
            println!("Failed to decode: {:?}", e);
            println!("Remaining bytes: {}", bytes.remaining());
        }
    }
}
