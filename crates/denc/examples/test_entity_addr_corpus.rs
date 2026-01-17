use bytes::Bytes;
use denc::{Denc, EntityAddr};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing EntityAddr decoding with corpus data");

    // Test 1: Simple entity_addr_t (type=none, nonce=0)
    let corpus1_path = "/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/entity_addr_t/f7711e9b632c102296d9b6f95e6abefd";
    let data1 = fs::read(corpus1_path)?;

    println!("Test 1: Simple entity_addr_t");
    println!("  Raw data: {} bytes", data1.len());
    println!("  Hex: {:02x?}", &data1[..std::cmp::min(16, data1.len())]);

    let mut cursor1 = Bytes::from(data1);
    match EntityAddr::decode(&mut cursor1) {
        Ok(addr) => {
            println!("  ✓ Decoded successfully: {:?}", addr);
            println!(
                "    addr_type: {:?}, nonce: {}, sockaddr_len: {}",
                addr.addr_type,
                addr.nonce,
                addr.sockaddr_data.len()
            );
        }
        Err(e) => {
            println!("  ✗ Decode failed: {}", e);
        }
    }

    // Test 2: Real IP entity_addr_t (type=any, has IP address)
    let corpus2_path = "/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/entity_addr_t/fbd20109db61ed93d0afbdfba359fb42";
    let data2 = fs::read(corpus2_path)?;

    println!("\nTest 2: IP entity_addr_t");
    println!("  Raw data: {} bytes", data2.len());
    println!("  Hex: {:02x?}", &data2[..std::cmp::min(16, data2.len())]);

    let mut cursor2 = Bytes::from(data2);
    match EntityAddr::decode(&mut cursor2) {
        Ok(addr) => {
            println!("  ✓ Decoded successfully: {:?}", addr);
            println!(
                "    addr_type: {:?}, nonce: {}, sockaddr_len: {}",
                addr.addr_type,
                addr.nonce,
                addr.sockaddr_data.len()
            );
            if !addr.sockaddr_data.is_empty() {
                println!(
                    "    sockaddr_data: {:02x?}",
                    &addr.sockaddr_data[..std::cmp::min(16, addr.sockaddr_data.len())]
                );
            }
        }
        Err(e) => {
            println!("  ✗ Decode failed: {}", e);
        }
    }

    // Test 3: Test encoding our default EntityAddr
    println!("\nTest 3: Encoding default EntityAddr");
    let default_addr = EntityAddr::default();
    println!("  Default addr: {:?}", default_addr);

    match default_addr.encode(0) {
        Ok(encoded) => {
            println!("  ✓ Encoded to {} bytes", encoded.len());
            println!(
                "  Hex: {:02x?}",
                &encoded[..std::cmp::min(16, encoded.len())]
            );

            // Try to decode our own encoding
            let mut test_cursor = encoded.clone();
            match EntityAddr::decode(&mut test_cursor) {
                Ok(decoded) => {
                    println!("  ✓ Round-trip successful: {:?}", decoded);
                }
                Err(e) => {
                    println!("  ✗ Round-trip failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("  ✗ Encode failed: {}", e);
        }
    }

    Ok(())
}
