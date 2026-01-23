//! Debug test to understand encoding issues with different object name lengths
//!
//! This test creates MOSDOp messages with different object name lengths and
//! inspects the encoded bytes to understand why certain lengths fail.

use osdclient::messages::MOSDOp;
use osdclient::types::{OSDOp, ObjectId, RequestId, StripedPgId};

#[test]
fn test_encoding_different_name_lengths() {
    // Test different object name lengths to find the pattern
    let test_cases = vec![
        ("test_xyz", 8, "8 chars - works"),
        ("test_12char", 11, "11 chars - works"),
        ("test_13chars", 12, "12 chars - FAILS"),
        ("test_14chars!", 13, "13 chars - works"),
        ("test_15chars!!", 14, "14 chars - FAILS"),
        ("test_16chars!!!", 15, "15 chars - FAILS"),
    ];

    for (name, expected_len, description) in test_cases {
        assert_eq!(name.len(), expected_len, "{}", description);

        let mut object = ObjectId::new(2, name);
        object.calculate_hash();

        let pgid = StripedPgId::from_pg(2, 0x1a);

        let ops = vec![OSDOp::write_full(bytes::Bytes::from("test"))];

        let reqid = RequestId {
            entity_name: "client.admin".to_string(),
            tid: 1,
            inc: 1,
        };

        let msg = MOSDOp::new(0, 1, 0, object, pgid, ops, reqid);

        let encoded = msg.encode().expect("Failed to encode");
        let data_section = msg.get_data_section();

        println!("\n{} ({})", description, name);
        println!("  Object name length: {}", name.len());
        println!("  Total encoded size: {} bytes", encoded.len());
        println!("  Data section size: {} bytes", data_section.len());

        // Let's inspect the bytes around the object name to see if there's a pattern
        // The object name should appear after the ObjectLocator
        let name_bytes = name.as_bytes();
        if let Some(pos) = find_subsequence(&encoded, name_bytes) {
            println!("  Object name found at byte offset: {}", pos);
            // Print 20 bytes before the name
            if pos >= 20 {
                print!("  20 bytes before name: ");
                for i in (pos - 20)..pos {
                    print!("{:02x} ", encoded[i]);
                }
                println!();
            }
            // The length field should be 4 bytes before the name
            if pos >= 4 {
                let len_offset = pos - 4;
                let stored_len = u32::from_le_bytes([
                    encoded[len_offset],
                    encoded[len_offset + 1],
                    encoded[len_offset + 2],
                    encoded[len_offset + 3],
                ]);
                println!(
                    "  Length field at offset {}: {} (expected {})",
                    len_offset,
                    stored_len,
                    name.len()
                );
            }
        } else {
            println!("  ⚠️  Object name not found in encoded bytes!");
        }

        // Let's also check for the ObjectLocator to understand its size
        // ObjectLocator should encode: pool_id (i64), preferred (i32), key (string), namespace (string), hash (i64)
        // With versioning: version (u8), compat (u8), length (u32), then content
    }
}

/// Helper function to find a subsequence in a slice
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[test]
fn test_objectlocator_encoding_size() {
    use bytes::BytesMut;
    use denc::denc::Denc;

    // Create an ObjectLocator like in MOSDOp encoding
    let locator = crush::ObjectLocator {
        pool_id: 2,
        key: String::new(),
        namespace: String::new(),
        hash: -1,
    };

    let mut buf = BytesMut::new();
    locator.encode(&mut buf, 0).expect("Failed to encode");

    println!("\nObjectLocator encoding:");
    println!("  Total size: {} bytes", buf.len());
    println!("  Hex dump:");
    for (i, byte) in buf.iter().enumerate() {
        if i % 16 == 0 {
            if i > 0 {
                println!();
            }
            print!("    {:04x}: ", i);
        }
        print!("{:02x} ", byte);
    }
    println!();

    // Expected structure with versioning:
    // version (1 byte): 0x06
    // compat (1 byte): 0x03
    // length (4 bytes): size of content
    // pool_id (8 bytes): 0x02 00 00 00 00 00 00 00
    // preferred (4 bytes): 0xff ff ff ff
    // key length (4 bytes): 0x00 00 00 00
    // namespace length (4 bytes): 0x00 00 00 00
    // hash (8 bytes): 0xff ff ff ff ff ff ff ff

    // Content size = 8 + 4 + 4 + 4 + 8 = 28 bytes
    // Total with header = 1 + 1 + 4 + 28 = 34 bytes
}
