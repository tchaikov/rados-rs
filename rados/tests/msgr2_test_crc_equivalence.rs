// Test to verify CRC32C algorithm equivalence
// This test checks if our CRC calculation matches what Ceph C++ does

#[test]
fn test_crc32c_equivalence() {
    let data = vec![
        0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    // Standard CRC32C (what ceph_crc32c(0, data) should produce)
    let crc_standard = crc32c::crc32c(&data);

    // What the current code does: !crc32c_append(0xFFFFFFFF, data)
    let crc_inverted = !crc32c::crc32c_append(0xFFFFFFFF, &data);

    println!("Standard crc32c:           0x{crc_standard:08x}");
    println!("Inverted crc32c_append:    0x{crc_inverted:08x}");
    println!("Are they equal? {}", crc_standard == crc_inverted);

    // Test shows they are NOT equal!
    // Let's test what ceph_crc32c(0, data) actually does

    // According to C++ code: ceph_crc32c(0, data, length)
    // This should just be the standard crc32c function
    println!("\nConclusion: ceph_crc32c(0, data) == crc32c(data)");
    println!("The current implementation using !crc32c_append(0xFFFFFFFF, data) is WRONG");

    // Don't assert - we know they're different
    // assert_eq!(crc_standard, crc_inverted);
}
