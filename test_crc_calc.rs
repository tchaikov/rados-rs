// Test to verify correct CRC calculation for HELLO segment

fn main() {
    // Our HELLO segment bytes (20 bytes)
    let segment_bytes = hex::decode("010101010c000000000000000000000000000000").unwrap();

    println!("Segment bytes ({} bytes): {:02x?}", segment_bytes.len(), segment_bytes);

    // Method 1: Standard crc32c
    let crc1 = crc32c::crc32c(&segment_bytes);
    println!("crc32c::crc32c(): {} (0x{:08x})", crc1, crc1);

    // Method 2: crc32c_append with 0xFFFFFFFF, then invert
    let crc2 = !crc32c::crc32c_append(0xFFFFFFFF, &segment_bytes);
    println!("!crc32c_append(0xFFFFFFFF): {} (0x{:08x})", crc2, crc2);

    // Method 3: crc32c_append with 0 (no inversion)
    let crc3 = crc32c::crc32c_append(0, &segment_bytes);
    println!("crc32c_append(0): {} (0x{:08x})", crc3, crc3);

    // Method 4: What Ceph expects
    println!("\nExpected by server: 768717020 (0x2dd1b0dc)");
    println!("Server calculated: 3526250275 (0xd2207023)");

    // Method 5: Try with -1 cast to u32
    let crc5 = crc32c::crc32c_append((-1i32) as u32, &segment_bytes);
    println!("crc32c_append(-1 as u32): {} (0x{:08x})", crc5, crc5);
}
