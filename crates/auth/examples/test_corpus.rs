use auth::{CryptoKey, CEPH_CRYPTO_AES};
use bytes::{BufMut, Bytes, BytesMut};
use denc::Denc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test decoding the corpus file
    // Read the corpus file
    let corpus_path = "/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/CryptoKey/000069034d5bb375116dd7dbf6121c4b";
    let corpus_data = std::fs::read(corpus_path)?;

    println!("Corpus data length: {} bytes", corpus_data.len());
    println!("Hex dump: {:02x?}", corpus_data);

    let mut data = Bytes::copy_from_slice(&corpus_data);
    println!("Attempting to decode CryptoKey from corpus...");

    match CryptoKey::decode(&mut data, 0) {
        Ok(key) => {
            println!("Successfully decoded CryptoKey:");
            println!("  Type: {} (expected: {})", key.get_type(), CEPH_CRYPTO_AES);
            println!("  Created: {:?}", key.get_created());
            println!("  Secret length: {} bytes", key.len());
            println!("  Remaining data: {} bytes", data.len());

            // Test encoding it back
            println!("\nTesting re-encoding...");
            let mut buf = BytesMut::new();
            match key.encode(&mut buf, 0) {
                Ok(()) => {
                    let encoded = buf.freeze();
                    println!("Re-encoded length: {} bytes", encoded.len());
                    println!("Re-encoded hex: {:02x?}", encoded.as_ref());

                    if encoded.as_ref() == corpus_data.as_slice() {
                        println!("✓ Perfect match! Encoding is correct.");
                    } else {
                        println!("✗ Mismatch detected:");
                        println!("  Expected: {:02x?}", corpus_data);
                        println!("  Got:      {:02x?}", encoded.as_ref());

                        // Compare byte by byte
                        for (i, (expected, actual)) in
                            corpus_data.iter().zip(encoded.iter()).enumerate()
                        {
                            if expected != actual {
                                println!(
                                    "  Difference at byte {}: expected 0x{:02x}, got 0x{:02x}",
                                    i, expected, actual
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("✗ Failed to re-encode: {}", e);
                }
            }
        }
        Err(e) => {
            println!("✗ Failed to decode: {}", e);
        }
    }

    // Test creating a new key and encoding it
    println!("\nTesting new key creation and encoding...");
    let secret = vec![
        0x04, 0x67, 0x12, 0xf8, 0x60, 0xd9, 0x49, 0x12, 0x87, 0xce, 0x15, 0x70, 0x2f, 0x8c, 0x5f,
        0xa5,
    ];
    let created_time =
        UNIX_EPOCH + Duration::from_secs(1727590173) + Duration::from_nanos(674657455);

    // Manually create a CryptoKey with specific values for testing
    let test_key = CryptoKey {
        crypto_type: CEPH_CRYPTO_AES,
        created: created_time,
        secret: Bytes::from(secret),
    };

    let mut test_buf = BytesMut::new();
    match test_key.encode(&mut test_buf, 0) {
        Ok(()) => {
            let encoded = test_buf.freeze();
            println!("Test key encoded length: {} bytes", encoded.len());
            println!("Test key hex: {:02x?}", encoded.as_ref());

            if encoded.as_ref() == corpus_data.as_slice() {
                println!("✓ Test key matches corpus perfectly!");
            } else {
                println!("✗ Test key differs from corpus");
            }
        }
        Err(e) => {
            println!("✗ Failed to encode test key: {}", e);
        }
    }

    Ok(())
}
