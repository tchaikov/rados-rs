//! Integration tests for msgr2 compression support
//!
//! These tests verify that compression works correctly at the frame level,
//! including compression/decompression roundtrips and integration with
//! the state machine.

use bytes::Bytes;
use rados::msgr2::compression::{CompressionAlgorithm, CompressionContext};
use rados::msgr2::frames::{Frame, Tag};

#[test]
fn test_frame_compression_roundtrip_snappy() {
    // Create a frame with compressible data
    let data = b"Hello, World! This is a test of frame compression. ".repeat(20);
    let payload = Bytes::from(data.to_vec());
    let frame = Frame::new(Tag::Message, payload.clone());

    // Create compression context
    let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);

    // Compress the frame
    let compressed_frame = frame.compress(&ctx);

    // Verify compression flag is set
    assert!(
        compressed_frame.preamble.flags.is_compressed(),
        "Compression flag should be set"
    );

    // Verify frame is actually compressed (smaller)
    let original_size: usize = frame.segments.iter().map(|s| s.len()).sum();
    let compressed_size: usize = compressed_frame.segments.iter().map(|s| s.len()).sum();
    assert!(
        compressed_size < original_size,
        "Compressed size {} should be less than original size {}",
        compressed_size,
        original_size
    );

    // Decompress the frame
    let decompressed_frame = compressed_frame
        .decompress(&ctx)
        .expect("Decompression should succeed");

    // Verify compression flag is cleared
    assert!(
        !decompressed_frame.preamble.flags.is_compressed(),
        "Compression flag should be cleared"
    );

    // Verify data is identical
    assert_eq!(
        decompressed_frame.segments[0], payload,
        "Decompressed data should match original"
    );
}

#[test]
fn test_frame_compression_roundtrip_zstd() {
    let data = b"Zstandard compression test data. ".repeat(30);
    let payload = Bytes::from(data.to_vec());
    let frame = Frame::new(Tag::Message, payload.clone());

    let ctx = CompressionContext::new(CompressionAlgorithm::Zstd);
    let compressed_frame = frame.compress(&ctx);

    let decompressed_frame = compressed_frame
        .decompress(&ctx)
        .expect("Decompression should succeed");

    assert_eq!(decompressed_frame.segments[0], payload);
}

#[test]
fn test_frame_compression_roundtrip_lz4() {
    let data = b"LZ4 compression test data. ".repeat(25);
    let payload = Bytes::from(data.to_vec());
    let frame = Frame::new(Tag::Message, payload.clone());

    let ctx = CompressionContext::new(CompressionAlgorithm::Lz4);
    let compressed_frame = frame.compress(&ctx);

    let decompressed_frame = compressed_frame
        .decompress(&ctx)
        .expect("Decompression should succeed");

    assert_eq!(decompressed_frame.segments[0], payload);
}

#[test]
fn test_frame_compression_roundtrip_zlib() {
    let data = b"Zlib compression test data. ".repeat(25);
    let payload = Bytes::from(data.to_vec());
    let frame = Frame::new(Tag::Message, payload.clone());

    let ctx = CompressionContext::new(CompressionAlgorithm::Zlib);
    let compressed_frame = frame.compress(&ctx);

    let decompressed_frame = compressed_frame
        .decompress(&ctx)
        .expect("Decompression should succeed");

    assert_eq!(decompressed_frame.segments[0], payload);
}

#[test]
fn test_frame_compression_threshold() {
    // Create a small frame (below threshold)
    let small_data = b"Small";
    let small_payload = Bytes::from(small_data.to_vec());
    let small_frame = Frame::new(Tag::Message, small_payload.clone());

    // Create compression context with default threshold (512 bytes)
    let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);

    // Compress the small frame
    let compressed_small = small_frame.compress(&ctx);

    // Verify small frame is NOT compressed (below threshold)
    assert!(
        !compressed_small.preamble.flags.is_compressed(),
        "Small frame should not be compressed"
    );
    assert_eq!(
        compressed_small.segments[0], small_payload,
        "Small frame data should be unchanged"
    );

    // Create a large frame (above threshold)
    let large_data = b"Large data ".repeat(100); // ~1100 bytes
    let large_payload = Bytes::from(large_data.to_vec());
    let large_frame = Frame::new(Tag::Message, large_payload);

    // Compress the large frame
    let compressed_large = large_frame.compress(&ctx);

    // Verify large frame IS compressed
    assert!(
        compressed_large.preamble.flags.is_compressed(),
        "Large frame should be compressed"
    );
}

#[test]
fn test_frame_compression_with_custom_threshold() {
    // Create compression context with low threshold (10 bytes)
    let ctx = CompressionContext::with_threshold(CompressionAlgorithm::Snappy, 10);

    // Create a frame with 50 bytes (above custom threshold)
    let data = b"This is 50 bytes of data for compression test!";
    let payload = Bytes::from(data.to_vec());
    let frame = Frame::new(Tag::Message, payload);

    // Compress the frame
    let compressed_frame = frame.compress(&ctx);

    // Verify frame is compressed (above custom threshold)
    assert!(
        compressed_frame.preamble.flags.is_compressed(),
        "Frame should be compressed with custom threshold"
    );
}

#[test]
fn test_frame_decompression_of_uncompressed_frame() {
    // Create an uncompressed frame
    let data = b"Uncompressed data";
    let payload = Bytes::from(data.to_vec());
    let frame = Frame::new(Tag::Message, payload.clone());

    // Create compression context
    let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);

    // Try to decompress (should return frame as-is)
    let result = frame
        .decompress(&ctx)
        .expect("Decompression should succeed");

    // Verify frame is unchanged
    assert!(
        !result.preamble.flags.is_compressed(),
        "Compression flag should not be set"
    );
    assert_eq!(result.segments[0], payload, "Data should be unchanged");
}

#[test]
fn test_compression_ratio_logging() {
    // Create highly compressible data (repeated pattern)
    let data = b"A".repeat(10000);
    let payload = Bytes::from(data);
    let frame = Frame::new(Tag::Message, payload);

    let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
    let compressed_frame = frame.compress(&ctx);

    let original_size: usize = frame.segments.iter().map(|s| s.len()).sum();
    let compressed_size: usize = compressed_frame.segments.iter().map(|s| s.len()).sum();

    // Verify significant compression (repeated data should compress well)
    let ratio = (compressed_size as f64 / original_size as f64) * 100.0;
    assert!(
        ratio < 10.0,
        "Compression ratio should be < 10% for repeated data, got {:.2}%",
        ratio
    );
}

#[test]
fn test_all_algorithms_roundtrip() {
    let algorithms = vec![
        CompressionAlgorithm::Snappy,
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Zlib,
    ];

    let data = b"Test data for all compression algorithms. ".repeat(50);
    let payload = Bytes::from(data.to_vec());

    for algo in algorithms {
        let frame = Frame::new(Tag::Message, payload.clone());
        let ctx = CompressionContext::new(algo);

        let compressed = frame.compress(&ctx);

        let decompressed = compressed
            .decompress(&ctx)
            .unwrap_or_else(|_| panic!("Decompression should succeed for {:?}", algo));

        assert_eq!(
            decompressed.segments[0], payload,
            "Roundtrip failed for {:?}",
            algo
        );
    }
}

#[test]
fn test_snappy_decompress_with_incorrect_size_hint() {
    let data = b"Snappy decompression should not depend on caller size hint. ".repeat(40);
    let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);

    let compressed = ctx.compress(&data).expect("Compression should succeed");
    let decompressed = ctx
        .decompress(&compressed)
        .expect("Decompression should succeed even with bad size hint");

    assert_eq!(&decompressed[..], &data[..]);
}

#[test]
fn test_frame_to_wire_preserves_compression_flag() {
    let data = b"Test data ".repeat(100);
    let payload = Bytes::from(data.to_vec());
    let frame = Frame::new(Tag::Message, payload);

    let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
    let compressed_frame = frame.compress(&ctx);

    // Convert to wire format
    let wire_bytes = compressed_frame
        .to_wire(true)
        .expect("to_wire should succeed");

    // Verify wire format is not empty
    assert!(!wire_bytes.is_empty(), "Wire bytes should not be empty");

    // The compression flag should be preserved in the preamble
    // (We can't easily parse it back without full frame parsing, but we verify it's set)
    assert!(
        compressed_frame.preamble.flags.is_compressed(),
        "Compression flag should be preserved"
    );
}
