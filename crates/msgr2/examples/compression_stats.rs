//! Example demonstrating compression statistics tracking
//!
//! This example shows how to use the CompressionStats API to monitor
//! compression effectiveness.

use msgr2::compression::{CompressionAlgorithm, CompressionContext};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create compression contexts for different algorithms
    let algorithms = vec![
        CompressionAlgorithm::Snappy,
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Zlib,
    ];

    // Test data: highly compressible (repeated data)
    let compressible_data = vec![b'A'; 10000];
    // Test data: not very compressible (random-like)
    let incompressible_data = b"The quick brown fox jumps over the lazy dog 1234567890";

    println!("Compression Statistics Example\n");
    println!("{:=<80}", "");

    for algo in algorithms {
        let ctx = CompressionContext::new(algo);

        println!("\nAlgorithm: {:?}", algo);
        println!("{:-<80}", "");

        // Compress highly compressible data
        let compressed = ctx.compress(&compressible_data)?;
        let _decompressed = ctx.decompress(&compressed, compressible_data.len())?;

        // Compress less compressible data
        let compressed2 = ctx.compress(incompressible_data)?;
        let _decompressed2 = ctx.decompress(&compressed2, incompressible_data.len())?;

        // Get statistics
        let stats = ctx.stats();

        println!("Total operations:");
        println!("  Compressions:   {}", stats.compression_count());
        println!("  Decompressions: {}", stats.decompression_count());
        println!();
        println!("Size statistics:");
        println!("  Initial size:    {} bytes", stats.initial_size());
        println!("  Compressed size: {} bytes", stats.compressed_size());
        println!(
            "  Compression ratio: {:.2}% (lower is better)",
            stats.ratio() * 100.0
        );
        println!(
            "  Space saved: {} bytes ({:.1}%)",
            stats.initial_size() - stats.compressed_size(),
            (1.0 - stats.ratio()) * 100.0
        );
    }

    println!("\n{:=<80}", "");

    // Example: monitoring compression over time
    println!("\nMonitoring compression over time:");
    println!("{:-<80}", "");

    let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);

    for i in 1..=5 {
        let data = vec![b'X'; 1000 * i];
        let _compressed = ctx.compress(&data)?;

        let stats = ctx.stats();
        println!(
            "After {} operations: ratio={:.2}%, total_initial={} bytes, total_compressed={} bytes",
            stats.compression_count(),
            stats.ratio() * 100.0,
            stats.initial_size(),
            stats.compressed_size()
        );
    }

    // Reset statistics
    println!("\nResetting statistics...");
    ctx.stats().reset();
    let stats = ctx.stats();
    println!(
        "After reset: operations={}, ratio={:.2}%",
        stats.compression_count(),
        stats.ratio() * 100.0
    );

    Ok(())
}
