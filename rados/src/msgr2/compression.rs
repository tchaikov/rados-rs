//! Compression support for msgr2 protocol
//!
//! This module implements compression algorithms for msgr2 frames.
//! Ceph supports multiple compression methods: Snappy, Zstd, LZ4, and Zlib.
//!
//! Reference: ~/dev/ceph/src/compressor/Compressor.h

use crate::msgr2::error::{Msgr2Error as Error, Result};
use bytes::Bytes;
use std::cell::Cell;

/// Compression algorithm identifiers (from Ceph's Compressor.h)
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u32)]
pub enum CompressionAlgorithm {
    None = 0,
    Snappy = 1,
    Zlib = 2,
    Zstd = 3,
    Lz4 = 4,
}

/// Trait for compression implementations
pub trait Compressor: Send + Sync {
    /// Compress data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Decompress data. Each algorithm embeds the original size in its stream.
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Get the algorithm identifier
    fn algorithm(&self) -> CompressionAlgorithm;
}

/// No compression (passthrough)
#[derive(Debug, Clone, Copy)]
pub struct NoneCompressor;

impl Compressor for NoneCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::None
    }
}

/// Snappy compression (most common in Ceph)
#[derive(Debug, Clone, Copy)]
pub struct SnappyCompressor;

impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = snap::raw::Encoder::new();
        encoder
            .compress_vec(data)
            .map_err(|e| Error::compression_error(&format!("Snappy compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Snappy embeds the uncompressed length in its stream header.
        let mut decoder = snap::raw::Decoder::new();
        decoder
            .decompress_vec(data)
            .map_err(|e| Error::compression_error(&format!("Snappy decompression failed: {}", e)))
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Snappy
    }
}

/// Zstandard compression
#[derive(Debug, Clone, Copy)]
pub struct ZstdCompressor {
    /// Compression level (1-21, default 3)
    level: i32,
}

impl ZstdCompressor {
    pub fn new() -> Self {
        Self { level: 3 }
    }

    pub fn with_level(level: i32) -> Self {
        Self { level }
    }
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::encode_all(data, self.level)
            .map_err(|e| Error::compression_error(&format!("Zstd compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Zstd embeds the uncompressed size in its frame header.
        zstd::decode_all(data)
            .map_err(|e| Error::compression_error(&format!("Zstd decompression failed: {}", e)))
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zstd
    }
}

/// LZ4 compression
#[derive(Debug, Clone, Copy)]
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // prepend_size=true writes a 4-byte little-endian original size header,
        // which decompress() reads to avoid needing an external size hint.
        lz4::block::compress(data, None, true)
            .map_err(|e| Error::compression_error(&format!("LZ4 compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // `lz4::block::compress` with `prepend_size=true` prefixes the output with
        // a 4-byte little-endian uncompressed size. Read it and pass it to decompress
        // explicitly, because `decompress(data, None)` is unreliable across crate versions.
        if data.len() < 4 {
            return Err(Error::compression_error(
                "LZ4 data too short to contain size header",
            ));
        }
        let mut size_bytes = [0u8; 4];
        size_bytes.copy_from_slice(&data[..4]);
        let original_size = i32::from_le_bytes(size_bytes);
        if original_size < 0 {
            return Err(Error::compression_error("LZ4 size header is negative"));
        }
        lz4::block::decompress(&data[4..], Some(original_size))
            .map_err(|e| Error::compression_error(&format!("LZ4 decompression failed: {}", e)))
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Lz4
    }
}

/// Zlib compression
#[derive(Debug, Clone, Copy)]
pub struct ZlibCompressor {
    /// Compression level (0-9, default 6)
    level: u32,
}

impl ZlibCompressor {
    pub fn new() -> Self {
        Self { level: 6 }
    }

    pub fn with_level(level: u32) -> Self {
        Self { level }
    }
}

impl Default for ZlibCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compressor for ZlibCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::Compression;
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(self.level));
        encoder
            .write_all(data)
            .map_err(|e| Error::compression_error(&format!("Zlib compression failed: {}", e)))?;
        encoder
            .finish()
            .map_err(|e| Error::compression_error(&format!("Zlib compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Zlib streams are self-describing; no external size hint required.
        use flate2::read::ZlibDecoder;
        use std::io::Read;

        let mut decoder = ZlibDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| Error::compression_error(&format!("Zlib decompression failed: {}", e)))?;
        Ok(decompressed)
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zlib
    }
}

/// Accumulated statistics for a compression context.
///
/// Snapshot returned by [`CompressionContext::stats`]. All byte counts refer
/// to payload data (not framing overhead).
#[derive(Debug, Default, Clone, Copy)]
pub struct CompressionStats {
    /// Total uncompressed bytes passed to `compress()`
    pub bytes_before_compression: u64,
    /// Total compressed bytes produced by `compress()`
    pub bytes_after_compression: u64,
    /// Number of `compress()` calls that produced output
    pub compression_ops: u64,
    /// Total compressed bytes passed to `decompress()`
    pub bytes_before_decompression: u64,
    /// Total decompressed bytes produced by `decompress()`
    pub bytes_after_decompression: u64,
    /// Number of `decompress()` calls
    pub decompression_ops: u64,
}

impl CompressionStats {
    /// Compression ratio (output / input). Returns `1.0` when nothing has been compressed yet.
    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_before_compression == 0 {
            1.0
        } else {
            self.bytes_after_compression as f64 / self.bytes_before_compression as f64
        }
    }

    /// Bytes saved by compression (`before - after`). Returns `0` if compression expanded data.
    pub fn bytes_saved(&self) -> u64 {
        self.bytes_before_compression
            .saturating_sub(self.bytes_after_compression)
    }
}

/// Internal per-field Cell storage — lets `compress()`/`decompress()` update counters
/// through a shared `&self` reference without requiring `&mut self`.
#[derive(Default)]
struct StatsCell {
    bytes_before_compression: Cell<u64>,
    bytes_after_compression: Cell<u64>,
    compression_ops: Cell<u64>,
    bytes_before_decompression: Cell<u64>,
    bytes_after_decompression: Cell<u64>,
    decompression_ops: Cell<u64>,
}

impl StatsCell {
    fn snapshot(&self) -> CompressionStats {
        CompressionStats {
            bytes_before_compression: self.bytes_before_compression.get(),
            bytes_after_compression: self.bytes_after_compression.get(),
            compression_ops: self.compression_ops.get(),
            bytes_before_decompression: self.bytes_before_decompression.get(),
            bytes_after_decompression: self.bytes_after_decompression.get(),
            decompression_ops: self.decompression_ops.get(),
        }
    }
}

/// Compression context that holds the active compressor
pub struct CompressionContext {
    compressor: Box<dyn Compressor>,
    /// Minimum size threshold for compression (bytes)
    /// Frames smaller than this won't be compressed
    threshold: usize,
    /// Algorithm for debugging
    algorithm: CompressionAlgorithm,
    /// Accumulated statistics (interior-mutable so compress/decompress stay &self)
    stats: StatsCell,
}

impl std::fmt::Debug for CompressionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionContext")
            .field("algorithm", &self.algorithm)
            .field("threshold", &self.threshold)
            .field("stats", &self.stats.snapshot())
            .finish()
    }
}

impl CompressionContext {
    fn make_compressor(algorithm: CompressionAlgorithm) -> Box<dyn Compressor> {
        match algorithm {
            CompressionAlgorithm::None => Box::new(NoneCompressor),
            CompressionAlgorithm::Snappy => Box::new(SnappyCompressor),
            CompressionAlgorithm::Zstd => Box::new(ZstdCompressor::new()),
            CompressionAlgorithm::Lz4 => Box::new(Lz4Compressor),
            CompressionAlgorithm::Zlib => Box::new(ZlibCompressor::new()),
        }
    }

    /// Create a new compression context with the specified algorithm
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self::with_threshold(algorithm, 512)
    }

    /// Create context with custom threshold
    pub fn with_threshold(algorithm: CompressionAlgorithm, threshold: usize) -> Self {
        Self {
            compressor: Self::make_compressor(algorithm),
            threshold,
            algorithm,
            stats: StatsCell::default(),
        }
    }

    /// Check if data should be compressed based on size threshold
    pub fn should_compress(&self, data_len: usize) -> bool {
        data_len >= self.threshold && self.algorithm() != CompressionAlgorithm::None
    }

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> Result<Bytes> {
        let compressed = self.compressor.compress(data)?;
        self.stats
            .bytes_before_compression
            .set(self.stats.bytes_before_compression.get() + data.len() as u64);
        self.stats
            .bytes_after_compression
            .set(self.stats.bytes_after_compression.get() + compressed.len() as u64);
        self.stats
            .compression_ops
            .set(self.stats.compression_ops.get() + 1);
        Ok(Bytes::from(compressed))
    }

    /// Decompress data. Each algorithm embeds the original size in its stream.
    pub fn decompress(&self, data: &[u8]) -> Result<Bytes> {
        let decompressed = self.compressor.decompress(data)?;
        self.stats
            .bytes_before_decompression
            .set(self.stats.bytes_before_decompression.get() + data.len() as u64);
        self.stats
            .bytes_after_decompression
            .set(self.stats.bytes_after_decompression.get() + decompressed.len() as u64);
        self.stats
            .decompression_ops
            .set(self.stats.decompression_ops.get() + 1);
        Ok(Bytes::from(decompressed))
    }

    /// Get the current algorithm
    pub fn algorithm(&self) -> CompressionAlgorithm {
        self.algorithm
    }

    /// Get the compression threshold
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Snapshot of accumulated compression statistics.
    pub fn stats(&self) -> CompressionStats {
        self.stats.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_compressor() {
        let compressor = NoneCompressor;
        let data = b"Hello, World!";

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed, data);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_compressor() {
        let compressor = SnappyCompressor;
        let data = b"Hello, World! This is a test of Snappy compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Snappy);
    }

    #[test]
    fn test_zstd_compressor() {
        let compressor = ZstdCompressor::new();
        let data = b"Hello, World! This is a test of Zstd compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_lz4_compressor() {
        let compressor = Lz4Compressor;
        let data = b"Hello, World! This is a test of LZ4 compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_compression_context() {
        let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
        let data = b"Hello, World! This is a test of compression context.";

        // Test compression
        let compressed = ctx.compress(data).unwrap();
        let decompressed = ctx.decompress(&compressed).unwrap();

        assert_eq!(&decompressed[..], data);
        assert_eq!(ctx.algorithm(), CompressionAlgorithm::Snappy);
    }

    #[test]
    fn test_compression_threshold() {
        let ctx = CompressionContext::with_threshold(CompressionAlgorithm::Snappy, 100);

        assert!(!ctx.should_compress(50));
        assert!(!ctx.should_compress(99));
        assert!(ctx.should_compress(100));
        assert!(ctx.should_compress(1000));

        assert_eq!(ctx.threshold(), 100);
    }

    #[test]
    fn test_large_data_compression() {
        let compressor = SnappyCompressor;
        let data = vec![b'A'; 10000]; // 10KB of 'A's

        let compressed = compressor.compress(&data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
        // Snappy should compress repeated data well
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_zlib_compressor() {
        let compressor = ZlibCompressor::new();
        let data = b"Hello, World! This is a test of Zlib compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Zlib);
    }

    #[test]
    fn test_compression_stats_tracking() {
        let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
        let data = b"Hello, World! This is a test of compression statistics tracking.".repeat(10);

        // Initial stats should be zero
        let initial = ctx.stats();
        assert_eq!(initial.bytes_before_compression, 0);
        assert_eq!(initial.bytes_after_compression, 0);
        assert_eq!(initial.compression_ops, 0);
        assert_eq!(initial.bytes_before_decompression, 0);
        assert_eq!(initial.bytes_after_decompression, 0);
        assert_eq!(initial.decompression_ops, 0);
        assert_eq!(initial.compression_ratio(), 1.0);
        assert_eq!(initial.bytes_saved(), 0);

        // After compression
        let compressed = ctx.compress(&data).unwrap();
        let after_compress = ctx.stats();
        assert_eq!(after_compress.bytes_before_compression, data.len() as u64);
        assert_eq!(
            after_compress.bytes_after_compression,
            compressed.len() as u64
        );
        assert_eq!(after_compress.compression_ops, 1);
        assert_eq!(after_compress.decompression_ops, 0);
        // Snappy should compress repeated data
        assert!(after_compress.compression_ratio() < 1.0);
        assert!(after_compress.bytes_saved() > 0);

        // After decompression
        ctx.decompress(&compressed).unwrap();
        let after_decompress = ctx.stats();
        assert_eq!(
            after_decompress.bytes_before_decompression,
            compressed.len() as u64
        );
        assert_eq!(
            after_decompress.bytes_after_decompression,
            data.len() as u64
        );
        assert_eq!(after_decompress.decompression_ops, 1);
        // Compression stats unchanged
        assert_eq!(after_decompress.compression_ops, 1);

        // Second compress — ops accumulate
        ctx.compress(&data).unwrap();
        let final_stats = ctx.stats();
        assert_eq!(final_stats.compression_ops, 2);
        assert_eq!(final_stats.bytes_before_compression, data.len() as u64 * 2);
    }

    #[test]
    fn test_all_algorithms() {
        let data = b"The quick brown fox jumps over the lazy dog. ".repeat(10);
        let algorithms = vec![
            CompressionAlgorithm::None,
            CompressionAlgorithm::Snappy,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Zlib,
        ];

        for algo in algorithms {
            let ctx = CompressionContext::new(algo);
            let compressed = ctx.compress(&data).unwrap();
            let decompressed = ctx.decompress(&compressed).unwrap();
            assert_eq!(&decompressed[..], &data[..], "Algorithm {:?} failed", algo);
        }
    }
}
