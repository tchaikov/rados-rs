//! Compression support for msgr2 protocol
//!
//! This module implements compression algorithms for msgr2 frames.
//! Ceph supports multiple compression methods: Snappy, Zstd, LZ4, and Zlib.
//!
//! Reference: ~/dev/ceph/src/compressor/Compressor.h

use crate::error::{Error, Result};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Compression algorithm identifiers (from Ceph's Compressor.h)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum CompressionAlgorithm {
    None = 0,
    Snappy = 1,
    Zlib = 2,
    Zstd = 3,
    Lz4 = 4,
}

impl CompressionAlgorithm {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::Snappy),
            2 => Some(Self::Zlib),
            3 => Some(Self::Zstd),
            4 => Some(Self::Lz4),
            _ => None,
        }
    }

    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Statistics for compression operations
///
/// Tracks compression effectiveness and operation counts.
/// Thread-safe using atomic operations.
///
/// # Example
///
/// ```rust
/// use msgr2::compression::{CompressionContext, CompressionAlgorithm};
///
/// let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
/// let data = b"Hello, World!";
///
/// // Perform operations
/// let compressed = ctx.compress(data).unwrap();
/// let _decompressed = ctx.decompress(&compressed, data.len()).unwrap();
///
/// // Check statistics
/// let stats = ctx.stats();
/// assert_eq!(stats.compression_count(), 1);
/// assert_eq!(stats.decompression_count(), 1);
/// assert!(stats.ratio() < 1.0); // Effective compression
/// ```
#[derive(Debug, Clone)]
pub struct CompressionStats {
    /// Algorithm used for compression
    algorithm: CompressionAlgorithm,
    /// Total bytes before compression
    initial_size: Arc<AtomicU64>,
    /// Total bytes after compression
    compressed_size: Arc<AtomicU64>,
    /// Number of compression operations
    compression_count: Arc<AtomicU64>,
    /// Number of decompression operations
    decompression_count: Arc<AtomicU64>,
}

impl CompressionStats {
    /// Create new statistics tracker
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self {
            algorithm,
            initial_size: Arc::new(AtomicU64::new(0)),
            compressed_size: Arc::new(AtomicU64::new(0)),
            compression_count: Arc::new(AtomicU64::new(0)),
            decompression_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get the algorithm
    pub fn algorithm(&self) -> CompressionAlgorithm {
        self.algorithm
    }

    /// Get total initial (uncompressed) size
    pub fn initial_size(&self) -> u64 {
        self.initial_size.load(Ordering::Relaxed)
    }

    /// Get total compressed size
    pub fn compressed_size(&self) -> u64 {
        self.compressed_size.load(Ordering::Relaxed)
    }

    /// Get number of compression operations
    pub fn compression_count(&self) -> u64 {
        self.compression_count.load(Ordering::Relaxed)
    }

    /// Get number of decompression operations
    pub fn decompression_count(&self) -> u64 {
        self.decompression_count.load(Ordering::Relaxed)
    }

    /// Calculate compression ratio (compressed / initial)
    /// Returns 0.0 if no data has been compressed
    pub fn ratio(&self) -> f64 {
        let initial = self.initial_size();
        let compressed = self.compressed_size();
        if initial == 0 {
            0.0
        } else {
            compressed as f64 / initial as f64
        }
    }

    /// Record a compression operation
    fn record_compression(&self, initial_size: usize, compressed_size: usize) {
        self.initial_size
            .fetch_add(initial_size as u64, Ordering::Relaxed);
        self.compressed_size
            .fetch_add(compressed_size as u64, Ordering::Relaxed);
        self.compression_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a decompression operation
    fn record_decompression(&self) {
        self.decompression_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.initial_size.store(0, Ordering::Relaxed);
        self.compressed_size.store(0, Ordering::Relaxed);
        self.compression_count.store(0, Ordering::Relaxed);
        self.decompression_count.store(0, Ordering::Relaxed);
    }
}

/// Trait for compression implementations
pub trait Compressor: Send + Sync {
    /// Compress data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Decompress data with known original size
    fn decompress(&self, data: &[u8], original_size: usize) -> Result<Vec<u8>>;

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

    fn decompress(&self, data: &[u8], _original_size: usize) -> Result<Vec<u8>> {
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

    fn decompress(&self, data: &[u8], original_size: usize) -> Result<Vec<u8>> {
        let mut decoder = snap::raw::Decoder::new();
        match decoder.decompress_vec(data) {
            Ok(output) => Ok(output),
            Err(e) => {
                tracing::debug!(
                    "Snappy decompress_vec failed, falling back to size-hinted decompress: {}",
                    e
                );
                let mut output = vec![0u8; original_size];
                let decompressed_len = decoder.decompress(data, &mut output).map_err(|e| {
                    Error::compression_error(&format!("Snappy decompression failed: {}", e))
                })?;
                if decompressed_len != original_size {
                    return Err(Error::compression_error(&format!(
                        "Snappy fallback decompression size mismatch: expected {}, got {}",
                        original_size, decompressed_len,
                    )));
                }
                Ok(output)
            }
        }
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

    fn decompress(&self, data: &[u8], _original_size: usize) -> Result<Vec<u8>> {
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
        lz4::block::compress(data, None, false)
            .map_err(|e| Error::compression_error(&format!("LZ4 compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8], original_size: usize) -> Result<Vec<u8>> {
        lz4::block::decompress(data, None)
            .or_else(|e| {
                tracing::debug!(
                    "LZ4 metadata-driven decompress failed, falling back to size hint: {}",
                    e
                );
                lz4::block::decompress(data, Some(original_size as i32))
            })
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
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(self.level));
        encoder
            .write_all(data)
            .map_err(|e| Error::compression_error(&format!("Zlib compression failed: {}", e)))?;
        encoder
            .finish()
            .map_err(|e| Error::compression_error(&format!("Zlib compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8], _original_size: usize) -> Result<Vec<u8>> {
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

/// Compression context that holds the active compressor
pub struct CompressionContext {
    compressor: Box<dyn Compressor>,
    /// Minimum size threshold for compression (bytes)
    /// Frames smaller than this won't be compressed
    threshold: usize,
    /// Algorithm for debugging
    algorithm: CompressionAlgorithm,
    /// Statistics tracking
    stats: CompressionStats,
}

impl std::fmt::Debug for CompressionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionContext")
            .field("algorithm", &self.algorithm)
            .field("threshold", &self.threshold)
            .field("stats", &self.stats)
            .finish()
    }
}

impl CompressionContext {
    /// Create a new compression context with the specified algorithm
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        let compressor: Box<dyn Compressor> = match algorithm {
            CompressionAlgorithm::None => Box::new(NoneCompressor),
            CompressionAlgorithm::Snappy => Box::new(SnappyCompressor),
            CompressionAlgorithm::Zstd => Box::new(ZstdCompressor::new()),
            CompressionAlgorithm::Lz4 => Box::new(Lz4Compressor),
            CompressionAlgorithm::Zlib => Box::new(ZlibCompressor::new()),
        };

        Self {
            compressor,
            threshold: 512, // Default: compress frames >= 512 bytes
            algorithm,
            stats: CompressionStats::new(algorithm),
        }
    }

    /// Create context with custom threshold
    pub fn with_threshold(algorithm: CompressionAlgorithm, threshold: usize) -> Self {
        let compressor: Box<dyn Compressor> = match algorithm {
            CompressionAlgorithm::None => Box::new(NoneCompressor),
            CompressionAlgorithm::Snappy => Box::new(SnappyCompressor),
            CompressionAlgorithm::Zstd => Box::new(ZstdCompressor::new()),
            CompressionAlgorithm::Lz4 => Box::new(Lz4Compressor),
            CompressionAlgorithm::Zlib => Box::new(ZlibCompressor::new()),
        };

        Self {
            compressor,
            threshold,
            algorithm,
            stats: CompressionStats::new(algorithm),
        }
    }

    /// Check if data should be compressed based on size threshold
    pub fn should_compress(&self, data_len: usize) -> bool {
        data_len >= self.threshold && self.algorithm() != CompressionAlgorithm::None
    }

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> Result<Bytes> {
        let initial_size = data.len();
        let compressed = self.compressor.compress(data)?;
        let compressed_size = compressed.len();

        // Record statistics
        self.stats.record_compression(initial_size, compressed_size);

        Ok(Bytes::from(compressed))
    }

    /// Decompress data
    pub fn decompress(&self, data: &[u8], original_size: usize) -> Result<Bytes> {
        let decompressed = self.compressor.decompress(data, original_size)?;

        // Record statistics
        self.stats.record_decompression();

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

    /// Get compression statistics
    pub fn stats(&self) -> &CompressionStats {
        &self.stats
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

        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_compressor() {
        let compressor = SnappyCompressor;
        let data = b"Hello, World! This is a test of Snappy compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Snappy);
    }

    #[test]
    fn test_zstd_compressor() {
        let compressor = ZstdCompressor::new();
        let data = b"Hello, World! This is a test of Zstd compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_lz4_compressor() {
        let compressor = Lz4Compressor;
        let data = b"Hello, World! This is a test of LZ4 compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_compression_context() {
        let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
        let data = b"Hello, World! This is a test of compression context.";

        // Test compression
        let compressed = ctx.compress(data).unwrap();
        let decompressed = ctx.decompress(&compressed, data.len()).unwrap();

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
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
        // Snappy should compress repeated data well
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_zlib_compressor() {
        let compressor = ZlibCompressor::new();
        let data = b"Hello, World! This is a test of Zlib compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Zlib);
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
            let decompressed = ctx.decompress(&compressed, data.len()).unwrap();
            assert_eq!(&decompressed[..], &data[..], "Algorithm {:?} failed", algo);
        }
    }

    #[test]
    fn test_compression_stats_basic() {
        let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
        let data = b"Hello, World! This is a test of compression statistics.";

        // Initially, stats should be zero
        let stats = ctx.stats();
        assert_eq!(stats.initial_size(), 0);
        assert_eq!(stats.compressed_size(), 0);
        assert_eq!(stats.compression_count(), 0);
        assert_eq!(stats.decompression_count(), 0);
        assert_eq!(stats.ratio(), 0.0);

        // Compress data
        let compressed = ctx.compress(data).unwrap();

        // Check stats after compression
        let stats = ctx.stats();
        assert_eq!(stats.initial_size(), data.len() as u64);
        assert_eq!(stats.compressed_size(), compressed.len() as u64);
        assert_eq!(stats.compression_count(), 1);
        assert_eq!(stats.decompression_count(), 0);
        assert!(stats.ratio() > 0.0);

        // Decompress data
        let _decompressed = ctx.decompress(&compressed, data.len()).unwrap();

        // Check stats after decompression
        let stats = ctx.stats();
        assert_eq!(stats.decompression_count(), 1);
    }

    #[test]
    fn test_compression_stats_multiple_operations() {
        let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
        let data1 = b"First data block for compression.";
        let data2 = b"Second data block for compression.";

        // Compress multiple times
        let compressed1 = ctx.compress(data1).unwrap();
        let compressed2 = ctx.compress(data2).unwrap();

        // Check accumulated stats
        let stats = ctx.stats();
        assert_eq!(stats.initial_size(), (data1.len() + data2.len()) as u64);
        assert_eq!(
            stats.compressed_size(),
            (compressed1.len() + compressed2.len()) as u64
        );
        assert_eq!(stats.compression_count(), 2);

        // Decompress multiple times
        let _decompressed1 = ctx.decompress(&compressed1, data1.len()).unwrap();
        let _decompressed2 = ctx.decompress(&compressed2, data2.len()).unwrap();

        // Check decompression count
        let stats = ctx.stats();
        assert_eq!(stats.decompression_count(), 2);
    }

    #[test]
    fn test_compression_stats_ratio() {
        let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
        // Use highly compressible data
        let data = vec![b'A'; 10000]; // 10KB of 'A's

        let compressed = ctx.compress(&data).unwrap();

        let stats = ctx.stats();
        let ratio = stats.ratio();

        // For highly compressible data, ratio should be much less than 1.0
        assert!(ratio < 1.0, "Expected ratio < 1.0, got {}", ratio);
        assert!(ratio > 0.0, "Expected ratio > 0.0, got {}", ratio);

        // Verify the ratio calculation
        let expected_ratio = compressed.len() as f64 / data.len() as f64;
        assert!(
            (ratio - expected_ratio).abs() < 0.0001,
            "Expected ratio {}, got {}",
            expected_ratio,
            ratio
        );
    }

    #[test]
    fn test_compression_stats_reset() {
        let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);
        let data = b"Test data for reset";

        // Perform some operations
        let compressed = ctx.compress(data).unwrap();
        let _decompressed = ctx.decompress(&compressed, data.len()).unwrap();

        // Verify stats are non-zero
        let stats = ctx.stats();
        assert!(stats.initial_size() > 0);
        assert!(stats.compression_count() > 0);
        assert!(stats.decompression_count() > 0);

        // Reset stats
        stats.reset();

        // Verify stats are zero
        assert_eq!(stats.initial_size(), 0);
        assert_eq!(stats.compressed_size(), 0);
        assert_eq!(stats.compression_count(), 0);
        assert_eq!(stats.decompression_count(), 0);
        assert_eq!(stats.ratio(), 0.0);
    }

    #[test]
    fn test_compression_stats_algorithm() {
        let algorithms = vec![
            CompressionAlgorithm::None,
            CompressionAlgorithm::Snappy,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Zlib,
        ];

        for algo in algorithms {
            let ctx = CompressionContext::new(algo);
            let stats = ctx.stats();
            assert_eq!(stats.algorithm(), algo);
        }
    }
}
