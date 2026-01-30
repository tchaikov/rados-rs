//! Compression support for msgr2 protocol
//!
//! This module implements compression algorithms for msgr2 frames.
//! Ceph supports multiple compression methods: Snappy, Zstd, LZ4, and Zlib.
//!
//! Reference: ~/dev/ceph/src/compressor/Compressor.h

use crate::error::{Error, Result};
use bytes::Bytes;

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
        let mut output = vec![0u8; original_size];
        let decompressed_len = decoder.decompress(data, &mut output).map_err(|e| {
            Error::compression_error(&format!("Snappy decompression failed: {}", e))
        })?;

        if decompressed_len != original_size {
            return Err(Error::compression_error(&format!(
                "Snappy decompression size mismatch: expected {}, got {}",
                original_size, decompressed_len
            )));
        }

        Ok(output)
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
        lz4::block::decompress(data, Some(original_size as i32))
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
}

impl std::fmt::Debug for CompressionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionContext")
            .field("algorithm", &self.algorithm)
            .field("threshold", &self.threshold)
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
        }
    }

    /// Check if data should be compressed based on size threshold
    pub fn should_compress(&self, data_len: usize) -> bool {
        data_len >= self.threshold && self.algorithm() != CompressionAlgorithm::None
    }

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> Result<Bytes> {
        let compressed = self.compressor.compress(data)?;
        Ok(Bytes::from(compressed))
    }

    /// Decompress data
    pub fn decompress(&self, data: &[u8], original_size: usize) -> Result<Bytes> {
        let decompressed = self.compressor.decompress(data, original_size)?;
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
}
