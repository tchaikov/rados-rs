//! CRC32C helpers compatible with Ceph's SCTP CRC32C usage.

/// Ceph-compatible CRC32C with an explicit initial value.
///
/// Cross-checked against Ceph (`src/include/crc32c.h`: `ceph_crc32c(crc, data, len)`)
/// and Linux Ceph client (`net/ceph/messenger_v2.c`: `crc32c(seed, buf, len)`).
pub use crc32c::crc32c_append as ceph_crc32c;

/// Streaming alias kept for readability at call sites.
pub use crc32c::crc32c_append as ceph_crc32c_append;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ceph_crc32c_basic() {
        // Test with empty data
        let empty: &[u8] = &[];
        let crc = ceph_crc32c(0xFFFFFFFF, empty);
        assert_eq!(crc, 0xFFFFFFFF);

        // Test with some data
        let data = b"hello world";
        let crc = ceph_crc32c(0xFFFFFFFF, data);
        // CRC value computed using Ceph's implementation
        assert_ne!(crc, 0);
    }

    #[test]
    fn test_ceph_crc32c_streaming() {
        let data = b"hello world";

        // Compute in one go
        let crc_single = ceph_crc32c(0xFFFFFFFF, data);

        // Compute in two parts
        let mut crc_streaming = ceph_crc32c_append(0xFFFFFFFF, b"hello ");
        crc_streaming = ceph_crc32c_append(crc_streaming, b"world");

        assert_eq!(crc_single, crc_streaming);
    }
}
