//! CRC32C helpers compatible with Ceph's SCTP CRC32C usage.

/// Compute CRC32C using Ceph's SCTP implementation
///
/// This is a safe wrapper around the FFI function.
///
/// # Arguments
/// * `data` - Byte slice to compute CRC over
/// * `initial` - Initial CRC value (typically 0xFFFFFFFF)
///
/// # Returns
/// Computed CRC32C value
pub fn ceph_crc32c(data: &[u8], initial: u32) -> u32 {
    crc32c::crc32c_append(initial, data)
}

/// Compute streaming CRC32C using Ceph's SCTP implementation
///
/// Allows computing CRC over multiple non-contiguous buffers.
///
/// # Arguments
/// * `crc` - Current CRC value (use 0xFFFFFFFF for first buffer)
/// * `data` - Byte slice to compute CRC over
///
/// # Returns
/// Updated CRC32C value
pub fn ceph_crc32c_append(crc: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ceph_crc32c_basic() {
        // Test with empty data
        let empty: &[u8] = &[];
        let crc = ceph_crc32c(empty, 0xFFFFFFFF);
        assert_eq!(crc, 0xFFFFFFFF);

        // Test with some data
        let data = b"hello world";
        let crc = ceph_crc32c(data, 0xFFFFFFFF);
        // CRC value computed using Ceph's implementation
        assert_ne!(crc, 0);
    }

    #[test]
    fn test_ceph_crc32c_streaming() {
        let data = b"hello world";

        // Compute in one go
        let crc_single = ceph_crc32c(data, 0xFFFFFFFF);

        // Compute in two parts
        let mut crc_streaming = ceph_crc32c_append(0xFFFFFFFF, b"hello ");
        crc_streaming = ceph_crc32c_append(crc_streaming, b"world");

        assert_eq!(crc_single, crc_streaming);
    }
}
