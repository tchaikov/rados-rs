/// Pure Rust implementation of Ceph's CRC32C
///
/// Ceph uses the SCTP/iSCSI variant of CRC32C (RFC 3720) with the Castagnoli polynomial.
/// The Rust `crc32c` crate implements the correct polynomial (0x1EDC6F41).
///
/// Note: The crc32c crate uses 0 as the initial value internally, but Ceph's API
/// uses 0xFFFFFFFF. We translate between these conventions.

use crc32c::crc32c_append as crc32c_append_internal;

/// Compute CRC32C using Ceph's initialization convention
///
/// # Arguments
/// * `data` - Byte slice to compute CRC over
/// * `initial` - Initial CRC value (Ceph uses 0xFFFFFFFF, we translate to 0 for crc32c crate)
///
/// # Returns
/// Computed CRC32C value
pub fn ceph_crc32c(data: &[u8], initial: u32) -> u32 {
    // Ceph uses 0xFFFFFFFF as initial value, but crc32c crate uses 0
    // Translate: if initial is 0xFFFFFFFF (Ceph's default), use 0 for crc32c crate
    let crc_initial = if initial == 0xFFFFFFFF { 0 } else { initial };
    crc32c_append_internal(crc_initial, data)
}

/// Compute streaming CRC32C
///
/// Allows computing CRC over multiple non-contiguous buffers.
///
/// # Arguments
/// * `crc` - Current CRC value (use 0xFFFFFFFF for first buffer in Ceph convention)
/// * `data` - Byte slice to compute CRC over
///
/// # Returns
/// Updated CRC32C value
pub fn ceph_crc32c_append(crc: u32, data: &[u8]) -> u32 {
    // Translate Ceph's 0xFFFFFFFF initial value to crc32c crate's 0
    let crc_value = if crc == 0xFFFFFFFF { 0 } else { crc };
    crc32c_append_internal(crc_value, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ceph_crc32c_basic() {
        // Test with empty data - when translating 0xFFFFFFFF to 0, empty data returns 0
        let empty: &[u8] = &[];
        let crc = ceph_crc32c(empty, 0xFFFFFFFF);
        assert_eq!(crc, 0); // crc32c crate returns 0 for empty data with init 0

        // Test with some data
        let data = b"hello world";
        let crc = ceph_crc32c(data, 0xFFFFFFFF);
        // CRC32C of "hello world"
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

    #[test]
    fn test_ceph_crc32c_known_values() {
        // Test vectors from RFC 3720 (iSCSI/SCTP CRC32C)
        // The crc32c crate uses 0 as initial value, so we translate

        // Single byte 0x00 with Ceph's init (0xFFFFFFFF -> 0)
        assert_eq!(ceph_crc32c(&[0x00], 0xFFFFFFFF), 0x527d5351);

        // Multiple bytes - "123456789"
        let test_data = b"123456789";
        let crc = ceph_crc32c(test_data, 0xFFFFFFFF);
        // Standard CRC32C of "123456789" is 0xe3069283
        assert_eq!(crc, 0xe3069283);
    }
}
