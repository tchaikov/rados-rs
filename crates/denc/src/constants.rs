//! Shared constants for Ceph protocol encoding/decoding
//!
//! This module centralizes magic numbers and constants used across multiple crates
//! to improve maintainability and reduce duplication.

/// CRUSH algorithm constants
pub mod crush {
    /// Fixed-point representation of 1.0 (16.16 format)
    /// Used for weight calculations in CRUSH bucket selection
    pub const FIXED_POINT_ONE: u32 = 0x10000;

    /// Mask for fixed-point fractional part
    pub const FIXED_POINT_MASK: u32 = 0xffff;

    /// Offset for natural log lookup table in CRUSH
    /// Used in straw2 bucket selection algorithm
    pub const LN_LOOKUP_OFFSET: i64 = 0x1000000000000i64;
}

/// Socket address constants
pub mod sockaddr {
    /// Size of sockaddr_storage structure
    pub const STORAGE_SIZE: usize = 128;

    /// Size of legacy entity_addr_t (marker + nonce + sockaddr_storage)
    pub const LEGACY_ENTITY_ADDR_SIZE: usize = 136;

    /// Address family: IPv4
    pub const AF_INET: u16 = 2;

    /// Address family: IPv6
    pub const AF_INET6: u16 = 10;
}

/// Pool type constants
pub mod pool {
    /// Replicated pool type
    pub const TYPE_REPLICATED: u8 = 1;

    /// Erasure-coded pool type
    pub const TYPE_ERASURE: u8 = 3;
}

/// OSD-related constants
pub mod osd {
    /// Scale factor for laggy probability (0xffffffff = 100%)
    pub const LAGGY_PROBABILITY_SCALE: u32 = 0xffffffff;
}
