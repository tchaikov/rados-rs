//! Strongly typed pool snapshot identifiers for public OSD client APIs.
//!
//! This module keeps snapshot-facing APIs explicit without changing the wire
//! format, which still uses raw `u64` values on the Ceph protocol boundary.

use std::fmt;

/// Strongly typed identifier for a pool snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct SnapId(u64);

impl SnapId {
    /// Create a snapshot identifier from its raw wire value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw Ceph snapshot identifier.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for SnapId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<SnapId> for u64 {
    fn from(value: SnapId) -> Self {
        value.0
    }
}

impl fmt::Display for SnapId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::SnapId;

    #[test]
    fn snap_id_roundtrips_through_u64() {
        let snap_id = SnapId::new(42);
        assert_eq!(snap_id.as_u64(), 42);
        assert_eq!(u64::from(snap_id), 42);
        assert_eq!(SnapId::from(42), snap_id);
    }

    #[test]
    fn snap_id_orders_by_raw_value() {
        assert!(SnapId::new(1) < SnapId::new(2));
    }
}
