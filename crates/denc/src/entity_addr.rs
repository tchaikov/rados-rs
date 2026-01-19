//! EntityAddr - Entity address type for Ceph
//!
//! This module re-exports the DencMut-based implementation for backward compatibility.

// Re-export the new DencMut-based implementation
pub use crate::entity_addr_dencmut::{EntityAddr, EntityAddrType};

// EntityAddrvec - keep old implementation for now (TODO: migrate to DencMut)
use serde::Serialize;

/// EntityAddrvec - a vector of EntityAddr (entity_addrvec_t in C++)
#[derive(Debug, Clone, Default, Serialize)]
pub struct EntityAddrvec {
    pub addrs: Vec<EntityAddr>,
}

impl EntityAddrvec {
    pub fn new() -> Self {
        Self { addrs: Vec::new() }
    }

    pub fn with_addr(addr: EntityAddr) -> Self {
        Self { addrs: vec![addr] }
    }
}
