//! Object locking support via cls_lock
//!
//! Implements distributed object locking using Ceph's cls_lock object class.

use bytes::{Buf, BufMut};
use denc::{Denc, RadosError};
use std::time::Duration;

/// Lock type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockType {
    None = 0,
    Exclusive = 1,
    Shared = 2,
    ExclusiveEphemeral = 3,
}

bitflags::bitflags! {
    /// Lock flags
    pub struct LockFlags: u8 {
        const MAY_RENEW = 0x01;
        const MUST_RENEW = 0x02;
    }
}

/// Lock request structure
pub struct LockRequest {
    pub name: String,
    pub lock_type: LockType,
    pub cookie: String,
    pub tag: String,
    pub description: String,
    pub duration: Duration,
    pub flags: LockFlags,
}

impl Denc for LockRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.name.encode(buf, features)?;
        (self.lock_type as u8).encode(buf, features)?;
        self.cookie.encode(buf, features)?;
        self.tag.encode(buf, features)?;
        self.description.encode(buf, features)?;
        (self.duration.as_secs() as u32).encode(buf, features)?;
        self.duration.subsec_nanos().encode(buf, features)?;
        self.flags.bits().encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(_buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        Err(RadosError::Protocol(
            "LockRequest decode is not supported".into(),
        ))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        None
    }
}

/// Unlock request structure
#[derive(denc::Denc)]
pub struct UnlockRequest {
    pub name: String,
    pub cookie: String,
}
