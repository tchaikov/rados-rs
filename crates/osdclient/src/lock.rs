//! Object locking support via cls_lock
//!
//! Implements distributed object locking using Ceph's cls_lock object class.

use bytes::{BufMut, Bytes, BytesMut};
use denc::Denc;
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

impl LockRequest {
    /// Encode lock request to bytes
    pub fn encode(&self) -> Result<Bytes, denc::RadosError> {
        let mut buf = BytesMut::new();

        // Encode fields in order
        self.name.encode(&mut buf, 0)?;
        (self.lock_type as u8).encode(&mut buf, 0)?;
        self.cookie.encode(&mut buf, 0)?;
        self.tag.encode(&mut buf, 0)?;
        self.description.encode(&mut buf, 0)?;

        // Encode duration as UTime (sec + nsec)
        let secs = self.duration.as_secs() as u32;
        let nsecs = self.duration.subsec_nanos();
        secs.encode(&mut buf, 0)?;
        nsecs.encode(&mut buf, 0)?;

        self.flags.bits().encode(&mut buf, 0)?;

        Ok(buf.freeze())
    }
}

/// Unlock request structure
pub struct UnlockRequest {
    pub name: String,
    pub cookie: String,
}

impl UnlockRequest {
    /// Encode unlock request to bytes
    pub fn encode(&self) -> Result<Bytes, denc::RadosError> {
        let mut buf = BytesMut::new();
        self.name.encode(&mut buf, 0)?;
        self.cookie.encode(&mut buf, 0)?;
        Ok(buf.freeze())
    }
}
