//! Public RADOS client library and internal protocol modules.
//!
//! This crate now contains the former `rados-denc`, `rados-auth`,
//! `rados-cephconfig`, `rados-crush`, `rados-msgr2`, `rados-monclient`, and
//! `rados-osdclient` packages as internal modules under one public crate.
//!
//! High-level object operations are re-exported at the crate root. Lower-level
//! protocol and support APIs remain available through submodules such as
//! [`denc`], [`auth`], [`msgr2`], [`monclient`], and [`osdclient`].

pub mod auth;
pub mod cephconfig;
mod client;
pub mod crush;
pub mod denc;
pub mod monclient;
pub mod msgr2;
pub mod osdclient;

pub use client::{Client, ClientBuilder, ClientError, connect};

pub use auth::{AuthResult, CephXClientHandler, CephXError, CephXServerHandler, Keyring};
pub use cephconfig::{
    CephConfig, ConfigError, ConfigOption, ConfigValue, Count, Duration, Ratio, RuntimeOptionValue,
    Size,
};
pub use denc::{
    CephFeatures, CodecError, Denc, EVersion, ElectionStrategy, EncodingMetadata, EntityAddr,
    EntityAddrType, EntityAddrvec, EntityName, EntityType, Epoch, FixedSize, FsId, GlobalId,
    HObject, HasEncodingMetadata, MonCephRelease, MonFeature, MonInfo, MonMap, OsdId, PoolId,
    RadosError, SIGNIFICANT_FEATURES, SNAP_DIR, SNAP_HEAD, StructVDenc, UTime, UuidD, Version,
    VersionedDenc, VersionedEncode, ZeroCopyDencode, encode_with_capacity, has_significant_feature,
};
// Submodule re-exports required for macro hygiene: ZeroCopyDencode derive generates
// `$crate::zero_copy::ZeroCopyDencode` and `$crate::zerocopy::{IntoBytes,FromBytes}`.
pub use denc::{zero_copy, zerocopy};
pub use monclient::{AuthConfig, MonClient, MonClientConfig, MonClientError, PoolOpResult};
pub use msgr2::{
    MapMessage, MapReceiver, MapSender, MessageThrottle, Msgr2Error, ThrottleConfig, map_channel,
};
pub use osdclient::{
    BuiltOp, IoCtx, LockFlags, LockRequest, LockType, OSDClient, OSDClientConfig, OSDClientError,
    OSDMap, OSDMapIncremental, ObjectId, ObjectLocator, ObjectstorePerfStat, OpBuilder, OpCode,
    OpState, OpTarget, OsdOpFlags, PgMergeMeta, PgNlsResponse, PgPool, PoolInfo, PoolSnapInfo,
    PoolStat, RadosObject, ReadResult, SnapId, SparseExtent, SparseReadResult, StatResult,
    StripedPgId, UnlockRequest, WriteResult, list_objects_stream,
};
