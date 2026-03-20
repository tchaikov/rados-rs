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
pub mod crush;
pub mod denc;
pub mod monclient;
pub mod msgr2;
pub mod osdclient;

pub mod rados_denc {
    pub use crate::denc::*;
    pub use crate::denc::{
        codec, codec_error, constants, encoding_metadata, entity_addr, error, features, hobject,
        ids, monmap, padding, types, zero_copy,
    };
    pub use crate::{
        check_min_version, decode_if_version, impl_denc_for_versioned,
        mark_feature_dependent_encoding, mark_simple_encoding, mark_versioned_encoding,
    };
}

pub mod rados_auth {
    pub use crate::auth::*;
    pub use crate::auth::{client, error, keyring, protocol, provider, server, types};
}

pub mod rados_cephconfig {
    pub use crate::cephconfig::*;
    pub use crate::cephconfig::{config, types};
    pub use crate::{define_options, runtime_config_options};
}

pub mod rados_crush {
    pub use crate::crush::*;
    pub use crate::crush::{bucket, crush_ln_table, decode, error, hash, mapper, placement, types};
}

pub mod rados_msgr2 {
    pub use crate::impl_denc_ceph_message;
    pub use crate::msgr2::*;
    pub use crate::msgr2::{
        banner, ceph_message, compression, crypto, error, frames, header, io_loop, map_channel,
        message, priority_queue, protocol, revocation, split, state_machine, throttle,
    };
}

pub mod rados_monclient {
    pub use crate::monclient::*;
    pub use crate::monclient::{
        auth_config, client, defaults, dns_srv, error, messages, monmap, types,
    };
}

pub mod rados_osdclient {
    pub use crate::osdclient::*;
    pub use crate::osdclient::{
        client, denc_types, error, ioctx, list_stream, lock, messages, object_io, operation,
        osdmap, pg_nls_response, pgmap_types, session, snapshot, types,
    };
}

pub use auth::{AuthResult, CephXClientHandler, CephXError, CephXServerHandler, Keyring};
pub use cephconfig::{
    CephConfig, ConfigError, ConfigOption, ConfigValue, Count, Duration, Ratio, RuntimeOptionValue,
    Size,
};
pub use denc::{
    CephFeatures, CodecError, Denc, EVersion, ElectionStrategy, EncodingMetadata, EntityAddr,
    EntityAddrType, EntityAddrvec, EntityName, EntityType, Epoch, FixedSize, FsId, GlobalId,
    HObject, HasEncodingMetadata, MonCephRelease, MonFeature, MonInfo, MonMap, OsdId, Padding,
    PoolId, RadosError, SIGNIFICANT_FEATURES, SNAP_DIR, SNAP_HEAD, StructVDenc, UTime, UuidD,
    Version, VersionedDenc, VersionedEncode, ZeroCopyDencode, encode_with_capacity,
    get_significant_features, has_feature, has_significant_feature, zerocopy,
};
pub use denc::{
    codec, codec_error, constants, encoding_metadata, entity_addr, error, features, hobject, ids,
    monmap, padding, types, zero_copy,
};
pub use monclient::{AuthConfig, MonClient, MonClientConfig, MonClientError, PoolOpResult};
pub use msgr2::{
    MapMessage, MapReceiver, MapSender, MessageThrottle, Msgr2Error, ThrottleConfig, map_channel,
};
pub use osdclient::{
    BuiltOp, IoCtx, LockFlags, LockRequest, LockType, OSDClient, OSDClientConfig, OSDClientError,
    OSDMap, OSDMapIncremental, ObjectId, ObjectstorePerfStat, OpBuilder, OpCode, OpState, OpTarget,
    OsdOpFlags, PgMergeMeta, PgNlsResponse, PgPool, PoolInfo, PoolSnapInfo, PoolStat, RadosObject,
    ReadResult, SnapId, SparseExtent, SparseReadResult, StatResult, StripedPgId, UnlockRequest,
    WriteResult, list_objects_stream,
};
