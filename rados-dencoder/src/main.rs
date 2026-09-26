//! Rust implementation of ceph-dencoder
//!
//! This tool can decode, encode, and inspect Ceph data structures from binary corpus files.
//!
//! Usage:
//!   dencoder type <typename> [command] [command] ...
//!   dencoder list_types
//!
//! Commands:
//!   type <name>        - Select type to work with
//!   import <file>      - Read binary file (use "-" for stdin)
//!   decode             - Decode binary data to object
//!   encode             - Encode object to binary
//!   dump_json          - Output object as JSON
//!   export <file>      - Write binary data to file
//!   hexdump            - Show hex representation of binary data
//!   set_features <hex> - Set feature flags (hex or decimal)
//!   get_features       - Show current feature flags
//!   list_types         - List all available types

use bytes::Bytes;
use rados::osdclient::osdmap::{OsdInfo, OsdXInfo, PgId};
use rados::osdclient::{OSDMap, ObjectLocator, ObjectstorePerfStat, PgMergeMeta, PgPool, PoolStat};
use rados::{
    Denc, EVersion, EntityAddr, HObject, ListWatchersReply, MonInfo, MonMap, PgNlsResponse,
    PoolSnapInfo, RadosError, UTime, UuidD, VersionedEncode, WatchItem,
};
use rados_cls::queue::{
    EnqueueOp as QueueEnqueueOp, Entry as QueueEntry, GetCapacityRet as QueueGetCapacityRet,
    Head as QueueHead, InitOp as QueueInitOp, ListOp as QueueListOp, ListRet as QueueListRet,
    Marker as QueueMarker, RemoveOp as QueueRemoveOp,
};
use rados_cls::refcount::{
    GetOp as RefcountGetOp, ObjRefcount, PutOp as RefcountPutOp, ReadOp as RefcountReadOp,
    ReadRet as RefcountReadRet, SetOp as RefcountSetOp,
};
use rados_cls::rgw::gc::{
    DeferEntryOp as RgwGcDeferEntryOp, ListOp as RgwGcListOp, ListRet as RgwGcListRet,
    RemoveOp as RgwGcRemoveOp, SetEntryOp as RgwGcSetEntryOp,
};
use rados_cls::rgw::index::{
    BiEntry as RgwBiEntry, BiLogEntry as RgwBiLogEntry,
    BucketInstanceEntry as RgwBucketInstanceEntry, CheckAttrsPrefixOp as RgwCheckAttrsPrefixOp,
    CheckIndexRet as RgwCheckIndexRet, ClearBucketReshardingOp as RgwClearBucketReshardingOp,
    CompleteOp as RgwCompleteOp, Dir as RgwDir, DirEntry as RgwDirEntry,
    DirEntryMeta as RgwDirEntryMeta, DirHeader as RgwDirHeader,
    GuardBucketReshardingOp as RgwGuardBucketReshardingOp, ListOp as RgwListOp,
    ListRet as RgwListRet, PrepareOp as RgwPrepareOp, RemoveObjOp as RgwRemoveObjOp,
    ReshardEntry as RgwReshardEntry, SetBucketReshardingOp as RgwSetBucketReshardingOp,
    StorePgVerOp as RgwStorePgVerOp, TagTimeoutOp as RgwTagTimeoutOp,
};
use rados_cls::rgw::lc::{
    GetEntryRet as RgwLcGetEntryRet, LcEntry as RgwLcEntry, LcObjHead as RgwLcObjHead,
    SetEntryOp as RgwLcSetEntryOp,
};
use rados_cls::rgw::olh::{
    ClearOlhOp as RgwClearOlhOp, LinkOlhOp as RgwLinkOlhOp, OlhEntry as RgwOlhEntry,
    OlhLogEntry as RgwOlhLogEntry, ReadOlhLogOp as RgwReadOlhLogOp,
    ReadOlhLogRet as RgwReadOlhLogRet, TrimOlhLogOp as RgwTrimOlhLogOp,
    UnlinkInstanceOp as RgwUnlinkInstanceOp,
};
use rados_cls::rgw::types::{
    CategoryStats as RgwCategoryStats, EntryVer as RgwEntryVer, GcObjInfo, Obj as RgwObj,
    ObjChain as RgwObjChain, ObjKey as RgwObjKey, PendingInfo as RgwPendingInfo,
    ZoneSet as RgwZoneSet,
};
use rados_cls::rgw::usage::{
    AddOp as RgwUsageLogAddOp, ReadOp as RgwUsageLogReadOp, ReadRet as RgwUsageLogReadRet,
    S3selectUsageData as RgwS3selectUsageData, TrimOp as RgwUsageLogTrimOp,
    UsageData as RgwUsageData, UsageLogEntry as RgwUsageLogEntry, UsageLogInfo as RgwUsageLogInfo,
    UserBucket as RgwUserBucket,
};
use rados_cls::rgw_gc::{InitOp as RgwGcQueueInitOp, UrgentData as RgwGcUrgentData};
use rados_cls::user::{
    AccountHeader as UserAccountHeader, AccountResource as UserAccountResource,
    AccountResourceAddOp as UserAccountResourceAddOp,
    AccountResourceGetOp as UserAccountResourceGetOp,
    AccountResourceGetRet as UserAccountResourceGetRet,
    AccountResourceListOp as UserAccountResourceListOp,
    AccountResourceListRet as UserAccountResourceListRet,
    AccountResourceRmOp as UserAccountResourceRmOp, Bucket as UserBucket,
    BucketEntry as UserBucketEntry, CompleteStatsSyncOp as UserCompleteStatsSyncOp,
    GetHeaderOp as UserGetHeaderOp, GetHeaderRet as UserGetHeaderRet, Header as UserHeader,
    ListBucketsOp as UserListBucketsOp, ListBucketsRet as UserListBucketsRet,
    RemoveBucketOp as UserRemoveBucketOp, SetBucketsOp as UserSetBucketsOp, Stats as UserStats,
};
use rados_cls::version::{
    CheckOp as VersionCheckOp, IncOp as VersionIncOp, ObjVersion, ReadRet as VersionReadRet,
    SetOp as VersionSetOp,
};
use serde::Serialize;
use std::any::Any;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::process;

/// Type-erased serializable object supporting JSON output and downcasting.
trait SerializableType: Any {
    fn to_json(&self) -> std::result::Result<serde_json::Value, serde_json::Error>;
    fn as_any(&self) -> &dyn Any;
}

impl<T: Serialize + 'static> SerializableType for T {
    fn to_json(&self) -> std::result::Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct TypeInfo {
    decode_fn: fn(&mut Bytes, u64) -> Result<Box<dyn SerializableType>>,
    encode_fn: fn(&dyn SerializableType, u64) -> Result<Bytes>,
}

fn decode_denc<T>(bytes: &mut Bytes, features: u64) -> Result<Box<dyn SerializableType>>
where
    T: Denc + Serialize + 'static,
{
    let obj = T::decode(bytes, features)?;
    Ok(Box::new(obj))
}

fn decode_versioned<T>(bytes: &mut Bytes, features: u64) -> Result<Box<dyn SerializableType>>
where
    T: VersionedEncode + Serialize + 'static,
{
    let obj = T::decode_versioned(bytes, features)?;
    Ok(Box::new(obj))
}

fn downcast_and_encode<T>(
    obj: &dyn SerializableType,
    features: u64,
    encode: fn(&T, &mut bytes::BytesMut, u64) -> std::result::Result<(), RadosError>,
) -> Result<Bytes>
where
    T: 'static,
{
    let typed = obj
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| RadosError::Protocol("Type mismatch".to_string()))?;
    let mut buf = bytes::BytesMut::with_capacity(256);
    encode(typed, &mut buf, features)?;
    Ok(buf.freeze())
}

fn encode_denc<T>(obj: &dyn SerializableType, features: u64) -> Result<Bytes>
where
    T: Denc + 'static,
{
    downcast_and_encode::<T>(obj, features, T::encode)
}

fn encode_versioned<T>(obj: &dyn SerializableType, features: u64) -> Result<Bytes>
where
    T: VersionedEncode + 'static,
{
    downcast_and_encode::<T>(obj, features, T::encode_versioned)
}

fn type_info_denc<T>() -> TypeInfo
where
    T: Denc + Serialize + 'static,
{
    TypeInfo {
        decode_fn: decode_denc::<T>,
        encode_fn: encode_denc::<T>,
    }
}

fn type_info_versioned<T>() -> TypeInfo
where
    T: VersionedEncode + Serialize + 'static,
{
    TypeInfo {
        decode_fn: decode_versioned::<T>,
        encode_fn: encode_versioned::<T>,
    }
}

fn get_type_info(name: &str) -> Option<TypeInfo> {
    match name {
        // Level 1: Primitive types
        "pg_t" | "pg_id" => Some(type_info_denc::<PgId>()),
        "eversion_t" => Some(type_info_denc::<EVersion>()),
        "utime_t" => Some(type_info_denc::<UTime>()),
        "uuid_d" => Some(type_info_denc::<UuidD>()),
        "osd_info_t" => Some(type_info_denc::<OsdInfo>()),

        // Level 2: Types depending on Level 1
        "entity_addr_t" => Some(type_info_denc::<EntityAddr>()),
        "pool_snap_info_t" => Some(type_info_versioned::<PoolSnapInfo>()),
        "osd_xinfo_t" => Some(type_info_denc::<OsdXInfo>()),

        // Level 3: Complex types
        "pg_merge_meta_t" => Some(type_info_denc::<PgMergeMeta>()),
        "object_locator_t" => Some(type_info_denc::<ObjectLocator>()),
        "objectstore_perf_stat_t" => Some(type_info_denc::<ObjectstorePerfStat>()),
        "pool_stat_t" => Some(type_info_denc::<PoolStat>()),
        "hobject_t" => Some(type_info_denc::<HObject>()),
        "pg_nls_response_t" => Some(type_info_denc::<PgNlsResponse>()),
        "pg_pool_t" => Some(type_info_denc::<PgPool>()),
        "watch_item_t" => Some(type_info_denc::<WatchItem>()),
        "obj_list_watch_response_t" => Some(type_info_denc::<ListWatchersReply>()),

        // Object classes (rados-cls)
        "obj_version" => Some(type_info_denc::<ObjVersion>()),
        "cls_version_set_op" => Some(type_info_denc::<VersionSetOp>()),
        "cls_version_inc_op" => Some(type_info_denc::<VersionIncOp>()),
        "cls_version_check_op" => Some(type_info_denc::<VersionCheckOp>()),
        "cls_version_read_ret" => Some(type_info_denc::<VersionReadRet>()),
        "cls_refcount_get_op" => Some(type_info_denc::<RefcountGetOp>()),
        "cls_refcount_put_op" => Some(type_info_denc::<RefcountPutOp>()),
        "cls_refcount_set_op" => Some(type_info_denc::<RefcountSetOp>()),
        "cls_refcount_read_op" => Some(type_info_denc::<RefcountReadOp>()),
        "cls_refcount_read_ret" => Some(type_info_denc::<RefcountReadRet>()),
        "obj_refcount" => Some(type_info_denc::<ObjRefcount>()),
        "cls_user_bucket" => Some(type_info_denc::<UserBucket>()),
        "cls_user_bucket_entry" => Some(type_info_denc::<UserBucketEntry>()),
        "cls_user_stats" => Some(type_info_denc::<UserStats>()),
        "cls_user_header" => Some(type_info_denc::<UserHeader>()),
        "cls_user_account_header" => Some(type_info_denc::<UserAccountHeader>()),
        "cls_user_account_resource" => Some(type_info_denc::<UserAccountResource>()),
        "cls_user_set_buckets_op" => Some(type_info_denc::<UserSetBucketsOp>()),
        "cls_user_remove_bucket_op" => Some(type_info_denc::<UserRemoveBucketOp>()),
        "cls_user_list_buckets_op" => Some(type_info_denc::<UserListBucketsOp>()),
        "cls_user_list_buckets_ret" => Some(type_info_denc::<UserListBucketsRet>()),
        "cls_user_get_header_op" => Some(type_info_denc::<UserGetHeaderOp>()),
        "cls_user_get_header_ret" => Some(type_info_denc::<UserGetHeaderRet>()),
        "cls_user_complete_stats_sync_op" => Some(type_info_denc::<UserCompleteStatsSyncOp>()),
        "cls_user_account_resource_add_op" => Some(type_info_denc::<UserAccountResourceAddOp>()),
        "cls_user_account_resource_get_op" => Some(type_info_denc::<UserAccountResourceGetOp>()),
        "cls_user_account_resource_get_ret" => Some(type_info_denc::<UserAccountResourceGetRet>()),
        "cls_user_account_resource_rm_op" => Some(type_info_denc::<UserAccountResourceRmOp>()),
        "cls_user_account_resource_list_op" => Some(type_info_denc::<UserAccountResourceListOp>()),
        "cls_user_account_resource_list_ret" => {
            Some(type_info_denc::<UserAccountResourceListRet>())
        }
        "cls_queue_entry" => Some(type_info_denc::<QueueEntry>()),
        "cls_queue_marker" => Some(type_info_denc::<QueueMarker>()),
        "cls_queue_head" => Some(type_info_denc::<QueueHead>()),
        "cls_queue_init_op" => Some(type_info_denc::<QueueInitOp>()),
        "cls_queue_enqueue_op" => Some(type_info_denc::<QueueEnqueueOp>()),
        "cls_queue_list_op" => Some(type_info_denc::<QueueListOp>()),
        "cls_queue_list_ret" => Some(type_info_denc::<QueueListRet>()),
        "cls_queue_remove_op" => Some(type_info_denc::<QueueRemoveOp>()),
        "cls_queue_get_capacity_ret" => Some(type_info_denc::<QueueGetCapacityRet>()),
        "cls_rgw_obj_key" => Some(type_info_denc::<RgwObjKey>()),
        "cls_rgw_obj" => Some(type_info_denc::<RgwObj>()),
        "cls_rgw_obj_chain" => Some(type_info_denc::<RgwObjChain>()),
        "cls_rgw_gc_obj_info" => Some(type_info_denc::<GcObjInfo>()),
        "rgw_bucket_entry_ver" => Some(type_info_denc::<RgwEntryVer>()),
        "rgw_bucket_pending_info" => Some(type_info_denc::<RgwPendingInfo>()),
        "rgw_bucket_category_stats" => Some(type_info_denc::<RgwCategoryStats>()),
        "rgw_zone_set" => Some(type_info_denc::<RgwZoneSet>()),
        "rgw_bucket_dir_entry_meta" => Some(type_info_denc::<RgwDirEntryMeta>()),
        "rgw_bucket_dir_entry" => Some(type_info_denc::<RgwDirEntry>()),
        "rgw_bucket_dir_header" => Some(type_info_denc::<RgwDirHeader>()),
        "rgw_bucket_dir" => Some(type_info_denc::<RgwDir>()),
        "rgw_bi_log_entry" => Some(type_info_denc::<RgwBiLogEntry>()),
        "cls_rgw_bucket_instance_entry" => Some(type_info_denc::<RgwBucketInstanceEntry>()),
        "cls_rgw_reshard_entry" => Some(type_info_denc::<RgwReshardEntry>()),
        "rgw_bucket_olh_log_entry" => Some(type_info_denc::<RgwOlhLogEntry>()),
        "rgw_bucket_olh_entry" => Some(type_info_denc::<RgwOlhEntry>()),
        "rgw_cls_bi_entry" => Some(type_info_denc::<RgwBiEntry>()),
        "rgw_usage_data" => Some(type_info_denc::<RgwUsageData>()),
        "rgw_s3select_usage_data" => Some(type_info_denc::<RgwS3selectUsageData>()),
        "rgw_usage_log_entry" => Some(type_info_denc::<RgwUsageLogEntry>()),
        "rgw_usage_log_info" => Some(type_info_denc::<RgwUsageLogInfo>()),
        "rgw_user_bucket" => Some(type_info_denc::<RgwUserBucket>()),
        "rgw_cls_usage_log_add_op" => Some(type_info_denc::<RgwUsageLogAddOp>()),
        "rgw_cls_usage_log_read_op" => Some(type_info_denc::<RgwUsageLogReadOp>()),
        "rgw_cls_usage_log_read_ret" => Some(type_info_denc::<RgwUsageLogReadRet>()),
        "rgw_cls_usage_log_trim_op" => Some(type_info_denc::<RgwUsageLogTrimOp>()),
        "cls_rgw_lc_entry" => Some(type_info_denc::<RgwLcEntry>()),
        "cls_rgw_lc_obj_head" => Some(type_info_denc::<RgwLcObjHead>()),
        "cls_rgw_lc_get_entry_ret" => Some(type_info_denc::<RgwLcGetEntryRet>()),
        "cls_rgw_lc_set_entry_op" => Some(type_info_denc::<RgwLcSetEntryOp>()),
        "cls_rgw_gc_set_entry_op" => Some(type_info_denc::<RgwGcSetEntryOp>()),
        "cls_rgw_gc_defer_entry_op" => Some(type_info_denc::<RgwGcDeferEntryOp>()),
        "cls_rgw_gc_list_op" => Some(type_info_denc::<RgwGcListOp>()),
        "cls_rgw_gc_list_ret" => Some(type_info_denc::<RgwGcListRet>()),
        "cls_rgw_gc_remove_op" => Some(type_info_denc::<RgwGcRemoveOp>()),
        "cls_rgw_gc_urgent_data" => Some(type_info_denc::<RgwGcUrgentData>()),
        "cls_rgw_gc_queue_init_op" => Some(type_info_denc::<RgwGcQueueInitOp>()),
        "rgw_cls_tag_timeout_op" => Some(type_info_denc::<RgwTagTimeoutOp>()),
        "rgw_cls_obj_prepare_op" => Some(type_info_denc::<RgwPrepareOp>()),
        "rgw_cls_obj_complete_op" => Some(type_info_denc::<RgwCompleteOp>()),
        "rgw_cls_list_op" => Some(type_info_denc::<RgwListOp>()),
        "rgw_cls_list_ret" => Some(type_info_denc::<RgwListRet>()),
        "rgw_cls_check_index_ret" => Some(type_info_denc::<RgwCheckIndexRet>()),
        "rgw_cls_obj_remove_op" => Some(type_info_denc::<RgwRemoveObjOp>()),
        "rgw_cls_obj_store_pg_ver_op" => Some(type_info_denc::<RgwStorePgVerOp>()),
        "rgw_cls_obj_check_attrs_prefix" => Some(type_info_denc::<RgwCheckAttrsPrefixOp>()),
        "cls_rgw_set_bucket_resharding_op" => Some(type_info_denc::<RgwSetBucketReshardingOp>()),
        "cls_rgw_clear_bucket_resharding_op" => {
            Some(type_info_denc::<RgwClearBucketReshardingOp>())
        }
        "cls_rgw_guard_bucket_resharding_op" => {
            Some(type_info_denc::<RgwGuardBucketReshardingOp>())
        }
        "rgw_cls_link_olh_op" => Some(type_info_denc::<RgwLinkOlhOp>()),
        "rgw_cls_unlink_instance_op" => Some(type_info_denc::<RgwUnlinkInstanceOp>()),
        "rgw_cls_read_olh_log_op" => Some(type_info_denc::<RgwReadOlhLogOp>()),
        "rgw_cls_read_olh_log_ret" => Some(type_info_denc::<RgwReadOlhLogRet>()),
        "rgw_cls_trim_olh_log_op" => Some(type_info_denc::<RgwTrimOlhLogOp>()),
        "rgw_cls_bucket_clear_olh_op" => Some(type_info_denc::<RgwClearOlhOp>()),

        // Level 4: Top-level cluster structures
        "OSDMap" => Some(type_info_versioned::<OSDMap>()),
        "mon_info_t" => Some(type_info_denc::<MonInfo>()),
        "MonMap" => Some(type_info_denc::<MonMap>()),

        _ => None,
    }
}

fn list_types() {
    println!("Available types (ordered by dependency level):");
    println!();
    println!("LEVEL 1: Primitive Types (no Denc dependencies)");
    println!("  Test these FIRST - they are the foundation");
    println!("  pg_t              - Placement group ID [simple]");
    println!("  eversion_t        - Event version [simple]");
    println!("  utime_t           - Unix timestamp [simple]");
    println!("  uuid_d            - UUID [simple]");
    println!("  osd_info_t        - OSD information [simple]");
    println!();
    println!("LEVEL 2: Types depending on Level 1");
    println!("  Test these ONLY after Level 1 is 100% validated");
    println!(
        "  entity_addr_t     - Entity address [versioned, modern encode with legacy decode compatibility]"
    );
    println!("  pool_snap_info_t  - Pool snapshot info [versioned]");
    println!("  osd_xinfo_t       - Extended OSD info [versioned, Quincy+ encode contract]");
    println!();
    println!("LEVEL 3: Complex types");
    println!("  Test these ONLY after Level 1 & 2 are validated");
    println!("  pg_merge_meta_t   - PG merge metadata [versioned]");
    println!("  object_locator_t  - Object placement information [versioned]");
    println!(
        "  objectstore_perf_stat_t - Objectstore latency stats [versioned, Quincy+ encode contract]"
    );
    println!("  pool_stat_t       - Aggregate per-pool stats [versioned, Quincy+ encode contract]");
    println!("  hobject_t         - Hashed object identifier [versioned]");
    println!("  pg_nls_response_t - PG namespace list response [versioned]");
    println!("  pg_pool_t         - Pool configuration [versioned, feature-dependent: multiple]");
    println!("  watch_item_t      - One watcher of an object [versioned]");
    println!("  obj_list_watch_response_t - LIST_WATCHERS reply [versioned]");
    println!();
    println!("OBJECT CLASSES (rados-cls)");
    println!("  obj_version       - The version class's (ver, tag) pair [versioned]");
    println!(
        "  cls_version_set_op / cls_version_inc_op / cls_version_check_op / cls_version_read_ret [versioned]"
    );
    println!(
        "  cls_refcount_{{get,put,set,read}}_op / cls_refcount_read_ret / obj_refcount [versioned]"
    );
    println!(
        "  cls_user_{{bucket,bucket_entry,stats,header,account_header,account_resource}} [versioned]"
    );
    println!(
        "  cls_user_{{set_buckets,remove_bucket,list_buckets,get_header,complete_stats_sync}}_op \
         / cls_user_{{list_buckets,get_header}}_ret [versioned]"
    );
    println!(
        "  cls_user_account_resource_{{add,get,rm,list}}_op / \
         cls_user_account_resource_{{get,list}}_ret [versioned]"
    );
    println!("  cls_queue_{{entry,marker,head}} [versioned]");
    println!(
        "  cls_queue_{{init,enqueue,list,remove}}_op / cls_queue_list_ret / \
         cls_queue_get_capacity_ret [versioned]"
    );
    println!("  cls_rgw_{{obj_key,obj,obj_chain,gc_obj_info}} [versioned]");
    println!("  rgw_bucket_{{entry_ver,pending_info,category_stats}} / rgw_zone_set [versioned]");
    println!(
        "  rgw_bucket_dir_{{entry_meta,entry,header}} / rgw_bucket_dir / rgw_bi_log_entry \
         [versioned]"
    );
    println!("  cls_rgw_{{bucket_instance_entry,reshard_entry}} [versioned]");
    println!("  rgw_bucket_olh_{{log_entry,entry}} / rgw_cls_bi_entry [versioned]");
    println!(
        "  rgw_usage_{{data,log_entry,log_info}} / rgw_s3select_usage_data / \
         rgw_user_bucket [versioned]"
    );
    println!("  rgw_cls_usage_log_{{add,read,trim}}_op / rgw_cls_usage_log_read_ret [versioned]");
    println!("  cls_rgw_lc_{{entry,obj_head,get_entry_ret,set_entry_op}} [versioned]");
    println!(
        "  cls_rgw_gc_{{set_entry,defer_entry,list,remove}}_op / cls_rgw_gc_list_ret [versioned]"
    );
    println!("  cls_rgw_gc_urgent_data / cls_rgw_gc_queue_init_op [versioned]");
    println!(
        "  rgw_cls_{{tag_timeout,obj_prepare,obj_complete,list,obj_remove,obj_store_pg_ver}}_op \
         / rgw_cls_{{list,check_index}}_ret / rgw_cls_obj_check_attrs_prefix [versioned]"
    );
    println!("  cls_rgw_{{set,clear,guard}}_bucket_resharding_op [versioned]");
    println!(
        "  rgw_cls_{{link_olh,unlink_instance,read_olh_log,trim_olh_log,bucket_clear_olh}}_op \
         / rgw_cls_read_olh_log_ret [versioned]"
    );
    println!();
    println!("LEVEL 4: Top-level cluster structures");
    println!("  Test these ONLY after all lower levels are validated");
    println!("  OSDMap            - OSD cluster map [versioned, feature-dependent: multiple]");
    println!(
        "  mon_info_t        - Monitor information [versioned, feature-dependent: SERVER_NAUTILUS]"
    );
    println!(
        "  MonMap            - Monitor cluster map [versioned, feature-dependent: MONENC, SERVER_NAUTILUS]"
    );
    println!();
    println!("Encoding Properties:");
    println!("  [simple]              - No versioning, no feature dependency");
    println!("  [versioned]           - Uses ENCODE_START/DECODE_START");
    println!("  [feature-dependent]   - Encoding changes based on feature flags");
    println!();
    println!("CRITICAL: All decodes must show '0 bytes remaining' for validation");
    println!("Testing order: Level 1 → Level 2 → Level 3");
}

struct DencoderState {
    current_type: Option<TypeInfo>,
    features: u64,
    raw_data: Option<Bytes>,
    decoded: Option<Box<dyn SerializableType>>,
}

impl DencoderState {
    fn new() -> Self {
        Self {
            current_type: None,
            features: 0,
            raw_data: None,
            decoded: None,
        }
    }
}

#[derive(Debug)]
enum DencoderError {
    NoTypeSelected,
    UnknownType(String),
    NoDataImported,
    NothingDecoded,
    Rados(RadosError),
    Io(io::Error),
    InvalidFeatures(String),
    Json(serde_json::Error),
    MissingArgument(String),
    UnknownCommand(String),
}

impl fmt::Display for DencoderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NoTypeSelected => {
                write!(f, "Error: No type selected. Use 'type <typename>' first.")
            }
            Self::UnknownType(t) => write!(
                f,
                "Error: Unknown type '{t}'. Use 'list_types' to see available types."
            ),
            Self::NoDataImported => {
                write!(f, "Error: No data loaded. Use 'import <file>' first.")
            }
            Self::NothingDecoded => {
                write!(f, "Error: Nothing decoded yet. Use 'decode' first.")
            }
            Self::Rados(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::InvalidFeatures(s) => write!(f, "Invalid features: {s}"),
            Self::Json(e) => write!(f, "JSON error: {e}"),
            Self::MissingArgument(cmd) => write!(f, "Error: Missing argument for '{cmd}'"),
            Self::UnknownCommand(cmd) => write!(
                f,
                "Error: Unknown command '{cmd}'. Use 'list_types' to see available commands."
            ),
        }
    }
}

impl From<io::Error> for DencoderError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<RadosError> for DencoderError {
    fn from(e: RadosError) -> Self {
        Self::Rados(e)
    }
}

impl From<serde_json::Error> for DencoderError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

type Result<T> = std::result::Result<T, DencoderError>;

fn cmd_type(state: &mut DencoderState, typename: &str) -> Result<()> {
    let type_info =
        get_type_info(typename).ok_or_else(|| DencoderError::UnknownType(typename.to_string()))?;

    state.current_type = Some(type_info);
    println!("Selected type: {typename}");
    Ok(())
}

fn cmd_import(state: &mut DencoderState, filename: &str) -> Result<()> {
    let data = if filename == "-" {
        let mut buffer = Vec::new();
        io::stdin().read_to_end(&mut buffer)?;
        buffer
    } else {
        fs::read(filename)?
    };

    println!("Imported {} bytes from {}", data.len(), filename);
    state.raw_data = Some(Bytes::from(data));
    Ok(())
}

fn cmd_decode(state: &mut DencoderState) -> Result<()> {
    let type_info = state
        .current_type
        .as_ref()
        .ok_or(DencoderError::NoTypeSelected)?;

    let mut data = state
        .raw_data
        .clone()
        .ok_or(DencoderError::NoDataImported)?;

    let original_len = data.len();
    state.decoded = Some((type_info.decode_fn)(&mut data, state.features)?);

    let consumed = original_len - data.len();
    println!(
        "Decoded successfully ({} bytes consumed, {} bytes remaining)",
        consumed,
        data.len()
    );

    Ok(())
}

fn cmd_encode(state: &mut DencoderState) -> Result<()> {
    let type_info = state
        .current_type
        .as_ref()
        .ok_or(DencoderError::NoTypeSelected)?;

    let decoded = state
        .decoded
        .as_ref()
        .ok_or(DencoderError::NothingDecoded)?;

    let encoded = (type_info.encode_fn)(decoded.as_ref(), state.features)?;
    println!("Encoded successfully ({} bytes)", encoded.len());
    state.raw_data = Some(encoded);
    Ok(())
}

fn cmd_dump_json(state: &DencoderState) -> Result<()> {
    let decoded = state
        .decoded
        .as_ref()
        .ok_or(DencoderError::NothingDecoded)?;

    let json = decoded.to_json()?;

    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}

fn cmd_export(state: &DencoderState, filename: &str) -> Result<()> {
    let data = state
        .raw_data
        .as_ref()
        .ok_or(DencoderError::NoDataImported)?;

    fs::write(filename, data)?;
    println!("Exported {} bytes to {}", data.len(), filename);
    Ok(())
}

fn cmd_hexdump(state: &DencoderState) -> Result<()> {
    let data = state
        .raw_data
        .as_ref()
        .ok_or(DencoderError::NoDataImported)?;

    println!("Hex dump ({} bytes):", data.len());
    for (i, chunk) in data.chunks(16).enumerate() {
        print!("{:08x}  ", i * 16);

        // Hex bytes
        for (j, byte) in chunk.iter().enumerate() {
            if j == 8 {
                print!(" ");
            }
            print!("{byte:02x} ");
        }

        // Padding
        for _ in chunk.len()..16 {
            print!("   ");
        }
        if chunk.len() <= 8 {
            print!(" ");
        }

        // ASCII representation
        print!(" |");
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }

    Ok(())
}

fn cmd_set_features(state: &mut DencoderState, features_str: &str) -> Result<()> {
    let features = if features_str.starts_with("0x") || features_str.starts_with("0X") {
        u64::from_str_radix(&features_str[2..], 16)
    } else {
        features_str.parse::<u64>()
    }
    .map_err(|e| DencoderError::InvalidFeatures(format!("{features_str}: {e}")))?;

    state.features = features;
    println!("Set features to 0x{features:x}");
    Ok(())
}

fn cmd_get_features(state: &DencoderState) -> Result<()> {
    println!(
        "Current features: 0x{:x} ({})",
        state.features, state.features
    );
    Ok(())
}

fn require_arg<'a>(args: &'a [String], cmd: &str) -> Result<&'a str> {
    args.first()
        .map(String::as_str)
        .ok_or_else(|| DencoderError::MissingArgument(cmd.to_string()))
}

fn process_command(state: &mut DencoderState, cmd: &str, args: &[String]) -> Result<()> {
    match cmd {
        "type" => cmd_type(state, require_arg(args, cmd)?),
        "import" => cmd_import(state, require_arg(args, cmd)?),
        "decode" => cmd_decode(state),
        "encode" => cmd_encode(state),
        "dump_json" => cmd_dump_json(state),
        "export" => cmd_export(state, require_arg(args, cmd)?),
        "hexdump" => cmd_hexdump(state),
        "set_features" => cmd_set_features(state, require_arg(args, cmd)?),
        "get_features" => cmd_get_features(state),
        "list_types" => {
            list_types();
            Ok(())
        }
        _ => {
            print_usage();
            Err(DencoderError::UnknownCommand(cmd.to_string()))
        }
    }
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  dencoder type <typename> [command] [command] ...");
    eprintln!("  dencoder list_types");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  type <name>        - Select type to work with");
    eprintln!("  import <file>      - Read binary file (use \"-\" for stdin)");
    eprintln!("  decode             - Decode binary data to object");
    eprintln!("  encode             - Encode object to binary");
    eprintln!("  dump_json          - Output object as JSON");
    eprintln!("  export <file>      - Write binary data to file");
    eprintln!("  hexdump            - Show hex representation of binary data");
    eprintln!("  set_features <hex> - Set feature flags (hex or decimal)");
    eprintln!("  get_features       - Show current feature flags");
    eprintln!("  list_types         - List all available types");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  dencoder type pg_pool_t import pool.bin decode dump_json");
    eprintln!(
        "  dencoder type entity_addr_t set_features 0x40000000000000 import addr.bin decode dump_json"
    );
    eprintln!("  dencoder list_types");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        process::exit(1);
    }

    // Special case: list_types can be called without other commands
    if args[1] == "list_types" {
        list_types();
        return;
    }

    let mut state = DencoderState::new();
    let mut i = 1;

    while i < args.len() {
        let cmd = &args[i];

        // Collect arguments for this command
        let mut cmd_args = Vec::new();
        let mut j = i + 1;

        // Determine how many arguments this command needs
        let arg_count = match cmd.as_str() {
            "type" | "import" | "export" | "set_features" => 1,
            _ => 0,
        };

        // Collect the arguments
        for _ in 0..arg_count {
            if j < args.len() {
                cmd_args.push(args[j].clone());
                j += 1;
            }
        }

        // Process the command
        if let Err(e) = process_command(&mut state, cmd, &cmd_args) {
            eprintln!("{e}");
            process::exit(1);
        }

        i = j;
    }
}
