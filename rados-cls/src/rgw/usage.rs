//! The usage log: from `cls_rgw_types.h`, the entry and its parts; from
//! `cls_rgw_ops.h`, the request and reply structs of the `rgw` class's
//! `user_usage_log_add`, `user_usage_log_read`, `user_usage_log_trim`
//! and `usage_log_clear` methods; and, as `cls_rgw_client.h` has them,
//! those methods' op constructors and async calls.
//!
//! Server facts (Ceph v19 `cls_rgw.cc`): a usage-log shard keeps each
//! entry twice in its omap, under the by-time key
//! `"%011llu_<user>_<bucket>"` of its epoch and the by-user key
//! `"<user>_%011llu_<bucket>"`, `<user>` being the payer when set and the
//! owner otherwise. [`add`] folds each entry into the one already stored
//! under its by-time key and writes both keys. [`read`] walks the by-time
//! keys from `start_epoch` to `end_epoch` when `user` is empty and that
//! user's by-user keys otherwise, skips entries of other buckets when
//! `bucket` is set, and merges every hour of a (user, bucket) pair into
//! one value. [`trim`] scans a range a thousand omap keys per call and
//! removes both keys of each entry found; it derives those keys from the
//! owner, so an entry stored under a payer is found but never removed,
//! and a trim over it never reaches `ENODATA` (see [`trim`]).

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut, Bytes};
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, OSDClientError, RadosError, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::{SerializeSeq, SerializeStruct};

use super::CLASS;
use crate::call;

/// `rgw_usage_data`: one bucket's traffic and request counts for a
/// category.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct UsageData {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub ops: u64,
    pub successful_ops: u64,
}

impl UsageData {
    pub fn aggregate(&mut self, other: &Self) {
        self.bytes_sent = self.bytes_sent.wrapping_add(other.bytes_sent);
        self.bytes_received = self.bytes_received.wrapping_add(other.bytes_received);
        self.ops = self.ops.wrapping_add(other.ops);
        self.successful_ops = self.successful_ops.wrapping_add(other.successful_ops);
    }
}

/// `rgw_s3select_usage_data`: bytes an S3 Select query scanned and
/// returned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct S3selectUsageData {
    pub bytes_processed: u64,
    pub bytes_returned: u64,
}

impl S3selectUsageData {
    pub fn aggregate(&mut self, other: &Self) {
        self.bytes_processed = self.bytes_processed.wrapping_add(other.bytes_processed);
        self.bytes_returned = self.bytes_returned.wrapping_add(other.bytes_returned);
    }
}

/// `rgw_usage_log_entry`: usage totals for one owner/bucket/epoch. The
/// wire writes `total_usage`'s four fields inline with no struct header,
/// `usage_map` before `payer`, and the `s3select_usage` block only from
/// version 4 (Squid v19.2.2); version 3, which the 19.2.0 corpus holds,
/// has no such block and decodes to zero counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageLogEntry {
    pub owner: String,
    pub payer: String,
    pub bucket: String,
    pub epoch: u64,
    pub total_usage: UsageData,
    pub usage_map: BTreeMap<String, UsageData>,
    pub s3select_usage: S3selectUsageData,
}

impl VersionedEncode for UsageLogEntry {
    const MAX_DECODE_VERSION: u8 = 4;

    fn encoding_version(&self, _features: u64) -> u8 {
        4
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.owner.encode(buf, features)?;
        self.bucket.encode(buf, features)?;
        self.epoch.encode(buf, features)?;
        self.total_usage.bytes_sent.encode(buf, features)?;
        self.total_usage.bytes_received.encode(buf, features)?;
        self.total_usage.ops.encode(buf, features)?;
        self.total_usage.successful_ops.encode(buf, features)?;
        self.usage_map.encode(buf, features)?;
        self.payer.encode(buf, features)?;
        self.s3select_usage.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        // The 19.2.0 corpus holds version-3 samples (v19.2.2 writes 4);
        // that is the one older form this floor reads.
        rados::check_min_version!(struct_v, 3, "UsageLogEntry", "Jewel v10+");

        let owner = String::decode(buf, features)?;
        let bucket = String::decode(buf, features)?;
        let epoch = u64::decode(buf, features)?;
        let bytes_sent = u64::decode(buf, features)?;
        let bytes_received = u64::decode(buf, features)?;
        let ops = u64::decode(buf, features)?;
        let successful_ops = u64::decode(buf, features)?;
        let total_usage = UsageData {
            bytes_sent,
            bytes_received,
            ops,
            successful_ops,
        };
        let usage_map = BTreeMap::<String, UsageData>::decode(buf, features)?;
        let payer = String::decode(buf, features)?;
        let s3select_usage = if struct_v >= 4 {
            S3selectUsageData::decode(buf, features)?
        } else {
            S3selectUsageData::default()
        };
        Ok(Self {
            owner,
            payer,
            bucket,
            epoch,
            total_usage,
            usage_map,
            s3select_usage,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(UsageLogEntry);

impl UsageLogEntry {
    /// `rgw_usage_log_entry::add_usage`.
    pub fn add_usage(&mut self, category: &str, data: &UsageData) {
        self.usage_map
            .entry(category.to_owned())
            .or_default()
            .aggregate(data);
        self.total_usage.aggregate(data);
    }

    /// `rgw_usage_log_entry::sum`: the total of `categories` (every
    /// category when `None` or empty).
    pub fn sum(&self, categories: Option<&BTreeSet<String>>) -> UsageData {
        let mut usage = UsageData::default();
        for (category, data) in &self.usage_map {
            if wanted(categories, category) {
                usage.aggregate(data);
            }
        }
        usage
    }

    /// `rgw_usage_log_entry::aggregate`: fold `other` in, taking its
    /// owner/bucket/epoch/payer when this entry has none yet.
    pub fn aggregate(&mut self, other: &Self, categories: Option<&BTreeSet<String>>) {
        if self.owner.is_empty() {
            self.owner = other.owner.clone();
            self.bucket = other.bucket.clone();
            self.epoch = other.epoch;
            self.payer = other.payer.clone();
        }
        for (category, data) in &other.usage_map {
            if wanted(categories, category) {
                self.add_usage(category, data);
            }
        }
        if wanted(categories, "s3select") {
            self.s3select_usage.aggregate(&other.s3select_usage);
        }
    }
}

/// `!categories || !categories->size() || categories->count(name)`.
fn wanted(categories: Option<&BTreeSet<String>>, name: &str) -> bool {
    match categories {
        None => true,
        Some(c) => c.is_empty() || c.contains(name),
    }
}

/// `rgw_usage_log_entry::categories`: an array of `{"category", ...}`
/// objects, the category's `UsageData` fields flattened in.
struct Categories<'a>(&'a BTreeMap<String, UsageData>);

impl Serialize for Categories<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Entry<'a> {
            category: &'a str,
            #[serde(flatten)]
            data: &'a UsageData,
        }

        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for (category, data) in self.0 {
            seq.serialize_element(&Entry { category, data })?;
        }
        seq.end()
    }
}

impl Serialize for UsageLogEntry {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("UsageLogEntry", 7)?;
        state.serialize_field("owner", &self.owner)?;
        state.serialize_field("payer", &self.payer)?;
        state.serialize_field("bucket", &self.bucket)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("total_usage", &self.total_usage)?;
        state.serialize_field("categories", &Categories(&self.usage_map))?;
        state.serialize_field("s3select", &self.s3select_usage)?;
        state.end()
    }
}

/// `rgw_usage_log_info`: a page of usage-log entries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct UsageLogInfo {
    pub entries: Vec<UsageLogEntry>,
}

/// `rgw_user_bucket`: a user and bucket pair, ordered lexicographically
/// as the C++ `operator<` does (`user` then `bucket`).
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct UserBucket {
    pub user: String,
    pub bucket: String,
}

/// `rgw_cls_usage_log_add_op`: entries to fold into the log. `user` is
/// never set by the C++ client nor read by the class. Version 2 added
/// `user`; the decoder floors at version 2.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 2,
    compat = 1,
    min_version = 2,
    ceph_release = "Jewel v10+"
)]
pub struct AddOp {
    pub info: UsageLogInfo,
    pub user: String,
}

/// `rgw_cls_usage_log_read_op`: up to `max_entries` omap keys (zero means
/// 1000) of `owner`'s entries, or of every user's when `owner` is empty,
/// with `start_epoch <= epoch < end_epoch`, after `iter`. Version 2
/// encodes `bucket` last though the dump prints it fourth; the decoder
/// floors at version 2.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadOp {
    pub start_epoch: u64,
    pub end_epoch: u64,
    pub owner: String,
    pub bucket: String,
    pub iter: String,
    pub max_entries: u32,
}

impl VersionedEncode for ReadOp {
    const MAX_DECODE_VERSION: u8 = 2;

    fn encoding_version(&self, _features: u64) -> u8 {
        2
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.start_epoch.encode(buf, features)?;
        self.end_epoch.encode(buf, features)?;
        self.owner.encode(buf, features)?;
        self.iter.encode(buf, features)?;
        self.max_entries.encode(buf, features)?;
        self.bucket.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 2, "ReadOp", "Nautilus v14+");

        let start_epoch = u64::decode(buf, features)?;
        let end_epoch = u64::decode(buf, features)?;
        let owner = String::decode(buf, features)?;
        let iter = String::decode(buf, features)?;
        let max_entries = u32::decode(buf, features)?;
        let bucket = String::decode(buf, features)?;
        Ok(Self {
            start_epoch,
            end_epoch,
            owner,
            bucket,
            iter,
            max_entries,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(ReadOp);

impl Serialize for ReadOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ReadOp", 6)?;
        state.serialize_field("start_epoch", &self.start_epoch)?;
        state.serialize_field("end_epoch", &self.end_epoch)?;
        state.serialize_field("owner", &self.owner)?;
        state.serialize_field("bucket", &self.bucket)?;
        state.serialize_field("iter", &self.iter)?;
        state.serialize_field("max_entries", &self.max_entries)?;
        state.end()
    }
}

/// `rgw_cls_usage_log_read_ret`: the page's entries merged per (user,
/// bucket), the user being the payer when set. `next_iter` is the last
/// omap key scanned and is set only when `truncated`; passing it back
/// resumes after that key. A page can hold fewer entries than keys
/// scanned, or none, and still be truncated.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ReadRet {
    pub usage: BTreeMap<UserBucket, UsageLogEntry>,
    pub truncated: bool,
    pub next_iter: String,
}

impl Serialize for ReadRet {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        struct Usage<'a>(&'a BTreeMap<UserBucket, UsageLogEntry>);

        impl Serialize for Usage<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                s: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                crate::dump::map_entries(self.0, s)
            }
        }

        let mut state = serializer.serialize_struct("ReadRet", 3)?;
        state.serialize_field("truncated", &self.truncated)?;
        state.serialize_field("next_iter", &self.next_iter)?;
        state.serialize_field("usage", &Usage(&self.usage))?;
        state.end()
    }
}

/// `rgw_cls_usage_log_trim_op`: remove `user`'s entries, or every user's
/// when `user` is empty, with `start_epoch <= epoch < end_epoch`, of
/// `bucket` only when it is set. Version 3 over compat 2; the decoder
/// floors at version 3, the first to carry `bucket`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TrimOp {
    pub start_epoch: u64,
    pub end_epoch: u64,
    pub user: String,
    pub bucket: String,
}

impl VersionedEncode for TrimOp {
    const MAX_DECODE_VERSION: u8 = 3;

    fn encoding_version(&self, _features: u64) -> u8 {
        3
    }

    fn compat_version(&self, _features: u64) -> u8 {
        2
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.start_epoch.encode(buf, features)?;
        self.end_epoch.encode(buf, features)?;
        self.user.encode(buf, features)?;
        self.bucket.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 3, "TrimOp", "Nautilus v14+");

        let start_epoch = u64::decode(buf, features)?;
        let end_epoch = u64::decode(buf, features)?;
        let user = String::decode(buf, features)?;
        let bucket = String::decode(buf, features)?;
        Ok(Self {
            start_epoch,
            end_epoch,
            user,
            bucket,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(TrimOp);

/// What the class answers once a trim call finds nothing left to remove.
const ENODATA: i32 = 61;

/// Rounds [`trim`] sends before giving up; each scans a thousand keys.
pub const MAX_TRIM_ROUNDS: usize = 1000;

fn add_req(info: &UsageLogInfo) -> AddOp {
    AddOp {
        info: info.clone(),
        user: String::new(),
    }
}

fn read_req(
    user: &str,
    bucket: &str,
    start_epoch: u64,
    end_epoch: u64,
    max_entries: u32,
    iter: &str,
) -> ReadOp {
    ReadOp {
        start_epoch,
        end_epoch,
        owner: user.to_owned(),
        bucket: bucket.to_owned(),
        iter: iter.to_owned(),
        max_entries,
    }
}

fn trim_req(user: &str, bucket: &str, start_epoch: u64, end_epoch: u64) -> TrimOp {
    TrimOp {
        start_epoch,
        end_epoch,
        user: user.to_owned(),
        bucket: bucket.to_owned(),
    }
}

/// `cls_rgw_usage_log_add`: fold each of `info`'s entries into the one
/// stored for its (payer-or-owner, bucket, epoch), creating the shard if
/// needed.
pub fn add_op(info: &UsageLogInfo) -> Result<OSDOp> {
    call::op(CLASS, "user_usage_log_add", &add_req(info))
}

/// `cls_rgw_usage_log_read`: one page of the log, as [`ReadOp`]
/// describes; `iter` is empty for the first page and the previous reply's
/// `next_iter` after. `ENOENT` when the shard does not exist. Decode the
/// reply with [`decode_read`].
pub fn read_op(
    user: &str,
    bucket: &str,
    start_epoch: u64,
    end_epoch: u64,
    max_entries: u32,
    iter: &str,
) -> Result<OSDOp> {
    call::op(
        CLASS,
        "user_usage_log_read",
        &read_req(user, bucket, start_epoch, end_epoch, max_entries, iter),
    )
}

/// Decode the reply to [`read_op`].
pub fn decode_read(reply: &OpReply) -> Result<ReadRet> {
    call::decode(reply)
}

/// One `cls_rgw_usage_log_trim` call: scan up to a thousand omap keys of
/// the range [`TrimOp`] describes and remove both keys of each entry
/// found. `ENOENT` when the shard does not exist; `ENODATA` when the scan
/// found no entry and was not truncated, which is where [`trim`] stops.
pub fn trim_op(user: &str, bucket: &str, start_epoch: u64, end_epoch: u64) -> Result<OSDOp> {
    call::op(
        CLASS,
        "user_usage_log_trim",
        &trim_req(user, bucket, start_epoch, end_epoch),
    )
}

/// `cls_rgw_usage_log_clear`: empty the shard's omap. A missing shard is
/// not an error.
pub fn clear_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "usage_log_clear", Bytes::new())
}

/// See [`add_op`].
pub async fn add(ioctx: &IoCtx, oid: &str, info: &UsageLogInfo) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "user_usage_log_add", &add_req(info))
        .await
        .map(drop)
}

/// See [`read_op`].
#[allow(clippy::too_many_arguments)]
pub async fn read(
    ioctx: &IoCtx,
    oid: &str,
    user: &str,
    bucket: &str,
    start_epoch: u64,
    end_epoch: u64,
    max_entries: u32,
    iter: &str,
) -> Result<ReadRet> {
    let op = read_req(user, bucket, start_epoch, end_epoch, max_entries, iter);
    let out = call::exec(ioctx, oid, CLASS, "user_usage_log_read", &op).await?;
    call::decode_bytes(out)
}

/// `cls_rgw_usage_log_trim`: send [`trim_op`] until the class answers
/// `ENODATA`, at most [`MAX_TRIM_ROUNDS`] times. `cls_rgw_client.cc` loops
/// without a bound, but on v19 two states make the class answer 0 forever
/// without progress: an entry stored under a payer (the class derives the keys
/// to remove from the owner, so the payer's keys stay) and a `bucket` filter
/// that skips the first thousand keys of the range (the request carries no
/// iter, so every round rescans them, whatever follows). Those end here as
/// [`OSDClientError::Other`] once the rounds are spent. A trim with no `bucket`
/// needs one round per thousand entries; with one, every round also rescans the
/// skipped keys ahead of the first match. `ENOENT` when the shard does not
/// exist.
pub async fn trim(
    ioctx: &IoCtx,
    oid: &str,
    user: &str,
    bucket: &str,
    start_epoch: u64,
    end_epoch: u64,
) -> Result<()> {
    let op = trim_req(user, bucket, start_epoch, end_epoch);
    for _ in 0..MAX_TRIM_ROUNDS {
        match call::exec(ioctx, oid, CLASS, "user_usage_log_trim", &op).await {
            Ok(_) => {}
            Err(OSDClientError::OSDError { code, .. }) if code == -ENODATA => return Ok(()),
            Err(err) => return Err(err),
        }
    }
    Err(OSDClientError::Other(format!(
        "rgw::usage::trim made {MAX_TRIM_ROUNDS} rounds without ENODATA"
    )))
}

/// See [`clear_op`].
pub async fn clear(ioctx: &IoCtx, oid: &str) -> Result<()> {
    call::exec_raw(ioctx, oid, CLASS, "usage_log_clear", Bytes::new())
        .await
        .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::encode_with_capacity;

    fn bytes<T: Denc>(v: &T) -> Vec<u8> {
        encode_with_capacity(v, 0).expect("encode").to_vec()
    }

    fn json<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("json")
    }

    /// Bytes from a hex string, as `ceph-dencoder ... encode export` wrote them.
    fn unhex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ascii"), 16).expect("hex")
            })
            .collect()
    }

    #[test]
    fn usage_data_is_four_u64s() {
        // Oracle rgw_usage_data instance 1: sent 1024, received 1024, ops 2,
        // successful_ops 1.
        let d = UsageData {
            bytes_sent: 1024,
            bytes_received: 1024,
            ops: 2,
            successful_ops: 1,
        };
        assert_eq!(
            bytes(&d),
            unhex("0101200000000004000000000000000400000000000002000000000000000100000000000000")
        );
        assert_eq!(
            json(&d),
            r#"{"bytes_sent":1024,"bytes_received":1024,"ops":2,"successful_ops":1}"#
        );
        let mut sum = UsageData::default();
        sum.aggregate(&d);
        sum.aggregate(&d);
        assert_eq!(sum.bytes_sent, 2048);
    }

    fn usage_log_entry_instance() -> UsageLogEntry {
        let mut usage_map = BTreeMap::new();
        usage_map.insert(
            "get_obj".to_owned(),
            UsageData {
                bytes_sent: 1024,
                bytes_received: 2048,
                ops: 0,
                successful_ops: 0,
            },
        );
        UsageLogEntry {
            owner: "owner".to_owned(),
            payer: "payer".to_owned(),
            bucket: "bucket".to_owned(),
            epoch: 1234,
            total_usage: UsageData {
                bytes_sent: 1024,
                bytes_received: 2048,
                ops: 0,
                successful_ops: 0,
            },
            usage_map,
            s3select_usage: S3selectUsageData {
                bytes_processed: 8192,
                bytes_returned: 4096,
            },
        }
    }

    #[test]
    fn usage_log_entry_writes_total_usage_inline_and_categories_last() {
        let e = usage_log_entry_instance();
        assert_eq!(
            bytes(&e),
            unhex(
                "04018f000000050000006f776e6572060000006275636b6574d204000000000000000400000000000000080000000000000000000000000000000000000000000001000000070000006765745f6f626a010120000000000400000000000000080000000000000000000000000000000000000000000005000000706179657201011000000000200000000000000010000000000000"
            )
        );
        assert_eq!(
            json(&e),
            r#"{"owner":"owner","payer":"payer","bucket":"bucket","epoch":1234,"total_usage":{"bytes_sent":1024,"bytes_received":2048,"ops":0,"successful_ops":0},"categories":[{"category":"get_obj","bytes_sent":1024,"bytes_received":2048,"ops":0,"successful_ops":0}],"s3select":{"bytes_processed":8192,"bytes_returned":4096}}"#
        );
        assert_eq!(
            UsageLogEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"),
            e
        );

        // The 19.2.0 corpus form: version 3, no s3select_usage block.
        let v3 = unhex(
            "030179000000050000006f776e6572060000006275636b6574d204000000000000000400000000000000080000000000000000000000000000000000000000000001000000070000006765745f6f626a0101200000000004000000000000000800000000000000000000000000000000000000000000050000007061796572",
        );
        let mut expect = e.clone();
        expect.s3select_usage = S3selectUsageData::default();
        assert_eq!(
            UsageLogEntry::decode(&mut &v3[..], 0).expect("decode"),
            expect
        );
    }

    #[test]
    fn add_usage_and_aggregate_fold_categories() {
        let mut e = UsageLogEntry {
            owner: "owner".to_owned(),
            ..Default::default()
        };
        e.add_usage(
            "get_obj",
            &UsageData {
                bytes_sent: 10,
                ..Default::default()
            },
        );
        e.add_usage(
            "put_obj",
            &UsageData {
                bytes_received: 5,
                ..Default::default()
            },
        );
        assert_eq!(e.total_usage.bytes_sent, 10);
        assert_eq!(e.total_usage.bytes_received, 5);

        let mut only_get: BTreeSet<String> = BTreeSet::new();
        only_get.insert("get_obj".to_owned());
        assert_eq!(e.sum(Some(&only_get)).bytes_sent, 10);
        assert_eq!(e.sum(Some(&only_get)).bytes_received, 0);
        assert_eq!(e.sum(None).bytes_received, 5);

        let mut total = UsageLogEntry::default();
        total.aggregate(&e, None);
        assert_eq!(total.owner, e.owner);
        assert_eq!(total.usage_map.get("get_obj").unwrap().bytes_sent, 10);
        // A second aggregate with an already-set owner does not overwrite it.
        let mut other = e.clone();
        other.owner = "someone-else".to_owned();
        total.aggregate(&other, None);
        assert_eq!(total.owner, e.owner);
        assert_eq!(total.usage_map.get("get_obj").unwrap().bytes_sent, 20);
    }

    #[test]
    fn usage_log_info_dumps_its_entries() {
        let info = UsageLogInfo {
            entries: vec![usage_log_entry_instance()],
        };
        assert_eq!(
            UsageLogInfo::decode(&mut &bytes(&info)[..], 0).expect("decode"),
            info
        );
        assert!(json(&info).starts_with(r#"{"entries":[{"owner":"owner""#));
    }

    #[test]
    fn user_bucket_orders_lexicographically() {
        let u = UserBucket {
            user: "user".to_owned(),
            bucket: "bucket".to_owned(),
        };
        assert_eq!(
            bytes(&u),
            unhex("0101120000000400000075736572060000006275636b6574")
        );
        assert_eq!(json(&u), r#"{"user":"user","bucket":"bucket"}"#);
        assert_eq!(
            UserBucket::decode(&mut &bytes(&u)[..], 0).expect("decode"),
            u
        );
        let a = UserBucket {
            user: "a".to_owned(),
            bucket: "z".to_owned(),
        };
        let b = UserBucket {
            user: "b".to_owned(),
            bucket: "a".to_owned(),
        };
        assert!(a < b);
    }

    #[test]
    fn add_op_wraps_the_info_and_an_empty_user() {
        // Oracle rgw_cls_usage_log_add_op instance 1: the default.
        let op = AddOp::default();
        let wire = unhex("02010e0000000101040000000000000000000000");
        assert_eq!(bytes(&op), wire);
        assert_eq!(json(&op), r#"{"info":{"entries":[]},"user":""}"#);
        assert_eq!(AddOp::decode(&mut &wire[..], 0).expect("decode"), op);
    }

    #[test]
    fn read_op_encodes_bucket_last_and_dumps_it_fourth() {
        // Oracle rgw_cls_usage_log_read_op instance 1.
        let op = ReadOp {
            start_epoch: 1,
            end_epoch: 2,
            owner: "owner".to_owned(),
            bucket: "bucket".to_owned(),
            iter: "iter".to_owned(),
            max_entries: 100,
        };
        let wire = unhex(
            "02012f00000001000000000000000200000000000000050000006f776e6572040000006974657264000000060000006275636b6574",
        );
        assert_eq!(bytes(&op), wire);
        assert_eq!(
            json(&op),
            r#"{"start_epoch":1,"end_epoch":2,"owner":"owner","bucket":"bucket","iter":"iter","max_entries":100}"#
        );
        assert_eq!(ReadOp::decode(&mut &wire[..], 0).expect("decode"), op);
        // Version 1 had no bucket; the decoder floors at version 2.
        let v1 = unhex("01011c00000001000000000000000200000000000000000000000000000064000000");
        assert!(matches!(
            ReadOp::decode(&mut &v1[..], 0),
            Err(RadosError::Codec(rados::CodecError::VersionTooOld {
                got: 1,
                ..
            }))
        ));
    }

    #[test]
    fn read_ret_dumps_truncated_and_next_iter_before_the_usage() {
        // Oracle rgw_cls_usage_log_read_ret instance 1.
        let ret = ReadRet {
            usage: BTreeMap::new(),
            truncated: true,
            next_iter: "123".to_owned(),
        };
        let wire = unhex("01010c000000000000000103000000313233");
        assert_eq!(bytes(&ret), wire);
        assert_eq!(
            json(&ret),
            r#"{"truncated":true,"next_iter":"123","usage":[]}"#
        );
        assert_eq!(ReadRet::decode(&mut &wire[..], 0).expect("decode"), ret);

        // Oracle instance 2: two default entries keyed by user and bucket.
        let mut usage = BTreeMap::new();
        for (user, bucket) in [("user1", "bucket1"), ("user2", "bucket2")] {
            usage.insert(
                UserBucket {
                    user: user.to_owned(),
                    bucket: bucket.to_owned(),
                },
                UsageLogEntry::default(),
            );
        }
        let ret = ReadRet {
            usage,
            truncated: true,
            next_iter: "next_iter".to_owned(),
        };
        let wire = unhex(
            "0101ee00000002000000010114000000050000007573657231070000006275636b65743104014e000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001011000000000000000000000000000000000000000010114000000050000007573657232070000006275636b65743204014e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000101100000000000000000000000000000000000000001090000006e6578745f69746572",
        );
        assert_eq!(bytes(&ret), wire);
        assert_eq!(ReadRet::decode(&mut &wire[..], 0).expect("decode"), ret);
        let empty = r#"{"owner":"","payer":"","bucket":"","epoch":0,"total_usage":{"bytes_sent":0,"bytes_received":0,"ops":0,"successful_ops":0},"categories":[],"s3select":{"bytes_processed":0,"bytes_returned":0}}"#;
        assert_eq!(
            json(&ret),
            format!(
                r#"{{"truncated":true,"next_iter":"next_iter","usage":[{{"key":{{"user":"user1","bucket":"bucket1"}},"val":{empty}}},{{"key":{{"user":"user2","bucket":"bucket2"}},"val":{empty}}}]}}"#
            )
        );
    }

    #[test]
    fn read_ret_dumps_one_entry_as_key_and_val() {
        let mut usage = BTreeMap::new();
        usage.insert(
            UserBucket {
                user: "u".to_owned(),
                bucket: "b".to_owned(),
            },
            usage_log_entry_instance(),
        );
        let ret = ReadRet {
            usage,
            ..ReadRet::default()
        };
        assert_eq!(
            json(&ret),
            format!(
                r#"{{"truncated":false,"next_iter":"","usage":[{{"key":{{"user":"u","bucket":"b"}},"val":{}}}]}}"#,
                json(&usage_log_entry_instance())
            )
        );
        assert_eq!(
            ReadRet::decode(&mut &bytes(&ret)[..], 0).expect("decode"),
            ret
        );
    }

    #[test]
    fn trim_op_is_version_3_over_compat_2() {
        // Oracle rgw_cls_usage_log_trim_op instance 1.
        let op = TrimOp {
            start_epoch: 1,
            end_epoch: 2,
            user: "user".to_owned(),
            bucket: "bucket".to_owned(),
        };
        let wire = unhex(
            "030222000000010000000000000002000000000000000400000075736572060000006275636b6574",
        );
        assert_eq!(bytes(&op), wire);
        assert_eq!(
            json(&op),
            r#"{"start_epoch":1,"end_epoch":2,"user":"user","bucket":"bucket"}"#
        );
        assert_eq!(TrimOp::decode(&mut &wire[..], 0).expect("decode"), op);
        // Version 2 had no bucket; the decoder floors at version 3.
        let v2 = unhex("020218000000010000000000000002000000000000000400000075736572");
        assert!(matches!(
            TrimOp::decode(&mut &v2[..], 0),
            Err(RadosError::Codec(rados::CodecError::VersionTooOld {
                got: 2,
                ..
            }))
        ));
    }

    fn call_indata(method: &str, req: &[u8]) -> Vec<u8> {
        let mut want = format!("rgw{method}").into_bytes();
        want.extend_from_slice(req);
        want
    }

    #[test]
    fn ops_name_the_rgw_class_and_their_method() {
        use rados::osdclient::types::OpData;

        let cases = [
            (
                add_op(&UsageLogInfo::default()),
                "user_usage_log_add",
                unhex("02010e0000000101040000000000000000000000"),
            ),
            (
                read_op("owner", "bucket", 1, 2, 100, "iter"),
                "user_usage_log_read",
                unhex(
                    "02012f00000001000000000000000200000000000000050000006f776e6572040000006974657264000000060000006275636b6574",
                ),
            ),
            (
                trim_op("user", "bucket", 1, 2),
                "user_usage_log_trim",
                unhex(
                    "030222000000010000000000000002000000000000000400000075736572060000006275636b6574",
                ),
            ),
            (clear_op(), "usage_log_clear", Vec::new()),
        ];
        for (op, method, req) in cases {
            let op = op.expect("op");
            assert_eq!(
                op.indata.as_ref(),
                &call_indata(method, &req)[..],
                "{method}"
            );
            assert!(
                matches!(
                    op.op_data,
                    OpData::Call { class_len: 3, method_len, .. } if usize::from(method_len) == method.len()
                ),
                "{method}"
            );
        }
    }

    #[test]
    fn decode_read_unwraps_the_reply() {
        let ret = ReadRet {
            usage: BTreeMap::new(),
            truncated: true,
            next_iter: "u1_01755892800_b1".to_owned(),
        };
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&ret, 0).expect("encode"),
        };
        assert_eq!(decode_read(&reply).expect("decode"), ret);
    }

    #[test]
    fn add_op_floors_at_version_2() {
        let mut wire = bytes(&AddOp::default());
        wire[..2].copy_from_slice(&[1, 1]);
        let err = AddOp::decode(&mut &wire[..], 0).expect_err("version 1");
        assert!(
            matches!(
                err,
                RadosError::Codec(rados::CodecError::VersionTooOld { got: 1, min: 2, .. })
            ),
            "{err:?}"
        );
    }
}
