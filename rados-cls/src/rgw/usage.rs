//! `cls_rgw_types.h`: the usage-log types the `rgw` class's
//! `user_usage_log_add`, `user_usage_log_read`, `user_usage_log_trim`
//! and `usage_log_clear` methods read and write. Those methods come with
//! a later plan.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut};
use rados::{Denc, RadosError, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::{SerializeSeq, SerializeStruct};

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
}
