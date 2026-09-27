//! Types the `rgw` object class and the classes RGW layers on it share
//! (`cls_rgw_types.h`, `cls_rgw_ops.h`), and the class's methods:
//! [`index`] has the bucket-index, resharding and head-object ones,
//! [`gc`] the omap-era GC ones, [`usage`] the usage-log ones, [`lc`] the
//! lifecycle ones and [`olh`] the object-versioning ones.
//!
//! # Release shapes
//!
//! Tentacle and Umbrella changed three request shapes, each keeping its
//! compat version:
//!
//! | request | Squid (19) | Tentacle (20) | Umbrella (21) |
//! |---|---|---|---|
//! | `bucket_update_stats` ([`index::UpdateStatsOp`]) | v1 | v2, `dec_stats` empty | v2 |
//! | `bucket_read_olh_log` ([`olh::ReadOlhLogOp`]) | v1 | v1 | v2, `get_stales = true` |
//! | meta inside `bucket_complete_op` / `bucket_link_olh` ([`index::DirEntryMeta`]) | v7 | v7 | v8, restore zero |
//!
//! Requests follow the cluster's `require_osd_release`
//! ([`rados::IoCtx::require_osd_release`], or
//! [`rados::OSDClientConfig::assume_osd_release`]): [`index::update_stats`],
//! [`olh::read_olh_log`] and their `*_op` forms take the release, and
//! [`index::DirEntryMeta::for_release`] builds request metadata. Replies
//! decode up to what Umbrella writes. Building a request for a release
//! later than the OSD's is a caller error: a Squid OSD skips the
//! compat-1 or compat-3 tail, so nothing breaks, but the rule is not to
//! send it.
//!
//! Not modelled: the OLH epoch that `bucket_link_olh` and
//! `bucket_unlink_instance` return on `main` is in no release and feeds
//! only the multisite bilog. `bucket_init_index2`, `bi_put_entries`,
//! `reshard_log_trim`, `bi_list` version 2 and `reshard_add` version 2
//! (Tentacle v20.2.0) serve resharding, which this crate leaves to
//! radosgw. `bucket_refresh_instance` (`main` only) serves cloud
//! restore, which is out of scope.

/// The class name.
pub const CLASS: &str = "rgw";

pub mod gc;
pub mod index;
pub mod lc;
pub mod olh;
mod packed;
pub mod types;
pub mod usage;
