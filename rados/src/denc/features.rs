//! Ceph feature bits and masks used to select or validate wire encodings.
//!
//! This module mirrors the feature definitions from Ceph's `ceph_features.h`
//! and exposes them through the `CephFeatures` bitflags type. It also retains
//! the release-incarnation mask behavior Ceph bakes into feature masks so
//! encode/decode decisions stay aligned with the reference implementation.

// Ceph feature flags used in encoding/decoding
// Based on include/ceph_features.h

// Feature incarnation masks — these are "implies" bits baked into every
// feature mask for the corresponding Ceph release.
// Private — only used by define_ceph_feature! below.
const CEPH_FEATURE_INCARNATION_1: u64 = 0;
const CEPH_FEATURE_INCARNATION_2: u64 = 1 << 57; // SERVER_JEWEL
const CEPH_FEATURE_INCARNATION_3: u64 = (1 << 57) | (1 << 28); // SERVER_MIMIC

/// Defines a pair of private Ceph feature constants used to seed the
/// `CephFeatures` bitflags struct:
///
/// * `CEPH_FEATURE_<NAME>`     = raw bit (`1u64 << bit`)
/// * `CEPH_FEATUREMASK_<NAME>` = raw bit | incarnation bits
///
/// Both are private to this module; callers use `CephFeatures::NAME` and
/// `CephFeatures::MASK_NAME` instead.
macro_rules! define_ceph_feature {
    ($bit:expr, $incarnation:expr, $name:ident) => {
        paste::paste! {
            const [<CEPH_FEATURE_ $name>]: u64 = 1u64 << $bit;
            const [<CEPH_FEATUREMASK_ $name>]: u64 = (1u64 << $bit) | $incarnation;
        }
    };
}

// All features, sorted by bit number.
// `define_ceph_feature!(bit, incarnation, NAME)` — arguments match C++
// `DEFINE_CEPH_FEATURE(bit, incarnation_index, NAME)` in ceph_features.h.
define_ceph_feature!(2, CEPH_FEATURE_INCARNATION_3, SERVER_NAUTILUS);
define_ceph_feature!(9, CEPH_FEATURE_INCARNATION_1, PGID64);
define_ceph_feature!(11, CEPH_FEATURE_INCARNATION_1, PGPOOL3);
define_ceph_feature!(13, CEPH_FEATURE_INCARNATION_1, OSDENC);
define_ceph_feature!(15, CEPH_FEATURE_INCARNATION_1, MONENC);
define_ceph_feature!(16, CEPH_FEATURE_INCARNATION_3, SERVER_OCTOPUS);
define_ceph_feature!(17, CEPH_FEATURE_INCARNATION_3, OS_PERF_STAT_NS);
define_ceph_feature!(21, CEPH_FEATURE_INCARNATION_2, SERVER_LUMINOUS);
define_ceph_feature!(23, CEPH_FEATURE_INCARNATION_2, OSD_POOLRESEND);
define_ceph_feature!(28, CEPH_FEATURE_INCARNATION_2, SERVER_MIMIC);
define_ceph_feature!(36, CEPH_FEATURE_INCARNATION_1, CRUSH_V2);
define_ceph_feature!(49, CEPH_FEATURE_INCARNATION_2, SERVER_SQUID);
define_ceph_feature!(50, CEPH_FEATURE_INCARNATION_2, SERVER_TENTACLE);
define_ceph_feature!(56, CEPH_FEATURE_INCARNATION_1, NEW_OSDOP_ENCODING);
define_ceph_feature!(57, CEPH_FEATURE_INCARNATION_1, SERVER_JEWEL);
define_ceph_feature!(59, CEPH_FEATURE_INCARNATION_1, MSG_ADDR2);

bitflags::bitflags! {
    /// Type-safe Ceph feature flags.
    ///
    /// Each `NAME` variant is the raw single feature bit equivalent to the old
    /// private `CEPH_FEATURE_NAME` constant.  Each `MASK_NAME` variant
    /// additionally ORs in the ancestor-release "incarnation" bits required to
    /// fully identify a Ceph release, equivalent to the old private
    /// `CEPH_FEATUREMASK_NAME` constants used by C++'s `HAVE_FEATURE` macro.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use crate::denc::features::{CephFeatures, has_feature, has_significant_feature};
    ///
    /// let peer_features: u64 = get_peer_features();
    ///
    /// // Single-bit check (any bit set)
    /// if has_feature(peer_features, CephFeatures::MSG_ADDR2) { ... }
    ///
    /// // Full-mask check — equivalent to C++ HAVE_FEATURE(x, SERVER_NAUTILUS)
    /// if has_significant_feature(peer_features, CephFeatures::MASK_SERVER_NAUTILUS) { ... }
    ///
    /// // Raw bits for passing to encode/decode
    /// let my_features: u64 = (CephFeatures::MSG_ADDR2 | CephFeatures::SERVER_NAUTILUS).bits();
    /// ```
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct CephFeatures: u64 {
        // ---- Individual feature bits ----------------------------------------
        const SERVER_NAUTILUS    = CEPH_FEATURE_SERVER_NAUTILUS;
        const PGID64             = CEPH_FEATURE_PGID64;
        const PGPOOL3            = CEPH_FEATURE_PGPOOL3;
        const OSDENC             = CEPH_FEATURE_OSDENC;
        const MONENC             = CEPH_FEATURE_MONENC;
        const SERVER_OCTOPUS     = CEPH_FEATURE_SERVER_OCTOPUS;
        const OS_PERF_STAT_NS    = CEPH_FEATURE_OS_PERF_STAT_NS;
        const SERVER_LUMINOUS    = CEPH_FEATURE_SERVER_LUMINOUS;
        const OSD_POOLRESEND     = CEPH_FEATURE_OSD_POOLRESEND;
        const SERVER_MIMIC       = CEPH_FEATURE_SERVER_MIMIC;
        const CRUSH_V2           = CEPH_FEATURE_CRUSH_V2;
        const SERVER_SQUID       = CEPH_FEATURE_SERVER_SQUID;
        const SERVER_TENTACLE    = CEPH_FEATURE_SERVER_TENTACLE;
        const NEW_OSDOP_ENCODING = CEPH_FEATURE_NEW_OSDOP_ENCODING;
        const SERVER_JEWEL       = CEPH_FEATURE_SERVER_JEWEL;
        const MSG_ADDR2          = CEPH_FEATURE_MSG_ADDR2;

        // ---- Feature masks (feature bit + ancestor-release incarnation bits) -
        const MASK_SERVER_NAUTILUS    = CEPH_FEATUREMASK_SERVER_NAUTILUS;
        const MASK_PGID64             = CEPH_FEATUREMASK_PGID64;
        const MASK_PGPOOL3            = CEPH_FEATUREMASK_PGPOOL3;
        const MASK_OSDENC             = CEPH_FEATUREMASK_OSDENC;
        const MASK_MONENC             = CEPH_FEATUREMASK_MONENC;
        const MASK_SERVER_OCTOPUS     = CEPH_FEATUREMASK_SERVER_OCTOPUS;
        const MASK_OS_PERF_STAT_NS    = CEPH_FEATUREMASK_OS_PERF_STAT_NS;
        const MASK_SERVER_LUMINOUS    = CEPH_FEATUREMASK_SERVER_LUMINOUS;
        const MASK_OSD_POOLRESEND     = CEPH_FEATUREMASK_OSD_POOLRESEND;
        const MASK_SERVER_MIMIC       = CEPH_FEATUREMASK_SERVER_MIMIC;
        const MASK_CRUSH_V2           = CEPH_FEATUREMASK_CRUSH_V2;
        const MASK_SERVER_SQUID       = CEPH_FEATUREMASK_SERVER_SQUID;
        const MASK_SERVER_TENTACLE    = CEPH_FEATUREMASK_SERVER_TENTACLE;
        const MASK_NEW_OSDOP_ENCODING = CEPH_FEATUREMASK_NEW_OSDOP_ENCODING;
        const MASK_SERVER_JEWEL       = CEPH_FEATUREMASK_SERVER_JEWEL;
        const MASK_MSG_ADDR2          = CEPH_FEATUREMASK_MSG_ADDR2;
    }
}

/// Significant features for OSD encoding (matching OSDMap.h SIGNIFICANT_FEATURES).
pub const SIGNIFICANT_FEATURES: CephFeatures = CephFeatures::MASK_PGID64
    .union(CephFeatures::MASK_PGPOOL3)
    .union(CephFeatures::MASK_OSDENC)
    .union(CephFeatures::MASK_OSD_POOLRESEND)
    .union(CephFeatures::MASK_NEW_OSDOP_ENCODING)
    .union(CephFeatures::MASK_MSG_ADDR2)
    .union(CephFeatures::MASK_SERVER_LUMINOUS)
    .union(CephFeatures::MASK_SERVER_MIMIC)
    .union(CephFeatures::MASK_SERVER_NAUTILUS)
    .union(CephFeatures::MASK_SERVER_OCTOPUS)
    .union(CephFeatures::MASK_SERVER_SQUID)
    .union(CephFeatures::MASK_SERVER_TENTACLE);

/// Returns `true` if `features` has the given feature bit set.
///
/// Equivalent to a non-zero bitwise AND check for a single-bit
/// [`CephFeatures`] variant such as `CephFeatures::MSG_ADDR2`.
pub fn has_feature(features: u64, flag: CephFeatures) -> bool {
    (features & flag.bits()) != 0
}

/// Returns `true` if all bits in `mask` are present in `features`.
///
/// Equivalent to C++'s `HAVE_FEATURE(x, name)` macro when called with a
/// `MASK_*` [`CephFeatures`] variant such as `CephFeatures::MASK_SERVER_NAUTILUS`.
pub fn has_significant_feature(features: u64, mask: CephFeatures) -> bool {
    (features & mask.bits()) == mask.bits()
}

/// Returns the significant subset of `features` as a typed [`CephFeatures`].
pub fn get_significant_features(features: u64) -> CephFeatures {
    CephFeatures::from_bits_retain(features) & SIGNIFICANT_FEATURES
}
