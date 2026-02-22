// Ceph feature flags used in encoding/decoding
// Based on include/ceph_features.h

// Feature incarnation masks — these are "implies" bits baked into every
// feature mask for the corresponding Ceph release.
pub const CEPH_FEATURE_INCARNATION_1: u64 = 0;
pub const CEPH_FEATURE_INCARNATION_2: u64 = 1 << 57; // SERVER_JEWEL
pub const CEPH_FEATURE_INCARNATION_3: u64 = (1 << 57) | (1 << 28); // SERVER_MIMIC

/// Defines a Ceph feature flag pair:
///
/// * `CEPH_FEATURE_<NAME>`     = raw bit (`1u64 << bit`)
/// * `CEPH_FEATUREMASK_<NAME>` = raw bit | incarnation bits
///
/// The mask is used for "does the peer support this version?" checks because it
/// also verifies that the peer supports the required ancestor releases.
macro_rules! define_ceph_feature {
    ($bit:expr, $incarnation:expr, $name:ident) => {
        paste::paste! {
            pub const [<CEPH_FEATURE_ $name>]: u64 = 1u64 << $bit;
            pub const [<CEPH_FEATUREMASK_ $name>]: u64 = (1u64 << $bit) | $incarnation;
        }
    };
}

// All features, sorted by bit number.
// `define_ceph_feature!(bit, incarnation, NAME)` — arguments match C++
// `DEFINE_CEPH_FEATURE(bit, incarnation_index, NAME)` in ceph_features.h.
define_ceph_feature!(2,  CEPH_FEATURE_INCARNATION_3, SERVER_NAUTILUS);
define_ceph_feature!(9,  CEPH_FEATURE_INCARNATION_1, PGID64);
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
    /// Type-safe Ceph feature flags (raw feature bits, without incarnation).
    ///
    /// Each variant equals the corresponding `CEPH_FEATURE_X` constant.  For
    /// checks that must also verify ancestor-release bits, use the plain-`u64`
    /// `CEPH_FEATUREMASK_X` constants from this module (e.g.
    /// `CEPH_FEATUREMASK_SERVER_NAUTILUS`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct CephFeatures: u64 {
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
    }
}

// Significant features for encoding (matching OSDMap.h SIGNIFICANT_FEATURES)
pub const SIGNIFICANT_FEATURES: u64 = CEPH_FEATUREMASK_PGID64
    | CEPH_FEATUREMASK_PGPOOL3
    | CEPH_FEATUREMASK_OSDENC
    | CEPH_FEATUREMASK_OSD_POOLRESEND
    | CEPH_FEATUREMASK_NEW_OSDOP_ENCODING
    | CEPH_FEATUREMASK_MSG_ADDR2
    | CEPH_FEATUREMASK_SERVER_LUMINOUS
    | CEPH_FEATUREMASK_SERVER_MIMIC
    | CEPH_FEATUREMASK_SERVER_NAUTILUS
    | CEPH_FEATUREMASK_SERVER_OCTOPUS
    | CEPH_FEATUREMASK_SERVER_SQUID
    | CEPH_FEATUREMASK_SERVER_TENTACLE;

// Helper to check if a feature is enabled (like HAVE_FEATURE)
pub fn has_feature(features: u64, feature: u64) -> bool {
    (features & feature) != 0
}

// Helper to check significant features (like HAVE_SIGNIFICANT_FEATURE)
pub fn has_significant_feature(features: u64, feature_mask: u64) -> bool {
    (features & feature_mask) == feature_mask
}

// Get significant features from a features set
pub fn get_significant_features(features: u64) -> u64 {
    SIGNIFICANT_FEATURES & features
}
