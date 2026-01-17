// Ceph feature flags used in encoding/decoding
// Based on include/ceph_features.h

// Feature incarnation masks
pub const CEPH_FEATURE_INCARNATION_1: u64 = 0;
pub const CEPH_FEATURE_INCARNATION_2: u64 = 1 << 57; // SERVER_JEWEL
pub const CEPH_FEATURE_INCARNATION_3: u64 = (1 << 57) | (1 << 28); // SERVER_MIMIC

// Macro to define Ceph features similar to DEFINE_CEPH_FEATURE in C++
macro_rules! define_ceph_feature {
    ($bit:expr, $incarnation:expr, $name:ident) => {
        paste::paste! {
            pub const [<CEPH_FEATURE_ $name>]: u64 = 1u64 << $bit;
            pub const [<CEPH_FEATUREMASK_ $name>]: u64 = (1u64 << $bit) | $incarnation;
        }
    };
}

// Define all the features we need using the macro
define_ceph_feature!(9, CEPH_FEATURE_INCARNATION_1, PGID64);
define_ceph_feature!(11, CEPH_FEATURE_INCARNATION_1, PGPOOL3);
define_ceph_feature!(13, CEPH_FEATURE_INCARNATION_1, OSDENC);
define_ceph_feature!(21, CEPH_FEATURE_INCARNATION_2, SERVER_LUMINOUS);
define_ceph_feature!(28, CEPH_FEATURE_INCARNATION_2, SERVER_MIMIC);
define_ceph_feature!(2, CEPH_FEATURE_INCARNATION_3, SERVER_NAUTILUS);
define_ceph_feature!(16, CEPH_FEATURE_INCARNATION_3, SERVER_OCTOPUS);
define_ceph_feature!(50, CEPH_FEATURE_INCARNATION_2, SERVER_TENTACLE);
define_ceph_feature!(56, CEPH_FEATURE_INCARNATION_1, NEW_OSDOP_ENCODING);
define_ceph_feature!(59, CEPH_FEATURE_INCARNATION_1, MSG_ADDR2);
define_ceph_feature!(36, CEPH_FEATURE_INCARNATION_1, CRUSH_V2);
define_ceph_feature!(23, CEPH_FEATURE_INCARNATION_2, OSD_POOLRESEND);

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
