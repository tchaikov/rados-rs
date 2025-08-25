// Simple test to verify conditional encoding works
use denc::{PgPool, VersionedEncode};
use denc::features::*;

fn main() {
    println!("Testing conditional encoding logic...");
    
    let pool = PgPool::new();
    
    // Test different feature combinations
    let test_cases = vec![
        (0u64, "No features"),
        (CEPH_FEATUREMASK_NEW_OSDOP_ENCODING, "NEW_OSDOP_ENCODING only"),
        (CEPH_FEATUREMASK_NEW_OSDOP_ENCODING | CEPH_FEATUREMASK_SERVER_LUMINOUS, "NEW_OSDOP + LUMINOUS"),
        (CEPH_FEATUREMASK_NEW_OSDOP_ENCODING | CEPH_FEATUREMASK_SERVER_LUMINOUS | CEPH_FEATUREMASK_SERVER_MIMIC, "NEW_OSDOP + LUMINOUS + MIMIC"),
        (CEPH_FEATUREMASK_NEW_OSDOP_ENCODING | CEPH_FEATUREMASK_SERVER_LUMINOUS | CEPH_FEATUREMASK_SERVER_MIMIC | CEPH_FEATUREMASK_SERVER_NAUTILUS, "NEW_OSDOP + LUMINOUS + MIMIC + NAUTILUS"),
        (CEPH_FEATUREMASK_SERVER_TENTACLE, "SERVER_TENTACLE"),
    ];
    
    for (features, description) in test_cases {
        let version = pool.encoding_version(features);
        println!("Features: {:<50} -> Version: {}", description, version);
    }
    
    // Expected outputs based on the C++ logic:
    // No features -> 21 (hammer)
    // NEW_OSDOP_ENCODING only -> 24 (luminous)
    // NEW_OSDOP + LUMINOUS -> 24 (luminous)
    // NEW_OSDOP + LUMINOUS + MIMIC -> 26 (mimic)
    // NEW_OSDOP + LUMINOUS + MIMIC + NAUTILUS -> 27 (nautilus)
    // SERVER_TENTACLE -> 32 (latest)
    
    println!("✓ Conditional encoding test completed!");
}