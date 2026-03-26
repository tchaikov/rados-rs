//! High-level CRUSH placement helpers for mapping objects and PGs onto OSD sets.
//!
//! This module exposes the public placement-facing domain types such as
//! [`ObjectLocator`] and [`PgId`], plus the helper functions that translate an
//! object identifier and pool context into placement groups and acting OSDs.
//! It is the main API surface callers use once they already have a decoded
//! [`CrushMap`](crate::crush::types::CrushMap).

use crate::crush::error::Result;
use crate::crush::mapper::crush_do_rule;
use crate::crush::types::CrushMap;
use crate::denc::{Denc, FixedSize, RadosError, VersionedEncode};
use bytes::{Buf, BufMut};

/// Sentinel value for `ObjectLocator::hash` meaning "calculate hash from object name".
/// Reference: linux/net/ceph/osd_client.c encode_request_partial()
pub(crate) const HASH_CALCULATE_FROM_NAME: i64 = -1;

/// Object locator information
/// Matches C++ object_locator_t from ~/dev/ceph/src/osd/osd_types.h
/// Contains pool ID, namespace, key, and hash for object placement
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ObjectLocator {
    /// Pool ID (u64::MAX for invalid/default)
    pub pool_id: u64,
    /// Key string (if non-empty) - alternative to hash for placement
    pub key: String,
    /// Object namespace (empty string for default)
    pub namespace: String,
    /// Hash position (if >= 0) - alternative to key for placement
    /// Note: You specify either hash or key, not both
    pub hash: i64,
}

impl ObjectLocator {
    /// Create a new object locator with just a pool ID
    pub fn new(pool_id: u64) -> Self {
        ObjectLocator {
            pool_id,
            key: String::new(),
            namespace: String::new(),
            hash: HASH_CALCULATE_FROM_NAME,
        }
    }

    /// Create an object locator with pool ID and namespace
    pub fn with_namespace(pool_id: u64, namespace: String) -> Self {
        ObjectLocator {
            pool_id,
            key: String::new(),
            namespace,
            hash: HASH_CALCULATE_FROM_NAME,
        }
    }

    /// Create an object locator with a key override
    pub fn with_key(pool_id: u64, key: String) -> Self {
        ObjectLocator {
            pool_id,
            key,
            namespace: String::new(),
            hash: HASH_CALCULATE_FROM_NAME,
        }
    }

    /// Create an object locator with a hash override
    pub fn with_hash(pool_id: u64, hash: i64) -> Self {
        ObjectLocator {
            pool_id,
            key: String::new(),
            namespace: String::new(),
            hash,
        }
    }

    /// Check if the locator is empty (has sentinel values)
    pub fn is_empty(&self) -> bool {
        self.pool_id == u64::MAX
            && self.key.is_empty()
            && self.namespace.is_empty()
            && self.hash == -1
    }
}

impl Default for ObjectLocator {
    fn default() -> Self {
        ObjectLocator {
            pool_id: u64::MAX,
            key: String::new(),
            namespace: String::new(),
            hash: HASH_CALCULATE_FROM_NAME,
        }
    }
}

/// Placement group identifier (pg_t in C++)
/// Combines pool ID and PG number
///
/// This type is defined in the rados-crush crate and has Denc encoding implemented
/// in the rados-denc crate's crush_types module.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct PgId {
    /// Pool ID
    pub pool: u64,
    /// PG seed/number within the pool
    pub seed: u32,
}

impl PgId {
    /// Create a new PG ID
    pub fn new(pool: u64, seed: u32) -> Self {
        PgId { pool, seed }
    }
}

impl std::fmt::Display for PgId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{:x}", self.pool, self.seed)
    }
}

/// Encoding format matches pg_t::encode() in ~/dev/ceph/src/osd/osd_types.h
impl Denc for PgId {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        1u8.encode(buf, 0)?; // version
        self.pool.encode(buf, 0)?;
        self.seed.encode(buf, 0)?;
        (-1i32).encode(buf, 0)?; // deprecated preferred field
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let version = u8::decode(buf, 0)?;
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported PgId version: {}",
                version
            )));
        }

        let pool = u64::decode(buf, 0)?;
        let seed = u32::decode(buf, 0)?;
        let _preferred = i32::decode(buf, 0)?;

        Ok(PgId { pool, seed })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(Self::SIZE)
    }
}

impl crate::FixedSize for PgId {
    const SIZE: usize = 17;
}

/// Matches C++ object_locator_t encoding (v6, Quincy v17+)
impl VersionedEncode for ObjectLocator {
    fn encoding_version(&self, _features: u64) -> u8 {
        6
    }

    fn compat_version(&self, _features: u64) -> u8 {
        if self.hash != -1 { 6 } else { 3 }
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        if self.hash != -1 && !self.key.is_empty() {
            return Err(RadosError::Protocol(
                "ObjectLocator: cannot have both hash and key set".into(),
            ));
        }

        self.pool_id.encode(buf, features)?;
        (-1i32).encode(buf, features)?; // deprecated preferred field

        self.key.encode(buf, features)?;
        self.namespace.encode(buf, features)?;
        self.hash.encode(buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        // Minimum supported project release boundary (Quincy v17+)
        crate::denc::check_min_version!(struct_v, 6, "ObjectLocator", "Quincy v17+");

        let pool_id = i64::decode(buf, features)? as u64;
        let _preferred = i32::decode(buf, features)?;
        let key = String::decode(buf, features)?;
        let namespace = String::decode(buf, features)?;
        let hash = i64::decode(buf, features)?;

        if hash != -1 && !key.is_empty() {
            return Err(RadosError::Protocol(
                "ObjectLocator: cannot have both hash and key set".into(),
            ));
        }

        Ok(Self {
            pool_id,
            key,
            namespace,
            hash,
        })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        // i64 pool + i32 preferred + key + namespace + i64 hash
        Some(8 + 4 + self.key.encoded_size(features)? + self.namespace.encoded_size(features)? + 8)
    }
}

crate::denc::impl_denc_for_versioned!(ObjectLocator);

/// Calculate PG ID from object name
///
/// This hashes the object name and maps it to a placement group
/// within the specified pool using Ceph's rjenkins hash function.
///
/// # Arguments
/// * `object_name` - Name of the object
/// * `locator` - Object locator with pool and namespace info
/// * `pg_num` - Number of PGs in the pool
///
/// # Returns
/// PG ID (pool + seed)
pub fn object_to_pg(object_name: &str, locator: &ObjectLocator, pg_num: u32) -> PgId {
    use crate::crush::hash::ceph_str_hash_rjenkins;

    // Determine what to hash
    let hash_key = if !locator.key.is_empty() {
        locator.key.as_str()
    } else {
        object_name
    };

    // Hash the key, prepending "namespace\n" when a namespace is set.
    // Avoids allocation in the common (empty namespace) case.
    let hash = if locator.namespace.is_empty() {
        ceph_str_hash_rjenkins(hash_key.as_bytes())
    } else {
        let hash_input = format!("{}\n{}", locator.namespace, hash_key);
        ceph_str_hash_rjenkins(hash_input.as_bytes())
    };

    // Map to PG number using modulo
    let pg_seed = hash % pg_num;

    PgId::new(locator.pool_id, pg_seed)
}

/// Map a PG to OSDs using CRUSH
///
/// This is the main function that uses the CRUSH algorithm to determine
/// which OSDs should store data for a given placement group.
///
/// # Arguments
/// * `crush_map` - The CRUSH map
/// * `pg` - Placement group ID
/// * `rule_id` - CRUSH rule to use (from pool configuration)
/// * `osd_weights` - OSD weights (from OSDMap)
/// * `result_max` - Maximum number of OSDs to return (typically pool size)
/// * `hashpspool` - Whether the pool has hashpspool flag (modern pools)
///
/// # Returns
/// Vector of OSD IDs (first is primary)
pub fn pg_to_osds(
    crush_map: &CrushMap,
    pg: PgId,
    rule_id: u32,
    osd_weights: &[u32],
    result_max: usize,
    hashpspool: bool,
) -> Result<Vec<i32>> {
    let mut result = Vec::new();

    // Calculate placement seed (PS) for CRUSH
    // When hashpspool flag is set (modern pools), hash the PG seed with pool ID
    // Reference: ~/dev/ceph/src/osd/osd_types.cc pg_pool_t::raw_pg_to_pps()
    let x = if hashpspool {
        // Hash PG seed with pool ID to avoid PG overlap between pools
        // Ceph uses: crush_hash32_2(CRUSH_HASH_RJENKINS1, pg.seed, pool_id)
        // Our Rust hash functions are already specialized to rjenkins1
        use crate::crush::hash::crush_hash32_2;
        crush_hash32_2(pg.seed, pg.pool as u32)
    } else {
        // Legacy: just use PG seed directly
        pg.seed
    };

    // Execute the CRUSH rule
    crush_do_rule(crush_map, rule_id, x, &mut result, result_max, osd_weights)?;

    Ok(result)
}

/// Map an object directly to OSDs
///
/// This is a convenience function that combines object_to_pg and pg_to_osds.
///
/// # Arguments
/// * `crush_map` - The CRUSH map
/// * `object_name` - Name of the object
/// * `locator` - Object locator with pool info
/// * `pg_num` - Number of PGs in the pool
/// * `rule_id` - CRUSH rule to use
/// * `osd_weights` - OSD weights
/// * `result_max` - Maximum number of OSDs to return
/// * `hashpspool` - Whether the pool has hashpspool flag
///
/// # Returns
/// Tuple of (PG ID, Vector of OSD IDs)
#[allow(clippy::too_many_arguments)]
pub fn object_to_osds(
    crush_map: &CrushMap,
    object_name: &str,
    locator: &ObjectLocator,
    pg_num: u32,
    rule_id: u32,
    osd_weights: &[u32],
    result_max: usize,
    hashpspool: bool,
) -> Result<(PgId, Vec<i32>)> {
    // First, map object to PG
    let pg = object_to_pg(object_name, locator, pg_num);

    // Then, map PG to OSDs
    let osds = pg_to_osds(crush_map, pg, rule_id, osd_weights, result_max, hashpspool)?;

    Ok((pg, osds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crush::types::{
        BucketAlgorithm, BucketData, CrushBucket, CrushRule, CrushRuleStep, RuleOp, RuleType,
    };

    #[test]
    fn test_object_locator() {
        let loc1 = ObjectLocator::new(1);
        assert_eq!(loc1.pool_id, 1);
        assert_eq!(loc1.namespace, "");
        assert_eq!(loc1.key, "");

        let loc2 = ObjectLocator::with_namespace(2, "ns1".to_string());
        assert_eq!(loc2.pool_id, 2);
        assert_eq!(loc2.namespace, "ns1");

        let loc3 = ObjectLocator::with_key(3, "key1".to_string());
        assert_eq!(loc3.pool_id, 3);
        assert_eq!(loc3.key, "key1");
    }

    #[test]
    fn test_pg_id() {
        let pg = PgId::new(1, 0x2a);
        assert_eq!(pg.pool, 1);
        assert_eq!(pg.seed, 0x2a);
        assert_eq!(format!("{}", pg), "1.2a");
    }

    #[test]
    fn test_object_to_pg() {
        let locator = ObjectLocator::new(1);

        // Same object should always map to same PG
        let pg1 = object_to_pg("myobject", &locator, 100);
        let pg2 = object_to_pg("myobject", &locator, 100);
        assert_eq!(pg1, pg2);
        assert_eq!(pg1.pool, 1);
        assert!(pg1.seed < 100);

        // Different objects should (usually) map to different PGs
        let pg3 = object_to_pg("otherobject", &locator, 100);
        assert_eq!(pg3.pool, 1);
        assert!(pg3.seed < 100);
    }

    #[test]
    fn test_object_to_pg_with_namespace() {
        let loc1 = ObjectLocator::new(1);
        let loc2 = ObjectLocator::with_namespace(1, "ns1".to_string());

        // Same object in different namespaces should map to different PGs
        let pg1 = object_to_pg("myobject", &loc1, 100);
        let pg2 = object_to_pg("myobject", &loc2, 100);

        // They might be the same by chance, but the hash input is different
        assert_eq!(pg1.pool, pg2.pool);
    }

    #[test]
    fn test_pg_to_osds() {
        // Create a simple CRUSH map
        let mut map = CrushMap::new();
        map.max_devices = 3;
        map.max_buckets = 1;

        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0, // CRUSH_HASH_RJENKINS1
            weight: 0x30000,
            size: 3,
            items: vec![0, 1, 2],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        let rule = CrushRule {
            rule_id: 0,
            rule_type: RuleType::Replicated,
            steps: vec![
                CrushRuleStep {
                    op: RuleOp::Take,
                    arg1: -1,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::ChooseLeafFirstN,
                    arg1: 2, // Select 2 OSDs
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::Emit,
                    arg1: 0,
                    arg2: 0,
                },
            ],
        };

        map.rules = vec![Some(rule)];

        let pg = PgId::new(1, 42);
        let weights = vec![0x10000, 0x10000, 0x10000];

        let result = pg_to_osds(&map, pg, 0, &weights, 2, true);
        assert!(result.is_ok());

        let osds = result.unwrap();
        assert!(osds.len() <= 2);

        // OSDs should be valid
        for &osd in &osds {
            assert!((0..3).contains(&osd));
        }

        // OSDs should be distinct
        if osds.len() == 2 {
            assert_ne!(osds[0], osds[1]);
        }
    }

    #[test]
    fn test_object_to_osds() {
        // Create a simple CRUSH map
        let mut map = CrushMap::new();
        map.max_devices = 2;
        map.max_buckets = 1;

        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0, // CRUSH_HASH_RJENKINS1
            weight: 0x20000,
            size: 2,
            items: vec![0, 1],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        let rule = CrushRule {
            rule_id: 0,
            rule_type: RuleType::Replicated,
            steps: vec![
                CrushRuleStep {
                    op: RuleOp::Take,
                    arg1: -1,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::ChooseLeafFirstN,
                    arg1: 1,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::Emit,
                    arg1: 0,
                    arg2: 0,
                },
            ],
        };

        map.rules = vec![Some(rule)];

        let locator = ObjectLocator::new(1);
        let weights = vec![0x10000, 0x10000];

        let result = object_to_osds(&map, "testobject", &locator, 100, 0, &weights, 1, true);
        assert!(result.is_ok());

        let (pg, osds) = result.unwrap();
        assert_eq!(pg.pool, 1);
        assert!(pg.seed < 100);
        assert_eq!(osds.len(), 1);
        assert!(osds[0] == 0 || osds[0] == 1);

        // Same object should always map to same PG and OSDs
        let result2 = object_to_osds(&map, "testobject", &locator, 100, 0, &weights, 1, true);
        let (pg2, osds2) = result2.unwrap();
        assert_eq!(pg, pg2);
        assert_eq!(osds, osds2);
    }

    #[test]
    fn test_pg_distribution() {
        // Test that objects are distributed across PGs
        let locator = ObjectLocator::new(1);
        let pg_num = 10;

        let mut pg_counts = vec![0; pg_num as usize];

        // Map 100 objects and count PG distribution
        for i in 0..100 {
            let object_name = format!("object_{}", i);
            let pg = object_to_pg(&object_name, &locator, pg_num);
            pg_counts[pg.seed as usize] += 1;
        }

        // Each PG should have at least one object (with high probability)
        let empty_pgs = pg_counts.iter().filter(|&&count| count == 0).count();
        assert!(
            empty_pgs < 3,
            "Too many empty PGs: {} out of {}",
            empty_pgs,
            pg_num
        );
    }
}
