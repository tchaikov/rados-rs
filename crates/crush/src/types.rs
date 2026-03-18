//! Core CRUSH map types describing buckets, rules, and placement metadata.
//!
//! This module defines the in-memory schema for decoded CRUSH maps, including
//! bucket algorithms, rule steps, bucket payloads, and the top-level
//! [`CrushMap`]. Higher-level placement code uses these types to inspect map
//! structure and to drive rule execution without depending on Ceph's C++
//! headers directly.

use std::collections::HashMap;

use crate::CrushError;
use num_enum::TryFromPrimitive;

/// CRUSH bucket selection algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u8)]
pub enum BucketAlgorithm {
    Uniform = 1,
    List = 2,
    Tree = 3,
    Straw = 4,
    Straw2 = 5,
}

/// CRUSH rule types
#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u8)]
pub enum RuleType {
    Replicated = 1,
    Erasure = 3,
    MsrFirstN = 4,
    MsrIndep = 5,
}

/// CRUSH rule operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u32)]
pub enum RuleOp {
    Noop = 0,
    Take = 1,
    ChooseFirstN = 2,
    ChooseIndep = 3,
    Emit = 4,
    ChooseLeafFirstN = 6,
    ChooseLeafIndep = 7,
    SetChooseTries = 8,
    SetChooseLeafTries = 9,
    SetChooseLocalTries = 10,
    SetChooseLocalFallbackTries = 11,
    SetChooseLeafVaryR = 12,
    SetChooseLeafStable = 13,
    SetMsrDescents = 14,
    SetMsrCollisionTries = 15,
    ChooseMsr = 16,
}

/// A single step in a CRUSH rule
#[derive(Debug, Clone)]
pub struct CrushRuleStep {
    pub op: RuleOp,
    pub arg1: i32,
    pub arg2: i32,
}

/// A CRUSH rule for mapping PGs to OSDs
#[derive(Debug, Clone)]
pub struct CrushRule {
    pub rule_id: u32,
    pub rule_type: RuleType,
    pub steps: Vec<CrushRuleStep>,
}

/// Algorithm-specific bucket data
#[derive(Debug, Clone)]
pub enum BucketData {
    /// Uniform bucket - all items have equal weight
    Uniform { item_weight: u32 },
    /// List bucket - items in a linked list
    List {
        item_weights: Vec<u32>,
        sum_weights: Vec<u32>,
    },
    /// Tree bucket - binary tree structure
    Tree {
        num_nodes: u32,
        node_weights: Vec<u32>,
    },
    /// Straw bucket (legacy)
    Straw {
        item_weights: Vec<u32>,
        straws: Vec<u32>,
    },
    /// Straw2 bucket (modern, optimal)
    Straw2 { item_weights: Vec<u32> },
}

/// A CRUSH bucket containing items (devices or other buckets)
#[derive(Debug, Clone)]
pub struct CrushBucket {
    /// Bucket ID (negative for buckets)
    pub id: i32,
    /// Bucket type in hierarchy (e.g., root, datacenter, rack, host)
    pub bucket_type: i32,
    /// Selection algorithm
    pub alg: BucketAlgorithm,
    /// Hash function type (CRUSH_HASH_RJENKINS1 = 0)
    pub hash: u8,
    /// Total weight (16.16 fixed-point)
    pub weight: u32,
    /// Number of items
    pub size: u32,
    /// Item IDs (negative = buckets, >= 0 = devices)
    pub items: Vec<i32>,
    /// Algorithm-specific data
    pub data: BucketData,
}

/// Main CRUSH map structure
#[derive(Debug, Clone)]
pub struct CrushMap {
    /// Maximum number of buckets
    pub max_buckets: i32,
    /// Maximum number of devices
    pub max_devices: i32,
    /// Maximum number of rules
    pub max_rules: u32,
    /// Buckets array (indexed by -1 - bucket_id)
    pub buckets: Vec<Option<CrushBucket>>,
    /// Rules array
    pub rules: Vec<Option<CrushRule>>,
    /// Type names (type_id -> name)
    pub type_names: HashMap<i32, String>,
    /// Bucket/device names (id -> name)
    pub names: HashMap<i32, String>,
    /// Rule names (rule_id -> name)
    pub rule_names: HashMap<u32, String>,
    /// Tunables
    pub choose_local_tries: u32,
    pub choose_local_fallback_tries: u32,
    pub choose_total_tries: u32,
    pub chooseleaf_descend_once: u32,
    pub chooseleaf_vary_r: u8,
    pub chooseleaf_stable: u8,
    pub allowed_bucket_algs: u32,
    /// Device classes (Luminous+)
    /// Maps device/OSD ID to class ID
    pub class_map: HashMap<i32, i32>,
    /// Maps class ID to class name (e.g., "ssd", "hdd", "nvme")
    pub class_name: HashMap<i32, String>,
    /// Shadow bucket mappings: `bucket[id][class_id] = shadow_bucket_id`
    /// Used for device class-specific CRUSH tree shadows
    pub class_bucket: HashMap<i32, HashMap<i32, i32>>,
}

impl CrushMap {
    /// Create a new empty CRUSH map
    pub fn new() -> Self {
        CrushMap {
            max_buckets: 0,
            max_devices: 0,
            max_rules: 0,
            buckets: Vec::new(),
            rules: Vec::new(),
            type_names: HashMap::new(),
            names: HashMap::new(),
            rule_names: HashMap::new(),
            choose_local_tries: 2,
            choose_local_fallback_tries: 5,
            choose_total_tries: 19,
            chooseleaf_descend_once: 0,
            chooseleaf_vary_r: 0,
            chooseleaf_stable: 0,
            allowed_bucket_algs: 0,
            class_map: HashMap::new(),
            class_name: HashMap::new(),
            class_bucket: HashMap::new(),
        }
    }

    /// Get a bucket by ID
    pub fn get_bucket(&self, id: i32) -> crate::Result<&CrushBucket> {
        if id >= 0 {
            return Err(CrushError::InvalidBucketId(id));
        }
        let index = (-1 - id) as usize;
        self.buckets
            .get(index)
            .and_then(|b| b.as_ref())
            .ok_or(CrushError::BucketNotFound(id))
    }

    /// Get a rule by ID
    pub fn get_rule(&self, rule_id: u32) -> crate::Result<&CrushRule> {
        self.rules
            .get(rule_id as usize)
            .and_then(|r| r.as_ref())
            .ok_or(CrushError::RuleNotFound(rule_id))
    }

    /// Get the device class name for a given device/OSD ID
    ///
    /// Returns None if the device has no class assigned
    pub fn get_device_class(&self, device_id: i32) -> Option<&str> {
        self.class_map
            .get(&device_id)
            .and_then(|class_id| self.class_name.get(class_id))
            .map(|s| s.as_str())
    }

    /// Get the class ID for a given class name
    ///
    /// Returns None if the class name doesn't exist
    pub fn get_class_id(&self, class_name: &str) -> Option<i32> {
        self.class_name
            .iter()
            .find(|(_, name)| name.as_str() == class_name)
            .map(|(id, _)| *id)
    }

    /// Check if a device belongs to a specific class
    pub fn device_has_class(&self, device_id: i32, class_name: &str) -> bool {
        self.get_device_class(device_id) == Some(class_name)
    }
}

impl Default for CrushMap {
    fn default() -> Self {
        Self::new()
    }
}
