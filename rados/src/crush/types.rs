//! Core CRUSH map types describing buckets, rules, and placement metadata.
//!
//! This module defines the in-memory schema for decoded CRUSH maps, including
//! bucket algorithms, rule steps, bucket payloads, and the top-level
//! [`CrushMap`]. Higher-level placement code uses these types to inspect map
//! structure and to drive rule execution without depending on Ceph's C++
//! headers directly.

use std::collections::HashMap;

use crate::crush::CrushError;
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
    pub id: i32,
    /// Type in hierarchy (e.g., root, datacenter, rack, host)
    pub bucket_type: i32,
    pub alg: BucketAlgorithm,
    pub hash: u8,
    /// Total weight (16.16 fixed-point)
    pub weight: u32,
    pub size: u32,
    /// Item IDs (negative = buckets, non-negative = devices)
    pub items: Vec<i32>,
    pub data: BucketData,
}

/// Per-bucket alternative STRAW2 inputs selected by a pool or default index.
///
/// IDs affect only the STRAW2 hash; selected values always come from the
/// bucket's canonical `items` array.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrushChooseArg {
    pub weight_set: Vec<Vec<u32>>,
    pub ids: Vec<i32>,
}

/// Opt-in counters for the retry attempts observed during CRUSH placement.
#[derive(Debug, Clone)]
pub struct ChooseProfile {
    bins: Option<Vec<u32>>,
    visible_len: usize,
}

impl ChooseProfile {
    fn new(choose_total_tries: u32) -> Self {
        let visible_len = choose_total_tries as usize;
        Self {
            bins: Some(vec![0; visible_len + 1]),
            visible_len,
        }
    }

    /// Discard earlier observations and start a new profile batch.
    pub fn start_choose_profile(&mut self, choose_total_tries: u32) {
        *self = Self::new(choose_total_tries);
    }

    /// Return the bins visible through Ceph's `get_choose_profile` API.
    pub fn get_choose_profile(&self) -> Option<&[u32]> {
        self.bins.as_deref().map(|bins| &bins[..self.visible_len])
    }

    /// Stop profiling and discard its accumulated observations.
    pub fn stop_choose_profile(&mut self) {
        self.bins = None;
    }

    pub(crate) fn record(&mut self, tries: u32) {
        if let Some(bin) = self
            .bins
            .as_deref_mut()
            .and_then(|bins| bins.get_mut(tries as usize))
        {
            // NOTE: Ceph's profiling bins are u32 counters and wrap on overflow.
            // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L622-L623
            *bin = bin.wrapping_add(1);
        }
    }
}

/// Main CRUSH map structure
#[derive(Debug, Clone)]
pub struct CrushMap {
    pub max_buckets: i32,
    pub max_devices: i32,
    pub max_rules: u32,
    /// Indexed by `-1 - bucket_id`
    pub buckets: Vec<Option<CrushBucket>>,
    pub rules: Vec<Option<CrushRule>>,
    pub type_names: HashMap<i32, String>,
    pub names: HashMap<i32, String>,
    pub rule_names: HashMap<u32, String>,
    pub choose_local_tries: u32,
    pub choose_local_fallback_tries: u32,
    pub choose_total_tries: u32,
    pub chooseleaf_descend_once: u32,
    pub chooseleaf_vary_r: u8,
    pub chooseleaf_stable: u8,
    pub allowed_bucket_algs: u32,
    pub msr_descents: u32,
    pub msr_collision_tries: u32,
    /// Device/OSD ID -> class ID
    pub class_map: HashMap<i32, i32>,
    /// Class ID -> class name (e.g., "ssd", "hdd", "nvme")
    pub class_name: HashMap<i32, String>,
    /// Class name -> class ID (reverse of `class_name`; mirrors C++ `class_rname`)
    pub class_rname: HashMap<String, i32>,
    /// `bucket[id][class_id] = shadow_bucket_id` for device-class tree shadows
    pub class_bucket: HashMap<i32, HashMap<i32, i32>>,
    /// `choose_args[index][bucket_index]`, including empty sets.
    pub choose_args: HashMap<i64, Vec<Option<CrushChooseArg>>>,
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
            msr_descents: 100,
            msr_collision_tries: 100,
            class_map: HashMap::new(),
            class_name: HashMap::new(),
            class_rname: HashMap::new(),
            class_bucket: HashMap::new(),
            choose_args: HashMap::new(),
        }
    }

    /// Start an opt-in retry profile batch for a caller-defined mapper batch.
    pub fn start_choose_profile(&self) -> ChooseProfile {
        ChooseProfile::new(self.choose_total_tries)
    }

    /// Get a bucket by ID
    pub fn get_bucket(&self, id: i32) -> crate::crush::Result<&CrushBucket> {
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
    pub fn get_rule(&self, rule_id: u32) -> crate::crush::Result<&CrushRule> {
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
        self.class_rname.get(class_name).copied()
    }

    /// Check if a device belongs to a specific class
    pub fn device_has_class(&self, device_id: i32, class_name: &str) -> bool {
        self.get_device_class(device_id) == Some(class_name)
    }

    /// Resolve a shadow bucket to its original bucket and optional device class.
    pub fn split_id_class(&self, id: i32) -> crate::crush::Result<(i32, Option<i32>)> {
        if !self.names.contains_key(&id) {
            return Err(CrushError::ItemNotFound(id));
        }
        for (&original, classes) in &self.class_bucket {
            if let Some(class) = classes
                .iter()
                .find_map(|(&class, &shadow)| (shadow == id).then_some(class))
            {
                return Ok((original, Some(class)));
            }
        }
        Ok((id, None))
    }

    pub fn immediate_parent(&self, id: i32) -> crate::crush::Result<(String, String)> {
        for bucket in self.buckets.iter().flatten() {
            if self.is_shadow_bucket(bucket.id) || !bucket.items.contains(&id) {
                continue;
            }
            let name = self
                .names
                .get(&bucket.id)
                .ok_or(CrushError::InvalidHierarchy("parent bucket has no name"))?;
            let bucket_type =
                self.type_names
                    .get(&bucket.bucket_type)
                    .ok_or(CrushError::InvalidHierarchy(
                        "parent bucket has no type name",
                    ))?;
            return Ok((bucket_type.clone(), name.clone()));
        }
        Err(CrushError::ItemNotFound(id))
    }

    pub fn location_ordered(&self, id: i32) -> crate::crush::Result<Vec<(String, String)>> {
        if !self.names.contains_key(&id) {
            return Err(CrushError::ItemNotFound(id));
        }
        let mut path = Vec::new();
        let mut current = id;
        let mut visited = std::collections::HashSet::from([id]);
        loop {
            let parent = match self.immediate_parent(current) {
                Ok(parent) => parent,
                Err(CrushError::ItemNotFound(_)) => break,
                Err(error) => return Err(error),
            };
            let parent_id = self
                .names
                .iter()
                .find_map(|(&parent_id, name)| (name == &parent.1).then_some(parent_id))
                .ok_or(CrushError::InvalidHierarchy("parent name has no ID"))?;
            if !visited.insert(parent_id) {
                return Err(CrushError::InvalidHierarchy("cycle"));
            }
            path.push(parent);
            current = parent_id;
        }
        Ok(path)
    }

    pub fn common_ancestor_distance(
        &self,
        id: i32,
        locations: &[(String, String)],
    ) -> crate::crush::Result<i32> {
        let path = self.location_ordered(id)?;
        self.type_names
            .iter()
            .filter_map(|(&distance, type_name)| {
                path.iter()
                    .find(|(candidate, _)| candidate == type_name)
                    .and_then(|(_, value)| {
                        locations
                            .iter()
                            .any(|(candidate, location)| {
                                candidate == type_name && location == value
                            })
                            .then_some(distance)
                    })
            })
            .min()
            .ok_or(CrushError::NoCommonAncestor)
    }

    pub fn item_at_location_weight(&self, id: i32, location: &[(String, String)]) -> Option<u32> {
        let mut types: Vec<_> = self.type_names.iter().collect();
        types.sort_unstable_by_key(|(id, _)| *id);
        for (&type_id, type_name) in types {
            if type_id == 0 {
                continue;
            }
            let Some((_, bucket_name)) = location
                .iter()
                .find(|(candidate, _)| candidate == type_name)
            else {
                continue;
            };
            let bucket_id = self.names.iter().find_map(|(&bucket_id, candidate)| {
                (candidate == bucket_name).then_some(bucket_id)
            })?;
            let bucket = self.get_bucket(bucket_id).ok()?;
            return bucket
                .items
                .iter()
                .position(|&item| item == id)
                .and_then(|index| bucket_item_weight(bucket, index));
        }
        None
    }

    pub fn type_count(&self) -> usize {
        self.type_names.len()
    }

    pub fn type_id(&self, name: &str) -> Option<i32> {
        self.type_names
            .iter()
            .find_map(|(&id, candidate)| (candidate == name).then_some(id))
    }

    pub fn type_name(&self, id: i32) -> Option<&str> {
        self.type_names.get(&id).map(String::as_str)
    }

    fn is_shadow_bucket(&self, id: i32) -> bool {
        self.names.get(&id).is_some_and(|name| {
            !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
    }
}

fn bucket_item_weight(bucket: &CrushBucket, index: usize) -> Option<u32> {
    match &bucket.data {
        BucketData::Uniform { item_weight } => Some(*item_weight),
        BucketData::List { item_weights, .. }
        | BucketData::Straw { item_weights, .. }
        | BucketData::Straw2 { item_weights } => item_weights.get(index).copied(),
        BucketData::Tree { node_weights, .. } => node_weights.get((index + 1) * 2 - 1).copied(),
    }
}

impl Default for CrushMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod profile_tests {
    use super::ChooseProfile;

    #[test]
    fn retry_counter_wraps_like_ceph() {
        let mut profile = ChooseProfile::new(1);
        profile.bins.as_mut().unwrap()[0] = u32::MAX;
        profile.record(0);
        assert_eq!(profile.get_choose_profile(), Some([0].as_slice()));
    }
}
