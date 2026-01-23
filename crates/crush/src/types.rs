/// CRUSH bucket selection algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BucketAlgorithm {
    Uniform = 1,
    List = 2,
    Tree = 3,
    Straw = 4,
    Straw2 = 5,
}

impl TryFrom<u8> for BucketAlgorithm {
    type Error = crate::error::CrushError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(BucketAlgorithm::Uniform),
            2 => Ok(BucketAlgorithm::List),
            3 => Ok(BucketAlgorithm::Tree),
            4 => Ok(BucketAlgorithm::Straw),
            5 => Ok(BucketAlgorithm::Straw2),
            _ => Err(crate::error::CrushError::InvalidBucketAlgorithm(value)),
        }
    }
}

/// CRUSH rule types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RuleType {
    Replicated = 1,
    Erasure = 3,
    MsrFirstN = 4,
    MsrIndep = 5,
}

impl From<u8> for RuleType {
    fn from(value: u8) -> Self {
        match value {
            1 => RuleType::Replicated,
            3 => RuleType::Erasure,
            4 => RuleType::MsrFirstN,
            5 => RuleType::MsrIndep,
            _ => RuleType::Replicated, // Default to replicated
        }
    }
}

/// CRUSH rule operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl TryFrom<u32> for RuleOp {
    type Error = crate::error::CrushError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RuleOp::Noop),
            1 => Ok(RuleOp::Take),
            2 => Ok(RuleOp::ChooseFirstN),
            3 => Ok(RuleOp::ChooseIndep),
            4 => Ok(RuleOp::Emit),
            6 => Ok(RuleOp::ChooseLeafFirstN),
            7 => Ok(RuleOp::ChooseLeafIndep),
            8 => Ok(RuleOp::SetChooseTries),
            9 => Ok(RuleOp::SetChooseLeafTries),
            10 => Ok(RuleOp::SetChooseLocalTries),
            11 => Ok(RuleOp::SetChooseLocalFallbackTries),
            12 => Ok(RuleOp::SetChooseLeafVaryR),
            13 => Ok(RuleOp::SetChooseLeafStable),
            14 => Ok(RuleOp::SetMsrDescents),
            15 => Ok(RuleOp::SetMsrCollisionTries),
            16 => Ok(RuleOp::ChooseMsr),
            _ => Err(crate::error::CrushError::InvalidRuleOp(value)),
        }
    }
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
    pub type_names: std::collections::HashMap<i32, String>,
    /// Bucket/device names (id -> name)
    pub names: std::collections::HashMap<i32, String>,
    /// Rule names (rule_id -> name)
    pub rule_names: std::collections::HashMap<u32, String>,
    /// Tunables
    pub choose_local_tries: u32,
    pub choose_local_fallback_tries: u32,
    pub choose_total_tries: u32,
    pub chooseleaf_descend_once: u32,
    pub chooseleaf_vary_r: u8,
    pub chooseleaf_stable: u8,
    pub allowed_bucket_algs: u32,
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
            type_names: std::collections::HashMap::new(),
            names: std::collections::HashMap::new(),
            rule_names: std::collections::HashMap::new(),
            choose_local_tries: 2,
            choose_local_fallback_tries: 5,
            choose_total_tries: 19,
            chooseleaf_descend_once: 0,
            chooseleaf_vary_r: 0,
            chooseleaf_stable: 0,
            allowed_bucket_algs: 0,
        }
    }

    /// Get a bucket by ID
    pub fn get_bucket(&self, id: i32) -> crate::error::Result<&CrushBucket> {
        if id >= 0 {
            return Err(crate::error::CrushError::InvalidBucketId(id));
        }
        let index = (-1 - id) as usize;
        self.buckets
            .get(index)
            .and_then(|b| b.as_ref())
            .ok_or(crate::error::CrushError::BucketNotFound(id))
    }

    /// Get a rule by ID
    pub fn get_rule(&self, rule_id: u32) -> crate::error::Result<&CrushRule> {
        self.rules
            .get(rule_id as usize)
            .and_then(|r| r.as_ref())
            .ok_or(crate::error::CrushError::RuleNotFound(rule_id))
    }
}

impl Default for CrushMap {
    fn default() -> Self {
        Self::new()
    }
}
