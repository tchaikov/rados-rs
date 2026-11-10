use thiserror::Error;

#[derive(Error, Debug)]
pub enum CrushError {
    #[error("Item not found: {0}")]
    ItemNotFound(i32),

    #[error("Invalid CRUSH hierarchy: {0}")]
    InvalidHierarchy(&'static str),

    #[error("No common ancestor")]
    NoCommonAncestor,

    #[error("Invalid bucket ID: {0}")]
    InvalidBucketId(i32),

    #[error("Invalid bucket algorithm: {0}")]
    InvalidBucketAlgorithm(u8),

    #[error("Bucket not found: {0}")]
    BucketNotFound(i32),

    #[error("Rule not found: {0}")]
    RuleNotFound(u32),

    #[error("Invalid rule type: {0}")]
    InvalidRuleType(u8),

    #[error("MSR fanout must be nonnegative: {0}")]
    InvalidMsrFanout(i32),

    #[error("Invalid rule execution state: {0}")]
    InvalidRuleState(&'static str),

    #[error("Decode error: {0}")]
    DecodeError(String),

    #[error(transparent)]
    Denc(#[from] crate::RadosError),
}

pub type Result<T> = std::result::Result<T, CrushError>;
