use thiserror::Error;

#[derive(Error, Debug)]
pub enum CrushError {
    #[error("Invalid bucket ID: {0}")]
    InvalidBucketId(i32),

    #[error("Invalid rule ID: {0}")]
    InvalidRuleId(u32),

    #[error("Invalid bucket algorithm: {0}")]
    InvalidBucketAlgorithm(u8),

    #[error("Bucket not found: {0}")]
    BucketNotFound(i32),

    #[error("Rule not found: {0}")]
    RuleNotFound(u32),

    #[error("Invalid rule step operation: {0}")]
    InvalidRuleOp(u32),

    #[error("Decode error: {0}")]
    DecodeError(String),

    #[error("No valid OSDs found")]
    NoValidOsds,

    #[error("Invalid weight: {0}")]
    InvalidWeight(u32),
}

pub type Result<T> = std::result::Result<T, CrushError>;
