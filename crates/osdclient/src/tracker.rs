//! Request tracker for timeout handling

use std::time::Duration;

/// Configuration for request tracking
#[derive(Debug, Clone)]
pub struct TrackerConfig {
    /// Default operation timeout
    pub operation_timeout: Duration,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(30),
        }
    }
}

/// Request tracker for handling timeouts
pub struct Tracker {
    config: TrackerConfig,
}

impl Tracker {
    pub fn new(config: TrackerConfig) -> Self {
        Self { config }
    }

    pub fn operation_timeout(&self) -> Duration {
        self.config.operation_timeout
    }
}
