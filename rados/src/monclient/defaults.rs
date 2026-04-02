//! Default configuration values for MonClient
//!
//! This module centralizes timeout and interval constants to improve
//! maintainability and make configuration more discoverable.

use std::time::Duration;

/// Default timeout for monitor commands
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// Default interval between monitor connection attempts during hunting
pub const HUNT_INTERVAL: Duration = Duration::from_secs(3);

/// Default number of monitors to try connecting to in parallel during hunt
/// (0 means try all available monitors)
pub const HUNT_PARALLEL: usize = 3;

/// Default interval for sending keepalive messages
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Default timeout for keepalive ACK responses
/// Set to 0 to disable keepalive timeout checking
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(30);

/// Backoff multiplier applied to hunt_interval after each failed hunt
pub const HUNT_INTERVAL_BACKOFF: f64 = 1.5;

/// Minimum multiplier for hunt interval backoff
pub const HUNT_INTERVAL_MIN_MULTIPLE: f64 = 1.0;

/// Maximum multiplier for hunt interval backoff
pub const HUNT_INTERVAL_MAX_MULTIPLE: f64 = 10.0;
