//! Ceph Messenger v2 Protocol Implementation
//!
//! This crate implements the Ceph msgr2.1 protocol for communication
//! with Ceph clusters, incorporating our latest protocol discoveries.

// Core modules that work
pub mod banner;
pub mod crypto;
pub mod error;
pub mod frames;
pub mod header;
pub mod message;
pub mod protocol;
pub mod state_machine;

// Re-export working components
pub use banner::*;
pub use error::*;
pub use frames::*;
pub use header::*;
pub use message::*;

// MSGR2 Protocol features (from include/msgr.h)
/// msgr2.1 protocol revision
pub const MSGR2_FEATURE_REVISION_1: u64 = 1 << 0; // bit 0
/// On-wire compression support
pub const MSGR2_FEATURE_COMPRESSION: u64 = 1 << 1; // bit 1

/// All msgr2 features (REVISION_1 | COMPRESSION)
pub const MSGR2_ALL_FEATURES: u64 = MSGR2_FEATURE_REVISION_1 | MSGR2_FEATURE_COMPRESSION;

/// Features we support
pub const MSGR2_SUPPORTED_FEATURES: u64 = MSGR2_FEATURE_REVISION_1 | MSGR2_FEATURE_COMPRESSION;

/// Features we require (empty for now)
pub const MSGR2_REQUIRED_FEATURES: u64 = 0;

/// Check if a feature is supported
pub fn has_msgr2_feature(features: u64, feature: u64) -> bool {
    (features & feature) == feature
}

/// Authentication method enum (from include/ceph_fs.h)
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u32)]
pub enum AuthMethod {
    Unknown = 0x0,
    None = 0x1,
    Cephx = 0x2,
    Gss = 0x4,
}

impl AuthMethod {
    pub fn as_u32(self) -> u32 {
        self.into()
    }
}

/// Connection mode enum
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u32)]
pub enum ConnectionMode {
    Unknown = 0x0,
    Crc = 0x1,
    Secure = 0x2,
}

impl ConnectionMode {
    pub fn as_u32(self) -> u32 {
        self.into()
    }
}

// FeatureSet wrapper type for msgr2 features
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureSet(u64);

impl FeatureSet {
    pub const EMPTY: Self = Self(0);
    pub const MSGR2: Self = Self(MSGR2_SUPPORTED_FEATURES);

    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    pub fn has_feature(self, feature: u64) -> bool {
        (self.0 & feature) != 0
    }

    pub fn with_feature(mut self, feature: u64) -> Self {
        self.0 |= feature;
        self
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl std::ops::BitAnd for FeatureSet {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

impl std::ops::BitOr for FeatureSet {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::Not for FeatureSet {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self(!self.0)
    }
}

impl denc::zerocopy::Encode for FeatureSet {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
    ) -> std::result::Result<(), denc::zerocopy::EncodeError> {
        self.0.encode(buf)
    }

    fn encoded_size(&self) -> usize {
        self.0.encoded_size()
    }
}

impl denc::zerocopy::Decode for FeatureSet {
    fn decode<B: bytes::Buf>(
        buf: &mut B,
    ) -> std::result::Result<Self, denc::zerocopy::DecodeError> {
        Ok(Self(u64::decode(buf)?))
    }
}

/// Configuration for msgr2 connection behavior
///
/// # Authentication
///
/// The authentication method can be configured in two ways (in order of precedence):
///
/// 1. **Explicitly via `auth_method` field**: Set to `Some(AuthMethod::None)` or `Some(AuthMethod::Cephx)`
/// 2. **Auto-detection**: If not explicitly set, auto-detects based on keyring file existence
///
/// ## Auto-detection behavior:
/// - Checks if `CEPH_KEYRING` environment variable is set and points to an existing file
/// - Falls back to checking `/etc/ceph/ceph.client.admin.keyring`
/// - If keyring file exists → uses `AuthMethod::Cephx`
/// - If keyring file doesn't exist → uses `AuthMethod::None`
///
/// **Note**: Applications can check environment variables (e.g., `CEPH_AUTH_METHOD`) and
/// explicitly set the `auth_method` field to override auto-detection.
///
/// # Example
///
/// ```rust,no_run
/// use msgr2::ConnectionConfig;
///
/// // Auto-detect authentication (recommended)
/// let config = ConnectionConfig::default();
///
/// // Explicitly use no authentication
/// let config = ConnectionConfig::with_no_auth();
///
/// // Explicitly use CephX authentication
/// let config = ConnectionConfig::with_cephx_auth();
/// ```
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Features to advertise as supported in banner exchange
    /// Default: MSGR2_ALL_FEATURES (REVISION_1 | COMPRESSION)
    pub supported_features: u64,

    /// Features to require from peer
    /// Default: 0 (no required features)
    pub required_features: u64,

    /// Preferred connection modes for authentication (in order of preference)
    /// Default: vec![ConnectionMode::Secure, ConnectionMode::Crc] (prefer encryption)
    /// The server will choose the final mode from this list
    pub preferred_modes: Vec<ConnectionMode>,

    /// Authentication method to use
    /// - `None`: Auto-detect from keyring file existence
    /// - `Some(AuthMethod::None)`: Force no authentication
    /// - `Some(AuthMethod::Cephx)`: Force CephX authentication
    ///
    /// See struct-level documentation for auto-detection behavior.
    pub auth_method: Option<AuthMethod>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            supported_features: MSGR2_ALL_FEATURES,
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Secure, ConnectionMode::Crc],
            auth_method: None, // Auto-detect
        }
    }
}

impl ConnectionConfig {
    /// Create config with compression disabled
    pub fn without_compression() -> Self {
        Self {
            supported_features: MSGR2_FEATURE_REVISION_1,
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Secure, ConnectionMode::Crc],
            auth_method: None,
        }
    }

    /// Create config with compression enabled (default)
    pub fn with_compression() -> Self {
        Self::default()
    }

    /// Create config preferring CRC mode (no encryption)
    pub fn prefer_crc_mode() -> Self {
        Self {
            supported_features: MSGR2_ALL_FEATURES,
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Crc],
            auth_method: None,
        }
    }

    /// Create config preferring SECURE mode (with encryption)
    pub fn prefer_secure_mode() -> Self {
        Self {
            supported_features: MSGR2_ALL_FEATURES,
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Secure],
            auth_method: None,
        }
    }

    /// Create config with both compression and encryption disabled
    pub fn minimal() -> Self {
        Self {
            supported_features: MSGR2_FEATURE_REVISION_1,
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Crc],
            auth_method: None,
        }
    }

    /// Create config with custom features and modes
    pub fn custom(supported: u64, required: u64, modes: Vec<ConnectionMode>) -> Self {
        Self {
            supported_features: supported,
            required_features: required,
            preferred_modes: modes,
            auth_method: None,
        }
    }

    /// Create config with no authentication
    /// Use this when connecting to a Ceph cluster with auth disabled
    pub fn with_no_auth() -> Self {
        Self {
            supported_features: MSGR2_ALL_FEATURES,
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Crc], // No encryption needed for no-auth
            auth_method: Some(AuthMethod::None),
        }
    }

    /// Create config with CephX authentication
    /// Use this to force CephX even if auto-detection would suggest otherwise
    pub fn with_cephx_auth() -> Self {
        Self {
            supported_features: MSGR2_ALL_FEATURES,
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Secure, ConnectionMode::Crc],
            auth_method: Some(AuthMethod::Cephx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_config_default() {
        let config = ConnectionConfig::default();
        assert_eq!(config.supported_features, MSGR2_ALL_FEATURES);
        assert_eq!(config.required_features, 0);
        assert_eq!(
            config.preferred_modes,
            vec![ConnectionMode::Secure, ConnectionMode::Crc]
        );
        assert!(has_msgr2_feature(
            config.supported_features,
            MSGR2_FEATURE_REVISION_1
        ));
        assert!(has_msgr2_feature(
            config.supported_features,
            MSGR2_FEATURE_COMPRESSION
        ));
    }

    #[test]
    fn test_connection_config_without_compression() {
        let config = ConnectionConfig::without_compression();
        assert_eq!(config.supported_features, MSGR2_FEATURE_REVISION_1);
        assert_eq!(config.required_features, 0);
        assert!(has_msgr2_feature(
            config.supported_features,
            MSGR2_FEATURE_REVISION_1
        ));
        assert!(!has_msgr2_feature(
            config.supported_features,
            MSGR2_FEATURE_COMPRESSION
        ));
    }

    #[test]
    fn test_connection_config_with_compression() {
        let config = ConnectionConfig::with_compression();
        assert_eq!(config.supported_features, MSGR2_ALL_FEATURES);
        assert!(has_msgr2_feature(
            config.supported_features,
            MSGR2_FEATURE_COMPRESSION
        ));
    }

    #[test]
    fn test_connection_config_prefer_crc_mode() {
        let config = ConnectionConfig::prefer_crc_mode();
        assert_eq!(config.preferred_modes, vec![ConnectionMode::Crc]);
        assert_eq!(config.supported_features, MSGR2_ALL_FEATURES);
    }

    #[test]
    fn test_connection_config_prefer_secure_mode() {
        let config = ConnectionConfig::prefer_secure_mode();
        assert_eq!(config.preferred_modes, vec![ConnectionMode::Secure]);
        assert_eq!(config.supported_features, MSGR2_ALL_FEATURES);
    }

    #[test]
    fn test_connection_config_minimal() {
        let config = ConnectionConfig::minimal();
        assert_eq!(config.supported_features, MSGR2_FEATURE_REVISION_1);
        assert_eq!(config.preferred_modes, vec![ConnectionMode::Crc]);
        assert!(!has_msgr2_feature(
            config.supported_features,
            MSGR2_FEATURE_COMPRESSION
        ));
    }

    #[test]
    fn test_connection_config_custom() {
        let config = ConnectionConfig::custom(0x1, 0x0, vec![ConnectionMode::Crc]);
        assert_eq!(config.supported_features, 0x1);
        assert_eq!(config.required_features, 0x0);
        assert_eq!(config.preferred_modes, vec![ConnectionMode::Crc]);
    }
}
