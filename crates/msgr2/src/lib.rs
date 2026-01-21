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
/// Authentication is configured using the `auth_provider` field, which accepts
/// an implementation of the `AuthProvider` trait:
///
/// - **`MonitorAuthProvider`**: Full CephX authentication for monitor connections
/// - **`ServiceAuthProvider`**: Authorizer-based authentication for OSD/MDS/MGR connections
/// - **`None`**: No authentication (for clusters with auth disabled)
///
/// ## Auto-detection behavior (Default):
/// - Checks if `CEPH_KEYRING` environment variable points to an existing file
/// - Falls back to checking `/etc/ceph/ceph.client.admin.keyring`
/// - If keyring file exists → `supported_auth_methods = [Cephx, None]`
/// - If keyring file doesn't exist → `supported_auth_methods = [None]`
///
/// # Example
///
/// ```rust,no_run
/// use msgr2::ConnectionConfig;
/// use auth::MonitorAuthProvider;
///
/// // No authentication (development/testing)
/// let config = ConnectionConfig::with_no_auth();
///
/// // CephX authentication with auth provider
/// let mut mon_auth = MonitorAuthProvider::new("client.admin".to_string())?;
/// mon_auth.set_secret_key_from_base64("AQA...")?;
/// let config = ConnectionConfig::with_auth_provider(Box::new(mon_auth));
///
/// // Custom: prefer CephX but fall back to no-auth
/// let mut config = ConnectionConfig::default();
/// config.supported_auth_methods = vec![AuthMethod::Cephx, AuthMethod::None];
/// # Ok::<(), Box<dyn std::error::Error>>(())
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

    /// Authentication methods supported by the client (in order of preference)
    /// The client will negotiate with the server to select a mutually supported method.
    /// Default: vec![AuthMethod::Cephx, AuthMethod::None] (prefer CephX if available)
    ///
    /// Examples:
    /// - vec![AuthMethod::None] - Only support no-auth clusters
    /// - vec![AuthMethod::Cephx] - Only support CephX auth clusters
    /// - vec![AuthMethod::Cephx, AuthMethod::None] - Try CephX first, fall back to None
    pub supported_auth_methods: Vec<AuthMethod>,

    /// Authentication provider for handling CephX authentication
    /// - `None`: Use AuthMethod::None (no authentication)
    /// - `Some(provider)`: Use the provided authentication strategy
    ///
    /// Use `MonitorAuthProvider` for monitor connections (full CephX)
    /// Use `ServiceAuthProvider` for OSD/MDS/MGR connections (authorizer-based)
    pub auth_provider: Option<Box<dyn auth::AuthProvider>>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        // Auto-detect keyring path from environment or use default
        let keyring_exists = std::env::var("CEPH_KEYRING")
            .ok()
            .map(|p| std::path::Path::new(&p).exists())
            .unwrap_or_else(|| {
                std::path::Path::new("/etc/ceph/ceph.client.admin.keyring").exists()
            });

        // Default: try CephX first if keyring exists, otherwise fall back to None
        let supported_auth_methods = if keyring_exists {
            vec![AuthMethod::Cephx, AuthMethod::None]
        } else {
            vec![AuthMethod::None]
        };

        Self {
            supported_features: MSGR2_ALL_FEATURES,
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Secure, ConnectionMode::Crc],
            supported_auth_methods,
            auth_provider: None,
        }
    }
}

impl ConnectionConfig {
    /// Create config with compression disabled
    pub fn without_compression() -> Self {
        Self {
            supported_features: MSGR2_FEATURE_REVISION_1,
            ..Default::default()
        }
    }

    /// Create config with compression enabled (default)
    pub fn with_compression() -> Self {
        Self::default()
    }

    /// Create config preferring CRC mode (no encryption)
    pub fn prefer_crc_mode() -> Self {
        Self {
            preferred_modes: vec![ConnectionMode::Crc],
            ..Default::default()
        }
    }

    /// Create config preferring SECURE mode (with encryption)
    pub fn prefer_secure_mode() -> Self {
        Self {
            preferred_modes: vec![ConnectionMode::Secure],
            ..Default::default()
        }
    }

    /// Create config with both compression and encryption disabled
    pub fn minimal() -> Self {
        Self {
            supported_features: MSGR2_FEATURE_REVISION_1,
            preferred_modes: vec![ConnectionMode::Crc],
            ..Default::default()
        }
    }

    /// Create config with custom features and modes
    pub fn custom(supported: u64, required: u64, modes: Vec<ConnectionMode>) -> Self {
        Self {
            supported_features: supported,
            required_features: required,
            preferred_modes: modes,
            ..Default::default()
        }
    }

    /// Create config with no authentication
    /// Use this when connecting to a Ceph cluster with auth disabled
    pub fn with_no_auth() -> Self {
        Self {
            supported_auth_methods: vec![AuthMethod::None],
            auth_provider: None,
            ..Default::default()
        }
    }

    /// Create config with CephX authentication using an auth provider
    /// This is the recommended way to configure authentication
    ///
    /// # Example
    /// ```no_run
    /// use msgr2::ConnectionConfig;
    /// use auth::MonitorAuthProvider;
    ///
    /// let mut mon_auth = MonitorAuthProvider::new("client.admin".to_string())?;
    /// mon_auth.set_secret_key_from_base64("AQA...")?;
    /// let config = ConnectionConfig::with_auth_provider(Box::new(mon_auth));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn with_auth_provider(provider: Box<dyn auth::AuthProvider>) -> Self {
        Self {
            supported_auth_methods: vec![AuthMethod::Cephx],
            auth_provider: Some(provider),
            ..Default::default()
        }
    }

    /// Set the authentication provider
    pub fn set_auth_provider(&mut self, provider: Box<dyn auth::AuthProvider>) {
        self.auth_provider = Some(provider);
        if !self.supported_auth_methods.contains(&AuthMethod::Cephx) {
            self.supported_auth_methods.insert(0, AuthMethod::Cephx);
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.supported_auth_methods.is_empty() {
            return Err("supported_auth_methods cannot be empty".to_string());
        }

        if self.supported_auth_methods.contains(&AuthMethod::Gss) {
            return Err("AuthMethod::Gss not yet implemented".to_string());
        }

        if self.preferred_modes.is_empty() {
            return Err("preferred_modes cannot be empty".to_string());
        }

        Ok(())
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
