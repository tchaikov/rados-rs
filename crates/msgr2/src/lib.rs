//! Ceph Messenger v2 Protocol Implementation
//!
//! This crate implements the Ceph msgr2.1 protocol for communication
//! with Ceph clusters, incorporating our latest protocol discoveries.

// Core modules that work
pub mod banner;
pub mod ceph_message;
pub mod compression;
pub mod crypto;
pub mod error;
pub mod frames;
pub mod header;
pub mod message;
pub mod message_bus;
pub mod protocol;
pub mod revocation;
pub mod state_machine;
pub mod throttle;

// Re-export working components
pub use banner::*;
pub use error::*;
pub use frames::*;
pub use header::*;
pub use message::*;
pub use message_bus::*;
pub use revocation::*;
pub use throttle::*;

// Messenger configuration options from ceph.conf
cephconfig::define_options! {
    /// Messenger configuration options from ceph.conf
    pub struct MessengerOptions {
        /// Dispatch throttle in bytes (default: no throttle)
        /// Controls receiver-side throttle for messages waiting to be dispatched
        ms_dispatch_throttle_bytes: cephconfig::Size = cephconfig::Size(0),
    }
}

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

impl denc::Denc for FeatureSet {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
        features: u64,
    ) -> std::result::Result<(), denc::RadosError> {
        self.0.encode(buf, features)
    }

    fn decode<B: bytes::Buf>(
        buf: &mut B,
        features: u64,
    ) -> std::result::Result<Self, denc::RadosError> {
        Ok(Self(u64::decode(buf, features)?))
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        self.0.encoded_size(features)
    }
}

// Implement ZeroCopyDencode marker to indicate this type is safe for zerocopy
impl denc::zerocopy::ZeroCopyDencode for FeatureSet {}

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
/// use msgr2::{ConnectionConfig, AuthMethod};
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

    /// Service ID for service-based authentication
    /// - 0: Monitor (default)
    /// - 4: OSD
    /// - 2: MDS
    /// - 16: MGR
    pub service_id: u32,

    /// Optional throttle configuration for this connection
    /// If None, no throttling is applied (default, matches Ceph client behavior)
    /// If Some, throttles message sending/receiving according to the config
    pub throttle_config: Option<ThrottleConfig>,
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
            service_id: 0,         // Default to monitor
            throttle_config: None, // No throttle by default (matches Ceph client)
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
        Self::with_auth_provider_and_service(provider, 0) // Default to monitor
    }

    /// Create config with authentication provider and service ID
    pub fn with_auth_provider_and_service(
        provider: Box<dyn auth::AuthProvider>,
        service_id: u32,
    ) -> Self {
        Self {
            supported_auth_methods: vec![AuthMethod::Cephx],
            auth_provider: Some(provider),
            service_id,
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

    /// Validate configuration for semantic correctness
    ///
    /// Checks that configuration values make sense and are internally consistent.
    /// This catches configuration errors early before attempting connection.
    ///
    /// # Errors
    ///
    /// Returns an error if any of these conditions are violated:
    /// - Empty auth methods or connection modes
    /// - Unknown/invalid auth methods or modes
    /// - Auth provider inconsistencies
    /// - Required features not supported
    /// - Invalid service ID
    /// - Unimplemented features
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate auth methods
        if self.supported_auth_methods.is_empty() {
            return Err("supported_auth_methods cannot be empty".to_string());
        }

        if self.supported_auth_methods.contains(&AuthMethod::Unknown) {
            return Err("supported_auth_methods contains AuthMethod::Unknown".to_string());
        }

        if self.supported_auth_methods.contains(&AuthMethod::Gss) {
            return Err("AuthMethod::Gss not yet implemented".to_string());
        }

        // Validate connection modes
        if self.preferred_modes.is_empty() {
            return Err("preferred_modes cannot be empty".to_string());
        }

        if self.preferred_modes.contains(&ConnectionMode::Unknown) {
            return Err("preferred_modes contains ConnectionMode::Unknown".to_string());
        }

        // Validate auth provider consistency
        if self.auth_provider.is_some() {
            if !self.supported_auth_methods.contains(&AuthMethod::Cephx) {
                return Err(
                    "auth_provider is set but supported_auth_methods doesn't include Cephx"
                        .to_string(),
                );
            }
        } else if self.supported_auth_methods.contains(&AuthMethod::Cephx) {
            return Err(
                "supported_auth_methods includes Cephx but auth_provider is None".to_string(),
            );
        }

        // Validate feature consistency
        if (self.required_features & self.supported_features) != self.required_features {
            return Err(format!(
                "required_features (0x{:x}) must be a subset of supported_features (0x{:x})",
                self.required_features, self.supported_features
            ));
        }

        // Validate service ID is a known value
        // Known values from Ceph: 0=Mon, 2=MDS, 4=OSD, 16=MGR
        const VALID_SERVICE_IDS: &[u32] = &[0, 2, 4, 16];
        if !VALID_SERVICE_IDS.contains(&self.service_id) {
            return Err(format!(
                "Invalid service_id {}. Valid values: 0 (Mon), 2 (MDS), 4 (OSD), 16 (MGR)",
                self.service_id
            ));
        }

        // Validate throttle config if present
        if let Some(ref throttle) = self.throttle_config {
            if throttle.max_bytes_per_sec == 0
                && throttle.max_messages_per_sec == 0
                && throttle.max_dispatch_queue_depth == 0
            {
                return Err(
                    "throttle_config is set but all limits are zero (throttle has no effect)"
                        .to_string(),
                );
            }

            // Check for unreasonably small byte rates (likely configuration error)
            if throttle.max_bytes_per_sec > 0 && throttle.max_bytes_per_sec < 1024 {
                return Err(format!(
                    "throttle max_bytes_per_sec ({}) is unreasonably small (< 1KB/s)",
                    throttle.max_bytes_per_sec
                ));
            }

            // Check for unreasonably small message rates
            if throttle.max_messages_per_sec > 0 && throttle.max_messages_per_sec < 10 {
                return Err(format!(
                    "throttle max_messages_per_sec ({}) is unreasonably small (< 10 msg/s)",
                    throttle.max_messages_per_sec
                ));
            }

            // Check for unreasonably small queue depth
            if throttle.max_dispatch_queue_depth > 0 && throttle.max_dispatch_queue_depth < 10 {
                return Err(format!(
                    "throttle max_dispatch_queue_depth ({}) is unreasonably small (< 10 messages)",
                    throttle.max_dispatch_queue_depth
                ));
            }
        }

        Ok(())
    }

    /// Create config from Ceph configuration file
    /// Reads throttle settings from ceph.conf to match Ceph client behavior
    ///
    /// # Example
    /// ```no_run
    /// use msgr2::ConnectionConfig;
    ///
    /// let config = ConnectionConfig::from_ceph_conf("/etc/ceph/ceph.conf")
    ///     .expect("Failed to read ceph.conf");
    /// ```
    pub fn from_ceph_conf(path: &str) -> Result<Self> {
        let ceph_config = cephconfig::CephConfig::from_file(path)
            .map_err(|e| Error::config_error(&format!("Failed to read ceph.conf: {}", e)))?;

        let mut config = Self::default();

        // Load messenger options from ceph.conf
        let sections: &[&str] = &["global", "client"];
        let msgr_opts = MessengerOptions::from_ceph_config(&ceph_config, sections);

        // Apply throttle config if set (non-zero)
        if msgr_opts.ms_dispatch_throttle_bytes.0 > 0 {
            config.throttle_config = Some(ThrottleConfig::with_byte_rate(
                msgr_opts.ms_dispatch_throttle_bytes.0,
            ));
            tracing::debug!(
                "Set dispatch throttle from ceph.conf: {} bytes",
                msgr_opts.ms_dispatch_throttle_bytes.0
            );
        }

        Ok(config)
    }

    /// Set throttle configuration
    pub fn with_throttle(mut self, throttle_config: ThrottleConfig) -> Self {
        self.throttle_config = Some(throttle_config);
        self
    }

    /// Set throttle to match Ceph's default (100MB dispatch throttle)
    pub fn with_ceph_default_throttle(mut self) -> Self {
        self.throttle_config = Some(ThrottleConfig::with_byte_rate(100 * 1024 * 1024));
        self
    }
}

/// Parse size string from ceph.conf (e.g., "100M", "1G", "512K")
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
        assert!(config.throttle_config.is_none());
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
    fn test_connection_config_with_throttle() {
        let throttle = ThrottleConfig::with_byte_rate(1024 * 1024);
        let config = ConnectionConfig::default().with_throttle(throttle);
        assert!(config.throttle_config.is_some());
        assert_eq!(
            config.throttle_config.unwrap().max_bytes_per_sec,
            1024 * 1024
        );
    }

    #[test]
    fn test_connection_config_with_ceph_default_throttle() {
        let config = ConnectionConfig::default().with_ceph_default_throttle();
        assert!(config.throttle_config.is_some());
        assert_eq!(
            config.throttle_config.unwrap().max_bytes_per_sec,
            100 * 1024 * 1024
        );
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

    #[test]
    fn test_connection_config_validate_success() {
        let config = ConnectionConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_connection_config_validate_empty_auth_methods() {
        let config = ConnectionConfig {
            supported_auth_methods: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("supported_auth_methods cannot be empty"));
    }

    #[test]
    fn test_connection_config_validate_unknown_auth_method() {
        let config = ConnectionConfig {
            supported_auth_methods: vec![AuthMethod::Unknown, AuthMethod::None],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("AuthMethod::Unknown"));
    }

    #[test]
    fn test_connection_config_validate_gss_not_implemented() {
        let config = ConnectionConfig {
            supported_auth_methods: vec![AuthMethod::Gss],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("AuthMethod::Gss not yet implemented"));
    }

    #[test]
    fn test_connection_config_validate_empty_modes() {
        let config = ConnectionConfig {
            preferred_modes: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("preferred_modes cannot be empty"));
    }

    #[test]
    fn test_connection_config_validate_unknown_mode() {
        let config = ConnectionConfig {
            preferred_modes: vec![ConnectionMode::Unknown],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("ConnectionMode::Unknown"));
    }

    #[test]
    fn test_connection_config_validate_auth_provider_mismatch() {
        let mut config = ConnectionConfig::with_no_auth();
        // Manually create an inconsistent state
        use auth::MonitorAuthProvider;
        let mon_auth =
            MonitorAuthProvider::new("client.admin".to_string()).expect("Failed to create auth");
        config.auth_provider = Some(Box::new(mon_auth));
        // supported_auth_methods is still [None] but provider is set
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("auth_provider is set but supported_auth_methods doesn't include Cephx"));
    }

    #[test]
    fn test_connection_config_validate_cephx_without_provider() {
        let config = ConnectionConfig {
            supported_auth_methods: vec![AuthMethod::Cephx],
            auth_provider: None,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("supported_auth_methods includes Cephx but auth_provider is None"));
    }

    #[test]
    fn test_connection_config_validate_required_features_not_supported() {
        let config = ConnectionConfig {
            supported_features: MSGR2_FEATURE_REVISION_1,
            required_features: MSGR2_FEATURE_COMPRESSION,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("required_features"));
        assert!(config.validate().unwrap_err().contains("subset"));
    }

    #[test]
    fn test_connection_config_validate_invalid_service_id() {
        let mut config = ConnectionConfig::with_no_auth();
        config.service_id = 999; // Invalid service ID
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("Invalid service_id"));
    }

    #[test]
    fn test_connection_config_validate_valid_service_ids() {
        // Test all valid service IDs
        for &service_id in &[0, 2, 4, 16] {
            let mut config = ConnectionConfig::with_no_auth();
            config.service_id = service_id;
            assert!(
                config.validate().is_ok(),
                "Service ID {} should be valid",
                service_id
            );
        }
    }

    #[test]
    fn test_connection_config_validate_throttle_all_zeros() {
        let config = ConnectionConfig {
            throttle_config: Some(ThrottleConfig {
                max_bytes_per_sec: 0,
                max_messages_per_sec: 0,
                max_dispatch_queue_depth: 0,
                rate_window: std::time::Duration::from_secs(1),
            }),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("all limits are zero"));
    }

    #[test]
    fn test_connection_config_validate_throttle_bytes_too_small() {
        let config = ConnectionConfig {
            throttle_config: Some(ThrottleConfig::with_byte_rate(512)),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("unreasonably small"));
    }

    #[test]
    fn test_connection_config_validate_throttle_messages_too_small() {
        let config = ConnectionConfig {
            throttle_config: Some(ThrottleConfig::with_message_rate(5)),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("unreasonably small"));
    }

    #[test]
    fn test_connection_config_validate_throttle_queue_too_small() {
        let config = ConnectionConfig {
            throttle_config: Some(ThrottleConfig::with_queue_depth(5)),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("unreasonably small"));
    }

    #[test]
    fn test_connection_config_validate_throttle_valid() {
        let config = ConnectionConfig {
            throttle_config: Some(ThrottleConfig::with_byte_rate(1024 * 1024)),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }
}
