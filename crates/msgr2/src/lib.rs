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
pub mod io_loop;
pub mod map_channel;
pub mod message;
pub(crate) mod phase;
pub mod priority_queue;
pub mod protocol;
pub mod revocation;
pub mod split;
pub mod state_machine;
pub mod throttle;

// Re-export public API
pub use banner::{Banner, ConnectMessage, ConnectReplyMessage, CEPH_BANNER, CEPH_BANNER_LEN};
pub use compression::{CompressionAlgorithm, CompressionStats};
pub use error::{Msgr2Error, Result};
pub use frames::{
    EncodeWithFeatures, Frame, FrameAssembler, FrameTrait, MessageFrame, Tag, DEFAULT_ALIGNMENT,
    MAX_NUM_SEGMENTS, PREAMBLE_SIZE,
};
pub use header::MsgHeader;
pub use map_channel::{map_channel, MapMessage, MapReceiver, MapSender};
pub use message::{Message, MessagePriority, MessageType, MsgFooter};
pub use revocation::{
    MessageHandle, MessageId, MessageStatus, RevocationManager, RevocationResult, RevocationStats,
};
pub use split::{RecvHalf, SendHalf, SharedState};
pub use throttle::{MessageThrottle, ThrottleConfig, ThrottleStats};

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
bitflags::bitflags! {
    /// msgr2 protocol feature flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FeatureSet: u64 {
        /// msgr2.1 protocol revision
        const REVISION_1 = 1 << 0; // bit 0
        /// On-wire compression support
        const COMPRESSION = 1 << 1; // bit 1
    }
}

impl FeatureSet {
    /// Empty feature set (no features)
    pub const EMPTY: Self = Self::empty();

    /// All msgr2 features (REVISION_1 | COMPRESSION)
    pub const ALL: Self = Self::REVISION_1.union(Self::COMPRESSION);
}

impl From<u64> for FeatureSet {
    fn from(value: u64) -> Self {
        Self::from_bits_truncate(value)
    }
}

impl From<FeatureSet> for u64 {
    fn from(features: FeatureSet) -> Self {
        features.bits()
    }
}

impl denc::Denc for FeatureSet {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
        features: u64,
    ) -> std::result::Result<(), denc::RadosError> {
        self.bits().encode(buf, features)
    }

    fn decode<B: bytes::Buf>(
        buf: &mut B,
        features: u64,
    ) -> std::result::Result<Self, denc::RadosError> {
        Ok(Self::from_bits_truncate(u64::decode(buf, features)?))
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        self.bits().encoded_size(features)
    }
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

/// Authentication mode constants (from src/auth/Auth.h)
///
/// These values appear as the first byte in AUTH_REQUEST payloads and determine
/// which authentication sub-protocol is used.
pub mod auth_mode {
    /// No authentication mode (unused in AUTH_REQUEST)
    pub const AUTH_MODE_NONE: u8 = 0;

    /// Service connection authorizer mode (OSDs, MDS, MGR)
    /// Range: 1-9
    pub const AUTH_MODE_AUTHORIZER: u8 = 1;
    pub const AUTH_MODE_AUTHORIZER_MAX: u8 = 9;

    /// Monitor authentication mode
    /// Range: 10-19
    pub const AUTH_MODE_MON: u8 = 10;
    pub const AUTH_MODE_MON_MAX: u8 = 19;
}

/// AUTH_NONE request payload for the AUTH_REQUEST frame.
///
/// This structure encodes the payload for AUTH_NONE authentication, used for
/// both monitor and service (OSD/MDS/MGR) connections. The auth_mode byte
/// determines which type of connection is being authenticated:
///
/// - **Monitor connections** (auth_mode=10): Initial authentication where the
///   monitor assigns a global_id. Reference: `Monitor::handle_auth_request()`
///   in `src/mon/Monitor.cc`.
///
/// - **Service connections** (auth_mode=1): Authorizer-based authentication
///   where the service validates the entity_name and global_id without
///   requiring a secret. Reference: `AuthNoneAuthorizer::build_authorizer()`
///   in `src/auth/none/AuthNoneProtocol.h`.
///
/// Wire format: auth_mode (u8) + entity_name + global_id (u64)
#[derive(Debug, Clone)]
pub struct AuthNonePayload {
    /// Authentication mode (AUTH_MODE_MON=10 or AUTH_MODE_AUTHORIZER=1)
    pub auth_mode: u8,
    /// Client's entity name (e.g., "client.admin")
    pub entity_name: denc::EntityName,
    /// Client's global_id (0 for initial monitor request; assigned value for services)
    pub global_id: u64,
}

impl AuthNonePayload {
    /// Create a monitor authentication request (auth_mode=10).
    ///
    /// Used for initial authentication to monitors. The monitor assigns a
    /// global_id when the client sends 0.
    pub fn for_monitor(entity_name: denc::EntityName, global_id: u64) -> Self {
        Self {
            auth_mode: auth_mode::AUTH_MODE_MON,
            entity_name,
            global_id,
        }
    }

    /// Create a service connection authorizer (auth_mode=1).
    ///
    /// Used for OSD/MDS/MGR connections. The global_id should be the value
    /// assigned by the monitor during initial authentication.
    pub fn for_service(entity_name: denc::EntityName, global_id: u64) -> Self {
        Self {
            auth_mode: auth_mode::AUTH_MODE_AUTHORIZER,
            entity_name,
            global_id,
        }
    }

    /// Encode into bytes for AUTH_REQUEST payload
    pub fn encode(&self) -> Result<bytes::Bytes> {
        let mut buf = bytes::BytesMut::new();
        use denc::Denc;
        self.auth_mode.encode(&mut buf, 0)?;
        self.entity_name.encode(&mut buf, 0)?;
        self.global_id.encode(&mut buf, 0)?;
        Ok(buf.freeze())
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

/// Connection mode constants for negotiation
pub const SECURE_MODE: u32 = ConnectionMode::Secure as u32;
pub const CRC_MODE: u32 = ConnectionMode::Crc as u32;

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
/// let mut mon_auth = MonitorAuthProvider::new("client.admin")?;
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
    /// Features to advertise as supported in banner exchange.
    /// Default: REVISION_1 only; compression must be explicitly enabled.
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

    /// Entity name for authentication (e.g., "client.admin")
    /// Used in AUTH_NONE payload to identify the client to the monitor.
    /// Defaults to EntityName::client("admin").
    pub entity_name: denc::EntityName,

    /// Global ID assigned by monitor during authentication
    /// Used for AUTH_NONE authorizers when connecting to services (OSDs)
    /// Set to 0 for initial monitor connections
    pub global_id: u64,

    /// Optional throttle configuration for this connection
    /// If None, no throttling is applied (default, matches Ceph client behavior)
    /// If Some, throttles message sending/receiving according to the config
    pub throttle_config: Option<ThrottleConfig>,

    /// Maximum number of unacknowledged messages to retain for replay on reconnect.
    /// If `None`, the replay queue is unbounded.
    pub max_replay_queue_len: Option<usize>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        // Default: use AuthMethod::None
        // Users should explicitly configure CephX authentication via
        // with_auth_provider() or with_auth_provider_and_service() methods
        let supported_auth_methods = vec![AuthMethod::None];

        Self {
            supported_features: FeatureSet::REVISION_1.bits(),
            required_features: 0,
            preferred_modes: vec![ConnectionMode::Secure, ConnectionMode::Crc],
            supported_auth_methods,
            auth_provider: None,
            service_id: 0, // Default to monitor
            entity_name: denc::EntityName::client("admin"),
            global_id: 0,          // Will be set after monitor authentication
            throttle_config: None, // No throttle by default (matches Ceph client)
            max_replay_queue_len: None,
        }
    }
}

impl ConnectionConfig {
    /// Create config with compression disabled
    pub fn without_compression() -> Self {
        Self {
            supported_features: FeatureSet::REVISION_1.bits(),
            ..Default::default()
        }
    }

    /// Create config with compression enabled
    pub fn with_compression() -> Self {
        Self {
            supported_features: FeatureSet::ALL.bits(),
            ..Default::default()
        }
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
            supported_features: FeatureSet::REVISION_1.bits(),
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
    /// let mut mon_auth = MonitorAuthProvider::new("client.admin")?;
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

        if let Some(limit) = self.max_replay_queue_len {
            if limit == 0 {
                return Err("max_replay_queue_len must be greater than zero".to_string());
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
            .map_err(|e| Msgr2Error::config_error(&format!("Failed to read ceph.conf: {}", e)))?;

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

    /// Set a limit on the number of unacknowledged messages retained for replay.
    pub fn with_replay_queue_limit(mut self, max_replay_queue_len: usize) -> Self {
        self.max_replay_queue_len = Some(max_replay_queue_len);
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
        assert_eq!(config.supported_features, FeatureSet::REVISION_1.bits());
        assert_eq!(config.required_features, 0);
        assert_eq!(
            config.preferred_modes,
            vec![ConnectionMode::Secure, ConnectionMode::Crc]
        );
        assert!(config.throttle_config.is_none());
        assert_eq!(config.max_replay_queue_len, None);
        let features = FeatureSet::from(config.supported_features);
        assert!(features.contains(FeatureSet::REVISION_1));
        assert!(!features.contains(FeatureSet::COMPRESSION));
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
        assert_eq!(config.supported_features, FeatureSet::REVISION_1.bits());
        assert_eq!(config.required_features, 0);
        let features = FeatureSet::from(config.supported_features);
        assert!(features.contains(FeatureSet::REVISION_1));
        assert!(!features.contains(FeatureSet::COMPRESSION));
    }

    #[test]
    fn test_connection_config_with_compression() {
        let config = ConnectionConfig::with_compression();
        assert_eq!(config.supported_features, FeatureSet::ALL.bits());
        let features = FeatureSet::from(config.supported_features);
        assert!(features.contains(FeatureSet::COMPRESSION));
    }

    #[test]
    fn test_connection_config_prefer_crc_mode() {
        let config = ConnectionConfig::prefer_crc_mode();
        assert_eq!(config.preferred_modes, vec![ConnectionMode::Crc]);
        assert_eq!(config.supported_features, FeatureSet::REVISION_1.bits());
    }

    #[test]
    fn test_connection_config_prefer_secure_mode() {
        let config = ConnectionConfig::prefer_secure_mode();
        assert_eq!(config.preferred_modes, vec![ConnectionMode::Secure]);
        assert_eq!(config.supported_features, FeatureSet::REVISION_1.bits());
    }

    #[test]
    fn test_connection_config_minimal() {
        let config = ConnectionConfig::minimal();
        assert_eq!(config.supported_features, FeatureSet::REVISION_1.bits());
        assert_eq!(config.preferred_modes, vec![ConnectionMode::Crc]);
        let features = FeatureSet::from(config.supported_features);
        assert!(!features.contains(FeatureSet::COMPRESSION));
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
        let mon_auth = MonitorAuthProvider::new("client.admin").expect("Failed to create auth");
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
            supported_features: FeatureSet::REVISION_1.bits(),
            required_features: FeatureSet::COMPRESSION.bits(),
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

    #[test]
    fn test_connection_config_replay_limit_builder() {
        let config = ConnectionConfig::default().with_replay_queue_limit(128);
        assert_eq!(config.max_replay_queue_len, Some(128));
    }

    #[test]
    fn test_connection_config_validate_replay_limit_zero() {
        let config = ConnectionConfig {
            max_replay_queue_len: Some(0),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("max_replay_queue_len"));
    }
}
