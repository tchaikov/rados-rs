//! Authentication configuration
//!
//! This module encapsulates all authentication-related settings, making it easy to
//! configure from different sources (ceph.conf, environment, explicit settings, etc.)

use crate::monclient::error::{MonClientError, Result};

/// Authentication configuration
///
/// This encapsulates all authentication-related settings, making it easy to
/// configure from different sources (ceph.conf, environment, explicit settings, etc.)
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Entity name (e.g., "client.admin")
    entity_name: String,

    /// Authentication provider (CephX, None, etc.)
    /// None means no authentication
    auth_provider: Option<crate::auth::MonitorAuthProvider>,
}

/// Create a `MonitorAuthProvider` for the given entity, mapping auth errors to `MonClientError`.
fn new_monitor_auth_provider(entity_name: &str) -> Result<crate::auth::MonitorAuthProvider> {
    crate::auth::MonitorAuthProvider::new(entity_name)
        .map_err(|e| MonClientError::ConfigError(format!("Failed to create auth provider: {e}")))
}

impl AuthConfig {
    /// Create auth config from ceph.conf (convenience method)
    ///
    /// This parses the ceph.conf file and automatically configures authentication
    /// based on the `auth_client_required` setting and keyring path.
    ///
    /// # Arguments
    /// * `ceph_conf_path` - Path to ceph.conf file
    ///
    /// # Returns
    /// * `Ok(AuthConfig)` with appropriate authentication provider
    /// * `Err(MonClientError)` if config parsing fails or keyring not found
    pub fn from_ceph_conf(ceph_conf_path: &str) -> Result<Self> {
        let config = crate::cephconfig::CephConfig::from_file(ceph_conf_path)
            .map_err(|e| MonClientError::ConfigError(format!("Failed to parse ceph.conf: {e}")))?;

        let entity_name = config.entity_name();
        let auth_methods = config.get_auth_client_required();

        let auth_provider = if auth_methods.contains(&crate::auth::protocol::CEPH_AUTH_CEPHX) {
            let keyring_path = config.keyring().map_err(|e| {
                MonClientError::ConfigError(format!("Failed to get keyring path: {e}"))
            })?;
            let mut provider = new_monitor_auth_provider(&entity_name)?;
            provider
                .set_secret_key_from_keyring(&keyring_path)
                .map_err(|e| MonClientError::ConfigError(format!("Failed to load keyring: {e}")))?;
            Some(provider)
        } else {
            None
        };

        Ok(Self {
            entity_name,
            auth_provider,
        })
    }

    /// Create auth config with explicit keyring file
    ///
    /// # Arguments
    /// * `entity_name` - Entity name (e.g., "client.admin")
    /// * `keyring_path` - Path to keyring file
    ///
    /// # Returns
    /// * `Ok(AuthConfig)` with CephX authentication
    /// * `Err(MonClientError)` if keyring loading fails
    pub fn from_keyring(entity_name: String, keyring_path: &str) -> Result<Self> {
        let mut provider = new_monitor_auth_provider(&entity_name)?;
        provider
            .set_secret_key_from_keyring(keyring_path)
            .map_err(|e| MonClientError::ConfigError(format!("Failed to load keyring: {e}")))?;

        Ok(Self {
            entity_name,
            auth_provider: Some(provider),
        })
    }

    /// Create auth config with explicit secret key (base64)
    ///
    /// # Arguments
    /// * `entity_name` - Entity name (e.g., "client.admin")
    /// * `secret_key_base64` - Base64-encoded secret key
    ///
    /// # Returns
    /// * `Ok(AuthConfig)` with CephX authentication
    /// * `Err(MonClientError)` if secret key is invalid
    pub fn from_secret_key(entity_name: String, secret_key_base64: &str) -> Result<Self> {
        let mut provider = new_monitor_auth_provider(&entity_name)?;
        provider
            .set_secret_key_from_base64(secret_key_base64)
            .map_err(|e| MonClientError::ConfigError(format!("Failed to set secret key: {e}")))?;

        Ok(Self {
            entity_name,
            auth_provider: Some(provider),
        })
    }

    /// Create auth config for no authentication (auth none)
    ///
    /// # Arguments
    /// * `entity_name` - Entity name (e.g., "client.admin")
    ///
    /// # Returns
    /// AuthConfig with no authentication provider
    pub fn no_auth(entity_name: String) -> Self {
        Self {
            entity_name,
            auth_provider: None,
        }
    }

    /// Get the entity name
    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    /// Clone the auth provider (for connection creation)
    pub(crate) fn clone_provider(&self) -> Option<crate::auth::MonitorAuthProvider> {
        self.auth_provider.clone()
    }
}

impl Default for AuthConfig {
    /// Default authentication configuration
    ///
    /// Tries to load from /etc/ceph/ceph.conf, falls back to no-auth with entity name "client.admin"
    fn default() -> Self {
        Self::from_ceph_conf("/etc/ceph/ceph.conf")
            .unwrap_or_else(|_| Self::no_auth("client.admin".to_string()))
    }
}
