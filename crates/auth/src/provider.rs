//! Async authentication provider trait
//!
//! Provides a clean interface for different authentication strategies
//! (full CephX authentication vs. authorizer-based authentication)

use crate::error::Result;
use bytes::Bytes;
use std::fmt::Debug;

/// Authentication provider trait
///
/// This trait abstracts authentication strategies, allowing the msgr2 layer
/// to request authentication payloads without needing to know the implementation details.
///
/// Implementations:
/// - `MonitorAuthProvider`: Full CephX authentication for monitor connections
/// - `ServiceAuthProvider`: Authorizer-based authentication for OSD/MDS/MGR connections
pub trait AuthProvider: Send + Sync + Debug {
    /// Build the initial authentication request payload
    ///
    /// For monitors: Builds full CephX authentication request
    /// For services: Builds authorizer from existing tickets
    ///
    /// # Arguments
    /// * `global_id` - Global ID (0 for initial request, actual ID for subsequent requests)
    /// * `service_id` - Service type (MON, OSD, MDS, etc.)
    fn build_auth_payload(&mut self, global_id: u64, service_id: u32) -> Result<Bytes>;

    /// Handle authentication response (AUTH_REPLY_MORE or AUTH_DONE)
    ///
    /// Returns (session_key, connection_secret) if available
    fn handle_auth_response(
        &mut self,
        payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)>;

    /// Check if this provider has valid credentials for the given service
    fn has_valid_ticket(&self, service_id: u32) -> bool;

    /// Clone this provider (for sharing across connections)
    fn clone_box(&self) -> Box<dyn AuthProvider>;
}

impl Clone for Box<dyn AuthProvider> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Monitor authentication provider (full CephX authentication)
///
/// Used for connecting to monitors. Performs full challenge-response
/// authentication and obtains service tickets.
#[derive(Debug, Clone)]
pub struct MonitorAuthProvider {
    handler: crate::client::CephXClientHandler,
}

impl MonitorAuthProvider {
    /// Create a new monitor auth provider
    pub fn new(entity_name: String) -> Result<Self> {
        let handler =
            crate::client::CephXClientHandler::new(entity_name, crate::protocol::AuthMode::Mon)?;
        Ok(Self { handler })
    }

    /// Set the secret key from base64 string
    pub fn set_secret_key_from_base64(&mut self, key_str: &str) -> Result<()> {
        self.handler.set_secret_key_from_base64(key_str)
    }

    /// Set the secret key from keyring file
    pub fn set_secret_key_from_keyring(&mut self, keyring_path: &str) -> Result<()> {
        use crate::keyring::Keyring;
        let keyring = Keyring::from_file(keyring_path)?;
        let entity_str = format!("client.{}", self.handler.entity_name.id);
        let key = keyring.get_key(&entity_str).ok_or_else(|| {
            crate::error::CephXError::InvalidKey(format!("Key not found for {}", entity_str))
        })?;
        self.handler.set_secret_key(key.clone());
        Ok(())
    }

    /// Get the underlying CephX handler (for accessing session and tickets)
    pub fn handler(&self) -> &crate::client::CephXClientHandler {
        &self.handler
    }

    /// Get mutable reference to the underlying CephX handler
    pub fn handler_mut(&mut self) -> &mut crate::client::CephXClientHandler {
        &mut self.handler
    }
}

impl AuthProvider for MonitorAuthProvider {
    fn build_auth_payload(&mut self, global_id: u64, _service_id: u32) -> Result<Bytes> {
        // For monitors, always do full CephX authentication
        self.handler.build_initial_request(global_id)
    }

    fn handle_auth_response(
        &mut self,
        payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)> {
        self.handler.handle_auth_done(payload, global_id, con_mode)
    }

    fn has_valid_ticket(&self, _service_id: u32) -> bool {
        // Monitors don't use tickets, they do full authentication
        false
    }

    fn clone_box(&self) -> Box<dyn AuthProvider> {
        Box::new(self.clone())
    }
}

/// Service authentication provider (authorizer-based)
///
/// Used for connecting to OSDs, MDSs, MGRs. Presents authorizers
/// obtained from monitor authentication instead of doing full CephX.
#[derive(Debug, Clone)]
pub struct ServiceAuthProvider {
    handler: crate::client::CephXClientHandler,
}

impl ServiceAuthProvider {
    /// Create a new service auth provider from an authenticated monitor session
    ///
    /// The handler should already have completed authentication with the monitor
    /// and obtained service tickets.
    pub fn from_authenticated_handler(handler: crate::client::CephXClientHandler) -> Self {
        Self { handler }
    }

    /// Get the underlying CephX handler
    pub fn handler(&self) -> &crate::client::CephXClientHandler {
        &self.handler
    }

    /// Get mutable reference to the underlying CephX handler
    pub fn handler_mut(&mut self) -> &mut crate::client::CephXClientHandler {
        &mut self.handler
    }
}

impl AuthProvider for ServiceAuthProvider {
    fn build_auth_payload(&mut self, global_id: u64, service_id: u32) -> Result<Bytes> {
        // For services, build authorizer from existing tickets
        self.handler.build_authorizer(service_id, global_id)
    }

    fn handle_auth_response(
        &mut self,
        _payload: Bytes,
        _global_id: u64,
        _con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)> {
        // For authorizer-based auth, there's usually no AUTH_DONE response
        // The server just validates the authorizer and we're connected
        Ok((None, None))
    }

    fn has_valid_ticket(&self, service_id: u32) -> bool {
        if let Some(session) = self.handler.get_session() {
            session.has_valid_ticket(service_id)
        } else {
            false
        }
    }

    fn clone_box(&self) -> Box<dyn AuthProvider> {
        Box::new(self.clone())
    }
}
