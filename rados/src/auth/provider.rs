//! Async authentication provider trait
//!
//! Provides a clean interface for different authentication strategies
//! (full CephX authentication vs. authorizer-based authentication)

use crate::Denc;
use crate::auth::error::{CephXError, Result};
use crate::auth::types::{CryptoKey, EntityType};
use bytes::Bytes;
use std::fmt::Debug;
use tracing::{debug, info};

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

    /// Downcast to Any for type checking
    fn as_any(&self) -> &dyn std::any::Any;
}

impl Clone for Box<dyn AuthProvider> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Lock a `CephXClientHandler` mutex, converting poison errors to `CephXError`.
fn lock_cephx_handler(
    handler: &std::sync::Mutex<crate::auth::client::CephXClientHandler>,
) -> Result<std::sync::MutexGuard<'_, crate::auth::client::CephXClientHandler>> {
    handler
        .lock()
        .map_err(|e| CephXError::ProtocolError(format!("Failed to lock handler: {}", e)))
}

/// Monitor authentication provider (full CephX authentication)
///
/// Used for connecting to monitors. Performs full challenge-response
/// authentication and obtains service tickets.
///
/// The handler is wrapped in Arc<Mutex<>> so it can be shared with
/// ServiceAuthProviders. When tickets are renewed, all ServiceAuthProviders
/// automatically see the updated tickets.
#[derive(Debug, Clone)]
pub struct MonitorAuthProvider {
    handler: std::sync::Arc<std::sync::Mutex<crate::auth::client::CephXClientHandler>>,
}

impl MonitorAuthProvider {
    /// Create a new monitor auth provider
    pub fn new(entity_name: &str) -> Result<Self> {
        let handler = crate::auth::client::CephXClientHandler::new(
            entity_name,
            crate::auth::protocol::AuthMode::Mon,
        )?;
        Ok(Self {
            handler: std::sync::Arc::new(std::sync::Mutex::new(handler)),
        })
    }

    fn lock_handler(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, crate::auth::client::CephXClientHandler>> {
        lock_cephx_handler(&self.handler)
    }

    /// Set the service ticket types to request alongside the AUTH ticket.
    ///
    /// `keys` is a bitmask of `EntityType::*` values. Defaults to
    /// `MON | OSD | MGR`, which is appropriate for rados clients. MDS clients
    /// should add `EntityType::MDS`.
    pub fn set_want_keys(&mut self, keys: EntityType) -> Result<()> {
        let mut handler = self.lock_handler()?;
        handler.set_want_keys(keys);
        Ok(())
    }

    /// Set the secret key from base64 string
    pub fn set_secret_key_from_base64(&mut self, key_str: &str) -> Result<()> {
        let mut handler = self.lock_handler()?;
        handler.set_secret_key_from_base64(key_str)
    }

    /// Set the secret key from keyring file
    pub fn set_secret_key_from_keyring(&mut self, keyring_path: &str) -> Result<()> {
        use crate::auth::keyring::Keyring;
        let keyring = Keyring::from_file(keyring_path)?;

        let mut handler = self.lock_handler()?;

        let entity_str = handler.entity_name.to_string();
        let key = keyring.get_key(&entity_str).ok_or_else(|| {
            crate::auth::error::CephXError::InvalidKey(format!("Key not found for {}", entity_str))
        })?;
        handler.set_secret_key(key.clone());
        Ok(())
    }

    /// Get a reference to the shared handler
    pub fn handler(
        &self,
    ) -> &std::sync::Arc<std::sync::Mutex<crate::auth::client::CephXClientHandler>> {
        &self.handler
    }
}

impl AuthProvider for MonitorAuthProvider {
    fn build_auth_payload(&mut self, global_id: u64, _service_id: u32) -> Result<Bytes> {
        // Lock the handler to build auth payload
        let handler = self.lock_handler()?;

        // For monitors, do full CephX authentication
        // Check if we've received server challenge
        if handler.server_challenge.is_some() {
            // Second round - send authenticate request with challenge response
            handler.build_authenticate_request()
        } else {
            // First round - send initial request
            handler.build_initial_request(global_id)
        }
    }

    fn handle_auth_response(
        &mut self,
        payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)> {
        // Lock the handler to handle auth response
        let mut handler = self.lock_handler()?;

        // Distinguish AUTH_REPLY_MORE from AUTH_DONE using the handler's state
        // machine — mirrors C++ CephxClientHandler::handle_response() which uses
        // its `starting` flag rather than peeking at payload bytes.
        if handler.starting {
            // AUTH_REPLY_MORE — server challenge (first round)
            handler.handle_auth_response(payload)?;
            Ok((None, None))
        } else {
            // AUTH_DONE — final authentication result
            handler.handle_auth_done(payload, global_id, con_mode)
        }
    }

    fn has_valid_ticket(&self, _service_id: u32) -> bool {
        // Monitors don't use tickets, they do full authentication
        false
    }

    fn clone_box(&self) -> Box<dyn AuthProvider> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Service authentication provider (authorizer-based)
///
/// Used for connecting to OSDs, MDSs, MGRs. Presents authorizers
/// obtained from monitor authentication instead of doing full CephX.
///
/// This provider shares the CephXClientHandler with MonClient via Arc<Mutex<>>,
/// so when MonClient renews tickets, all ServiceAuthProviders automatically
/// see the updated tickets.
#[derive(Debug, Clone)]
pub struct ServiceAuthProvider {
    handler: std::sync::Arc<std::sync::Mutex<crate::auth::client::CephXClientHandler>>,
    /// Track if we've already sent an authorizer (to avoid sending twice)
    authorizer_sent: bool,
    /// Pending server challenge (if received)
    server_challenge: Option<u64>,
    /// Service ID we're authenticating with (set when build_auth_payload is called)
    service_id: Option<u32>,
}

impl ServiceAuthProvider {
    /// Create a new service auth provider from a shared authenticated handler
    ///
    /// The handler should already have completed authentication with the monitor
    /// and obtained service tickets. The handler is shared via Arc<Mutex<>> so
    /// that ticket renewals are automatically visible to all ServiceAuthProviders.
    pub fn from_shared_handler(
        handler: std::sync::Arc<std::sync::Mutex<crate::auth::client::CephXClientHandler>>,
    ) -> Self {
        Self {
            handler,
            authorizer_sent: false,
            server_challenge: None,
            service_id: None,
        }
    }

    fn lock_handler(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, crate::auth::client::CephXClientHandler>> {
        lock_cephx_handler(&self.handler)
    }

    /// Get a reference to the shared handler
    pub fn handler(
        &self,
    ) -> &std::sync::Arc<std::sync::Mutex<crate::auth::client::CephXClientHandler>> {
        &self.handler
    }

    /// Try to extract connection_secret from AUTH_DONE payload.
    ///
    /// Returns `None` if the payload is empty, con_mode is 0, no session key
    /// is available, or decryption/decoding fails.
    fn try_extract_connection_secret(
        con_mode: u32,
        payload: &Bytes,
        session_key: Option<&CryptoKey>,
    ) -> Option<Bytes> {
        if con_mode == 0 || payload.is_empty() {
            return None;
        }
        let sess_key = session_key?;
        let mut buf = payload.clone();
        let encrypted_data = Bytes::decode(&mut buf, 0).ok()?;
        let decrypted = sess_key.decrypt(&encrypted_data).ok()?;
        let mut dec_buf = decrypted;
        let envelope = crate::auth::protocol::CephXEncryptedEnvelope::<
            crate::auth::protocol::CephXAuthorizeReply,
        >::decode(&mut dec_buf, 0)
        .ok()?;

        debug!(
            "CephXAuthorizeReply: nonce_plus_one=0x{:016x}",
            envelope.payload.nonce_plus_one
        );
        envelope.payload.connection_secret
    }
}

impl AuthProvider for ServiceAuthProvider {
    fn build_auth_payload(&mut self, global_id: u64, service_id: u32) -> Result<Bytes> {
        debug!(
            "ServiceAuthProvider::build_auth_payload called with service_id={}, global_id={}",
            service_id, global_id
        );

        // Store service_id for later use in handle_auth_response
        self.service_id = Some(service_id);

        // Lock the handler to build authorizer from existing tickets
        // This is a very fast operation (just crypto operations)
        let result = {
            let mut handler = self.lock_handler()?;
            handler.build_authorizer(
                EntityType::from_bits_retain(service_id),
                global_id,
                self.server_challenge,
            )
        };

        if let Err(ref e) = result {
            debug!("build_authorizer failed: {:?}", e);
        } else {
            debug!("build_authorizer succeeded");
            self.authorizer_sent = true;
        }
        result
    }

    fn handle_auth_response(
        &mut self,
        payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)> {
        debug!(
            "ServiceAuthProvider::handle_auth_response: payload={} bytes, con_mode={}",
            payload.len(),
            con_mode
        );

        // For authorizer-based auth, the first AUTH_REPLY_MORE contains an encrypted
        // CephXAuthorizeReply with server_challenge. We need to decrypt it and
        // rebuild the authorizer with the challenge included.
        // The second AUTH_REPLY_MORE (if any) would be AUTH_DONE.

        // Encrypted authorize challenge: u32 length prefix (4) + 2 AES blocks (32) = 36 bytes.
        // The inner plaintext is: struct_v(1) + magic(8) + CephXAuthorizeReply(1+8) = 18 bytes,
        // PKCS7-padded to 32 bytes.
        const ENCRYPTED_AUTHORIZE_CHALLENGE_LEN: usize = 36;
        if payload.len() == ENCRYPTED_AUTHORIZE_CHALLENGE_LEN {
            debug!("Received encrypted authorize challenge, decrypting...");

            // Get the service_id we're authenticating with
            let service_id = self.service_id.ok_or_else(|| {
                CephXError::ProtocolError("No service_id set in ServiceAuthProvider".into())
            })?;

            // Lock the handler to decrypt the challenge
            let server_challenge = {
                let handler = self.lock_handler()?;
                // Decrypt and extract server_challenge
                handler.decrypt_authorize_challenge(
                    EntityType::from_bits_retain(service_id),
                    payload.clone(),
                )?
            };

            debug!(
                "Successfully extracted server_challenge: 0x{:016x}",
                server_challenge
            );

            // Store the challenge for the next authorizer
            self.server_challenge = Some(server_challenge);

            // Reset the flag so build_auth_payload sends it again with the challenge
            self.authorizer_sent = false;

            return Ok((None, None));
        }

        info!(
            "AUTH_DONE received for OSD: global_id={}, con_mode={}",
            global_id, con_mode
        );

        // Lock the handler to get the session key
        let handler = self.lock_handler()?;

        // Get the session key for this service from the ticket handler
        let session_key = if let Some(session) = handler.get_session() {
            // Get the ticket handler for the connected service
            let stype = self
                .service_id
                .map(EntityType::from_bits_retain)
                .ok_or_else(|| {
                    CephXError::ProtocolError("No service_id set in ServiceAuthProvider".into())
                })?;
            session
                .ticket_handlers
                .get(&stype)
                .map(|handler| handler.session_key.clone())
        } else {
            None
        };

        debug!(
            "Returning session_key: {} bytes",
            session_key.as_ref().map(|k| k.secret.len()).unwrap_or(0)
        );

        // Extract connection_secret from AUTH_DONE payload using CephXEncryptedEnvelope
        let connection_secret =
            Self::try_extract_connection_secret(con_mode, &payload, session_key.as_ref());

        if let Some(ref secret) = connection_secret {
            debug!("Extracted connection_secret: {} bytes", secret.len());
        }

        // Return the session key so AUTH_SIGNATURE can be computed
        // Also return connection_secret for SECURE mode
        Ok((session_key.map(|k| k.secret.clone()), connection_secret))
    }

    fn has_valid_ticket(&self, service_id: u32) -> bool {
        // Lock the handler to check ticket validity
        if let Ok(handler) = self.handler.lock() {
            if let Some(session) = handler.get_session() {
                session.has_valid_ticket(EntityType::from_bits_retain(service_id))
            } else {
                false
            }
        } else {
            false
        }
    }

    fn clone_box(&self) -> Box<dyn AuthProvider> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
