//! Async authentication provider trait
//!
//! Provides a clean interface for different authentication strategies
//! (full CephX authentication vs. authorizer-based authentication)

use crate::error::{CephXError, Result};
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

    /// Downcast to Any for type checking
    fn as_any(&self) -> &dyn std::any::Any;
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
        // For monitors, do full CephX authentication
        // Check if we've received server challenge
        if self.handler.server_challenge.is_some() {
            // Second round - send authenticate request with challenge response
            self.handler.build_authenticate_request()
        } else {
            // First round - send initial request
            self.handler.build_initial_request(global_id)
        }
    }

    fn handle_auth_response(
        &mut self,
        payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)> {
        use bytes::Buf;

        // Distinguish between AUTH_REPLY_MORE (server challenge) and AUTH_DONE (final result)
        // AUTH_REPLY_MORE: starts with u32 length prefix followed by CephXServerChallenge
        // AUTH_DONE: starts with CephXResponseHeader (u16 request_type = 0x0100)

        // Peek at the payload to determine the type
        let mut peek = payload.clone();
        if peek.len() >= 2 {
            // Read first u16 to check if it's the request_type
            let first_u16 = peek.get_u16_le();

            if first_u16 == crate::protocol::CEPHX_GET_AUTH_SESSION_KEY {
                // This is AUTH_DONE - handle final authentication
                self.handler.handle_auth_done(payload, global_id, con_mode)
            } else {
                // This is AUTH_REPLY_MORE - handle server challenge
                self.handler.handle_auth_response(payload)?;
                // Return (None, None) since AUTH_REPLY_MORE doesn't provide keys
                Ok((None, None))
            }
        } else {
            Err(crate::error::CephXError::ProtocolError(
                "Auth response payload too short".into(),
            ))
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
#[derive(Debug, Clone)]
pub struct ServiceAuthProvider {
    handler: crate::client::CephXClientHandler,
    /// Track if we've already sent an authorizer (to avoid sending twice)
    authorizer_sent: bool,
    /// Pending server challenge (if received)
    server_challenge: Option<u64>,
    /// Service ID we're authenticating with (set when build_auth_payload is called)
    service_id: Option<u32>,
}

impl ServiceAuthProvider {
    /// Create a new service auth provider from an authenticated monitor session
    ///
    /// The handler should already have completed authentication with the monitor
    /// and obtained service tickets.
    pub fn from_authenticated_handler(handler: crate::client::CephXClientHandler) -> Self {
        use tracing::debug;
        let has_session = handler.get_session().is_some();
        let ticket_count = handler
            .get_session()
            .map(|s| s.ticket_handlers.len())
            .unwrap_or(0);
        eprintln!(
            "DEBUG: ServiceAuthProvider::from_authenticated_handler: has_session={}, ticket_count={}",
            has_session, ticket_count
        );
        debug!(
            "ServiceAuthProvider::from_authenticated_handler: has_session={}, ticket_count={}",
            has_session, ticket_count
        );
        if let Some(session) = handler.get_session() {
            for (sid, handler) in &session.ticket_handlers {
                eprintln!(
                    "DEBUG:   Handler has ticket for service {}: have_key={}",
                    sid, handler.have_key
                );
                debug!(
                    "  Handler has ticket for service {}: have_key={}",
                    sid, handler.have_key
                );
            }
        }
        Self { handler, authorizer_sent: false, server_challenge: None, service_id: None }
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
        use tracing::debug;
        eprintln!(
            "DEBUG: ServiceAuthProvider::build_auth_payload called with service_id={}, global_id={}, authorizer_sent={}, has_challenge={}",
            service_id, global_id, self.authorizer_sent, self.server_challenge.is_some()
        );
        debug!(
            "ServiceAuthProvider::build_auth_payload called with service_id={}, global_id={}",
            service_id, global_id
        );

        // Store service_id for later use in handle_auth_response
        self.service_id = Some(service_id);

        // Build authorizer from existing tickets, including server_challenge if we have it
        let result = self.handler.build_authorizer(service_id, global_id, self.server_challenge);
        if let Err(ref e) = result {
            eprintln!("DEBUG: build_authorizer failed: {:?}", e);
        } else {
            eprintln!("DEBUG: build_authorizer succeeded");
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
        use tracing::{debug, info};
        eprintln!("DEBUG: ServiceAuthProvider::handle_auth_response called");
        eprintln!("DEBUG:   payload length: {}", payload.len());
        eprintln!("DEBUG:   global_id: {}", global_id);
        eprintln!("DEBUG:   con_mode: {} (0=CRC, 1=SECURE)", con_mode);
        eprintln!("DEBUG:   payload hex (first 64 bytes): {}",
            payload.iter().take(64).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(""));

        debug!("ServiceAuthProvider::handle_auth_response: payload={} bytes",
            payload.len());

        // For authorizer-based auth, the first AUTH_REPLY_MORE contains an encrypted
        // CephXAuthorizeReply with server_challenge. We need to decrypt it and
        // rebuild the authorizer with the challenge included.
        // The second AUTH_REPLY_MORE (if any) would be AUTH_DONE.

        if payload.len() == 36 {
            // This is the encrypted CephXAuthorizeReply with server_challenge
            eprintln!("DEBUG: Received encrypted challenge (36 bytes), decrypting...");

            // Get the service_id we're authenticating with
            let service_id = self.service_id.ok_or_else(|| {
                CephXError::ProtocolError("No service_id set in ServiceAuthProvider".into())
            })?;

            // Decrypt and extract server_challenge
            match self.handler.decrypt_authorize_challenge(service_id, &payload) {
                Ok(server_challenge) => {
                    eprintln!("DEBUG: Successfully extracted server_challenge: 0x{:016x}", server_challenge);
                    debug!("Successfully extracted server_challenge: 0x{:016x}", server_challenge);

                    // Store the challenge for the next authorizer
                    self.server_challenge = Some(server_challenge);

                    // Reset the flag so build_auth_payload sends it again with the challenge
                    self.authorizer_sent = false;

                    return Ok((None, None));
                }
                Err(e) => {
                    eprintln!("DEBUG: Failed to decrypt challenge: {:?}", e);
                    return Err(e);
                }
            }
        }

        info!("AUTH_DONE received for OSD: global_id={}, con_mode={}", global_id, con_mode);

        // Get the session key for this service from the ticket handler
        let session_key = if let Some(session) = self.handler.get_session() {
            // Get the OSD ticket handler (service_id = 4)
            session.ticket_handlers.get(&4)
                .map(|handler| handler.session_key.clone())
        } else {
            None
        };

        eprintln!("DEBUG: Returning session_key: {} bytes",
            session_key.as_ref().map(|k| k.get_secret().len()).unwrap_or(0));

        // Return the session key so AUTH_SIGNATURE can be computed
        // Connection secret extraction can be added later if needed for SECURE mode
        Ok((session_key.map(|k| bytes::Bytes::copy_from_slice(k.get_secret())), None))
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
