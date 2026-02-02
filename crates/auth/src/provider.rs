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
///
/// The handler is wrapped in Arc<Mutex<>> so it can be shared with
/// ServiceAuthProviders. When tickets are renewed, all ServiceAuthProviders
/// automatically see the updated tickets.
#[derive(Debug, Clone)]
pub struct MonitorAuthProvider {
    handler: std::sync::Arc<std::sync::Mutex<crate::client::CephXClientHandler>>,
}

impl MonitorAuthProvider {
    /// Create a new monitor auth provider
    pub fn new(entity_name: String) -> Result<Self> {
        let handler =
            crate::client::CephXClientHandler::new(entity_name, crate::protocol::AuthMode::Mon)?;
        Ok(Self {
            handler: std::sync::Arc::new(std::sync::Mutex::new(handler)),
        })
    }

    /// Set the secret key from base64 string
    pub fn set_secret_key_from_base64(&mut self, key_str: &str) -> Result<()> {
        let mut handler = self
            .handler
            .lock()
            .map_err(|e| CephXError::ProtocolError(format!("Failed to lock handler: {}", e)))?;
        handler.set_secret_key_from_base64(key_str)
    }

    /// Set the secret key from keyring file
    pub fn set_secret_key_from_keyring(&mut self, keyring_path: &str) -> Result<()> {
        use crate::keyring::Keyring;
        let keyring = Keyring::from_file(keyring_path)?;

        let mut handler = self
            .handler
            .lock()
            .map_err(|e| CephXError::ProtocolError(format!("Failed to lock handler: {}", e)))?;

        let entity_str = format!("client.{}", handler.entity_name.id);
        let key = keyring.get_key(&entity_str).ok_or_else(|| {
            crate::error::CephXError::InvalidKey(format!("Key not found for {}", entity_str))
        })?;
        handler.set_secret_key(key.clone());
        Ok(())
    }

    /// Get a reference to the shared handler
    pub fn handler(&self) -> &std::sync::Arc<std::sync::Mutex<crate::client::CephXClientHandler>> {
        &self.handler
    }
}

impl AuthProvider for MonitorAuthProvider {
    fn build_auth_payload(&mut self, global_id: u64, _service_id: u32) -> Result<Bytes> {
        // Lock the handler to build auth payload
        let handler = self
            .handler
            .lock()
            .map_err(|e| CephXError::ProtocolError(format!("Failed to lock handler: {}", e)))?;

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
        use denc::Denc;

        // Lock the handler to handle auth response
        let mut handler = self
            .handler
            .lock()
            .map_err(|e| CephXError::ProtocolError(format!("Failed to lock handler: {}", e)))?;

        // Distinguish between AUTH_REPLY_MORE (server challenge) and AUTH_DONE (final result)
        // AUTH_REPLY_MORE: starts with u32 length prefix followed by CephXServerChallenge
        // AUTH_DONE: starts with CephXResponseHeader (u16 request_type = 0x0100)

        // Peek at the payload to determine the type
        let mut peek = payload.clone();
        if peek.len() >= 2 {
            // Read first u16 to check if it's the request_type
            let first_u16 = u16::decode(&mut peek, 0).map_err(|e| {
                crate::error::CephXError::ProtocolError(format!(
                    "Failed to decode first_u16: {}",
                    e
                ))
            })?;

            if first_u16 == crate::protocol::CEPHX_GET_AUTH_SESSION_KEY {
                // This is AUTH_DONE - handle final authentication
                handler.handle_auth_done(payload, global_id, con_mode)
            } else {
                // This is AUTH_REPLY_MORE - handle server challenge
                handler.handle_auth_response(payload)?;
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
///
/// This provider shares the CephXClientHandler with MonClient via Arc<Mutex<>>,
/// so when MonClient renews tickets, all ServiceAuthProviders automatically
/// see the updated tickets.
#[derive(Debug, Clone)]
pub struct ServiceAuthProvider {
    handler: std::sync::Arc<std::sync::Mutex<crate::client::CephXClientHandler>>,
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
        handler: std::sync::Arc<std::sync::Mutex<crate::client::CephXClientHandler>>,
    ) -> Self {
        Self {
            handler,
            authorizer_sent: false,
            server_challenge: None,
            service_id: None,
        }
    }

    /// Get a reference to the shared handler
    pub fn handler(&self) -> &std::sync::Arc<std::sync::Mutex<crate::client::CephXClientHandler>> {
        &self.handler
    }
}

impl AuthProvider for ServiceAuthProvider {
    fn build_auth_payload(&mut self, global_id: u64, service_id: u32) -> Result<Bytes> {
        use tracing::debug;
        debug!(
            "ServiceAuthProvider::build_auth_payload called with service_id={}, global_id={}",
            service_id, global_id
        );

        // Store service_id for later use in handle_auth_response
        self.service_id = Some(service_id);

        // Lock the handler to build authorizer from existing tickets
        // This is a very fast operation (just crypto operations)
        let mut handler = self
            .handler
            .lock()
            .map_err(|e| CephXError::ProtocolError(format!("Failed to lock handler: {}", e)))?;
        let result = handler.build_authorizer(service_id, global_id, self.server_challenge);

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
        use tracing::{debug, info};
        eprintln!("DEBUG: ServiceAuthProvider::handle_auth_response called");
        eprintln!("DEBUG:   payload length: {}", payload.len());
        eprintln!("DEBUG:   global_id: {}", global_id);
        eprintln!("DEBUG:   con_mode: {} (0=CRC, 1=SECURE)", con_mode);
        eprintln!(
            "DEBUG:   payload hex (first 64 bytes): {}",
            payload
                .iter()
                .take(64)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
        );

        debug!(
            "ServiceAuthProvider::handle_auth_response: payload={} bytes",
            payload.len()
        );

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

            // Lock the handler to decrypt the challenge
            let handler = self
                .handler
                .lock()
                .map_err(|e| CephXError::ProtocolError(format!("Failed to lock handler: {}", e)))?;

            // Decrypt and extract server_challenge
            match handler.decrypt_authorize_challenge(service_id, payload.clone()) {
                Ok(server_challenge) => {
                    eprintln!(
                        "DEBUG: Successfully extracted server_challenge: 0x{:016x}",
                        server_challenge
                    );
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
                Err(e) => {
                    eprintln!("DEBUG: Failed to decrypt challenge: {:?}", e);
                    return Err(e);
                }
            }
        }

        info!(
            "AUTH_DONE received for OSD: global_id={}, con_mode={}",
            global_id, con_mode
        );

        // Lock the handler to get the session key
        let handler = self
            .handler
            .lock()
            .map_err(|e| CephXError::ProtocolError(format!("Failed to lock handler: {}", e)))?;

        // Get the session key for this service from the ticket handler
        let session_key = if let Some(session) = handler.get_session() {
            // Get the OSD ticket handler (service_id = 4)
            session
                .ticket_handlers
                .get(&4)
                .map(|handler| handler.session_key.clone())
        } else {
            None
        };

        eprintln!(
            "DEBUG: Returning session_key: {} bytes",
            session_key
                .as_ref()
                .map(|k| k.get_secret().len())
                .unwrap_or(0)
        );

        // For SECURE mode (con_mode >= 1), extract connection_secret from AUTH_DONE payload
        // For authorizer-based auth (OSDs), AUTH_DONE contains encrypted CephXAuthorizeReply
        // The reply contains: struct_v (u8) + nonce_plus_one (u64) + connection_secret (u32 len + bytes)
        let connection_secret = if con_mode >= 1 && !payload.is_empty() {
            eprintln!(
                "DEBUG: Extracting connection_secret from AUTH_DONE payload (con_mode={})",
                con_mode
            );

            // Decrypt the AUTH_DONE payload using the service's session key
            // The payload is encrypted with the session key we got from the ticket
            if let Some(ref sess_key) = session_key {
                use bytes::Buf;
                use denc::Denc;
                let mut buf = payload.clone();

                // Read encrypted length
                let encrypted_len = u32::decode(&mut buf, 0).map_err(|e| {
                    CephXError::ProtocolError(format!("Failed to decode encrypted_len: {}", e))
                })? as usize;
                if buf.remaining() >= encrypted_len {
                    let encrypted_data = buf.copy_to_bytes(encrypted_len);

                    eprintln!(
                        "DEBUG: Decrypting AUTH_DONE: encrypted_len={}",
                        encrypted_len
                    );

                    // Decrypt using the session key (AES-CBC with fixed IV)
                    match crate::client::CephXClientHandler::decrypt_with_key(
                        sess_key,
                        &encrypted_data,
                    ) {
                        Ok(decrypted) => {
                            eprintln!(
                                "DEBUG: Successfully decrypted AUTH_DONE: {} bytes",
                                decrypted.len()
                            );
                            eprintln!(
                                "DEBUG: Decrypted hex (first 64 bytes): {}",
                                decrypted
                                    .iter()
                                    .take(64)
                                    .map(|b| format!("{:02x}", b))
                                    .collect::<Vec<_>>()
                                    .join("")
                            );

                            // The decrypted data contains:
                            // 1. ceph_x_encrypt_header (9 bytes):
                            //    - struct_v (u8)
                            //    - magic (u64 LE)
                            // 2. CephXAuthorizeReply:
                            //    - struct_v (u8)
                            //    - nonce_plus_one (u64 LE)
                            //    - connection_secret (u32 len + bytes) if struct_v >= 2

                            let mut dec_buf = bytes::Bytes::from(decrypted);

                            if dec_buf.remaining() < 9 {
                                eprintln!("DEBUG: Decrypted data too short for encryption header");
                                None
                            } else {
                                // Skip encryption header (struct_v + magic)
                                let enc_struct_v = dec_buf.get_u8();
                                let magic = u64::decode(&mut dec_buf, 0).map_err(|e| {
                                    CephXError::ProtocolError(format!(
                                        "Failed to decode magic: {}",
                                        e
                                    ))
                                })?;
                                eprintln!(
                                    "DEBUG: Encryption header: struct_v={}, magic=0x{:016x}",
                                    enc_struct_v, magic
                                );

                                // Validate magic number (CEPHX_ENC_MAGIC = 0xff009cad8826aa55)
                                const CEPHX_ENC_MAGIC: u64 = 0xff009cad8826aa55;
                                if magic != CEPHX_ENC_MAGIC {
                                    eprintln!("DEBUG: ERROR: Invalid encryption magic! Expected 0x{:016x}, got 0x{:016x}",
                                        CEPHX_ENC_MAGIC, magic);
                                    return Err(CephXError::ProtocolError(
                                        format!("Invalid encryption magic in AUTH_DONE: expected 0x{:016x}, got 0x{:016x}",
                                            CEPHX_ENC_MAGIC, magic)
                                    ));
                                }

                                // Now parse the actual CephXAuthorizeReply
                                if dec_buf.remaining() < 9 {
                                    // struct_v (1) + nonce_plus_one (8)
                                    eprintln!("DEBUG: Not enough bytes for CephXAuthorizeReply");
                                    None
                                } else {
                                    let struct_v = dec_buf.get_u8();
                                    let nonce_plus_one =
                                        u64::decode(&mut dec_buf, 0).map_err(|e| {
                                            CephXError::ProtocolError(format!(
                                                "Failed to decode nonce_plus_one: {}",
                                                e
                                            ))
                                        })?;

                                    eprintln!("DEBUG: CephXAuthorizeReply: struct_v={}, nonce_plus_one=0x{:016x}", struct_v, nonce_plus_one);

                                    // Try to extract connection_secret even if struct_v < 2
                                    // Some servers might still include it
                                    if dec_buf.remaining() >= 4 {
                                        let con_secret_len =
                                            u32::decode(&mut dec_buf, 0).map_err(|e| {
                                                CephXError::ProtocolError(format!(
                                                    "Failed to decode con_secret_len: {}",
                                                    e
                                                ))
                                            })?
                                                as usize;
                                        eprintln!(
                                            "DEBUG: connection_secret length: {} (struct_v={})",
                                            con_secret_len, struct_v
                                        );

                                        if con_secret_len > 0
                                            && con_secret_len <= 256
                                            && dec_buf.remaining() >= con_secret_len
                                        {
                                            let connection_secret_bytes =
                                                dec_buf.copy_to_bytes(con_secret_len);
                                            eprintln!(
                                                "DEBUG: Extracted connection_secret: {} bytes",
                                                connection_secret_bytes.len()
                                            );
                                            eprintln!(
                                                "DEBUG: Connection_secret hex: {}",
                                                connection_secret_bytes
                                                    .iter()
                                                    .take(64)
                                                    .map(|b| format!("{:02x}", b))
                                                    .collect::<Vec<_>>()
                                                    .join("")
                                            );

                                            Some(connection_secret_bytes)
                                        } else {
                                            eprintln!("DEBUG: connection_secret length invalid or not enough data: len={}, remaining={}", con_secret_len, dec_buf.remaining());
                                            None
                                        }
                                    } else {
                                        eprintln!("DEBUG: No connection_secret (struct_v={}, not enough remaining bytes)", struct_v);
                                        None
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("DEBUG: Failed to decrypt AUTH_DONE: {:?}", e);
                            None
                        }
                    }
                } else {
                    eprintln!("DEBUG: AUTH_DONE payload too short for encrypted data");
                    None
                }
            } else {
                eprintln!("DEBUG: No session key available to decrypt AUTH_DONE");
                None
            }
        } else {
            eprintln!(
                "DEBUG: Skipping connection_secret extraction (con_mode={}, payload empty={})",
                con_mode,
                payload.is_empty()
            );
            None
        };

        eprintln!(
            "DEBUG: Returning connection_secret: {} bytes",
            connection_secret.as_ref().map(|c| c.len()).unwrap_or(0)
        );

        // Return the session key so AUTH_SIGNATURE can be computed
        // Also return connection_secret for SECURE mode
        Ok((
            session_key.map(|k| bytes::Bytes::copy_from_slice(k.get_secret())),
            connection_secret,
        ))
    }

    fn has_valid_ticket(&self, service_id: u32) -> bool {
        // Lock the handler to check ticket validity
        if let Ok(handler) = self.handler.lock() {
            if let Some(session) = handler.get_session() {
                session.has_valid_ticket(service_id)
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
