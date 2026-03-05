//! CephX client-side authentication handler

use crate::error::{CephXError, Result};
use crate::protocol::{
    AuthMode, CephXAuthenticate, CephXRequestHeader, CephXServerChallenge,
    CEPHX_GET_AUTH_SESSION_KEY,
};
use crate::types::{CephXSession, CephXTicketBlob, CryptoKey, EntityName, EntityType};
use bytes::{Buf, Bytes, BytesMut};
use denc::Denc;
use rand::RngCore;
use std::time::Duration;
use tracing::{debug, trace, warn};

/// Decoded service ticket information: (service_type, session_key, secret_id, ticket_blob, validity)
type DecodedServiceTicket = (EntityType, CryptoKey, u64, CephXTicketBlob, Duration);

/// Authentication result from handler
#[derive(Debug, PartialEq)]
pub enum AuthResult {
    /// Authentication successful
    Success,
    /// Need more data (multi-round auth)
    NeedMoreData,
    /// Authentication failed
    Failed(String),
}

/// CephX client authentication handler
#[derive(Debug, Clone)]
pub struct CephXClientHandler {
    /// Entity name (e.g., "client.admin")
    pub entity_name: EntityName,
    /// Server challenge received
    pub server_challenge: Option<u64>,
    /// Whether we're in the initial handshake phase
    pub starting: bool,
    /// Session information
    pub session: Option<CephXSession>,
    /// Client secret key
    secret_key: Option<CryptoKey>,
    /// Auth mode (Authorizer for OSDs, Mon for monitors)
    auth_mode: AuthMode,
    /// Bitmask of service ticket types to request alongside AUTH.
    ///
    /// Mirrors `want_keys` in `AuthClientHandler` (C++) / `want_keys` in the Linux kernel
    /// client. AUTH is always implied and should not be included here. Defaults to
    /// `MON | OSD | MGR`, matching what librados requests.
    want_keys: EntityType,
}

impl CephXClientHandler {
    /// Create a new client handler
    ///
    /// # Arguments
    /// * `entity_name_str` - Entity name (e.g., "client.admin")
    /// * `auth_mode` - Auth mode (AuthMode::Mon for monitors, AuthMode::Authorizer for OSDs)
    pub fn new(entity_name_str: &str, auth_mode: AuthMode) -> Result<Self> {
        let entity_name = entity_name_str.parse()?;
        Ok(Self {
            entity_name,
            server_challenge: None,
            starting: true,
            session: None,
            secret_key: None,
            auth_mode,
            // Default matches librados: MON | OSD | MGR
            want_keys: EntityType::MON | EntityType::OSD | EntityType::MGR,
        })
    }

    /// Set the service ticket types to request alongside the AUTH ticket.
    ///
    /// `keys` is a bitmask of `EntityType::*` values. AUTH is always requested
    /// implicitly and does not need to be included. Defaults to `MON | OSD | MGR`.
    pub fn set_want_keys(&mut self, keys: EntityType) {
        self.want_keys = keys;
    }

    /// Set the client's secret key
    pub fn set_secret_key(&mut self, key: CryptoKey) {
        debug!(
            "Setting secret key for {}: {} bytes, first 16: {}",
            self.entity_name,
            key.len(),
            hex::encode(&key.secret[..16.min(key.len())])
        );
        self.secret_key = Some(key);
    }

    /// Load secret key from base64
    pub fn set_secret_key_from_base64(&mut self, base64_key: &str) -> Result<()> {
        let key = CryptoKey::from_base64(base64_key)?;
        self.secret_key = Some(key);
        Ok(())
    }

    /// Build initial auth request (first phase)
    /// Sends entity_name and global_id only (no CephXAuthenticate yet)
    /// This matches C++ MonConnection::get_auth_request()
    pub fn build_initial_request(&self, global_id: u64) -> Result<Bytes> {
        debug!(
            "Building initial CephX auth request for {} (global_id={})",
            self.entity_name, global_id
        );

        // Initial request contains: auth_mode + entity_name + global_id
        // Pre-allocate: 1 byte (auth_mode) + entity_name size + 8 bytes (global_id)
        let estimated_size = 1 + 16 + 8; // Conservative estimate
        let mut payload = BytesMut::with_capacity(estimated_size);
        self.auth_mode.as_u8().encode(&mut payload, 0)?;
        self.entity_name.encode(&mut payload, 0)?;
        global_id.encode(&mut payload, 0)?;

        Ok(payload.freeze())
    }

    /// Build authenticate request (second phase) after receiving server challenge
    /// Sends CephXRequestHeader + CephXAuthenticate (NO auth_mode)
    /// This matches C++ CephxClientHandler::build_request()
    pub fn build_authenticate_request(&self) -> Result<Bytes> {
        let server_challenge = self
            .server_challenge
            .ok_or_else(|| CephXError::ProtocolError("No server challenge received".into()))?;

        let secret_key = self
            .secret_key
            .as_ref()
            .ok_or_else(|| CephXError::AuthenticationFailed("No secret key set".into()))?;

        // Generate client challenge
        let mut rng = rand::thread_rng();
        let client_challenge = rng.next_u64();

        debug!(
            "Building CephX authenticate: client_challenge=0x{:016x}, server_challenge=0x{:016x}",
            client_challenge, server_challenge
        );

        // Calculate session key from challenges
        let session_key =
            self.calculate_session_key(secret_key, server_challenge, client_challenge)?;

        // Build the request header
        let header = CephXRequestHeader {
            request_type: CEPHX_GET_AUTH_SESSION_KEY,
        };

        // Request the configured service tickets alongside AUTH.
        // Mirrors `other_keys = want_keys & ~CEPH_ENTITY_TYPE_AUTH` from the Linux kernel client.
        let other_keys: u32 = (self.want_keys & !EntityType::AUTH).bits();
        let auth_request = CephXAuthenticate::new(
            client_challenge,
            session_key,
            CephXTicketBlob::default(), // No old ticket for initial auth
            other_keys,
        );

        // Encode header + authenticate (NO auth_mode for second request)
        // Pre-allocate: header + authenticate structure
        let estimated_size = 256; // Conservative estimate for CephXAuthenticate
        let mut payload = BytesMut::with_capacity(estimated_size);
        header.encode(&mut payload, 0)?;
        auth_request.encode(&mut payload, 0)?;

        Ok(payload.freeze())
    }

    /// Calculate session key from client secret and challenges
    ///
    /// Implements the CephX challenge calculation algorithm:
    /// cephx_calc_client_server_challenge() from CephxProtocol.cc
    fn calculate_session_key(
        &self,
        secret_key: &CryptoKey,
        server_challenge: u64,
        client_challenge: u64,
    ) -> Result<u64> {
        use crate::protocol::{CephXChallengeBlob, CephXEncryptedEnvelope};

        debug!(
            "Calculating session key: server_challenge=0x{:016x}, client_challenge=0x{:016x}",
            server_challenge, client_challenge
        );

        // Create challenge blob wrapped in encrypted envelope
        let envelope = CephXEncryptedEnvelope {
            payload: CephXChallengeBlob {
                server_challenge,
                client_challenge,
            },
        };

        let estimated_size = 32; // struct_v + magic + 2 u64s
        let mut bl = BytesMut::with_capacity(estimated_size);
        envelope.encode(&mut bl, 0)?;

        // Encrypt the envelope using the client's secret key
        let ciphertext = secret_key.encrypt(&bl)?;

        // In C++, encode_encrypt() adds a u32 length prefix to the encrypted data
        // before XOR folding. We need to replicate this behavior.
        let estimated_folding_size = 4 + ciphertext.len();
        let mut folding_buffer = BytesMut::with_capacity(estimated_folding_size);
        ciphertext.encode(&mut folding_buffer, 0)?;
        let mut folding_data = folding_buffer.freeze();

        // XOR fold the entire buffer (length prefix + encrypted data) to get a 64-bit key
        // C++ only processes complete 8-byte chunks, ignoring any remaining bytes
        let num_complete_chunks = folding_data.len() / 8;
        let mut key = 0u64;

        for _ in 0..num_complete_chunks {
            key ^= u64::decode(&mut folding_data, 0)?;
        }

        debug!("Calculated session key: 0x{:016x}", key);
        Ok(key)
    }

    /// Handle server auth response
    pub fn handle_auth_response(&mut self, mut response: Bytes) -> Result<AuthResult> {
        if self.starting {
            // First response should be server challenge
            debug!(
                "Handling initial server challenge ({} bytes)",
                response.len()
            );

            // AUTH_REPLY_MORE has a u32 length prefix before the actual CephXServerChallenge
            // Format: [u32 length][u8 struct_v][u64 server_challenge]
            if response.len() < 4 {
                return Err(CephXError::ProtocolError(
                    "AUTH_REPLY_MORE too short".into(),
                ));
            }
            let payload_len = u32::decode(&mut response, 0)? as usize;
            debug!(
                "AUTH_REPLY_MORE: length_prefix={}, remaining={} bytes",
                payload_len,
                response.len()
            );

            let challenge = CephXServerChallenge::decode(&mut response, 0)?;
            self.server_challenge = Some(challenge.server_challenge);
            self.starting = false;

            debug!(
                "Received server challenge: {:x}",
                challenge.server_challenge
            );
            Ok(AuthResult::NeedMoreData)
        } else {
            // Subsequent AUTH_REPLY_MORE messages should not occur in the normal flow.
            // After the initial server challenge, we should get AUTH_DONE instead.
            // If we reach here, it's an unexpected protocol state.
            Err(CephXError::ProtocolError(
                "Unexpected AUTH_REPLY_MORE after initial challenge. Expected AUTH_DONE.".into(),
            ))
        }
    }

    /// Decrypt an EncryptedServiceTicket and extract session key and validity
    fn decrypt_service_ticket(
        &self,
        encrypted_ticket: &crate::protocol::EncryptedServiceTicket,
        secret_key: &CryptoKey,
    ) -> Result<(CryptoKey, Duration)> {
        use crate::protocol::CephXEncryptedEnvelope;

        // Decrypt the encrypted data
        let decrypted = secret_key.decrypt(&encrypted_ticket.encrypted_data)?;
        let mut decrypted_data = decrypted;

        // Decode using CephXEncryptedEnvelope<CephXServiceTicket>
        let envelope = CephXEncryptedEnvelope::<crate::protocol::CephXServiceTicket>::decode(
            &mut decrypted_data,
            0,
        )?;

        let service_ticket = envelope.payload;
        Ok((service_ticket.session_key, service_ticket.validity))
    }

    /// Decode extra_tickets in the simpler non-versioned format
    ///
    /// Format: u8 version, u32 num, for each: u32 service_id, EncryptedServiceTicket, u8 enc, ticket_blob
    ///
    /// Returns partial results if some tickets fail to decode (common for placeholder tickets)
    fn decode_extra_tickets(
        &self,
        buf: &mut Bytes,
        auth_session_key: &CryptoKey,
    ) -> Result<Vec<DecodedServiceTicket>> {
        let _version = u8::decode(buf, 0)?;
        let num = u32::decode(buf, 0)?;
        debug!("Decoding {} extra tickets", num);

        let mut result =
            Vec::with_capacity(num.min(crate::protocol::MAX_EXTRA_TICKETS as u32) as usize);

        for i in 0..num {
            match self.try_decode_single_ticket(buf, auth_session_key) {
                Ok(ticket_info) => {
                    trace!(
                        "Decoded extra ticket {}/{} for service {:?}",
                        i + 1,
                        num,
                        ticket_info.0
                    );
                    result.push(ticket_info);
                }
                Err(e) => {
                    // Extra tickets may contain invalid/placeholder data
                    // Return successfully decoded tickets so far
                    debug!(
                        "Stopping at ticket {}/{} due to error: {:?} (decoded {} valid tickets)",
                        i + 1,
                        num,
                        e,
                        result.len()
                    );
                    break;
                }
            }
        }

        Ok(result)
    }

    /// Try to decode a single extra ticket
    ///
    /// Uses `?` operator for clean error propagation - caller handles partial results
    fn try_decode_single_ticket(
        &self,
        buf: &mut Bytes,
        auth_session_key: &CryptoKey,
    ) -> Result<DecodedServiceTicket> {
        let service_type = EntityType::from_bits_retain(u32::decode(buf, 0)?);
        let encrypted_ticket = crate::protocol::EncryptedServiceTicket::decode(buf, 0)?;
        let (session_key, validity) =
            self.decrypt_service_ticket(&encrypted_ticket, auth_session_key)?;

        // Extract ticket blob bytes (decrypt if encrypted)
        let ticket_enc = u8::decode(buf, 0)?;
        let mut ticket_blob_bytes = if ticket_enc != 0 {
            // Encrypted: read length-prefixed encrypted data and decrypt
            // Bytes::decode() is zero-copy when buf is Bytes (just increments refcount)
            let encrypted_bl = Bytes::decode(buf, 0)?;
            session_key.decrypt(&encrypted_bl)?
        } else {
            // Unencrypted: read length-prefixed data directly
            Bytes::decode(buf, 0)?
        };

        // Decode ticket blob from the extracted bytes
        let ticket_blob = CephXTicketBlob::decode(&mut ticket_blob_bytes, 0)?;

        Ok((
            service_type,
            session_key,
            ticket_blob.secret_id,
            ticket_blob,
            validity,
        ))
    }

    /// Try to decode connection_secret from payload (inner method with ? error propagation)
    fn try_decode_connection_secret(
        payload: &mut Bytes,
        session_key: &CryptoKey,
    ) -> Result<Option<Bytes>> {
        use crate::protocol::CephXEncryptedEnvelope;

        // Read outer bufferlist (length-prefixed)
        let mut encrypted_secret_bl = Bytes::decode(payload, 0)?;
        if encrypted_secret_bl.is_empty() {
            return Ok(None);
        }

        // Read inner encrypted data (length-prefixed)
        let encrypted_secret = Bytes::decode(&mut encrypted_secret_bl, 0)?;
        if encrypted_secret.is_empty() {
            return Ok(None);
        }

        // Decrypt connection_secret
        let mut decrypted_secret = session_key.decrypt(&encrypted_secret)?;

        // Decode envelope
        let envelope = CephXEncryptedEnvelope::<Bytes>::decode(&mut decrypted_secret, 0)?;

        // Return payload if non-empty (CRC mode has empty connection_secret)
        if envelope.payload.is_empty() {
            Ok(None)
        } else {
            Ok(Some(envelope.payload))
        }
    }

    /// Decode connection_secret from payload (outer method that handles errors gracefully)
    ///
    /// Returns Ok(None) if decoding fails (connection_secret is optional for CRC mode)
    fn decode_connection_secret(
        &self,
        payload: &mut Bytes,
        session_key: &CryptoKey,
    ) -> Result<Option<Bytes>> {
        if payload.remaining() == 0 {
            return Ok(None);
        }

        match Self::try_decode_connection_secret(payload, session_key) {
            Ok(secret) => {
                if let Some(ref s) = secret {
                    debug!("Connection secret: {} bytes", s.len());
                } else {
                    debug!("Connection secret length is 0 (CRC mode), leaving as None");
                }
                Ok(secret)
            }
            Err(e) => {
                debug!("Failed to decode connection_secret: {:?}", e);
                Ok(None)
            }
        }
    }

    /// Store service tickets in session
    ///
    /// Creates session if it doesn't exist yet, then stores all ticket handlers
    fn store_ticket_handlers(
        &mut self,
        ticket_handlers: Vec<DecodedServiceTicket>,
        global_id: u64,
    ) -> Result<()> {
        let secret_key = self
            .secret_key
            .as_ref()
            .ok_or_else(|| CephXError::AuthenticationFailed("No secret key set".into()))?;

        // Create session if it doesn't exist yet
        if self.session.is_none() {
            debug!("Creating new session with global_id={}", global_id);
            self.session = Some(CephXSession::new(
                self.entity_name.clone(),
                global_id,
                secret_key.clone(),
            ));
        }

        // Store all service tickets in the session
        if let Some(session) = &mut self.session {
            for (service_type, session_key, secret_id, ticket_blob, validity) in ticket_handlers {
                let handler = session.get_ticket_handler(service_type);
                handler.update(session_key, secret_id, ticket_blob, validity);
                debug!(
                    "Stored ticket for service {:?} (secret_id={})",
                    service_type, secret_id
                );
            }
        } else {
            warn!("No session available to store ticket handlers");
        }

        Ok(())
    }

    /// Handle AUTH_DONE payload to extract session_key and connection_secret
    /// Returns (session_key_bytes, connection_secret_bytes) if in SECURE mode
    pub fn handle_auth_done(
        &mut self,
        mut auth_payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)> {
        use crate::protocol::CephXResponseHeader;

        debug!(
            "Handling AUTH_DONE: global_id={}, con_mode={}, payload={} bytes",
            global_id,
            con_mode,
            auth_payload.len()
        );

        let secret_key = self
            .secret_key
            .as_ref()
            .ok_or_else(|| CephXError::AuthenticationFailed("No secret key set".into()))?;

        // AUTH_DONE auth_payload structure:
        // 1. CephXResponseHeader (u16 request_type) - should be CEPHX_GET_AUTH_SESSION_KEY (0x0100)
        // 2. Service ticket reply (parsed by verify_service_ticket_reply)
        // 3. connection_secret bufferlist (u32 len + encrypted data)
        // 4. extra_tickets bufferlist (u32 len + data)

        // Decode CephXResponseHeader
        let header = CephXResponseHeader::decode(&mut auth_payload, 0)?;
        debug!(
            "CephXResponseHeader: request_type=0x{:04x}, status={}",
            header.request_type, header.status
        );

        if header.request_type != crate::protocol::CEPHX_GET_AUTH_SESSION_KEY {
            return Err(CephXError::ProtocolError(format!(
                "Unexpected request_type: 0x{:04x}, expected CEPHX_GET_AUTH_SESSION_KEY (0x0100)",
                header.request_type
            )));
        }

        if header.status != 0 {
            return Err(CephXError::AuthenticationFailed(format!(
                "Authentication failed with status: {}",
                header.status
            )));
        }

        // Decode the service ticket reply using the new Denc structure
        let ticket_reply = crate::protocol::ServiceTicketReply::decode(&mut auth_payload, 0)?;
        debug!(
            "service_ticket_reply_v: {}, num_tickets: {}",
            ticket_reply.struct_v,
            ticket_reply.tickets.len()
        );

        if ticket_reply.tickets.is_empty() {
            return Err(CephXError::ProtocolError("No tickets in AUTH_DONE".into()));
        }

        // Process all service tickets
        let mut ticket_handlers: Vec<DecodedServiceTicket> = Vec::new();

        for ticket_info in &ticket_reply.tickets {
            // Decrypt the encrypted service ticket to get session key and validity
            let (session_key, validity) =
                self.decrypt_service_ticket(&ticket_info.encrypted_service_ticket, secret_key)?;

            debug!(
                "Decoded ticket for service_id={}, validity={:?}",
                ticket_info.service_id, validity
            );

            ticket_handlers.push((
                EntityType::from_bits_retain(ticket_info.service_id),
                session_key,
                ticket_info.ticket_blob.secret_id,
                ticket_info.ticket_blob.clone(),
                validity,
            ));
        }

        // Extract first ticket's session key (AUTH service) for returning and decryption
        let (_, first_session_key, _, _, _) = ticket_handlers
            .first()
            .ok_or_else(|| CephXError::ProtocolError("No tickets available".into()))?;
        let session_key_bytes = first_session_key.secret.clone();
        let auth_session_key = first_session_key.clone();

        // Decode connection_secret blob (encrypted with session_key)
        let connection_secret_bytes =
            self.decode_connection_secret(&mut auth_payload, &auth_session_key)?;

        // Parse extra_tickets if any remain in the payload
        trace!(
            "After connection_secret, auth_payload remaining: {} bytes",
            auth_payload.remaining()
        );
        if auth_payload.remaining() > 0 {
            if let Ok(extra_tickets_len) = u32::decode(&mut auth_payload, 0) {
                let extra_tickets_len = extra_tickets_len as usize;
                debug!("extra_tickets blob length: {}", extra_tickets_len);

                if extra_tickets_len > 0 && auth_payload.remaining() >= extra_tickets_len {
                    let mut extra_tickets_bl = auth_payload.split_to(extra_tickets_len);
                    trace!("Parsing extra_tickets: {} bytes", extra_tickets_bl.len());

                    // Extra tickets are encrypted with the AUTH session key (from first ticket)
                    match self.decode_extra_tickets(&mut extra_tickets_bl, &auth_session_key) {
                        Ok(extra_handlers) => {
                            ticket_handlers.extend(extra_handlers);
                        }
                        Err(e) => {
                            debug!("Failed to decode extra_tickets: {:?}", e);
                        }
                    }
                }
            }
        }

        // Store all tickets in session
        self.store_ticket_handlers(ticket_handlers, global_id)?;

        Ok((Some(session_key_bytes), connection_secret_bytes))
    }

    /// Decrypt the server challenge from AUTH_REPLY_MORE
    /// Returns the server_challenge value
    pub fn decrypt_authorize_challenge(
        &self,
        service_type: EntityType,
        mut encrypted_payload: Bytes,
    ) -> Result<u64> {
        use crate::protocol::{CephXAuthorizeReply, CephXEncryptedEnvelope};

        debug!(
            "Decrypting authorize challenge for service {:?}",
            service_type
        );
        trace!(
            "decrypt_authorize_challenge: payload length={}",
            encrypted_payload.len()
        );

        // Get session or return error
        let session = self
            .session
            .as_ref()
            .ok_or_else(|| CephXError::AuthenticationFailed("No session available".into()))?;

        // Get ticket handler for the service
        let handler = session.ticket_handlers.get(&service_type).ok_or_else(|| {
            CephXError::AuthenticationFailed(format!(
                "No ticket handler for service {:?}",
                service_type
            ))
        })?;

        if !handler.have_key {
            return Err(CephXError::AuthenticationFailed(format!(
                "No session key for service {:?}",
                service_type
            )));
        }

        // Decode length-prefixed encrypted data using Denc
        let encrypted_data = Bytes::decode(&mut encrypted_payload, 0)?;
        trace!("encrypted_len: {}", encrypted_data.len());

        // Decrypt using session key
        let mut dec_buf = handler.session_key.decrypt(&encrypted_data)?;

        // Decode the encrypted envelope containing CephXAuthorizeReply
        let envelope = CephXEncryptedEnvelope::<CephXAuthorizeReply>::decode(&mut dec_buf, 0)?;

        let server_challenge = envelope.payload.nonce_plus_one;
        debug!("Extracted server_challenge: 0x{:016x}", server_challenge);

        Ok(server_challenge)
    }

    /// Build an authorizer for a service (OSD, MDS, etc.)
    /// This is used when connecting to services after obtaining tickets from the monitor
    /// Returns the authorizer buffer to be sent to the service
    ///
    /// # Arguments
    /// * `service_id` - Service type (4=OSD, 2=MDS, etc.)
    /// * `global_id` - Client global ID
    /// * `server_challenge` - Optional server challenge (for challenge-response)
    pub fn build_authorizer(
        &mut self,
        service_type: EntityType,
        global_id: u64,
        server_challenge: Option<u64>,
    ) -> Result<Bytes> {
        use crate::protocol::{CephXAuthorizeA, CephXAuthorizeB};

        debug!(
            "Building authorizer for service_type={:?} (global_id={})",
            service_type, global_id
        );

        // Get session or return error
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| CephXError::AuthenticationFailed("No session available".into()))?;

        // Get global_id first (before mut borrow)
        let actual_global_id = session.global_id;

        // Get or create ticket handler
        let handler = session.get_ticket_handler(service_type);

        if !handler.have_key {
            return Err(CephXError::AuthenticationFailed(format!(
                "No ticket available for service {:?}",
                service_type
            )));
        }

        let ticket_blob = handler
            .ticket_blob
            .as_ref()
            .ok_or_else(|| {
                CephXError::AuthenticationFailed(format!(
                    "No ticket blob for service {:?}",
                    service_type
                ))
            })?
            .clone();

        // Clone the session key and secret_id to avoid borrow checker issues
        let session_key = handler.session_key.clone();
        let secret_id = handler.secret_id;

        debug!(
            "Building authorizer: global_id={}, service_type={:?}, secret_id={}, session_key_len={}",
            actual_global_id,
            service_type,
            secret_id,
            session_key.secret.len()
        );

        // Build CephXAuthorizeA
        let authorize_a = CephXAuthorizeA::new(actual_global_id, service_type.bits(), ticket_blob);

        // Generate nonce and build CephXAuthorizeB
        let mut rng = rand::thread_rng();
        let nonce = rng.next_u64();
        let authorize_b = if let Some(challenge) = server_challenge {
            CephXAuthorizeB::with_challenge(nonce, challenge)
        } else {
            CephXAuthorizeB::new(nonce)
        };

        // Encode authorize_a
        let estimated_size = 128; // Conservative estimate for CephXAuthorizeA
        let mut authorizer_buf = BytesMut::with_capacity(estimated_size);
        authorize_a.encode(&mut authorizer_buf, 0)?;

        trace!(
            "nonce: 0x{:016x}, base_bl length: {}",
            nonce,
            authorizer_buf.len()
        );

        // Encrypt authorize_b with session key (envelope wrapping happens inside)
        let encrypted_b = Self::encrypt_authorize_b(&session_key, &authorize_b)?;

        trace!("encrypted_b length: {}", encrypted_b.len());

        // Append encrypted authorize_b to the authorizer buffer
        authorizer_buf.extend_from_slice(&encrypted_b);

        debug!("Built authorizer: {} bytes", authorizer_buf.len());
        Ok(authorizer_buf.freeze())
    }

    /// Encrypt CephXAuthorizeB using the service session key
    /// This replicates the C++ ceph_x_encrypt behavior
    fn encrypt_authorize_b(
        session_key: &CryptoKey,
        authorize_b: &crate::protocol::CephXAuthorizeB,
    ) -> Result<Bytes> {
        use crate::protocol::CephXEncryptedEnvelope;

        // Wrap authorize_b in encrypted envelope and encode
        let envelope = CephXEncryptedEnvelope {
            payload: authorize_b.clone(),
        };

        let estimated_size = 64; // Conservative estimate for envelope
        let mut envelope_buf = BytesMut::with_capacity(estimated_size);
        envelope.encode(&mut envelope_buf, 0)?;

        // Encrypt with AES-128-CBC using session key
        let ciphertext = session_key.encrypt(&envelope_buf)?;

        // Add length prefix (wire format: u32 len + encrypted data)
        let mut result = BytesMut::with_capacity(4 + ciphertext.len());
        (ciphertext.len() as u32).encode(&mut result, 0)?;
        result.extend_from_slice(&ciphertext);

        debug!("Encrypted CephXAuthorizeB: {} bytes total", result.len());
        Ok(result.freeze())
    }

    /// Get the current session if authenticated
    pub fn get_session(&self) -> Option<&CephXSession> {
        self.session.as_ref()
    }

    /// Get mutable session reference
    pub fn get_session_mut(&mut self) -> Option<&mut CephXSession> {
        self.session.as_mut()
    }

    /// Build ticket renewal request (CEPHX_GET_PRINCIPAL_SESSION_KEY)
    ///
    /// This builds a request to renew service tickets (OSD, MDS, MGR, etc.)
    /// The request includes:
    /// 1. CephXRequestHeader with CEPHX_GET_PRINCIPAL_SESSION_KEY
    /// 2. An authorizer built from the AUTH ticket handler
    /// 3. CephXServiceTicketRequest with the needed service keys bitmask
    ///
    /// # Arguments
    /// * `global_id` - Global ID assigned by monitor
    /// * `needed_keys` - Bitmask of service types that need renewal (MON|OSD|MDS|MGR)
    ///
    /// # Returns
    /// Returns the encoded ticket renewal request payload
    pub fn build_ticket_renewal_request(
        &mut self,
        global_id: u64,
        needed_keys: EntityType,
    ) -> Result<Bytes> {
        debug!(
            "Building ticket renewal request for global_id={}, needed_keys={:?}",
            global_id, needed_keys
        );

        // Get the session (must be authenticated first)
        let session = self
            .session
            .as_ref()
            .ok_or_else(|| CephXError::AuthenticationFailed("No session available".into()))?;

        // Build the request payload
        let mut payload = BytesMut::new();

        // 1. Encode request header
        let header = crate::protocol::CephXRequestHeader {
            request_type: crate::protocol::CEPHX_GET_PRINCIPAL_SESSION_KEY,
        };
        header.encode(&mut payload, 0)?;

        // 2. Build and encode authorizer from AUTH ticket handler
        // The authorizer proves we have a valid AUTH ticket
        let auth_ticket_handler =
            session
                .ticket_handlers
                .get(&EntityType::AUTH)
                .ok_or_else(|| {
                    CephXError::AuthenticationFailed("No AUTH ticket handler available".into())
                })?;

        if !auth_ticket_handler.have_key {
            return Err(CephXError::AuthenticationFailed(
                "AUTH ticket handler has no key".into(),
            ));
        }

        // Build authorizer using the AUTH ticket
        // The authorizer proves we have a valid AUTH ticket
        let authorizer = self.build_authorizer(
            EntityType::AUTH,
            global_id,
            None, // No server challenge for ticket renewal
        )?;

        payload.extend_from_slice(&authorizer);

        // 3. Encode service ticket request with needed keys
        let ticket_request = crate::protocol::CephXServiceTicketRequest::new(needed_keys.bits());
        ticket_request.encode(&mut payload, 0)?;

        debug!(
            "Built ticket renewal request: {} bytes (header + authorizer + ticket_request)",
            payload.len()
        );
        Ok(payload.freeze())
    }

    /// Reset the handler for a new authentication attempt
    pub fn reset(&mut self) {
        debug!("Resetting CephX client handler");
        self.server_challenge = None;
        self.starting = true;
        self.session = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::AuthMode;
    use crate::types::{CephXSession, CryptoKey};

    #[test]
    fn test_client_handler_creation() {
        let handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        assert_eq!(handler.entity_name.to_string(), "client.admin");
        assert!(handler.starting);
        assert!(handler.session.is_none());
        assert!(handler.secret_key.is_none());
        assert!(handler.server_challenge.is_none());
    }

    #[test]
    fn test_client_handler_creation_with_authorizer_mode() {
        let handler = CephXClientHandler::new("client.test", AuthMode::Authorizer).unwrap();
        assert_eq!(handler.entity_name.to_string(), "client.test");
        assert!(handler.starting);
    }

    #[test]
    fn test_set_secret_key() {
        let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        let key =
            CryptoKey::from_base64("AQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAABAgMEBQYHCA==")
                .unwrap();

        handler.set_secret_key(key.clone());
        assert!(handler.secret_key.is_some());
        assert_eq!(handler.secret_key.unwrap().len(), key.len());
    }

    #[test]
    fn test_set_secret_key_from_base64() {
        let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        let base64_key = "AQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAABAgMEBQYHCA==";

        handler.set_secret_key_from_base64(base64_key).unwrap();
        assert!(handler.secret_key.is_some());
    }

    #[test]
    fn test_set_secret_key_from_base64_invalid() {
        let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        let result = handler.set_secret_key_from_base64("invalid-base64!");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_initial_request() {
        let handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        let global_id = 12345u64;

        let request = handler.build_initial_request(global_id).unwrap();
        assert!(!request.is_empty());

        // Check that it starts with auth_mode byte (10 for Mon)
        assert_eq!(request[0], 10);
    }

    #[test]
    fn test_build_initial_request_authorizer_mode() {
        let handler = CephXClientHandler::new("client.test", AuthMode::Authorizer).unwrap();
        let global_id = 54321u64;

        let request = handler.build_initial_request(global_id).unwrap();
        assert!(!request.is_empty());

        // Check that it starts with auth_mode byte (1 for Authorizer)
        assert_eq!(request[0], 1);
    }

    #[test]
    fn test_build_authenticate_request_without_server_challenge() {
        let handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();

        // Should fail without server challenge
        let result = handler.build_authenticate_request();
        assert!(result.is_err());
    }

    #[test]
    fn test_build_authenticate_request_without_secret_key() {
        let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        handler.server_challenge = Some(0x1234567890abcdef);

        // Should fail without secret key
        let result = handler.build_authenticate_request();
        assert!(result.is_err());
    }

    #[test]
    fn test_get_session_when_none() {
        let handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        assert!(handler.get_session().is_none());
    }

    #[test]
    fn test_get_session_mut_when_none() {
        let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        assert!(handler.get_session_mut().is_none());
    }

    #[test]
    fn test_reset() {
        let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();

        // Set some state
        handler.server_challenge = Some(12345);
        handler.starting = false;
        let entity_name = handler.entity_name.clone();
        handler.session = Some(CephXSession::new(
            entity_name,
            12345,
            CryptoKey::from_base64("AQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAABAgMEBQYHCA==")
                .unwrap(),
        ));

        // Reset
        handler.reset();

        // Check that state is cleared
        assert!(handler.server_challenge.is_none());
        assert!(handler.starting);
        assert!(handler.session.is_none());
    }

    #[test]
    fn test_crypto_key_decrypt_invalid_ciphertext() {
        let key =
            CryptoKey::from_base64("AQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAABAgMEBQYHCA==")
                .unwrap();

        // Too short ciphertext (less than AES block size)
        let result = key.decrypt(&[1, 2, 3, 4]);
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_result_equality() {
        assert_eq!(AuthResult::Success, AuthResult::Success);
        assert_eq!(AuthResult::NeedMoreData, AuthResult::NeedMoreData);
        assert_eq!(
            AuthResult::Failed("error".to_string()),
            AuthResult::Failed("error".to_string())
        );
        assert_ne!(
            AuthResult::Failed("error1".to_string()),
            AuthResult::Failed("error2".to_string())
        );
        assert_ne!(AuthResult::Success, AuthResult::NeedMoreData);
    }

    #[test]
    fn test_entity_name_parsing() {
        // Valid entity names
        let handler1 = CephXClientHandler::new("client.admin", AuthMode::Mon);
        assert!(handler1.is_ok());

        let handler2 = CephXClientHandler::new("client.test", AuthMode::Mon);
        assert!(handler2.is_ok());

        let handler3 = CephXClientHandler::new("osd.0", AuthMode::Authorizer);
        assert!(handler3.is_ok());

        let handler4 = CephXClientHandler::new("mon.a", AuthMode::Mon);
        assert!(handler4.is_ok());
    }

    #[test]
    fn test_crypto_key_decrypt_empty_ciphertext() {
        let key =
            CryptoKey::from_base64("AQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAABAgMEBQYHCA==")
                .unwrap();

        let result = key.decrypt(&[]);
        assert!(result.is_err());
    }
}
