//! CephX client-side authentication handler

use crate::error::{CephXError, Result};
use crate::protocol::{
    AuthMode, CephXAuthenticate, CephXRequestHeader, CephXServerChallenge,
    CEPHX_GET_AUTH_SESSION_KEY,
};
use crate::types::{CephXSession, CephXTicketBlob, CryptoKey, EntityName};
use bytes::{BufMut, Bytes, BytesMut};
use denc::Denc;
use rand::RngCore;
use std::time::Duration;
use tracing::{debug, trace, warn};

/// Decoded service ticket information: (service_id, session_key, secret_id, ticket_blob, validity)
type DecodedServiceTicket = (u32, CryptoKey, u64, CephXTicketBlob, Duration);

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
}

impl CephXClientHandler {
    /// Create a new client handler
    ///
    /// # Arguments
    /// * `entity_name_str` - Entity name (e.g., "client.admin")
    /// * `auth_mode` - Auth mode (AuthMode::Mon for monitors, AuthMode::Authorizer for OSDs)
    pub fn new(entity_name_str: String, auth_mode: AuthMode) -> Result<Self> {
        let entity_name = entity_name_str.parse()?;
        Ok(Self {
            entity_name,
            server_challenge: None,
            starting: true,
            session: None,
            secret_key: None,
            auth_mode,
        })
    }

    /// Set the client's secret key
    pub fn set_secret_key(&mut self, key: CryptoKey) {
        debug!(
            "Setting secret key for {}: {} bytes, first 16: {}",
            self.entity_name,
            key.len(),
            hex::encode(&key.get_secret()[..16.min(key.len())])
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
        // NO CephXRequestHeader, NO CephXAuthenticate
        let mut payload = BytesMut::new();

        // 1. auth_mode (1 byte) - Authorizer for OSDs, Mon for monitors
        payload.put_u8(self.auth_mode.as_u8());

        // 2. entity_name (encoded string)
        self.entity_name.encode(&mut payload, 0)?;
        let entity_name_len = self.entity_name.encoded_size(0).unwrap_or(0);

        // 3. global_id (u64)
        global_id.encode(&mut payload, 0)?;

        // 4. build_initial_request() is empty for CephX (no payload)

        trace!(
            "CephX initial request built, size: {} bytes (auth_mode=1, entity_name={}, global_id=8)",
            payload.len(),
            entity_name_len
        );
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

        debug!(
            "Building CephX authenticate request with server_challenge: {:x}",
            server_challenge
        );

        // Generate client challenge
        let mut rng = rand::thread_rng();
        let client_challenge = rng.next_u64();

        debug!(
            "Generated client_challenge: 0x{:016x}, server_challenge: 0x{:016x}",
            client_challenge, server_challenge
        );

        // Calculate session key from challenges
        let session_key =
            self.calculate_session_key(secret_key, server_challenge, client_challenge)?;

        // Build the request header
        let header = CephXRequestHeader {
            request_type: CEPHX_GET_AUTH_SESSION_KEY,
        };

        // Build the authenticate request
        let auth_request = CephXAuthenticate {
            client_challenge,
            key: session_key,
            old_ticket: CephXTicketBlob::default(), // No old ticket for initial auth
            other_keys: 0x3F,                       // Request all service keys (MON|OSD|MDS|MGR)
        };

        // Encode header + authenticate (NO auth_mode for second request)
        let mut payload = BytesMut::new();
        let header_size = header.encoded_size(0).unwrap_or(0);
        let auth_size = auth_request.encoded_size(0).unwrap_or(0);

        header.encode(&mut payload, 0)?;
        auth_request.encode(&mut payload, 0)?;

        trace!(
            "CephX authenticate request built, size: {} bytes (header={}, auth={})",
            payload.len(),
            header_size,
            auth_size
        );
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
        use crate::protocol::CephXChallengeBlob;
        use aes::Aes128;
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};
        use cbc::Encryptor;

        debug!("=== CephX Session Key Calculation Start ===");
        debug!(
            "Input: server_challenge=0x{:016x}, client_challenge=0x{:016x}",
            server_challenge, client_challenge
        );
        debug!(
            "Challenge bytes: server={:02x?}, client={:02x?}",
            server_challenge.to_le_bytes(),
            client_challenge.to_le_bytes()
        );

        // Create challenge blob
        let challenge_blob = CephXChallengeBlob {
            server_challenge,
            client_challenge,
        };

        // Wrap in encrypted envelope and encode
        use crate::protocol::CephXEncryptedEnvelope;
        let envelope = CephXEncryptedEnvelope {
            payload: challenge_blob,
        };

        let mut bl = BytesMut::new();
        envelope.encode(&mut bl, 0)?;
        let plaintext = bl.freeze();

        debug!(
            "Encoded challenge envelope ({} bytes): {}",
            plaintext.len(),
            hex::encode(&plaintext)
        );

        // Prepare AES key and IV
        // IMPORTANT: CryptoKey has a header before the actual key:
        //   - crypto_type: u16 (2 bytes)
        //   - created.sec: u32 (4 bytes)
        //   - created.nsec: u32 (4 bytes)
        //   - secret_length: u16 (2 bytes)
        // The actual AES key starts after this 12-byte header
        // Ceph uses the first 16 bytes of the ACTUAL KEY DATA (after the header) as the AES key
        const CRYPTO_KEY_HEADER_SIZE: usize = std::mem::size_of::<u16>()  // crypto_type
            + std::mem::size_of::<u32>()  // created.sec
            + std::mem::size_of::<u32>()  // created.nsec
            + std::mem::size_of::<u16>(); // secret_length
        let secret_bytes = secret_key.get_secret();

        if secret_bytes.len() < CRYPTO_KEY_HEADER_SIZE + 16 {
            return Err(CephXError::CryptographicError(format!(
                "Secret key too short: {} bytes, need at least {}",
                secret_bytes.len(),
                CRYPTO_KEY_HEADER_SIZE + 16
            )));
        }

        // Skip the header to get to the actual key material
        let actual_key_start = CRYPTO_KEY_HEADER_SIZE;
        let key_bytes = &secret_bytes[actual_key_start..actual_key_start + 16];

        debug!(
            "Step 5: AES key (first 16 bytes of secret): {}",
            hex::encode(key_bytes)
        );
        debug!(
            "Step 6: AES IV: {}",
            hex::encode(crate::protocol::CEPH_AES_IV)
        );

        // Use Ceph's IV
        use crate::protocol::CEPH_AES_IV;

        // Encrypt with AES-128-CBC using Pkcs7 padding
        type Aes128CbcEnc = Encryptor<Aes128>;
        let cipher = Aes128CbcEnc::new(key_bytes.into(), CEPH_AES_IV.into());

        // Allocate buffer with room for padding
        let mut buffer = vec![0u8; plaintext.len() + 16];
        buffer[..plaintext.len()].copy_from_slice(&plaintext);

        let ciphertext = cipher
            .encrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer, plaintext.len())
            .map_err(|e| {
                CephXError::CryptographicError(format!("AES encryption failed: {:?}", e))
            })?;

        debug!(
            "Step 7: Ciphertext after AES-CBC ({} bytes): {}",
            ciphertext.len(),
            hex::encode(ciphertext)
        );

        // In C++, encode_encrypt() adds a u32 length prefix to the encrypted data
        // before XOR folding. We need to replicate this behavior.
        // Use Denc to create length-prefixed buffer
        let encrypted_bytes = Bytes::copy_from_slice(ciphertext);
        let mut folding_buffer = BytesMut::new();
        encrypted_bytes.encode(&mut folding_buffer, 0)?;
        let folding_data = folding_buffer.freeze();

        debug!(
            "Step 8: Buffer for XOR folding with length prefix ({} bytes): {}",
            folding_data.len(),
            hex::encode(&folding_data)
        );

        // XOR fold the entire buffer (length prefix + encrypted data) to get a 64-bit key
        // C++ only processes complete 8-byte chunks, ignoring any remaining bytes
        debug!(
            "Step 9: XOR folding {} bytes as little-endian u64 chunks:",
            folding_data.len()
        );
        let mut key = 0u64;
        let mut buf = folding_data.clone();
        let num_complete_chunks = buf.len() / 8;

        for idx in 0..num_complete_chunks {
            let chunk_val = u64::decode(&mut buf, 0)?;
            debug!("  Chunk {}: 0x{:016x}", idx, chunk_val);
            key ^= chunk_val;
            debug!("  Running XOR result: 0x{:016x}", key);
        }

        let remaining_bytes = folding_data.len() % 8;
        if remaining_bytes > 0 {
            debug!(
                "  Note: Ignoring last {} bytes (C++ only processes complete 8-byte chunks)",
                remaining_bytes
            );
        }

        debug!("=== Final Result ===");
        debug!("Calculated session key: 0x{:016x}", key);
        debug!("======================");

        Ok(key)
    }

    /// Handle server auth response
    pub fn handle_auth_response(&mut self, mut response: Bytes) -> Result<AuthResult> {
        if self.starting {
            // First response should be server challenge
            debug!("Handling initial server challenge");
            debug!(
                "AUTH_REPLY_MORE payload: {} bytes, hex: {}",
                response.len(),
                response
                    .iter()
                    .take(32)
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>()
                    .join("")
            );

            // AUTH_REPLY_MORE has a u32 length prefix before the actual CephXServerChallenge
            // Format: [u32 length][u8 struct_v][u64 server_challenge]
            if response.len() < 4 {
                return Err(CephXError::ProtocolError(
                    "AUTH_REPLY_MORE too short".into(),
                ));
            }
            let payload_len = u32::decode(&mut response, 0).map_err(|e| {
                CephXError::ProtocolError(format!("Failed to decode payload_len: {}", e))
            })? as usize;
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
        use denc::Denc;

        let _version = u8::decode(buf, 0)?;
        let num = u32::decode(buf, 0)?;
        debug!("Decoding {} extra tickets", num);

        let mut result = Vec::with_capacity(num.min(16) as usize);

        for i in 0..num {
            match self.try_decode_single_ticket(buf, auth_session_key) {
                Ok(ticket_info) => {
                    trace!(
                        "Decoded extra ticket {}/{} for service {}",
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
        use denc::Denc;

        let service_id = u32::decode(buf, 0)?;
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
            service_id,
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
        use bytes::Buf;
        use denc::Denc;

        // Read outer bufferlist length
        let cbl_len = u32::decode(payload, 0)? as usize;
        if cbl_len == 0 {
            return Ok(None);
        }

        if payload.remaining() < cbl_len {
            return Err(CephXError::ProtocolError(format!(
                "Insufficient data for connection_secret: need {}, have {}",
                cbl_len,
                payload.remaining()
            )));
        }

        let mut encrypted_secret_bl = payload.split_to(cbl_len);

        // Read inner encrypted data length
        let inner_len = u32::decode(&mut encrypted_secret_bl, 0)? as usize;
        if inner_len == 0 {
            return Ok(None);
        }

        if encrypted_secret_bl.remaining() < inner_len {
            return Err(CephXError::ProtocolError(format!(
                "Insufficient data for encrypted connection_secret: need {}, have {}",
                inner_len,
                encrypted_secret_bl.remaining()
            )));
        }

        let encrypted_secret = encrypted_secret_bl.split_to(inner_len);

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
        use bytes::Buf;

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
            debug!(
                "Storing {} ticket handlers in session",
                ticket_handlers.len()
            );
            for (service_id, session_key, secret_id, ticket_blob, validity) in ticket_handlers {
                trace!(
                    "Storing ticket for service {} (secret_id={})",
                    service_id,
                    secret_id
                );
                let handler = session.get_ticket_handler(service_id);
                handler.update(session_key, secret_id, ticket_blob, validity);
                debug!(
                    "✓ Stored ticket for service {} (secret_id={}, have_key={}, expired={})",
                    service_id,
                    secret_id,
                    handler.have_key,
                    handler.is_expired()
                );
            }
            // Log all available tickets
            debug!("Available service tickets after storage:");
            for (service_id, handler) in &session.ticket_handlers {
                debug!(
                    "  Service {}: have_key={}, expired={}",
                    service_id,
                    handler.have_key,
                    handler.is_expired()
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
        use bytes::Buf;
        use denc::Denc;

        debug!(
            "Handling AUTH_DONE: global_id={}, con_mode={}, payload={} bytes",
            global_id,
            con_mode,
            auth_payload.len()
        );
        debug!(
            "AUTH_DONE payload hex (first 64 bytes): {}",
            auth_payload
                .iter()
                .take(64)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
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
        let mut first_session_key_bytes: Option<Bytes> = None;
        let mut ticket_handlers: Vec<(u32, CryptoKey, u64, CephXTicketBlob, Duration)> = Vec::new();

        for (i, ticket_info) in ticket_reply.tickets.iter().enumerate() {
            debug!(
                "Processing ticket {}/{}: service_id={}",
                i + 1,
                ticket_reply.tickets.len(),
                ticket_info.service_id
            );

            // Decrypt the encrypted service ticket to get session key and validity
            let (session_key, validity) =
                self.decrypt_service_ticket(&ticket_info.encrypted_service_ticket, secret_key)?;

            debug!(
                "Service {} session key type: {}, length: {}",
                ticket_info.service_id,
                session_key.get_type(),
                session_key.len()
            );
            debug!("Validity: {:?}", validity);

            // Store the first ticket's session key for returning (this is the AUTH service)
            if i == 0 {
                first_session_key_bytes = Some(session_key.get_secret().clone());
            }

            ticket_handlers.push((
                ticket_info.service_id,
                session_key,
                ticket_info.ticket_blob.secret_id,
                ticket_info.ticket_blob.clone(),
                validity,
            ));
        }

        let session_key_bytes = first_session_key_bytes
            .ok_or_else(|| CephXError::ProtocolError("No session key found in tickets".into()))?;

        // Get the first ticket's session key for decrypting connection_secret
        let first_ticket_session_key = ticket_handlers
            .first()
            .map(|(_, sk, _, _, _)| sk)
            .ok_or_else(|| CephXError::ProtocolError("No tickets available".into()))?;

        // Decode connection_secret blob (encrypted with session_key)
        let connection_secret_bytes =
            self.decode_connection_secret(&mut auth_payload, first_ticket_session_key)?;

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

                    // Parse extra_tickets using ServiceTicketReply (same format as main tickets)
                    // Extra tickets are encrypted with the AUTH session key (from first ticket)
                    let auth_session_key = ticket_handlers
                        .first()
                        .map(|(_, session_key, _, _, _)| session_key.clone())
                        .ok_or_else(|| {
                            CephXError::ProtocolError(
                                "No AUTH ticket to decrypt extra tickets".into(),
                            )
                        })?;

                    // Decode extra_tickets using the simpler non-versioned format
                    // Format: u8 version, u32 num, for each: u32 service_id, u8 ticket_v, EncryptedServiceTicket, u8 enc, ticket_blob
                    match self.decode_extra_tickets(&mut extra_tickets_bl, &auth_session_key) {
                        Ok(extra_handlers) => {
                            for (service_id, session_key, secret_id, ticket_blob, validity) in
                                extra_handlers
                            {
                                ticket_handlers.push((
                                    service_id,
                                    session_key,
                                    secret_id,
                                    ticket_blob,
                                    validity,
                                ));
                            }
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
        service_id: u32,
        mut encrypted_payload: Bytes,
    ) -> Result<u64> {
        use crate::protocol::{CephXAuthorizeReply, CephXEncryptedEnvelope};
        use denc::Denc;

        debug!("Decrypting authorize challenge for service {}", service_id);
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
        let handler = session.ticket_handlers.get(&service_id).ok_or_else(|| {
            CephXError::AuthenticationFailed(format!(
                "No ticket handler for service {}",
                service_id
            ))
        })?;

        if !handler.have_key {
            return Err(CephXError::AuthenticationFailed(format!(
                "No session key for service {}",
                service_id
            )));
        }

        // Decode length-prefixed encrypted data using Denc
        let encrypted_data = Bytes::decode(&mut encrypted_payload, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to decode encrypted data: {}", e))
        })?;
        trace!("encrypted_len: {}", encrypted_data.len());

        // Decrypt using session key
        let decrypted = Self::decrypt_with_key(&handler.session_key, &encrypted_data)?;
        let mut dec_buf = Bytes::from(decrypted);

        // Decode the encrypted envelope containing CephXAuthorizeReply
        let envelope = CephXEncryptedEnvelope::<CephXAuthorizeReply>::decode(&mut dec_buf, 0)
            .map_err(|e| {
                CephXError::ProtocolError(format!("Failed to decode authorize reply: {}", e))
            })?;

        let server_challenge = envelope.payload.nonce_plus_one;
        debug!("Extracted server_challenge: 0x{:016x}", server_challenge);

        Ok(server_challenge)
    }

    /// Decrypt data using AES-128-CBC with the given key
    /// Ceph uses a fixed IV: "cephsageyudagreg"
    pub fn decrypt_with_key(key: &CryptoKey, ciphertext: &[u8]) -> Result<Vec<u8>> {
        use crate::protocol::CEPH_AES_IV;
        use aes::cipher::generic_array::GenericArray;
        use aes::Aes128;
        use cbc::cipher::{BlockDecryptMut, KeyIvInit};
        use cbc::Decryptor;

        // Verify key length
        if key.get_secret().len() != 16 {
            return Err(CephXError::CryptographicError(format!(
                "Invalid key length: expected 16, got {}",
                key.get_secret().len()
            )));
        }

        // Create decryptor with fixed IV - convert slices to GenericArray
        type Aes128CbcDec = Decryptor<Aes128>;
        let key_array = GenericArray::from_slice(key.get_secret());
        let iv_array = GenericArray::from_slice(CEPH_AES_IV);
        let cipher = Aes128CbcDec::new(key_array, iv_array);

        // Decrypt (need to copy because decrypt_padded_mut modifies in place)
        let mut buffer = ciphertext.to_vec();
        let decrypted = cipher
            .decrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer)
            .map_err(|e| CephXError::DecryptionFailed(format!("CBC decryption failed: {:?}", e)))?;

        Ok(decrypted.to_vec())
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
        service_id: u32,
        global_id: u64,
        server_challenge: Option<u64>,
    ) -> Result<Bytes> {
        use crate::protocol::{CephXAuthorizeA, CephXAuthorizeB};
        use rand::RngCore;

        debug!(
            "Building authorizer for service_id={} (global_id={})",
            service_id, global_id
        );

        // Get session or return error
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| CephXError::AuthenticationFailed("No session available".into()))?;

        // Get global_id first (before mut borrow)
        let actual_global_id = session.global_id;

        // Debug: log all available ticket handlers
        debug!(
            "build_authorizer: Requesting service_id={}, Session has {} ticket handlers",
            service_id,
            session.ticket_handlers.len()
        );
        for (sid, handler) in &session.ticket_handlers {
            debug!(
                "  Ticket handler for service {}: have_key={}, expired={}, ticket_blob={}",
                sid,
                handler.have_key,
                handler.is_expired(),
                if handler.ticket_blob.is_some() {
                    "present"
                } else {
                    "absent"
                }
            );
        }

        // Get or create ticket handler
        let handler = session.get_ticket_handler(service_id);

        if !handler.have_key {
            return Err(CephXError::AuthenticationFailed(format!(
                "No ticket available for service {}",
                service_id
            )));
        }

        let ticket_blob = handler
            .ticket_blob
            .as_ref()
            .ok_or_else(|| {
                CephXError::AuthenticationFailed(format!(
                    "No ticket blob for service {}",
                    service_id
                ))
            })?
            .clone();

        // Clone the session key and secret_id to avoid borrow checker issues
        let session_key = handler.session_key.clone();
        let secret_id = handler.secret_id;

        debug!(
            "Building authorizer: global_id={}, service_id={}, secret_id={}, session_key_len={}",
            actual_global_id,
            service_id,
            secret_id,
            session_key.get_secret().len()
        );

        // Build CephXAuthorizeA
        let authorize_a = CephXAuthorizeA::new(actual_global_id, service_id, ticket_blob);

        // Generate nonce
        let mut rng = rand::thread_rng();
        let nonce = rng.next_u64();
        debug!("Generated nonce: 0x{:016x}", nonce);

        // Build CephXAuthorizeB - include server challenge if provided
        let authorize_b = if let Some(challenge) = server_challenge {
            debug!(
                "Building authorizer with server_challenge: 0x{:016x}",
                challenge
            );
            CephXAuthorizeB::with_challenge(nonce, challenge)
        } else {
            CephXAuthorizeB::new(nonce)
        };

        // Encode authorize_a
        let mut authorizer_buf = BytesMut::new();
        authorize_a.encode(&mut authorizer_buf, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to encode CephXAuthorizeA: {:?}", e))
        })?;

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
        use aes::Aes128;
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};
        use cbc::Encryptor;
        use denc::Denc;

        debug!("Encrypting CephXAuthorizeB");

        // Wrap authorize_b in encrypted envelope and encode
        let envelope = CephXEncryptedEnvelope {
            payload: authorize_b.clone(),
        };

        let mut envelope_buf = BytesMut::new();
        envelope.encode(&mut envelope_buf, 0)?;

        debug!(
            "Encryption envelope ({} bytes): {}",
            envelope_buf.len(),
            hex::encode(&envelope_buf[..32.min(envelope_buf.len())])
        );

        // Extract AES key from session key
        let secret_bytes = session_key.get_secret();

        // Session keys from tickets are typically raw 16-byte keys
        let key_bytes = if secret_bytes.len() == 16 {
            secret_bytes.as_ref()
        } else if secret_bytes.len() >= 28 {
            // Has header, skip to actual key
            const CRYPTO_KEY_HEADER_SIZE: usize = 12;
            &secret_bytes[CRYPTO_KEY_HEADER_SIZE..CRYPTO_KEY_HEADER_SIZE + 16]
        } else {
            return Err(CephXError::CryptographicError(format!(
                "Invalid session key length: {}",
                secret_bytes.len()
            )));
        };

        debug!(
            "Using AES key (first 16 bytes): {}",
            hex::encode(&key_bytes[..16])
        );

        // Use Ceph's IV
        use crate::protocol::CEPH_AES_IV;

        // Encrypt with AES-128-CBC
        type Aes128CbcEnc = Encryptor<Aes128>;
        let cipher = Aes128CbcEnc::new(key_bytes.into(), CEPH_AES_IV.into());

        let mut buffer = vec![0u8; envelope_buf.len() + 16];
        buffer[..envelope_buf.len()].copy_from_slice(&envelope_buf);

        let ciphertext = cipher
            .encrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(
                &mut buffer,
                envelope_buf.len(),
            )
            .map_err(|e| {
                CephXError::CryptographicError(format!("AES encryption failed: {:?}", e))
            })?;

        // Add length prefix
        let mut result = BytesMut::with_capacity(4 + ciphertext.len());
        (ciphertext.len() as u32)
            .encode(&mut result, 0)
            .map_err(|e| {
                CephXError::EncodingError(format!("Failed to encode ciphertext length: {}", e))
            })?;
        result.extend_from_slice(ciphertext);

        debug!("Encrypted result: {} bytes total", result.len());
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
        needed_keys: u32,
    ) -> Result<Bytes> {
        debug!(
            "Building ticket renewal request for global_id={}, needed_keys=0x{:x}",
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
        header.encode(&mut payload, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to encode request header: {:?}", e))
        })?;

        // 2. Build and encode authorizer from AUTH ticket handler
        // The authorizer proves we have a valid AUTH ticket
        let auth_ticket_handler = session
            .ticket_handlers
            .get(&crate::types::service_id::AUTH)
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
            crate::types::service_id::AUTH,
            global_id,
            None, // No server challenge for ticket renewal
        )?;

        payload.extend_from_slice(&authorizer);

        // 3. Encode service ticket request with needed keys
        let ticket_request = crate::protocol::CephXServiceTicketRequest { keys: needed_keys };
        ticket_request.encode(&mut payload, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to encode ticket request: {:?}", e))
        })?;

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
