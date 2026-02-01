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
use tracing::{debug, trace};

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
        payload.put_u64_le(global_id);

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
        use bytes::BufMut;
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

        // Encode the challenge blob (like encode_encrypt_enc_bl in C++)
        let mut bl = BytesMut::new();

        // struct_v = 1
        bl.put_u8(1);
        debug!("Step 1: Added struct_v = 1");

        // AUTH_ENC_MAGIC = 0xff009cad8826aa55ULL
        let magic: u64 = 0xff009cad8826aa55;
        bl.put_u64_le(magic);
        debug!("Step 2: Added AUTH_ENC_MAGIC = 0x{:016x}", magic);

        // Encode the challenge blob itself
        let challenge_size = challenge_blob.encoded_size(0).unwrap_or(0);
        challenge_blob.encode(&mut bl, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to encode challenge: {:?}", e))
        })?;
        debug!("Step 3: Added challenge blob ({} bytes)", challenge_size);

        // Prepare for AES encryption
        let plaintext = bl.freeze();

        debug!(
            "Step 4: Plaintext for AES encryption ({} bytes): {}",
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
        debug!("Step 6: AES IV: {}", hex::encode(b"cephsageyudagreg"));

        // Use Ceph's IV: "cephsageyudagreg" (16 bytes)
        const CEPH_AES_IV: &[u8; 16] = b"cephsageyudagreg";

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
        // Create a buffer with: [u32 length][encrypted data]
        let mut folding_buffer = BytesMut::with_capacity(4 + ciphertext.len());
        folding_buffer.put_u32_le(ciphertext.len() as u32);
        folding_buffer.extend_from_slice(ciphertext);
        let folding_data = folding_buffer.freeze();

        debug!(
            "Step 8: Buffer for XOR folding with length prefix ({} bytes): {}",
            folding_data.len(),
            hex::encode(&folding_data)
        );
        debug!(
            "  - Length prefix (4 bytes): {}",
            hex::encode(&folding_data[..4])
        );
        debug!(
            "  - Encrypted data ({} bytes): {}",
            ciphertext.len(),
            hex::encode(&folding_data[4..])
        );

        // XOR fold the entire buffer (length prefix + encrypted data) to get a 64-bit key
        // IMPORTANT: C++ only processes complete 8-byte chunks, ignoring any remaining bytes
        debug!(
            "Step 9: XOR folding {} bytes as little-endian u64 chunks (complete chunks only):",
            folding_data.len()
        );
        let mut key = 0u64;
        let num_complete_chunks = folding_data.len() / 8; // Only complete 8-byte chunks
        for idx in 0..num_complete_chunks {
            let offset = idx * 8;
            let chunk = &folding_data[offset..offset + 8];
            let mut chunk_val = 0u64;
            for (i, &byte) in chunk.iter().enumerate() {
                chunk_val |= (byte as u64) << (i * 8);
            }
            debug!("  Chunk {}: 8 bytes = 0x{:016x}", idx, chunk_val);
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
            use bytes::Buf;
            if response.len() < 4 {
                return Err(CephXError::ProtocolError(
                    "AUTH_REPLY_MORE too short".into(),
                ));
            }
            let payload_len = response.get_u32_le() as usize;
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
            // Subsequent responses contain auth results
            debug!("Handling auth result response");

            // TODO: Parse the actual auth response
            // For now, assume success if we get here

            // Create a session with placeholder values
            if let Some(secret_key) = &self.secret_key {
                self.session = Some(CephXSession::new(
                    self.entity_name.clone(),
                    0, // Global ID would come from auth response
                    secret_key.clone(),
                ));
            }

            Ok(AuthResult::Success)
        }
    }

    /// Parse a single service ticket from the AUTH_DONE response
    /// Returns (service_id, session_key, secret_id, ticket_blob, validity_duration)
    fn parse_service_ticket(
        &self,
        auth_payload: &mut Bytes,
        secret_key: &CryptoKey,
    ) -> Result<(u32, CryptoKey, u64, CephXTicketBlob, Duration)> {
        use crate::protocol::AUTH_ENC_MAGIC;
        use bytes::Buf;

        // Read service_id
        if auth_payload.len() < 4 {
            return Err(CephXError::ProtocolError(
                "Insufficient data for service_id".into(),
            ));
        }
        let service_id = auth_payload.get_u32_le();
        eprintln!(
            "DEBUG: parse_service_ticket: Parsing ticket for service_id: {} (0x{:08x})",
            service_id, service_id
        );
        debug!(
            "Parsing ticket for service_id: {} (0x{:08x})",
            service_id, service_id
        );

        // Read service_ticket_v
        if auth_payload.is_empty() {
            return Err(CephXError::ProtocolError(
                "Insufficient data for service_ticket_v".into(),
            ));
        }
        let service_ticket_v = auth_payload.get_u8();
        debug!("service_ticket_v: {}", service_ticket_v);

        // Read encrypted CephXServiceTicket (length-prefixed)
        if auth_payload.len() < 4 {
            return Err(CephXError::ProtocolError(
                "Insufficient data for encrypted ticket length".into(),
            ));
        }
        let encrypted_len = auth_payload.get_u32_le() as usize;
        debug!("encrypted ticket length: {}", encrypted_len);

        if auth_payload.len() < encrypted_len {
            return Err(CephXError::ProtocolError(
                "Insufficient data for encrypted ticket".into(),
            ));
        }

        let encrypted_ticket = auth_payload.split_to(encrypted_len);

        // Decrypt the CephXServiceTicket
        let decrypted = secret_key.decrypt(&encrypted_ticket)?;
        let mut decrypted_data = decrypted;

        // Parse decrypted data: [struct_v:u8][magic:u64][CephXServiceTicket_struct_v:u8][session_key:CryptoKey][validity:utime_t]
        if decrypted_data.len() < 9 {
            return Err(CephXError::ProtocolError(
                "Decrypted ticket too short".into(),
            ));
        }

        let _struct_v = decrypted_data.get_u8();
        let magic = decrypted_data.get_u64_le();

        if magic != AUTH_ENC_MAGIC {
            return Err(CephXError::CryptographicError(format!(
                "Bad magic in decrypted ticket: 0x{:016x}",
                magic
            )));
        }

        // CephXServiceTicket struct_v
        if decrypted_data.is_empty() {
            return Err(CephXError::ProtocolError(
                "No data for CephXServiceTicket struct_v".into(),
            ));
        }
        let ticket_struct_v = decrypted_data.get_u8();
        debug!("CephXServiceTicket struct_v: {}", ticket_struct_v);

        // Decode session_key
        let session_key = CryptoKey::decode(&mut decrypted_data, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to decode session_key: {:?}", e))
        })?;
        debug!(
            "Service {} session key type: {}, length: {}",
            service_id,
            session_key.get_type(),
            session_key.len()
        );

        // Decode validity using Duration's Denc implementation
        let validity_duration = if decrypted_data.len() >= 8 {
            use denc::Denc;
            let duration = Duration::decode(&mut decrypted_data, 0).map_err(|e| {
                CephXError::ProtocolError(format!("Failed to decode validity: {}", e))
            })?;
            debug!("Validity: {:?}", duration);
            duration
        } else {
            // Default validity if not specified
            Duration::from_secs(3600) // 1 hour
        };

        // Check if session key is CEPH_CRYPTO_NONE (type 0, length 0)
        // This indicates a dummy/placeholder ticket with no actual ticket blob
        // This can happen for extra tickets that are not fully populated
        if session_key.get_type() == 0 && session_key.is_empty() {
            debug!(
                "Service {} has CEPH_CRYPTO_NONE session key, skipping ticket blob parsing",
                service_id
            );
            // Return a minimal ticket with empty blob
            return Ok((
                service_id,
                session_key,
                0, // secret_id
                CephXTicketBlob::new(0, Bytes::new()),
                validity_duration,
            ));
        }

        // Read ticket_enc byte
        if auth_payload.is_empty() {
            return Err(CephXError::ProtocolError(
                "Insufficient data for ticket_enc".into(),
            ));
        }
        let ticket_enc = auth_payload.get_u8();
        debug!("ticket_enc: {}", ticket_enc);

        // Read the ticket blob (secret_id + blob)
        if auth_payload.len() < 4 {
            return Err(CephXError::ProtocolError(
                "Insufficient data for ticket blob length".into(),
            ));
        }
        let ticket_blob_len = auth_payload.get_u32_le() as usize;
        debug!("ticket_blob_len: {}", ticket_blob_len);

        if auth_payload.len() < ticket_blob_len {
            return Err(CephXError::ProtocolError(
                "Insufficient data for ticket blob".into(),
            ));
        }
        let ticket_blob_data = auth_payload.split_to(ticket_blob_len);

        // Parse ticket blob: [struct_v:u8][secret_id:u64][blob_len:u32][blob:bytes]
        let mut ticket_data = ticket_blob_data;
        if ticket_data.len() < 13 {
            return Err(CephXError::ProtocolError("Ticket blob too short".into()));
        }
        let _blob_struct_v = ticket_data.get_u8();
        let secret_id = ticket_data.get_u64_le();
        let blob_len = ticket_data.get_u32_le() as usize;

        if ticket_data.len() < blob_len {
            return Err(CephXError::ProtocolError(
                "Insufficient data for ticket blob data".into(),
            ));
        }
        let blob = ticket_data.split_to(blob_len);

        let ticket_blob = CephXTicketBlob::new(secret_id, blob);
        debug!(
            "Service {} ticket: secret_id={}, blob_len={}",
            service_id, secret_id, blob_len
        );

        Ok((
            service_id,
            session_key,
            secret_id,
            ticket_blob,
            validity_duration,
        ))
    }

    /// Handle AUTH_DONE payload to extract session_key and connection_secret
    /// Returns (session_key_bytes, connection_secret_bytes) if in SECURE mode
    pub fn handle_auth_done(
        &mut self,
        mut auth_payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)> {
        use crate::protocol::{CephXResponseHeader, AUTH_ENC_MAGIC};
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

        if auth_payload.len() < 2 {
            return Err(CephXError::ProtocolError(
                "AUTH_DONE payload too short for header".into(),
            ));
        }

        // Decode CephXResponseHeader
        let header = CephXResponseHeader::decode(&mut auth_payload, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to decode CephXResponseHeader: {:?}", e))
        })?;
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

        // Now decode the service ticket reply
        // Format: [u8 service_ticket_reply_v][u32 num_tickets]
        // For each ticket: [u32 service_id][u8 service_ticket_v][encrypted CephXServiceTicket][u8 ticket_enc][ticket blob]

        if auth_payload.len() < 5 {
            return Err(CephXError::ProtocolError(
                "AUTH_DONE payload too short after header".into(),
            ));
        }

        let service_ticket_reply_v = auth_payload.get_u8();
        eprintln!("DEBUG: service_ticket_reply_v: {}", service_ticket_reply_v);
        debug!("service_ticket_reply_v: {}", service_ticket_reply_v);

        let num_tickets = auth_payload.get_u32_le();
        eprintln!("DEBUG: num_tickets: {}", num_tickets);
        debug!("num_tickets: {}", num_tickets);

        if num_tickets == 0 {
            return Err(CephXError::ProtocolError("No tickets in AUTH_DONE".into()));
        }

        // Parse all service tickets
        let mut first_session_key_bytes: Option<Bytes> = None;
        let mut ticket_handlers: Vec<(u32, CryptoKey, u64, CephXTicketBlob, Duration)> = Vec::new();

        for i in 0..num_tickets {
            debug!("Processing ticket {}/{}", i + 1, num_tickets);
            let (service_id, session_key, secret_id, ticket_blob, validity) =
                self.parse_service_ticket(&mut auth_payload, secret_key)?;

            // Store the first ticket's session key for returning (this is the AUTH service)
            if i == 0 {
                first_session_key_bytes = Some(session_key.get_secret().clone());
            }

            ticket_handlers.push((service_id, session_key, secret_id, ticket_blob, validity));
        }

        let session_key_bytes = first_session_key_bytes
            .ok_or_else(|| CephXError::ProtocolError("No session key found in tickets".into()))?;

        // Get the first ticket's session key for decrypting connection_secret
        let first_ticket_session_key = ticket_handlers
            .first()
            .map(|(_, sk, _, _, _)| sk)
            .ok_or_else(|| CephXError::ProtocolError("No tickets available".into()))?;

        // Check for connection_secret blob (encrypted with session_key) and extra_tickets
        let mut connection_secret_bytes = None;
        if auth_payload.len() >= 4 {
            let cbl_len = auth_payload.get_u32_le() as usize;
            debug!("connection_secret blob length: {}", cbl_len);

            if cbl_len > 0 && auth_payload.len() >= cbl_len {
                let mut encrypted_secret_bl = auth_payload.split_to(cbl_len);
                debug!(
                    "connection_secret bufferlist: {} bytes",
                    encrypted_secret_bl.len()
                );

                // The bufferlist contains another nested length prefix for the actual encrypted data
                if encrypted_secret_bl.len() < 4 {
                    debug!("connection_secret bufferlist too short for inner length");
                } else {
                    let inner_len = encrypted_secret_bl.get_u32_le() as usize;
                    debug!("Inner encrypted data length: {}", inner_len);

                    if encrypted_secret_bl.len() >= inner_len {
                        let encrypted_secret = encrypted_secret_bl.split_to(inner_len);
                        debug!(
                            "Encrypted connection_secret: {} bytes",
                            encrypted_secret.len()
                        );
                        debug!(
                            "Encrypted connection_secret hex (first 64 bytes): {}",
                            encrypted_secret
                                .iter()
                                .take(64)
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join("")
                        );
                        debug!(
                            "Session key for decryption - type: {}, secret length: {}",
                            first_ticket_session_key.get_type(),
                            first_ticket_session_key.get_secret().len()
                        );

                        // Decrypt connection_secret using the session_key we just extracted
                        match first_ticket_session_key.decrypt(&encrypted_secret) {
                            Ok(mut decrypted_secret) => {
                                // Parse: [struct_v:u8][magic:u64][connection_secret:string]
                                if decrypted_secret.len() >= 9 {
                                    let struct_v = decrypted_secret.get_u8();
                                    let magic = decrypted_secret.get_u64_le();
                                    debug!(
                                        "Connection secret struct_v: {}, magic: 0x{:016x}",
                                        struct_v, magic
                                    );

                                    if magic == AUTH_ENC_MAGIC {
                                        // Read length-prefixed string
                                        if decrypted_secret.len() >= 4 {
                                            let secret_len = decrypted_secret.get_u32_le() as usize;
                                            // Only set connection_secret if length > 0 (CRC mode has 0-length secret)
                                            if secret_len > 0
                                                && decrypted_secret.len() >= secret_len
                                            {
                                                let secret_data =
                                                    decrypted_secret.split_to(secret_len);
                                                debug!(
                                                    "Connection secret: {} bytes",
                                                    secret_data.len()
                                                );
                                                connection_secret_bytes = Some(secret_data);
                                            } else if secret_len == 0 {
                                                debug!(
                                                    "Connection secret length is 0 (CRC mode), leaving as None"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("Failed to decrypt connection_secret: {:?}", e);
                            }
                        }
                    }
                }
            }
        }

        // Parse extra_tickets if any remain in the payload
        eprintln!(
            "DEBUG: After connection_secret, auth_payload.len() = {}",
            auth_payload.len()
        );
        if auth_payload.len() >= 4 {
            let extra_tickets_len = auth_payload.get_u32_le() as usize;
            eprintln!("DEBUG: extra_tickets blob length: {}", extra_tickets_len);

            if extra_tickets_len > 0 && auth_payload.len() >= extra_tickets_len {
                let mut extra_tickets_bl = auth_payload.split_to(extra_tickets_len);
                eprintln!(
                    "DEBUG: Parsing extra_tickets: {} bytes",
                    extra_tickets_bl.len()
                );
                eprintln!(
                    "DEBUG: extra_tickets first 64 bytes: {}",
                    extra_tickets_bl
                        .iter()
                        .take(64)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join("")
                );

                // The extra_tickets bufferlist might contain a CephXServiceTicketReply structure
                // Let's try parsing it as such: struct_v (u8) + num_tickets (u32) + tickets
                if extra_tickets_bl.len() >= 5 {
                    let extra_v = extra_tickets_bl.get_u8();
                    let extra_num_tickets = extra_tickets_bl.get_u32_le();
                    eprintln!(
                        "DEBUG: extra_tickets struct_v: {}, num_tickets: {}",
                        extra_v, extra_num_tickets
                    );

                    // Extra tickets are encrypted with the AUTH session key (from first ticket)
                    // Get the AUTH session key from ticket_handlers (clone to avoid borrow issues)
                    let auth_session_key = ticket_handlers
                        .first()
                        .map(|(_, session_key, _, _, _)| session_key.clone())
                        .ok_or_else(|| {
                            CephXError::ProtocolError(
                                "No AUTH ticket to decrypt extra tickets".into(),
                            )
                        })?;

                    eprintln!("DEBUG: Using AUTH session key to decrypt extra tickets");

                    // Parse each extra ticket using the AUTH session key
                    for i in 0..extra_num_tickets {
                        match self.parse_service_ticket(&mut extra_tickets_bl, &auth_session_key) {
                            Ok((service_id, session_key, secret_id, ticket_blob, validity)) => {
                                ticket_handlers.push((
                                    service_id,
                                    session_key,
                                    secret_id,
                                    ticket_blob,
                                    validity,
                                ));
                            }
                            Err(e) => {
                                // It's normal for the server to indicate more tickets than actually present
                                // Just log and break when we run out of data
                                debug!(
                                    "Finished parsing extra tickets at {}/{}: {:?}",
                                    i + 1,
                                    extra_num_tickets,
                                    e
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }

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
            eprintln!(
                "DEBUG: Storing {} ticket handlers in session",
                ticket_handlers.len()
            );
            debug!(
                "Storing {} ticket handlers in session",
                ticket_handlers.len()
            );
            for (service_id, session_key, secret_id, ticket_blob, validity) in ticket_handlers {
                eprintln!(
                    "DEBUG: Storing ticket for service {} (secret_id={})",
                    service_id, secret_id
                );
                let handler = session.get_ticket_handler(service_id);
                handler.update(session_key, secret_id, ticket_blob, validity);
                debug!(
                    "Stored ticket for service {} (secret_id={})",
                    service_id, secret_id
                );
            }
        } else {
            eprintln!("DEBUG: Warning: No session available to store ticket handlers");
            debug!("Warning: No session available to store ticket handlers");
        }

        Ok((Some(session_key_bytes), connection_secret_bytes))
    }

    /// Decrypt the server challenge from AUTH_REPLY_MORE
    /// Returns the server_challenge value
    pub fn decrypt_authorize_challenge(
        &self,
        service_id: u32,
        encrypted_payload: &[u8],
    ) -> Result<u64> {
        use crate::protocol::AUTH_ENC_MAGIC;
        use bytes::Buf;

        debug!("Decrypting authorize challenge for service {}", service_id);
        eprintln!(
            "DEBUG: decrypt_authorize_challenge: payload length={}",
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

        // Payload format: [len:u32][encrypted_data]
        // encrypted_data format after decryption: [struct_v:u8][magic:u64][CephXAuthorizeReply]
        let mut buf = Bytes::copy_from_slice(encrypted_payload);

        // Read length prefix
        if buf.remaining() < 4 {
            return Err(CephXError::ProtocolError(
                "Challenge payload too short".into(),
            ));
        }
        let encrypted_len = buf.get_u32_le() as usize;
        eprintln!("DEBUG: encrypted_len: {}", encrypted_len);

        if buf.remaining() < encrypted_len {
            return Err(CephXError::ProtocolError(format!(
                "Insufficient data: need {} bytes, have {}",
                encrypted_len,
                buf.remaining()
            )));
        }

        // Extract encrypted data
        let encrypted_data = buf.copy_to_bytes(encrypted_len);

        // Decrypt using session key
        let decrypted = Self::decrypt_with_key(&handler.session_key, &encrypted_data)?;

        let mut dec_buf = Bytes::from(decrypted);

        // Verify struct_v
        if dec_buf.remaining() < 1 {
            return Err(CephXError::ProtocolError(
                "Decrypted data too short for struct_v".into(),
            ));
        }
        let struct_v = dec_buf.get_u8();
        eprintln!("DEBUG: struct_v: {}", struct_v);

        // Verify magic
        if dec_buf.remaining() < 8 {
            return Err(CephXError::ProtocolError(
                "Decrypted data too short for magic".into(),
            ));
        }
        let magic = dec_buf.get_u64_le();
        eprintln!("DEBUG: magic: 0x{:016x}", magic);

        if magic != AUTH_ENC_MAGIC {
            return Err(CephXError::ProtocolError(format!(
                "Invalid magic in decrypted challenge: expected 0x{:016x}, got 0x{:016x}",
                AUTH_ENC_MAGIC, magic
            )));
        }

        // Parse CephXAuthorizeReply: [struct_v:u8][server_challenge:u64]
        if dec_buf.remaining() < 1 {
            return Err(CephXError::ProtocolError(
                "No struct_v for CephXAuthorizeReply".into(),
            ));
        }
        let reply_struct_v = dec_buf.get_u8();
        eprintln!("DEBUG: reply struct_v: {}", reply_struct_v);

        if dec_buf.remaining() < 8 {
            return Err(CephXError::ProtocolError(
                "No server_challenge in reply".into(),
            ));
        }
        let server_challenge = dec_buf.get_u64_le();
        eprintln!(
            "DEBUG: Extracted server_challenge: 0x{:016x}",
            server_challenge
        );
        debug!("Extracted server_challenge: 0x{:016x}", server_challenge);

        Ok(server_challenge)
    }

    /// Decrypt data using AES-128-CBC with the given key
    /// Ceph uses a fixed IV: "cephsageyudagreg"
    pub fn decrypt_with_key(key: &CryptoKey, ciphertext: &[u8]) -> Result<Vec<u8>> {
        use aes::cipher::generic_array::GenericArray;
        use aes::Aes128;
        use cbc::cipher::{BlockDecryptMut, KeyIvInit};
        use cbc::Decryptor;

        // Ceph uses a fixed IV for AES-CBC encryption
        const CEPH_AES_IV: &[u8; 16] = b"cephsageyudagreg";

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
        eprintln!(
            "DEBUG: build_authorizer: Session has {} ticket handlers",
            session.ticket_handlers.len()
        );
        debug!(
            "Session has {} ticket handlers",
            session.ticket_handlers.len()
        );
        for (sid, handler) in &session.ticket_handlers {
            eprintln!(
                "DEBUG:   Ticket handler for service {}: have_key={}, ticket_blob={}",
                sid,
                handler.have_key,
                if handler.ticket_blob.is_some() {
                    "present"
                } else {
                    "absent"
                }
            );
            debug!(
                "  Ticket handler for service {}: have_key={}, ticket_blob={}",
                sid,
                handler.have_key,
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

        // Debug output
        eprintln!("DEBUG: === Rust CephX Authorizer Debug ===");
        eprintln!("DEBUG:   global_id: {}", actual_global_id);
        eprintln!("DEBUG:   service_id: {}", service_id);
        eprintln!("DEBUG:   ticket.secret_id: {}", secret_id);
        eprintln!("DEBUG:   ticket.blob.length(): {}", ticket_blob.blob.len());
        eprintln!("DEBUG:   session_key type: {}", session_key.get_type());
        eprintln!(
            "DEBUG:   session_key length: {}",
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
            eprintln!(
                "DEBUG:   Building authorizer WITH server_challenge: 0x{:016x}",
                challenge
            );
            debug!(
                "Building authorizer with server_challenge: 0x{:016x}",
                challenge
            );
            CephXAuthorizeB::with_challenge(nonce, challenge)
        } else {
            eprintln!("DEBUG:   Building authorizer WITHOUT server_challenge");
            CephXAuthorizeB::new(nonce)
        };

        // Encode authorize_a
        let mut authorizer_buf = BytesMut::new();
        authorize_a.encode(&mut authorizer_buf, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to encode CephXAuthorizeA: {:?}", e))
        })?;

        eprintln!("DEBUG:   nonce: 0x{:016x}", nonce);
        eprintln!(
            "DEBUG:   base_bl length (before encryption): {}",
            authorizer_buf.len()
        );
        eprintln!(
            "DEBUG:   base_bl hex (first 64 bytes): {}",
            authorizer_buf[..std::cmp::min(64, authorizer_buf.len())]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
        );

        // Encode authorize_b
        let mut authorize_b_buf = BytesMut::new();
        authorize_b.encode(&mut authorize_b_buf, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to encode CephXAuthorizeB: {:?}", e))
        })?;

        eprintln!(
            "DEBUG:   authorize_b buffer length: {}",
            authorize_b_buf.len()
        );
        eprintln!(
            "DEBUG:   authorize_b hex: {}",
            authorize_b_buf
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
        );

        // Encrypt authorize_b with session key
        let encrypted_b = Self::encrypt_authorize_b(&session_key, &authorize_b_buf)?;

        eprintln!("DEBUG:   encrypted_b length: {}", encrypted_b.len());
        eprintln!(
            "DEBUG:   encrypted_b hex: {}",
            encrypted_b
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
        );

        // Append encrypted authorize_b to the authorizer buffer
        authorizer_buf.extend_from_slice(&encrypted_b);

        eprintln!("DEBUG:   final authorizer length: {}", authorizer_buf.len());
        debug!("Built authorizer: {} bytes", authorizer_buf.len());
        Ok(authorizer_buf.freeze())
    }

    /// Encrypt CephXAuthorizeB using the service session key
    /// This replicates the C++ ceph_x_encrypt behavior
    fn encrypt_authorize_b(session_key: &CryptoKey, plaintext: &[u8]) -> Result<Bytes> {
        use aes::Aes128;
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};
        use cbc::Encryptor;

        debug!("Encrypting CephXAuthorizeB ({} bytes)", plaintext.len());

        // Prepare encryption envelope: [struct_v:u8][magic:u64][plaintext]
        let mut envelope = BytesMut::new();
        envelope.put_u8(1); // struct_v
        envelope.put_u64_le(crate::protocol::AUTH_ENC_MAGIC);
        envelope.extend_from_slice(plaintext);

        debug!(
            "Encryption envelope ({} bytes): {}",
            envelope.len(),
            hex::encode(&envelope[..32.min(envelope.len())])
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
        const CEPH_AES_IV: &[u8; 16] = b"cephsageyudagreg";

        // Encrypt with AES-128-CBC
        type Aes128CbcEnc = Encryptor<Aes128>;
        let cipher = Aes128CbcEnc::new(key_bytes.into(), CEPH_AES_IV.into());

        let mut buffer = vec![0u8; envelope.len() + 16];
        buffer[..envelope.len()].copy_from_slice(&envelope);

        let ciphertext = cipher
            .encrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer, envelope.len())
            .map_err(|e| {
                CephXError::CryptographicError(format!("AES encryption failed: {:?}", e))
            })?;

        // Add length prefix
        let mut result = BytesMut::with_capacity(4 + ciphertext.len());
        result.put_u32_le(ciphertext.len() as u32);
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
