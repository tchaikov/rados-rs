//! CephX client-side authentication handler

use crate::error::{CephXError, Result};
use crate::protocol::{
    CephXAuthenticate, CephXRequestHeader, CephXServerChallenge, AUTH_MODE_MON,
    CEPHX_GET_AUTH_SESSION_KEY,
};
use crate::types::{CephXSession, CephXTicketBlob, CryptoKey, EntityName};
use bytes::{BufMut, Bytes, BytesMut};
use denc::Denc;
use rand::RngCore;
use std::str::FromStr;
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
#[derive(Debug)]
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
}

impl CephXClientHandler {
    /// Create a new client handler
    pub fn new(entity_name_str: String) -> Result<Self> {
        let entity_name = EntityName::from_str(&entity_name_str)?;
        Ok(Self {
            entity_name,
            server_challenge: None,
            starting: true,
            session: None,
            secret_key: None,
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

        // 1. auth_mode (1 byte)
        payload.put_u8(AUTH_MODE_MON);

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
        debug!("service_ticket_reply_v: {}", service_ticket_reply_v);

        let num_tickets = auth_payload.get_u32_le();
        debug!("num_tickets: {}", num_tickets);

        if num_tickets == 0 {
            return Err(CephXError::ProtocolError("No tickets in AUTH_DONE".into()));
        }

        // Process the first ticket (AUTH service ticket)
        let service_id = auth_payload.get_u32_le();
        debug!("service_id: {} (0x{:08x})", service_id, service_id);

        let service_ticket_v = auth_payload.get_u8();
        debug!("service_ticket_v: {}", service_ticket_v);

        // Read encrypted CephXServiceTicket (length-prefixed)
        let encrypted_len = auth_payload.get_u32_le() as usize;
        debug!("encrypted ticket length: {}", encrypted_len);

        if auth_payload.len() < encrypted_len {
            return Err(CephXError::ProtocolError(
                "Insufficient data for encrypted ticket".into(),
            ));
        }

        let encrypted_ticket = auth_payload.split_to(encrypted_len);
        debug!(
            "Encrypted ticket hex (first 32 bytes): {}",
            encrypted_ticket
                .iter()
                .take(32)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
        );

        // Decrypt the CephXServiceTicket using the client's secret key
        let decrypted = secret_key.decrypt(&encrypted_ticket)?;
        debug!("Decrypted ticket: {} bytes", decrypted.len());

        // Parse decrypted data: [struct_v:u8][magic:u64][session_key:CryptoKey][validity:utime_t]
        let mut decrypted_data = decrypted.clone();

        if decrypted_data.len() < 9 {
            return Err(CephXError::ProtocolError(
                "Decrypted ticket too short".into(),
            ));
        }

        let struct_v = decrypted_data.get_u8();
        debug!("Decrypted struct_v: {}", struct_v);

        let magic = decrypted_data.get_u64_le();
        debug!(
            "Decrypted magic: 0x{:016x} (expected: 0x{:016x})",
            magic, AUTH_ENC_MAGIC
        );

        if magic != AUTH_ENC_MAGIC {
            return Err(CephXError::CryptographicError(format!(
                "Bad magic in decrypted ticket: 0x{:016x}, expected 0x{:016x}",
                magic, AUTH_ENC_MAGIC
            )));
        }

        // The CephXServiceTicket structure itself also starts with struct_v
        if decrypted_data.is_empty() {
            return Err(CephXError::ProtocolError(
                "No data for CephXServiceTicket struct_v".into(),
            ));
        }
        let ticket_struct_v = decrypted_data.get_u8();
        debug!("CephXServiceTicket struct_v: {}", ticket_struct_v);

        debug!(
            "Remaining bytes for session_key decode: {}",
            decrypted_data.len()
        );
        debug!(
            "Remaining data hex: {}",
            decrypted_data
                .iter()
                .take(64)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
        );

        // Decode session_key (CryptoKey structure)
        let session_key = CryptoKey::decode(&mut decrypted_data, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to decode session_key: {:?}", e))
        })?;
        debug!(
            "Session key type: {}, length: {}",
            session_key.get_type(),
            session_key.len()
        );

        // Store the session_key bytes for returning
        let session_key_bytes = session_key.get_secret().clone();

        // Skip validity (utime_t: u32 sec + u32 nsec)
        if decrypted_data.len() >= 8 {
            let validity_sec = decrypted_data.get_u32_le();
            let validity_nsec = decrypted_data.get_u32_le();
            debug!("Validity: {}s {}ns", validity_sec, validity_nsec);
        }

        // Now check if there's connection_secret data after the first ticket
        // Read ticket_enc byte
        if auth_payload.is_empty() {
            debug!("No more data in AUTH_DONE payload");
            return Ok((Some(session_key_bytes), None));
        }

        let ticket_enc = auth_payload.get_u8();
        debug!("ticket_enc: {}", ticket_enc);

        // Skip the ticket blob (length-prefixed)
        if auth_payload.len() >= 4 {
            let ticket_blob_len = auth_payload.get_u32_le() as usize;
            debug!("ticket_blob_len: {}", ticket_blob_len);
            if auth_payload.len() >= ticket_blob_len {
                auth_payload.advance(ticket_blob_len);
            }
        }

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
                            session_key.get_type(),
                            session_key.get_secret().len()
                        );

                        // Decrypt connection_secret using the session_key we just extracted
                        match session_key.decrypt(&encrypted_secret) {
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

        Ok((Some(session_key_bytes), connection_secret_bytes))
    }

    /// Get the current session if authenticated
    pub fn get_session(&self) -> Option<&CephXSession> {
        self.session.as_ref()
    }

    /// Reset the handler for a new authentication attempt
    pub fn reset(&mut self) {
        debug!("Resetting CephX client handler");
        self.server_challenge = None;
        self.starting = true;
        self.session = None;
    }
}
