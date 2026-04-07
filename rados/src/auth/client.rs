//! CephX client-side authentication handler

use crate::Denc;
use crate::auth::error::{CephXError, Result};
use crate::auth::protocol::{
    AuthMode, CEPHX_GET_AUTH_SESSION_KEY, CephXAuthenticate, CephXRequestHeader,
    CephXServerChallenge,
};
use crate::auth::types::{CephXSession, CephXTicketBlob, CryptoKey, EntityName, EntityType};
use bytes::{Buf, Bytes, BytesMut};
use rand::RngCore;
use std::time::Duration;
use tracing::{debug, trace};

/// Decoded service ticket information returned by `try_decode_single_ticket`.
struct DecodedServiceTicket {
    service_type: EntityType,
    session_key: CryptoKey,
    ticket_blob: CephXTicketBlob,
    validity: Duration,
}

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
            "Setting secret key for {}: {} bytes",
            self.entity_name,
            key.len()
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

        let mut payload = BytesMut::with_capacity(1 + 16 + 8);
        self.auth_mode.as_u8().encode(&mut payload, 0)?;
        self.entity_name.encode(&mut payload, 0)?;
        global_id.encode(&mut payload, 0)?;

        Ok(payload.freeze())
    }

    fn require_secret_key(&self) -> Result<&CryptoKey> {
        self.secret_key
            .as_ref()
            .ok_or_else(|| CephXError::AuthenticationFailed("No secret key set".into()))
    }

    /// Build authenticate request (second phase) after receiving server challenge
    /// Sends CephXRequestHeader + CephXAuthenticate (NO auth_mode)
    /// This matches C++ CephxClientHandler::build_request()
    pub fn build_authenticate_request(&self) -> Result<Bytes> {
        let server_challenge = self
            .server_challenge
            .ok_or_else(|| CephXError::ProtocolError("No server challenge received".into()))?;

        let secret_key = self.require_secret_key()?;

        let mut rng = rand::thread_rng();
        let client_challenge = rng.next_u64();

        debug!(
            "Building CephX authenticate: client_challenge=0x{:016x}, server_challenge=0x{:016x}",
            client_challenge, server_challenge
        );

        let session_key =
            self.calculate_session_key(secret_key, server_challenge, client_challenge)?;

        let header = CephXRequestHeader {
            request_type: CEPHX_GET_AUTH_SESSION_KEY,
        };

        // Mirrors `other_keys = want_keys & ~CEPH_ENTITY_TYPE_AUTH` from the Linux kernel client.
        let other_keys: u32 = (self.want_keys & !EntityType::AUTH).bits();
        let auth_request = CephXAuthenticate::new(
            client_challenge,
            session_key,
            CephXTicketBlob::default(),
            other_keys,
        );

        let mut payload = BytesMut::with_capacity(256);
        header.encode(&mut payload, 0)?;
        auth_request.encode(&mut payload, 0)?;

        Ok(payload.freeze())
    }

    /// Calculate session key from client secret and challenges
    ///
    /// Implements cephx_calc_client_server_challenge() from CephxProtocol.cc
    fn calculate_session_key(
        &self,
        secret_key: &CryptoKey,
        server_challenge: u64,
        client_challenge: u64,
    ) -> Result<u64> {
        use crate::auth::protocol::{CephXChallengeBlob, CephXEncryptedEnvelope};

        debug!(
            "Calculating session key: server_challenge=0x{:016x}, client_challenge=0x{:016x}",
            server_challenge, client_challenge
        );

        let envelope = CephXEncryptedEnvelope {
            payload: CephXChallengeBlob {
                server_challenge,
                client_challenge,
            },
        };

        let mut bl = BytesMut::with_capacity(32);
        envelope.encode(&mut bl, 0)?;

        let ciphertext = secret_key.encrypt(&bl)?;

        // C++ encode_encrypt() adds a u32 length prefix before XOR folding
        let mut folding_buffer = BytesMut::with_capacity(4 + ciphertext.len());
        ciphertext.encode(&mut folding_buffer, 0)?;
        let mut folding_data = folding_buffer.freeze();

        // XOR fold into 64 bits; C++ only processes complete 8-byte chunks
        let num_complete_chunks = folding_data.len() / 8;
        let mut key = 0u64;

        for _ in 0..num_complete_chunks {
            key ^= u64::decode(&mut folding_data, 0)?;
        }

        debug!("Calculated session key: 0x{:016x}", key);
        Ok(key)
    }

    pub fn handle_auth_response(&mut self, mut response: Bytes) -> Result<AuthResult> {
        if self.starting {
            debug!(
                "Handling initial server challenge ({} bytes)",
                response.len()
            );

            if response.len() < 4 {
                return Err(CephXError::ProtocolError(
                    "AUTH_REPLY_MORE too short".into(),
                ));
            }
            let _payload_len = u32::decode(&mut response, 0)?;

            let challenge = CephXServerChallenge::decode(&mut response, 0)?;
            self.server_challenge = Some(challenge.server_challenge);
            self.starting = false;

            debug!(
                "Received server challenge: {:x}",
                challenge.server_challenge
            );
            Ok(AuthResult::NeedMoreData)
        } else {
            Err(CephXError::ProtocolError(
                "Unexpected AUTH_REPLY_MORE after initial challenge. Expected AUTH_DONE.".into(),
            ))
        }
    }

    fn decrypt_service_ticket(
        &self,
        encrypted_ticket: &crate::auth::protocol::EncryptedServiceTicket,
        secret_key: &CryptoKey,
    ) -> Result<(CryptoKey, Duration)> {
        use crate::auth::protocol::CephXEncryptedEnvelope;

        let mut decrypted_data = secret_key.decrypt(&encrypted_ticket.encrypted_data)?;

        let envelope = CephXEncryptedEnvelope::<crate::auth::protocol::CephXServiceTicket>::decode(
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
            Vec::with_capacity(num.min(crate::auth::protocol::MAX_EXTRA_TICKETS as u32) as usize);

        for i in 0..num {
            match self.try_decode_single_ticket(buf, auth_session_key) {
                Ok(ticket_info) => {
                    trace!(
                        "Decoded extra ticket {}/{} for service {:?}",
                        i + 1,
                        num,
                        ticket_info.service_type
                    );
                    result.push(ticket_info);
                }
                Err(e) => {
                    // Extra tickets may contain invalid/placeholder data
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
        let encrypted_ticket = crate::auth::protocol::EncryptedServiceTicket::decode(buf, 0)?;
        let (session_key, validity) =
            self.decrypt_service_ticket(&encrypted_ticket, auth_session_key)?;

        let ticket_enc = u8::decode(buf, 0)?;
        let mut ticket_blob_bytes = if ticket_enc != 0 {
            let encrypted_bl = Bytes::decode(buf, 0)?;
            session_key.decrypt(&encrypted_bl)?
        } else {
            Bytes::decode(buf, 0)?
        };

        let ticket_blob = CephXTicketBlob::decode(&mut ticket_blob_bytes, 0)?;

        Ok(DecodedServiceTicket {
            service_type,
            session_key,
            ticket_blob,
            validity,
        })
    }

    /// Try to decode connection_secret from payload (inner method with ? error propagation)
    fn try_decode_connection_secret(
        payload: &mut Bytes,
        session_key: &CryptoKey,
    ) -> Result<Option<Bytes>> {
        use crate::auth::protocol::CephXEncryptedEnvelope;

        let mut encrypted_secret_bl = Bytes::decode(payload, 0)?;
        if encrypted_secret_bl.is_empty() {
            return Ok(None);
        }

        let encrypted_secret = Bytes::decode(&mut encrypted_secret_bl, 0)?;
        if encrypted_secret.is_empty() {
            return Ok(None);
        }

        let mut decrypted_secret = session_key.decrypt(&encrypted_secret)?;

        let envelope = CephXEncryptedEnvelope::<Bytes>::decode(&mut decrypted_secret, 0)?;

        // CRC mode has empty connection_secret
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

        let session = self.session.get_or_insert_with(|| {
            debug!("Creating new session with global_id={}", global_id);
            CephXSession::new(self.entity_name.clone(), global_id, secret_key.clone())
        });

        for ticket in ticket_handlers {
            let handler = session.get_ticket_handler(ticket.service_type);
            debug!(
                "Stored ticket for service {:?} (secret_id={})",
                ticket.service_type, ticket.ticket_blob.secret_id
            );
            handler.update(ticket.session_key, ticket.ticket_blob, ticket.validity);
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
        use crate::auth::protocol::CephXResponseHeader;

        debug!(
            "Handling AUTH_DONE: global_id={}, con_mode={}, payload={} bytes",
            global_id,
            con_mode,
            auth_payload.len()
        );

        let secret_key = self.require_secret_key()?;

        let header = CephXResponseHeader::decode(&mut auth_payload, 0)?;
        debug!(
            "CephXResponseHeader: request_type=0x{:04x}, status={}",
            header.request_type, header.status
        );

        if header.request_type != crate::auth::protocol::CEPHX_GET_AUTH_SESSION_KEY {
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

        let ticket_reply = crate::auth::protocol::ServiceTicketReply::decode(&mut auth_payload, 0)?;
        debug!(
            "service_ticket_reply: num_tickets={}",
            ticket_reply.tickets.len()
        );

        if ticket_reply.tickets.is_empty() {
            return Err(CephXError::ProtocolError("No tickets in AUTH_DONE".into()));
        }

        let mut ticket_handlers: Vec<DecodedServiceTicket> =
            Vec::with_capacity(ticket_reply.tickets.len());

        for ticket_info in ticket_reply.tickets {
            let (session_key, validity) =
                self.decrypt_service_ticket(&ticket_info.encrypted_service_ticket, secret_key)?;

            debug!(
                "Decoded ticket for service_id={}, validity={:?}",
                ticket_info.service_id, validity
            );

            ticket_handlers.push(DecodedServiceTicket {
                service_type: EntityType::from_bits_retain(ticket_info.service_id),
                session_key,
                ticket_blob: ticket_info.ticket_blob,
                validity,
            });
        }

        let auth_session_key = ticket_handlers
            .first()
            .ok_or_else(|| CephXError::ProtocolError("No tickets available".into()))?
            .session_key
            .clone();
        let session_key_bytes = auth_session_key.secret.clone();

        let connection_secret_bytes =
            self.decode_connection_secret(&mut auth_payload, &auth_session_key)?;

        trace!(
            "After connection_secret, auth_payload remaining: {} bytes",
            auth_payload.remaining()
        );
        if auth_payload.remaining() > 0
            && let Ok(extra_tickets_len) = u32::decode(&mut auth_payload, 0)
        {
            let extra_tickets_len = extra_tickets_len as usize;
            debug!("extra_tickets blob length: {}", extra_tickets_len);

            if extra_tickets_len > 0 && auth_payload.remaining() >= extra_tickets_len {
                let mut extra_tickets_bl = auth_payload.split_to(extra_tickets_len);
                trace!("Parsing extra_tickets: {} bytes", extra_tickets_bl.len());

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
        use crate::auth::protocol::{CephXAuthorizeReply, CephXEncryptedEnvelope};

        debug!(
            "Decrypting authorize challenge for service {:?}",
            service_type
        );
        trace!(
            "decrypt_authorize_challenge: payload length={}",
            encrypted_payload.len()
        );

        let session = self
            .session
            .as_ref()
            .ok_or_else(|| CephXError::AuthenticationFailed("No session available".into()))?;

        let handler = session.ticket_handlers.get(&service_type).ok_or_else(|| {
            CephXError::AuthenticationFailed(format!(
                "No ticket handler for service {:?}",
                service_type
            ))
        })?;

        if handler.ticket_blob.is_none() {
            return Err(CephXError::AuthenticationFailed(format!(
                "No session key for service {:?}",
                service_type
            )));
        }

        let encrypted_data = Bytes::decode(&mut encrypted_payload, 0)?;
        trace!("encrypted_len: {}", encrypted_data.len());

        let mut dec_buf = handler.session_key.decrypt(&encrypted_data)?;

        let envelope = CephXEncryptedEnvelope::<CephXAuthorizeReply>::decode(&mut dec_buf, 0)?;

        let server_challenge = envelope.payload.nonce_plus_one;
        debug!("Extracted server_challenge: 0x{:016x}", server_challenge);

        Ok(server_challenge)
    }

    /// Build an authorizer for a service (OSD, MDS, etc.)
    /// This is used when connecting to services after obtaining tickets from the monitor
    /// Returns the authorizer buffer to be sent to the service
    pub fn build_authorizer(
        &mut self,
        service_type: EntityType,
        global_id: u64,
        server_challenge: Option<u64>,
    ) -> Result<Bytes> {
        use crate::auth::protocol::{CephXAuthorizeA, CephXAuthorizeB};

        debug!(
            "Building authorizer for service_type={:?} (global_id={})",
            service_type, global_id
        );

        let session = self
            .session
            .as_mut()
            .ok_or_else(|| CephXError::AuthenticationFailed("No session available".into()))?;

        let actual_global_id = session.global_id;

        let handler = session.get_ticket_handler(service_type);

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

        debug!(
            "Building authorizer: global_id={}, service_type={:?}, secret_id={}, session_key_len={}",
            actual_global_id,
            service_type,
            ticket_blob.secret_id,
            handler.session_key.secret.len()
        );

        let authorize_a = CephXAuthorizeA::new(actual_global_id, service_type.bits(), ticket_blob);

        let mut rng = rand::thread_rng();
        let nonce = rng.next_u64();
        let authorize_b = if let Some(challenge) = server_challenge {
            CephXAuthorizeB::with_challenge(nonce, challenge)
        } else {
            CephXAuthorizeB::new(nonce)
        };

        let mut authorizer_buf = BytesMut::with_capacity(128);
        authorize_a.encode(&mut authorizer_buf, 0)?;

        trace!(
            "nonce: 0x{:016x}, base_bl length: {}",
            nonce,
            authorizer_buf.len()
        );

        let encrypted_b = Self::encrypt_authorize_b(&handler.session_key, &authorize_b)?;
        trace!("encrypted_b length: {}", encrypted_b.len());
        authorizer_buf.extend_from_slice(&encrypted_b);

        debug!("Built authorizer: {} bytes", authorizer_buf.len());
        Ok(authorizer_buf.freeze())
    }

    /// Encrypt CephXAuthorizeB using the service session key
    /// This replicates the C++ ceph_x_encrypt behavior
    fn encrypt_authorize_b(
        session_key: &CryptoKey,
        authorize_b: &crate::auth::protocol::CephXAuthorizeB,
    ) -> Result<Bytes> {
        use crate::auth::protocol::CephXEncryptedEnvelope;

        let envelope = CephXEncryptedEnvelope {
            payload: authorize_b.clone(),
        };

        let mut envelope_buf = BytesMut::with_capacity(64);
        envelope.encode(&mut envelope_buf, 0)?;

        let ciphertext = session_key.encrypt(&envelope_buf)?;

        let mut result = BytesMut::with_capacity(4 + ciphertext.len());
        (ciphertext.len() as u32).encode(&mut result, 0)?;
        result.extend_from_slice(&ciphertext);

        debug!("Encrypted CephXAuthorizeB: {} bytes total", result.len());
        Ok(result.freeze())
    }

    pub fn get_session(&self) -> Option<&CephXSession> {
        self.session.as_ref()
    }

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
    pub fn build_ticket_renewal_request(
        &mut self,
        global_id: u64,
        needed_keys: EntityType,
    ) -> Result<Bytes> {
        debug!(
            "Building ticket renewal request for global_id={}, needed_keys={:?}",
            global_id, needed_keys
        );

        let mut payload = BytesMut::with_capacity(256);

        let header = crate::auth::protocol::CephXRequestHeader {
            request_type: crate::auth::protocol::CEPHX_GET_PRINCIPAL_SESSION_KEY,
        };
        header.encode(&mut payload, 0)?;

        let authorizer = self.build_authorizer(EntityType::AUTH, global_id, None)?;
        payload.extend_from_slice(&authorizer);

        let ticket_request =
            crate::auth::protocol::CephXServiceTicketRequest::new(needed_keys.bits());
        ticket_request.encode(&mut payload, 0)?;

        debug!(
            "Built ticket renewal request: {} bytes (header + authorizer + ticket_request)",
            payload.len()
        );
        Ok(payload.freeze())
    }

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
    use crate::auth::protocol::AuthMode;
    use crate::auth::types::{CephXSession, CryptoKey};

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

        assert_eq!(request[0], 10); // AuthMode::Mon
    }

    #[test]
    fn test_build_initial_request_authorizer_mode() {
        let handler = CephXClientHandler::new("client.test", AuthMode::Authorizer).unwrap();
        let global_id = 54321u64;

        let request = handler.build_initial_request(global_id).unwrap();
        assert!(!request.is_empty());

        assert_eq!(request[0], 1); // AuthMode::Authorizer
    }

    #[test]
    fn test_build_authenticate_request_without_server_challenge() {
        let handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();

        let result = handler.build_authenticate_request();
        assert!(result.is_err());
    }

    #[test]
    fn test_build_authenticate_request_without_secret_key() {
        let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        handler.server_challenge = Some(0x1234567890abcdef);

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

        handler.server_challenge = Some(12345);
        handler.starting = false;
        let entity_name = handler.entity_name.clone();
        handler.session = Some(CephXSession::new(
            entity_name,
            12345,
            CryptoKey::from_base64("AQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAABAgMEBQYHCA==")
                .unwrap(),
        ));

        handler.reset();
        assert!(handler.server_challenge.is_none());
        assert!(handler.starting);
        assert!(handler.session.is_none());
    }

    #[test]
    fn test_crypto_key_decrypt_invalid_ciphertext() {
        let key =
            CryptoKey::from_base64("AQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAABAgMEBQYHCA==")
                .unwrap();

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
