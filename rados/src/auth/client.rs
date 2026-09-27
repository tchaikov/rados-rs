//! CephX client-side authentication handler

use crate::Denc;
use crate::auth::error::{CephXError, Result};
use crate::auth::protocol::{
    AuthMode, CEPHX_GET_AUTH_SESSION_KEY, CEPHX_KEY_USAGE_AUTH_CONNECTION_SECRET,
    CEPHX_KEY_USAGE_AUTHORIZE, CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE, CEPHX_KEY_USAGE_TICKET_BLOB,
    CEPHX_KEY_USAGE_TICKET_SESSION_KEY, CephXAuthenticate, CephXRequestHeader,
    CephXServerChallenge, ServiceTicketInfo, TicketBlobField,
};
use crate::auth::types::{
    CephXSession, CephXTicketBlob, CryptoKey, EntityName, EntityType, KeyType,
};
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

/// C++ `cephx_calc_client_server_challenge`: the response a client proves
/// its key with, and the value the monitor recomputes to check it.
///
/// An AES key encrypts the challenge blob (C++ `encode_encrypt`: a `u32`
/// length and the ciphertext of the magic envelope). Every other key type
/// takes `HMAC-SHA256` of the bare blob under the raw secret instead, because
/// aes256k's random confounder makes its ciphertext differ on every call. The
/// result is XOR-folded over its complete little-endian `u64` words.
pub(crate) fn cephx_calc_client_server_challenge(
    key: &CryptoKey,
    server_challenge: u64,
    client_challenge: u64,
) -> Result<u64> {
    use crate::auth::protocol::{CephXChallengeBlob, CephXEncryptedEnvelope};

    let blob = CephXChallengeBlob {
        server_challenge,
        client_challenge,
    };
    let mut folding_data = match key.key_type()? {
        KeyType::Aes => {
            let mut bl = BytesMut::with_capacity(32);
            CephXEncryptedEnvelope { payload: blob }.encode(&mut bl, 0)?;
            let ciphertext = key.encrypt(0, &bl)?;
            let mut out = BytesMut::with_capacity(4 + ciphertext.len());
            ciphertext.encode(&mut out, 0)?;
            out.freeze()
        }
        _ => {
            let mut bl = BytesMut::with_capacity(16);
            blob.encode(&mut bl, 0)?;
            Bytes::copy_from_slice(&key.hmac_sha256(&bl)?)
        }
    };

    let mut folded = 0u64;
    for _ in 0..folding_data.len() / 8 {
        folded ^= u64::decode(&mut folding_data, 0)?;
    }
    Ok(folded)
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
        u8::from(self.auth_mode).encode(&mut payload, 0)?;
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
            Self::calculate_session_key(secret_key, server_challenge, client_challenge)?;

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

    /// The folded challenge response a client sends in `CephXAuthenticate::key`.
    fn calculate_session_key(
        secret_key: &CryptoKey,
        server_challenge: u64,
        client_challenge: u64,
    ) -> Result<u64> {
        debug!(
            "Calculating session key: server_challenge=0x{:016x}, client_challenge=0x{:016x}",
            server_challenge, client_challenge
        );
        let key =
            cephx_calc_client_server_challenge(secret_key, server_challenge, client_challenge)?;
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

        let mut decrypted_data = secret_key.decrypt(
            CEPHX_KEY_USAGE_TICKET_SESSION_KEY,
            &encrypted_ticket.encrypted_data,
        )?;

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
        let info = ServiceTicketInfo::decode(buf, 0)?;
        self.decode_service_ticket(info, auth_session_key)
    }

    /// One ticket of a reply, as C++ `verify_service_ticket_reply` reads it:
    /// the session key and validity under `key`, then the ticket blob.
    fn decode_service_ticket(
        &self,
        info: ServiceTicketInfo,
        key: &CryptoKey,
    ) -> Result<DecodedServiceTicket> {
        let service_type = EntityType::from_bits_retain(info.service_id);
        let (session_key, validity) =
            self.decrypt_service_ticket(&info.encrypted_service_ticket, key)?;
        let ticket_blob = match info.ticket_blob {
            TicketBlobField::Clear(blob) => blob,
            TicketBlobField::Encrypted(ciphertext) => {
                self.decrypt_ticket_blob(service_type, &ciphertext)?
            }
        };
        Ok(DecodedServiceTicket {
            service_type,
            session_key,
            ticket_blob,
            validity,
        })
    }

    /// An encrypted ticket blob is sealed under the session key this handler
    /// held for the service before the reply, not the one the reply carries.
    fn decrypt_ticket_blob(
        &self,
        service_type: EntityType,
        ciphertext: &[u8],
    ) -> Result<CephXTicketBlob> {
        use crate::auth::protocol::CephXEncryptedEnvelope;

        let previous_key = self
            .session
            .as_ref()
            .and_then(|session| session.ticket_handlers.get(&service_type))
            .map(|handler| &handler.session_key)
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                CephXError::ProtocolError(format!(
                    "ticket blob encrypted but no previous session key for {service_type:?}"
                ))
            })?;
        let mut plain = previous_key.decrypt(CEPHX_KEY_USAGE_TICKET_BLOB, ciphertext)?;
        let mut blob_bl = CephXEncryptedEnvelope::<Bytes>::decode(&mut plain, 0)?.payload;
        Ok(CephXTicketBlob::decode(&mut blob_bl, 0)?)
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

        let mut decrypted_secret =
            session_key.decrypt(CEPHX_KEY_USAGE_AUTH_CONNECTION_SECRET, &encrypted_secret)?;

        let envelope = CephXEncryptedEnvelope::<Bytes>::decode(&mut decrypted_secret, 0)?;

        // CRC mode has empty connection_secret
        if envelope.payload.is_empty() {
            Ok(None)
        } else {
            Ok(Some(envelope.payload))
        }
    }

    /// Decode the connection secret that follows the tickets in AUTH_DONE.
    ///
    /// In SECURE mode a secret that is missing, short, or fails to decrypt or
    /// decode is an error, because the connection cannot be encrypted without
    /// it. In CRC mode the secret is unused, so a failure yields `None`.
    fn decode_connection_secret(
        &self,
        payload: &mut Bytes,
        session_key: &CryptoKey,
        con_mode: u32,
    ) -> Result<Option<Bytes>> {
        use crate::auth::protocol::{CEPH_CON_MODE_SECURE, require_secure_connection_secret};

        let decoded = if payload.remaining() == 0 {
            Ok(None)
        } else {
            Self::try_decode_connection_secret(payload, session_key)
        };

        if con_mode == CEPH_CON_MODE_SECURE {
            let secret = require_secure_connection_secret(decoded?)?;
            debug!("Connection secret: {} bytes", secret.len());
            return Ok(Some(secret));
        }
        match decoded {
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
            let ticket = self.decode_service_ticket(ticket_info, secret_key)?;
            debug!(
                "Decoded ticket for service {:?}, validity={:?}",
                ticket.service_type, ticket.validity
            );
            ticket_handlers.push(ticket);
        }

        let auth_session_key = ticket_handlers
            .first()
            .ok_or_else(|| CephXError::ProtocolError("No tickets available".into()))?
            .session_key
            .clone();
        let session_key_bytes = auth_session_key.secret.clone();

        let connection_secret_bytes =
            self.decode_connection_secret(&mut auth_payload, &auth_session_key, con_mode)?;

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
                "No ticket handler for service {service_type:?}"
            ))
        })?;

        if handler.ticket_blob.is_none() {
            return Err(CephXError::AuthenticationFailed(format!(
                "No session key for service {service_type:?}"
            )));
        }

        let encrypted_data = Bytes::decode(&mut encrypted_payload, 0)?;
        trace!("encrypted_len: {}", encrypted_data.len());

        let mut dec_buf = handler
            .session_key
            .decrypt(CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE, &encrypted_data)?;

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
                    "No ticket blob for service {service_type:?}"
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

        let ciphertext = session_key.encrypt(CEPHX_KEY_USAGE_AUTHORIZE, &envelope_buf)?;

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

    /// The services whose tickets are due for renewal, for a
    /// `CEPHX_GET_PRINCIPAL_SESSION_KEY` request. AUTH is never included:
    /// Ceph's client does not ask for it there and the monitor skips it.
    pub fn service_keys_due(&self) -> EntityType {
        let Some(session) = self.session.as_ref() else {
            return EntityType::empty();
        };
        session
            .ticket_handlers
            .iter()
            .filter(|(service, handler)| **service != EntityType::AUTH && handler.need_key())
            .fold(EntityType::empty(), |keys, (service, _)| keys | *service)
    }

    /// Mark every service ticket except AUTH as due for renewal, keeping its
    /// blob, as C++ `invalidate_all_tickets` does when the monmap's auth epoch
    /// rises. The AUTH ticket stays valid: the wipe that raises the epoch keeps
    /// the AUTH rotating keys, and it authorizes the renewal request.
    pub fn invalidate_service_tickets(&mut self) {
        let Some(session) = self.session.as_mut() else {
            return;
        };
        for (service, handler) in session.ticket_handlers.iter_mut() {
            if *service != EntityType::AUTH {
                handler.renew_after = Some(std::time::UNIX_EPOCH);
            }
        }
    }

    /// Store the tickets of a monitor's reply to a
    /// `CEPHX_GET_PRINCIPAL_SESSION_KEY` request (an `MAuthReply`'s
    /// `result_bl`). The ticket list is encrypted under the AUTH session key.
    /// Returns the services whose tickets were stored.
    pub fn handle_principal_reply(&mut self, mut reply: Bytes) -> Result<EntityType> {
        use crate::auth::protocol::{CEPHX_GET_PRINCIPAL_SESSION_KEY, CephXResponseHeader};

        let header = CephXResponseHeader::decode(&mut reply, 0)?;
        if header.request_type != CEPHX_GET_PRINCIPAL_SESSION_KEY {
            debug!(
                "Ignoring cephx reply of type 0x{:04x} outside authentication",
                header.request_type
            );
            return Ok(EntityType::empty());
        }
        if header.status != 0 {
            return Err(CephXError::AuthenticationFailed(format!(
                "principal session key request failed with status {}",
                header.status
            )));
        }

        let session = self
            .session
            .as_ref()
            .ok_or_else(|| CephXError::AuthenticationFailed("No session available".into()))?;
        let global_id = session.global_id;
        let auth_session_key = session
            .ticket_handlers
            .get(&EntityType::AUTH)
            .map(|handler| handler.session_key.clone())
            .ok_or_else(|| CephXError::AuthenticationFailed("No AUTH ticket".into()))?;

        let tickets = self.decode_extra_tickets(&mut reply, &auth_session_key)?;
        debug!("Principal reply renewed {} tickets", tickets.len());
        let stored = tickets.iter().fold(EntityType::empty(), |keys, ticket| {
            keys | ticket.service_type
        });
        self.store_ticket_handlers(tickets, global_id)?;
        Ok(stored)
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
    use crate::auth::types::test_keys::{AES_TEST_KEY, AES256K_TEST_KEY};
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
        let key = CryptoKey::from_base64(AES_TEST_KEY).unwrap();

        handler.set_secret_key(key.clone());
        assert!(handler.secret_key.is_some());
        assert_eq!(handler.secret_key.unwrap().len(), key.len());
    }

    #[test]
    fn test_set_secret_key_from_base64() {
        let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
        let base64_key = AES_TEST_KEY;

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
            CryptoKey::from_base64(AES_TEST_KEY).unwrap(),
        ));

        handler.reset();
        assert!(handler.server_challenge.is_none());
        assert!(handler.starting);
        assert!(handler.session.is_none());
    }

    #[test]
    fn test_crypto_key_decrypt_invalid_ciphertext() {
        let key = CryptoKey::from_base64(AES_TEST_KEY).unwrap();

        let result = key.decrypt(0, &[1, 2, 3, 4]);
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
        let key = CryptoKey::from_base64(AES_TEST_KEY).unwrap();

        let result = key.decrypt(0, &[]);
        assert!(result.is_err());
    }

    const SERVER_CHALLENGE: u64 = 0x0123456789abcdef;
    const CLIENT_CHALLENGE: u64 = 0xfedcba9876543210;

    // The value calculate_session_key produced for this key and these
    // challenges before the key type was parsed (6632c80), confirmed in
    // Python with the `cryptography` package.
    #[test]
    fn aes_challenge_fold_is_unchanged() {
        let key = CryptoKey::from_base64(AES_TEST_KEY).unwrap();
        assert_eq!(
            cephx_calc_client_server_challenge(&key, SERVER_CHALLENGE, CLIENT_CHALLENGE).unwrap(),
            0xda40f59232ab17c2
        );
    }

    // HMAC-SHA256(RFC 8009 key, le64(s) | le64(c)) =
    // 87956304acfb25b7e02f0ef45a6b68737b1c2a819da54d8e2f8b36f77ab8770c, folded;
    // computed with Python's hmac module.
    #[test]
    fn aes256k_challenge_is_a_folded_hmac() {
        let key = CryptoKey::from_base64(AES256K_TEST_KEY).unwrap();
        assert_eq!(
            cephx_calc_client_server_challenge(&key, SERVER_CHALLENGE, CLIENT_CHALLENGE).unwrap(),
            0x46778d1186712d33
        );
    }

    #[test]
    fn aes256k_challenge_is_deterministic_where_encryption_is_not() {
        let key = CryptoKey::from_base64(AES256K_TEST_KEY).unwrap();
        let fold = || {
            cephx_calc_client_server_challenge(&key, SERVER_CHALLENGE, CLIENT_CHALLENGE).unwrap()
        };
        assert_eq!(fold(), fold());

        let mut blob = BytesMut::new();
        crate::auth::protocol::CephXChallengeBlob {
            server_challenge: SERVER_CHALLENGE,
            client_challenge: CLIENT_CHALLENGE,
        }
        .encode(&mut blob, 0)
        .unwrap();
        assert_ne!(
            key.encrypt(0, &blob).unwrap(),
            key.encrypt(0, &blob).unwrap()
        );
    }

    mod auth_done {
        use super::*;
        use crate::auth::protocol::{
            CEPHX_KEY_USAGE_AUTHORIZE_REPLY, CephXAuthorizeA, CephXAuthorizeB, CephXAuthorizeReply,
            CephXEncryptedEnvelope, CephXResponseHeader, CephXServiceTicket,
            EncryptedServiceTicket, ServiceTicketInfo,
        };
        use crate::auth::provider::{AuthProvider, ServiceAuthProvider};
        use std::collections::BTreeMap;
        use std::sync::{Arc, Mutex};

        const GLOBAL_ID: u64 = 4242;
        const SECURE: u32 = crate::auth::protocol::CEPH_CON_MODE_SECURE;
        const CRC: u32 = crate::auth::protocol::CEPH_CON_MODE_CRC;
        /// A connection secret as long as a SECURE-mode Ceph server makes.
        const CONNECTION_SECRET: [u8; 64] = [0x5a; 64];

        pub(crate) fn random_key(key_type: KeyType) -> CryptoKey {
            let len = match key_type {
                KeyType::Aes => 16,
                _ => 32,
            };
            let mut secret = vec![0u8; len];
            rand::thread_rng().fill_bytes(&mut secret);
            CryptoKey::new(key_type, Bytes::from(secret)).unwrap()
        }

        fn enveloped<T: Denc>(payload: T) -> BytesMut {
            let mut bl = BytesMut::new();
            CephXEncryptedEnvelope { payload }
                .encode(&mut bl, 0)
                .unwrap();
            bl
        }

        pub(crate) fn encrypted_service_ticket(
            under: &CryptoKey,
            session_key: &CryptoKey,
        ) -> EncryptedServiceTicket {
            let ticket = CephXServiceTicket::new(session_key.clone(), Duration::from_secs(3600));
            EncryptedServiceTicket {
                version: 1,
                encrypted_data: under
                    .encrypt(CEPHX_KEY_USAGE_TICKET_SESSION_KEY, &enveloped(ticket))
                    .unwrap(),
            }
        }

        pub(crate) fn ticket_blob(secret_id: u64) -> CephXTicketBlob {
            CephXTicketBlob::new(secret_id, Bytes::from(vec![secret_id as u8; 8]))
        }

        fn encode_bytes(bytes: &[u8]) -> Bytes {
            let mut bl = BytesMut::new();
            Bytes::copy_from_slice(bytes).encode(&mut bl, 0).unwrap();
            bl.freeze()
        }

        /// One ticket of a reply: `session_key` under `under`, and a blob
        /// with `secret_id` that is sealed under `blob_key` when it is given.
        pub(crate) fn ticket_info(
            service: EntityType,
            under: &CryptoKey,
            session_key: &CryptoKey,
            secret_id: u64,
            blob_key: Option<&CryptoKey>,
        ) -> ServiceTicketInfo {
            let (ticket_enc, ticket_blob) = match blob_key {
                None => (0, TicketBlobField::Clear(ticket_blob(secret_id))),
                Some(key) => {
                    let mut blob = BytesMut::new();
                    ticket_blob(secret_id).encode(&mut blob, 0).unwrap();
                    let sealed = key
                        .encrypt(CEPHX_KEY_USAGE_TICKET_BLOB, &enveloped(blob.freeze()))
                        .unwrap();
                    (1, TicketBlobField::Encrypted(sealed))
                }
            };
            ServiceTicketInfo {
                service_id: service.bits(),
                encrypted_service_ticket: encrypted_service_ticket(under, session_key),
                ticket_enc,
                ticket_blob,
            }
        }

        /// A ticket list as `decode_extra_tickets` and the principal reply
        /// read it.
        pub(crate) fn ticket_list(tickets: &[ServiceTicketInfo]) -> Bytes {
            let mut bl = BytesMut::new();
            1u8.encode(&mut bl, 0).unwrap();
            (tickets.len() as u32).encode(&mut bl, 0).unwrap();
            for ticket in tickets {
                ticket.encode(&mut bl, 0).unwrap();
            }
            bl.freeze()
        }

        /// Extra tickets with clear blobs whose `secret_id` is the service bit.
        pub(crate) fn extra_tickets(
            auth_key: &CryptoKey,
            tickets: &[(EntityType, CryptoKey)],
        ) -> Vec<ServiceTicketInfo> {
            tickets
                .iter()
                .map(|(service, key)| {
                    ticket_info(*service, auth_key, key, u64::from(service.bits()), None)
                })
                .collect()
        }

        struct Built {
            payload: Bytes,
            encrypted_connection_secret: Bytes,
        }

        /// An AUTH_DONE payload in the layout `handle_auth_done` decodes.
        fn build(
            client_key: &CryptoKey,
            auth_key: &CryptoKey,
            extras: &[(EntityType, CryptoKey)],
            connection_secret: &[u8],
        ) -> Built {
            let primary = ticket_info(
                EntityType::AUTH,
                client_key,
                auth_key,
                u64::from(EntityType::AUTH.bits()),
                None,
            );
            build_with(
                primary,
                auth_key,
                &extra_tickets(auth_key, extras),
                connection_secret,
            )
        }

        fn build_with(
            primary: ServiceTicketInfo,
            auth_key: &CryptoKey,
            extras: &[ServiceTicketInfo],
            connection_secret: &[u8],
        ) -> Built {
            let mut bl = BytesMut::new();
            CephXResponseHeader {
                request_type: CEPHX_GET_AUTH_SESSION_KEY,
                status: 0,
            }
            .encode(&mut bl, 0)
            .unwrap();

            bl.extend_from_slice(&ticket_list(&[primary]));

            let encrypted_connection_secret = auth_key
                .encrypt(
                    CEPHX_KEY_USAGE_AUTH_CONNECTION_SECRET,
                    &enveloped(Bytes::copy_from_slice(connection_secret)),
                )
                .unwrap();
            bl.extend_from_slice(&encode_bytes(&encode_bytes(&encrypted_connection_secret)));

            ticket_list(extras).encode(&mut bl, 0).unwrap();

            Built {
                payload: bl.freeze(),
                encrypted_connection_secret,
            }
        }

        /// Authenticates a handler against a hand-built AUTH_DONE and checks
        /// everything it stored; returns the handler and the keys built.
        fn authenticate(
            client_type: KeyType,
            auth_type: KeyType,
            extra_types: &[(EntityType, KeyType)],
        ) -> (CephXClientHandler, BTreeMap<u32, CryptoKey>) {
            let client_key = random_key(client_type);
            let auth_key = random_key(auth_type);
            let extras: Vec<(EntityType, CryptoKey)> = extra_types
                .iter()
                .map(|(service, t)| (*service, random_key(*t)))
                .collect();
            let connection_secret = CONNECTION_SECRET;
            let built = build(&client_key, &auth_key, &extras, &connection_secret);

            let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
            handler.set_secret_key(client_key);
            let (session_key, secret) = handler
                .handle_auth_done(built.payload.clone(), GLOBAL_ID, SECURE)
                .unwrap();
            assert_eq!(session_key.unwrap(), auth_key.secret);
            assert_eq!(secret.unwrap().as_ref(), connection_secret);

            let mut expected: BTreeMap<u32, CryptoKey> = extras
                .iter()
                .map(|(service, key)| (service.bits(), key.clone()))
                .collect();
            expected.insert(EntityType::AUTH.bits(), auth_key.clone());

            let session = handler.get_session().unwrap();
            let stored: Vec<u32> = session
                .ticket_handlers
                .keys()
                .map(|s| s.bits())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            assert_eq!(stored, expected.keys().copied().collect::<Vec<_>>());
            for (service, key) in &expected {
                let th = &session.ticket_handlers[&EntityType::from_bits_retain(*service)];
                assert_eq!(
                    th.session_key.crypto_type, key.crypto_type,
                    "service {service}"
                );
                assert_eq!(th.session_key.secret, key.secret, "service {service}");
            }

            // The blob is sealed under the connection-secret key usage only.
            let mut plain = auth_key
                .decrypt(
                    CEPHX_KEY_USAGE_AUTH_CONNECTION_SECRET,
                    &built.encrypted_connection_secret,
                )
                .unwrap();
            let envelope = CephXEncryptedEnvelope::<Bytes>::decode(&mut plain, 0).unwrap();
            assert_eq!(envelope.payload.as_ref(), connection_secret);
            if auth_type == KeyType::Aes256Krb5 {
                assert!(
                    auth_key
                        .decrypt(
                            CEPHX_KEY_USAGE_TICKET_SESSION_KEY,
                            &built.encrypted_connection_secret
                        )
                        .is_err()
                );
            }

            (handler, expected)
        }

        fn check_authorizer(
            handler: &mut CephXClientHandler,
            service: EntityType,
            key: &CryptoKey,
        ) {
            let mut authorizer = handler.build_authorizer(service, GLOBAL_ID, None).unwrap();
            let a = CephXAuthorizeA::decode(&mut authorizer, 0).unwrap();
            assert_eq!(a.ticket_blob.secret_id, u64::from(service.bits()));
            let encrypted = Bytes::decode(&mut authorizer, 0).unwrap();
            let mut plain = key.decrypt(CEPHX_KEY_USAGE_AUTHORIZE, &encrypted).unwrap();
            CephXEncryptedEnvelope::<CephXAuthorizeB>::decode(&mut plain, 0).unwrap();
            if key.key_type().unwrap() == KeyType::Aes256Krb5 {
                assert!(
                    key.decrypt(CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE, &encrypted)
                        .is_err()
                );
            }
        }

        fn encrypted_reply(key: &CryptoKey, usage: u32, reply: CephXAuthorizeReply) -> Bytes {
            encode_bytes(&key.encrypt(usage, &enveloped(reply)).unwrap())
        }

        fn check_aes256k_challenge(
            handler: &CephXClientHandler,
            service: EntityType,
            key: &CryptoKey,
        ) {
            let challenge = 0x1122334455667788;
            let good = encrypted_reply(
                key,
                CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE,
                CephXAuthorizeReply::new(challenge),
            );
            assert_eq!(
                handler.decrypt_authorize_challenge(service, good).unwrap(),
                challenge
            );
            let wrong_usage = encrypted_reply(
                key,
                CEPHX_KEY_USAGE_AUTHORIZE_REPLY,
                CephXAuthorizeReply::new(challenge),
            );
            assert!(
                handler
                    .decrypt_authorize_challenge(service, wrong_usage)
                    .is_err()
            );
        }

        #[test]
        fn all_aes() {
            let (mut handler, keys) = authenticate(
                KeyType::Aes,
                KeyType::Aes,
                &[
                    (EntityType::OSD, KeyType::Aes),
                    (EntityType::MGR, KeyType::Aes),
                ],
            );
            check_authorizer(
                &mut handler,
                EntityType::OSD,
                &keys[&EntityType::OSD.bits()],
            );
        }

        #[test]
        fn all_aes256k() {
            let (mut handler, keys) = authenticate(
                KeyType::Aes256Krb5,
                KeyType::Aes256Krb5,
                &[
                    (EntityType::OSD, KeyType::Aes256Krb5),
                    (EntityType::MGR, KeyType::Aes256Krb5),
                ],
            );
            let osd = &keys[&EntityType::OSD.bits()];
            check_authorizer(&mut handler, EntityType::OSD, osd);
            check_aes256k_challenge(&handler, EntityType::OSD, osd);
        }

        #[test]
        fn mixed_ticket_types() {
            let (mut handler, keys) = authenticate(
                KeyType::Aes256Krb5,
                KeyType::Aes256Krb5,
                &[
                    (EntityType::OSD, KeyType::Aes),
                    (EntityType::MGR, KeyType::Aes256Krb5),
                ],
            );
            check_authorizer(
                &mut handler,
                EntityType::OSD,
                &keys[&EntityType::OSD.bits()],
            );
            let mgr = &keys[&EntityType::MGR.bits()];
            check_authorizer(&mut handler, EntityType::MGR, mgr);
            check_aes256k_challenge(&handler, EntityType::MGR, mgr);
        }

        fn check_authorize_reply(osd_type: KeyType) {
            let (handler, keys) = authenticate(osd_type, osd_type, &[(EntityType::OSD, osd_type)]);
            let osd = keys[&EntityType::OSD.bits()].clone();
            let mut provider =
                ServiceAuthProvider::from_shared_handler(Arc::new(Mutex::new(handler)));
            provider
                .build_auth_payload(GLOBAL_ID, EntityType::OSD.bits())
                .unwrap();

            let secret = Bytes::from_static(&[0x77; 64]);
            let reply = CephXAuthorizeReply::with_connection_secret(1, secret.clone());
            let payload = encrypted_reply(&osd, CEPHX_KEY_USAGE_AUTHORIZE_REPLY, reply);
            let (session_key, connection_secret) = provider
                .handle_auth_response(payload.clone(), GLOBAL_ID, SECURE)
                .unwrap();
            assert_eq!(session_key.unwrap(), osd.secret);
            assert_eq!(connection_secret.unwrap(), secret);

            // The reply is sealed under the authorize-reply key usage only.
            let encrypted = Bytes::decode(&mut payload.clone(), 0).unwrap();
            osd.decrypt(CEPHX_KEY_USAGE_AUTHORIZE_REPLY, &encrypted)
                .unwrap();
            if osd_type == KeyType::Aes256Krb5 {
                assert!(osd.decrypt(CEPHX_KEY_USAGE_AUTHORIZE, &encrypted).is_err());
            }
        }

        #[test]
        fn authorize_reply_aes() {
            check_authorize_reply(KeyType::Aes);
        }

        #[test]
        fn authorize_reply_aes256k() {
            check_authorize_reply(KeyType::Aes256Krb5);
        }

        /// `payload` with the first byte of `blob` flipped, which garbles the
        /// first cipher block (AES) or fails the HMAC (aes256k).
        fn corrupted(payload: &Bytes, blob: &[u8]) -> Bytes {
            let at = payload
                .windows(blob.len())
                .position(|w| w == blob)
                .expect("blob in payload");
            let mut bl = BytesMut::from(payload.as_ref());
            bl[at] ^= 0xff;
            bl.freeze()
        }

        fn auth_done_with_secret(
            t: KeyType,
            connection_secret: &[u8],
        ) -> (CephXClientHandler, Built) {
            let client_key = random_key(t);
            let auth_key = random_key(t);
            let built = build(&client_key, &auth_key, &[], connection_secret);
            let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
            handler.set_secret_key(client_key);
            (handler, built)
        }

        #[test]
        fn secure_auth_done_with_an_undecryptable_secret_fails() {
            for t in KEY_TYPES {
                let (mut handler, built) = auth_done_with_secret(t, &CONNECTION_SECRET);
                let payload = corrupted(&built.payload, &built.encrypted_connection_secret);
                assert!(
                    handler
                        .handle_auth_done(payload.clone(), GLOBAL_ID, SECURE)
                        .is_err(),
                    "{t:?}"
                );
                let (session_key, secret) =
                    handler.handle_auth_done(payload, GLOBAL_ID, CRC).unwrap();
                assert!(session_key.is_some(), "{t:?}");
                assert!(secret.is_none(), "{t:?}");
            }
        }

        #[test]
        fn secure_auth_done_with_a_short_or_empty_secret_fails() {
            for t in KEY_TYPES {
                for secret in [&[0x5a; 39][..], &[][..]] {
                    let (mut handler, built) = auth_done_with_secret(t, secret);
                    let err = handler
                        .handle_auth_done(built.payload.clone(), GLOBAL_ID, SECURE)
                        .unwrap_err()
                        .to_string();
                    assert!(err.contains("SECURE mode"), "{t:?}: {err}");
                    handler
                        .handle_auth_done(built.payload, GLOBAL_ID, CRC)
                        .unwrap();
                }
                let (mut handler, built) = auth_done_with_secret(t, &[0x5a; 40]);
                let (_, secret) = handler
                    .handle_auth_done(built.payload, GLOBAL_ID, SECURE)
                    .unwrap();
                assert_eq!(secret.unwrap().len(), 40);
            }
        }

        fn service_provider(t: KeyType) -> (ServiceAuthProvider, CryptoKey) {
            let (handler, keys) = authenticate(t, t, &[(EntityType::OSD, t)]);
            let osd = keys[&EntityType::OSD.bits()].clone();
            let mut provider =
                ServiceAuthProvider::from_shared_handler(Arc::new(Mutex::new(handler)));
            provider
                .build_auth_payload(GLOBAL_ID, EntityType::OSD.bits())
                .unwrap();
            (provider, osd)
        }

        #[test]
        fn secure_authorize_reply_with_an_undecryptable_secret_fails() {
            for t in KEY_TYPES {
                let (mut provider, osd) = service_provider(t);
                let reply =
                    CephXAuthorizeReply::with_connection_secret(1, Bytes::from_static(&[0x77; 64]));
                let payload = encrypted_reply(&osd, CEPHX_KEY_USAGE_AUTHORIZE_REPLY, reply);
                let encrypted = Bytes::decode(&mut payload.clone(), 0).unwrap();
                let payload = corrupted(&payload, &encrypted);
                assert!(
                    provider
                        .handle_auth_response(payload.clone(), GLOBAL_ID, SECURE)
                        .is_err(),
                    "{t:?}"
                );
                let (session_key, secret) = provider
                    .handle_auth_response(payload, GLOBAL_ID, CRC)
                    .unwrap();
                assert_eq!(session_key.unwrap(), osd.secret, "{t:?}");
                assert!(secret.is_none(), "{t:?}");
            }
        }

        #[test]
        fn secure_authorize_reply_with_a_short_or_missing_secret_fails() {
            for t in KEY_TYPES {
                let (mut provider, osd) = service_provider(t);
                for reply in [
                    CephXAuthorizeReply::with_connection_secret(1, Bytes::from_static(&[0x77; 39])),
                    CephXAuthorizeReply::new(1),
                ] {
                    let payload = encrypted_reply(&osd, CEPHX_KEY_USAGE_AUTHORIZE_REPLY, reply);
                    assert!(
                        provider
                            .handle_auth_response(payload.clone(), GLOBAL_ID, SECURE)
                            .is_err(),
                        "{t:?}"
                    );
                    provider
                        .handle_auth_response(payload, GLOBAL_ID, CRC)
                        .unwrap();
                }
            }
        }

        const KEY_TYPES: [KeyType; 2] = [KeyType::Aes, KeyType::Aes256Krb5];

        fn encoded(info: &ServiceTicketInfo) -> Bytes {
            let mut bl = BytesMut::new();
            info.encode(&mut bl, 0).unwrap();
            bl.freeze()
        }

        fn stored(handler: &CephXClientHandler, service: EntityType) -> (CryptoKey, u64) {
            let th = &handler.get_session().unwrap().ticket_handlers[&service];
            (
                th.session_key.clone(),
                th.ticket_blob.as_ref().unwrap().secret_id,
            )
        }

        #[test]
        fn extra_ticket_blob_opens_with_the_previous_key() {
            for t in KEY_TYPES {
                let (mut handler, keys) = authenticate(t, t, &[(EntityType::OSD, t)]);
                let auth = keys[&EntityType::AUTH.bits()].clone();
                let old_osd = keys[&EntityType::OSD.bits()].clone();
                let new_osd = random_key(t);

                let sealed = ticket_info(EntityType::OSD, &auth, &new_osd, 99, Some(&old_osd));
                let ticket = handler
                    .try_decode_single_ticket(&mut encoded(&sealed), &auth)
                    .unwrap();
                assert_eq!(ticket.ticket_blob.secret_id, 99);
                assert_eq!(ticket.session_key.secret, new_osd.secret);

                let wrong = ticket_info(EntityType::OSD, &auth, &new_osd, 99, Some(&new_osd));
                assert!(
                    handler
                        .try_decode_single_ticket(&mut encoded(&wrong), &auth)
                        .is_err(),
                    "{t:?}"
                );

                // The same ticket through a whole AUTH_DONE, which stores it.
                let client_key = handler.secret_key.clone().unwrap();
                let new_auth = random_key(t);
                let primary = ticket_info(
                    EntityType::AUTH,
                    &client_key,
                    &new_auth,
                    u64::from(EntityType::AUTH.bits()),
                    None,
                );
                let extra = ticket_info(EntityType::OSD, &new_auth, &new_osd, 99, Some(&old_osd));
                let built = build_with(primary, &new_auth, &[extra], &CONNECTION_SECRET);
                handler
                    .handle_auth_done(built.payload, GLOBAL_ID, SECURE)
                    .unwrap();
                let (key, secret_id) = stored(&handler, EntityType::OSD);
                assert_eq!(key.secret, new_osd.secret);
                assert_eq!(secret_id, 99);
            }
        }

        #[test]
        fn extra_ticket_blob_without_a_previous_key_fails() {
            for t in KEY_TYPES {
                let handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
                let auth = random_key(t);
                let osd = random_key(t);
                let sealed = ticket_info(EntityType::OSD, &auth, &osd, 99, Some(&osd));
                let err = handler
                    .try_decode_single_ticket(&mut encoded(&sealed), &auth)
                    .err()
                    .unwrap()
                    .to_string();
                assert!(err.contains("no previous session key"), "{err}");
            }
        }

        #[test]
        fn primary_ticket_blob_opens_with_the_previous_auth_key() {
            for t in KEY_TYPES {
                let (mut handler, keys) = authenticate(t, t, &[]);
                let client_key = handler.secret_key.clone().unwrap();
                let old_auth = keys[&EntityType::AUTH.bits()].clone();
                let new_auth = random_key(t);

                let wrong = ticket_info(
                    EntityType::AUTH,
                    &client_key,
                    &new_auth,
                    77,
                    Some(&new_auth),
                );
                let built = build_with(wrong, &new_auth, &[], &CONNECTION_SECRET);
                assert!(
                    handler
                        .handle_auth_done(built.payload, GLOBAL_ID, SECURE)
                        .is_err()
                );

                let sealed = ticket_info(
                    EntityType::AUTH,
                    &client_key,
                    &new_auth,
                    77,
                    Some(&old_auth),
                );
                let built = build_with(sealed, &new_auth, &[], &CONNECTION_SECRET);
                let (session_key, _) = handler
                    .handle_auth_done(built.payload, GLOBAL_ID, SECURE)
                    .unwrap();
                assert_eq!(session_key.unwrap(), new_auth.secret);
                let (key, secret_id) = stored(&handler, EntityType::AUTH);
                assert_eq!(key.secret, new_auth.secret);
                assert_eq!(secret_id, 77);
            }
        }

        #[test]
        fn primary_ticket_blob_without_a_previous_key_fails() {
            for t in KEY_TYPES {
                let client_key = random_key(t);
                let auth = random_key(t);
                let mut handler = CephXClientHandler::new("client.admin", AuthMode::Mon).unwrap();
                handler.set_secret_key(client_key.clone());
                let sealed = ticket_info(EntityType::AUTH, &client_key, &auth, 77, Some(&auth));
                let built = build_with(sealed, &auth, &[], &CONNECTION_SECRET);
                let err = handler
                    .handle_auth_done(built.payload, GLOBAL_ID, SECURE)
                    .unwrap_err()
                    .to_string();
                assert!(err.contains("no previous session key"), "{err}");
            }
        }

        fn principal_reply(status: i32, request_type: u16, tickets: &[ServiceTicketInfo]) -> Bytes {
            let mut bl = BytesMut::new();
            CephXResponseHeader {
                request_type,
                status,
            }
            .encode(&mut bl, 0)
            .unwrap();
            bl.extend_from_slice(&ticket_list(tickets));
            bl.freeze()
        }

        fn stored_services(handler: &CephXClientHandler) -> Vec<u32> {
            let mut services: Vec<u32> = handler
                .get_session()
                .unwrap()
                .ticket_handlers
                .keys()
                .map(|s| s.bits())
                .collect();
            services.sort();
            services
        }

        #[test]
        fn principal_reply_replaces_service_tickets() {
            use crate::auth::protocol::CEPHX_GET_PRINCIPAL_SESSION_KEY;
            for t in KEY_TYPES {
                let (mut handler, keys) =
                    authenticate(t, t, &[(EntityType::OSD, t), (EntityType::MGR, t)]);
                let services_before = stored_services(&handler);
                let auth = keys[&EntityType::AUTH.bits()].clone();
                let old_osd = keys[&EntityType::OSD.bits()].clone();
                let new_osd = random_key(t);
                let new_mgr = random_key(t);

                let reply = principal_reply(
                    0,
                    CEPHX_GET_PRINCIPAL_SESSION_KEY,
                    &[
                        ticket_info(EntityType::OSD, &auth, &new_osd, 55, None),
                        // A sealed blob opens with the previous MGR key.
                        ticket_info(
                            EntityType::MGR,
                            &auth,
                            &new_mgr,
                            56,
                            Some(&keys[&EntityType::MGR.bits()]),
                        ),
                    ],
                );
                assert_eq!(
                    handler.handle_principal_reply(reply).unwrap(),
                    EntityType::OSD | EntityType::MGR
                );

                assert_eq!(stored_services(&handler), services_before);
                let (key, secret_id) = stored(&handler, EntityType::OSD);
                assert_eq!(key.crypto_type, new_osd.crypto_type);
                assert_eq!(key.secret, new_osd.secret);
                assert_ne!(key.secret, old_osd.secret);
                assert_eq!(secret_id, 55);
                let (key, secret_id) = stored(&handler, EntityType::MGR);
                assert_eq!(key.secret, new_mgr.secret);
                assert_eq!(secret_id, 56);
                let (key, _) = stored(&handler, EntityType::AUTH);
                assert_eq!(key.secret, auth.secret);
            }
        }

        #[test]
        fn principal_reply_of_another_type_is_ignored() {
            let (mut handler, keys) = authenticate(
                KeyType::Aes,
                KeyType::Aes,
                &[(EntityType::OSD, KeyType::Aes)],
            );
            let auth = keys[&EntityType::AUTH.bits()].clone();
            let reply = principal_reply(
                0,
                CEPHX_GET_AUTH_SESSION_KEY,
                &[ticket_info(
                    EntityType::OSD,
                    &auth,
                    &random_key(KeyType::Aes),
                    55,
                    None,
                )],
            );
            assert!(handler.handle_principal_reply(reply).unwrap().is_empty());
            let (key, _) = stored(&handler, EntityType::OSD);
            assert_eq!(key.secret, keys[&EntityType::OSD.bits()].secret);
        }

        #[test]
        fn principal_reply_with_an_error_status_fails() {
            use crate::auth::protocol::CEPHX_GET_PRINCIPAL_SESSION_KEY;
            let (mut handler, _) = authenticate(KeyType::Aes, KeyType::Aes, &[]);
            let reply = principal_reply(-13, CEPHX_GET_PRINCIPAL_SESSION_KEY, &[]);
            assert!(handler.handle_principal_reply(reply).is_err());
        }

        #[test]
        fn invalidation_leaves_auth_valid_and_keeps_blobs() {
            let (mut handler, _) = authenticate(
                KeyType::Aes256Krb5,
                KeyType::Aes256Krb5,
                &[
                    (EntityType::OSD, KeyType::Aes256Krb5),
                    (EntityType::MGR, KeyType::Aes),
                ],
            );
            assert!(handler.service_keys_due().is_empty());

            handler.invalidate_service_tickets();

            let session = handler.get_session().unwrap();
            for (service, th) in &session.ticket_handlers {
                assert_eq!(th.need_key(), *service != EntityType::AUTH, "{service:?}");
                assert!(th.ticket_blob.is_some(), "{service:?}");
            }
            assert_eq!(
                handler.service_keys_due(),
                EntityType::OSD | EntityType::MGR
            );
        }

        #[test]
        fn renewal_request_never_asks_for_auth() {
            let (mut handler, _) = authenticate(
                KeyType::Aes,
                KeyType::Aes,
                &[
                    (EntityType::OSD, KeyType::Aes),
                    (EntityType::MGR, KeyType::Aes),
                ],
            );
            for th in handler
                .get_session_mut()
                .unwrap()
                .ticket_handlers
                .values_mut()
            {
                th.renew_after = Some(std::time::UNIX_EPOCH);
            }
            let needed = handler.service_keys_due();
            assert_eq!(needed, EntityType::OSD | EntityType::MGR);

            // The request ends with CephXServiceTicketRequest: struct_v, keys.
            let request = handler
                .build_ticket_renewal_request(GLOBAL_ID, needed)
                .unwrap();
            let keys = u32::from_le_bytes(request[request.len() - 4..].try_into().unwrap());
            assert_eq!(keys, (EntityType::OSD | EntityType::MGR).bits());
            assert_eq!(keys & EntityType::AUTH.bits(), 0);
        }
    }
}
