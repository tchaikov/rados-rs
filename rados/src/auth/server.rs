//! CephX server-side authentication handler

use crate::Denc;
use crate::auth::error::{CephXError, Result};
use crate::auth::keyring::Keyring;
use crate::auth::protocol::{
    AES_KEY_LEN, AuthMode, CEPHX_GET_AUTH_SESSION_KEY, CephXAuthenticate, CephXRequestHeader,
    CephXServerChallenge,
};
use crate::auth::types::{
    AuthCapsInfo, AuthTicket, CephXServiceTicketInfo, CephXTicketBlob, CryptoKey, EntityName,
};
use bytes::{Bytes, BytesMut};
use rand::RngCore;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Default initial global_id for new clients
const DEFAULT_INITIAL_GLOBAL_ID: u64 = 1000;
/// Default service ticket TTL in seconds (1 hour)
const DEFAULT_SERVICE_TICKET_TTL_SECS: u64 = 3600;

/// Server-side authentication handler for CephX protocol
///
/// This handler verifies client credentials, generates session keys,
/// and creates service tickets for authenticated clients.
#[derive(Debug)]
pub struct CephXServerHandler {
    /// Keyring containing client secrets
    keyring: Keyring,
    /// Server challenge sent to client
    server_challenge: Option<u64>,
    /// Next global_id to assign
    next_global_id: u64,
    /// Service secrets for generating tickets
    service_secrets: HashMap<u32, CryptoKey>,
    /// Ticket time-to-live
    ticket_ttl: Duration,
}

impl CephXServerHandler {
    pub fn new(keyring: Keyring) -> Self {
        Self {
            keyring,
            server_challenge: None,
            next_global_id: DEFAULT_INITIAL_GLOBAL_ID,
            service_secrets: HashMap::new(),
            ticket_ttl: Duration::from_secs(DEFAULT_SERVICE_TICKET_TTL_SECS),
        }
    }

    pub fn set_ticket_ttl(&mut self, ttl: Duration) {
        self.ticket_ttl = ttl;
    }

    /// Add a service secret for generating tickets
    ///
    /// # Arguments
    /// * `service_id` - Service bit from `EntityType` (MON=1, MDS=2, OSD=4, MGR=16)
    /// * `secret` - Service secret key
    pub fn add_service_secret(&mut self, service_id: u32, secret: CryptoKey) {
        self.service_secrets.insert(service_id, secret);
    }

    fn random_aes_key() -> CryptoKey {
        let mut bytes = vec![0u8; AES_KEY_LEN];
        rand::thread_rng().fill_bytes(&mut bytes);
        CryptoKey::new(Bytes::from(bytes))
    }

    fn allocate_global_id(&mut self) -> u64 {
        let id = self.next_global_id;
        self.next_global_id += 1;
        id
    }

    /// Handle initial authentication request (first phase)
    ///
    /// Client sends: auth_mode + entity_name + global_id
    /// Server responds with: server_challenge
    pub fn handle_initial_request(&mut self, payload: &[u8]) -> Result<(EntityName, u64, Bytes)> {
        let mut buf = Bytes::copy_from_slice(payload);

        let auth_mode_byte = u8::decode(&mut buf, 0)?;
        let auth_mode = AuthMode::from_u8(auth_mode_byte).ok_or_else(|| {
            CephXError::ProtocolError(format!("Invalid auth mode: {}", auth_mode_byte))
        })?;

        debug!("Server: Received auth request with mode: {:?}", auth_mode);

        let entity_name = EntityName::decode(&mut buf, 0)?;
        debug!("Server: Client entity name: {}", entity_name);

        let client_global_id = u64::decode(&mut buf, 0)?;
        debug!("Server: Client requested global_id: {}", client_global_id);

        // Verify client exists in keyring
        if !self.keyring.has_entity(&entity_name.to_string()) {
            warn!("Server: Client {} not found in keyring", entity_name);
            return Err(CephXError::AuthenticationFailed(format!(
                "Client {} not found",
                entity_name
            )));
        }

        // Assign global_id (use client's if non-zero, otherwise allocate new)
        let global_id = if client_global_id == 0 {
            self.allocate_global_id()
        } else {
            client_global_id
        };

        debug!("Server: Assigned global_id: {}", global_id);

        // Generate server challenge
        let mut rng = rand::thread_rng();
        let server_challenge = rng.next_u64();
        self.server_challenge = Some(server_challenge);

        debug!("Server: Generated challenge: {}", server_challenge);

        let challenge = CephXServerChallenge::new(server_challenge);

        let mut response = BytesMut::new();
        challenge.encode(&mut response, 0)?;

        Ok((entity_name, global_id, response.freeze()))
    }

    /// Handle authentication with challenge response (second phase)
    ///
    /// Client sends: CephXRequestHeader + CephXAuthenticate
    /// Server responds with: session_key + service_tickets
    pub fn handle_authenticate(
        &mut self,
        entity_name: &EntityName,
        global_id: u64,
        payload: &[u8],
    ) -> Result<(CryptoKey, CryptoKey, Bytes)> {
        let mut buf = Bytes::copy_from_slice(payload);

        let header = CephXRequestHeader::decode(&mut buf, 0)?;

        debug!(
            "Server: Received auth request type: 0x{:04x}",
            header.request_type
        );

        if header.request_type != CEPHX_GET_AUTH_SESSION_KEY {
            return Err(CephXError::ProtocolError(format!(
                "Unexpected request type: 0x{:04x}",
                header.request_type
            )));
        }

        let authenticate = CephXAuthenticate::decode(&mut buf, 0)?;

        debug!(
            "Server: Client challenge response: {}",
            authenticate.client_challenge
        );
        debug!(
            "Server: Requested extra service tickets: 0x{:08x}",
            authenticate.other_keys
        );

        let client_secret = self
            .keyring
            .get_key(&entity_name.to_string())
            .ok_or_else(|| {
                CephXError::AuthenticationFailed(format!("No secret for {}", entity_name))
            })?;

        let expected_response = self.server_challenge.ok_or_else(|| {
            CephXError::ProtocolError("Server challenge not set before authenticate".to_string())
        })? + 1;

        let key_bytes = authenticate.key.to_le_bytes();
        let decrypted = client_secret.decrypt(&key_bytes)?;

        if decrypted.len() < std::mem::size_of::<u64>() {
            return Err(CephXError::AuthenticationFailed(
                "Invalid challenge response".to_string(),
            ));
        }

        let mut decrypted_buf = decrypted.as_ref();
        let client_response = u64::decode(&mut decrypted_buf, 0)?;

        if client_response != expected_response {
            warn!(
                "Server: Challenge verification failed - expected {}, got {}",
                expected_response, client_response
            );
            return Err(CephXError::AuthenticationFailed(
                "Challenge verification failed".to_string(),
            ));
        }

        info!("Server: Client {} authenticated successfully", entity_name);

        let session_key = Self::random_aes_key();

        debug!("Server: Generated session key: {} bytes", session_key.len());

        let service_tickets =
            self.generate_service_tickets(entity_name, global_id, authenticate.other_keys)?;

        let mut response = BytesMut::new();
        let encrypted_session_key = client_secret.encrypt(&session_key.secret)?;
        encrypted_session_key.encode(&mut response, 0)?;
        service_tickets.encode(&mut response, 0)?;

        let connection_secret = Self::random_aes_key();

        debug!(
            "Server: Generated connection_secret: {} bytes",
            connection_secret.len()
        );

        Ok((session_key, connection_secret, response.freeze()))
    }

    fn generate_service_tickets(
        &self,
        entity_name: &EntityName,
        global_id: u64,
        requested_services: u32,
    ) -> Result<Vec<CephXTicketBlob>> {
        let mut tickets = Vec::with_capacity(self.service_secrets.len());

        let valid_from = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| CephXError::TimeError(format!("system clock before UNIX_EPOCH: {e}")))?
            .as_secs();
        let valid_until = valid_from + self.ticket_ttl.as_secs();

        // Generate tickets only for the requested services we have secrets for.
        for (&service_id, service_secret) in &self.service_secrets {
            if requested_services & service_id == 0 {
                debug!(
                    "Server: Skipping unrequested service_id: {} (requested=0x{:08x})",
                    service_id, requested_services
                );
                continue;
            }

            debug!("Server: Generating ticket for service_id: {}", service_id);

            let service_key = Self::random_aes_key();

            let mut ticket = AuthTicket::new(entity_name.clone(), global_id);
            ticket.set_validity(valid_from, valid_until);
            ticket.caps = AuthCapsInfo::default();
            ticket.flags = 0;

            let ticket_info = CephXServiceTicketInfo::new(ticket, service_key);

            let mut encoded_ticket = BytesMut::new();
            ticket_info.encode(&mut encoded_ticket, 0)?;

            let encrypted_ticket = service_secret.encrypt(&encoded_ticket)?;
            let ticket_blob = CephXTicketBlob::new(0, encrypted_ticket);

            tickets.push(ticket_blob);
        }

        Ok(tickets)
    }

    /// Build AUTH_DONE response
    ///
    /// Returns: (global_id, connection_mode, auth_payload)
    pub fn build_auth_done_response(
        &self,
        global_id: u64,
        connection_mode: u8,
        session_key: &CryptoKey,
        connection_secret: &CryptoKey,
        service_tickets: Bytes,
    ) -> Result<Bytes> {
        let mut response = BytesMut::new();
        global_id.encode(&mut response, 0)?;
        connection_mode.encode(&mut response, 0)?;
        response.extend_from_slice(&service_tickets);

        let encrypted_connection_secret = session_key.encrypt(&connection_secret.secret)?;
        encrypted_connection_secret.encode(&mut response, 0)?;

        // Empty extra_tickets
        0u32.encode(&mut response, 0)?;

        Ok(response.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::types::EntityType;

    const CLIENT_KEYRING: &str = r#"
[client.admin]
    key = AQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAABAgMEBQYHCA==
"#;

    fn test_keyring() -> Keyring {
        Keyring::from_string(CLIENT_KEYRING).expect("test keyring should parse")
    }

    fn test_secret(seed: u8) -> CryptoKey {
        CryptoKey::new(Bytes::from(vec![seed; AES_KEY_LEN]))
    }

    fn decode_ticket_services(
        tickets: &[CephXTicketBlob],
        service_secrets: &[(EntityType, CryptoKey)],
    ) -> Vec<EntityType> {
        let mut decoded_services = Vec::new();

        for ticket in tickets {
            let mut matched = None;

            for (service_type, secret) in service_secrets {
                let Ok(decrypted) = secret.decrypt(&ticket.blob) else {
                    continue;
                };
                let mut decrypted_buf = decrypted.as_ref();
                let Ok(ticket_info) = CephXServiceTicketInfo::decode(&mut decrypted_buf, 0) else {
                    continue;
                };

                matched = Some(*service_type);
                let decoded_global_id = ticket_info.ticket.global_id;
                let decoded_name = ticket_info.ticket.name.to_string();
                let decoded_service_key_len = ticket_info.session_key.len();
                assert_eq!(decoded_global_id, 4242);
                assert_eq!(decoded_name, "client.admin");
                assert_eq!(decoded_service_key_len, AES_KEY_LEN);
                break;
            }

            decoded_services
                .push(matched.expect("ticket should decrypt with one configured secret"));
        }

        decoded_services.sort_by_key(|service| service.bits());
        decoded_services
    }

    fn generate_and_decode_tickets(
        requested_services: EntityType,
        configured_services: &[(EntityType, CryptoKey)],
    ) -> Vec<EntityType> {
        let mut server = CephXServerHandler::new(test_keyring());
        for (service_type, secret) in configured_services {
            server.add_service_secret(service_type.bits(), secret.clone());
        }

        let tickets = server
            .generate_service_tickets(
                &"client.admin".parse().expect("entity name should parse"),
                4242,
                requested_services.bits(),
            )
            .expect("ticket generation should succeed");

        decode_ticket_services(&tickets, configured_services)
    }

    #[test]
    fn test_server_handler_creation() {
        let keyring = Keyring::new();
        let handler = CephXServerHandler::new(keyring);
        assert_eq!(handler.next_global_id, DEFAULT_INITIAL_GLOBAL_ID);
    }

    #[test]
    fn test_global_id_allocation() {
        let keyring = Keyring::new();
        let mut handler = CephXServerHandler::new(keyring);

        let id1 = handler.allocate_global_id();
        let id2 = handler.allocate_global_id();

        assert_eq!(id1, DEFAULT_INITIAL_GLOBAL_ID);
        assert_eq!(id2, DEFAULT_INITIAL_GLOBAL_ID + 1);
    }

    #[test]
    fn test_generate_service_tickets_only_issues_requested_services() {
        let configured_services = vec![
            (EntityType::MON, test_secret(0x11)),
            (EntityType::OSD, test_secret(0x22)),
            (EntityType::MGR, test_secret(0x33)),
        ];

        let decoded_services =
            generate_and_decode_tickets(EntityType::MON | EntityType::MGR, &configured_services);

        assert_eq!(decoded_services, vec![EntityType::MON, EntityType::MGR]);
    }

    #[test]
    fn test_generate_service_tickets_omits_unrequested_services() {
        let configured_services = vec![
            (EntityType::MON, test_secret(0x44)),
            (EntityType::OSD, test_secret(0x55)),
        ];

        let decoded_services =
            generate_and_decode_tickets(EntityType::empty(), &configured_services);

        assert!(decoded_services.is_empty());
    }
}
