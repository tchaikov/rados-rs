//! CephX server-side authentication handler

use crate::error::{CephXError, Result};
use crate::keyring::Keyring;
use crate::protocol::{
    AuthMode, CephXAuthenticate, CephXRequestHeader, CephXServerChallenge, AES_KEY_LEN,
    CEPHX_GET_AUTH_SESSION_KEY,
};
use crate::types::{
    AuthCapsInfo, AuthTicket, CephXServiceTicketInfo, CephXTicketBlob, CryptoKey, EntityName,
    CEPH_CRYPTO_AES,
};
use bytes::{BufMut, Bytes, BytesMut};
use denc::Denc;
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
    /// Create a new server handler with a keyring
    pub fn new(keyring: Keyring) -> Self {
        Self {
            keyring,
            server_challenge: None,
            next_global_id: DEFAULT_INITIAL_GLOBAL_ID,
            service_secrets: HashMap::new(),
            ticket_ttl: Duration::from_secs(DEFAULT_SERVICE_TICKET_TTL_SECS),
        }
    }

    /// Set the ticket time-to-live for generated service tickets
    pub fn set_ticket_ttl(&mut self, ttl: Duration) {
        self.ticket_ttl = ttl;
    }

    /// Add a service secret for generating tickets
    ///
    /// # Arguments
    /// * `service_id` - Service ID (MON=1, OSD=2, MDS=4, MGR=8)
    /// * `secret` - Service secret key
    pub fn add_service_secret(&mut self, service_id: u32, secret: CryptoKey) {
        self.service_secrets.insert(service_id, secret);
    }

    /// Generate a new global_id for a client
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

        // 1. Parse auth_mode (1 byte)
        if buf.is_empty() {
            return Err(CephXError::ProtocolError("Empty auth request".to_string()));
        }
        let auth_mode_byte = buf[0];
        buf = buf.slice(1..);
        let auth_mode = AuthMode::from_u8(auth_mode_byte).ok_or_else(|| {
            CephXError::ProtocolError(format!("Invalid auth mode: {}", auth_mode_byte))
        })?;

        debug!("Server: Received auth request with mode: {:?}", auth_mode);

        // 2. Parse entity_name
        let entity_name = EntityName::decode(&mut buf, 0)?;
        debug!("Server: Client entity name: {}", entity_name);

        // 3. Parse global_id
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

        // Build response: CephXServerChallenge
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
    ) -> Result<(CryptoKey, Bytes)> {
        let mut buf = Bytes::copy_from_slice(payload);

        // 1. Parse CephXRequestHeader
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

        // 2. Parse CephXAuthenticate
        let authenticate = CephXAuthenticate::decode(&mut buf, 0)?;

        debug!(
            "Server: Client challenge response: {}",
            authenticate.client_challenge
        );

        // 3. Get client's secret key from keyring
        let client_secret = self
            .keyring
            .get_key(&entity_name.to_string())
            .ok_or_else(|| {
                CephXError::AuthenticationFailed(format!("No secret for {}", entity_name))
            })?;

        // 4. Verify client's challenge response
        // Client should have encrypted: server_challenge + 1
        let expected_response = self.server_challenge.ok_or_else(|| {
            CephXError::ProtocolError("Server challenge not set before authenticate".to_string())
        })? + 1;

        // Decrypt client's response using client's secret
        // The encrypted response is in the 'key' field
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

        info!(
            "Server: ✓ Client {} authenticated successfully",
            entity_name
        );

        // 5. Generate session key
        let mut session_key_bytes = vec![0u8; AES_KEY_LEN];
        rand::thread_rng().fill_bytes(&mut session_key_bytes);
        let session_key = CryptoKey::new_with_type(CEPH_CRYPTO_AES, Bytes::from(session_key_bytes));

        debug!("Server: Generated session key: {} bytes", session_key.len());

        // 6. Generate service tickets
        let service_tickets =
            self.generate_service_tickets(entity_name, global_id, &session_key)?;

        // 7. Build response with session key and tickets
        let mut response = BytesMut::new();

        // Encrypt session key with client's secret
        let encrypted_session_key = client_secret.encrypt(session_key.get_secret())?;
        encrypted_session_key.encode(&mut response, 0)?;

        // Add service tickets
        service_tickets.encode(&mut response, 0)?;

        Ok((session_key, response.freeze()))
    }

    /// Generate service tickets for the client
    fn generate_service_tickets(
        &self,
        entity_name: &EntityName,
        global_id: u64,
        _session_key: &CryptoKey,
    ) -> Result<Vec<CephXTicketBlob>> {
        let mut tickets = Vec::new();

        // Get current time for ticket validity
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e| {
            CephXError::ProtocolError(format!("System clock before UNIX epoch: {}", e))
        })?;

        let valid_from = now.as_secs();
        let valid_until = valid_from + self.ticket_ttl.as_secs();

        // Generate tickets for each service we have secrets for
        for (&service_id, service_secret) in &self.service_secrets {
            debug!("Server: Generating ticket for service_id: {}", service_id);

            // Generate service session key
            let mut service_key_bytes = vec![0u8; AES_KEY_LEN];
            rand::thread_rng().fill_bytes(&mut service_key_bytes);
            let service_key =
                CryptoKey::new_with_type(CEPH_CRYPTO_AES, Bytes::from(service_key_bytes));

            // Create the authentication ticket
            let mut ticket = AuthTicket::new(entity_name.clone(), global_id);
            ticket.set_validity(valid_from, valid_until);
            ticket.caps = AuthCapsInfo::default();
            ticket.flags = 0;

            // Create the service ticket info
            let ticket_info = CephXServiceTicketInfo::new(ticket, service_key);

            // Encode the ticket info using Denc
            let mut encoded_ticket = BytesMut::new();
            ticket_info.encode(&mut encoded_ticket, 0)?;

            // Encrypt the ticket with the service's secret
            let encrypted_ticket = service_secret.encrypt(&encoded_ticket)?;

            // Create ticket blob
            let ticket_blob = CephXTicketBlob::new(
                0, // Not used in basic implementation
                encrypted_ticket,
            );

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
        _session_key: &CryptoKey,
        service_tickets: Bytes,
    ) -> Result<Bytes> {
        use denc::Denc;
        let mut response = BytesMut::new();

        // 1. global_id
        global_id.encode(&mut response, 0)?;

        // 2. connection_mode
        response.put_u8(connection_mode);

        // 3. auth_payload (session key + tickets)
        response.extend_from_slice(&service_tickets);

        Ok(response.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
