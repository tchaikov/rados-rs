use crate::auth::{CephXKey, CephXRequest, CephXReply, CephXSession, CephXAuthenticator};
use crate::error::{Error, Result};
use crate::types::{EntityName, GlobalId};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthState {
    Initial,
    GetAuthSessionKey,
    GetPrincipalSessionKey,
    GetRotatingKey,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct CephXClientAuth {
    pub entity_name: EntityName,
    pub entity_key: CephXKey,
    pub global_id: Option<GlobalId>,
    pub session_key: Option<CephXKey>,
    pub state: AuthState,
    pub server_challenge: Option<u64>,
    pub client_challenge: Option<u64>,
}

impl CephXClientAuth {
    pub fn new(entity_name: EntityName, entity_key: CephXKey) -> Self {
        Self {
            entity_name,
            entity_key,
            global_id: None,
            session_key: None,
            state: AuthState::Initial,
            server_challenge: None,
            client_challenge: None,
        }
    }

    pub fn start_auth(&mut self) -> Result<Bytes> {
        debug!("Starting CephX authentication for {}", self.entity_name);
        self.state = AuthState::GetAuthSessionKey;
        
        // Step 1: Request authentication session key
        let request = CephXRequest::get_auth_session_key();
        Ok(request.encode())
    }

    pub fn handle_auth_reply(&mut self, reply_data: &[u8]) -> Result<Option<Bytes>> {
        match self.state {
            AuthState::GetAuthSessionKey => {
                self.handle_get_auth_session_key_reply(reply_data)
            }
            AuthState::GetPrincipalSessionKey => {
                self.handle_get_principal_session_key_reply(reply_data)
            }
            AuthState::GetRotatingKey => {
                self.handle_get_rotating_key_reply(reply_data)
            }
            _ => Err(Error::Authentication(format!("Unexpected state: {:?}", self.state))),
        }
    }

    fn handle_get_auth_session_key_reply(&mut self, reply_data: &[u8]) -> Result<Option<Bytes>> {
        debug!("Handling auth session key reply");
        
        let reply = CephXReply::decode(reply_data)?;
        if !reply.is_success() {
            self.state = AuthState::Failed;
            return Err(Error::Authentication(format!("Auth failed: status={}", reply.status)));
        }

        // Extract session key
        if let Some(session_key) = reply.session_key {
            self.session_key = Some(session_key.clone());
            debug!("Received session key");

            // Move to next phase
            self.state = AuthState::GetPrincipalSessionKey;
            
            // Create authenticator for service ticket request
            let auth = CephXAuthenticator::new(
                self.global_id.unwrap_or(0),
                1, // MON service
            );
            
            // Sign authenticator with session key
            let auth_data = auth.encode()?;
            let signature = session_key.sign(&auth_data)?;
            
            // Create request for principal session key
            let mut request_data = BytesMut::new();
            request_data.extend_from_slice(&auth_data);
            request_data.extend_from_slice(&signature);
            
            Ok(Some(request_data.freeze()))
        } else {
            self.state = AuthState::Failed;
            Err(Error::Authentication("No session key in reply".into()))
        }
    }

    fn handle_get_principal_session_key_reply(&mut self, reply_data: &[u8]) -> Result<Option<Bytes>> {
        debug!("Handling principal session key reply");
        
        let reply = CephXReply::decode(reply_data)?;
        if !reply.is_success() {
            self.state = AuthState::Failed;
            return Err(Error::Authentication(format!("Principal auth failed: status={}", reply.status)));
        }

        // For now, we'll complete the authentication here
        // In a full implementation, we might need to handle service tickets
        self.state = AuthState::Completed;
        debug!("CephX authentication completed successfully");
        
        Ok(None) // No more requests needed
    }

    fn handle_get_rotating_key_reply(&mut self, reply_data: &[u8]) -> Result<Option<Bytes>> {
        debug!("Handling rotating key reply");
        
        let reply = CephXReply::decode(reply_data)?;
        if !reply.is_success() {
            self.state = AuthState::Failed;
            return Err(Error::Authentication(format!("Rotating key failed: status={}", reply.status)));
        }

        self.state = AuthState::Completed;
        Ok(None)
    }

    pub fn is_completed(&self) -> bool {
        self.state == AuthState::Completed
    }

    pub fn is_failed(&self) -> bool {
        self.state == AuthState::Failed
    }

    pub fn get_session(&self) -> Option<CephXSession> {
        if let Some(session_key) = &self.session_key {
            let session = CephXSession::new(
                self.entity_name,
                self.global_id.unwrap_or(0),
                session_key.clone(),
            );
            Some(session)
        } else {
            None
        }
    }
}

// Simplified CephX protocol implementation for testing
pub struct SimpleCephXProtocol;

impl SimpleCephXProtocol {
    pub fn create_auth_request(entity_name: &EntityName) -> Bytes {
        // Create a simple auth request
        let mut buf = BytesMut::new();
        
        // Auth method (CephX = 1)
        buf.put_u32_le(1);
        
        // Entity name length
        let name_str = format!("{}", entity_name);
        buf.put_u32_le(name_str.len() as u32);
        buf.extend_from_slice(name_str.as_bytes());
        
        // Global ID (initially 0)
        buf.put_u64_le(0);
        
        buf.freeze()
    }

    pub fn parse_auth_reply(data: &[u8]) -> Result<(i32, Option<GlobalId>)> {
        let mut buf = data;
        
        if buf.remaining() < 4 {
            return Err(Error::Authentication("Invalid auth reply length".into()));
        }
        
        let result = buf.get_i32_le();
        
        let global_id = if buf.remaining() >= 8 {
            Some(buf.get_u64_le())
        } else {
            None
        };
        
        Ok((result, global_id))
    }

    pub fn create_auth_done() -> Bytes {
        // Simple auth done message
        let mut buf = BytesMut::new();
        buf.put_u32_le(0); // Success
        buf.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EntityType;

    #[test]
    fn test_cephx_auth_creation() {
        let entity_name = EntityName::new(EntityType::TYPE_CLIENT, 0);
        let entity_key = CephXKey::from_base64("AQCbbKloRQdAEhAAdkMo2F9iYJQEErX/1uwLNg==").unwrap();
        
        let auth = CephXClientAuth::new(entity_name, entity_key);
        assert_eq!(auth.state, AuthState::Initial);
        assert!(auth.session_key.is_none());
    }

    #[test]
    fn test_auth_request_creation() {
        let entity_name = EntityName::new(EntityType::TYPE_CLIENT, 0);
        let request = SimpleCephXProtocol::create_auth_request(&entity_name);
        assert!(!request.is_empty());
    }

    #[test]
    fn test_auth_reply_parsing() {
        let mut data = BytesMut::new();
        data.put_i32_le(0); // Success
        data.put_u64_le(12345); // Global ID
        
        let (result, global_id) = SimpleCephXProtocol::parse_auth_reply(&data).unwrap();
        assert_eq!(result, 0);
        assert_eq!(global_id, Some(12345));
    }
}