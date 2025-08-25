use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::{Error, Result};
use crate::types::{EntityName, GlobalId};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

pub const CEPHX_GET_AUTH_SESSION_KEY: u16 = 0x0100;
pub const CEPHX_GET_PRINCIPAL_SESSION_KEY: u16 = 0x0200;
pub const CEPHX_GET_ROTATING_KEY: u16 = 0x0400;

#[derive(Debug, Clone)]
pub struct CephXKey {
    data: Bytes,
}

impl CephXKey {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    pub fn from_base64(base64_str: &str) -> Result<Self> {
        let data = STANDARD.decode(base64_str)
            .map_err(|e| Error::Authentication(format!("Invalid base64 key: {}", e)))?;
        Ok(Self { data: Bytes::from(data) })
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn sign(&self, message: &[u8]) -> Result<Bytes> {
        let mut mac = HmacSha256::new_from_slice(&self.data)
            .map_err(|e| Error::Authentication(format!("HMAC key error: {}", e)))?;
        mac.update(message);
        Ok(Bytes::from(mac.finalize().into_bytes().to_vec()))
    }

    pub fn verify(&self, message: &[u8], signature: &[u8]) -> Result<bool> {
        let expected_sig = self.sign(message)?;
        Ok(expected_sig == signature)
    }
}

#[derive(Debug, Clone)]
pub struct CephXTicket {
    pub source: GlobalId,
    pub dest: GlobalId,
    pub secret_key: CephXKey,
    pub expires: SystemTime,
}

impl CephXTicket {
    pub fn new(source: GlobalId, dest: GlobalId, secret_key: CephXKey) -> Self {
        let expires = SystemTime::now() + Duration::from_secs(3600); // 1 hour
        Self {
            source,
            dest,
            secret_key,
            expires,
        }
    }

    pub fn is_expired(&self) -> bool {
        SystemTime::now() > self.expires
    }

    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_u64_le(self.source);
        buf.put_u64_le(self.dest);
        buf.put_u32_le(self.secret_key.len() as u32);
        buf.extend_from_slice(&self.secret_key.data);
        
        let duration = self.expires.duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Authentication(format!("Time error: {}", e)))?;
        buf.put_u64_le(duration.as_secs());
        
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 24 {
            return Err(Error::Authentication("Insufficient ticket data".into()));
        }

        let source = data.get_u64_le();
        let dest = data.get_u64_le();
        let key_len = data.get_u32_le() as usize;
        
        if data.remaining() < key_len + 8 {
            return Err(Error::Authentication("Insufficient key data".into()));
        }

        let mut key_data = vec![0u8; key_len];
        data.copy_to_slice(&mut key_data);
        let secret_key = CephXKey::new(Bytes::from(key_data));
        
        let expires_secs = data.get_u64_le();
        let expires = UNIX_EPOCH + Duration::from_secs(expires_secs);

        Ok(Self {
            source,
            dest,
            secret_key,
            expires,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CephXAuthenticator {
    pub client_challenge: u64,
    pub server_challenge: u64,
    pub global_id: GlobalId,
    pub service_id: u32,
    pub timestamp: SystemTime,
    pub nonce: u64,
}

impl CephXAuthenticator {
    pub fn new(global_id: GlobalId, service_id: u32) -> Self {
        let mut rng = rand::thread_rng();
        Self {
            client_challenge: rng.next_u64(),
            server_challenge: 0,
            global_id,
            service_id,
            timestamp: SystemTime::now(),
            nonce: rng.next_u64(),
        }
    }

    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_u64_le(self.client_challenge);
        buf.put_u64_le(self.server_challenge);
        buf.put_u64_le(self.global_id);
        buf.put_u32_le(self.service_id);
        
        let timestamp = self.timestamp.duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Authentication(format!("Time error: {}", e)))?;
        buf.put_u64_le(timestamp.as_secs());
        buf.put_u32_le(timestamp.subsec_nanos());
        buf.put_u64_le(self.nonce);
        
        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 44 {
            return Err(Error::Authentication("Insufficient authenticator data".into()));
        }

        let client_challenge = data.get_u64_le();
        let server_challenge = data.get_u64_le();
        let global_id = data.get_u64_le();
        let service_id = data.get_u32_le();
        let timestamp_secs = data.get_u64_le();
        let timestamp_nanos = data.get_u32_le();
        let nonce = data.get_u64_le();

        let timestamp = UNIX_EPOCH + Duration::from_secs(timestamp_secs) + Duration::from_nanos(timestamp_nanos as u64);

        Ok(Self {
            client_challenge,
            server_challenge,
            global_id,
            service_id,
            timestamp,
            nonce,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CephXSession {
    pub entity_name: EntityName,
    pub global_id: GlobalId,
    pub session_key: CephXKey,
    pub ticket: Option<CephXTicket>,
    pub service_tickets: HashMap<u32, CephXTicket>,
}

impl CephXSession {
    pub fn new(entity_name: EntityName, global_id: GlobalId, session_key: CephXKey) -> Self {
        Self {
            entity_name,
            global_id,
            session_key,
            ticket: None,
            service_tickets: HashMap::new(),
        }
    }

    pub fn add_service_ticket(&mut self, service_id: u32, ticket: CephXTicket) {
        self.service_tickets.insert(service_id, ticket);
    }

    pub fn get_service_ticket(&self, service_id: u32) -> Option<&CephXTicket> {
        self.service_tickets.get(&service_id)
    }

    pub fn create_authenticator(&self, service_id: u32) -> CephXAuthenticator {
        CephXAuthenticator::new(self.global_id, service_id)
    }

    pub fn sign_authenticator(&self, auth: &CephXAuthenticator) -> Result<Bytes> {
        let auth_data = auth.encode()?;
        self.session_key.sign(&auth_data)
    }
}

#[derive(Debug, Clone)]
pub struct CephXRequest {
    pub request_type: u16,
    pub keys: Vec<u32>,
    pub other_keys: bool,
}

impl CephXRequest {
    pub fn new(request_type: u16) -> Self {
        Self {
            request_type,
            keys: Vec::new(),
            other_keys: false,
        }
    }

    pub fn get_auth_session_key() -> Self {
        Self::new(CEPHX_GET_AUTH_SESSION_KEY)
    }

    pub fn get_principal_session_key() -> Self {
        Self::new(CEPHX_GET_PRINCIPAL_SESSION_KEY)
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u16_le(self.request_type);
        buf.put_u32_le(self.keys.len() as u32);
        
        for &key in &self.keys {
            buf.put_u32_le(key);
        }
        
        buf.put_u8(if self.other_keys { 1 } else { 0 });
        buf.freeze()
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 7 {
            return Err(Error::Authentication("Insufficient request data".into()));
        }

        let request_type = data.get_u16_le();
        let keys_len = data.get_u32_le() as usize;
        
        if data.remaining() < keys_len * 4 + 1 {
            return Err(Error::Authentication("Insufficient key data".into()));
        }

        let mut keys = Vec::with_capacity(keys_len);
        for _ in 0..keys_len {
            keys.push(data.get_u32_le());
        }
        
        let other_keys = data.get_u8() != 0;

        Ok(Self {
            request_type,
            keys,
            other_keys,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CephXReply {
    pub status: i32,
    pub tickets: Vec<CephXTicket>,
    pub session_key: Option<CephXKey>,
}

impl CephXReply {
    pub fn new(status: i32) -> Self {
        Self {
            status,
            tickets: Vec::new(),
            session_key: None,
        }
    }

    pub fn success() -> Self {
        Self::new(0)
    }

    pub fn with_session_key(mut self, key: CephXKey) -> Self {
        self.session_key = Some(key);
        self
    }

    pub fn with_ticket(mut self, ticket: CephXTicket) -> Self {
        self.tickets.push(ticket);
        self
    }

    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_i32_le(self.status);
        buf.put_u32_le(self.tickets.len() as u32);

        for ticket in &self.tickets {
            let ticket_data = ticket.encode()?;
            buf.put_u32_le(ticket_data.len() as u32);
            buf.extend_from_slice(&ticket_data);
        }

        match &self.session_key {
            Some(key) => {
                buf.put_u32_le(key.len() as u32);
                buf.extend_from_slice(&key.data);
            }
            None => {
                buf.put_u32_le(0);
            }
        }

        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 8 {
            return Err(Error::Authentication("Insufficient reply data".into()));
        }

        let status = data.get_i32_le();
        let tickets_len = data.get_u32_le() as usize;

        let mut tickets = Vec::with_capacity(tickets_len);
        for _ in 0..tickets_len {
            if data.remaining() < 4 {
                return Err(Error::Authentication("Insufficient ticket length data".into()));
            }
            
            let ticket_len = data.get_u32_le() as usize;
            if data.remaining() < ticket_len {
                return Err(Error::Authentication("Insufficient ticket data".into()));
            }

            let mut ticket_data = vec![0u8; ticket_len];
            data.copy_to_slice(&mut ticket_data);
            let ticket = CephXTicket::decode(&ticket_data)?;
            tickets.push(ticket);
        }

        let session_key = if data.remaining() >= 4 {
            let key_len = data.get_u32_le() as usize;
            if key_len > 0 && data.remaining() >= key_len {
                let mut key_data = vec![0u8; key_len];
                data.copy_to_slice(&mut key_data);
                Some(CephXKey::new(Bytes::from(key_data)))
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            status,
            tickets,
            session_key,
        })
    }

    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}