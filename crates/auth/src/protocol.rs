//! CephX protocol structures and constants

use crate::error::{CephXError, Result};
use crate::types::{CephXTicketBlob, CryptoKey};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use denc::{Denc, RadosError};
use serde::Serialize;

/// CephX request types
pub const CEPHX_GET_AUTH_SESSION_KEY: u16 = 0x0100;
pub const CEPHX_GET_PRINCIPAL_SESSION_KEY: u16 = 0x0200;
pub const CEPHX_GET_ROTATING_KEY: u16 = 0x0400;

/// Authentication modes
/// From src/auth/Auth.h
pub const AUTH_MODE_MON: u8 = 10;

/// Magic value for encrypted CephX data
/// From src/auth/cephx/CephxProtocol.h
pub const AUTH_ENC_MAGIC: u64 = 0xff009cad8826aa55;

/// CephX request header structure
///
/// Corresponds to C++ `struct CephXRequestHeader` in `/src/auth/cephx/CephxProtocol.h`
///
/// C++ encoding format:
/// - `__u16 request_type` - Request type (CEPHX_GET_AUTH_SESSION_KEY, etc.)
///
/// This is the header for all CephX protocol messages after initial authentication.
#[derive(Debug, Clone, Serialize)]
pub struct CephXRequestHeader {
    pub request_type: u16,
}

impl Denc for CephXRequestHeader {
    fn encode(&self, _features: u64) -> std::result::Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u16_le(self.request_type);
        Ok(buf.freeze())
    }

    fn decode(data: &mut Bytes) -> std::result::Result<Self, RadosError> {
        if data.len() < 2 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for request header".to_string(),
            ));
        }
        let request_type = data.get_u16_le();
        Ok(Self { request_type })
    }
}

/// CephX response header structure
///
/// Corresponds to C++ `struct CephXResponseHeader` in `/src/auth/cephx/CephxProtocol.h`
///
/// C++ encoding format:
/// - `__u16 request_type` - Request type (CEPHX_GET_AUTH_SESSION_KEY, etc.)
/// - `__s32 status` - Status code (0 = success)
///
/// This is the header for all CephX protocol response messages.
#[derive(Debug, Clone, Serialize)]
pub struct CephXResponseHeader {
    pub request_type: u16,
    pub status: i32,
}

impl Denc for CephXResponseHeader {
    fn encode(&self, _features: u64) -> std::result::Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(6);
        buf.put_u16_le(self.request_type);
        buf.put_i32_le(self.status);
        Ok(buf.freeze())
    }

    fn decode(data: &mut Bytes) -> std::result::Result<Self, RadosError> {
        if data.len() < 6 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for response header (need 6 bytes)".to_string(),
            ));
        }
        let request_type = data.get_u16_le();
        let status = data.get_i32_le();
        Ok(Self {
            request_type,
            status,
        })
    }
}

/// CephX authenticate request structure
///
/// Corresponds to C++ `struct CephXAuthenticate` in `/src/auth/cephx/CephxProtocol.h`
///
/// C++ encoding format:
/// - `__u8 struct_v` - Structure version (currently 3)
/// - `uint64_t client_challenge` - Random challenge from client
/// - `uint64_t key` - Encrypted session key (result of cephx_calc_client_server_challenge)
/// - `CephXTicketBlob old_ticket` - Previous ticket if re-authenticating
/// - `uint32_t other_keys` - Bitmask of other service keys to request
///
/// This is sent by client in response to server's challenge.
#[derive(Debug, Clone, Serialize)]
pub struct CephXAuthenticate {
    pub client_challenge: u64,
    pub key: u64, // Encrypted session key (result of cephx_calc_client_server_challenge)
    pub old_ticket: CephXTicketBlob,
    pub other_keys: u32, // What other service keys we want
}

impl Denc for CephXAuthenticate {
    fn encode(&self, features: u64) -> std::result::Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();

        // Encode struct version (must be first!)
        buf.put_u8(3); // struct_v = 3

        // Encode client_challenge
        buf.put_u64_le(self.client_challenge);

        // Encode key (as u64, not a buffer)
        buf.put_u64_le(self.key);

        // Encode old_ticket
        let ticket_encoded = self.old_ticket.encode(features)?;
        buf.extend_from_slice(&ticket_encoded);

        // Encode other_keys
        buf.put_u32_le(self.other_keys);

        Ok(buf.freeze())
    }

    fn decode(data: &mut Bytes) -> std::result::Result<Self, RadosError> {
        if data.is_empty() {
            return Err(RadosError::Protocol(
                "Insufficient bytes for struct_v".to_string(),
            ));
        }

        // Decode struct version
        let struct_v = data.get_u8();
        if !(1..=3).contains(&struct_v) {
            return Err(RadosError::Protocol(format!(
                "Unsupported CephXAuthenticate version: {}",
                struct_v
            )));
        }

        if data.len() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for auth data".to_string(),
            ));
        }

        let client_challenge = data.get_u64_le();

        let key = data.get_u64_le();

        let old_ticket = CephXTicketBlob::decode(data)?;

        // other_keys was added in v2
        let other_keys = if struct_v >= 2 {
            if data.len() < 4 {
                return Err(RadosError::Protocol(
                    "Insufficient bytes for other_keys".to_string(),
                ));
            }
            data.get_u32_le()
        } else {
            0
        };

        Ok(Self {
            client_challenge,
            key,
            old_ticket,
            other_keys,
        })
    }
}

/// CephX server challenge structure
///
/// Corresponds to C++ `struct CephXServerChallenge` in `/src/auth/cephx/CephxProtocol.h`
///
/// C++ encoding format:
/// - `__u8 struct_v` - Structure version (currently 1)
/// - `uint64_t server_challenge` - Random challenge from server
///
/// This is the initial challenge sent by server to client to start CephX authentication.
#[derive(Debug, Clone)]
pub struct CephXServerChallenge {
    pub server_challenge: u64,
}

impl Denc for CephXServerChallenge {
    fn encode(&self, _features: u64) -> std::result::Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(self.server_challenge);
        Ok(buf.freeze())
    }

    fn decode(data: &mut Bytes) -> std::result::Result<Self, RadosError> {
        if data.len() < 9 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for CephXServerChallenge (need 9 bytes: 1 struct_v + 8 challenge)".to_string(),
            ));
        }
        // Read and validate struct_v
        let struct_v = data.get_u8();
        if struct_v != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported CephXServerChallenge version: {}",
                struct_v
            )));
        }
        // Read server challenge
        let server_challenge = data.get_u64_le();
        Ok(Self { server_challenge })
    }
}

/// CephX challenge blob for session key calculation
///
/// Corresponds to C++ `struct CephXChallengeBlob` in `/src/auth/cephx/CephxProtocol.h`
///
/// This structure is used in the challenge-response authentication
/// to calculate the session key from server and client challenges.
#[derive(Debug, Clone)]
pub struct CephXChallengeBlob {
    pub server_challenge: u64,
    pub client_challenge: u64,
}

impl Denc for CephXChallengeBlob {
    fn encode(&self, _features: u64) -> std::result::Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.server_challenge);
        buf.put_u64_le(self.client_challenge);
        Ok(buf.freeze())
    }

    fn decode(data: &mut Bytes) -> std::result::Result<Self, RadosError> {
        if data.len() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for challenge blob".to_string(),
            ));
        }
        let server_challenge = data.get_u64_le();
        let client_challenge = data.get_u64_le();
        Ok(Self {
            server_challenge,
            client_challenge,
        })
    }
}

/// CephX request structure
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
            return Err(CephXError::ProtocolError(
                "Insufficient request data".into(),
            ));
        }

        let request_type = data.get_u16_le();
        let keys_len = data.get_u32_le() as usize;

        if data.remaining() < keys_len * 4 + 1 {
            return Err(CephXError::ProtocolError("Insufficient key data".into()));
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

/// CephX reply structure
#[derive(Debug, Clone)]
pub struct CephXReply {
    pub status: i32,
    pub tickets: Vec<CephXTicketBlob>,
    pub session_key: Option<CryptoKey>,
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

    pub fn with_session_key(mut self, key: CryptoKey) -> Self {
        self.session_key = Some(key);
        self
    }

    pub fn with_ticket(mut self, ticket: CephXTicketBlob) -> Self {
        self.tickets.push(ticket);
        self
    }

    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_i32_le(self.status);
        buf.put_u32_le(self.tickets.len() as u32);

        for ticket in &self.tickets {
            let ticket_data = ticket
                .encode(0)
                .map_err(|e| CephXError::EncodingError(e.to_string()))?;
            buf.put_u32_le(ticket_data.len() as u32);
            buf.extend_from_slice(&ticket_data);
        }

        match &self.session_key {
            Some(key) => {
                let key_encoded = key
                    .encode(0)
                    .map_err(|e| CephXError::EncodingError(e.to_string()))?;
                buf.extend_from_slice(&key_encoded);
            }
            None => {
                buf.put_u32_le(0);
            }
        }

        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        if data.remaining() < 8 {
            return Err(CephXError::ProtocolError("Insufficient reply data".into()));
        }

        let status = data.get_i32_le();
        let tickets_len = data.get_u32_le() as usize;

        let mut tickets = Vec::with_capacity(tickets_len);
        for _ in 0..tickets_len {
            if data.remaining() < 4 {
                return Err(CephXError::ProtocolError(
                    "Insufficient ticket length data".into(),
                ));
            }

            let ticket_len = data.get_u32_le() as usize;
            if data.remaining() < ticket_len {
                return Err(CephXError::ProtocolError("Insufficient ticket data".into()));
            }

            let mut ticket_data = vec![0u8; ticket_len];
            data.copy_to_slice(&mut ticket_data);
            let ticket = CephXTicketBlob::decode(&mut Bytes::copy_from_slice(&ticket_data))
                .map_err(|e| CephXError::EncodingError(e.to_string()))?;
            tickets.push(ticket);
        }

        let session_key = if data.remaining() >= 4 {
            let mut key_data = Bytes::copy_from_slice(data);
            let key = CryptoKey::decode(&mut key_data)
                .map_err(|e| CephXError::EncodingError(e.to_string()))?;
            if !key.is_empty() {
                Some(key)
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
