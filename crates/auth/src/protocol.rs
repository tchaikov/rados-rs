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
// Auth modes for different Ceph services
// Reference: ~/dev/ceph/src/auth/Auth.h
/// Authentication mode for different Ceph services
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// No authentication
    None = 0,
    /// Authorizer mode - used for OSDs, MDSs, MGRs, and other data services
    Authorizer = 1,
    /// Monitor mode - used specifically for monitor connections
    Mon = 10,
}

impl AuthMode {
    /// Convert to u8 for wire protocol
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Create from u8
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(AuthMode::None),
            1 => Some(AuthMode::Authorizer),
            10 => Some(AuthMode::Mon),
            _ => None,
        }
    }
}

// Legacy constants for backwards compatibility
pub const AUTH_MODE_NONE: u8 = AuthMode::None as u8;
pub const AUTH_MODE_AUTHORIZER: u8 = AuthMode::Authorizer as u8;
pub const AUTH_MODE_MON: u8 = AuthMode::Mon as u8;

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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        buf.put_u16_le(self.request_type);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 2 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for request header".to_string(),
            ));
        }
        let request_type = buf.get_u16_le();
        Ok(Self { request_type })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(2)
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        buf.put_u16_le(self.request_type);
        buf.put_i32_le(self.status);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 6 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for response header (need 6 bytes)".to_string(),
            ));
        }
        let request_type = buf.get_u16_le();
        let status = buf.get_i32_le();
        Ok(Self {
            request_type,
            status,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(6)
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
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        // Encode struct version (must be first!)
        buf.put_u8(3); // struct_v = 3

        // Encode client_challenge
        buf.put_u64_le(self.client_challenge);

        // Encode key (as u64, not a buffer)
        buf.put_u64_le(self.key);

        // Encode old_ticket
        self.old_ticket.encode(buf, features)?;

        // Encode other_keys
        buf.put_u32_le(self.other_keys);

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for struct_v".to_string(),
            ));
        }

        // Decode struct version
        let struct_v = buf.get_u8();
        if !(1..=3).contains(&struct_v) {
            return Err(RadosError::Protocol(format!(
                "Unsupported CephXAuthenticate version: {}",
                struct_v
            )));
        }

        if buf.remaining() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for auth data".to_string(),
            ));
        }

        let client_challenge = buf.get_u64_le();

        let key = buf.get_u64_le();

        let old_ticket = CephXTicketBlob::decode(buf, features)?;

        // other_keys was added in v2
        let other_keys = if struct_v >= 2 {
            if buf.remaining() < 4 {
                return Err(RadosError::Protocol(
                    "Insufficient bytes for other_keys".to_string(),
                ));
            }
            buf.get_u32_le()
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

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(1 + 8 + 8 + self.old_ticket.encoded_size(features)? + 4)
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        buf.put_u64_le(self.server_challenge);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 9 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for CephXServerChallenge (need 9 bytes: 1 struct_v + 8 challenge)".to_string(),
            ));
        }
        // Read and validate struct_v
        let struct_v = buf.get_u8();
        if struct_v != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported CephXServerChallenge version: {}",
                struct_v
            )));
        }
        // Read server challenge
        let server_challenge = buf.get_u64_le();
        Ok(Self { server_challenge })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        buf.put_u64_le(self.server_challenge);
        buf.put_u64_le(self.client_challenge);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for challenge blob".to_string(),
            ));
        }
        let server_challenge = buf.get_u64_le();
        let client_challenge = buf.get_u64_le();
        Ok(Self {
            server_challenge,
            client_challenge,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(16)
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
            let mut ticket_buf = BytesMut::new();
            ticket
                .encode(&mut ticket_buf, 0)
                .map_err(|e| CephXError::EncodingError(e.to_string()))?;
            buf.put_u32_le(ticket_buf.len() as u32);
            buf.extend_from_slice(&ticket_buf);
        }

        match &self.session_key {
            Some(key) => {
                key.encode(&mut buf, 0)
                    .map_err(|e| CephXError::EncodingError(e.to_string()))?;
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
            let mut ticket_buf = Bytes::copy_from_slice(&ticket_data);
            let ticket = CephXTicketBlob::decode(&mut ticket_buf, 0)
                .map_err(|e| CephXError::EncodingError(e.to_string()))?;
            tickets.push(ticket);
        }

        let session_key = if data.remaining() >= 4 {
            let mut key_data = Bytes::copy_from_slice(data);
            let key = CryptoKey::decode(&mut key_data, 0)
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

/// CephX Authorize A structure
/// Corresponds to C++ `struct ceph_x_authorize_a` in auth_x_protocol.h
///
/// This is the first part of the authorizer sent to a service (OSD, MDS, etc.)
/// Contains the service ticket obtained from the monitor
#[derive(Debug, Clone)]
pub struct CephXAuthorizeA {
    pub global_id: u64,
    pub service_id: u32,
    pub ticket_blob: CephXTicketBlob,
}

impl CephXAuthorizeA {
    pub fn new(global_id: u64, service_id: u32, ticket_blob: CephXTicketBlob) -> Self {
        Self {
            global_id,
            service_id,
            ticket_blob,
        }
    }
}

impl Denc for CephXAuthorizeA {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        // struct_v = 1
        buf.put_u8(1);
        // global_id
        buf.put_u64_le(self.global_id);
        // service_id
        buf.put_u32_le(self.service_id);
        // ticket_blob (includes struct_v, secret_id, blob)
        self.ticket_blob.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 13 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for CephXAuthorizeA".to_string(),
            ));
        }
        let _struct_v = buf.get_u8();
        let global_id = buf.get_u64_le();
        let service_id = buf.get_u32_le();
        let ticket_blob = CephXTicketBlob::decode(buf, features)?;
        Ok(Self {
            global_id,
            service_id,
            ticket_blob,
        })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(1 + 8 + 4 + self.ticket_blob.encoded_size(features)?)
    }
}

/// CephX Authorize B structure
/// Corresponds to C++ `struct ceph_x_authorize_b` in auth_x_protocol.h
///
/// This is the second part of the authorizer (encrypted with session key)
/// Contains a nonce and optionally a server challenge response
#[derive(Debug, Clone)]
pub struct CephXAuthorizeB {
    pub nonce: u64,
    pub have_challenge: bool,
    pub server_challenge_plus_one: u64,
}

impl CephXAuthorizeB {
    pub fn new(nonce: u64) -> Self {
        Self {
            nonce,
            have_challenge: false,
            server_challenge_plus_one: 0,
        }
    }

    pub fn with_challenge(nonce: u64, server_challenge: u64) -> Self {
        Self {
            nonce,
            have_challenge: true,
            server_challenge_plus_one: server_challenge.wrapping_add(1),
        }
    }
}

impl Denc for CephXAuthorizeB {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        // struct_v = 2 (per Linux kernel implementation)
        buf.put_u8(2);
        // nonce
        buf.put_u64_le(self.nonce);
        // have_challenge
        buf.put_u8(if self.have_challenge { 1 } else { 0 });
        // server_challenge_plus_one (always encode, even if 0)
        buf.put_u64_le(self.server_challenge_plus_one);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 10 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for CephXAuthorizeB".to_string(),
            ));
        }
        let _struct_v = buf.get_u8();
        let nonce = buf.get_u64_le();
        let have_challenge = buf.get_u8() != 0;
        let server_challenge_plus_one = if have_challenge {
            if buf.remaining() < 8 {
                return Err(RadosError::Protocol(
                    "Insufficient bytes for server_challenge_plus_one".to_string(),
                ));
            }
            buf.get_u64_le()
        } else {
            0
        };
        Ok(Self {
            nonce,
            have_challenge,
            server_challenge_plus_one,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        let base_size = 1 + 8 + 1; // struct_v + nonce + have_challenge
        let challenge_size = if self.have_challenge { 8 } else { 0 };
        Some(base_size + challenge_size)
    }
}

/// CephX Authorize Reply structure
/// Corresponds to C++ `struct ceph_x_authorize_reply` in auth_x_protocol.h
///
/// Sent by the service back to the client after validating the authorizer
#[derive(Debug, Clone)]
pub struct CephXAuthorizeReply {
    pub nonce_plus_one: u64,
}

impl Denc for CephXAuthorizeReply {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        // struct_v = 1
        buf.put_u8(1);
        // nonce_plus_one
        buf.put_u64_le(self.nonce_plus_one);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 9 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for CephXAuthorizeReply".to_string(),
            ));
        }
        let _struct_v = buf.get_u8();
        let nonce_plus_one = buf.get_u64_le();
        Ok(Self { nonce_plus_one })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1 + 8) // struct_v + nonce_plus_one
    }
}
