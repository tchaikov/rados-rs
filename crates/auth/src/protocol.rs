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

/// CephX service ticket request structure
///
/// Corresponds to C++ `struct CephXServiceTicketRequest` in `/src/auth/cephx/CephxProtocol.h`
///
/// C++ encoding format:
/// - `__u8 struct_v` - Structure version (currently 1)
/// - `uint32_t keys` - Bitmask of service types (MON|OSD|MDS|MGR)
///
/// This structure is used to request service tickets from the monitor.
#[derive(Debug, Clone)]
pub struct CephXServiceTicketRequest {
    pub keys: u32,
}

impl Denc for CephXServiceTicketRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        1u8.encode(buf, 0)?; // struct_v
        self.keys.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let _struct_v = u8::decode(buf, 0)?;
        let keys = u32::decode(buf, 0)?;
        Ok(Self { keys })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(5) // 1 byte struct_v + 4 bytes keys
    }
}

/// CephX service ticket structure (encrypted payload)
///
/// Corresponds to C++ `struct CephXServiceTicket` in `/src/auth/cephx/CephxProtocol.h`
///
/// C++ encoding format:
/// - `__u8 struct_v` - Structure version (currently 1)
/// - `CryptoKey session_key` - Session key for the service
/// - `utime_t validity` - Ticket validity period
///
/// This structure is sent encrypted inside the AUTH_DONE response.
/// It contains the session key and validity for a specific service.
#[derive(Debug, Clone)]
pub struct CephXServiceTicket {
    pub session_key: CryptoKey,
    pub validity: std::time::Duration,
}

impl Denc for CephXServiceTicket {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        1u8.encode(buf, 0)?; // struct_v
        self.session_key.encode(buf, 0)?;
        self.validity.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let _struct_v = u8::decode(buf, 0)?;
        let session_key = CryptoKey::decode(buf, 0)?;
        let validity = std::time::Duration::decode(buf, 0)?;
        Ok(Self {
            session_key,
            validity,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // struct_v (1) + session_key (variable) + validity (8)
        Some(1 + self.session_key.encoded_size(0)? + 8)
    }
}

/// Encrypted service ticket (before decryption)
///
/// Represents the encrypted form of CephXServiceTicket as it appears on the wire.
/// After decryption, contains CephXServiceTicket inside an encrypted envelope.
///
/// Wire format:
/// - `version: u8` - Service ticket version
/// - `encrypted_data: Bytes` - Length-prefixed encrypted CephXServiceTicket
#[derive(Debug, Clone)]
pub struct EncryptedServiceTicket {
    pub version: u8,
    pub encrypted_data: Bytes,
}

impl Denc for EncryptedServiceTicket {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        self.version.encode(buf, 0)?;
        self.encrypted_data.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let version = u8::decode(buf, 0)?;
        let encrypted_data = Bytes::decode(buf, 0)?;
        Ok(Self {
            version,
            encrypted_data,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // version (1) + length_prefix (4) + encrypted_data length
        Some(1 + 4 + self.encrypted_data.len())
    }
}

/// Complete service ticket information
///
/// Represents a single service ticket entry in the service ticket reply.
/// Contains all fields needed to authenticate with a specific service.
///
/// Wire format:
/// - `service_id: u32` - Service type (MON=6, OSD=4, MDS=2, MGR=32)
/// - `encrypted_service_ticket: EncryptedServiceTicket` - Encrypted ticket
/// - `ticket_enc: u8` - Ticket encoding type (1 = encrypted, 0 = unencrypted)
/// - `ticket_blob: CephXTicketBlob` - The actual ticket blob for the service
#[derive(Debug, Clone)]
pub struct ServiceTicketInfo {
    pub service_id: u32,
    pub encrypted_service_ticket: EncryptedServiceTicket,
    pub ticket_enc: u8,
    pub ticket_blob: CephXTicketBlob,
}

impl Denc for ServiceTicketInfo {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        self.service_id.encode(buf, 0)?;
        self.encrypted_service_ticket.encode(buf, features)?;
        self.ticket_enc.encode(buf, 0)?;

        // Encode ticket_blob with outer length prefix
        // First encode to a temp buffer to get the length
        let mut temp_buf = bytes::BytesMut::new();
        self.ticket_blob.encode(&mut temp_buf, features)?;

        // Write the length prefix
        (temp_buf.len() as u32).encode(buf, 0)?;

        // Write the encoded ticket_blob
        buf.put_slice(&temp_buf);

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        let service_id = u32::decode(buf, 0)?;
        let encrypted_service_ticket = EncryptedServiceTicket::decode(buf, features)?;
        let ticket_enc = u8::decode(buf, 0)?;

        // Read outer length prefix for ticket_blob
        let ticket_blob_len = u32::decode(buf, 0)? as usize;

        // Read the ticket_blob data into a temporary buffer
        if buf.remaining() < ticket_blob_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient data for ticket blob: need {}, have {}",
                ticket_blob_len,
                buf.remaining()
            )));
        }

        let mut ticket_blob_data = vec![0u8; ticket_blob_len];
        buf.copy_to_slice(&mut ticket_blob_data);
        let mut ticket_blob_buf = &ticket_blob_data[..];

        // Now decode CephXTicketBlob from the sized buffer
        let ticket_blob = CephXTicketBlob::decode(&mut ticket_blob_buf, features)?;

        Ok(Self {
            service_id,
            encrypted_service_ticket,
            ticket_enc,
            ticket_blob,
        })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(
            4 + // service_id
            self.encrypted_service_ticket.encoded_size(features)? +
            1 + // ticket_enc
            4 + // ticket_blob_len (outer length prefix)
            self.ticket_blob.encoded_size(features)?,
        )
    }
}

/// Service ticket reply containing all requested service tickets
///
/// Represents the complete response from the monitor containing service tickets
/// for authentication with various Ceph services (MON, OSD, MDS, MGR).
///
/// Wire format:
/// - `struct_v: u8` - Structure version (currently 1)
/// - `num_tickets: u32` - Number of tickets in the list (implicit in Vec encoding)
/// - `tickets: Vec<ServiceTicketInfo>` - List of service tickets
#[derive(Debug, Clone)]
pub struct ServiceTicketReply {
    pub struct_v: u8,
    pub tickets: Vec<ServiceTicketInfo>,
}

impl Denc for ServiceTicketReply {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        self.struct_v.encode(buf, 0)?;
        // Encode Vec length as u32
        (self.tickets.len() as u32).encode(buf, 0)?;
        // Encode each ticket
        for ticket in &self.tickets {
            ticket.encode(buf, features)?;
        }
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        let struct_v = u8::decode(buf, 0)?;
        let num_tickets = u32::decode(buf, 0)? as usize;
        let mut tickets = Vec::with_capacity(num_tickets);
        for _ in 0..num_tickets {
            tickets.push(ServiceTicketInfo::decode(buf, features)?);
        }
        Ok(Self { struct_v, tickets })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let mut size = 1 + 4; // struct_v + num_tickets
        for ticket in &self.tickets {
            size += ticket.encoded_size(features)?;
        }
        Some(size)
    }
}

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

/// Fixed IV used for all CephX AES-128-CBC encryption
/// From src/auth/Crypto.cc
pub const CEPH_AES_IV: &[u8; 16] = b"cephsageyudagreg";

/// Encrypted envelope wrapper for CephX encrypted data
/// Contains struct_v, magic verification, and the payload
/// This is the standard CephX encryption envelope format
#[derive(Debug, Clone)]
pub struct CephXEncryptedEnvelope<T> {
    pub payload: T,
}

impl<T: Denc> Denc for CephXEncryptedEnvelope<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        1u8.encode(buf, 0)?;
        AUTH_ENC_MAGIC.encode(buf, 0)?;
        self.payload.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        let _struct_v = u8::decode(buf, 0)?;
        let magic = u64::decode(buf, 0)?;
        if magic != AUTH_ENC_MAGIC {
            return Err(RadosError::Protocol(format!(
                "Invalid magic: expected 0x{:016x}, got 0x{:016x}",
                AUTH_ENC_MAGIC, magic
            )));
        }
        let payload = T::decode(buf, features)?;
        Ok(Self { payload })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(1 + 8 + self.payload.encoded_size(features)?)
    }
}

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
        self.request_type.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let request_type = u16::decode(buf, 0)?;
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
        self.request_type.encode(buf, 0)?;
        self.status.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let request_type = u16::decode(buf, 0)?;
        let status = i32::decode(buf, 0)?;
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
        3u8.encode(buf, 0)?; // struct_v = 3

        // Encode client_challenge
        self.client_challenge.encode(buf, 0)?;

        // Encode key (as u64, not a buffer)
        self.key.encode(buf, 0)?;

        // Encode old_ticket
        self.old_ticket.encode(buf, features)?;

        // Encode other_keys
        self.other_keys.encode(buf, 0)?;

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        // Decode struct version
        let struct_v = u8::decode(buf, 0)?;
        if !(1..=3).contains(&struct_v) {
            return Err(RadosError::Protocol(format!(
                "Unsupported CephXAuthenticate version: {}",
                struct_v
            )));
        }

        let client_challenge = u64::decode(buf, 0)?;
        let key = u64::decode(buf, 0)?;
        let old_ticket = CephXTicketBlob::decode(buf, features)?;

        // other_keys was added in v2
        let other_keys = if struct_v >= 2 {
            u32::decode(buf, 0)?
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
        self.server_challenge.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        // Read and validate struct_v
        let struct_v = u8::decode(buf, 0)?;
        if struct_v != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported CephXServerChallenge version: {}",
                struct_v
            )));
        }
        // Read server challenge
        let server_challenge = u64::decode(buf, 0)?;
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
        self.server_challenge.encode(buf, 0)?;
        self.client_challenge.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let server_challenge = u64::decode(buf, 0)?;
        let client_challenge = u64::decode(buf, 0)?;
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
        // Note: Using direct encoding since this is not a Denc impl
        self.request_type.encode(&mut buf, 0).unwrap();
        (self.keys.len() as u32).encode(&mut buf, 0).unwrap();

        for &key in &self.keys {
            key.encode(&mut buf, 0).unwrap();
        }

        (if self.other_keys { 1u8 } else { 0u8 })
            .encode(&mut buf, 0)
            .unwrap();
        buf.freeze()
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        let request_type = u16::decode(&mut data, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to decode request_type: {}", e))
        })?;
        let keys_len = u32::decode(&mut data, 0)
            .map_err(|e| CephXError::ProtocolError(format!("Failed to decode keys_len: {}", e)))?
            as usize;

        if data.remaining() < keys_len * 4 + 1 {
            return Err(CephXError::ProtocolError("Insufficient key data".into()));
        }

        let mut keys = Vec::with_capacity(keys_len);
        for _ in 0..keys_len {
            let key = u32::decode(&mut data, 0)
                .map_err(|e| CephXError::ProtocolError(format!("Failed to decode key: {}", e)))?;
            keys.push(key);
        }

        let other_keys_byte = u8::decode(&mut data, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to decode other_keys: {}", e))
        })?;
        let other_keys = other_keys_byte != 0;

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
        self.status
            .encode(&mut buf, 0)
            .map_err(|e| CephXError::EncodingError(format!("Failed to encode status: {}", e)))?;
        (self.tickets.len() as u32)
            .encode(&mut buf, 0)
            .map_err(|e| {
                CephXError::EncodingError(format!("Failed to encode tickets length: {}", e))
            })?;

        for ticket in &self.tickets {
            let mut ticket_buf = BytesMut::new();
            ticket
                .encode(&mut ticket_buf, 0)
                .map_err(|e| CephXError::EncodingError(e.to_string()))?;
            (ticket_buf.len() as u32).encode(&mut buf, 0).map_err(|e| {
                CephXError::EncodingError(format!("Failed to encode ticket length: {}", e))
            })?;
            buf.extend_from_slice(&ticket_buf);
        }

        match &self.session_key {
            Some(key) => {
                key.encode(&mut buf, 0)
                    .map_err(|e| CephXError::EncodingError(e.to_string()))?;
            }
            None => {
                0u32.encode(&mut buf, 0).map_err(|e| {
                    CephXError::EncodingError(format!("Failed to encode empty session key: {}", e))
                })?;
            }
        }

        Ok(buf.freeze())
    }

    pub fn decode(mut data: &[u8]) -> Result<Self> {
        let status = i32::decode(&mut data, 0)
            .map_err(|e| CephXError::ProtocolError(format!("Failed to decode status: {}", e)))?;
        let tickets_len = u32::decode(&mut data, 0).map_err(|e| {
            CephXError::ProtocolError(format!("Failed to decode tickets_len: {}", e))
        })? as usize;

        let mut tickets = Vec::with_capacity(tickets_len);
        for _ in 0..tickets_len {
            let ticket_len = u32::decode(&mut data, 0).map_err(|e| {
                CephXError::ProtocolError(format!("Failed to decode ticket length: {}", e))
            })? as usize;

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
        1u8.encode(buf, 0)?;
        // global_id
        self.global_id.encode(buf, 0)?;
        // service_id
        self.service_id.encode(buf, 0)?;
        // ticket_blob (includes struct_v, secret_id, blob)
        self.ticket_blob.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        let _struct_v = u8::decode(buf, 0)?;
        let global_id = u64::decode(buf, 0)?;
        let service_id = u32::decode(buf, 0)?;
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
        2u8.encode(buf, 0)?;
        // nonce
        self.nonce.encode(buf, 0)?;
        // have_challenge
        (if self.have_challenge { 1u8 } else { 0u8 }).encode(buf, 0)?;
        // server_challenge_plus_one (always encode, even if 0)
        self.server_challenge_plus_one.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let _struct_v = u8::decode(buf, 0)?;
        let nonce = u64::decode(buf, 0)?;
        let have_challenge_byte = u8::decode(buf, 0)?;
        let have_challenge = have_challenge_byte != 0;
        let server_challenge_plus_one = if have_challenge {
            u64::decode(buf, 0)?
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
        1u8.encode(buf, 0)?;
        // nonce_plus_one
        self.nonce_plus_one.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let _struct_v = u8::decode(buf, 0)?;
        let nonce_plus_one = u64::decode(buf, 0)?;
        Ok(Self { nonce_plus_one })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1 + 8) // struct_v + nonce_plus_one
    }
}
