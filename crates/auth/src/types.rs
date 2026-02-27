//! Common types for CephX authentication

use crate::error::{CephXError, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use denc::{Denc, RadosError};
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::Serialize;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

// Use the canonical EntityType and EntityName from the denc crate
pub(crate) use denc::EntityName;
pub use denc::EntityType;

/// Global ID type for Ceph entities
pub type GlobalId = u64;

/// Cryptographic key for CephX authentication
///
/// Corresponds to C++ `CryptoKey` class in `/src/auth/Crypto.h`
///
/// C++ encoding format:
/// - `__u16 type` - Crypto type (CEPH_CRYPTO_NONE=0x0, CEPH_CRYPTO_AES=0x1)
/// - `utime_t created` - Creation timestamp
/// - `__u16 len` - Secret length
/// - `buffer::ptr secret` - The actual secret key data
#[derive(Debug, Clone)]
pub struct CryptoKey {
    pub crypto_type: u16,
    pub created: SystemTime,
    pub secret: Bytes,
}

/// Crypto type constants from ceph_fs.h
pub const CEPH_CRYPTO_NONE: u16 = 0x0;
pub const CEPH_CRYPTO_AES: u16 = 0x1;

impl CryptoKey {
    pub fn new(secret: Bytes) -> Self {
        Self {
            crypto_type: CEPH_CRYPTO_AES,
            created: SystemTime::now(),
            secret,
        }
    }

    pub fn new_with_type(crypto_type: u16, secret: Bytes) -> Self {
        Self {
            crypto_type,
            created: SystemTime::now(),
            secret,
        }
    }

    pub fn from_base64(base64_str: &str) -> Result<Self> {
        let secret_data = STANDARD
            .decode(base64_str)
            .map_err(|e| CephXError::InvalidKey(format!("Invalid base64 key: {}", e)))?;
        Ok(Self::new(Bytes::from(secret_data)))
    }

    pub fn get_secret(&self) -> &Bytes {
        &self.secret
    }

    pub fn get_type(&self) -> u16 {
        self.crypto_type
    }

    pub fn get_created(&self) -> SystemTime {
        self.created
    }

    pub fn len(&self) -> usize {
        self.secret.len()
    }

    pub fn is_empty(&self) -> bool {
        self.secret.is_empty()
    }

    pub fn sign(&self, message: &[u8]) -> Result<Bytes> {
        let mut mac = HmacSha256::new_from_slice(&self.secret)
            .map_err(|e| CephXError::CryptographicError(format!("HMAC key error: {}", e)))?;
        mac.update(message);
        Ok(Bytes::from(mac.finalize().into_bytes().to_vec()))
    }

    pub fn verify(&self, message: &[u8], signature: &[u8]) -> Result<bool> {
        let expected_sig = self.sign(message)?;
        Ok(expected_sig == signature)
    }

    /// Decrypt data using AES-128-CBC
    /// This corresponds to CryptoKey::decrypt() in C++
    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Bytes> {
        use aes::Aes128;
        use cbc::cipher::{BlockDecryptMut, KeyIvInit};
        use cbc::Decryptor;

        if self.crypto_type != CEPH_CRYPTO_AES {
            return Err(CephXError::CryptographicError(format!(
                "Unsupported crypto type: {}",
                self.crypto_type
            )));
        }

        // Extract the AES key from the secret
        // Session keys from CephXServiceTicket contain just the raw 16-byte key,
        // while client keys contain 12-byte header + 16-byte key
        use crate::protocol::{AES_KEY_LEN, CRYPTO_KEY_HEADER_SIZE};

        let secret_bytes = self.get_secret();

        let key_bytes = if secret_bytes.len() == AES_KEY_LEN {
            // Raw 16-byte AES key (e.g., session key from CephXServiceTicket)
            secret_bytes
        } else if secret_bytes.len() >= CRYPTO_KEY_HEADER_SIZE + AES_KEY_LEN {
            // Skip the 12-byte header to get to the actual key material
            let actual_key_start = CRYPTO_KEY_HEADER_SIZE;
            &secret_bytes[actual_key_start..actual_key_start + AES_KEY_LEN]
        } else {
            return Err(CephXError::CryptographicError(format!(
                "Secret key has invalid length: {} bytes (expected 16 or >= 28)",
                secret_bytes.len()
            )));
        };

        // Use Ceph's IV: "cephsageyudagreg" (16 bytes)
        use crate::protocol::CEPH_AES_IV;

        // Decrypt with AES-128-CBC using Pkcs7 padding
        type Aes128CbcDec = Decryptor<Aes128>;
        let cipher = Aes128CbcDec::new(key_bytes.into(), CEPH_AES_IV.into());

        // Allocate buffer for decryption
        let mut buffer = ciphertext.to_vec();

        let plaintext = cipher
            .decrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer)
            .map_err(|e| {
                CephXError::CryptographicError(format!("AES decryption failed: {:?}", e))
            })?;

        Ok(Bytes::copy_from_slice(plaintext))
    }

    /// Encrypt data using AES-128-CBC
    /// This corresponds to CryptoKey::encrypt() in C++
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Bytes> {
        use aes::Aes128;
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};
        use cbc::Encryptor;

        if self.crypto_type != CEPH_CRYPTO_AES {
            return Err(CephXError::CryptographicError(format!(
                "Unsupported crypto type: {}",
                self.crypto_type
            )));
        }

        // Extract the AES key from the secret
        use crate::protocol::{AES_BLOCK_LEN, AES_KEY_LEN, CRYPTO_KEY_HEADER_SIZE};

        let secret_bytes = self.get_secret();

        let key_bytes = if secret_bytes.len() == AES_KEY_LEN {
            // Raw 16-byte AES key (e.g., session key from CephXServiceTicket)
            secret_bytes
        } else if secret_bytes.len() >= CRYPTO_KEY_HEADER_SIZE + AES_KEY_LEN {
            // Skip the 12-byte header to get to the actual key material
            let actual_key_start = CRYPTO_KEY_HEADER_SIZE;
            &secret_bytes[actual_key_start..actual_key_start + AES_KEY_LEN]
        } else {
            return Err(CephXError::CryptographicError(format!(
                "Secret key has invalid length: {} bytes (expected 16 or >= 28)",
                secret_bytes.len()
            )));
        };

        // Use Ceph's IV: "cephsageyudagreg" (16 bytes)
        use crate::protocol::CEPH_AES_IV;

        // Encrypt with AES-128-CBC using Pkcs7 padding
        type Aes128CbcEnc = Encryptor<Aes128>;
        let cipher = Aes128CbcEnc::new(key_bytes.into(), CEPH_AES_IV.into());

        // Calculate padded length (must be multiple of AES block size)
        let padded_len = ((plaintext.len() / AES_BLOCK_LEN) + 1) * AES_BLOCK_LEN;
        let mut buffer = vec![0u8; padded_len];
        buffer[..plaintext.len()].copy_from_slice(plaintext);

        let ciphertext = cipher
            .encrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer, plaintext.len())
            .map_err(|e| {
                CephXError::CryptographicError(format!("AES encryption failed: {:?}", e))
            })?;

        Ok(Bytes::copy_from_slice(ciphertext))
    }
}

impl Denc for CryptoKey {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        // Encode type (u16)
        self.crypto_type.encode(buf, 0)?;

        // Encode created time using SystemTime's Denc implementation
        self.created.encode(buf, features)?;

        // Encode secret length (u16) + secret data
        (self.secret.len() as u16).encode(buf, 0)?;
        buf.put_slice(&self.secret);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 10 {
            // u16 + 8 (SystemTime) + u16 minimum
            return Err(RadosError::Protocol(
                "Insufficient bytes for CryptoKey".to_string(),
            ));
        }

        let crypto_type = u16::decode(buf, 0)?;

        // Decode created time using SystemTime's Denc implementation
        let created = SystemTime::decode(buf, features)?;

        let secret_len = u16::decode(buf, 0)? as usize;
        if buf.remaining() < secret_len {
            return Err(RadosError::Protocol(
                "Insufficient bytes for secret".to_string(),
            ));
        }

        let mut secret_bytes = vec![0u8; secret_len];
        buf.copy_to_slice(&mut secret_bytes);
        let secret = Bytes::from(secret_bytes);
        Ok(Self {
            crypto_type,
            created,
            secret,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(2 + 4 + 4 + 2 + self.secret.len())
    }
}

impl Serialize for CryptoKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("CryptoKey", 4)?;
        state.serialize_field("crypto_type", &self.crypto_type)?;

        let timestamp = self
            .created
            .duration_since(UNIX_EPOCH)
            .map_err(serde::ser::Error::custom)?;
        state.serialize_field("created_sec", &timestamp.as_secs())?;
        state.serialize_field("created_nsec", &timestamp.subsec_nanos())?;
        state.serialize_field("secret_hex", &hex::encode(&self.secret))?;

        state.end()
    }
}

/// CephX ticket blob for service authorization
///
/// Corresponds to C++ `CephXTicketBlob` struct in `/src/auth/cephx/CephxProtocol.h`
///
/// C++ encoding format:
/// - `__u8 struct_v` - Structure version (currently 1)
/// - `uint64_t secret_id` - Secret/rotating key ID
/// - `buffer::list blob` - Encrypted ticket data
#[derive(Debug, Clone, Serialize, denc::Denc)]
pub struct CephXTicketBlob {
    struct_v: u8,
    pub secret_id: u64,
    pub blob: Bytes, // Encrypted ticket data
}

impl CephXTicketBlob {
    pub fn new(secret_id: u64, blob: Bytes) -> Self {
        let struct_v = 1u8;
        Self {
            struct_v,
            secret_id,
            blob,
        }
    }

    pub fn empty() -> Self {
        Self {
            struct_v: 1u8,
            secret_id: 0,
            blob: Bytes::new(),
        }
    }
}

impl Default for CephXTicketBlob {
    fn default() -> Self {
        Self::empty()
    }
}

/// Authentication ticket containing authorization information
///
/// Corresponds to C++ `AuthTicket` struct in `/src/auth/Auth.h`
#[derive(Debug, Clone)]
pub struct AuthTicket {
    struct_v: u8,
    pub name: EntityName,
    pub global_id: u64,
    old_auid: u64,
    pub created: SystemTime,
    pub expires: SystemTime,
    pub caps: AuthCapsInfo,
    pub flags: u32,
}

impl AuthTicket {
    const STRUCT_V: u8 = 2;
    const AUTH_UID_DEFAULT: u64 = u64::MAX;

    pub fn new(name: EntityName, global_id: u64) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            name,
            global_id,
            old_auid: Self::AUTH_UID_DEFAULT,
            created: SystemTime::now(),
            expires: SystemTime::now(),
            caps: AuthCapsInfo::default(),
            flags: 0,
        }
    }

    pub fn set_validity(&mut self, created_secs: u64, expires_secs: u64) {
        self.created = UNIX_EPOCH + Duration::from_secs(created_secs);
        self.expires = UNIX_EPOCH + Duration::from_secs(expires_secs);
    }
}

impl Denc for AuthTicket {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        self.struct_v.encode(buf, features)?;
        self.name.encode(buf, features)?;
        self.global_id.encode(buf, features)?;
        self.old_auid.encode(buf, features)?;
        self.created.encode(buf, features)?;
        self.expires.encode(buf, features)?;
        self.caps.encode(buf, features)?;
        self.flags.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        let struct_v = u8::decode(buf, features)?;

        // Minimum supported version check (Nautilus v14+)
        denc::check_min_version!(struct_v, 2, "AuthTicket", "Nautilus v14+");

        let name = EntityName::decode(buf, features)?;
        let global_id = u64::decode(buf, features)?;
        // Always decode old_auid (v2+)
        let old_auid = u64::decode(buf, features)?;
        let created = SystemTime::decode(buf, features)?;
        let expires = SystemTime::decode(buf, features)?;
        let caps = AuthCapsInfo::decode(buf, features)?;
        let flags = u32::decode(buf, features)?;

        Ok(Self {
            struct_v,
            name,
            global_id,
            old_auid,
            created,
            expires,
            caps,
            flags,
        })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(
            self.struct_v.encoded_size(features)?
                + self.name.encoded_size(features)?
                + self.global_id.encoded_size(features)?
                + self.old_auid.encoded_size(features)?
                + self.created.encoded_size(features)?
                + self.expires.encoded_size(features)?
                + self.caps.encoded_size(features)?
                + self.flags.encoded_size(features)?,
        )
    }
}

/// Service ticket information containing authorization ticket and session key
///
/// Corresponds to C++ `CephXServiceTicketInfo` struct in `/src/auth/cephx/CephxProtocol.h`
#[derive(Debug, Clone)]
pub struct CephXServiceTicketInfo {
    pub ticket: AuthTicket,
    pub session_key: CryptoKey,
}

impl CephXServiceTicketInfo {
    pub fn new(ticket: AuthTicket, session_key: CryptoKey) -> Self {
        Self {
            ticket,
            session_key,
        }
    }
}

impl Denc for CephXServiceTicketInfo {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        // Encode struct version
        buf.put_u8(1);

        // Encode ticket
        self.ticket.encode(buf, features)?;

        // Encode session_key
        self.session_key.encode(buf, features)?;

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for CephXServiceTicketInfo".to_string(),
            ));
        }

        let _struct_v = buf.get_u8();

        let ticket = AuthTicket::decode(buf, features)?;
        let session_key = CryptoKey::decode(buf, features)?;

        Ok(Self {
            ticket,
            session_key,
        })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(
            1 + // struct_v
            self.ticket.encoded_size(features)? +
            self.session_key.encoded_size(features)?,
        )
    }
}

/// Authentication capabilities information
///
/// Corresponds to C++ `AuthCapsInfo` struct in `/src/auth/Auth.h`
#[derive(Debug, Clone, Default, Serialize)]
pub struct AuthCapsInfo {
    pub caps: HashMap<String, String>,
}

impl Denc for AuthCapsInfo {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        // Encode struct version
        buf.put_u8(1);

        // Encode caps as a map
        (self.caps.len() as u32).encode(buf, 0)?;
        for (key, value) in &self.caps {
            key.encode(buf, features)?;
            value.encode(buf, features)?;
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for AuthCapsInfo".to_string(),
            ));
        }

        let _struct_v = buf.get_u8();

        if buf.remaining() < 4 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for caps count".to_string(),
            ));
        }

        let count = u32::decode(buf, 0)?;
        let mut caps = HashMap::new();

        for _ in 0..count {
            let key = String::decode(buf, features)?;
            let value = String::decode(buf, features)?;
            caps.insert(key, value);
        }

        Ok(Self { caps })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let mut size = 1 + 4; // struct_v + count
        for (key, value) in &self.caps {
            size += key.encoded_size(features)?;
            size += value.encoded_size(features)?;
        }
        Some(size)
    }
}

/// CephX authenticator for service requests
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
}

impl Denc for CephXAuthenticator {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        self.client_challenge.encode(buf, 0)?;
        self.server_challenge.encode(buf, 0)?;
        self.global_id.encode(buf, 0)?;
        self.service_id.encode(buf, 0)?;

        // Encode timestamp as u64 seconds + u32 nanoseconds (12 bytes)
        let timestamp = self
            .timestamp
            .duration_since(UNIX_EPOCH)
            .map_err(|e| RadosError::Protocol(format!("Time error: {}", e)))?;
        timestamp.as_secs().encode(buf, 0)?;
        timestamp.subsec_nanos().encode(buf, 0)?;

        self.nonce.encode(buf, 0)?;

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 44 {
            return Err(RadosError::Protocol(
                "Insufficient authenticator data".to_string(),
            ));
        }

        let client_challenge = u64::decode(buf, 0)?;
        let server_challenge = u64::decode(buf, 0)?;
        let global_id = u64::decode(buf, 0)?;
        let service_id = u32::decode(buf, 0)?;
        let timestamp_secs = u64::decode(buf, 0)?;
        let timestamp_nanos = u32::decode(buf, 0)?;
        let nonce = u64::decode(buf, 0)?;

        let timestamp = UNIX_EPOCH
            + Duration::from_secs(timestamp_secs)
            + Duration::from_nanos(timestamp_nanos as u64);

        Ok(Self {
            client_challenge,
            server_challenge,
            global_id,
            service_id,
            timestamp,
            nonce,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8 + 8 + 8 + 4 + 8 + 4 + 8) // 44 bytes total
    }
}

/// Ticket handler for a single service
/// Stores the service-specific session key and ticket information
/// Corresponds to C++ `ceph_x_ticket_handler` in Linux kernel auth_x.h
#[derive(Debug, Clone)]
pub struct TicketHandler {
    pub service: EntityType,
    pub session_key: CryptoKey,
    pub have_key: bool,
    pub secret_id: u64,
    pub ticket_blob: Option<CephXTicketBlob>,
    pub renew_after: Option<SystemTime>,
    pub expires: Option<SystemTime>,
}

impl TicketHandler {
    pub fn new(service: EntityType) -> Self {
        Self {
            service,
            session_key: CryptoKey::new(Bytes::new()),
            have_key: false,
            secret_id: 0,
            ticket_blob: None,
            renew_after: None,
            expires: None,
        }
    }

    pub fn update(
        &mut self,
        session_key: CryptoKey,
        secret_id: u64,
        ticket_blob: CephXTicketBlob,
        validity: Duration,
    ) {
        self.session_key = session_key;
        self.have_key = true;
        self.secret_id = secret_id;
        self.ticket_blob = Some(ticket_blob);

        let now = SystemTime::now();
        self.expires = Some(now + validity);
        // Renew at 75% of validity period (matches Linux kernel auth_x.c:215)
        self.renew_after = Some(now + validity - validity / 4);
    }

    pub fn need_key(&self) -> bool {
        if !self.have_key {
            return true;
        }
        if let Some(renew_after) = self.renew_after {
            return SystemTime::now() >= renew_after;
        }
        false
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires {
            return SystemTime::now() >= expires;
        }
        true
    }
}

/// CephX session containing authentication state
#[derive(Debug, Clone)]
pub struct CephXSession {
    pub entity_name: EntityName,
    pub global_id: GlobalId,
    pub session_key: CryptoKey,
    pub ticket: Option<CephXTicketBlob>,
    pub service_tickets: HashMap<EntityType, CephXTicketBlob>,
    /// Ticket handlers for service tickets (OSD, MDS, etc.)
    pub ticket_handlers: HashMap<EntityType, TicketHandler>,
}

impl CephXSession {
    pub fn new(entity_name: EntityName, global_id: GlobalId, session_key: CryptoKey) -> Self {
        Self {
            entity_name,
            global_id,
            session_key,
            ticket: None,
            service_tickets: HashMap::new(),
            ticket_handlers: HashMap::new(),
        }
    }

    pub fn add_service_ticket(&mut self, service_type: EntityType, ticket: CephXTicketBlob) {
        self.service_tickets.insert(service_type, ticket);
    }

    pub fn get_service_ticket(&self, service_type: EntityType) -> Option<&CephXTicketBlob> {
        self.service_tickets.get(&service_type)
    }

    /// Get or create a ticket handler for a service
    pub fn get_ticket_handler(&mut self, service_type: EntityType) -> &mut TicketHandler {
        self.ticket_handlers
            .entry(service_type)
            .or_insert_with(|| TicketHandler::new(service_type))
    }

    /// Check if we have a valid ticket for a service
    pub fn has_valid_ticket(&self, service_type: EntityType) -> bool {
        if let Some(handler) = self.ticket_handlers.get(&service_type) {
            handler.have_key && !handler.is_expired()
        } else {
            false
        }
    }

    pub fn create_authenticator(&self, service_type: EntityType) -> CephXAuthenticator {
        CephXAuthenticator::new(self.global_id, service_type.bits())
    }

    pub fn sign_authenticator(&self, auth: &CephXAuthenticator) -> Result<Bytes> {
        use denc::Denc;
        let mut buf = BytesMut::new();
        auth.encode(&mut buf, 0).map_err(|e| {
            CephXError::EncodingError(format!("Failed to encode authenticator: {}", e))
        })?;
        self.session_key.sign(&buf.freeze())
    }
}
