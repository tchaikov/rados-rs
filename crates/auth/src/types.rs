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

/// Ceph entity type constants from src/include/msgr.h
/// These identify different types of Ceph daemons
pub mod entity_type {
    /// Monitor daemon
    pub const MON: u32 = 0x01;
    /// Metadata Server
    pub const MDS: u32 = 0x02;
    /// Object Storage Daemon
    pub const OSD: u32 = 0x04;
    /// Client (librados users, ceph-fuse, etc.)
    pub const CLIENT: u32 = 0x08;
    /// Manager daemon
    pub const MGR: u32 = 0x10;
    /// Auth service (for ticket requests)
    pub const AUTH: u32 = 0x20;
}

/// Service ID constants for ticket handlers
/// Used as keys in the ticket_handlers HashMap
pub use entity_type as service_id;

/// Global ID type for Ceph entities
pub type GlobalId = u64;

/// Entity name type
/// Corresponds to C++ `EntityName` struct in `/src/common/entity_name.h`
///
/// C++ encoding format:
/// - `__u32 type` - Entity type (CEPH_ENTITY_TYPE_CLIENT=1, etc.)
/// - `std::string id` - Entity ID (e.g., "admin" for "client.admin")
#[derive(Debug, Clone, PartialEq)]
pub struct EntityName {
    pub entity_type: u32,
    pub id: String,
}

/// Entity type constants from include/msgr.h
pub const CEPH_ENTITY_TYPE_MON: u32 = 0x01;
pub const CEPH_ENTITY_TYPE_MDS: u32 = 0x02;
pub const CEPH_ENTITY_TYPE_OSD: u32 = 0x04;
pub const CEPH_ENTITY_TYPE_CLIENT: u32 = 0x08;
pub const CEPH_ENTITY_TYPE_MGR: u32 = 0x10;

impl EntityName {
    pub fn new(entity_type: u32, id: String) -> Self {
        Self { entity_type, id }
    }

    /// Create a client entity (most common case)
    pub fn client(id: String) -> Self {
        Self::new(CEPH_ENTITY_TYPE_CLIENT, id)
    }

    /// Convert to string format like "client.admin"
    pub fn to_str(&self) -> String {
        let type_str = match self.entity_type {
            CEPH_ENTITY_TYPE_MON => "mon",
            CEPH_ENTITY_TYPE_CLIENT => "client",
            CEPH_ENTITY_TYPE_OSD => "osd",
            CEPH_ENTITY_TYPE_MDS => "mds",
            CEPH_ENTITY_TYPE_MGR => "mgr",
            _ => "unknown",
        };
        format!("{}.{}", type_str, self.id)
    }
}

impl std::fmt::Display for EntityName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

impl std::str::FromStr for EntityName {
    type Err = CephXError;

    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 {
            return Err(CephXError::ProtocolError(format!(
                "Invalid entity name format: {}",
                s
            )));
        }

        let entity_type = match parts[0] {
            "mon" => CEPH_ENTITY_TYPE_MON,
            "client" => CEPH_ENTITY_TYPE_CLIENT,
            "osd" => CEPH_ENTITY_TYPE_OSD,
            "mds" => CEPH_ENTITY_TYPE_MDS,
            "mgr" => CEPH_ENTITY_TYPE_MGR,
            _ => {
                return Err(CephXError::ProtocolError(format!(
                    "Unknown entity type: {}",
                    parts[0]
                )))
            }
        };

        Ok(Self::new(entity_type, parts[1].to_string()))
    }
}

impl Denc for EntityName {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        // Encode type (u32)
        self.entity_type.encode(buf, 0)?;

        // Encode id (string with length prefix)
        (self.id.len() as u32).encode(buf, 0)?;
        buf.put_slice(self.id.as_bytes());

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for EntityName".to_string(),
            ));
        }

        let entity_type = u32::decode(buf, 0)?;

        let id_len = u32::decode(buf, 0)? as usize;
        if buf.remaining() < id_len {
            return Err(RadosError::Protocol(
                "Insufficient bytes for EntityName id".to_string(),
            ));
        }

        let mut id_bytes = vec![0u8; id_len];
        buf.copy_to_slice(&mut id_bytes);
        let id = String::from_utf8(id_bytes)
            .map_err(|e| RadosError::Protocol(format!("Invalid UTF-8 in EntityName: {}", e)))?;

        Ok(Self::new(entity_type, id))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4 + 4 + self.id.len())
    }
}

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
        const CRYPTO_KEY_HEADER_SIZE: usize = std::mem::size_of::<u16>()  // crypto_type
            + std::mem::size_of::<u32>()  // created.sec
            + std::mem::size_of::<u32>()  // created.nsec
            + std::mem::size_of::<u16>(); // secret_length

        let secret_bytes = self.get_secret();

        let key_bytes = if secret_bytes.len() == 16 {
            // Raw 16-byte AES key (e.g., session key from CephXServiceTicket)
            secret_bytes
        } else if secret_bytes.len() >= CRYPTO_KEY_HEADER_SIZE + 16 {
            // Skip the 12-byte header to get to the actual key material
            let actual_key_start = CRYPTO_KEY_HEADER_SIZE;
            &secret_bytes[actual_key_start..actual_key_start + 16]
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
        const CRYPTO_KEY_HEADER_SIZE: usize = std::mem::size_of::<u16>()  // crypto_type
            + std::mem::size_of::<u32>()  // created.sec
            + std::mem::size_of::<u32>()  // created.nsec
            + std::mem::size_of::<u16>(); // secret_length

        let secret_bytes = self.get_secret();

        let key_bytes = if secret_bytes.len() == 16 {
            // Raw 16-byte AES key (e.g., session key from CephXServiceTicket)
            secret_bytes
        } else if secret_bytes.len() >= CRYPTO_KEY_HEADER_SIZE + 16 {
            // Skip the 12-byte header to get to the actual key material
            let actual_key_start = CRYPTO_KEY_HEADER_SIZE;
            &secret_bytes[actual_key_start..actual_key_start + 16]
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

        // Calculate padded length (must be multiple of 16)
        let block_size = 16;
        let padded_len = ((plaintext.len() / block_size) + 1) * block_size;
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
#[derive(Debug, Clone, Serialize)]
pub struct CephXTicketBlob {
    pub secret_id: u64,
    pub blob: Bytes, // Encrypted ticket data
}

impl CephXTicketBlob {
    pub fn new(secret_id: u64, blob: Bytes) -> Self {
        Self { secret_id, blob }
    }

    pub fn empty() -> Self {
        Self {
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

impl Denc for CephXTicketBlob {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        // Encode struct version
        buf.put_u8(1);

        // Encode secret_id
        self.secret_id.encode(buf, 0)?;

        // Encode blob with length prefix
        self.blob.encode(buf, features)?;

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for CephXTicketBlob".to_string(),
            ));
        }

        let _struct_v = buf.get_u8();

        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for secret_id".to_string(),
            ));
        }

        let secret_id = u64::decode(buf, 0)?;
        let blob = Bytes::decode(buf, features)?;

        Ok(Self { secret_id, blob })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(1 + 8 + self.blob.encoded_size(features)?)
    }
}

/// Authentication ticket containing authorization information
///
/// Corresponds to C++ `AuthTicket` struct in `/src/auth/Auth.h`
#[derive(Debug, Clone)]
pub struct AuthTicket {
    pub name: EntityName,
    pub global_id: u64,
    pub created: SystemTime,
    pub expires: SystemTime,
    pub caps: AuthCapsInfo,
    pub flags: u32,
}

impl AuthTicket {
    pub fn new(name: EntityName, global_id: u64) -> Self {
        Self {
            name,
            global_id,
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
        // Encode struct version
        buf.put_u8(2);

        // Encode entity name
        self.name.encode(buf, features)?;

        // Encode global_id
        self.global_id.encode(buf, 0)?;

        // Encode old_auid (always CEPH_AUTH_UID_DEFAULT = -1)
        u64::MAX.encode(buf, 0)?;

        // Encode created time using SystemTime's Denc implementation
        self.created.encode(buf, features)?;

        // Encode expires time using SystemTime's Denc implementation
        self.expires.encode(buf, features)?;

        // Encode caps
        self.caps.encode(buf, features)?;

        // Encode flags
        self.flags.encode(buf, 0)?;

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for AuthTicket".to_string(),
            ));
        }

        let struct_v = buf.get_u8();

        let name = EntityName::decode(buf, features)?;
        let global_id = u64::decode(buf, 0)?;

        // Decode old_auid if struct_v >= 2
        if struct_v >= 2 {
            let _old_auid = u64::decode(buf, 0)?;
        }

        // Decode created time using SystemTime's Denc implementation
        let created = SystemTime::decode(buf, features)?;

        // Decode expires time using SystemTime's Denc implementation
        let expires = SystemTime::decode(buf, features)?;

        let caps = AuthCapsInfo::decode(buf, features)?;
        let flags = u32::decode(buf, 0)?;

        Ok(Self {
            name,
            global_id,
            created,
            expires,
            caps,
            flags,
        })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(
            1 + // struct_v
            self.name.encoded_size(features)? +
            8 + // global_id
            8 + // old_auid
            4 + 4 + // created time
            4 + 4 + // expires time
            self.caps.encoded_size(features)? +
            4, // flags
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
    pub service: u32,
    pub session_key: CryptoKey,
    pub have_key: bool,
    pub secret_id: u64,
    pub ticket_blob: Option<CephXTicketBlob>,
    pub renew_after: Option<SystemTime>,
    pub expires: Option<SystemTime>,
}

impl TicketHandler {
    pub fn new(service: u32) -> Self {
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
        // Renew after half the validity period
        self.renew_after = Some(now + validity / 2);
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
    pub service_tickets: HashMap<u32, CephXTicketBlob>,
    /// Ticket handlers for service tickets (OSD, MDS, etc.)
    pub ticket_handlers: HashMap<u32, TicketHandler>,
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

    pub fn add_service_ticket(&mut self, service_id: u32, ticket: CephXTicketBlob) {
        self.service_tickets.insert(service_id, ticket);
    }

    pub fn get_service_ticket(&self, service_id: u32) -> Option<&CephXTicketBlob> {
        self.service_tickets.get(&service_id)
    }

    /// Get or create a ticket handler for a service
    pub fn get_ticket_handler(&mut self, service_id: u32) -> &mut TicketHandler {
        self.ticket_handlers
            .entry(service_id)
            .or_insert_with(|| TicketHandler::new(service_id))
    }

    /// Check if we have a valid ticket for a service
    pub fn has_valid_ticket(&self, service_id: u32) -> bool {
        if let Some(handler) = self.ticket_handlers.get(&service_id) {
            handler.have_key && !handler.is_expired()
        } else {
            false
        }
    }

    pub fn create_authenticator(&self, service_id: u32) -> CephXAuthenticator {
        CephXAuthenticator::new(self.global_id, service_id)
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
