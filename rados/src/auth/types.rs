//! Common types for CephX authentication

use crate::auth::error::{CephXError, Result};
use crate::denc::{Denc, RadosError};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::{Buf, BufMut, Bytes};
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Use the canonical EntityType and EntityName from the rados-denc crate
pub(crate) use crate::EntityName;
pub use crate::EntityType;

/// Global ID type for auth session tracking (raw u64).
type GlobalId = u64;

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

    pub fn len(&self) -> usize {
        self.secret.len()
    }

    pub fn is_empty(&self) -> bool {
        self.secret.is_empty()
    }

    /// Extract the raw 16-byte AES key from the secret.
    ///
    /// Session keys are raw 16-byte keys. Client keys from keyring files
    /// contain a 12-byte header (u16 type + u32 sec + u32 nsec + u16 len)
    /// followed by the 16-byte key.
    pub fn aes_key_bytes(&self) -> Result<&[u8]> {
        use crate::auth::protocol::{AES_KEY_LEN, CRYPTO_KEY_HEADER_SIZE};

        let secret_bytes = &self.secret;
        if secret_bytes.len() == AES_KEY_LEN {
            Ok(secret_bytes)
        } else if secret_bytes.len() >= CRYPTO_KEY_HEADER_SIZE + AES_KEY_LEN {
            Ok(&secret_bytes[CRYPTO_KEY_HEADER_SIZE..CRYPTO_KEY_HEADER_SIZE + AES_KEY_LEN])
        } else {
            Err(CephXError::CryptographicError(format!(
                "Secret key has invalid length: {} bytes (expected 16 or >= 28)",
                secret_bytes.len()
            )))
        }
    }

    /// Decrypt data using AES-128-CBC
    /// This corresponds to CryptoKey::decrypt() in C++
    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Bytes> {
        use aes::Aes128;
        use cbc::Decryptor;
        use cbc::cipher::{BlockDecryptMut, KeyIvInit};

        if self.crypto_type != CEPH_CRYPTO_AES {
            return Err(CephXError::CryptographicError(format!(
                "Unsupported crypto type: {}",
                self.crypto_type
            )));
        }

        let key_bytes = self.aes_key_bytes()?;

        use crate::auth::protocol::CEPH_AES_IV;

        type Aes128CbcDec = Decryptor<Aes128>;
        let cipher = Aes128CbcDec::new(key_bytes.into(), CEPH_AES_IV.into());

        let mut buffer = ciphertext.to_vec();
        let pt_len = cipher
            .decrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer)
            .map_err(|e| CephXError::CryptographicError(format!("AES decryption failed: {:?}", e)))?
            .len();
        buffer.truncate(pt_len);
        Ok(Bytes::from(buffer))
    }

    /// Encrypt data using AES-128-CBC
    /// This corresponds to CryptoKey::encrypt() in C++
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Bytes> {
        use aes::Aes128;
        use cbc::Encryptor;
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};

        if self.crypto_type != CEPH_CRYPTO_AES {
            return Err(CephXError::CryptographicError(format!(
                "Unsupported crypto type: {}",
                self.crypto_type
            )));
        }

        let key_bytes = self.aes_key_bytes()?;

        use crate::auth::protocol::{AES_BLOCK_LEN, CEPH_AES_IV};

        type Aes128CbcEnc = Encryptor<Aes128>;
        let cipher = Aes128CbcEnc::new(key_bytes.into(), CEPH_AES_IV.into());

        // Calculate padded length (must be multiple of AES block size)
        let padded_len = ((plaintext.len() / AES_BLOCK_LEN) + 1) * AES_BLOCK_LEN;
        let mut buffer = vec![0u8; padded_len];
        buffer[..plaintext.len()].copy_from_slice(plaintext);

        let ct_len = cipher
            .encrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer, plaintext.len())
            .map_err(|e| CephXError::CryptographicError(format!("AES encryption failed: {:?}", e)))?
            .len();
        buffer.truncate(ct_len);
        Ok(Bytes::from(buffer))
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
#[derive(Debug, Clone, Serialize, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct CephXTicketBlob {
    #[serde(skip)]
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
}

impl Default for CephXTicketBlob {
    fn default() -> Self {
        Self {
            struct_v: 1u8,
            secret_id: 0,
            blob: Bytes::new(),
        }
    }
}

/// Authentication ticket containing authorization information
///
/// Corresponds to C++ `AuthTicket` struct in `/src/auth/Auth.h`
#[derive(Debug, Clone, crate::StructVDenc)]
#[denc(
    crate = "crate",
    struct_v = 2,
    min_struct_v = 2,
    ceph_release = "Quincy v17+"
)]
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

/// Service ticket information containing authorization ticket and session key
///
/// Corresponds to C++ `CephXServiceTicketInfo` struct in `/src/auth/cephx/CephxProtocol.h`
#[derive(Debug, Clone, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct CephXServiceTicketInfo {
    struct_v: u8,
    pub ticket: AuthTicket,
    pub session_key: CryptoKey,
}

impl CephXServiceTicketInfo {
    const STRUCT_V: u8 = 1;

    pub fn new(ticket: AuthTicket, session_key: CryptoKey) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            ticket,
            session_key,
        }
    }
}

/// Authentication capabilities information
///
/// Corresponds to C++ `AuthCapsInfo` struct in `/src/auth/Auth.h`
#[derive(Debug, Clone, Serialize, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct AuthCapsInfo {
    #[serde(skip)]
    struct_v: u8,
    pub caps: HashMap<String, String>,
}

impl AuthCapsInfo {
    const STRUCT_V: u8 = 1;
}

impl Default for AuthCapsInfo {
    fn default() -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            caps: HashMap::new(),
        }
    }
}

/// Ticket handler for a single service
/// Stores the service-specific session key and ticket information
/// Corresponds to C++ `ceph_x_ticket_handler` in Linux kernel auth_x.h
#[derive(Debug, Clone)]
pub struct TicketHandler {
    pub service: EntityType,
    pub session_key: CryptoKey,
    pub ticket_blob: Option<CephXTicketBlob>,
    pub renew_after: Option<SystemTime>,
    pub expires: Option<SystemTime>,
}

impl TicketHandler {
    pub fn new(service: EntityType) -> Self {
        Self {
            service,
            session_key: CryptoKey::new(Bytes::new()),
            ticket_blob: None,
            renew_after: None,
            expires: None,
        }
    }

    pub fn update(
        &mut self,
        session_key: CryptoKey,
        ticket_blob: CephXTicketBlob,
        validity: Duration,
    ) {
        self.session_key = session_key;
        self.ticket_blob = Some(ticket_blob);

        let now = SystemTime::now();
        self.expires = Some(now + validity);
        // Renew at 75% of validity period (matches Linux kernel auth_x.c:215)
        self.renew_after = Some(now + validity - validity / 4);
    }

    pub fn need_key(&self) -> bool {
        self.ticket_blob.is_none() || self.renew_after.is_some_and(|t| SystemTime::now() >= t)
    }

    pub fn is_expired(&self) -> bool {
        self.expires.is_none_or(|t| SystemTime::now() >= t)
    }
}

/// CephX session containing authentication state
#[derive(Debug, Clone)]
pub struct CephXSession {
    pub entity_name: EntityName,
    pub global_id: GlobalId,
    pub session_key: CryptoKey,
    pub ticket: Option<CephXTicketBlob>,
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
            ticket_handlers: HashMap::new(),
        }
    }

    /// Get or create a ticket handler for a service
    pub fn get_ticket_handler(&mut self, service_type: EntityType) -> &mut TicketHandler {
        self.ticket_handlers
            .entry(service_type)
            .or_insert_with(|| TicketHandler::new(service_type))
    }

    /// Check if we have a valid ticket for a service
    pub fn has_valid_ticket(&self, service_type: EntityType) -> bool {
        self.ticket_handlers
            .get(&service_type)
            .is_some_and(|h| h.ticket_blob.is_some() && !h.is_expired())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cephx_ticket_blob_json_omits_struct_v() {
        let blob = CephXTicketBlob::new(7, Bytes::from_static(b"abc"));
        let value = serde_json::to_value(blob).expect("ticket blob JSON should serialize");

        assert_eq!(
            value,
            json!({
                "secret_id": 7u64,
                "blob": [97, 98, 99]
            })
        );
    }

    #[test]
    fn test_auth_caps_info_json_omits_struct_v() {
        let mut caps = HashMap::new();
        caps.insert("osd".to_string(), "allow rw".to_string());
        let info = AuthCapsInfo { struct_v: 1, caps };
        let value = serde_json::to_value(info).expect("caps info JSON should serialize");

        assert_eq!(
            value,
            json!({
                "caps": {
                    "osd": "allow rw"
                }
            })
        );
    }
}
