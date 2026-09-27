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

/// Cryptographic key for CephX authentication
///
/// Corresponds to C++ `CryptoKey` class in `/src/auth/Crypto.h`
///
/// C++ encoding format:
/// - `__u16 type` - Key type (see [`KeyType`])
/// - `utime_t created` - Creation timestamp
/// - `__u16 len` - Secret length
/// - `buffer::ptr secret` - The actual secret key data
///
/// `secret` holds the raw secret, without the encoding header.
#[derive(Clone)]
pub struct CryptoKey {
    pub crypto_type: u16,
    pub created: SystemTime,
    pub secret: Bytes,
}

impl std::fmt::Debug for CryptoKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CryptoKey")
            .field("crypto_type", &self.crypto_type)
            .field("created", &self.created)
            .field("secret", &Redacted(&self.secret))
            .finish()
    }
}

/// Debug-formats secret bytes as their length only, so a key never reaches
/// a log.
pub(crate) struct Redacted<'a>(pub(crate) &'a [u8]);

impl std::fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} bytes redacted>", self.0.len())
    }
}

pub const CEPH_CRYPTO_NONE: u16 = 0x0;
pub const CEPH_CRYPTO_AES: u16 = 0x1;
pub const CEPH_CRYPTO_AES256KRB5: u16 = 0x2;

/// A cephx key type (`CEPH_CRYPTO_*` in `ceph_fs.h`). The variants are in
/// numeric order, so `min` picks the weaker type as the monitor does when it
/// issues a service ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyType {
    None,
    Aes,
    /// `aes256k`: AES256-CTS-HMAC-SHA384-192 (RFC 8009), Ceph v19.2.6+.
    Aes256Krb5,
}

impl KeyType {
    pub const fn as_u16(self) -> u16 {
        match self {
            KeyType::None => CEPH_CRYPTO_NONE,
            KeyType::Aes => CEPH_CRYPTO_AES,
            KeyType::Aes256Krb5 => CEPH_CRYPTO_AES256KRB5,
        }
    }

    /// The per-type secret check of C++ `CryptoHandler::validate_secret`.
    fn validate_secret(self, secret: &[u8]) -> Result<()> {
        use crate::auth::protocol::AES_KEY_LEN;
        match self {
            KeyType::None => Ok(()),
            KeyType::Aes if secret.len() < AES_KEY_LEN => Err(CephXError::InvalidKey(format!(
                "aes secret shorter than {AES_KEY_LEN} bytes ({} bytes)",
                secret.len()
            ))),
            KeyType::Aes => Ok(()),
            KeyType::Aes256Krb5 => crate::auth::aes256krb5::validate_secret(secret),
        }
    }
}

impl TryFrom<u16> for KeyType {
    type Error = CephXError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            CEPH_CRYPTO_NONE => Ok(KeyType::None),
            CEPH_CRYPTO_AES => Ok(KeyType::Aes),
            CEPH_CRYPTO_AES256KRB5 => Ok(KeyType::Aes256Krb5),
            other => Err(CephXError::UnsupportedKeyType(other)),
        }
    }
}

impl CryptoKey {
    /// A key of `key_type` holding `secret`, validated as C++
    /// `CryptoKey::_set_secret` does: an empty secret is accepted with any
    /// type, otherwise the type must be known and the secret long enough.
    pub fn new(key_type: KeyType, secret: Bytes) -> Result<Self> {
        Self::from_parts(key_type.as_u16(), SystemTime::now(), secret)
    }

    /// The empty key of a default-constructed C++ `CryptoKey`.
    pub fn empty() -> Self {
        Self {
            crypto_type: CEPH_CRYPTO_NONE,
            created: UNIX_EPOCH,
            secret: Bytes::new(),
        }
    }

    fn from_parts(crypto_type: u16, created: SystemTime, secret: Bytes) -> Result<Self> {
        if !secret.is_empty() {
            KeyType::try_from(crypto_type)?.validate_secret(&secret)?;
        }
        Ok(Self {
            crypto_type,
            created,
            secret,
        })
    }

    /// Parse a keyring `key = ...` value: the base64 of an encoded key.
    /// Bytes after the encoded key are ignored, as in C++
    /// `CryptoKey::decode_base64`.
    pub fn from_base64(base64_str: &str) -> Result<Self> {
        let data = STANDARD
            .decode(base64_str)
            .map_err(|e| CephXError::InvalidKey(format!("Invalid base64 key: {e}")))?;
        Self::decode_checked(&mut data.as_slice(), 0)
    }

    fn decode_checked<B: Buf>(buf: &mut B, features: u64) -> Result<Self> {
        let crypto_type = u16::decode(buf, 0)?;
        let created = SystemTime::decode(buf, features)?;
        let secret_len = u16::decode(buf, 0)? as usize;
        if buf.remaining() < secret_len {
            return Err(CephXError::InvalidKey(format!(
                "secret length {secret_len} exceeds the {} bytes left",
                buf.remaining()
            )));
        }
        let secret = buf.copy_to_bytes(secret_len);
        Self::from_parts(crypto_type, created, secret)
    }

    pub fn key_type(&self) -> Result<KeyType> {
        KeyType::try_from(self.crypto_type)
    }

    pub fn len(&self) -> usize {
        self.secret.len()
    }

    pub fn is_empty(&self) -> bool {
        self.secret.is_empty()
    }

    fn cipher_type(&self) -> Result<KeyType> {
        if self.secret.is_empty() {
            return Err(CephXError::CryptographicError(format!(
                "empty key of type {}",
                self.crypto_type
            )));
        }
        match self.key_type()? {
            KeyType::None => Err(CephXError::CryptographicError(format!(
                "key type none={CEPH_CRYPTO_NONE} cannot encrypt or decrypt"
            ))),
            t => Ok(t),
        }
    }

    /// AES-128 uses the first 16 bytes of the secret.
    fn aes_key_bytes(&self) -> Result<&[u8]> {
        use crate::auth::protocol::AES_KEY_LEN;
        self.secret.get(..AES_KEY_LEN).ok_or_else(|| {
            CephXError::CryptographicError(format!(
                "aes secret shorter than {AES_KEY_LEN} bytes ({} bytes)",
                self.secret.len()
            ))
        })
    }

    /// Decrypt `ciphertext` under cephx key usage `usage` (a
    /// `CEPHX_KEY_USAGE_*` value, or 0 for C++'s plain `decrypt`). AES keys
    /// ignore the usage.
    pub fn decrypt(&self, usage: u32, ciphertext: &[u8]) -> Result<Bytes> {
        match self.cipher_type()? {
            KeyType::Aes256Krb5 => {
                crate::auth::aes256krb5::decrypt(&self.secret, usage, ciphertext).map(Bytes::from)
            }
            _ => self.aes_decrypt(ciphertext),
        }
    }

    /// Encrypt `plaintext` under cephx key usage `usage` (a
    /// `CEPHX_KEY_USAGE_*` value, or 0 for C++'s plain `encrypt`). AES keys
    /// ignore the usage.
    pub fn encrypt(&self, usage: u32, plaintext: &[u8]) -> Result<Bytes> {
        match self.cipher_type()? {
            KeyType::Aes256Krb5 => {
                crate::auth::aes256krb5::encrypt(&self.secret, usage, plaintext).map(Bytes::from)
            }
            _ => self.aes_encrypt(plaintext),
        }
    }

    /// HMAC-SHA256 keyed with the whole raw secret, whatever the key's type
    /// (C++ `CryptoKey::hmac_sha256`).
    pub fn hmac_sha256(&self, data: &[u8]) -> Result<[u8; 32]> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        if self.secret.is_empty() {
            return Err(CephXError::CryptographicError(format!(
                "empty key of type {}",
                self.crypto_type
            )));
        }
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.secret)
            .map_err(|e| CephXError::CryptographicError(format!("invalid HMAC key: {e}")))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().into())
    }

    fn aes_decrypt(&self, ciphertext: &[u8]) -> Result<Bytes> {
        use aes::Aes128;
        use cbc::Decryptor;
        use cbc::cipher::{BlockDecryptMut, KeyIvInit};

        let key_bytes = self.aes_key_bytes()?;

        use crate::auth::protocol::CEPH_AES_IV;

        type Aes128CbcDec = Decryptor<Aes128>;
        let cipher = Aes128CbcDec::new(key_bytes.into(), CEPH_AES_IV.into());

        let mut buffer = ciphertext.to_vec();
        let pt_len = cipher
            .decrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer)
            .map_err(|e| CephXError::CryptographicError(format!("AES decryption failed: {e:?}")))?
            .len();
        buffer.truncate(pt_len);
        Ok(Bytes::from(buffer))
    }

    fn aes_encrypt(&self, plaintext: &[u8]) -> Result<Bytes> {
        use aes::Aes128;
        use cbc::Encryptor;
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};

        let key_bytes = self.aes_key_bytes()?;

        use crate::auth::protocol::{AES_BLOCK_LEN, CEPH_AES_IV};

        type Aes128CbcEnc = Encryptor<Aes128>;
        let cipher = Aes128CbcEnc::new(key_bytes.into(), CEPH_AES_IV.into());

        let padded_len = ((plaintext.len() / AES_BLOCK_LEN) + 1) * AES_BLOCK_LEN;
        let mut buffer = vec![0u8; padded_len];
        buffer[..plaintext.len()].copy_from_slice(plaintext);

        let ct_len = cipher
            .encrypt_padded_mut::<cbc::cipher::block_padding::Pkcs7>(&mut buffer, plaintext.len())
            .map_err(|e| CephXError::CryptographicError(format!("AES encryption failed: {e:?}")))?
            .len();
        buffer.truncate(ct_len);
        Ok(Bytes::from(buffer))
    }
}

impl Denc for CryptoKey {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        self.crypto_type.encode(buf, 0)?;

        self.created.encode(buf, features)?;

        (self.secret.len() as u16).encode(buf, 0)?;
        buf.put_slice(&self.secret);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        Self::decode_checked(buf, features)
            .map_err(|e| RadosError::Protocol(format!("CryptoKey: {e}")))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(crate::auth::protocol::CRYPTO_KEY_HEADER_SIZE + self.secret.len())
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
    pub blob: Bytes,
}

impl CephXTicketBlob {
    const STRUCT_V: u8 = 1;

    pub fn new(secret_id: u64, blob: Bytes) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            secret_id,
            blob,
        }
    }
}

impl Default for CephXTicketBlob {
    fn default() -> Self {
        Self {
            struct_v: Self::STRUCT_V,
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
            session_key: CryptoKey::empty(),
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
    pub global_id: u64,
    pub session_key: CryptoKey,
    /// Ticket handlers for service tickets (OSD, MDS, etc.)
    pub ticket_handlers: HashMap<EntityType, TicketHandler>,
}

impl CephXSession {
    pub fn new(entity_name: EntityName, global_id: u64, session_key: CryptoKey) -> Self {
        Self {
            entity_name,
            global_id,
            session_key,
            ticket_handlers: HashMap::new(),
        }
    }

    pub fn get_ticket_handler(&mut self, service_type: EntityType) -> &mut TicketHandler {
        self.ticket_handlers
            .entry(service_type)
            .or_insert_with(|| TicketHandler::new(service_type))
    }

    pub fn has_valid_ticket(&self, service_type: EntityType) -> bool {
        self.ticket_handlers
            .get(&service_type)
            .is_some_and(|h| h.ticket_blob.is_some() && !h.is_expired())
    }
}

/// Encoded keys shared by the auth unit tests.
#[cfg(test)]
pub(crate) mod test_keys {
    /// AES, secret `00 01 .. 0f`.
    pub(crate) const AES_TEST_KEY: &str = "AQAAAAAAAAAAABAAAAECAwQFBgcICQoLDA0ODw==";
    /// aes256k, secret the RFC 8009 test key `6D404D37 .. 82460C52`.
    pub(crate) const AES256K_TEST_KEY: &str =
        "AgAAAAAAAAAAACAAbUBNN/r3n53w0zVo0yBmmADrSDZHLqigJtFrcYJGDFI=";
    /// Type 3, which Ceph does not define, with a 32-byte zero secret.
    pub(crate) const TYPE3_TEST_KEY: &str =
        "AwAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    /// aes256k with a 31-byte secret, one byte short.
    pub(crate) const AES256K_31_BYTE_TEST_KEY: &str =
        "AgAAAAAAAAAAAB8AEREREREREREREREREREREREREREREREREREREREREQ==";
    /// The RFC 8009 test key's raw secret.
    pub(crate) const RFC8009_SECRET: &str =
        "6d404d37faf79f9df0d33568d320669800eb4836472ea8a026d16b7182460c52";
}

#[cfg(test)]
mod tests {
    use super::test_keys::*;
    use super::*;
    use serde_json::json;

    fn hx(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap()
    }

    #[test]
    fn parse_aes_key() {
        let key = CryptoKey::from_base64(AES_TEST_KEY).unwrap();
        assert_eq!(key.crypto_type, CEPH_CRYPTO_AES);
        assert_eq!(key.key_type().unwrap(), KeyType::Aes);
        assert_eq!(key.secret.as_ref(), hx("000102030405060708090a0b0c0d0e0f"));
    }

    #[test]
    fn parse_aes256k_key() {
        let key = CryptoKey::from_base64(AES256K_TEST_KEY).unwrap();
        assert_eq!(key.crypto_type, CEPH_CRYPTO_AES256KRB5);
        assert_eq!(key.key_type().unwrap(), KeyType::Aes256Krb5);
        assert_eq!(key.secret.as_ref(), hx(RFC8009_SECRET));
    }

    #[test]
    fn parse_unknown_type_names_it() {
        let err = CryptoKey::from_base64(TYPE3_TEST_KEY).unwrap_err();
        assert!(matches!(err, CephXError::UnsupportedKeyType(3)), "{err:?}");
        assert!(err.to_string().contains('3'), "{err}");
    }

    #[test]
    fn parse_rejects_short_aes256k_secret() {
        let err = CryptoKey::from_base64(AES256K_31_BYTE_TEST_KEY).unwrap_err();
        assert!(err.to_string().contains("shorter than 32 bytes"), "{err}");
    }

    #[test]
    fn parse_accepts_33_byte_aes256k_secret() {
        let key =
            CryptoKey::from_base64("AgAAAAAAAAAAACEAbUBNN/r3n53w0zVo0yBmmADrSDZHLqigJtFrcYJGDFIB")
                .unwrap();
        assert_eq!(key.key_type().unwrap(), KeyType::Aes256Krb5);
        assert_eq!(key.len(), 33);
    }

    #[test]
    fn parse_empty_secret_is_an_empty_key() {
        let key = CryptoKey::from_base64("AgAAAAAAAAAAAAAA").unwrap();
        assert!(key.is_empty());
        assert_eq!(key.crypto_type, CEPH_CRYPTO_AES256KRB5);
    }

    #[test]
    fn parse_ignores_trailing_bytes() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        let mut data = STANDARD.decode(AES_TEST_KEY).unwrap();
        data.extend_from_slice(b"trailing");
        let key = CryptoKey::from_base64(&STANDARD.encode(data)).unwrap();
        assert_eq!(key.secret.as_ref(), hx("000102030405060708090a0b0c0d0e0f"));
    }

    #[test]
    fn denc_decode_validates_the_secret() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        let data = STANDARD.decode(AES256K_31_BYTE_TEST_KEY).unwrap();
        assert!(CryptoKey::decode(&mut data.as_slice(), 0).is_err());

        let data = STANDARD.decode(AES256K_TEST_KEY).unwrap();
        let key = CryptoKey::decode(&mut data.as_slice(), 0).unwrap();
        let mut encoded = Vec::new();
        key.encode(&mut encoded, 0).unwrap();
        assert_eq!(encoded, data);
    }

    #[test]
    fn new_validates_the_secret() {
        assert!(CryptoKey::new(KeyType::Aes, Bytes::from(vec![0u8; 15])).is_err());
        assert!(CryptoKey::new(KeyType::Aes, Bytes::from(vec![0u8; 16])).is_ok());
        assert!(CryptoKey::new(KeyType::Aes256Krb5, Bytes::from(vec![0u8; 31])).is_err());
        assert!(CryptoKey::new(KeyType::Aes256Krb5, Bytes::from(vec![0u8; 32])).is_ok());
        assert!(CryptoKey::new(KeyType::None, Bytes::from(vec![0u8; 3])).is_ok());
        assert!(CryptoKey::new(KeyType::Aes256Krb5, Bytes::new()).is_ok());
    }

    #[test]
    fn key_type_round_trips() {
        for t in [KeyType::None, KeyType::Aes, KeyType::Aes256Krb5] {
            assert_eq!(KeyType::try_from(t.as_u16()).unwrap(), t);
        }
        assert!(KeyType::Aes < KeyType::Aes256Krb5);
    }

    #[test]
    fn empty_and_none_keys_cannot_encrypt() {
        assert!(CryptoKey::empty().encrypt(0, b"x").is_err());
        let none = CryptoKey::new(KeyType::None, Bytes::from(vec![1u8; 16])).unwrap();
        let err = none.encrypt(0, b"x").unwrap_err().to_string();
        assert!(err.contains("none=0"), "{err}");
    }

    // src/test/crypto.cc:42-80@v19.2.6.
    #[test]
    fn aes_is_byte_identical_for_every_usage() {
        let key = CryptoKey::from_base64(AES_TEST_KEY).unwrap();
        let plaintext = hx("00112233445566778899aabbccddeeff");
        let expected = hx("b38f5bc9354cf8c61315666f37d7793a11907be9d83c3570587b979b03d2a501");
        for usage in [0, 0x04, 0x10, 0x30] {
            let ct = key.encrypt(usage, &plaintext).unwrap();
            assert_eq!(ct.as_ref(), expected, "usage 0x{usage:02x}");
            assert_eq!(key.decrypt(usage, &ct).unwrap().as_ref(), plaintext);
        }
    }

    // RFC 8009 vector 2 from src/test/crypto.cc@v19.2.6, usage 2.
    #[test]
    fn aes256k_dispatch() {
        let key = CryptoKey::from_base64(AES256K_TEST_KEY).unwrap();
        let ct = hx(
            "4ED7B37C2BCAC8F74F23C1CF07E62BC7B75FB3F637B9F559C7F664F69EAB7B6092237526EA0D1F61CB20D69D10F2",
        );
        assert_eq!(key.decrypt(2, &ct).unwrap().as_ref(), hx("000102030405"));

        let ct = key.encrypt(0x04, b"round trip").unwrap();
        assert_eq!(ct.len(), 16 + 10 + 24);
        assert_eq!(key.decrypt(0x04, &ct).unwrap().as_ref(), b"round trip");
        assert!(key.decrypt(0x03, &ct).is_err());
    }

    // src/test/crypto.cc:668-718@v19.2.6.
    #[test]
    fn hmac_sha256_ceph_vectors() {
        let key = CryptoKey::new(
            KeyType::Aes,
            Bytes::from(hx("00112233445566778899aabbccddeeff")),
        )
        .unwrap();
        assert_eq!(
            hex::encode(key.hmac_sha256(b"blablabla").unwrap()),
            "42c7027e8be06dca2c0b444373fefdbeac5b4034eca44a69de3a291634ed8df9"
        );
        assert_eq!(
            hex::encode(key.hmac_sha256(b"testing1234blablabla").unwrap()),
            "4bd3ac394acc9706dd09e65c68add4cf092ccda1e799e35c527385bd7973c698"
        );
    }

    // Computed with Python's hmac module.
    #[test]
    fn hmac_sha256_uses_the_whole_aes256k_secret() {
        let key = CryptoKey::from_base64(AES256K_TEST_KEY).unwrap();
        assert_eq!(
            hex::encode(key.hmac_sha256(&[0u8; 16]).unwrap()),
            "480e73b4fe5f14c60d407394f7ca4fa4107aa6ca79de56579e888e622ba72ac2"
        );
        assert!(CryptoKey::empty().hmac_sha256(b"x").is_err());
    }

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

    #[test]
    fn crypto_key_debug_redacts_the_secret() {
        let secret: Vec<u8> = (0xa0..0xc0).collect();
        let key = CryptoKey::new(KeyType::Aes256Krb5, Bytes::from(secret.clone())).unwrap();
        let out = format!("{key:?}");
        let pretty = format!("{key:#?}");
        assert!(out.contains("crypto_type: 2"), "{out}");
        assert!(out.contains("created"), "{out}");
        assert!(out.contains("<32 bytes redacted>"), "{out}");
        let hex: String = secret.iter().map(|b| format!("{b:02x}")).collect();
        for text in [&out, &pretty] {
            let lower = text.to_lowercase();
            // Hex, `Bytes`' escaped form, the byte list, and base64.
            assert!(!lower.contains(&hex[..8]), "{text}");
            assert!(!lower.contains("\\xa0\\xa1"), "{text}");
            assert!(!text.contains("160, 161"), "{text}");
            assert!(!text.contains(&STANDARD.encode(&secret)[..8]), "{text}");
        }
    }
}
