//! Frame encryption/decryption for msgr2 secure mode
//!
//! This module implements AES-128-GCM encryption for msgr2 protocol frames.
//! When connection_mode is SECURE (2), all frames after AUTH_DONE are encrypted.

use bytes::{Bytes, BytesMut};
use std::fmt;

/// Frame encryptor trait - encrypts frame data before sending
pub trait FrameEncryptor: Send + Sync + fmt::Debug {
    /// Encrypt frame data
    fn encrypt(&mut self, plaintext: &[u8]) -> Result<Bytes, CryptoError>;
}

/// Frame decryptor trait - decrypts received frame data
pub trait FrameDecryptor: Send + Sync + fmt::Debug {
    /// Decrypt frame data
    fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Bytes, CryptoError>;
}

/// Crypto errors
#[derive(Debug, Clone)]
pub enum CryptoError {
    /// Encryption failed
    EncryptionFailed(String),
    /// Decryption failed
    DecryptionFailed(String),
    /// Invalid key or nonce
    InvalidParameters(String),
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CryptoError::EncryptionFailed(msg) => write!(f, "Encryption failed: {}", msg),
            CryptoError::DecryptionFailed(msg) => write!(f, "Decryption failed: {}", msg),
            CryptoError::InvalidParameters(msg) => write!(f, "Invalid parameters: {}", msg),
        }
    }
}

impl std::error::Error for CryptoError {}

/// Type alias for parsed connection secret: (key, rx_nonce, tx_nonce)
pub type ConnectionSecret = (Vec<u8>, Vec<u8>, Vec<u8>);

/// Parse connection_secret to extract encryption key and nonces
///
/// Connection secret format (from C++ crypto_onwire.cc):
/// - 16 bytes: AES-128-GCM key
/// - 12 bytes: rx_nonce (for receiving/decryption)
/// - 12 bytes: tx_nonce (for sending/encryption)
/// - Remaining: padding (unused)
pub fn parse_connection_secret(secret: &[u8]) -> Result<ConnectionSecret, CryptoError> {
    const KEY_SIZE: usize = 16;
    const NONCE_SIZE: usize = 12;
    const MIN_SIZE: usize = KEY_SIZE + 2 * NONCE_SIZE;

    if secret.len() < MIN_SIZE {
        return Err(CryptoError::InvalidParameters(format!(
            "Connection secret too short: {} bytes, need at least {}",
            secret.len(),
            MIN_SIZE
        )));
    }

    let key = secret[0..KEY_SIZE].to_vec();
    let rx_nonce = secret[KEY_SIZE..KEY_SIZE + NONCE_SIZE].to_vec();
    let tx_nonce = secret[KEY_SIZE + NONCE_SIZE..KEY_SIZE + 2 * NONCE_SIZE].to_vec();

    tracing::debug!(
        "Parsed connection_secret: key={} bytes, rx_nonce={} bytes, tx_nonce={} bytes",
        key.len(),
        rx_nonce.len(),
        tx_nonce.len()
    );

    Ok((key, rx_nonce, tx_nonce))
}

/// AES-128-GCM frame decryptor
#[derive(Debug)]
pub struct Aes128GcmDecryptor {
    key: Vec<u8>,
    nonce: Vec<u8>,
    sequence: u64,
}

impl Aes128GcmDecryptor {
    /// Create a new decryptor with the given key and nonce
    pub fn new(key: Vec<u8>, nonce: Vec<u8>) -> Result<Self, CryptoError> {
        if key.len() != 16 {
            return Err(CryptoError::InvalidParameters(format!(
                "Invalid key length: {} bytes, expected 16",
                key.len()
            )));
        }
        if nonce.len() != 12 {
            return Err(CryptoError::InvalidParameters(format!(
                "Invalid nonce length: {} bytes, expected 12",
                nonce.len()
            )));
        }

        Ok(Self {
            key,
            nonce,
            sequence: 0,
        })
    }
}

impl FrameDecryptor for Aes128GcmDecryptor {
    fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Bytes, CryptoError> {
        use aes_gcm::{
            aead::{Aead, KeyInit},
            Aes128Gcm, Nonce,
        };

        tracing::info!("🔐 RX Decrypting with sequence: {}", self.sequence);
        tracing::info!(
            "RX ciphertext ({} bytes): {:02x?}",
            ciphertext.len(),
            &ciphertext[..ciphertext.len().min(64)]
        );

        // Build nonce from base nonce + sequence number
        // Nonce structure (12 bytes):
        // - bytes 0-3: fixed field (little-endian u32)
        // - bytes 4-11: counter field (little-endian u64)
        let mut full_nonce = BytesMut::with_capacity(12);
        full_nonce.extend_from_slice(&self.nonce);

        // Add sequence number to the counter field (last 8 bytes)
        // This matches Ceph's: nonce.counter = nonce.counter + sequence
        let counter_bytes = &mut full_nonce[4..12];
        let mut counter = u64::from_le_bytes(counter_bytes.try_into().unwrap());
        counter = counter.wrapping_add(self.sequence);
        counter_bytes.copy_from_slice(&counter.to_le_bytes());

        let cipher = Aes128Gcm::new_from_slice(&self.key)
            .map_err(|e| CryptoError::InvalidParameters(format!("Invalid key: {:?}", e)))?;

        let nonce_array = Nonce::from_slice(&full_nonce);
        tracing::info!("RX nonce: {:02x?}", &full_nonce[..]);

        let plaintext = cipher.decrypt(nonce_array, ciphertext).map_err(|e| {
            CryptoError::DecryptionFailed(format!(
                "AES-GCM decrypt failed at sequence {}: {:?}",
                self.sequence, e
            ))
        })?;

        // Increment sequence AFTER decryption (for next frame)
        // Ceph increments in reset_rx_handler which is called before the next decryption
        self.sequence += 1;

        tracing::debug!(
            "Decrypted frame: seq={}, input={} bytes, output={} bytes",
            self.sequence - 1,
            ciphertext.len(),
            plaintext.len()
        );

        Ok(Bytes::from(plaintext))
    }
}

/// AES-128-GCM frame encryptor
#[derive(Debug)]
pub struct Aes128GcmEncryptor {
    key: Vec<u8>,
    nonce: Vec<u8>,
    sequence: u64,
}

impl Aes128GcmEncryptor {
    /// Create a new encryptor with the given key and nonce
    pub fn new(key: Vec<u8>, nonce: Vec<u8>) -> Result<Self, CryptoError> {
        if key.len() != 16 {
            return Err(CryptoError::InvalidParameters(format!(
                "Invalid key length: {} bytes, expected 16",
                key.len()
            )));
        }
        if nonce.len() != 12 {
            return Err(CryptoError::InvalidParameters(format!(
                "Invalid nonce length: {} bytes, expected 12",
                nonce.len()
            )));
        }

        Ok(Self {
            key,
            nonce,
            sequence: 0,
        })
    }
}

impl FrameEncryptor for Aes128GcmEncryptor {
    fn encrypt(&mut self, plaintext: &[u8]) -> Result<Bytes, CryptoError> {
        use aes_gcm::{
            aead::{Aead, KeyInit},
            Aes128Gcm, Nonce,
        };

        tracing::info!("🔐 TX Encrypting with sequence: {}", self.sequence);

        // Build nonce from base nonce + sequence number
        // Nonce structure (12 bytes):
        // - bytes 0-3: fixed field (little-endian u32)
        // - bytes 4-11: counter field (little-endian u64)
        let mut full_nonce = BytesMut::with_capacity(12);
        full_nonce.extend_from_slice(&self.nonce);

        // Add sequence number to the counter field (last 8 bytes)
        // This matches Ceph's: nonce.counter = nonce.counter + sequence
        let counter_bytes = &mut full_nonce[4..12];
        let mut counter = u64::from_le_bytes(counter_bytes.try_into().unwrap());
        counter = counter.wrapping_add(self.sequence);
        counter_bytes.copy_from_slice(&counter.to_le_bytes());

        let cipher = Aes128Gcm::new_from_slice(&self.key)
            .map_err(|e| CryptoError::InvalidParameters(format!("Invalid key: {:?}", e)))?;

        let nonce_array = Nonce::from_slice(&full_nonce);

        let ciphertext = cipher.encrypt(nonce_array, plaintext).map_err(|e| {
            CryptoError::EncryptionFailed(format!("AES-GCM encrypt failed: {:?}", e))
        })?;

        // Increment sequence AFTER encryption (for next frame)
        // Ceph increments in reset_tx_handler which is called before the next encryption
        self.sequence += 1;

        tracing::debug!(
            "Encrypted frame: seq={}, input={} bytes, output={} bytes",
            self.sequence - 1,
            plaintext.len(),
            ciphertext.len()
        );

        Ok(Bytes::from(ciphertext))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_connection_secret() {
        // Create a 64-byte connection secret
        let mut secret = vec![0u8; 64];
        // Set key bytes
        secret[0..16].copy_from_slice(&[1u8; 16]);
        // Set rx_nonce bytes
        secret[16..28].copy_from_slice(&[2u8; 12]);
        // Set tx_nonce bytes
        secret[28..40].copy_from_slice(&[3u8; 12]);

        let (key, rx_nonce, tx_nonce) = parse_connection_secret(&secret).unwrap();

        assert_eq!(key.len(), 16);
        assert_eq!(rx_nonce.len(), 12);
        assert_eq!(tx_nonce.len(), 12);
        assert_eq!(key, vec![1u8; 16]);
        assert_eq!(rx_nonce, vec![2u8; 12]);
        assert_eq!(tx_nonce, vec![3u8; 12]);
    }

    #[test]
    fn test_parse_connection_secret_too_short() {
        let secret = vec![0u8; 30]; // Too short
        let result = parse_connection_secret(&secret);
        assert!(result.is_err());
    }

    #[test]
    fn test_aes_gcm_roundtrip() {
        let key = vec![0x42u8; 16];
        let nonce = vec![0x13u8; 12];

        let mut encryptor = Aes128GcmEncryptor::new(key.clone(), nonce.clone()).unwrap();
        let mut decryptor = Aes128GcmDecryptor::new(key, nonce).unwrap();

        let plaintext = b"Hello, World!";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();
        let decrypted = decryptor.decrypt(&ciphertext).unwrap();

        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn test_aes_gcm_sequence_numbers() {
        let key = vec![0x42u8; 16];
        let nonce = vec![0x13u8; 12];

        let mut encryptor = Aes128GcmEncryptor::new(key.clone(), nonce.clone()).unwrap();
        let mut decryptor = Aes128GcmDecryptor::new(key, nonce).unwrap();

        // Encrypt and decrypt multiple messages
        for i in 0..5 {
            let plaintext = format!("Message {}", i);
            let ciphertext = encryptor.encrypt(plaintext.as_bytes()).unwrap();
            let decrypted = decryptor.decrypt(&ciphertext).unwrap();
            assert_eq!(&decrypted[..], plaintext.as_bytes());
        }
    }
}
