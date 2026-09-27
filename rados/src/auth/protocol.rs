//! CephX protocol structures and constants

use crate::auth::types::{CephXTicketBlob, CryptoKey};
use crate::denc::{Denc, RadosError};
use bytes::{Buf, BufMut, Bytes};
use serde::Serialize;

/// Authentication protocol identifiers (from ceph_fs.h)
pub const CEPH_AUTH_NONE: u32 = 0x1;
pub const CEPH_AUTH_CEPHX: u32 = 0x2;
pub const CEPH_AUTH_GSS: u32 = 0x4;

/// CephX request types
pub const CEPHX_GET_AUTH_SESSION_KEY: u16 = 0x0100;
pub const CEPHX_GET_PRINCIPAL_SESSION_KEY: u16 = 0x0200;

/// Cephx key usages (`CephxProtocol.h`). The aes256k cipher derives its keys
/// per usage; AES ignores it. C++'s plain `encrypt`/`decrypt` is usage 0.
pub const CEPHX_KEY_USAGE_AUTH_CONNECTION_SECRET: u32 = 0x03;
pub const CEPHX_KEY_USAGE_TICKET_SESSION_KEY: u32 = 0x04;
pub const CEPHX_KEY_USAGE_TICKET_BLOB: u32 = 0x05;
pub const CEPHX_KEY_USAGE_AUTHORIZE: u32 = 0x10;
pub const CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE: u32 = 0x11;
pub const CEPHX_KEY_USAGE_AUTHORIZE_REPLY: u32 = 0x12;
pub const CEPHX_KEY_USAGE_ROTATING_SECRET: u32 = 0x20;
pub const CEPHX_KEY_USAGE_TICKET_INFO: u32 = 0x30;

/// msgr2 connection modes (`CEPH_CON_MODE_*`, ceph_fs.h)
pub const CEPH_CON_MODE_CRC: u32 = 0x1;
pub const CEPH_CON_MODE_SECURE: u32 = 0x2;

/// Shortest connection secret SECURE mode can key from: an AES-128-GCM key
/// and two 12-byte nonces, which C++ `rxtx_t::create_handler_pair` asserts.
pub const CONNECTION_SECRET_MIN_LEN: usize = 16 + 2 * 12;

/// Connection secret length a server generates for a connection mode, as C++
/// `AuthConnectionMeta::get_connection_secret_length`: four 16-byte blocks
/// for SECURE, none for CRC.
pub fn connection_secret_len(con_mode: u32) -> usize {
    match con_mode {
        CEPH_CON_MODE_SECURE => 16 * 4,
        _ => 0,
    }
}

/// A SECURE-mode connection secret: the given one, or an error when it is
/// missing or too short to key the connection. Ceph never continues a SECURE
/// connection without one.
pub fn require_secure_connection_secret(secret: Option<Bytes>) -> crate::auth::Result<Bytes> {
    match secret {
        Some(secret) if secret.len() >= CONNECTION_SECRET_MIN_LEN => Ok(secret),
        Some(secret) => Err(crate::auth::CephXError::ProtocolError(format!(
            "SECURE mode connection secret is {} bytes, need at least {CONNECTION_SECRET_MIN_LEN}",
            secret.len()
        ))),
        None => Err(crate::auth::CephXError::ProtocolError(
            "SECURE mode without a connection secret".into(),
        )),
    }
}

/// AES-128 key length in bytes
pub const AES_KEY_LEN: usize = 16;
/// AES block size in bytes
pub const AES_BLOCK_LEN: usize = 16;
/// Size of the CryptoKey header: u16 type + u32 created.sec + u32 created.nsec + u16 secret_len
pub const CRYPTO_KEY_HEADER_SIZE: usize = std::mem::size_of::<u16>()
    + std::mem::size_of::<u32>()
    + std::mem::size_of::<u32>()
    + std::mem::size_of::<u16>();
/// Maximum number of extra tickets to decode
pub const MAX_EXTRA_TICKETS: usize = 16;

/// CephX service ticket request structure
///
/// Corresponds to C++ `struct CephXServiceTicketRequest` in `/src/auth/cephx/CephxProtocol.h`
///
/// C++ encoding format:
/// - `__u8 struct_v` - Structure version (currently 1)
/// - `uint32_t keys` - Bitmask of service types (MON|OSD|MDS|MGR)
///
/// This structure is used to request service tickets from the monitor.
#[derive(Debug, Clone, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct CephXServiceTicketRequest {
    struct_v: u8,
    pub keys: u32,
}

impl CephXServiceTicketRequest {
    const STRUCT_V: u8 = 1;
    pub fn new(keys: u32) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            keys,
        }
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
#[derive(Debug, Clone, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct CephXServiceTicket {
    struct_v: u8,
    pub session_key: CryptoKey,
    pub validity: std::time::Duration,
}

impl CephXServiceTicket {
    const STRUCT_V: u8 = 1;

    pub fn new(session_key: CryptoKey, validity: std::time::Duration) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            session_key,
            validity,
        }
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
#[derive(Debug, Clone, crate::Denc)]
#[denc(crate = "crate")]
pub struct EncryptedServiceTicket {
    pub version: u8,
    pub encrypted_data: Bytes,
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
/// - `ticket_blob` - The ticket blob for the service, see [`TicketBlobField`]
#[derive(Debug, Clone)]
pub struct ServiceTicketInfo {
    pub service_id: u32,
    pub encrypted_service_ticket: EncryptedServiceTicket,
    pub ticket_enc: u8,
    pub ticket_blob: TicketBlobField,
}

/// The ticket blob of a [`ServiceTicketInfo`], a length-prefixed buffer.
#[derive(Debug, Clone)]
pub enum TicketBlobField {
    /// `ticket_enc == 0`: an encoded [`CephXTicketBlob`].
    Clear(CephXTicketBlob),
    /// `ticket_enc != 0`: the encoded blob in an encrypted envelope, under
    /// usage `CEPHX_KEY_USAGE_TICKET_BLOB` and the session key the client held
    /// for the service before this reply. Only that client can open it.
    Encrypted(Bytes),
}

impl Denc for ServiceTicketInfo {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        self.service_id.encode(buf, 0)?;
        self.encrypted_service_ticket.encode(buf, features)?;
        self.ticket_enc.encode(buf, 0)?;

        match &self.ticket_blob {
            TicketBlobField::Clear(blob) => {
                // C++ encode(bufferlist, bl): outer length prefix around the ticket blob
                let mut temp_buf =
                    bytes::BytesMut::with_capacity(blob.encoded_size(features).unwrap_or(64));
                blob.encode(&mut temp_buf, features)?;
                temp_buf.freeze().encode(buf, 0)?;
            }
            TicketBlobField::Encrypted(ciphertext) => ciphertext.encode(buf, 0)?,
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        let service_id = u32::decode(buf, 0)?;
        let encrypted_service_ticket = EncryptedServiceTicket::decode(buf, features)?;
        let ticket_enc = u8::decode(buf, 0)?;

        let ticket_blob_bytes = Bytes::decode(buf, 0)?;
        let ticket_blob = if ticket_enc == 0 {
            TicketBlobField::Clear(CephXTicketBlob::decode(
                &mut ticket_blob_bytes.as_ref(),
                features,
            )?)
        } else {
            TicketBlobField::Encrypted(ticket_blob_bytes)
        };

        Ok(Self {
            service_id,
            encrypted_service_ticket,
            ticket_enc,
            ticket_blob,
        })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let blob_len = match &self.ticket_blob {
            TicketBlobField::Clear(blob) => blob.encoded_size(features)?,
            TicketBlobField::Encrypted(ciphertext) => ciphertext.len(),
        };
        Some(
            4 + // service_id
            self.encrypted_service_ticket.encoded_size(features)? +
            1 + // ticket_enc
            4 + // outer length prefix
            blob_len,
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
#[derive(Debug, Clone, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct ServiceTicketReply {
    struct_v: u8,
    pub tickets: Vec<ServiceTicketInfo>,
}

/// Authentication mode for different Ceph services
/// From src/auth/Auth.h
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum AuthMode {
    /// No authentication
    None = 0,
    /// Authorizer mode - used for OSDs, MDSs, MGRs, and other data services
    Authorizer = 1,
    /// Monitor mode - used specifically for monitor connections
    Mon = 10,
}

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
                "Invalid magic: expected 0x{AUTH_ENC_MAGIC:016x}, got 0x{magic:016x}"
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
#[derive(Debug, Clone, Serialize, crate::Denc)]
#[denc(crate = "crate")]
pub struct CephXRequestHeader {
    pub request_type: u16,
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
#[derive(Debug, Clone, Serialize, crate::Denc)]
#[denc(crate = "crate")]
pub struct CephXResponseHeader {
    pub request_type: u16,
    pub status: i32,
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
#[derive(Debug, Clone, Serialize, crate::StructVDenc)]
#[denc(
    crate = "crate",
    struct_v = 3,
    min_struct_v = 3,
    ceph_release = "Quincy v17+"
)]
pub struct CephXAuthenticate {
    #[serde(skip)]
    struct_v: u8,
    pub client_challenge: u64,
    pub key: u64,
    pub old_ticket: CephXTicketBlob,
    pub other_keys: u32,
}

impl CephXAuthenticate {
    const STRUCT_V: u8 = 3;
    pub fn new(
        client_challenge: u64,
        key: u64,
        old_ticket: CephXTicketBlob,
        other_keys: u32,
    ) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            client_challenge,
            key,
            old_ticket,
            other_keys,
        }
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
#[derive(Debug, Clone, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct CephXServerChallenge {
    struct_v: u8,
    pub server_challenge: u64,
}

impl CephXServerChallenge {
    const STRUCT_V: u8 = 1;

    pub fn new(server_challenge: u64) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            server_challenge,
        }
    }
}

/// CephX challenge blob for session key calculation
///
/// Corresponds to C++ `struct CephXChallengeBlob` in `/src/auth/cephx/CephxProtocol.h`
///
/// This structure is used in the challenge-response authentication
/// to calculate the session key from server and client challenges.
#[derive(Debug, Clone, crate::Denc)]
#[denc(crate = "crate")]
pub struct CephXChallengeBlob {
    pub server_challenge: u64,
    pub client_challenge: u64,
}

/// CephX Authorize A structure
/// Corresponds to C++ `struct ceph_x_authorize_a` in auth_x_protocol.h
///
/// This is the first part of the authorizer sent to a service (OSD, MDS, etc.)
/// Contains the service ticket obtained from the monitor
#[derive(Debug, Clone, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct CephXAuthorizeA {
    struct_v: u8,
    pub global_id: u64,
    pub service_id: u32,
    pub ticket_blob: CephXTicketBlob,
}

impl CephXAuthorizeA {
    const STRUCT_V: u8 = 1;

    pub fn new(global_id: u64, service_id: u32, ticket_blob: CephXTicketBlob) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            global_id,
            service_id,
            ticket_blob,
        }
    }
}

/// CephX Authorize B structure
/// Corresponds to C++ `struct ceph_x_authorize_b` in auth_x_protocol.h
///
/// This is the second part of the authorizer (encrypted with session key)
/// Contains a nonce and optionally a server challenge response
#[derive(Debug, Clone, crate::StructVDenc)]
#[denc(
    crate = "crate",
    struct_v = 2,
    min_struct_v = 2,
    ceph_release = "Quincy v17+"
)]
pub struct CephXAuthorizeB {
    struct_v: u8,
    pub nonce: u64,
    pub have_challenge: bool,
    pub server_challenge_plus_one: u64,
}

impl CephXAuthorizeB {
    const STRUCT_V: u8 = 2;

    pub fn new(nonce: u64) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            nonce,
            have_challenge: false,
            server_challenge_plus_one: 0,
        }
    }

    pub fn with_challenge(nonce: u64, server_challenge: u64) -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            nonce,
            have_challenge: true,
            server_challenge_plus_one: server_challenge.wrapping_add(1),
        }
    }
}

/// CephX Authorize Reply structure
/// Corresponds to C++ `struct ceph_x_authorize_reply` in auth_x_protocol.h
///
/// Sent by the service back to the client after validating the authorizer.
/// struct_v >= 2 includes connection_secret for SECURE mode.
#[derive(Clone)]
pub struct CephXAuthorizeReply {
    pub nonce_plus_one: u64,
    pub connection_secret: Option<Bytes>,
}

impl std::fmt::Debug for CephXAuthorizeReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::auth::types::Redacted;
        f.debug_struct("CephXAuthorizeReply")
            .field("nonce_plus_one", &self.nonce_plus_one)
            .field(
                "connection_secret",
                &self.connection_secret.as_deref().map(Redacted),
            )
            .finish()
    }
}

impl CephXAuthorizeReply {
    pub fn new(nonce_plus_one: u64) -> Self {
        Self {
            nonce_plus_one,
            connection_secret: None,
        }
    }

    pub fn with_connection_secret(nonce_plus_one: u64, connection_secret: Bytes) -> Self {
        Self {
            nonce_plus_one,
            connection_secret: Some(connection_secret),
        }
    }
}

impl Denc for CephXAuthorizeReply {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        if self.connection_secret.is_some() {
            2u8.encode(buf, 0)?;
        } else {
            1u8.encode(buf, 0)?;
        }
        self.nonce_plus_one.encode(buf, 0)?;
        if let Some(ref secret) = self.connection_secret {
            secret.encode(buf, 0)?;
        }
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let struct_v = u8::decode(buf, 0)?;
        let nonce_plus_one = u64::decode(buf, 0)?;
        let connection_secret = if struct_v >= 2 {
            let secret = Bytes::decode(buf, 0)?;
            if secret.is_empty() {
                None
            } else {
                Some(secret)
            }
        } else {
            None
        };
        Ok(Self {
            nonce_plus_one,
            connection_secret,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        let base = 1 + 8; // struct_v + nonce_plus_one
        // + length prefix + data when connection_secret is present
        Some(
            self.connection_secret
                .as_ref()
                .map_or(base, |s| base + 4 + s.len()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::types::test_keys::AES_TEST_KEY;
    use bytes::BytesMut;
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn test_service_ticket_request_encode_decode() {
        let request = CephXServiceTicketRequest::new(0x12345678);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.len(), 5); // 1 byte struct_v + 4 bytes keys

        let mut read_buf = buf.freeze();
        let decoded = CephXServiceTicketRequest::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.keys, 0x12345678);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn authorize_reply_debug_redacts_the_connection_secret() {
        let reply = CephXAuthorizeReply::with_connection_secret(7, Bytes::from_static(&[0xab; 64]));
        let out = format!("{reply:?}");
        assert!(out.contains("nonce_plus_one: 7"), "{out}");
        assert!(out.contains("<64 bytes redacted>"), "{out}");
        assert!(!out.to_lowercase().contains("xab"), "{out}");
        assert!(!out.contains("171"), "{out}");
    }

    #[test]
    fn test_service_ticket_encode_decode() {
        let key = CryptoKey::from_base64(AES_TEST_KEY).unwrap();
        let validity = Duration::from_secs(3600);
        let ticket = CephXServiceTicket::new(key.clone(), validity);

        let mut buf = BytesMut::new();
        ticket.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CephXServiceTicket::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.session_key.len(), key.len());
        assert_eq!(decoded.validity, validity);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_encrypted_service_ticket_encode_decode() {
        let encrypted_data = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let ticket = EncryptedServiceTicket {
            version: 1,
            encrypted_data: encrypted_data.clone(),
        };

        let mut buf = BytesMut::new();
        ticket.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = EncryptedServiceTicket::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.encrypted_data, encrypted_data);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_service_ticket_info_encode_decode() {
        let encrypted_data = Bytes::from(vec![1, 2, 3, 4]);
        let ticket_blob_data = Bytes::from(vec![5, 6, 7, 8]);
        let info = ServiceTicketInfo {
            service_id: 4, // OSD
            encrypted_service_ticket: EncryptedServiceTicket {
                version: 1,
                encrypted_data: encrypted_data.clone(),
            },
            ticket_enc: 0,
            ticket_blob: TicketBlobField::Clear(CephXTicketBlob::new(42, ticket_blob_data.clone())),
        };

        let mut buf = BytesMut::new();
        info.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), info.encoded_size(0).unwrap());

        let mut read_buf = buf.freeze();
        let decoded = ServiceTicketInfo::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.service_id, 4);
        assert_eq!(decoded.encrypted_service_ticket.version, 1);
        assert_eq!(
            decoded.encrypted_service_ticket.encrypted_data,
            encrypted_data
        );
        assert_eq!(decoded.ticket_enc, 0);
        let TicketBlobField::Clear(blob) = decoded.ticket_blob else {
            panic!("clear ticket blob decoded as encrypted");
        };
        assert_eq!(blob.secret_id, 42);
        assert_eq!(blob.blob, ticket_blob_data);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_service_ticket_info_keeps_an_encrypted_blob() {
        let ciphertext = Bytes::from(vec![9u8; 48]);
        let info = ServiceTicketInfo {
            service_id: 32, // AUTH
            encrypted_service_ticket: EncryptedServiceTicket {
                version: 1,
                encrypted_data: Bytes::from(vec![1, 2, 3, 4]),
            },
            ticket_enc: 1,
            ticket_blob: TicketBlobField::Encrypted(ciphertext.clone()),
        };

        let mut buf = BytesMut::new();
        info.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), info.encoded_size(0).unwrap());

        let mut read_buf = buf.freeze();
        let decoded = ServiceTicketInfo::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.ticket_enc, 1);
        let TicketBlobField::Encrypted(bytes) = decoded.ticket_blob else {
            panic!("encrypted ticket blob decoded as clear");
        };
        assert_eq!(bytes, ciphertext);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_service_ticket_reply_encode_decode() {
        let encrypted_data = Bytes::from(vec![1, 2, 3, 4]);
        let ticket_blob_data = Bytes::from(vec![5, 6, 7, 8]);
        let info = ServiceTicketInfo {
            service_id: 4,
            encrypted_service_ticket: EncryptedServiceTicket {
                version: 1,
                encrypted_data: encrypted_data.clone(),
            },
            ticket_enc: 0,
            ticket_blob: TicketBlobField::Clear(CephXTicketBlob::new(42, ticket_blob_data.clone())),
        };

        let reply = ServiceTicketReply {
            struct_v: 1,
            tickets: vec![info],
        };

        let mut buf = BytesMut::new();
        reply.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = ServiceTicketReply::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.struct_v, 1);
        assert_eq!(decoded.tickets.len(), 1);
        assert_eq!(decoded.tickets[0].service_id, 4);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_auth_mode_conversions() {
        assert_eq!(u8::from(AuthMode::Mon), 10);
        assert_eq!(u8::from(AuthMode::Authorizer), 1);
        assert_eq!(u8::from(AuthMode::None), 0);

        assert_eq!(AuthMode::try_from(10).ok(), Some(AuthMode::Mon));
        assert_eq!(AuthMode::try_from(1).ok(), Some(AuthMode::Authorizer));
        assert_eq!(AuthMode::try_from(0).ok(), Some(AuthMode::None));
        assert!(AuthMode::try_from(99u8).is_err());
    }

    #[test]
    fn test_cephx_encrypted_envelope_encode_decode() {
        // Test with Bytes payload (more appropriate for encrypted data)
        let payload = Bytes::from(vec![1, 2, 3, 4, 5]);
        let envelope = CephXEncryptedEnvelope {
            payload: payload.clone(),
        };

        let mut buf = BytesMut::new();
        envelope.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CephXEncryptedEnvelope::<Bytes>::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.payload, Bytes::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_request_header_encode_decode() {
        let header = CephXRequestHeader {
            request_type: CEPHX_GET_AUTH_SESSION_KEY,
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.len(), 2); // u16

        let mut read_buf = buf.freeze();
        let decoded = CephXRequestHeader::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.request_type, CEPHX_GET_AUTH_SESSION_KEY);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_response_header_encode_decode() {
        let header = CephXResponseHeader {
            request_type: CEPHX_GET_AUTH_SESSION_KEY,
            status: 0,
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.len(), 6); // u16 + i32

        let mut read_buf = buf.freeze();
        let decoded = CephXResponseHeader::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.request_type, CEPHX_GET_AUTH_SESSION_KEY);
        assert_eq!(decoded.status, 0);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_authenticate_encode_decode() {
        let auth = CephXAuthenticate::new(
            0x1234567890abcdef,
            0xabcd,
            CephXTicketBlob::default(),
            0x0F, // Request all services
        );

        let mut buf = BytesMut::new();
        auth.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CephXAuthenticate::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.client_challenge, 0x1234567890abcdef);
        assert_eq!(decoded.key, 0xabcd);
        assert_eq!(decoded.other_keys, 0x0F);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_authenticate_json_omits_struct_v() {
        let auth = CephXAuthenticate::new(
            0x1234567890abcdef,
            0xabcd,
            CephXTicketBlob::new(42, Bytes::from_static(b"ticket")),
            0x0F,
        );

        let value =
            serde_json::to_value(auth).expect("CephXAuthenticate JSON should serialize cleanly");

        assert_eq!(
            value,
            json!({
                "client_challenge": 0x1234567890abcdefu64,
                "key": 0xabcdu64,
                "old_ticket": {
                    "secret_id": 42u64,
                    "blob": [116, 105, 99, 107, 101, 116]
                },
                "other_keys": 0x0Fu32
            })
        );
    }

    #[test]
    fn test_cephx_server_challenge_encode_decode() {
        let challenge = CephXServerChallenge::new(0xfedcba9876543210);

        let mut buf = BytesMut::new();
        challenge.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.len(), 9); // struct_v (1) + server_challenge (8)

        let mut read_buf = buf.freeze();
        let decoded = CephXServerChallenge::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.server_challenge, 0xfedcba9876543210);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_challenge_blob_encode_decode() {
        let blob = CephXChallengeBlob {
            server_challenge: 0x1122334455667788,
            client_challenge: 0x8877665544332211,
        };

        let mut buf = BytesMut::new();
        blob.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CephXChallengeBlob::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.server_challenge, 0x1122334455667788);
        assert_eq!(decoded.client_challenge, 0x8877665544332211);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_authorize_a_encode_decode() {
        let ticket_blob = CephXTicketBlob::new(99, Bytes::from(vec![10, 20, 30]));
        let auth_a = CephXAuthorizeA::new(12345, 4, ticket_blob.clone());

        assert_eq!(auth_a.global_id, 12345);
        assert_eq!(auth_a.service_id, 4);

        let mut buf = BytesMut::new();
        auth_a.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CephXAuthorizeA::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.global_id, 12345);
        assert_eq!(decoded.service_id, 4);
        assert_eq!(decoded.ticket_blob.secret_id, 99);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_authorize_b_no_challenge() {
        let auth_b = CephXAuthorizeB::new(54321);

        assert_eq!(auth_b.nonce, 54321);
        assert!(!auth_b.have_challenge);
        assert_eq!(auth_b.server_challenge_plus_one, 0);

        let mut buf = BytesMut::new();
        auth_b.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CephXAuthorizeB::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.nonce, 54321);
        assert!(!decoded.have_challenge);
        assert_eq!(decoded.server_challenge_plus_one, 0);
    }

    #[test]
    fn test_cephx_authorize_b_with_challenge() {
        let auth_b = CephXAuthorizeB::with_challenge(11111, 99999);

        assert_eq!(auth_b.nonce, 11111);
        assert!(auth_b.have_challenge);
        assert_eq!(auth_b.server_challenge_plus_one, 100000); // 99999 + 1

        let mut buf = BytesMut::new();
        auth_b.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CephXAuthorizeB::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.nonce, 11111);
        assert!(decoded.have_challenge);
        assert_eq!(decoded.server_challenge_plus_one, 100000);
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_authorize_b_rejects_pre_v2() {
        let mut buf = BytesMut::new();
        // Legacy layout: struct_v + nonce (no challenge fields for v1)
        1u8.encode(&mut buf, 0).unwrap();
        54321u64.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let err = CephXAuthorizeB::decode(&mut read_buf, 0).unwrap_err();
        assert!(
            matches!(
                err,
                RadosError::Codec(crate::CodecError::VersionTooOld {
                    type_name: "CephXAuthorizeB",
                    ..
                })
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_cephx_authorize_reply_encode_decode() {
        let reply = CephXAuthorizeReply::new(98765);

        let mut buf = BytesMut::new();
        reply.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.len(), 9); // struct_v (1) + u64 (8)

        let mut read_buf = buf.freeze();
        let decoded = CephXAuthorizeReply::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.nonce_plus_one, 98765);
        assert!(decoded.connection_secret.is_none());
        assert_eq!(read_buf.remaining(), 0);
    }

    #[test]
    fn test_cephx_authorize_reply_with_connection_secret() {
        let secret = Bytes::from(vec![0xaa; 32]);
        let reply = CephXAuthorizeReply::with_connection_secret(12345, secret.clone());

        let mut buf = BytesMut::new();
        reply.encode(&mut buf, 0).unwrap();

        // struct_v (1) + nonce_plus_one (8) + len prefix (4) + secret (32) = 45
        assert_eq!(buf.len(), 45);

        let mut read_buf = buf.freeze();
        let decoded = CephXAuthorizeReply::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.nonce_plus_one, 12345);
        assert_eq!(decoded.connection_secret.unwrap(), secret);
        assert_eq!(read_buf.remaining(), 0);
    }
}
