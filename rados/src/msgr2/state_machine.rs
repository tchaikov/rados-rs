//! msgr2 connection context — encryption, compression, session identity,
//! and protocol-level bookkeeping for an established or in-progress connection.

use crate::msgr2::error::{Msgr2Error as Error, Result};
use bytes::{Bytes, BytesMut};

/// Protocol state identifier - represents which state the connection is in
/// This matches the State enum in C++ ProtocolV2.h
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateKind {
    /// Initial state before connection starts
    None,
    /// Client: Sending banner
    BannerConnecting,
    /// Client: Sending HELLO frame
    HelloConnecting,
    /// Client: Performing CephX authentication
    AuthConnecting,
    /// Client: Exchanging AUTH_SIGNATURE frames
    AuthConnectingSign,
    /// Client: Negotiating compression
    CompressionConnecting,
    /// Client: Sending CLIENT_IDENT
    SessionConnecting,
    /// Server: Accepting banner
    BannerAccepting,
    /// Server: Accepting HELLO frame
    HelloAccepting,
    /// Server: Performing authentication
    AuthAccepting,
    /// Server: Exchanging AUTH_SIGNATURE frames
    AuthAcceptingSign,
    /// Server: Negotiating compression
    CompressionAccepting,
    /// Server: Accepting session
    SessionAccepting,
    /// Connection established and ready for messages
    Ready,
}

impl StateKind {
    /// Get the state name as a string (for debugging/logging)
    pub fn as_str(&self) -> &'static str {
        match self {
            StateKind::None => "NONE",
            StateKind::BannerConnecting => "BANNER_CONNECTING",
            StateKind::HelloConnecting => "HELLO_CONNECTING",
            StateKind::AuthConnecting => "AUTH_CONNECTING",
            StateKind::AuthConnectingSign => "AUTH_CONNECTING_SIGN",
            StateKind::CompressionConnecting => "COMPRESSION_CONNECTING",
            StateKind::SessionConnecting => "SESSION_CONNECTING",
            StateKind::BannerAccepting => "BANNER_ACCEPTING",
            StateKind::HelloAccepting => "HELLO_ACCEPTING",
            StateKind::AuthAccepting => "AUTH_ACCEPTING",
            StateKind::AuthAcceptingSign => "AUTH_ACCEPTING_SIGN",
            StateKind::CompressionAccepting => "COMPRESSION_ACCEPTING",
            StateKind::SessionAccepting => "SESSION_ACCEPTING",
            StateKind::Ready => "READY",
        }
    }
}

/// Connection context that holds protocol state for the msgr2 connection.
#[derive(Debug)]
pub struct StateMachine {
    state_kind: StateKind,
    /// Frame decryptor for SECURE mode (connection_mode = 2)
    frame_decryptor: Option<Box<dyn crate::msgr2::crypto::FrameDecryptor>>,
    /// Frame encryptor for SECURE mode (connection_mode = 2)
    frame_encryptor: Option<Box<dyn crate::msgr2::crypto::FrameEncryptor>>,
    /// Compression context for frame compression/decompression
    compression_ctx: Option<crate::msgr2::compression::CompressionContext>,
    /// Pre-auth buffer tracking for AUTH_SIGNATURE
    /// Tracks all bytes received before AUTH_DONE for signature verification
    pre_auth_rxbuf: BytesMut,
    /// Tracks all bytes sent before AUTH_DONE for signature generation
    pre_auth_txbuf: BytesMut,
    /// Whether we're in pre-auth phase (before AUTH_DONE)
    pre_auth_enabled: bool,
    /// Session key from AUTH_DONE, used for HMAC-SHA256 signatures
    session_key: Option<Bytes>,
    /// Negotiated connection mode (0=none, 1=CRC, 2=SECURE), set during auth phase.
    connection_mode: u32,
    /// Connection secret for SECURE mode encryption, set during auth phase.
    connection_secret: Option<Bytes>,
    /// Server address we're connecting to (for CLIENT_IDENT)
    server_addr: Option<crate::EntityAddr>,
    /// Our own client address (for CLIENT_IDENT)
    client_addr: Option<crate::EntityAddr>,
    /// Peer's supported msgr2 features from banner exchange
    peer_supported_features: u64,
    /// Connection configuration
    config: crate::msgr2::ConnectionConfig,
    /// Server-side auth handler (for server connections only)
    server_auth_handler: Option<crate::auth::CephXServerHandler>,
    /// Global ID assigned by the server during authentication
    /// Used to uniquely identify this client session
    global_id: u64,
    /// Ceph session features negotiated during the ident exchange.
    negotiated_features: u64,
    /// Last time we received a Keepalive2Ack (for timeout detection)
    last_keepalive_ack: Option<std::time::Instant>,
}

impl StateMachine {
    /// Shared initializer for both client and server state machines.
    fn base(
        state_kind: StateKind,
        config: crate::msgr2::ConnectionConfig,
        server_auth_handler: Option<crate::auth::CephXServerHandler>,
    ) -> Self {
        Self {
            state_kind,
            frame_decryptor: None,
            frame_encryptor: None,
            compression_ctx: None,
            pre_auth_rxbuf: BytesMut::new(),
            pre_auth_txbuf: BytesMut::new(),
            pre_auth_enabled: true,
            session_key: None,
            connection_mode: 0,
            connection_secret: None,
            server_addr: None,
            client_addr: None,
            peer_supported_features: 0,
            config,
            server_auth_handler,
            global_id: 0,
            negotiated_features: 0,
            last_keepalive_ack: None,
        }
    }

    /// Create a new state machine for client connection
    pub fn new_client(config: crate::msgr2::ConnectionConfig) -> Self {
        Self::base(StateKind::BannerConnecting, config, None)
    }

    /// Set the server address (called from protocol.rs after connection)
    pub fn set_server_addr(&mut self, addr: crate::EntityAddr) {
        self.server_addr = Some(addr);
    }

    /// Set the client address (called from protocol.rs after connection)
    pub fn set_client_addr(&mut self, addr: crate::EntityAddr) {
        self.client_addr = Some(addr);
    }

    /// Set peer's supported msgr2 features (from banner exchange)
    pub fn set_peer_supported_features(&mut self, features: u64) {
        self.peer_supported_features = features;
        let feature_set = crate::msgr2::FeatureSet::from(features);
        tracing::debug!(
            "Peer supported features set: 0x{:x} (REVISION_1={}, COMPRESSION={})",
            features,
            feature_set.contains(crate::msgr2::FeatureSet::REVISION_1),
            feature_set.contains(crate::msgr2::FeatureSet::COMPRESSION)
        );
    }

    /// Check if peer supports compression feature
    pub fn peer_supports_compression(&self) -> bool {
        let features = crate::msgr2::FeatureSet::from(self.peer_supported_features);
        features.contains(crate::msgr2::FeatureSet::COMPRESSION)
    }

    /// Check if we (client) support compression feature
    pub fn we_support_compression(&self) -> bool {
        let features = crate::msgr2::FeatureSet::from(self.config.supported_features);
        features.contains(crate::msgr2::FeatureSet::COMPRESSION)
    }

    /// Check if both we and peer support compression
    pub fn compression_negotiation_needed(&self) -> bool {
        self.we_support_compression() && self.peer_supports_compression()
    }

    /// Get the global_id assigned during authentication.
    pub fn global_id(&self) -> u64 {
        self.global_id
    }

    /// Get the negotiated Ceph session features from the ident exchange.
    pub fn negotiated_features(&self) -> u64 {
        self.negotiated_features
    }

    /// Get the last keepalive ACK timestamp
    /// Returns None if no keepalive ACK has been received yet
    pub fn last_keepalive_ack(&self) -> Option<std::time::Instant> {
        self.last_keepalive_ack
    }

    /// Record that a Keepalive2Ack was received now.
    pub fn record_keepalive_ack(&mut self) {
        self.last_keepalive_ack = Some(std::time::Instant::now());
    }

    // ── Phase-coordinator helpers ─────────────────────────────────────────────

    /// Store the global ID assigned by the server during authentication.
    pub fn set_global_id(&mut self, id: u64) {
        self.global_id = id;
    }

    /// Store the connection mode negotiated during authentication.
    pub fn set_connection_mode(&mut self, mode: u32) {
        self.connection_mode = mode;
    }

    /// Store the connection secret (used for reconnection state).
    pub fn set_connection_secret(&mut self, secret: Option<Bytes>) {
        self.connection_secret = secret;
    }

    /// Transition the state machine to the Ready state after session establishment.
    pub fn transition_to_ready(&mut self, negotiated_features: u64) {
        self.negotiated_features = negotiated_features;
        self.state_kind = StateKind::Ready;
    }

    // ── Phase-coordinator read accessors ─────────────────────────────────────

    /// Peer's supported msgr2 feature bits (set after banner exchange).
    pub fn peer_supported_features(&self) -> u64 {
        self.peer_supported_features
    }

    /// Our own advertised msgr2 feature bits (from config).
    pub fn our_supported_features(&self) -> u64 {
        self.config.supported_features
    }

    /// Clone the server entity address, falling back to default if not set.
    pub fn server_addr_clone(&self) -> crate::EntityAddr {
        self.server_addr.unwrap_or_default()
    }

    /// Clone the client entity address, falling back to default if not set.
    pub fn client_addr_clone(&self) -> crate::EntityAddr {
        self.client_addr.unwrap_or_default()
    }

    /// Preferred connection modes from config.
    pub fn config_preferred_modes(&self) -> &[crate::msgr2::ConnectionMode] {
        &self.config.preferred_modes
    }

    /// Supported auth methods from config.
    pub fn config_supported_auth_methods(&self) -> &[crate::msgr2::AuthMethod] {
        &self.config.supported_auth_methods
    }

    /// Service ID from config.
    pub fn config_service_id(&self) -> u32 {
        self.config.service_id
    }

    /// Entity name from config.
    pub fn config_entity_name(&self) -> &crate::EntityName {
        &self.config.entity_name
    }

    /// Take the server-side auth handler (consumed once during `accept_session`).
    pub fn take_server_auth_handler(&mut self) -> Option<crate::auth::CephXServerHandler> {
        self.server_auth_handler.take()
    }

    /// Create a new state machine for server connection with optional auth handler
    pub fn new_server_with_auth(auth_handler: Option<crate::auth::CephXServerHandler>) -> Self {
        Self::base(
            StateKind::HelloAccepting,
            crate::msgr2::ConnectionConfig::default(),
            auth_handler,
        )
    }

    /// Get current state name
    pub fn current_state_name(&self) -> &'static str {
        self.state_kind.as_str()
    }

    /// Get the current state kind (typed enum value)
    pub fn current_state_kind(&self) -> StateKind {
        self.state_kind
    }

    /// Setup frame encryption/decryption handlers for SECURE mode
    /// Called after AUTH_DONE when connection_secret is available
    pub fn setup_encryption(&mut self, connection_secret: &[u8]) -> Result<()> {
        use crate::msgr2::crypto::{
            Aes128GcmDecryptor, Aes128GcmEncryptor, parse_connection_secret,
        };

        let (key, rx_nonce, tx_nonce) = parse_connection_secret(connection_secret)?;

        // Setup decryptor and encryptor with nonces
        // Client uses crossed=false (no swap): rx_nonce for RX, tx_nonce for TX
        // Server uses crossed=true (swapped): tx_nonce for RX, rx_nonce for TX
        let decryptor = Aes128GcmDecryptor::new(key.clone(), rx_nonce)?;
        self.frame_decryptor = Some(Box::new(decryptor));

        let encryptor = Aes128GcmEncryptor::new(key, tx_nonce)?;
        self.frame_encryptor = Some(Box::new(encryptor));

        tracing::info!("Frame encryption/decryption handlers initialized for SECURE mode");

        Ok(())
    }

    /// Decrypt frame data if in SECURE mode
    pub fn decrypt_frame_data(&mut self, data: &[u8]) -> Result<Bytes> {
        if let Some(decryptor) = &mut self.frame_decryptor {
            decryptor.decrypt(data).map_err(Into::into)
        } else {
            Ok(Bytes::copy_from_slice(data))
        }
    }

    /// Encrypt frame data if in SECURE mode
    pub fn encrypt_frame_data(&mut self, data: &[u8]) -> Result<Bytes> {
        if let Some(encryptor) = &mut self.frame_encryptor {
            encryptor.encrypt(data).map_err(Into::into)
        } else {
            Ok(Bytes::copy_from_slice(data))
        }
    }

    /// Check if frame encryption is active
    pub fn has_encryption(&self) -> bool {
        self.frame_decryptor.is_some()
    }

    /// Check if using msgr2.1 (rev1) — true when the peer supports REVISION_1
    pub fn is_rev1(&self) -> bool {
        let peer = crate::msgr2::FeatureSet::from(self.peer_supported_features);
        peer.contains(crate::msgr2::FeatureSet::REVISION_1)
    }

    /// Get compression context reference
    pub fn compression_ctx(&self) -> Option<&crate::msgr2::compression::CompressionContext> {
        self.compression_ctx.as_ref()
    }

    /// Setup compression with the negotiated algorithm
    pub fn setup_compression(
        &mut self,
        algorithm: crate::msgr2::compression::CompressionAlgorithm,
    ) {
        tracing::info!("Setting up compression with algorithm: {:?}", algorithm);
        self.compression_ctx = Some(crate::msgr2::compression::CompressionContext::new(
            algorithm,
        ));
    }

    // The three take_* methods below support the post-handshake Framed
    // codec transition. They're wired into ConnectionState::transition_to_framed
    // in step 2, and will gain their first real caller in step 3 when
    // run_io_loop starts using the Framed data plane. The `allow(dead_code)`
    // attributes keep the intermediate commits compiling under -D warnings.

    /// Take ownership of the frame decryptor for the post-handshake codec.
    ///
    /// After the call the state machine no longer has a decryptor, which is
    /// correct because all subsequent recv traffic is driven by the
    /// [`Msgr2Decoder`] owning it inside a [`FramedRead`]. Used by
    /// `ConnectionState::transition_to_framed`.
    ///
    /// [`Msgr2Decoder`]: crate::msgr2::codec::Msgr2Decoder
    /// [`FramedRead`]: tokio_util::codec::FramedRead
    #[allow(dead_code)]
    pub fn take_frame_decryptor(
        &mut self,
    ) -> Option<Box<dyn crate::msgr2::crypto::FrameDecryptor>> {
        self.frame_decryptor.take()
    }

    /// Take ownership of the frame encryptor for the post-handshake codec.
    ///
    /// Counterpart to [`take_frame_decryptor`](Self::take_frame_decryptor).
    /// After the call all subsequent send traffic is driven by the
    /// [`Msgr2Encoder`] owning the encryptor inside a [`FramedWrite`].
    ///
    /// [`Msgr2Encoder`]: crate::msgr2::codec::Msgr2Encoder
    /// [`FramedWrite`]: tokio_util::codec::FramedWrite
    #[allow(dead_code)]
    pub fn take_frame_encryptor(
        &mut self,
    ) -> Option<Box<dyn crate::msgr2::crypto::FrameEncryptor>> {
        self.frame_encryptor.take()
    }

    /// Take ownership of the compression context, wrapping it in [`Arc`] so
    /// both the decoder and encoder halves of a split codec pair can hold
    /// the same instance. Returns `None` if no compression was negotiated.
    ///
    /// [`Arc`]: std::sync::Arc
    #[allow(dead_code)]
    pub fn take_compression_ctx_arc(
        &mut self,
    ) -> Option<std::sync::Arc<crate::msgr2::compression::CompressionContext>> {
        self.compression_ctx.take().map(std::sync::Arc::new)
    }

    /// Record bytes sent during pre-auth phase (for AUTH_SIGNATURE)
    pub fn record_sent(&mut self, data: &[u8]) {
        if self.pre_auth_enabled {
            self.pre_auth_txbuf.extend_from_slice(data);
        }
    }

    /// Record bytes received during pre-auth phase (for AUTH_SIGNATURE)
    pub fn record_received(&mut self, data: &[u8]) {
        if self.pre_auth_enabled {
            self.pre_auth_rxbuf.extend_from_slice(data);
        }
    }

    /// Set session key and disable pre-auth tracking (called after AUTH_DONE)
    pub fn complete_pre_auth(&mut self, session_key: Option<Bytes>) {
        self.session_key = session_key;
        self.pre_auth_enabled = false;
    }

    /// Get pre-auth received buffer (for computing signature we send)
    pub fn get_pre_auth_rxbuf(&self) -> &[u8] {
        &self.pre_auth_rxbuf
    }

    /// Get pre-auth sent buffer (for verifying server's signature)
    pub fn get_pre_auth_txbuf(&self) -> &[u8] {
        &self.pre_auth_txbuf
    }

    /// Get a clone of the auth provider from the connection config.
    pub fn get_auth_provider(&self) -> Option<Box<dyn crate::auth::AuthProvider>> {
        self.config.auth_provider.clone()
    }

    /// Compute HMAC-SHA256 over `data` using `key`.  Returns 32 zero bytes when `key` is `None`.
    pub fn hmac_sha256(key: Option<&Bytes>, data: &[u8]) -> Result<Bytes> {
        match key {
            Some(k) => {
                use hmac::{Hmac, Mac};
                use sha2::Sha256;
                type HmacSha256 = Hmac<Sha256>;
                let mut mac = HmacSha256::new_from_slice(k)
                    .map_err(|e| Error::protocol_error(&format!("Invalid HMAC key: {e:?}")))?;
                mac.update(data);
                Ok(Bytes::copy_from_slice(&mac.finalize().into_bytes()))
            }
            None => Ok(Bytes::from_static(&[0u8; 32])),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_compression_support_detection() {
        let mut sm = StateMachine::new_client(crate::msgr2::ConnectionConfig::default());

        sm.set_peer_supported_features(0x3);
        assert!(
            sm.peer_supports_compression(),
            "Should support compression with features=0x3"
        );

        sm.set_peer_supported_features(0x1);
        assert!(
            !sm.peer_supports_compression(),
            "Should NOT support compression with features=0x1"
        );

        sm.set_peer_supported_features(0x0);
        assert!(
            !sm.peer_supports_compression(),
            "Should NOT support compression with features=0x0"
        );

        sm.set_peer_supported_features(0x2);
        assert!(
            sm.peer_supports_compression(),
            "Should support compression with features=0x2"
        );
    }

    #[test]
    fn test_default_config_does_not_support_compression() {
        let sm = StateMachine::new_client(crate::msgr2::ConnectionConfig::default());
        assert!(
            !sm.we_support_compression(),
            "Default msgr2 config should not advertise compression"
        );
    }

    #[test]
    fn test_auth_method_config() {
        let config = crate::msgr2::ConnectionConfig::default();
        assert!(
            !config.supported_auth_methods.is_empty(),
            "Default config should have at least one supported auth method"
        );

        let config = crate::msgr2::ConnectionConfig::with_no_auth();
        assert_eq!(
            config.supported_auth_methods,
            vec![crate::msgr2::AuthMethod::None],
            "with_no_auth should set only AuthMethod::None"
        );
        assert!(
            config.auth_provider.is_none(),
            "with_no_auth should have no auth provider"
        );

        let config = crate::msgr2::ConnectionConfig::with_no_auth();
        let sm = StateMachine::new_client(config);
        assert_eq!(sm.current_state_kind(), StateKind::BannerConnecting);
    }
}
