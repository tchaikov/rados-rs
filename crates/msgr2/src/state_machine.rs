//! msgr2 protocol state machine implementation using the State pattern
//!
//! Based on ~/dev/ceph/src/msg/async/ProtocolV2.h and msgr2.rst documentation

use crate::error::{Error, Result};
use crate::frames::{
    AuthDoneFrame, AuthRequestFrame, AuthRequestMoreFrame, AuthSignatureFrame, ClientIdentFrame,
    CompressionRequestFrame, Frame, FrameTrait, HelloFrame, Keepalive2AckFrame, ServerIdentFrame,
    Tag,
};
use auth::{AuthResult, CephXClientHandler, Keyring};
use bytes::{Bytes, BytesMut};
use denc::Denc;
use std::fmt::Debug;
use tracing;

/// Helper function to create a Frame from a FrameTrait
pub fn create_frame_from_trait<F: FrameTrait>(frame_trait: &F, tag: Tag) -> Frame {
    // Use msgr2_frame_assumed features for frame encoding
    // msgr2_frame_assumed = MSG_ADDR2 | SERVER_NAUTILUS
    use denc::features::{CEPH_FEATUREMASK_MSG_ADDR2, CEPH_FEATUREMASK_SERVER_NAUTILUS};
    const MSGR2_FRAME_ASSUMED: u64 = CEPH_FEATUREMASK_MSG_ADDR2 | CEPH_FEATUREMASK_SERVER_NAUTILUS;

    let segments = frame_trait.get_segments(MSGR2_FRAME_ASSUMED);
    Frame {
        preamble: crate::frames::Preamble {
            tag,
            num_segments: segments.len() as u8,
            segments: {
                let mut descs = [crate::frames::SegmentDescriptor::default(); 4];
                for (i, seg) in segments.iter().enumerate() {
                    descs[i] = crate::frames::SegmentDescriptor {
                        logical_len: seg.len() as u32,
                        align: 8,
                    };
                }
                descs
            },
            flags: 0,
            reserved: 0,
            crc: 0,
        },
        segments,
    }
}

/// Result of frame processing that indicates next action
#[derive(Debug)]
pub enum StateResult {
    /// Continue in current state
    Continue,
    /// Transition to a new state
    Transition(Box<dyn State>),
    /// Send a frame and optionally transition
    SendFrame {
        frame: Frame,
        next_state: Option<Box<dyn State>>,
    },
    /// Send a frame and wait for specific response
    SendAndWait {
        frame: Frame,
        next_state: Box<dyn State>,
    },
    /// Connection established successfully
    Ready,
    /// Connection should be closed due to error
    Fault(String),
}

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
    /// Server: Accepting session
    SessionAccepting,
    /// Connection established and ready for messages
    Ready,
}

impl StateKind {
    /// Check if this state is an authentication state
    pub fn is_auth_state(&self) -> bool {
        matches!(
            self,
            StateKind::AuthConnecting | StateKind::AuthConnectingSign | StateKind::AuthAccepting
        )
    }

    /// Check if this state is a connecting (client) state
    pub fn is_connecting_state(&self) -> bool {
        matches!(
            self,
            StateKind::BannerConnecting
                | StateKind::HelloConnecting
                | StateKind::AuthConnecting
                | StateKind::AuthConnectingSign
                | StateKind::CompressionConnecting
                | StateKind::SessionConnecting
        )
    }

    /// Check if this state is an accepting (server) state
    pub fn is_accepting_state(&self) -> bool {
        matches!(
            self,
            StateKind::BannerAccepting
                | StateKind::HelloAccepting
                | StateKind::AuthAccepting
                | StateKind::SessionAccepting
        )
    }

    /// Check if authentication is complete (past auth states)
    pub fn is_authenticated(&self) -> bool {
        matches!(
            self,
            StateKind::CompressionConnecting | StateKind::SessionConnecting | StateKind::Ready
        )
    }

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
            StateKind::SessionAccepting => "SESSION_ACCEPTING",
            StateKind::Ready => "READY",
        }
    }
}

/// Core trait for all protocol states
/// Each state can only handle specific frame types according to msgr2.rst
pub trait State: Debug + Send {
    /// Get the state kind (typed enum value)
    fn kind(&self) -> StateKind;

    /// Get the state name for debugging (deprecated - use kind().as_str() instead)
    fn name(&self) -> &'static str {
        self.kind().as_str()
    }

    /// Handle an incoming frame - only certain frames are valid in each state
    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult>;

    /// Start/enter this state - may send initial frames
    fn enter(&mut self) -> Result<StateResult> {
        Ok(StateResult::Continue)
    }

    /// Get expected frame types for this state (for validation/debugging)
    fn expected_frames(&self) -> &[Tag];

    /// Enable downcasting to concrete state types
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Connection establishment states (client-side)

#[derive(Debug)]
pub struct BannerConnecting {
    pub local_supported_features: u64,
    pub local_required_features: u64,
    pub preferred_modes: Vec<crate::ConnectionMode>,
    pub auth_method: Option<crate::AuthMethod>,
}

impl State for BannerConnecting {
    fn kind(&self) -> StateKind {
        StateKind::BannerConnecting
    }

    fn expected_frames(&self) -> &[Tag] {
        // Banner exchange happens outside frame protocol
        &[]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, _frame: Frame) -> Result<StateResult> {
        Err(Error::protocol_error(
            "No frames expected during banner exchange",
        ))
    }

    fn enter(&mut self) -> Result<StateResult> {
        // Banner is sent outside of frame protocol
        Ok(StateResult::Transition(Box::new(HelloConnecting::new(
            self.preferred_modes.clone(),
            self.auth_method,
        ))))
    }
}

#[derive(Debug)]
pub struct HelloConnecting {
    hello_sent: bool,
    preferred_modes: Vec<crate::ConnectionMode>,
    auth_method: Option<crate::AuthMethod>,
}

impl HelloConnecting {
    pub fn new(preferred_modes: Vec<crate::ConnectionMode>, auth_method: Option<crate::AuthMethod>) -> Self {
        Self {
            hello_sent: false,
            preferred_modes,
            auth_method,
        }
    }
}

impl State for HelloConnecting {
    fn kind(&self) -> StateKind {
        StateKind::HelloConnecting
    }

    fn expected_frames(&self) -> &[Tag] {
        &[Tag::Hello]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::Hello => {
                // Decode and validate HELLO frame
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error("HELLO frame missing payload"));
                }

                // Deserialize HELLO frame from segment data
                let mut payload = frame.segments[0].clone();
                let entity_type = u8::decode(&mut payload, 0).map_err(|e| {
                    Error::protocol_error(&format!("Failed to decode entity_type: {:?}", e))
                })?;
                let _peer_addr = denc::EntityAddr::decode(&mut payload, 0).map_err(|e| {
                    Error::protocol_error(&format!("Failed to decode peer_addr: {:?}", e))
                })?;

                tracing::debug!("Received HELLO from entity_type={}", entity_type);

                // Transition to auth phase
                Ok(StateResult::Transition(Box::new(AuthConnecting::new(
                    self.preferred_modes.clone(),
                    self.auth_method,
                ))))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in HELLO_CONNECTING state",
                frame.preamble.tag
            ))),
        }
    }

    fn enter(&mut self) -> Result<StateResult> {
        if !self.hello_sent {
            self.hello_sent = true;
            // Create HELLO frame
            let hello_frame = HelloFrame::new(
                denc::EntityType::TYPE_MON.value() as u8,
                denc::EntityAddr::default(),
            );
            let frame = create_frame_from_trait(&hello_frame, Tag::Hello);

            Ok(StateResult::SendAndWait {
                frame,
                next_state: Box::new(HelloConnecting::new(self.preferred_modes.clone(), self.auth_method)),
            })
        } else {
            Ok(StateResult::Continue)
        }
    }
}

#[derive(Debug)]
pub struct AuthConnecting {
    auth_method: crate::AuthMethod,
    cephx_handler: Option<CephXClientHandler>,
    preferred_modes: Vec<crate::ConnectionMode>,
}

impl AuthConnecting {
    pub fn new(preferred_modes: Vec<crate::ConnectionMode>, auth_method: Option<crate::AuthMethod>) -> Self {
        // Determine which auth method to use
        let actual_auth_method = auth_method.unwrap_or_else(|| {
            // Auto-detect: try to load keyring, if it fails use None auth
            let keyring_path = std::env::var("CEPH_KEYRING")
                .unwrap_or_else(|_| "/etc/ceph/ceph.client.admin.keyring".to_string());
            
            if std::path::Path::new(&keyring_path).exists() {
                tracing::info!("Keyring file found, using CephX authentication");
                crate::AuthMethod::Cephx
            } else {
                tracing::info!("No keyring file found, using no authentication");
                crate::AuthMethod::None
            }
        });

        match actual_auth_method {
            crate::AuthMethod::None => {
                tracing::info!("Using AuthMethod::None (no authentication)");
                Self {
                    auth_method: actual_auth_method,
                    cephx_handler: None,
                    preferred_modes,
                }
            }
            crate::AuthMethod::Cephx => {
                tracing::info!("Using AuthMethod::Cephx authentication");
                // Try environment variable first, then fall back to default path
                let keyring_path = std::env::var("CEPH_KEYRING")
                    .unwrap_or_else(|_| "/etc/ceph/ceph.client.admin.keyring".to_string());

                Self::with_keyring_path(&keyring_path, preferred_modes.clone(), actual_auth_method)
                    .unwrap_or_else(|e| {
                        tracing::warn!("Failed to load keyring from {}: {}", keyring_path, e);
                        tracing::warn!("Using fallback authentication key");
                        Self::with_fallback_key(preferred_modes, actual_auth_method)
                    })
            }
            _ => {
                tracing::warn!("Unsupported auth method {:?}, falling back to None", actual_auth_method);
                Self {
                    auth_method: crate::AuthMethod::None,
                    cephx_handler: None,
                    preferred_modes,
                }
            }
        }
    }

    pub fn with_keyring_path(
        keyring_path: &str,
        preferred_modes: Vec<crate::ConnectionMode>,
        auth_method: crate::AuthMethod,
    ) -> Result<Self> {
        let keyring = Keyring::from_file(keyring_path)
            .map_err(|e| Error::Protocol(format!("Failed to load keyring: {}", e)))?;

        let entity_name = "client.admin";
        let crypto_key = keyring
            .get_key(entity_name)
            .ok_or_else(|| Error::Protocol(format!("No key found for entity: {}", entity_name)))?;

        let mut handler = CephXClientHandler::new(entity_name.to_string())?;
        handler.set_secret_key(crypto_key.clone());

        Ok(Self {
            auth_method,
            cephx_handler: Some(handler),
            preferred_modes,
        })
    }

    fn with_fallback_key(preferred_modes: Vec<crate::ConnectionMode>, auth_method: crate::AuthMethod) -> Self {
        let mut handler = CephXClientHandler::new("client.admin".to_string())
            .expect("Failed to create CephXClientHandler");
        let _ = handler.set_secret_key_from_base64("AQAHpMtmRCPGIxAAXvJkMZFCE6/k/x+CxU6t9Q==");

        Self {
            auth_method,
            cephx_handler: Some(handler),
            preferred_modes,
        }
    }
}

impl State for AuthConnecting {
    fn kind(&self) -> StateKind {
        StateKind::AuthConnecting
    }

    fn expected_frames(&self) -> &[Tag] {
        &[Tag::AuthBadMethod, Tag::AuthReplyMore, Tag::AuthDone]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::AuthBadMethod => {
                // Parse AuthBadMethod to see what the server is saying
                if let Some(segment) = frame.segments.first() {
                    tracing::error!(
                        "AuthBadMethod payload ({} bytes): {:02x?}",
                        segment.len(),
                        segment
                    );

                    // Try to decode the fields:
                    // method: u32, result: i32, allowed_methods: Vec<u32>, allowed_modes: Vec<u32>
                    if segment.len() >= 8 {
                        let mut payload = segment.clone();
                        let method = u32::decode(&mut payload, 0).unwrap_or(0);
                        let result = i32::decode(&mut payload, 0).unwrap_or(0);
                        tracing::error!("AuthBadMethod: method={}, result={}", method, result);
                    }
                }
                Ok(StateResult::Fault(
                    "Authentication method rejected".to_string(),
                ))
            }
            Tag::AuthReplyMore => {
                // Handle CephX multi-round auth
                if let Some(handler) = &mut self.cephx_handler {
                    if let Some(payload) = frame.segments.first() {
                        match handler.handle_auth_response(payload.clone()) {
                            Ok(AuthResult::Success) => {
                                Ok(StateResult::Transition(Box::new(SessionConnecting::new())))
                            }
                            Ok(AuthResult::NeedMoreData) => {
                                // Need to send another auth request with challenge response
                                let auth_payload = handler
                                    .build_authenticate_request()
                                    .map_err(|e| Error::protocol_error(&e.to_string()))?;

                                // For AuthRequestMore, we only send the auth_payload (no method/preferred_modes)
                                let auth_frame = AuthRequestMoreFrame::new(auth_payload);
                                let response_frame =
                                    create_frame_from_trait(&auth_frame, Tag::AuthRequestMore);

                                // IMPORTANT: Don't create a new AuthConnecting - we need to preserve
                                // the CephXClientHandler state (server_challenge, session, etc.)
                                // The state machine will stay in the current state (self)
                                Ok(StateResult::SendFrame {
                                    frame: response_frame,
                                    next_state: None,
                                })
                            }
                            Ok(AuthResult::Failed(msg)) => Ok(StateResult::Fault(msg)),
                            Err(e) => Err(Error::protocol_error(&e.to_string())),
                        }
                    } else {
                        Err(Error::protocol_error(
                            "AUTH_REPLY_MORE frame missing payload",
                        ))
                    }
                } else {
                    Err(Error::protocol_error(
                        "Received AUTH_REPLY_MORE but using AuthMethod::None",
                    ))
                }
            }
            Tag::AuthDone => {
                // Parse AUTH_DONE frame to get global_id and connection mode
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let global_id = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode global_id: {:?}", e))
                    })?;
                    let con_mode = u32::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode con_mode: {:?}", e))
                    })?;
                    let auth_payload = Bytes::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode auth_payload: {:?}", e))
                    })?;

                    tracing::info!(
                        "Authentication completed successfully - global_id: {}, connection_mode: {}",
                        global_id,
                        con_mode
                    );

                    // For AuthMethod::None, we don't need to extract session_key and connection_secret
                    if self.auth_method == crate::AuthMethod::None {
                        tracing::info!("AuthMethod::None - no session key/connection secret needed");
                        
                        // For no-auth with CRC mode, skip directly to session connecting
                        // For SECURE mode, we'd need signature exchange but that doesn't make sense with no-auth
                        if con_mode == crate::ConnectionMode::Secure.as_u32() {
                            tracing::warn!("SECURE mode with AuthMethod::None is unusual, skipping to session");
                        }
                        
                        // Go directly to SessionConnecting
                        Ok(StateResult::Transition(Box::new(SessionConnecting::new())))
                    } else {
                        // CephX authentication - extract session_key and connection_secret
                        let handler = self.cephx_handler.as_mut()
                            .ok_or_else(|| Error::protocol_error("No CephX handler available"))?;
                        
                        let (session_key, connection_secret) = handler
                            .handle_auth_done(auth_payload, global_id, con_mode)
                            .map_err(|e| Error::protocol_error(&e.to_string()))?;

                        tracing::debug!("Extracted from AUTH_DONE - session_key: {:?} bytes, connection_secret: {:?} bytes",
                            session_key.as_ref().map(|k| k.len()),
                            connection_secret.as_ref().map(|s| s.len()));
                        if let Some(ref key) = session_key {
                            tracing::debug!(
                                "Session key first 16 bytes: {:02x?}",
                                &key[..16.min(key.len())]
                            );
                        }

                        // Authentication completed successfully, transition to AUTH_CONNECTING_SIGN
                        // for signature exchange, then to session phase
                        // Note: Signature, server_addr, and client_addr will be set in apply_result()
                        Ok(StateResult::Transition(Box::new(
                            AuthConnectingSign::new_with_encryption(
                                con_mode,
                                session_key,
                                connection_secret,
                                Bytes::new(), // Placeholder, will be replaced with computed signature
                                None, // Placeholder, will be replaced with computed expected server signature
                                global_id,
                                denc::EntityAddr::default(), // Placeholder, will be replaced with actual server_addr
                                denc::EntityAddr::default(), // Placeholder, will be replaced with actual client_addr
                            ),
                        )))
                    }
                } else {
                    Err(Error::protocol_error("AUTH_DONE frame missing payload"))
                }
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in AUTH_CONNECTING state",
                frame.preamble.tag
            ))),
        }
    }

    fn enter(&mut self) -> Result<StateResult> {
        // Send initial AUTH_REQUEST frame
        let preferred_modes: Vec<u32> = self.preferred_modes.iter().map(|m| m.as_u32()).collect();
        
        let (method, auth_payload) = match self.auth_method {
            crate::AuthMethod::None => {
                tracing::info!("Sending AUTH_REQUEST with AuthMethod::None (no authentication)");
                // For no-auth, send empty payload
                (crate::AuthMethod::None.as_u32(), Bytes::new())
            }
            crate::AuthMethod::Cephx => {
                tracing::info!("Sending AUTH_REQUEST with AuthMethod::Cephx");
                let handler = self.cephx_handler.as_mut()
                    .ok_or_else(|| Error::protocol_error("No CephX handler available"))?;
                let payload = handler
                    .build_initial_request(0) // Initial request uses global_id=0
                    .map_err(|e| Error::protocol_error(&e.to_string()))?;
                (crate::AuthMethod::Cephx.as_u32(), payload)
            }
            _ => {
                return Err(Error::protocol_error(&format!(
                    "Unsupported auth method: {:?}",
                    self.auth_method
                )));
            }
        };
        
        let auth_frame = AuthRequestFrame::new(
            method,
            preferred_modes.clone(),
            auth_payload,
        );
        let frame = create_frame_from_trait(&auth_frame, Tag::AuthRequest);

        Ok(StateResult::SendAndWait {
            frame,
            next_state: Box::new(AuthConnecting::new(self.preferred_modes.clone(), Some(self.auth_method))),
        })
    }
}

/// Auth Connecting Sign state - exchange AUTH_SIGNATURE frames after AUTH_DONE
/// This implements the mutual signature verification step in SECURE mode
#[derive(Debug)]
pub struct AuthConnectingSign {
    pub connection_mode: u32,
    pub session_key: Option<Bytes>,
    pub connection_secret: Option<Bytes>,
    /// Pre-computed HMAC-SHA256 signature to send to server
    pub our_signature: Bytes,
    /// Expected signature from server for verification
    pub expected_server_signature: Option<Bytes>,
    /// Global ID received from AUTH_DONE
    pub global_id: u64,
    /// Server address we're connecting to
    pub server_addr: denc::EntityAddr,
    /// Our own client address
    pub client_addr: denc::EntityAddr,
}

/// Compression Connecting state - negotiate compression after AUTH_SIGNATURE
/// This implements the compression negotiation step in msgr2.1
#[derive(Debug)]
pub struct CompressionConnecting {
    /// Connection mode from AUTH_DONE
    pub connection_mode: u32,
    /// Session key for encryption
    pub session_key: Option<Bytes>,
    /// Connection secret for encryption
    pub connection_secret: Option<Bytes>,
    /// Global ID from AUTH_DONE
    pub global_id: u64,
    /// Server address we're connecting to
    pub server_addr: denc::EntityAddr,
    /// Our own client address
    pub client_addr: denc::EntityAddr,
}

impl CompressionConnecting {
    pub fn new_with_encryption(
        connection_mode: u32,
        session_key: Option<Bytes>,
        connection_secret: Option<Bytes>,
        global_id: u64,
        server_addr: denc::EntityAddr,
        client_addr: denc::EntityAddr,
    ) -> Self {
        Self {
            connection_mode,
            session_key,
            connection_secret,
            global_id,
            server_addr,
            client_addr,
        }
    }
}

impl State for CompressionConnecting {
    fn kind(&self) -> StateKind {
        StateKind::CompressionConnecting
    }

    fn expected_frames(&self) -> &[Tag] {
        &[Tag::CompressionDone]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::CompressionDone => {
                // Parse COMPRESSION_DONE frame
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let is_compress = bool::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode is_compress: {:?}", e))
                    })?;
                    let method = u32::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode method: {:?}", e))
                    })?;

                    tracing::info!(
                        "Received COMPRESSION_DONE - is_compress: {}, method: {}",
                        is_compress,
                        method
                    );

                    // Transition to SESSION_CONNECTING to send CLIENT_IDENT
                    Ok(StateResult::Transition(Box::new(
                        SessionConnecting::new_with_encryption(
                            self.connection_mode,
                            self.session_key.clone(),
                            self.connection_secret.clone(),
                            None, // No expected signature needed anymore
                            self.global_id,
                            self.server_addr.clone(),
                            self.client_addr.clone(),
                        ),
                    )))
                } else {
                    Err(Error::protocol_error(
                        "COMPRESSION_DONE frame missing payload",
                    ))
                }
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in COMPRESSION_CONNECTING state",
                frame.preamble.tag
            ))),
        }
    }

    fn enter(&mut self) -> Result<StateResult> {
        // Send COMPRESSION_REQUEST frame
        // For now, we request compression with no preferred methods (empty list)
        // The server will decide whether to enable compression
        let is_compress = true; // Request compression
        let preferred_methods = vec![]; // Empty list means we accept any method the server chooses

        tracing::debug!(
            "Sending COMPRESSION_REQUEST: is_compress={}, preferred_methods={:?}",
            is_compress,
            preferred_methods
        );

        let compression_request = CompressionRequestFrame::new(is_compress, preferred_methods);
        let frame = create_frame_from_trait(&compression_request, Tag::CompressionRequest);

        Ok(StateResult::SendAndWait {
            frame,
            next_state: Box::new(CompressionConnecting::new_with_encryption(
                self.connection_mode,
                self.session_key.clone(),
                self.connection_secret.clone(),
                self.global_id,
                self.server_addr.clone(),
                self.client_addr.clone(),
            )),
        })
    }
}

impl AuthConnectingSign {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_encryption(
        connection_mode: u32,
        session_key: Option<Bytes>,
        connection_secret: Option<Bytes>,
        our_signature: Bytes,
        expected_server_signature: Option<Bytes>,
        global_id: u64,
        server_addr: denc::EntityAddr,
        client_addr: denc::EntityAddr,
    ) -> Self {
        Self {
            connection_mode,
            session_key,
            connection_secret,
            our_signature,
            expected_server_signature,
            global_id,
            server_addr,
            client_addr,
        }
    }
}

impl State for AuthConnectingSign {
    fn kind(&self) -> StateKind {
        StateKind::AuthConnectingSign
    }

    fn expected_frames(&self) -> &[Tag] {
        &[Tag::AuthSignature]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::AuthSignature => {
                // Verify server's AUTH_SIGNATURE
                if let Some(ref expected_sig) = self.expected_server_signature {
                    // Extract server's signature from frame payload
                    // Note: The signature is sent as raw bytes, not length-prefixed
                    if let Some(segment) = frame.segments.first() {
                        let server_signature = segment.clone();

                        // Verify signature matches expected
                        if server_signature == *expected_sig {
                            tracing::info!(
                                "✓ Server AUTH_SIGNATURE verified successfully ({} bytes)",
                                server_signature.len()
                            );
                        } else {
                            tracing::error!(
                                "✗ Server AUTH_SIGNATURE verification failed - expected {} bytes, got {} bytes",
                                expected_sig.len(),
                                server_signature.len()
                            );
                            return Err(Error::protocol_error(
                                "Server AUTH_SIGNATURE verification failed",
                            ));
                        }
                    } else {
                        return Err(Error::protocol_error(
                            "AUTH_SIGNATURE frame missing payload",
                        ));
                    }
                } else {
                    // No expected signature (CRC mode), just accept it
                    tracing::debug!(
                        "Received AUTH_SIGNATURE in CRC mode, accepting without verification"
                    );
                }

                // After verification, transition to COMPRESSION_CONNECTING for compression negotiation
                tracing::debug!("Transitioning to COMPRESSION_CONNECTING");
                Ok(StateResult::Transition(Box::new(
                    CompressionConnecting::new_with_encryption(
                        self.connection_mode,
                        self.session_key.clone(),
                        self.connection_secret.clone(),
                        self.global_id,
                        self.server_addr.clone(),
                        self.client_addr.clone(),
                    ),
                )))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in AUTH_CONNECTING_SIGN state",
                frame.preamble.tag
            ))),
        }
    }

    fn enter(&mut self) -> Result<StateResult> {
        // Send AUTH_SIGNATURE frame with pre-computed HMAC-SHA256 signature
        let auth_sig_frame = AuthSignatureFrame::new(self.our_signature.clone());
        let frame = create_frame_from_trait(&auth_sig_frame, Tag::AuthSignature);

        tracing::debug!(
            "Sending AUTH_SIGNATURE to server (signature: {} bytes)",
            self.our_signature.len()
        );

        Ok(StateResult::SendAndWait {
            frame,
            next_state: Box::new(AuthConnectingSign::new_with_encryption(
                self.connection_mode,
                self.session_key.clone(),
                self.connection_secret.clone(),
                self.our_signature.clone(),
                self.expected_server_signature.clone(),
                self.global_id,
                self.server_addr.clone(),
                self.client_addr.clone(),
            )),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SessionConnecting {
    // Connection information received from SERVER_IDENT
    pub peer_addrs: Vec<denc::EntityAddr>,
    pub peer_gid: u64,
    pub peer_global_seq: u64,
    pub negotiated_features: u64,
    // Encryption information from AUTH_DONE
    pub connection_mode: u32,
    pub session_key: Option<Bytes>,
    pub connection_secret: Option<Bytes>,
    // Expected server signature for verification
    pub expected_server_signature: Option<Bytes>,
    // Client information for CLIENT_IDENT
    pub our_global_id: u64,
    pub server_addr: denc::EntityAddr,
    pub client_addr: denc::EntityAddr,
}

impl Default for SessionConnecting {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionConnecting {
    pub fn new() -> Self {
        Self {
            peer_addrs: Vec::new(),
            peer_gid: 0,
            peer_global_seq: 0,
            negotiated_features: 0,
            connection_mode: crate::ConnectionMode::Crc.as_u32(),
            session_key: None,
            connection_secret: None,
            expected_server_signature: None,
            our_global_id: 0,
            server_addr: denc::EntityAddr::default(),
            client_addr: denc::EntityAddr::default(),
        }
    }

    pub fn new_with_encryption(
        connection_mode: u32,
        session_key: Option<Bytes>,
        connection_secret: Option<Bytes>,
        expected_server_signature: Option<Bytes>,
        our_global_id: u64,
        server_addr: denc::EntityAddr,
        client_addr: denc::EntityAddr,
    ) -> Self {
        Self {
            peer_addrs: Vec::new(),
            peer_gid: 0,
            peer_global_seq: 0,
            negotiated_features: 0,
            connection_mode,
            session_key,
            connection_secret,
            expected_server_signature,
            our_global_id,
            server_addr,
            client_addr,
        }
    }
}

impl State for SessionConnecting {
    fn kind(&self) -> StateKind {
        StateKind::SessionConnecting
    }

    fn expected_frames(&self) -> &[Tag] {
        &[
            Tag::ServerIdent,
            Tag::IdentMissingFeatures,
            Tag::Wait,
            Tag::AuthSignature,
        ]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::ServerIdent => {
                // Parse SERVER_IDENT frame
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let addrvec = denc::EntityAddrvec::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode addrs: {:?}", e))
                    })?;
                    let addrs = addrvec.addrs;
                    let gid = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode gid: {:?}", e))
                    })?;
                    let global_seq = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode global_seq: {:?}", e))
                    })?;
                    let features_supported = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!(
                            "Failed to decode features_supported: {:?}",
                            e
                        ))
                    })?;
                    let features_required = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!(
                            "Failed to decode features_required: {:?}",
                            e
                        ))
                    })?;
                    let flags = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode flags: {:?}", e))
                    })?;
                    let cookie = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode cookie: {:?}", e))
                    })?;

                    tracing::info!(
                        "Received SERVER_IDENT - addrs: {:?}, gid: {}, global_seq: {}, features_supported: 0x{:x}, features_required: 0x{:x}, flags: 0x{:x}, cookie: {}",
                        addrs,
                        gid,
                        global_seq,
                        features_supported,
                        features_required,
                        flags,
                        cookie
                    );

                    // Validate that we support the required features
                    // Use the features we advertised in CLIENT_IDENT, not just msgr2 features
                    const CEPH_FEATURE_MSG_ADDR2: u64 = 1 << 59;
                    const CEPH_FEATURE_SERVER_NAUTILUS: u64 = 1 << 61;
                    const CEPH_FEATURE_SERVER_OCTOPUS: u64 = 1 << 62;
                    let our_features: u64 = CEPH_FEATURE_MSG_ADDR2
                        | CEPH_FEATURE_SERVER_NAUTILUS
                        | CEPH_FEATURE_SERVER_OCTOPUS
                        | (0x3fffffffffffffff); // All features up to bit 61

                    let missing_features = features_required & !our_features;
                    if missing_features != 0 {
                        tracing::error!(
                            "Missing required features: 0x{:x} (required: 0x{:x}, we have: 0x{:x})",
                            missing_features,
                            features_required,
                            our_features
                        );
                        return Ok(StateResult::Fault(format!(
                            "Missing required features: 0x{:x}",
                            missing_features
                        )));
                    }

                    // Store connection information for future use
                    self.peer_addrs = addrs;
                    self.peer_gid = gid;
                    self.peer_global_seq = global_seq;
                    self.negotiated_features = our_features & features_supported;

                    tracing::info!(
                        "Session established successfully - negotiated features: 0x{:x}",
                        self.negotiated_features
                    );

                    // Session established successfully
                    Ok(StateResult::Ready)
                } else {
                    Err(Error::protocol_error("SERVER_IDENT frame missing payload"))
                }
            }
            Tag::IdentMissingFeatures => {
                Ok(StateResult::Fault("Missing required features".to_string()))
            }
            Tag::Wait => {
                // Server requested wait - continue waiting
                tracing::debug!("Server requested wait, continuing to wait for SERVER_IDENT");
                Ok(StateResult::Continue)
            }
            Tag::AuthSignature => {
                // Verify server's AUTH_SIGNATURE
                if let Some(ref expected_sig) = self.expected_server_signature {
                    // Extract server's signature from frame payload
                    if let Some(segment) = frame.segments.first() {
                        let mut payload = segment.clone();
                        let server_signature = Bytes::decode(&mut payload, 0).map_err(|e| {
                            Error::protocol_error(&format!(
                                "Failed to decode server AUTH_SIGNATURE: {:?}",
                                e
                            ))
                        })?;

                        // Verify signature matches expected
                        if server_signature == *expected_sig {
                            tracing::info!(
                                "✓ Server AUTH_SIGNATURE verified successfully ({} bytes)",
                                server_signature.len()
                            );
                            Ok(StateResult::Continue)
                        } else {
                            tracing::error!(
                                "✗ Server AUTH_SIGNATURE verification failed - expected {} bytes, got {} bytes",
                                expected_sig.len(),
                                server_signature.len()
                            );
                            Err(Error::protocol_error(
                                "Server AUTH_SIGNATURE verification failed",
                            ))
                        }
                    } else {
                        Err(Error::protocol_error(
                            "AUTH_SIGNATURE frame missing payload",
                        ))
                    }
                } else {
                    // No expected signature (CRC mode), just accept it
                    tracing::debug!(
                        "Received AUTH_SIGNATURE in CRC mode, accepting without verification"
                    );
                    Ok(StateResult::Continue)
                }
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in SESSION_CONNECTING state",
                frame.preamble.tag
            ))),
        }
    }

    fn enter(&mut self) -> Result<StateResult> {
        // Send CLIENT_IDENT frame with proper values
        // Use our real client address from the connection
        // Note: EntityAddrvec now includes marker byte (0x02) in encoding
        let addrs = denc::EntityAddrvec::with_addr(self.client_addr.clone());

        // Target address is the server we're connecting to
        let target_addr = self.server_addr.clone();

        // Global ID from authentication
        let gid = self.our_global_id as i64;

        // Features - basic msgr2 features plus common Ceph features
        // Include MSG_ADDR2 (required for msgr2) and other common features
        const CEPH_FEATURE_MSG_ADDR2: u64 = 1 << 59;
        const CEPH_FEATURE_SERVER_NAUTILUS: u64 = 1 << 61;
        const CEPH_FEATURE_SERVER_OCTOPUS: u64 = 1 << 62;

        let features_supported: u64 = CEPH_FEATURE_MSG_ADDR2
            | CEPH_FEATURE_SERVER_NAUTILUS
            | CEPH_FEATURE_SERVER_OCTOPUS
            | (0x3fffffffffffffff); // All features up to bit 61

        let features_required: u64 = CEPH_FEATURE_MSG_ADDR2;

        // Flags - default to 0 (non-lossy)
        let flags: u64 = 0;

        // Cookie - generate a random cookie
        let cookie: u64 = rand::random();

        tracing::debug!(
            "Sending CLIENT_IDENT: gid={}, target={:?}, client_addr={:?}, cookie={}",
            gid,
            target_addr,
            addrs,
            cookie
        );

        let client_ident = ClientIdentFrame::new(
            addrs.clone(),
            target_addr.clone(),
            gid,
            0, // global_seq - use 0 for new connections
            features_supported,
            features_required,
            flags,
            cookie,
        );
        let frame = create_frame_from_trait(&client_ident, Tag::ClientIdent);

        // Log the exact bytes we're encoding for CLIENT_IDENT
        if !frame.segments.is_empty() {
            let payload_bytes = &frame.segments[0];
            tracing::info!(
                "CLIENT_IDENT payload: {} bytes, hex: {}",
                payload_bytes.len(),
                hex::encode(&payload_bytes[..payload_bytes.len().min(128)])
            );
        }

        Ok(StateResult::SendAndWait {
            frame,
            next_state: Box::new(self.clone()),
        })
    }
}

// Server-side states (mirror of client states)

#[derive(Debug)]
pub struct HelloAccepting;

impl State for HelloAccepting {
    fn kind(&self) -> StateKind {
        StateKind::HelloAccepting
    }

    fn expected_frames(&self) -> &[Tag] {
        &[Tag::Hello]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::Hello => {
                // Send HELLO response and transition to auth
                let hello_frame = HelloFrame::new(
                    denc::EntityType::TYPE_MON.value() as u8,
                    denc::EntityAddr::default(),
                );
                let response_frame = create_frame_from_trait(&hello_frame, Tag::Hello);

                Ok(StateResult::SendFrame {
                    frame: response_frame,
                    next_state: Some(Box::new(AuthAccepting::new())),
                })
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in HELLO_ACCEPTING state",
                frame.preamble.tag
            ))),
        }
    }
}

#[derive(Debug)]
pub struct AuthAccepting;

impl Default for AuthAccepting {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthAccepting {
    pub fn new() -> Self {
        Self
    }
}

impl State for AuthAccepting {
    fn kind(&self) -> StateKind {
        StateKind::AuthAccepting
    }

    fn expected_frames(&self) -> &[Tag] {
        &[Tag::AuthRequest]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::AuthRequest => {
                // Process auth request and send AUTH_DONE
                let auth_done =
                    AuthDoneFrame::new(1001, crate::ConnectionMode::Crc.as_u32(), Bytes::new()); // global_id=1001
                let response_frame = create_frame_from_trait(&auth_done, Tag::AuthDone);

                Ok(StateResult::SendFrame {
                    frame: response_frame,
                    next_state: Some(Box::new(SessionAccepting::new())),
                })
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in AUTH_ACCEPTING state",
                frame.preamble.tag
            ))),
        }
    }
}

#[derive(Debug)]
pub struct SessionAccepting;

impl Default for SessionAccepting {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAccepting {
    pub fn new() -> Self {
        Self
    }
}

impl State for SessionAccepting {
    fn kind(&self) -> StateKind {
        StateKind::SessionAccepting
    }

    fn expected_frames(&self) -> &[Tag] {
        &[Tag::ClientIdent]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::ClientIdent => {
                // Send SERVER_IDENT and complete handshake
                let addrs = denc::EntityAddrvec::with_addr(denc::EntityAddr::default());
                let server_ident = ServerIdentFrame::new(addrs, 0, 0, 0, 0, 0, 0);
                let response_frame = create_frame_from_trait(&server_ident, Tag::ServerIdent);

                Ok(StateResult::SendFrame {
                    frame: response_frame,
                    next_state: None, // Transition to READY handled by state machine
                })
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in SESSION_ACCEPTING state",
                frame.preamble.tag
            ))),
        }
    }
}

#[derive(Debug)]
pub struct Ready;

impl State for Ready {
    fn kind(&self) -> StateKind {
        StateKind::Ready
    }

    fn expected_frames(&self) -> &[Tag] {
        &[Tag::Message, Tag::Keepalive2, Tag::Keepalive2Ack, Tag::Ack]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::Message => {
                // Messages should be handled by the application layer
                // The protocol layer just passes them through
                tracing::debug!("Received message in READY state");
                Ok(StateResult::Continue)
            }
            Tag::Keepalive2 => {
                // Parse keepalive and send ack with same timestamp
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let timestamp_sec = u32::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode timestamp_sec: {:?}", e))
                    })?;
                    let timestamp_nsec = u32::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode timestamp_nsec: {:?}", e))
                    })?;

                    tracing::debug!(
                        "Received KEEPALIVE2 with timestamp {}.{:09}",
                        timestamp_sec,
                        timestamp_nsec
                    );

                    // Send keepalive ack with the same timestamp
                    let ack_frame = Keepalive2AckFrame::new(timestamp_sec, timestamp_nsec);
                    let response_frame = create_frame_from_trait(&ack_frame, Tag::Keepalive2Ack);

                    Ok(StateResult::SendFrame {
                        frame: response_frame,
                        next_state: None, // Stay in READY state
                    })
                } else {
                    Err(Error::protocol_error("KEEPALIVE2 frame missing payload"))
                }
            }
            Tag::Keepalive2Ack => {
                // Handle keepalive ack - just log it
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let timestamp_sec = u32::decode(&mut payload, 0).unwrap_or(0);
                    let timestamp_nsec = u32::decode(&mut payload, 0).unwrap_or(0);

                    tracing::debug!(
                        "Received KEEPALIVE2_ACK with timestamp {}.{:09}",
                        timestamp_sec,
                        timestamp_nsec
                    );
                }
                Ok(StateResult::Continue)
            }
            Tag::Ack => {
                // Handle ACK frame
                tracing::debug!("Received ACK in READY state");
                Ok(StateResult::Continue)
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in READY state",
                frame.preamble.tag
            ))),
        }
    }
}

/// State machine context that manages state transitions
#[derive(Debug)]
pub struct StateMachine {
    current_state: Box<dyn State>,
    #[allow(dead_code)]
    is_client: bool,
    /// Frame decryptor for SECURE mode (connection_mode = 2)
    frame_decryptor: Option<Box<dyn crate::crypto::FrameDecryptor>>,
    /// Frame encryptor for SECURE mode (connection_mode = 2)
    frame_encryptor: Option<Box<dyn crate::crypto::FrameEncryptor>>,
    /// Pre-auth buffer tracking for AUTH_SIGNATURE
    /// Tracks all bytes received before AUTH_DONE for signature verification
    pre_auth_rxbuf: BytesMut,
    /// Tracks all bytes sent before AUTH_DONE for signature generation
    pre_auth_txbuf: BytesMut,
    /// Whether we're in pre-auth phase (before AUTH_DONE)
    pre_auth_enabled: bool,
    /// Session key from AUTH_DONE, used for HMAC-SHA256 signatures
    session_key: Option<Bytes>,
    /// Server address we're connecting to (for CLIENT_IDENT)
    server_addr: Option<denc::EntityAddr>,
    /// Our own client address (for CLIENT_IDENT)
    client_addr: Option<denc::EntityAddr>,
    /// Peer's supported msgr2 features from banner exchange
    peer_supported_features: u64,
    /// Connection configuration
    #[allow(dead_code)]
    config: crate::ConnectionConfig,
}

impl StateMachine {
    /// Create a new state machine for client connection
    pub fn new_client(config: crate::ConnectionConfig) -> Self {
        let preferred_modes = config.preferred_modes.clone();
        let auth_method = config.auth_method;
        Self {
            current_state: Box::new(BannerConnecting {
                local_supported_features: 3, // MSGR2 | REVISION_1
                local_required_features: 0,
                preferred_modes: preferred_modes.clone(),
                auth_method,
            }),
            is_client: true,
            frame_decryptor: None,
            frame_encryptor: None,
            pre_auth_rxbuf: BytesMut::new(),
            pre_auth_txbuf: BytesMut::new(),
            pre_auth_enabled: true,
            session_key: None,
            server_addr: None,
            client_addr: None,
            peer_supported_features: 0, // Will be set after banner exchange
            config,
        }
    }

    /// Set the server address (called from protocol.rs after connection)
    pub fn set_server_addr(&mut self, addr: denc::EntityAddr) {
        self.server_addr = Some(addr);
    }

    /// Set the client address (called from protocol.rs after connection)
    pub fn set_client_addr(&mut self, addr: denc::EntityAddr) {
        self.client_addr = Some(addr);
    }

    /// Set peer's supported msgr2 features (from banner exchange)
    pub fn set_peer_supported_features(&mut self, features: u64) {
        self.peer_supported_features = features;
        tracing::debug!(
            "Peer supported features set: 0x{:x} (REVISION_1={}, COMPRESSION={})",
            features,
            crate::has_msgr2_feature(features, crate::MSGR2_FEATURE_REVISION_1),
            crate::has_msgr2_feature(features, crate::MSGR2_FEATURE_COMPRESSION)
        );
    }

    /// Check if peer supports compression feature
    pub fn peer_supports_compression(&self) -> bool {
        crate::has_msgr2_feature(
            self.peer_supported_features,
            crate::MSGR2_FEATURE_COMPRESSION,
        )
    }

    /// Check if we (client) support compression feature
    pub fn we_support_compression(&self) -> bool {
        crate::has_msgr2_feature(
            self.config.supported_features,
            crate::MSGR2_FEATURE_COMPRESSION,
        )
    }

    /// Check if both we and peer support compression
    pub fn compression_negotiation_needed(&self) -> bool {
        self.we_support_compression() && self.peer_supports_compression()
    }

    /// Check if pre-auth recording is enabled
    pub fn is_pre_auth_enabled(&self) -> bool {
        self.pre_auth_enabled
    }

    /// Create a new state machine for server connection
    pub fn new_server() -> Self {
        Self {
            current_state: Box::new(HelloAccepting),
            is_client: false,
            frame_decryptor: None,
            frame_encryptor: None,
            pre_auth_rxbuf: BytesMut::new(),
            pre_auth_txbuf: BytesMut::new(),
            pre_auth_enabled: true,
            session_key: None,
            server_addr: None,
            client_addr: None,
            peer_supported_features: 0, // Will be set after banner exchange
            config: crate::ConnectionConfig::default(),
        }
    }

    /// Process an incoming frame
    pub fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        // Validate frame is expected in current state
        let expected = self.current_state.expected_frames();
        if !expected.is_empty() && !expected.contains(&frame.preamble.tag) {
            return Err(Error::protocol_error(&format!(
                "Frame {:?} not valid in state {}, expected one of {:?}",
                frame.preamble.tag,
                self.current_state.name(),
                expected
            )));
        }

        let result = self.current_state.handle_frame(frame)?;
        self.apply_result(result)
    }

    /// Enter current state (for initialization)
    pub fn enter(&mut self) -> Result<StateResult> {
        let result = self.current_state.enter()?;
        self.apply_result(result)
    }

    /// Get current state name
    pub fn current_state_name(&self) -> &'static str {
        self.current_state.name()
    }

    /// Get the current state kind (typed enum value)
    pub fn current_state_kind(&self) -> StateKind {
        self.current_state.kind()
    }

    /// Setup frame encryption/decryption handlers for SECURE mode
    /// Called after AUTH_DONE when connection_secret is available
    pub fn setup_encryption(&mut self, connection_secret: &[u8]) -> Result<()> {
        use crate::crypto::{parse_connection_secret, Aes128GcmDecryptor, Aes128GcmEncryptor};

        let (key, rx_nonce, tx_nonce) =
            parse_connection_secret(connection_secret).map_err(|e| {
                Error::protocol_error(&format!("Failed to parse connection_secret: {}", e))
            })?;

        // Setup decryptor (uses rx_nonce for receiving)
        let decryptor = Aes128GcmDecryptor::new(key.clone(), rx_nonce)
            .map_err(|e| Error::protocol_error(&format!("Failed to create decryptor: {}", e)))?;
        self.frame_decryptor = Some(Box::new(decryptor));

        // Setup encryptor (uses tx_nonce for sending)
        let encryptor = Aes128GcmEncryptor::new(key, tx_nonce)
            .map_err(|e| Error::protocol_error(&format!("Failed to create encryptor: {}", e)))?;
        self.frame_encryptor = Some(Box::new(encryptor));

        tracing::info!("Frame encryption/decryption handlers initialized for SECURE mode");

        Ok(())
    }

    /// Decrypt frame data if in SECURE mode
    pub fn decrypt_frame_data(&mut self, data: &[u8]) -> Result<Bytes> {
        if let Some(decryptor) = &mut self.frame_decryptor {
            decryptor
                .decrypt(data)
                .map_err(|e| Error::protocol_error(&format!("Frame decryption failed: {}", e)))
        } else {
            // No encryption, return as-is
            Ok(Bytes::copy_from_slice(data))
        }
    }

    /// Encrypt frame data if in SECURE mode
    pub fn encrypt_frame_data(&mut self, data: &[u8]) -> Result<Bytes> {
        if let Some(encryptor) = &mut self.frame_encryptor {
            encryptor
                .encrypt(data)
                .map_err(|e| Error::protocol_error(&format!("Frame encryption failed: {}", e)))
        } else {
            // No encryption, return as-is
            Ok(Bytes::copy_from_slice(data))
        }
    }

    /// Check if frame encryption is active
    pub fn has_encryption(&self) -> bool {
        self.frame_decryptor.is_some()
    }

    /// Record bytes sent during pre-auth phase (for AUTH_SIGNATURE)
    pub fn record_sent(&mut self, data: &[u8]) {
        if self.pre_auth_enabled {
            self.pre_auth_txbuf.extend_from_slice(data);
            tracing::trace!(
                "Pre-auth: recorded {} bytes to txbuf (enabled=true, total={})",
                data.len(),
                self.pre_auth_txbuf.len()
            );
        } else {
            tracing::trace!(
                "Pre-auth: skipped recording {} bytes to txbuf (enabled=false)",
                data.len()
            );
        }
    }

    /// Record bytes received during pre-auth phase (for AUTH_SIGNATURE)
    pub fn record_received(&mut self, data: &[u8]) {
        if self.pre_auth_enabled {
            self.pre_auth_rxbuf.extend_from_slice(data);
            tracing::trace!(
                "Pre-auth: recorded {} bytes to rxbuf (enabled=true, total={})",
                data.len(),
                self.pre_auth_rxbuf.len()
            );
        } else {
            tracing::trace!(
                "Pre-auth: skipped recording {} bytes to rxbuf (enabled=false)",
                data.len()
            );
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

    /// Get session key for HMAC computation
    pub fn get_session_key(&self) -> Option<&Bytes> {
        self.session_key.as_ref()
    }

    /// Clear pre-auth buffers (after AUTH_SIGNATURE exchange)
    pub fn clear_pre_auth_buffers(&mut self) {
        self.pre_auth_rxbuf.clear();
        self.pre_auth_txbuf.clear();
    }

    fn apply_result(&mut self, result: StateResult) -> Result<StateResult> {
        match result {
            StateResult::Transition(new_state) => {
                // Check if transitioning to AUTH_CONNECTING_SIGN or SESSION_CONNECTING with encryption
                if new_state.kind() == StateKind::AuthConnectingSign {
                    // Downcast to get access to connection_secret and session_key
                    let auth_sign = new_state
                        .as_any()
                        .downcast_ref::<AuthConnectingSign>()
                        .expect("State kind is AUTH_CONNECTING_SIGN but downcast failed");

                    // Extract parameters
                    let connection_mode = auth_sign.connection_mode;
                    let session_key = auth_sign.session_key.clone();
                    let connection_secret = auth_sign.connection_secret.clone();
                    let global_id = auth_sign.global_id;
                    let server_addr = self.server_addr.clone().unwrap_or_default();
                    let client_addr = self.client_addr.clone().unwrap_or_default();

                    // Compute HMAC-SHA256 signature of pre_auth_rxbuf using session_key
                    let our_signature = if let Some(ref key) = session_key {
                        use hmac::{Hmac, Mac};
                        use sha2::Sha256;

                        type HmacSha256 = Hmac<Sha256>;

                        tracing::debug!(
                            "Computing client AUTH_SIGNATURE: HMAC-SHA256(key={} bytes, rxbuf={} bytes)",
                            key.len(),
                            self.pre_auth_rxbuf.len()
                        );

                        let mut mac = HmacSha256::new_from_slice(key)
                            .map_err(|e| Error::protocol_error(&format!("Invalid key: {:?}", e)))?;
                        mac.update(&self.pre_auth_rxbuf);
                        let result = mac.finalize();
                        Bytes::copy_from_slice(&result.into_bytes())
                    } else {
                        // No session key, send zero signature
                        Bytes::from(vec![0u8; 32])
                    };

                    // Compute expected server signature (HMAC of pre_auth_txbuf)
                    let expected_server_signature = if let Some(ref key) = session_key {
                        use hmac::{Hmac, Mac};
                        use sha2::Sha256;

                        type HmacSha256 = Hmac<Sha256>;

                        let mut mac = HmacSha256::new_from_slice(key)
                            .map_err(|e| Error::protocol_error(&format!("Invalid key: {:?}", e)))?;
                        mac.update(&self.pre_auth_txbuf);
                        let result = mac.finalize();
                        Some(Bytes::copy_from_slice(&result.into_bytes()))
                    } else {
                        None
                    };

                    tracing::debug!(
                        "Computed AUTH_SIGNATURE: {} bytes from {} bytes rxbuf",
                        our_signature.len(),
                        self.pre_auth_rxbuf.len()
                    );
                    tracing::debug!(
                        "AUTH_SIGNATURE first 16 bytes: {:02x?}",
                        &our_signature[..16.min(our_signature.len())]
                    );
                    tracing::debug!(
                        "Computed expected server signature: {} bytes from {} bytes txbuf",
                        expected_server_signature
                            .as_ref()
                            .map(|s| s.len())
                            .unwrap_or(0),
                        self.pre_auth_txbuf.len()
                    );

                    // Complete pre-auth phase and store session_key
                    self.complete_pre_auth(session_key.clone());
                    tracing::debug!(
                        "Pre-auth phase completed: rxbuf={} bytes, txbuf={} bytes",
                        self.pre_auth_rxbuf.len(),
                        self.pre_auth_txbuf.len()
                    );
                    tracing::debug!(
                        "Pre-auth rxbuf first 64 bytes: {:02x?}",
                        &self.pre_auth_rxbuf[..64.min(self.pre_auth_rxbuf.len())]
                    );
                    tracing::debug!(
                        "Pre-auth txbuf first 64 bytes: {:02x?}",
                        &self.pre_auth_txbuf[..64.min(self.pre_auth_txbuf.len())]
                    );

                    // Save buffers to files for debugging
                    std::fs::write("/tmp/pre_auth_rxbuf.bin", &self.pre_auth_rxbuf[..]).ok();
                    std::fs::write("/tmp/pre_auth_txbuf.bin", &self.pre_auth_txbuf[..]).ok();
                    tracing::info!("Saved pre-auth buffers to /tmp/pre_auth_rxbuf.bin and /tmp/pre_auth_txbuf.bin");

                    if let Some(ref connection_secret) = connection_secret {
                        tracing::info!("Setting up frame encryption for SECURE mode");
                        self.setup_encryption(connection_secret)?;
                    }

                    // Create new AuthConnectingSign with computed signatures and addresses
                    let new_state_with_sig = Box::new(AuthConnectingSign::new_with_encryption(
                        connection_mode,
                        session_key,
                        connection_secret,
                        our_signature,
                        expected_server_signature,
                        global_id,
                        server_addr,
                        client_addr,
                    ));

                    self.current_state = new_state_with_sig;
                    // Enter new state
                    let enter_result = self.current_state.enter()?;
                    return self.apply_result(enter_result);
                } else if new_state.kind() == StateKind::CompressionConnecting {
                    // Check if compression negotiation is needed (both client and server must support it)
                    if !self.compression_negotiation_needed() {
                        tracing::info!(
                            "Compression negotiation not needed (our features: 0x{:x}, peer features: 0x{:x}), skipping compression negotiation",
                            self.config.supported_features,
                            self.peer_supported_features
                        );

                        // Skip compression negotiation and go directly to SESSION_CONNECTING
                        // Extract parameters from CompressionConnecting state
                        let compression_state = new_state
                            .as_any()
                            .downcast_ref::<CompressionConnecting>()
                            .expect("State kind is COMPRESSION_CONNECTING but downcast failed");

                        let new_session_state = Box::new(SessionConnecting::new_with_encryption(
                            compression_state.connection_mode,
                            compression_state.session_key.clone(),
                            compression_state.connection_secret.clone(),
                            None, // No expected signature needed
                            compression_state.global_id,
                            compression_state.server_addr.clone(),
                            compression_state.client_addr.clone(),
                        ));

                        self.current_state = new_session_state;
                        let enter_result = self.current_state.enter()?;
                        return self.apply_result(enter_result);
                    }
                    // If peer supports compression, proceed normally
                }

                self.current_state = new_state;

                // For SessionConnecting, call enter() to send CLIENT_IDENT
                // For other states, also call enter()
                let enter_result = self.current_state.enter()?;
                self.apply_result(enter_result)
            }
            StateResult::SendFrame { frame, next_state } => {
                if let Some(new_state) = next_state {
                    self.current_state = new_state;
                }
                Ok(StateResult::SendFrame {
                    frame,
                    next_state: None,
                })
            }
            StateResult::SendAndWait { frame, next_state } => {
                self.current_state = next_state;
                Ok(StateResult::SendAndWait {
                    frame,
                    next_state: Box::new(Ready), // Placeholder
                })
            }
            other => Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_compression_support_detection() {
        let mut sm = StateMachine::new_client(crate::ConnectionConfig::default());

        // Test 1: Peer with both REVISION_1 and COMPRESSION (0x3)
        sm.set_peer_supported_features(0x3);
        assert!(
            sm.peer_supports_compression(),
            "Should support compression with features=0x3"
        );

        // Test 2: Peer with only REVISION_1 (0x1)
        sm.set_peer_supported_features(0x1);
        assert!(
            !sm.peer_supports_compression(),
            "Should NOT support compression with features=0x1"
        );

        // Test 3: Peer with no features (0x0)
        sm.set_peer_supported_features(0x0);
        assert!(
            !sm.peer_supports_compression(),
            "Should NOT support compression with features=0x0"
        );

        // Test 4: Peer with only COMPRESSION (0x2) - unusual but valid
        sm.set_peer_supported_features(0x2);
        assert!(
            sm.peer_supports_compression(),
            "Should support compression with features=0x2"
        );
    }
}
