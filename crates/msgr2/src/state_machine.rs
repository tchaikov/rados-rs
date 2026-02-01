//! msgr2 protocol state machine implementation using the State pattern
//!
//! Based on ~/dev/ceph/src/msg/async/ProtocolV2.h and msgr2.rst documentation

use crate::error::{Error, Result};
use crate::frames::{
    AuthDoneFrame, AuthRequestFrame, AuthRequestMoreFrame, AuthSignatureFrame,
    CompressionRequestFrame, Frame, FrameTrait, HelloFrame, Keepalive2AckFrame, ServerIdentFrame,
    Tag,
};
use auth::EntityName;
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
    /// Reconnection established successfully - replay messages after msg_seq
    ReconnectReady {
        /// Last message sequence acknowledged by server
        msg_seq: u64,
    },
    /// Update last keepalive ACK timestamp
    SetKeepAliveAck(std::time::Instant),
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

/// Macro to implement boilerplate State trait methods
///
/// This macro eliminates the repetitive implementation of `kind()`, `expected_frames()`,
/// and `as_any()` methods that are identical across all State implementations.
///
/// # Example
/// ```ignore
/// impl_state_boilerplate!(BannerConnecting, StateKind::BannerConnecting, &[]);
/// impl_state_boilerplate!(HelloConnecting, StateKind::HelloConnecting, &[Tag::Hello]);
/// ```
macro_rules! impl_state_boilerplate {
    ($state_type:ty, $state_kind:expr, $expected_frames:expr) => {
        fn kind(&self) -> StateKind {
            $state_kind
        }

        fn expected_frames(&self) -> &[Tag] {
            $expected_frames
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    };
}

/// Connection establishment states (client-side)

#[derive(Clone)]
pub struct BannerConnecting {
    pub local_supported_features: u64,
    pub local_required_features: u64,
    pub preferred_modes: Vec<crate::ConnectionMode>,
    pub supported_auth_methods: Vec<crate::AuthMethod>,
    pub auth_provider: Option<Box<dyn auth::AuthProvider>>,
    pub service_id: u32,
}

impl std::fmt::Debug for BannerConnecting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BannerConnecting")
            .field("local_supported_features", &self.local_supported_features)
            .field("local_required_features", &self.local_required_features)
            .field("preferred_modes", &self.preferred_modes)
            .field("supported_auth_methods", &self.supported_auth_methods)
            .field(
                "auth_provider",
                &self.auth_provider.as_ref().map(|_| "Some(...)"),
            )
            .finish()
    }
}

impl State for BannerConnecting {
    impl_state_boilerplate!(BannerConnecting, StateKind::BannerConnecting, &[]);

    fn handle_frame(&mut self, _frame: Frame) -> Result<StateResult> {
        Err(Error::protocol_error(
            "No frames expected during banner exchange",
        ))
    }

    fn enter(&mut self) -> Result<StateResult> {
        // Banner is sent outside of frame protocol
        Ok(StateResult::Transition(Box::new(HelloConnecting::new(
            self.preferred_modes.clone(),
            self.supported_auth_methods.clone(),
            self.auth_provider.clone(),
            self.service_id,
        ))))
    }
}

#[derive(Clone)]
pub struct HelloConnecting {
    hello_sent: bool,
    preferred_modes: Vec<crate::ConnectionMode>,
    supported_auth_methods: Vec<crate::AuthMethod>,
    auth_provider: Option<Box<dyn auth::AuthProvider>>,
    service_id: u32,
}

impl std::fmt::Debug for HelloConnecting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HelloConnecting")
            .field("hello_sent", &self.hello_sent)
            .field("preferred_modes", &self.preferred_modes)
            .field("supported_auth_methods", &self.supported_auth_methods)
            .field(
                "auth_provider",
                &self.auth_provider.as_ref().map(|_| "Some(...)"),
            )
            .finish()
    }
}

impl HelloConnecting {
    pub fn new(
        preferred_modes: Vec<crate::ConnectionMode>,
        supported_auth_methods: Vec<crate::AuthMethod>,
        auth_provider: Option<Box<dyn auth::AuthProvider>>,
        service_id: u32,
    ) -> Self {
        Self {
            hello_sent: false,
            preferred_modes,
            supported_auth_methods,
            auth_provider,
            service_id,
        }
    }
}

impl State for HelloConnecting {
    impl_state_boilerplate!(HelloConnecting, StateKind::HelloConnecting, &[Tag::Hello]);

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
                    self.supported_auth_methods.clone(),
                    self.auth_provider.clone(),
                    self.service_id,
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
                denc::EntityType::TYPE_CLIENT.value() as u8,
                denc::EntityAddr::default(),
            );
            let frame = create_frame_from_trait(&hello_frame, Tag::Hello);

            Ok(StateResult::SendAndWait {
                frame,
                next_state: Box::new(HelloConnecting::new(
                    self.preferred_modes.clone(),
                    self.supported_auth_methods.clone(),
                    self.auth_provider.clone(),
                    self.service_id,
                )),
            })
        } else {
            Ok(StateResult::Continue)
        }
    }
}

#[derive(Clone)]
pub struct AuthConnecting {
    auth_method: crate::AuthMethod,
    auth_provider: Option<Box<dyn auth::AuthProvider>>,
    preferred_modes: Vec<crate::ConnectionMode>,
    /// Original list of supported auth methods for potential retry/negotiation
    supported_auth_methods: Vec<crate::AuthMethod>,
    /// Track methods we've tried to prevent infinite loops (max 3 attempts)
    tried_methods: Vec<crate::AuthMethod>,
    /// Service ID for service-based auth (OSD=4, MDS=2, MGR=16, MON=0)
    service_id: u32,
}

impl std::fmt::Debug for AuthConnecting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConnecting")
            .field("auth_method", &self.auth_method)
            .field(
                "auth_provider",
                &self.auth_provider.as_ref().map(|_| "Some(...)"),
            )
            .field("preferred_modes", &self.preferred_modes)
            .field("supported_auth_methods", &self.supported_auth_methods)
            .field("tried_methods", &self.tried_methods)
            .field("service_id", &self.service_id)
            .finish()
    }
}

impl AuthConnecting {
    /// Create new AuthConnecting state with authentication method negotiation
    ///
    /// # Arguments
    /// * `preferred_modes` - Connection modes to negotiate with server
    /// * `supported_auth_methods` - List of auth methods supported by client (in order of preference)
    /// * `auth_provider` - Optional authentication provider for CephX auth
    ///
    /// The client will send all supported auth methods to the server and negotiate
    /// the final method based on mutual support.
    pub fn new(
        preferred_modes: Vec<crate::ConnectionMode>,
        supported_auth_methods: Vec<crate::AuthMethod>,
        auth_provider: Option<Box<dyn auth::AuthProvider>>,
        service_id: u32,
    ) -> Self {
        // Determine the preferred auth method (first in the list)
        let preferred_auth_method = supported_auth_methods
            .first()
            .copied()
            .unwrap_or(crate::AuthMethod::None);

        tracing::debug!(
            "AuthConnecting::new with auth method {:?}, has_provider={}, service_id={}",
            preferred_auth_method,
            auth_provider.is_some(),
            service_id
        );

        Self {
            auth_method: preferred_auth_method,
            auth_provider,
            preferred_modes,
            supported_auth_methods,
            tried_methods: Vec::new(),
            service_id,
        }
    }

    /// Select auth method from server's allowed list that we also support
    fn negotiate_auth_method(&self, allowed_methods: &[u32]) -> Result<crate::AuthMethod> {
        let allowed: Vec<crate::AuthMethod> = allowed_methods
            .iter()
            .filter_map(|&m| crate::AuthMethod::try_from(m).ok())
            .collect();

        // Find first supported method that's in allowed list
        for method in &self.supported_auth_methods {
            if allowed.contains(method) {
                return Ok(*method);
            }
        }

        Err(Error::Protocol(format!(
            "No mutually supported auth method. Client: {:?}, Server: {:?}",
            self.supported_auth_methods, allowed
        )))
    }

    /// Negotiate connection modes from server's allowed list
    fn negotiate_connection_modes(&self, allowed_modes: &[u32]) -> Vec<crate::ConnectionMode> {
        let allowed: Vec<crate::ConnectionMode> = allowed_modes
            .iter()
            .filter_map(|&m| crate::ConnectionMode::try_from(m).ok())
            .collect();

        let negotiated: Vec<crate::ConnectionMode> = self
            .preferred_modes
            .iter()
            .filter(|&mode| allowed.contains(mode))
            .cloned()
            .collect();

        if !negotiated.is_empty() {
            return negotiated;
        }

        // Fallback: use server's first mode or CRC
        allowed
            .first()
            .cloned()
            .map(|m| vec![m])
            .unwrap_or_else(|| vec![crate::ConnectionMode::Crc])
    }
}

impl State for AuthConnecting {
    impl_state_boilerplate!(
        AuthConnecting,
        StateKind::AuthConnecting,
        &[Tag::AuthBadMethod, Tag::AuthReplyMore, Tag::AuthDone]
    );

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        tracing::debug!(
            "DEBUG: AuthConnecting received frame tag: {:?}",
            frame.preamble.tag
        );
        match frame.preamble.tag {
            Tag::AuthBadMethod => {
                // Parse frame
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error("AUTH_BAD_METHOD missing payload"));
                }

                let mut payload = frame.segments[0].clone();
                let rejected_method = u32::decode(&mut payload, 0)?;
                let result = i32::decode(&mut payload, 0)?;
                let allowed_methods = Vec::<u32>::decode(&mut payload, 0)?;
                let allowed_modes = Vec::<u32>::decode(&mut payload, 0)?;

                tracing::debug!(
                    "DEBUG: AUTH_BAD_METHOD - method={}, result={}, allowed={:?}, modes={:?}",
                    rejected_method,
                    result,
                    allowed_methods,
                    allowed_modes
                );
                tracing::debug!(
                    "Server rejected method {} (err={}), allowed_methods={:?}, allowed_modes={:?}",
                    rejected_method,
                    result,
                    allowed_methods,
                    allowed_modes
                );

                // Check retry limit
                if self.tried_methods.len() >= 3 {
                    return Err(Error::Protocol(format!(
                        "Auth retry limit exceeded. Tried: {:?}",
                        self.tried_methods
                    )));
                }

                // Record this attempt
                let mut new_tried = self.tried_methods.clone();
                new_tried.push(self.auth_method);

                // Negotiate new method
                let negotiated_method = self.negotiate_auth_method(&allowed_methods)?;

                // Prevent duplicate attempts
                if new_tried.contains(&negotiated_method) {
                    return Err(Error::Protocol(format!(
                        "Would retry with {:?} but already tried it. No working auth method.",
                        negotiated_method
                    )));
                }

                tracing::debug!(
                    "Negotiated auth method: {:?} (client={:?}, server={:?})",
                    negotiated_method,
                    self.supported_auth_methods,
                    allowed_methods
                );

                // Negotiate connection modes
                let negotiated_modes = self.negotiate_connection_modes(&allowed_modes);

                tracing::debug!("Negotiated connection modes: {:?}", negotiated_modes);

                // Create new AuthConnecting state with negotiated parameters
                let mut new_state = Self {
                    auth_method: negotiated_method,
                    auth_provider: self.auth_provider.clone(),
                    preferred_modes: negotiated_modes,
                    supported_auth_methods: self.supported_auth_methods.clone(),
                    tried_methods: Vec::new(),
                    service_id: self.service_id,
                };

                new_state.tried_methods = new_tried;

                // Transition back to AuthConnecting (will send new AUTH_REQUEST)
                Ok(StateResult::Transition(Box::new(new_state)))
            }
            Tag::AuthReplyMore => {
                // Handle CephX multi-round auth
                if let Some(provider) = &mut self.auth_provider {
                    if let Some(payload) = frame.segments.first() {
                        tracing::debug!("DEBUG: AUTH_REPLY_MORE payload length: {}", payload.len());
                        tracing::debug!(
                            "DEBUG: AUTH_REPLY_MORE payload hex (first 128 bytes): {}",
                            payload
                                .iter()
                                .take(128)
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join("")
                        );

                        // Call auth provider to handle the response
                        // For multi-round auth, handle_auth_response processes the challenge
                        // and we need to send another request
                        let _result = provider
                            .handle_auth_response(payload.clone(), 0, 0)
                            .map_err(|e| Error::protocol_error(&e.to_string()))?;

                        // Build the next auth payload (challenge response)
                        let auth_payload = provider
                            .build_auth_payload(0, self.service_id)
                            .map_err(|e| Error::protocol_error(&e.to_string()))?;

                        // For AuthRequestMore, we only send the auth_payload
                        let auth_frame = AuthRequestMoreFrame::new(auth_payload);
                        let response_frame =
                            create_frame_from_trait(&auth_frame, Tag::AuthRequestMore);

                        // Preserve auth_provider state by not creating new AuthConnecting
                        Ok(StateResult::SendFrame {
                            frame: response_frame,
                            next_state: None,
                        })
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
                    tracing::debug!("DEBUG: AUTH_DONE frame segment length: {}", segment.len());
                    tracing::debug!(
                        "DEBUG: AUTH_DONE frame segment hex (first 64 bytes): {}",
                        segment
                            .iter()
                            .take(64)
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join("")
                    );

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

                    tracing::debug!("DEBUG: Decoded from AUTH_DONE frame:");
                    tracing::debug!("DEBUG:   global_id: {}", global_id);
                    tracing::debug!("DEBUG:   con_mode: {}", con_mode);
                    tracing::debug!("DEBUG:   auth_payload length: {}", auth_payload.len());

                    tracing::info!(
                        "Authentication completed successfully - global_id: {}, connection_mode: {}",
                        global_id,
                        con_mode
                    );

                    // For AuthMethod::None, we don't need to extract session_key and connection_secret
                    if self.auth_method == crate::AuthMethod::None {
                        tracing::debug!(
                            "AuthMethod::None - no session key/connection secret needed"
                        );

                        // For no-auth with CRC mode, skip directly to session connecting
                        // For SECURE mode, we'd need signature exchange but that doesn't make sense with no-auth
                        if con_mode == crate::ConnectionMode::Secure.as_u32() {
                            tracing::warn!(
                                "SECURE mode with AuthMethod::None is unusual, skipping to session"
                            );
                        }

                        // Go directly to SessionConnecting
                        // The global_id and addresses will be set in apply_result()
                        Ok(StateResult::Transition(Box::new(
                            SessionConnecting::new_with_encryption(
                                con_mode,
                                None, // No session key for AuthMethod::None
                                None, // No connection secret for AuthMethod::None
                                None, // No expected server signature for AuthMethod::None
                                global_id,
                                denc::EntityAddr::default(), // Placeholder, will be set in apply_result
                                denc::EntityAddr::default(), // Placeholder, will be set in apply_result
                                rand::random(), // client_cookie - will be overridden in apply_result
                                0,              // server_cookie - will be set in apply_result
                                0,              // global_seq - will be set in apply_result
                                0,              // connect_seq - will be set in apply_result
                                0,              // in_seq - will be set in apply_result
                            ),
                        )))
                    } else {
                        // CephX authentication - extract session_key and connection_secret
                        let provider = self
                            .auth_provider
                            .as_mut()
                            .ok_or_else(|| Error::protocol_error("No auth provider available"))?;

                        let (session_key, connection_secret) = provider
                            .handle_auth_response(auth_payload, global_id, con_mode)
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
                                0, // Placeholder, will be replaced with actual peer_supported_features
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
                tracing::debug!("Sending AUTH_REQUEST with AuthMethod::None (no authentication)");
                // For no-auth, send empty payload
                (crate::AuthMethod::None.as_u32(), Bytes::new())
            }
            crate::AuthMethod::Cephx => {
                tracing::debug!("Sending AUTH_REQUEST with AuthMethod::Cephx");
                let provider = self
                    .auth_provider
                    .as_mut()
                    .ok_or_else(|| Error::protocol_error("No auth provider available"))?;
                let payload = provider
                    .build_auth_payload(0, self.service_id) // Initial request uses global_id=0
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

        let auth_frame = AuthRequestFrame::new(method, preferred_modes.clone(), auth_payload);
        let frame = create_frame_from_trait(&auth_frame, Tag::AuthRequest);

        Ok(StateResult::SendAndWait {
            frame,
            next_state: Box::new(self.clone()),
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
    /// Peer supported features (to check for compression support)
    pub peer_supported_features: u64,
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
    /// Negotiated compression algorithm (set after receiving COMPRESSION_DONE)
    pub compression_algorithm: Option<crate::compression::CompressionAlgorithm>,
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
            compression_algorithm: None,
        }
    }
}

impl State for CompressionConnecting {
    impl_state_boilerplate!(
        CompressionConnecting,
        StateKind::CompressionConnecting,
        &[Tag::CompressionDone]
    );

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

                    tracing::debug!(
                        "Received COMPRESSION_DONE - is_compress: {}, method: {}",
                        is_compress,
                        method
                    );

                    // Store compression algorithm
                    let compression_algorithm = if is_compress {
                        crate::compression::CompressionAlgorithm::from_u32(method)
                            .unwrap_or(crate::compression::CompressionAlgorithm::None)
                    } else {
                        crate::compression::CompressionAlgorithm::None
                    };

                    tracing::debug!(
                        "Compression negotiation complete: algorithm={:?}",
                        compression_algorithm
                    );

                    // Create a new CompressionConnecting state with the algorithm stored
                    let mut new_state = CompressionConnecting::new_with_encryption(
                        self.connection_mode,
                        self.session_key.clone(),
                        self.connection_secret.clone(),
                        self.global_id,
                        self.server_addr.clone(),
                        self.client_addr.clone(),
                    );
                    new_state.compression_algorithm = Some(compression_algorithm);

                    // Return a special result that will trigger compression setup in apply_result
                    Ok(StateResult::Transition(Box::new(new_state)))
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
        peer_supported_features: u64,
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
            peer_supported_features,
        }
    }
}

impl State for AuthConnectingSign {
    impl_state_boilerplate!(
        AuthConnectingSign,
        StateKind::AuthConnectingSign,
        &[Tag::AuthSignature]
    );

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        tracing::debug!(
            "DEBUG: AuthConnectingSign::handle_frame received tag: {:?}",
            frame.preamble.tag
        );
        match frame.preamble.tag {
            Tag::AuthSignature => {
                tracing::debug!("DEBUG: Received AUTH_SIGNATURE from server");
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

                // After verification, transition based on compression support
                tracing::debug!(
                    "DEBUG: peer_supported_features = 0x{:x}",
                    self.peer_supported_features
                );
                if crate::has_msgr2_feature(
                    self.peer_supported_features,
                    crate::MSGR2_FEATURE_COMPRESSION,
                ) {
                    tracing::debug!(
                        "Peer supports COMPRESSION, transitioning to COMPRESSION_CONNECTING"
                    );
                    tracing::debug!(
                        "DEBUG: Peer supports COMPRESSION, transitioning to COMPRESSION_CONNECTING"
                    );
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
                } else {
                    tracing::debug!("Peer does not support COMPRESSION, transitioning directly to SESSION_CONNECTING");
                    tracing::debug!("DEBUG: Peer does NOT support COMPRESSION, transitioning directly to SESSION_CONNECTING");
                    Ok(StateResult::Transition(Box::new(
                        SessionConnecting::new_with_encryption(
                            self.connection_mode,
                            self.session_key.clone(),
                            self.connection_secret.clone(),
                            None, // No expected signature needed anymore
                            self.global_id,
                            self.server_addr.clone(),
                            self.client_addr.clone(),
                            rand::random(), // client_cookie - will be overridden in apply_result
                            0,              // server_cookie - will be set in apply_result
                            0,              // global_seq - will be set in apply_result
                            0,              // connect_seq - will be set in apply_result
                            0,              // in_seq - will be set in apply_result
                        ),
                    )))
                }
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in AUTH_CONNECTING_SIGN state",
                frame.preamble.tag
            ))),
        }
    }

    fn enter(&mut self) -> Result<StateResult> {
        // Send AUTH_SIGNATURE frame with pre-computed HMAC-SHA256 signature
        tracing::debug!("DEBUG: AuthConnectingSign::enter - Sending AUTH_SIGNATURE to server");
        tracing::debug!(
            "DEBUG:   signature length: {} bytes",
            self.our_signature.len()
        );
        tracing::debug!(
            "DEBUG:   signature hex (first 32 bytes): {}",
            self.our_signature
                .iter()
                .take(32)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
        );

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
                self.peer_supported_features,
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
    // Session cookies for reconnection
    pub client_cookie: u64,
    pub server_cookie: u64,
    pub global_seq: u64,
    pub connect_seq: u64,
    pub in_seq: u64,
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
            client_cookie: rand::random(),
            server_cookie: 0,
            global_seq: 0,
            connect_seq: 0,
            in_seq: 0,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_encryption(
        connection_mode: u32,
        session_key: Option<Bytes>,
        connection_secret: Option<Bytes>,
        expected_server_signature: Option<Bytes>,
        our_global_id: u64,
        server_addr: denc::EntityAddr,
        client_addr: denc::EntityAddr,
        client_cookie: u64,
        server_cookie: u64,
        global_seq: u64,
        connect_seq: u64,
        in_seq: u64,
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
            client_cookie,
            server_cookie,
            global_seq,
            connect_seq,
            in_seq,
        }
    }
}

impl State for SessionConnecting {
    impl_state_boilerplate!(
        SessionConnecting,
        StateKind::SessionConnecting,
        &[
            Tag::ServerIdent,
            Tag::IdentMissingFeatures,
            Tag::Wait,
            Tag::AuthSignature,
            Tag::SessionReconnectOk,
            Tag::SessionRetry,
            Tag::SessionRetryGlobal,
            Tag::SessionReset,
        ]
    );

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        tracing::debug!(
            "DEBUG: SessionConnecting::handle_frame received tag: {:?}",
            frame.preamble.tag
        );
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

                    tracing::debug!(
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
            Tag::SessionReconnectOk => {
                // Server accepted our reconnection
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let msg_seq = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode msg_seq: {:?}", e))
                    })?;

                    tracing::info!(
                        "✓ Reconnection successful! Server acknowledged up to msg_seq={}",
                        msg_seq
                    );

                    // Return ReconnectReady with msg_seq so Connection can replay unacknowledged messages
                    Ok(StateResult::ReconnectReady { msg_seq })
                } else {
                    Err(Error::protocol_error(
                        "SESSION_RECONNECT_OK frame missing payload",
                    ))
                }
            }
            Tag::SessionRetry => {
                // Server wants us to retry with a new connect_seq
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let server_connect_seq = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode connect_seq: {:?}", e))
                    })?;

                    tracing::warn!(
                        "Server sent SESSION_RETRY: server_connect_seq={}, our_connect_seq={}. Incrementing and retrying.",
                        server_connect_seq,
                        self.connect_seq
                    );
                    tracing::debug!(
                        "DEBUG: SESSION_RETRY received, incrementing connect_seq and retrying"
                    );

                    // Increment connect_seq and resend RECONNECT
                    self.connect_seq += 1;

                    // Re-enter to send reconnect again
                    self.enter()
                } else {
                    Err(Error::protocol_error("SESSION_RETRY frame missing payload"))
                }
            }
            Tag::SessionRetryGlobal => {
                // Server wants us to retry with a new global_seq
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let server_global_seq = u64::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode global_seq: {:?}", e))
                    })?;

                    tracing::warn!(
                        "Server sent SESSION_RETRY_GLOBAL: server_global_seq={}, our_global_seq={}. Updating and retrying.",
                        server_global_seq,
                        self.global_seq
                    );
                    tracing::debug!(
                        "DEBUG: SESSION_RETRY_GLOBAL received, updating global_seq and retrying"
                    );

                    // Update global_seq to server's value
                    self.global_seq = server_global_seq.max(self.global_seq + 1);

                    // Re-enter to send reconnect again
                    self.enter()
                } else {
                    Err(Error::protocol_error(
                        "SESSION_RETRY_GLOBAL frame missing payload",
                    ))
                }
            }
            Tag::SessionReset => {
                // Server wants us to reset the session and start fresh
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let full = bool::decode(&mut payload, 0).map_err(|e| {
                        Error::protocol_error(&format!("Failed to decode full flag: {:?}", e))
                    })?;

                    tracing::warn!(
                        "Server sent SESSION_RESET (full={}). Resetting session and sending CLIENT_IDENT",
                        full
                    );
                    tracing::debug!(
                        "DEBUG: SESSION_RESET received (full={}), resetting session",
                        full
                    );

                    // Reset session cookies (server_cookie = 0 will trigger CLIENT_IDENT in enter())
                    self.server_cookie = 0;
                    if full {
                        // Full reset - also reset sequences
                        self.global_seq = 0;
                        self.connect_seq = 0;
                        self.in_seq = 0;
                    }

                    // Re-enter to send CLIENT_IDENT
                    self.enter()
                } else {
                    Err(Error::protocol_error("SESSION_RESET frame missing payload"))
                }
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in SESSION_CONNECTING state",
                frame.preamble.tag
            ))),
        }
    }

    fn enter(&mut self) -> Result<StateResult> {
        tracing::debug!("DEBUG: SessionConnecting::enter() called");
        tracing::debug!("DEBUG:   our_global_id={}", self.our_global_id);
        tracing::debug!("DEBUG:   connection_mode={}", self.connection_mode);
        tracing::debug!("DEBUG:   client_cookie={}", self.client_cookie);
        tracing::debug!("DEBUG:   server_cookie={}", self.server_cookie);
        tracing::debug!(
            "DEBUG:   has_connection_secret={}",
            self.connection_secret.is_some()
        );

        // Check if we're reconnecting to an existing session
        if self.server_cookie != 0 {
            // Reconnecting to existing session - send SESSION_RECONNECT
            tracing::debug!(
                "Reconnecting to existing session: client_cookie={}, server_cookie={}, global_seq={}, connect_seq={}, in_seq={}",
                self.client_cookie,
                self.server_cookie,
                self.global_seq,
                self.connect_seq,
                self.in_seq
            );

            let addrs = denc::EntityAddrvec::with_addr(self.client_addr.clone());

            let reconnect = crate::frames::SessionReconnectFrame::new(
                addrs.clone(),
                self.client_cookie,
                self.server_cookie,
                self.global_seq,
                self.connect_seq,
                self.in_seq,
            );

            let frame = create_frame_from_trait(&reconnect, Tag::SessionReconnect);

            tracing::debug!("DEBUG: Created SESSION_RECONNECT frame");
            tracing::debug!("DEBUG:   addrs: {:?}", addrs);
            tracing::debug!("DEBUG:   client_cookie: {}", self.client_cookie);
            tracing::debug!("DEBUG:   server_cookie: {}", self.server_cookie);
            tracing::debug!("DEBUG:   global_seq: {}", self.global_seq);
            tracing::debug!("DEBUG:   connect_seq: {}", self.connect_seq);
            tracing::debug!("DEBUG:   in_seq (msg_seq): {}", self.in_seq);

            Ok(StateResult::SendAndWait {
                frame,
                next_state: Box::new(self.clone()),
            })
        } else {
            // New connection - send CLIENT_IDENT
            tracing::debug!("Starting new session: client_cookie={}", self.client_cookie);

            let addrs = denc::EntityAddrvec::with_addr(self.client_addr.clone());
            let target_addr = self.server_addr.clone();
            let gid = self.our_global_id as i64;

            // Features - basic msgr2 features plus common Ceph features
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

            tracing::debug!(
                "Sending CLIENT_IDENT: gid={}, target={:?}, client_addr={:?}, client_cookie={}",
                gid,
                target_addr,
                addrs,
                self.client_cookie
            );

            let client_ident = crate::frames::ClientIdentFrame::new(
                addrs.clone(),
                target_addr.clone(),
                gid,
                self.global_seq,
                features_supported,
                features_required,
                flags,
                self.client_cookie,
            );
            let frame = create_frame_from_trait(&client_ident, Tag::ClientIdent);

            tracing::debug!(
                "DEBUG: Created CLIENT_IDENT frame, {} segments",
                frame.segments.len()
            );
            tracing::debug!("DEBUG: CLIENT_IDENT values:");
            tracing::debug!("DEBUG:   addrs: {:?}", addrs);
            tracing::debug!("DEBUG:   target_addr: {:?}", target_addr);
            tracing::debug!("DEBUG:   gid: {}", gid);
            tracing::debug!("DEBUG:   global_seq: {}", self.global_seq);
            tracing::debug!("DEBUG:   features_supported: 0x{:x}", features_supported);
            tracing::debug!("DEBUG:   features_required: 0x{:x}", features_required);
            tracing::debug!("DEBUG:   flags: 0x{:x}", flags);
            tracing::debug!("DEBUG:   cookie: {}", self.client_cookie);

            if !frame.segments.is_empty() {
                let payload_bytes = &frame.segments[0];
                tracing::debug!(
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
}

// Server-side states (mirror of client states)

#[derive(Debug)]
pub struct HelloAccepting;

impl State for HelloAccepting {
    impl_state_boilerplate!(HelloAccepting, StateKind::HelloAccepting, &[Tag::Hello]);

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::Hello => {
                // Send HELLO response and transition to auth
                let hello_frame = HelloFrame::new(
                    denc::EntityType::TYPE_CLIENT.value() as u8,
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
pub struct AuthAccepting {
    /// Server-side authentication handler
    auth_handler: Option<auth::CephXServerHandler>,
    /// Entity name from initial request
    entity_name: Option<EntityName>,
    /// Global ID assigned to client
    global_id: Option<u64>,
    /// Authentication phase (0 = initial, 1 = challenge response)
    phase: u8,
}

impl Default for AuthAccepting {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthAccepting {
    pub fn new() -> Self {
        Self {
            auth_handler: None,
            entity_name: None,
            global_id: None,
            phase: 0,
        }
    }

    pub fn with_handler(auth_handler: auth::CephXServerHandler) -> Self {
        Self {
            auth_handler: Some(auth_handler),
            entity_name: None,
            global_id: None,
            phase: 0,
        }
    }
}

impl State for AuthAccepting {
    impl_state_boilerplate!(AuthAccepting, StateKind::AuthAccepting, &[Tag::AuthRequest]);

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::AuthRequest => {
                // Extract payload from frame
                // AUTH_REQUEST frame has payload in segment 0
                let payload = if !frame.segments.is_empty() {
                    &frame.segments[0]
                } else {
                    return Err(Error::protocol_error("AUTH_REQUEST frame has no payload"));
                };

                if self.phase == 0 {
                    // Phase 1: Initial authentication request
                    // Client sends: auth_mode + entity_name + global_id
                    // Server responds with: server_challenge

                    if let Some(ref mut handler) = self.auth_handler {
                        let (entity_name, global_id, challenge_payload) = handler
                            .handle_initial_request(payload)
                            .map_err(|e| Error::protocol_error(&format!("Auth failed: {}", e)))?;

                        tracing::debug!(
                            "Server: Initial auth request from {}, assigned global_id {}",
                            entity_name,
                            global_id
                        );

                        // Store for next phase
                        self.entity_name = Some(entity_name);
                        self.global_id = Some(global_id);
                        self.phase = 1;

                        // Send AUTH_REPLY_MORE with server challenge
                        let auth_more = AuthRequestMoreFrame::new(challenge_payload);
                        let response_frame =
                            create_frame_from_trait(&auth_more, Tag::AuthReplyMore);

                        Ok(StateResult::SendFrame {
                            frame: response_frame,
                            next_state: None, // Stay in AuthAccepting for phase 2
                        })
                    } else {
                        // No auth handler - send AUTH_DONE with no authentication
                        tracing::warn!(
                            "Server: No auth handler configured, skipping authentication"
                        );
                        let auth_done = AuthDoneFrame::new(
                            1001,
                            crate::ConnectionMode::Crc.as_u32(),
                            Bytes::new(),
                        );
                        let response_frame = create_frame_from_trait(&auth_done, Tag::AuthDone);

                        Ok(StateResult::SendFrame {
                            frame: response_frame,
                            next_state: Some(Box::new(SessionAccepting::new())),
                        })
                    }
                } else {
                    // Phase 2: Challenge response
                    // Client sends: CephXRequestHeader + CephXAuthenticate
                    // Server responds with: session_key + service_tickets

                    if let (Some(ref mut handler), Some(ref entity_name), Some(global_id)) =
                        (&mut self.auth_handler, &self.entity_name, self.global_id)
                    {
                        let (session_key, auth_payload) = handler
                            .handle_authenticate(entity_name, global_id, payload)
                            .map_err(|e| Error::protocol_error(&format!("Auth failed: {}", e)))?;

                        tracing::info!("Server: Client {} authenticated successfully", entity_name);

                        // Build AUTH_DONE response
                        let auth_done_payload = handler
                            .build_auth_done_response(
                                global_id,
                                crate::ConnectionMode::Crc.as_u32() as u8,
                                &session_key,
                                auth_payload,
                            )
                            .map_err(|e| {
                                Error::protocol_error(&format!("Failed to build AUTH_DONE: {}", e))
                            })?;

                        let auth_done = AuthDoneFrame::new(
                            global_id,
                            crate::ConnectionMode::Crc.as_u32(),
                            auth_done_payload,
                        );
                        let response_frame = create_frame_from_trait(&auth_done, Tag::AuthDone);

                        Ok(StateResult::SendFrame {
                            frame: response_frame,
                            next_state: Some(Box::new(SessionAccepting::new())),
                        })
                    } else {
                        Err(Error::protocol_error(
                            "Invalid auth state - missing handler or entity info",
                        ))
                    }
                }
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
    impl_state_boilerplate!(
        SessionAccepting,
        StateKind::SessionAccepting,
        &[Tag::ClientIdent]
    );

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::ClientIdent => {
                // Parse CLIENT_IDENT to get client information
                // For now, just send a default SERVER_IDENT response
                let addrs = denc::EntityAddrvec::with_addr(denc::EntityAddr::default());
                let server_ident = ServerIdentFrame::new(addrs, 0, 0, 0, 0, 0, 0);
                let response_frame = create_frame_from_trait(&server_ident, Tag::ServerIdent);

                // After sending SERVER_IDENT, transition to Ready state
                // The state machine will handle the transition after the frame is sent
                Ok(StateResult::SendFrame {
                    frame: response_frame,
                    next_state: Some(Box::new(Ready)),
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
    impl_state_boilerplate!(
        Ready,
        StateKind::Ready,
        &[Tag::Message, Tag::Keepalive2, Tag::Keepalive2Ack, Tag::Ack]
    );

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
                // Handle keepalive ack - update timestamp for timeout detection
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
                // Update last_keepalive_ack timestamp for timeout detection
                Ok(StateResult::SetKeepAliveAck(std::time::Instant::now()))
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
    /// Compression context for frame compression/decompression
    compression_ctx: Option<crate::compression::CompressionContext>,
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
    /// Preserved auth provider (kept across state transitions)
    preserved_auth_provider: Option<Box<dyn auth::AuthProvider>>,
    /// Server-side auth handler (for server connections only)
    server_auth_handler: Option<auth::CephXServerHandler>,
    /// Global ID assigned by the server during authentication
    /// Used to uniquely identify this client session
    global_id: u64,
    // Session state for reconnection
    /// Client cookie - generated on first connection
    client_cookie: u64,
    /// Server cookie - assigned by server in SERVER_IDENT
    server_cookie: u64,
    /// Global sequence number
    global_seq: u64,
    /// Connection sequence number
    connect_seq: u64,
    /// Last received message sequence number
    in_seq: u64,
    /// Last time we received a Keepalive2Ack (for timeout detection)
    last_keepalive_ack: Option<std::time::Instant>,
}

impl StateMachine {
    /// Create a new state machine for client connection
    pub fn new_client(config: crate::ConnectionConfig) -> Self {
        let preferred_modes = config.preferred_modes.clone();
        let supported_auth_methods = config.supported_auth_methods.clone();
        let auth_provider = config.auth_provider.clone();
        let service_id = config.service_id;
        // Clone the auth provider for preservation across state transitions
        let preserved_auth_provider = auth_provider.clone();
        Self {
            current_state: Box::new(BannerConnecting {
                local_supported_features: 3, // MSGR2 | REVISION_1
                local_required_features: 0,
                preferred_modes: preferred_modes.clone(),
                supported_auth_methods,
                auth_provider,
                service_id,
            }),
            is_client: true,
            frame_decryptor: None,
            frame_encryptor: None,
            compression_ctx: None, // Will be set after compression negotiation
            pre_auth_rxbuf: BytesMut::new(),
            pre_auth_txbuf: BytesMut::new(),
            pre_auth_enabled: true,
            session_key: None,
            server_addr: None,
            client_addr: None,
            peer_supported_features: 0, // Will be set after banner exchange
            config,
            preserved_auth_provider,
            server_auth_handler: None, // Client doesn't use server auth handler
            global_id: 0,              // Will be set during authentication
            client_cookie: rand::random(), // Generate random client cookie
            server_cookie: 0,          // Will be assigned by server
            global_seq: 0,
            connect_seq: 0,
            in_seq: 0,
            last_keepalive_ack: None, // Will be set when we receive first Keepalive2Ack
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

    /// Set session cookies for reconnection (called when resuming a session)
    pub fn set_session_cookies(
        &mut self,
        client_cookie: u64,
        server_cookie: u64,
        global_seq: u64,
        connect_seq: u64,
        in_seq: u64,
    ) {
        self.client_cookie = client_cookie;
        self.server_cookie = server_cookie;
        self.global_seq = global_seq;
        self.connect_seq = connect_seq;
        self.in_seq = in_seq;
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

    /// Get the global_id assigned during authentication
    /// Returns 0 if authentication hasn't completed yet
    pub fn global_id(&self) -> u64 {
        self.global_id
    }

    /// Get the last keepalive ACK timestamp
    /// Returns None if no keepalive ACK has been received yet
    pub fn last_keepalive_ack(&self) -> Option<std::time::Instant> {
        self.last_keepalive_ack
    }

    /// Create a new state machine for server connection
    pub fn new_server() -> Self {
        Self::new_server_with_auth(None)
    }

    /// Create a new state machine for server connection with optional auth handler
    pub fn new_server_with_auth(auth_handler: Option<auth::CephXServerHandler>) -> Self {
        Self {
            current_state: Box::new(HelloAccepting),
            is_client: false,
            frame_decryptor: None,
            frame_encryptor: None,
            compression_ctx: None, // Will be set after compression negotiation
            pre_auth_rxbuf: BytesMut::new(),
            pre_auth_txbuf: BytesMut::new(),
            pre_auth_enabled: true,
            session_key: None,
            server_addr: None,
            client_addr: None,
            peer_supported_features: 0, // Will be set after banner exchange
            config: crate::ConnectionConfig::default(),
            preserved_auth_provider: None, // Server doesn't use auth provider
            server_auth_handler: auth_handler,
            global_id: 0, // Server doesn't have a global_id (it assigns them to clients)
            client_cookie: 0, // Will be received from client
            server_cookie: rand::random(), // Generate random server cookie
            global_seq: 0,
            connect_seq: 0,
            in_seq: 0,
            last_keepalive_ack: None,
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

        // Setup decryptor and encryptor with nonces
        // Client uses crossed=false (no swap): rx_nonce for RX, tx_nonce for TX
        // Server uses crossed=true (swapped): tx_nonce for RX, rx_nonce for TX
        let decryptor = Aes128GcmDecryptor::new(key.clone(), rx_nonce)
            .map_err(|e| Error::protocol_error(&format!("Failed to create decryptor: {}", e)))?;
        self.frame_decryptor = Some(Box::new(decryptor));

        let encryptor = Aes128GcmEncryptor::new(key, tx_nonce)
            .map_err(|e| Error::protocol_error(&format!("Failed to create encryptor: {}", e)))?;
        self.frame_encryptor = Some(Box::new(encryptor));

        tracing::info!("Frame encryption/decryption handlers initialized for SECURE mode");

        Ok(())
    }

    /// Decrypt frame data if in SECURE mode
    pub fn decrypt_frame_data(&mut self, data: &[u8]) -> Result<Bytes> {
        tracing::debug!(
            "DEBUG: decrypt_frame_data called, has_decryptor={}, data_len={}",
            self.frame_decryptor.is_some(),
            data.len()
        );
        if let Some(decryptor) = &mut self.frame_decryptor {
            tracing::debug!(
                "DEBUG: Attempting AES-GCM decryption of {} bytes",
                data.len()
            );
            let result = decryptor
                .decrypt(data)
                .map_err(|e| Error::protocol_error(&format!("Frame decryption failed: {}", e)));
            if let Err(ref e) = result {
                tracing::debug!("DEBUG: Decryption FAILED: {:?}", e);
            } else {
                tracing::debug!("DEBUG: Decryption SUCCESS");
            }
            result
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

    /// Check if compression is enabled
    pub fn has_compression(&self) -> bool {
        self.compression_ctx.is_some()
    }

    /// Get compression context reference
    pub fn compression_ctx(&self) -> Option<&crate::compression::CompressionContext> {
        self.compression_ctx.as_ref()
    }

    /// Setup compression with the negotiated algorithm
    pub fn setup_compression(&mut self, algorithm: crate::compression::CompressionAlgorithm) {
        tracing::info!("Setting up compression with algorithm: {:?}", algorithm);
        self.compression_ctx = Some(crate::compression::CompressionContext::new(algorithm));
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

    /// Get a clone of the authenticated auth provider
    ///
    /// This returns the preserved auth provider that was stored during initialization.
    /// The provider contains the session and service tickets obtained during authentication.
    pub fn get_auth_provider(&self) -> Option<Box<dyn auth::AuthProvider>> {
        self.preserved_auth_provider.clone()
    }

    /// Clear pre-auth buffers (after AUTH_SIGNATURE exchange)
    pub fn clear_pre_auth_buffers(&mut self) {
        self.pre_auth_rxbuf.clear();
        self.pre_auth_txbuf.clear();
    }

    fn apply_result(&mut self, result: StateResult) -> Result<StateResult> {
        match result {
            StateResult::Transition(new_state) => {
                // Before transitioning, try to preserve the auth provider from the current state
                // This is important for AUTH_CONNECTING -> AUTH_CONNECTING_SIGN transition
                // where the auth provider has been updated with session and tickets

                // Try all state types that might have auth_provider
                if let Some(auth_connecting) =
                    self.current_state.as_any().downcast_ref::<AuthConnecting>()
                {
                    if let Some(ref provider) = auth_connecting.auth_provider {
                        tracing::debug!("Preserving auth provider from AuthConnecting");
                        self.preserved_auth_provider = Some(provider.clone_box());
                    }
                } else if let Some(hello_connecting) = self
                    .current_state
                    .as_any()
                    .downcast_ref::<HelloConnecting>()
                {
                    if let Some(ref provider) = hello_connecting.auth_provider {
                        tracing::debug!("Preserving auth provider from HelloConnecting");
                        self.preserved_auth_provider = Some(provider.clone_box());
                    }
                }

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

                    // Store global_id in StateMachine for later access
                    self.global_id = global_id;
                    tracing::debug!("Stored global_id {} in StateMachine", global_id);

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
                    tracing::debug!("Saved pre-auth buffers to /tmp/pre_auth_rxbuf.bin and /tmp/pre_auth_txbuf.bin");

                    if let Some(ref connection_secret) = connection_secret {
                        tracing::debug!("Setting up frame encryption for SECURE mode");
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
                        self.peer_supported_features,
                    ));

                    self.current_state = new_state_with_sig;
                    // Enter new state
                    let enter_result = self.current_state.enter()?;
                    return self.apply_result(enter_result);
                } else if new_state.kind() == StateKind::CompressionConnecting {
                    // Check if this is after COMPRESSION_DONE (has algorithm set)
                    let compression_state = new_state
                        .as_any()
                        .downcast_ref::<CompressionConnecting>()
                        .expect("State kind is COMPRESSION_CONNECTING but downcast failed");

                    if let Some(algorithm) = compression_state.compression_algorithm {
                        // COMPRESSION_DONE received, set up compression and transition to SESSION_CONNECTING
                        tracing::debug!("Setting up compression with algorithm: {:?}", algorithm);
                        self.setup_compression(algorithm);

                        // Transition to SESSION_CONNECTING
                        let new_session_state = Box::new(SessionConnecting::new_with_encryption(
                            compression_state.connection_mode,
                            compression_state.session_key.clone(),
                            compression_state.connection_secret.clone(),
                            None, // No expected signature needed
                            compression_state.global_id,
                            compression_state.server_addr.clone(),
                            compression_state.client_addr.clone(),
                            self.client_cookie,
                            self.server_cookie,
                            self.global_seq,
                            self.connect_seq,
                            self.in_seq,
                        ));

                        self.global_id = compression_state.global_id;
                        self.current_state = new_session_state;
                        let enter_result = self.current_state.enter()?;
                        return self.apply_result(enter_result);
                    } else if !self.compression_negotiation_needed() {
                        // Check if compression negotiation is needed (both client and server must support it)
                        tracing::debug!(
                            "Compression negotiation not needed (our features: 0x{:x}, peer features: 0x{:x}), skipping compression negotiation",
                            self.config.supported_features,
                            self.peer_supported_features
                        );

                        // Skip compression negotiation and go directly to SESSION_CONNECTING
                        let new_session_state = Box::new(SessionConnecting::new_with_encryption(
                            compression_state.connection_mode,
                            compression_state.session_key.clone(),
                            compression_state.connection_secret.clone(),
                            None, // No expected signature needed
                            compression_state.global_id,
                            compression_state.server_addr.clone(),
                            compression_state.client_addr.clone(),
                            self.client_cookie,
                            self.server_cookie,
                            self.global_seq,
                            self.connect_seq,
                            self.in_seq,
                        ));

                        // Store global_id in StateMachine for later access
                        self.global_id = compression_state.global_id;
                        tracing::debug!(
                            "Stored global_id {} in StateMachine (from CompressionConnecting)",
                            compression_state.global_id
                        );

                        self.current_state = new_session_state;
                        let enter_result = self.current_state.enter()?;
                        return self.apply_result(enter_result);
                    }
                    // If peer supports compression and no algorithm set yet, proceed normally (will send COMPRESSION_REQUEST)
                } else if new_state.kind() == StateKind::SessionConnecting {
                    // Store global_id from SessionConnecting state and fix up addresses
                    let session_state = new_state
                        .as_any()
                        .downcast_ref::<SessionConnecting>()
                        .expect("State kind is SESSION_CONNECTING but downcast failed");

                    self.global_id = session_state.our_global_id;
                    tracing::debug!(
                        "Stored global_id {} in StateMachine (from SessionConnecting)",
                        session_state.our_global_id
                    );

                    // If addresses are default, fix them up with actual addresses
                    if session_state.server_addr == denc::EntityAddr::default()
                        || session_state.client_addr == denc::EntityAddr::default()
                    {
                        let server_addr = self.server_addr.clone().unwrap_or_default();
                        let client_addr = self.client_addr.clone().unwrap_or_default();

                        // Create new SessionConnecting with correct addresses
                        let new_session_state = Box::new(SessionConnecting::new_with_encryption(
                            session_state.connection_mode,
                            session_state.session_key.clone(),
                            session_state.connection_secret.clone(),
                            session_state.expected_server_signature.clone(),
                            session_state.our_global_id,
                            server_addr,
                            client_addr,
                            self.client_cookie,
                            self.server_cookie,
                            self.global_seq,
                            self.connect_seq,
                            self.in_seq,
                        ));

                        self.current_state = new_session_state;
                        let enter_result = self.current_state.enter()?;
                        return self.apply_result(enter_result);
                    }
                }

                self.current_state = new_state;

                // For SessionConnecting, call enter() to send CLIENT_IDENT
                // For other states, also call enter()
                let enter_result = self.current_state.enter()?;
                self.apply_result(enter_result)
            }
            StateResult::SendFrame { frame, next_state } => {
                if let Some(new_state) = next_state {
                    // Check if transitioning to AuthAccepting and inject auth handler
                    if new_state.kind() == StateKind::AuthAccepting {
                        // Take the auth handler from StateMachine and pass it to AuthAccepting
                        if let Some(auth_handler) = self.server_auth_handler.take() {
                            self.current_state =
                                Box::new(AuthAccepting::with_handler(auth_handler));
                        } else {
                            self.current_state = new_state;
                        }
                    } else {
                        self.current_state = new_state;
                    }
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
            StateResult::SetKeepAliveAck(timestamp) => {
                // Update last_keepalive_ack timestamp
                self.last_keepalive_ack = Some(timestamp);
                Ok(StateResult::Continue)
            }
            StateResult::Ready => {
                // Transition to Ready state
                self.current_state = Box::new(Ready);
                Ok(StateResult::Ready)
            }
            StateResult::ReconnectReady { msg_seq } => {
                // Transition to Ready state after reconnection
                self.current_state = Box::new(Ready);
                Ok(StateResult::ReconnectReady { msg_seq })
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

    #[test]
    fn test_auth_method_config() {
        // Test 1: Default config should auto-detect supported_auth_methods
        let config = crate::ConnectionConfig::default();
        assert!(
            !config.supported_auth_methods.is_empty(),
            "Default config should have at least one supported auth method"
        );

        // Test 2: with_no_auth should set only AuthMethod::None
        let config = crate::ConnectionConfig::with_no_auth();
        assert_eq!(
            config.supported_auth_methods,
            vec![crate::AuthMethod::None],
            "with_no_auth should set only AuthMethod::None"
        );
        assert!(
            config.auth_provider.is_none(),
            "with_no_auth should have no auth provider"
        );

        // Test 3: State machine should be created successfully
        let config = crate::ConnectionConfig::with_no_auth();
        let sm = StateMachine::new_client(config);
        // Verify the state machine was created successfully with BannerConnecting state
        assert_eq!(sm.current_state.kind(), StateKind::BannerConnecting);
    }
}
