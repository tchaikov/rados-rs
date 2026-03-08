//! msgr2 protocol state machine implementation using the State pattern
//!
//! Based on ~/dev/ceph/src/msg/async/ProtocolV2.h and msgr2.rst documentation

use crate::error::{Msgr2Error as Error, Result};
use crate::frames::{
    AuthDoneFrame, AuthRequestFrame, AuthRequestMoreFrame, AuthSignatureFrame,
    CompressionDoneFrame, CompressionRequestFrame, Frame, FrameTrait, HelloFrame,
    Keepalive2AckFrame, ServerIdentFrame, Tag,
};
use bytes::{Bytes, BytesMut};
use denc::Denc;
use denc::EntityName;
use std::fmt::Debug;
use tracing;

/// Helper function to create a Frame from a FrameTrait
pub fn create_frame_from_trait<F: FrameTrait>(frame_trait: &F, tag: Tag) -> Result<Frame> {
    // Use msgr2_frame_assumed features for frame encoding
    // msgr2_frame_assumed = MSG_ADDR2 | SERVER_NAUTILUS
    use denc::features::CephFeatures;
    const MSGR2_FRAME_ASSUMED: u64 = CephFeatures::MASK_MSG_ADDR2
        .union(CephFeatures::MASK_SERVER_NAUTILUS)
        .bits();

    let segments = frame_trait.get_segments(MSGR2_FRAME_ASSUMED)?;
    Ok(Frame {
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
    })
}

/// Side-effect emitted by a state alongside a transition.
///
/// States return `ConnectionEffect` to communicate configuration changes to the
/// `StateMachine` coordinator without requiring `as_any()` downcasting.
pub enum ConnectionEffect {
    /// No side effects — install the new state as-is.
    None,
    /// Preserve an updated auth provider (emitted by auth-carrying states when transitioning).
    PreserveAuthProvider(Box<dyn auth::AuthProvider>),
    /// Auth phase complete: compute HMAC-SHA256 signatures and optionally set up encryption.
    ///
    /// `apply_transition_effect` reads `pre_auth_rx/txbuf` from the machine to compute HMACs,
    /// then builds the real `AuthConnectingSign` / `AuthAcceptingSign` state.
    AuthSign {
        connection_mode: u32,
        session_key: Option<Bytes>,
        connection_secret: Option<Bytes>,
        global_id: u64,
        /// Updated auth provider to preserve, if any.
        auth_provider: Option<Box<dyn auth::AuthProvider>>,
    },
    /// Client: session phase ready (optionally with compression).
    ///
    /// `apply_transition_effect` calls `setup_compression` when `compression_algorithm` is
    /// `Some`, then builds `SessionConnecting` from machine-level address/cookie fields.
    SessionReady {
        compression_algorithm: Option<crate::compression::CompressionAlgorithm>,
    },
    /// Server: auth-sign phase complete — route to `CompressionAccepting` or `SessionAccepting`
    /// based on `compression_negotiation_needed()`.
    ServerAuthComplete,
    /// Server: install this compression algorithm when transitioning to `SessionAccepting`.
    ServerInstallCompression(crate::compression::CompressionAlgorithm),
}

impl std::fmt::Debug for ConnectionEffect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionEffect::None => write!(f, "None"),
            ConnectionEffect::PreserveAuthProvider(_) => write!(f, "PreserveAuthProvider"),
            ConnectionEffect::AuthSign {
                connection_mode,
                global_id,
                ..
            } => {
                write!(f, "AuthSign(mode={connection_mode}, gid={global_id})")
            }
            ConnectionEffect::SessionReady {
                compression_algorithm,
            } => {
                write!(f, "SessionReady(compression={compression_algorithm:?})")
            }
            ConnectionEffect::ServerAuthComplete => write!(f, "ServerAuthComplete"),
            ConnectionEffect::ServerInstallCompression(alg) => {
                write!(f, "ServerInstallCompression({alg:?})")
            }
        }
    }
}

/// Result of frame processing that indicates next action
#[derive(Debug)]
pub enum StateResult {
    /// Continue in current state
    Continue,
    /// Transition to a new state, with an optional side-effect for the coordinator.
    Transition(Box<dyn State>, ConnectionEffect),
    /// Send a frame and optionally transition, with an optional side-effect.
    SendFrame {
        frame: Frame,
        next_state: Option<Box<dyn State>>,
        effect: ConnectionEffect,
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
    /// Check if this state is an authentication state
    pub fn is_auth_state(&self) -> bool {
        matches!(
            self,
            StateKind::AuthConnecting
                | StateKind::AuthConnectingSign
                | StateKind::AuthAccepting
                | StateKind::AuthAcceptingSign
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
                | StateKind::AuthAcceptingSign
                | StateKind::CompressionAccepting
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
            StateKind::AuthAcceptingSign => "AUTH_ACCEPTING_SIGN",
            StateKind::CompressionAccepting => "COMPRESSION_ACCEPTING",
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

    /// Returns the negotiated Ceph session features (non-zero only in `SessionConnecting`).
    fn negotiated_features(&self) -> u64 {
        0
    }
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
    pub entity_name: denc::EntityName,
    pub global_id: u64,
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
        Ok(StateResult::Transition(
            Box::new(HelloConnecting::new(
                self.preferred_modes.clone(),
                self.supported_auth_methods.clone(),
                self.auth_provider.clone(),
                self.service_id,
                self.entity_name.clone(),
                self.global_id,
            )),
            ConnectionEffect::None,
        ))
    }
}

#[derive(Clone)]
pub struct HelloConnecting {
    hello_sent: bool,
    preferred_modes: Vec<crate::ConnectionMode>,
    supported_auth_methods: Vec<crate::AuthMethod>,
    auth_provider: Option<Box<dyn auth::AuthProvider>>,
    service_id: u32,
    entity_name: denc::EntityName,
    global_id: u64,
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
        entity_name: denc::EntityName,
        global_id: u64,
    ) -> Self {
        Self {
            hello_sent: false,
            preferred_modes,
            supported_auth_methods,
            auth_provider,
            service_id,
            entity_name,
            global_id,
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
                let entity_type = u8::decode(&mut payload, 0)?;
                let _peer_addr = denc::EntityAddr::decode(&mut payload, 0)?;

                tracing::debug!("Received HELLO from entity_type={}", entity_type);

                // Transition to auth phase
                let effect = match self.auth_provider.clone() {
                    Some(p) => ConnectionEffect::PreserveAuthProvider(p),
                    None => ConnectionEffect::None,
                };
                Ok(StateResult::Transition(
                    Box::new(AuthConnecting::new(
                        self.preferred_modes.clone(),
                        self.supported_auth_methods.clone(),
                        self.auth_provider.take(),
                        self.service_id,
                        self.entity_name.clone(),
                        self.global_id,
                    )),
                    effect,
                ))
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
                denc::EntityType::CLIENT.bits() as u8,
                denc::EntityAddr::default(),
            );
            let frame = create_frame_from_trait(&hello_frame, Tag::Hello)?;

            Ok(StateResult::SendAndWait {
                frame,
                next_state: Box::new(HelloConnecting::new(
                    self.preferred_modes.clone(),
                    self.supported_auth_methods.clone(),
                    self.auth_provider.clone(),
                    self.service_id,
                    self.entity_name.clone(),
                    self.global_id,
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
    /// Entity name for AUTH_NONE payload
    entity_name: denc::EntityName,
    /// Global ID from monitor authentication (for AUTH_NONE authorizers)
    global_id: u64,
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
        entity_name: denc::EntityName,
        global_id: u64,
    ) -> Self {
        // Determine the preferred auth method (first in the list)
        let preferred_auth_method = supported_auth_methods
            .first()
            .copied()
            .unwrap_or(crate::AuthMethod::None);

        tracing::debug!(
            "AuthConnecting::new with auth method {:?}, has_provider={}, service_id={}, global_id={}",
            preferred_auth_method,
            auth_provider.is_some(),
            service_id,
            global_id
        );
        tracing::info!(
            "Starting auth with method={:?}, supported_methods={:?}",
            preferred_auth_method,
            supported_auth_methods
        );

        Self {
            auth_method: preferred_auth_method,
            auth_provider,
            preferred_modes,
            supported_auth_methods,
            tried_methods: Vec::new(),
            service_id,
            entity_name,
            global_id,
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
            "AuthConnecting received frame tag: {:?}",
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
                    "AUTH_BAD_METHOD - method={}, result={}, allowed={:?}, modes={:?}",
                    rejected_method,
                    result,
                    allowed_methods,
                    allowed_modes
                );
                tracing::info!(
                    "Server rejected auth method {} (error code={}), allowed_methods={:?}, our_supported={:?}",
                    rejected_method,
                    result,
                    allowed_methods,
                    self.supported_auth_methods
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
                    entity_name: self.entity_name.clone(),
                    global_id: self.global_id,
                };

                new_state.tried_methods = new_tried;

                // Transition back to AuthConnecting (will send new AUTH_REQUEST)
                let effect = match self.auth_provider.take() {
                    Some(p) => ConnectionEffect::PreserveAuthProvider(p),
                    None => ConnectionEffect::None,
                };
                Ok(StateResult::Transition(Box::new(new_state), effect))
            }
            Tag::AuthReplyMore => {
                // Handle CephX multi-round auth
                if let Some(provider) = &mut self.auth_provider {
                    if let Some(payload) = frame.segments.first() {
                        tracing::debug!("AUTH_REPLY_MORE payload length: {}", payload.len());
                        tracing::trace!(
                            "AUTH_REPLY_MORE payload hex (first 128 bytes): {}",
                            payload
                                .iter()
                                .take(128)
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join("")
                        );

                        // Process the challenge and build the response
                        let _result = provider.handle_auth_response(payload.clone(), 0, 0)?;
                        let auth_payload = provider.build_auth_payload(0, self.service_id)?;

                        // For AuthRequestMore, we only send the auth_payload
                        let auth_frame = AuthRequestMoreFrame::new(auth_payload);
                        let response_frame =
                            create_frame_from_trait(&auth_frame, Tag::AuthRequestMore)?;

                        // Preserve auth_provider state by not creating new AuthConnecting
                        Ok(StateResult::SendFrame {
                            frame: response_frame,
                            next_state: None,
                            effect: ConnectionEffect::None,
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
                    tracing::debug!("AUTH_DONE frame segment length: {}", segment.len());
                    tracing::trace!(
                        "AUTH_DONE frame segment hex (first 64 bytes): {}",
                        segment
                            .iter()
                            .take(64)
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join("")
                    );

                    let mut payload = segment.clone();
                    let global_id = u64::decode(&mut payload, 0)?;
                    let con_mode = u32::decode(&mut payload, 0)?;
                    let auth_payload = Bytes::decode(&mut payload, 0)?;

                    tracing::debug!("Decoded from AUTH_DONE frame:");
                    tracing::debug!("  global_id: {}", global_id);
                    tracing::debug!("  con_mode: {}", con_mode);
                    tracing::debug!("  auth_payload length: {}", auth_payload.len());

                    tracing::info!(
                        "Authentication completed successfully - global_id: {}, connection_mode: {}",
                        global_id,
                        con_mode
                    );

                    // Both auth paths transition to AUTH_CONNECTING_SIGN.
                    // Reference: ProtocolV2::handle_auth_done() always transitions to AUTH_CONNECTING_SIGN
                    if self.auth_method == crate::AuthMethod::None {
                        tracing::debug!(
                            "AuthMethod::None - no session key/connection secret, will send empty signature"
                        );
                        Ok(StateResult::Transition(
                            Box::new(AuthConnectingSign::new_with_encryption(
                                con_mode,
                                None,
                                None,
                                Bytes::new(),
                                None,
                                global_id,
                                denc::EntityAddr::default(),
                                denc::EntityAddr::default(),
                                0,
                                0, // placeholder; overridden by apply_result
                            )),
                            ConnectionEffect::AuthSign {
                                connection_mode: con_mode,
                                session_key: None,
                                connection_secret: None,
                                global_id,
                                auth_provider: self.auth_provider.take(),
                            },
                        ))
                    } else {
                        // CephX authentication - extract session_key and connection_secret
                        let provider = self
                            .auth_provider
                            .as_mut()
                            .ok_or_else(|| Error::protocol_error("No auth provider available"))?;

                        let (session_key, connection_secret) =
                            provider.handle_auth_response(auth_payload, global_id, con_mode)?;

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
                        // for signature exchange. apply_result handles HMAC computation.
                        Ok(StateResult::Transition(
                            Box::new(AuthConnectingSign::new_with_encryption(
                                con_mode,
                                session_key.clone(),
                                connection_secret.clone(),
                                Bytes::new(), // Placeholder; replaced with computed signature by apply_result
                                None, // Placeholder; replaced with computed expected sig by apply_result
                                global_id,
                                denc::EntityAddr::default(),
                                denc::EntityAddr::default(),
                                0,
                                0, // placeholder; overridden by apply_result
                            )),
                            ConnectionEffect::AuthSign {
                                connection_mode: con_mode,
                                session_key,
                                connection_secret,
                                global_id,
                                auth_provider: self.auth_provider.take(),
                            },
                        ))
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
        let preferred_modes: Vec<u32> = self.preferred_modes.iter().map(|m| (*m).into()).collect();

        let (method, auth_payload) = match self.auth_method {
            crate::AuthMethod::None => {
                // AUTH_NONE uses different payload formats depending on connection target:
                // - Service connections (OSD/MDS/MGR): auth_mode=1 (authorizer)
                // - Monitor connections: auth_mode=10 (mon auth)
                let is_service_connection = self.service_id != 0 && self.global_id != 0;
                let auth_none = if is_service_connection {
                    tracing::debug!(
                        "Sending AUTH_REQUEST for service (service_id={}, global_id={})",
                        self.service_id,
                        self.global_id
                    );
                    crate::AuthNonePayload::for_service(self.entity_name.clone(), self.global_id)
                } else {
                    tracing::debug!(
                        "Sending AUTH_REQUEST for monitor (entity={}, global_id=0)",
                        self.entity_name
                    );
                    crate::AuthNonePayload::for_monitor(
                        self.entity_name.clone(),
                        0, // monitor assigns global_id
                    )
                };
                (crate::AuthMethod::None.into(), auth_none.encode()?)
            }
            crate::AuthMethod::Cephx => {
                tracing::debug!("Sending AUTH_REQUEST with AuthMethod::Cephx");
                let provider = self
                    .auth_provider
                    .as_mut()
                    .ok_or_else(|| Error::protocol_error("No auth provider available"))?;
                let payload = provider.build_auth_payload(0, self.service_id)?;
                (crate::AuthMethod::Cephx.into(), payload)
            }
            _ => {
                return Err(Error::protocol_error(&format!(
                    "Unsupported auth method: {:?}",
                    self.auth_method
                )));
            }
        };

        let auth_frame = AuthRequestFrame::new(method, preferred_modes.clone(), auth_payload);
        let frame = create_frame_from_trait(&auth_frame, Tag::AuthRequest)?;

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
    /// Our own supported features (to check if we advertised compression)
    pub our_supported_features: u64,
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
                    let is_compress = bool::decode(&mut payload, 0)?;
                    let method = u32::decode(&mut payload, 0)?;

                    tracing::debug!(
                        "Received COMPRESSION_DONE - is_compress: {}, method: {}",
                        is_compress,
                        method
                    );

                    // Store compression algorithm
                    let compression_algorithm = if is_compress {
                        crate::compression::CompressionAlgorithm::try_from(method)
                            .unwrap_or(crate::compression::CompressionAlgorithm::None)
                    } else {
                        crate::compression::CompressionAlgorithm::None
                    };

                    tracing::debug!(
                        "Compression negotiation complete: algorithm={:?}",
                        compression_algorithm
                    );

                    // Emit SessionReady effect — apply_result installs compression and
                    // builds SessionConnecting from machine-level address/cookie fields.
                    Ok(StateResult::Transition(
                        Box::new(CompressionConnecting::new_with_encryption(
                            self.connection_mode,
                            self.session_key.clone(),
                            self.connection_secret.clone(),
                            self.global_id,
                            self.server_addr.clone(),
                            self.client_addr.clone(),
                        )),
                        ConnectionEffect::SessionReady {
                            compression_algorithm: Some(compression_algorithm),
                        },
                    ))
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
        let frame = create_frame_from_trait(&compression_request, Tag::CompressionRequest)?;

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
        our_supported_features: u64,
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
            our_supported_features,
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
            "AuthConnectingSign::handle_frame received tag: {:?}",
            frame.preamble.tag
        );
        match frame.preamble.tag {
            Tag::AuthSignature => {
                tracing::debug!("Received AUTH_SIGNATURE from server");
                // Verify server's AUTH_SIGNATURE
                if let Some(ref expected_sig) = self.expected_server_signature {
                    // Extract server's signature from frame payload
                    // Note: The signature is sent as raw bytes, not length-prefixed
                    if let Some(segment) = frame.segments.first() {
                        let server_signature = segment.clone();

                        // Verify signature matches expected
                        if server_signature == *expected_sig {
                            tracing::info!(
                                "Server AUTH_SIGNATURE verified successfully ({} bytes)",
                                server_signature.len()
                            );
                        } else {
                            tracing::error!(
                                "Server AUTH_SIGNATURE verification failed - expected {} bytes, got {} bytes",
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
                    "peer_supported_features = 0x{:x}, our_supported_features = 0x{:x}",
                    self.peer_supported_features,
                    self.our_supported_features,
                );
                let peer_features = crate::FeatureSet::from(self.peer_supported_features);
                let our_features = crate::FeatureSet::from(self.our_supported_features);
                if peer_features.contains(crate::FeatureSet::COMPRESSION)
                    && our_features.contains(crate::FeatureSet::COMPRESSION)
                {
                    tracing::debug!(
                        "Both sides support COMPRESSION, transitioning to COMPRESSION_CONNECTING"
                    );
                    Ok(StateResult::Transition(
                        Box::new(CompressionConnecting::new_with_encryption(
                            self.connection_mode,
                            self.session_key.clone(),
                            self.connection_secret.clone(),
                            self.global_id,
                            self.server_addr.clone(),
                            self.client_addr.clone(),
                        )),
                        ConnectionEffect::None,
                    ))
                } else {
                    tracing::debug!("Peer does NOT support COMPRESSION, transitioning directly to SESSION_CONNECTING");
                    Ok(StateResult::Transition(
                        Box::new(SessionConnecting::new_with_encryption(
                            self.connection_mode,
                            self.session_key.clone(),
                            self.connection_secret.clone(),
                            None,
                            self.global_id,
                            self.server_addr.clone(),
                            self.client_addr.clone(),
                            rand::random(), // client_cookie — overridden in apply_result
                            0,              // server_cookie — set in apply_result
                            0,              // global_seq — set in apply_result
                            0,              // connect_seq — set in apply_result
                            0,              // in_seq — set in apply_result
                        )),
                        ConnectionEffect::SessionReady {
                            compression_algorithm: None,
                        },
                    ))
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
        tracing::debug!("AuthConnectingSign::enter - Sending AUTH_SIGNATURE to server");
        tracing::debug!("  signature length: {} bytes", self.our_signature.len());
        tracing::trace!(
            "  signature hex (first 32 bytes): {}",
            self.our_signature
                .iter()
                .take(32)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("")
        );

        let auth_sig_frame = AuthSignatureFrame::new(self.our_signature.clone());
        let frame = create_frame_from_trait(&auth_sig_frame, Tag::AuthSignature)?;

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
                self.our_supported_features,
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
            connection_mode: crate::ConnectionMode::Crc.into(),
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

    fn negotiated_features(&self) -> u64 {
        self.negotiated_features
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        tracing::debug!(
            "SessionConnecting::handle_frame received tag: {:?}",
            frame.preamble.tag
        );
        match frame.preamble.tag {
            Tag::ServerIdent => {
                // Parse SERVER_IDENT frame
                if let Some(segment) = frame.segments.first() {
                    let mut payload = segment.clone();
                    let addrvec = denc::EntityAddrvec::decode(&mut payload, 0)?;
                    let addrs = addrvec.addrs;
                    let gid = u64::decode(&mut payload, 0)?;
                    let global_seq = u64::decode(&mut payload, 0)?;
                    let features_supported = u64::decode(&mut payload, 0)?;
                    let features_required = u64::decode(&mut payload, 0)?;
                    let flags = u64::decode(&mut payload, 0)?;
                    let cookie = u64::decode(&mut payload, 0)?;

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
                    use denc::features::CephFeatures;
                    let our_features: u64 = (CephFeatures::MSG_ADDR2
                        | CephFeatures::SERVER_NAUTILUS
                        | CephFeatures::SERVER_OCTOPUS)
                        .bits()
                        | 0x3fffffffffffffff; // All features up to bit 61

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
                        let server_signature = Bytes::decode(&mut payload, 0)?;

                        // Verify signature matches expected
                        if server_signature == *expected_sig {
                            tracing::info!(
                                "Server AUTH_SIGNATURE verified successfully ({} bytes)",
                                server_signature.len()
                            );
                            Ok(StateResult::Continue)
                        } else {
                            tracing::error!(
                                "Server AUTH_SIGNATURE verification failed - expected {} bytes, got {} bytes",
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
                    let msg_seq = u64::decode(&mut payload, 0)?;

                    tracing::info!(
                        "Reconnection successful! Server acknowledged up to msg_seq={}",
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
                    let server_connect_seq = u64::decode(&mut payload, 0)?;

                    tracing::warn!(
                        "Server sent SESSION_RETRY: server_connect_seq={}, our_connect_seq={}. Incrementing and retrying.",
                        server_connect_seq,
                        self.connect_seq
                    );
                    tracing::debug!(
                        "SESSION_RETRY received, incrementing connect_seq and retrying"
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
                    let server_global_seq = u64::decode(&mut payload, 0)?;

                    tracing::warn!(
                        "Server sent SESSION_RETRY_GLOBAL: server_global_seq={}, our_global_seq={}. Updating and retrying.",
                        server_global_seq,
                        self.global_seq
                    );
                    tracing::debug!(
                        "SESSION_RETRY_GLOBAL received, updating global_seq and retrying"
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
                    let full = bool::decode(&mut payload, 0)?;

                    tracing::warn!(
                        "Server sent SESSION_RESET (full={}). Resetting session and sending CLIENT_IDENT",
                        full
                    );
                    tracing::debug!("SESSION_RESET received (full={}), resetting session", full);

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
        tracing::info!("SessionConnecting::enter() - Starting session establishment");
        tracing::debug!("  our_global_id={}", self.our_global_id);
        tracing::debug!("  connection_mode={}", self.connection_mode);
        tracing::debug!("  client_cookie={}", self.client_cookie);
        tracing::debug!("  server_cookie={}", self.server_cookie);
        tracing::debug!(
            "  has_connection_secret={}",
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

            let frame = create_frame_from_trait(&reconnect, Tag::SessionReconnect)?;

            tracing::debug!("Created SESSION_RECONNECT frame");
            tracing::debug!("  addrs: {:?}", addrs);
            tracing::debug!("  client_cookie: {}", self.client_cookie);
            tracing::debug!("  server_cookie: {}", self.server_cookie);
            tracing::debug!("  global_seq: {}", self.global_seq);
            tracing::debug!("  connect_seq: {}", self.connect_seq);
            tracing::debug!("  in_seq (msg_seq): {}", self.in_seq);

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
            use denc::features::CephFeatures;

            let features_supported: u64 = (CephFeatures::MSG_ADDR2
                | CephFeatures::SERVER_NAUTILUS
                | CephFeatures::SERVER_OCTOPUS)
                .bits()
                | 0x3fffffffffffffff; // All features up to bit 61

            let features_required: u64 = CephFeatures::MSG_ADDR2.bits();

            // Flags - default to 0 (non-lossy)
            let flags: u64 = 0;

            tracing::debug!(
                "Sending CLIENT_IDENT: gid={}, target={:?}, client_addr={:?}, client_cookie={}",
                gid,
                target_addr,
                addrs,
                self.client_cookie
            );
            tracing::info!(
                "Sending CLIENT_IDENT frame: gid={}, global_seq={}, cookie={}",
                gid,
                self.global_seq,
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
            let frame = create_frame_from_trait(&client_ident, Tag::ClientIdent)?;

            tracing::debug!(
                "Created CLIENT_IDENT frame, {} segments",
                frame.segments.len()
            );
            tracing::debug!("CLIENT_IDENT values:");
            tracing::debug!("  addrs: {:?}", addrs);
            tracing::debug!("  target_addr: {:?}", target_addr);
            tracing::debug!("  gid: {}", gid);
            tracing::debug!("  global_seq: {}", self.global_seq);
            tracing::debug!("  features_supported: 0x{:x}", features_supported);
            tracing::debug!("  features_required: 0x{:x}", features_required);
            tracing::debug!("  flags: 0x{:x}", flags);
            tracing::debug!("  cookie: {}", self.client_cookie);

            if !frame.segments.is_empty() {
                let payload_bytes = &frame.segments[0];
                tracing::trace!(
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
                    denc::EntityType::CLIENT.bits() as u8,
                    denc::EntityAddr::default(),
                );
                let response_frame = create_frame_from_trait(&hello_frame, Tag::Hello)?;

                Ok(StateResult::SendFrame {
                    frame: response_frame,
                    next_state: Some(Box::new(AuthAccepting::new())),
                    effect: ConnectionEffect::None,
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
    /// Negotiated connection mode (CRC=1, SECURE=2)
    connection_mode: u32,
    /// Client's preferred connection modes
    client_preferred_modes: Vec<u32>,
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
            connection_mode: crate::ConnectionMode::Crc.into(), // Default to CRC
            client_preferred_modes: Vec::new(),
        }
    }

    pub fn with_handler(auth_handler: auth::CephXServerHandler) -> Self {
        Self {
            auth_handler: Some(auth_handler),
            entity_name: None,
            global_id: None,
            phase: 0,
            connection_mode: crate::ConnectionMode::Crc.into(), // Default to CRC
            client_preferred_modes: Vec::new(),
        }
    }

    /// Negotiate connection mode from client's preferred modes.
    /// Server preference order: SECURE > CRC
    fn negotiate_connection_mode(&self, client_modes: &[u32]) -> u32 {
        use crate::{CRC_MODE, SECURE_MODE};

        if client_modes.contains(&SECURE_MODE) {
            SECURE_MODE
        } else {
            CRC_MODE // Default to CRC (covers both explicit CRC and no match)
        }
    }
}

impl State for AuthAccepting {
    impl_state_boilerplate!(AuthAccepting, StateKind::AuthAccepting, &[Tag::AuthRequest]);

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::AuthRequest => {
                // Parse AUTH_REQUEST frame to extract method, preferred_modes, and auth_payload
                let auth_request = AuthRequestFrame::from_frame(&frame)?;

                if self.phase == 0 {
                    // Store client's preferred modes and negotiate connection mode
                    self.client_preferred_modes = auth_request.preferred_modes.clone();
                    self.connection_mode =
                        self.negotiate_connection_mode(&auth_request.preferred_modes);

                    tracing::debug!(
                        "Server: Negotiated connection_mode={} from client preferred_modes={:?}",
                        self.connection_mode,
                        auth_request.preferred_modes
                    );
                    // Phase 1: Initial authentication request
                    // Client sends: auth_mode + entity_name + global_id
                    // Server responds with: server_challenge

                    if let Some(ref mut handler) = self.auth_handler {
                        let (entity_name, global_id, challenge_payload) =
                            handler.handle_initial_request(&auth_request.auth_payload)?;

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
                            create_frame_from_trait(&auth_more, Tag::AuthReplyMore)?;

                        Ok(StateResult::SendFrame {
                            frame: response_frame,
                            next_state: None, // Stay in AuthAccepting for phase 2
                            effect: ConnectionEffect::None,
                        })
                    } else {
                        // No auth handler - send AUTH_DONE with negotiated connection mode
                        tracing::warn!(
                            "Server: No auth handler configured, skipping authentication"
                        );
                        let auth_done =
                            AuthDoneFrame::new(1001, self.connection_mode, Bytes::new());
                        let response_frame = create_frame_from_trait(&auth_done, Tag::AuthDone)?;

                        let effect = if self.connection_mode == crate::ConnectionMode::Secure as u32
                        {
                            ConnectionEffect::AuthSign {
                                connection_mode: self.connection_mode,
                                session_key: None,
                                connection_secret: None,
                                global_id: 1001,
                                auth_provider: None,
                            }
                        } else {
                            ConnectionEffect::ServerAuthComplete
                        };

                        Ok(StateResult::SendFrame {
                            frame: response_frame,
                            next_state: None,
                            effect,
                        })
                    }
                } else {
                    // Phase 2: Challenge response
                    // Client sends: CephXRequestHeader + CephXAuthenticate
                    // Server responds with: session_key + service_tickets

                    if let (Some(ref mut handler), Some(ref entity_name), Some(global_id)) =
                        (&mut self.auth_handler, &self.entity_name, self.global_id)
                    {
                        let (session_key, connection_secret, auth_payload) = handler
                            .handle_authenticate(
                                entity_name,
                                global_id,
                                &auth_request.auth_payload,
                            )?;

                        tracing::info!("Server: Client {} authenticated successfully", entity_name);

                        // Build AUTH_DONE response with negotiated connection mode
                        let auth_done_payload = handler.build_auth_done_response(
                            global_id,
                            self.connection_mode as u8,
                            &session_key,
                            &connection_secret,
                            auth_payload,
                        )?;

                        let auth_done =
                            AuthDoneFrame::new(global_id, self.connection_mode, auth_done_payload);
                        let response_frame = create_frame_from_trait(&auth_done, Tag::AuthDone)?;

                        let effect = if self.connection_mode == crate::ConnectionMode::Secure as u32
                        {
                            ConnectionEffect::AuthSign {
                                connection_mode: self.connection_mode,
                                session_key: Some(session_key.secret.clone()),
                                connection_secret: Some(connection_secret.secret.clone()),
                                global_id,
                                auth_provider: None,
                            }
                        } else {
                            ConnectionEffect::ServerAuthComplete
                        };

                        Ok(StateResult::SendFrame {
                            frame: response_frame,
                            next_state: None,
                            effect,
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
pub struct AuthAcceptingSign {
    /// Negotiated connection mode
    connection_mode: u32,
    /// Session key for HMAC computation
    session_key: Option<Bytes>,
    /// Connection secret for encryption
    connection_secret: Option<Bytes>,
    /// Server's signature to send
    our_signature: Bytes,
    /// Expected client signature to verify
    expected_client_signature: Option<Bytes>,
}

impl AuthAcceptingSign {
    pub fn new(
        connection_mode: u32,
        session_key: Option<Bytes>,
        connection_secret: Option<Bytes>,
    ) -> Self {
        Self {
            connection_mode,
            session_key,
            connection_secret,
            our_signature: Bytes::new(), // Will be computed in apply_result
            expected_client_signature: None, // Will be computed in apply_result
        }
    }

    pub fn new_with_signatures(
        connection_mode: u32,
        session_key: Option<Bytes>,
        connection_secret: Option<Bytes>,
        our_signature: Bytes,
        expected_client_signature: Option<Bytes>,
    ) -> Self {
        Self {
            connection_mode,
            session_key,
            connection_secret,
            our_signature,
            expected_client_signature,
        }
    }
}

impl State for AuthAcceptingSign {
    impl_state_boilerplate!(
        AuthAcceptingSign,
        StateKind::AuthAcceptingSign,
        &[Tag::AuthSignature]
    );

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::AuthSignature => {
                tracing::debug!("Server: Received AUTH_SIGNATURE from client");

                // Verify client's AUTH_SIGNATURE
                if let Some(ref expected_sig) = self.expected_client_signature {
                    // Extract client's signature from frame payload
                    if let Some(segment) = frame.segments.first() {
                        let client_signature = segment.clone();

                        // Verify signature matches expected
                        if client_signature == *expected_sig {
                            tracing::info!(
                                "Client AUTH_SIGNATURE verified successfully ({} bytes)",
                                client_signature.len()
                            );
                        } else {
                            tracing::error!(
                                "Client AUTH_SIGNATURE verification failed - expected {} bytes, got {} bytes",
                                expected_sig.len(),
                                client_signature.len()
                            );
                            return Err(Error::protocol_error(
                                "Client AUTH_SIGNATURE verification failed",
                            ));
                        }
                    } else {
                        return Err(Error::protocol_error(
                            "AUTH_SIGNATURE frame missing payload",
                        ));
                    }
                } else {
                    // No expected signature (CRC mode or AUTH_NONE), just accept it
                    tracing::debug!(
                        "Received AUTH_SIGNATURE in CRC/AUTH_NONE mode, accepting without verification"
                    );
                }

                // After verification, transition to SessionAccepting.
                // apply_result will redirect to CompressionAccepting if compression is needed.
                Ok(StateResult::Transition(
                    Box::new(SessionAccepting::new()),
                    ConnectionEffect::ServerAuthComplete,
                ))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in AUTH_ACCEPTING_SIGN state",
                frame.preamble.tag
            ))),
        }
    }

    fn enter(&mut self) -> Result<StateResult> {
        // Send AUTH_SIGNATURE frame with pre-computed HMAC-SHA256 signature
        tracing::debug!("AuthAcceptingSign::enter - Sending AUTH_SIGNATURE to client");
        tracing::debug!("  signature length: {} bytes", self.our_signature.len());

        let auth_sig_frame = AuthSignatureFrame::new(self.our_signature.clone());
        let frame = create_frame_from_trait(&auth_sig_frame, Tag::AuthSignature)?;

        tracing::debug!(
            "Sending AUTH_SIGNATURE to client (signature: {} bytes)",
            self.our_signature.len()
        );

        Ok(StateResult::SendAndWait {
            frame,
            next_state: Box::new(AuthAcceptingSign::new_with_signatures(
                self.connection_mode,
                self.session_key.clone(),
                self.connection_secret.clone(),
                self.our_signature.clone(),
                self.expected_client_signature.clone(),
            )),
        })
    }
}

#[derive(Debug, Default)]
pub struct CompressionAccepting;

impl CompressionAccepting {
    pub fn new() -> Self {
        Self
    }

    fn select_compression_algorithm(
        request: &CompressionRequestFrame,
    ) -> Option<crate::compression::CompressionAlgorithm> {
        use crate::compression::CompressionAlgorithm;

        if !request.is_compress {
            return None;
        }

        if request.preferred_methods.is_empty() {
            return Some(CompressionAlgorithm::Snappy);
        }

        request.preferred_methods.iter().find_map(|method| {
            CompressionAlgorithm::try_from(*method)
                .ok()
                .filter(|algorithm| *algorithm != CompressionAlgorithm::None)
        })
    }
}

impl State for CompressionAccepting {
    impl_state_boilerplate!(
        CompressionAccepting,
        StateKind::CompressionAccepting,
        &[Tag::CompressionRequest]
    );

    fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        match frame.preamble.tag {
            Tag::CompressionRequest => {
                tracing::debug!("Server: Received COMPRESSION_REQUEST from client");
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error(
                        "COMPRESSION_REQUEST frame missing payload",
                    ));
                }
                let mut payload = frame.segments[0].clone();
                let request = CompressionRequestFrame {
                    is_compress: bool::decode(&mut payload, 0)?,
                    preferred_methods: Vec::<u32>::decode(&mut payload, 0)?,
                };
                let compression_algorithm = Self::select_compression_algorithm(&request);
                let compression_done = match compression_algorithm {
                    Some(algorithm) => CompressionDoneFrame::new(true, algorithm.into()),
                    None => CompressionDoneFrame::new(false, 0u32),
                };
                let response_frame =
                    create_frame_from_trait(&compression_done, Tag::CompressionDone)?;

                tracing::debug!(
                    "Server: Sending COMPRESSION_DONE (enabled={}, method={})",
                    compression_done.is_compress,
                    compression_done.method
                );

                // After COMPRESSION_DONE, install SessionAccepting.
                // Emit ServerInstallCompression so apply_result calls setup_compression if needed.
                let effect = match compression_algorithm {
                    Some(alg) => ConnectionEffect::ServerInstallCompression(alg),
                    None => ConnectionEffect::None,
                };
                Ok(StateResult::SendFrame {
                    frame: response_frame,
                    next_state: Some(Box::new(SessionAccepting::new())),
                    effect,
                })
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in COMPRESSION_ACCEPTING state",
                frame.preamble.tag
            ))),
        }
    }
}

#[derive(Debug, Default)]
pub struct SessionAccepting;

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
                let response_frame = create_frame_from_trait(&server_ident, Tag::ServerIdent)?;

                // After sending SERVER_IDENT, transition to Ready state
                // The state machine will handle the transition after the frame is sent
                Ok(StateResult::SendFrame {
                    frame: response_frame,
                    next_state: Some(Box::new(Ready)),
                    effect: ConnectionEffect::None,
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
                    let timestamp_sec = u32::decode(&mut payload, 0)?;
                    let timestamp_nsec = u32::decode(&mut payload, 0)?;

                    tracing::debug!(
                        "Received KEEPALIVE2 with timestamp {}.{:09}",
                        timestamp_sec,
                        timestamp_nsec
                    );

                    // Send keepalive ack with the same timestamp
                    let ack_frame = Keepalive2AckFrame::new(timestamp_sec, timestamp_nsec);
                    let response_frame = create_frame_from_trait(&ack_frame, Tag::Keepalive2Ack)?;

                    Ok(StateResult::SendFrame {
                        frame: response_frame,
                        next_state: None,
                        effect: ConnectionEffect::None,
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
    /// Negotiated connection mode (0=none, 1=CRC, 2=SECURE), set during auth phase.
    connection_mode: u32,
    /// Connection secret for SECURE mode encryption, set during auth phase.
    connection_secret: Option<Bytes>,
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
    /// Ceph session features negotiated during the ident exchange.
    negotiated_features: u64,
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
                local_supported_features: config.supported_features,
                local_required_features: config.required_features,
                preferred_modes: preferred_modes.clone(),
                supported_auth_methods,
                auth_provider,
                service_id,
                entity_name: config.entity_name.clone(),
                global_id: config.global_id,
            }),
            is_client: true,
            frame_decryptor: None,
            frame_encryptor: None,
            compression_ctx: None, // Will be set after compression negotiation
            pre_auth_rxbuf: BytesMut::new(),
            pre_auth_txbuf: BytesMut::new(),
            pre_auth_enabled: true,
            session_key: None,
            connection_mode: 0,
            connection_secret: None,
            server_addr: None,
            client_addr: None,
            peer_supported_features: 0, // Will be set after banner exchange
            config,
            preserved_auth_provider,
            server_auth_handler: None, // Client doesn't use server auth handler
            global_id: 0,              // Will be set during authentication
            negotiated_features: 0,
            client_cookie: rand::random(), // Generate random client cookie
            server_cookie: 0,              // Will be assigned by server
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
        let feature_set = crate::FeatureSet::from(features);
        tracing::debug!(
            "Peer supported features set: 0x{:x} (REVISION_1={}, COMPRESSION={})",
            features,
            feature_set.contains(crate::FeatureSet::REVISION_1),
            feature_set.contains(crate::FeatureSet::COMPRESSION)
        );
    }

    /// Check if peer supports compression feature
    pub fn peer_supports_compression(&self) -> bool {
        let features = crate::FeatureSet::from(self.peer_supported_features);
        features.contains(crate::FeatureSet::COMPRESSION)
    }

    /// Check if we (client) support compression feature
    pub fn we_support_compression(&self) -> bool {
        let features = crate::FeatureSet::from(self.config.supported_features);
        features.contains(crate::FeatureSet::COMPRESSION)
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

    /// Get the negotiated Ceph session features from the ident exchange.
    pub fn negotiated_features(&self) -> u64 {
        self.negotiated_features
    }

    /// Get the last keepalive ACK timestamp
    /// Returns None if no keepalive ACK has been received yet
    pub fn last_keepalive_ack(&self) -> Option<std::time::Instant> {
        self.last_keepalive_ack
    }

    /// Reset transient per-connection state after a fault.
    ///
    /// Clears crypto/compression handlers and pre-authentication buffers so
    /// the state machine is ready for a fresh connection attempt.  Session
    /// identity fields (client/server cookies, global_id) are intentionally
    /// preserved so that SESSION_RECONNECT can be attempted if the peer
    /// supports it.
    pub fn fault_reset(&mut self) {
        self.frame_decryptor = None;
        self.frame_encryptor = None;
        self.compression_ctx = None;
        self.session_key = None;
        self.negotiated_features = 0;
        self.pre_auth_rxbuf.clear();
        self.pre_auth_txbuf.clear();
        self.pre_auth_enabled = true; // re-enable for next connection attempt
    }

    // ── Phase-coordinator helpers ─────────────────────────────────────────────

    /// Expose HMAC-SHA256 to the phase coordinator in `protocol.rs`.
    pub fn hmac_sha256_pub(key: Option<&Bytes>, data: &[u8]) -> Result<Bytes> {
        Self::hmac_sha256(key, data)
    }

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

    /// Record the server cookie assigned in SERVER_IDENT.
    pub fn set_server_cookie(&mut self, cookie: u64) {
        self.server_cookie = cookie;
    }

    /// Transition the state machine to the Ready state after session establishment.
    ///
    /// Sets `negotiated_features` and installs the `Ready` state so that
    /// post-handshake frames (keepalive, messages) are handled correctly.
    pub fn transition_to_ready(&mut self, negotiated_features: u64) {
        self.negotiated_features = negotiated_features;
        self.current_state = Box::new(Ready);
    }

    // ── Phase-coordinator read accessors ─────────────────────────────────────

    /// Peer's supported msgr2 feature bits (set after banner exchange).
    pub fn peer_supported_features_val(&self) -> u64 {
        self.peer_supported_features
    }

    /// Our own advertised msgr2 feature bits (from config).
    pub fn our_supported_features(&self) -> u64 {
        self.config.supported_features
    }

    /// Clone the server entity address, falling back to default if not set.
    pub fn server_addr_clone(&self) -> denc::EntityAddr {
        self.server_addr.clone().unwrap_or_default()
    }

    /// Clone the client entity address, falling back to default if not set.
    pub fn client_addr_clone(&self) -> denc::EntityAddr {
        self.client_addr.clone().unwrap_or_default()
    }

    /// Client cookie for this connection.
    pub fn client_cookie_val(&self) -> u64 {
        self.client_cookie
    }

    /// Server cookie (0 if not yet assigned by the peer).
    pub fn server_cookie_val(&self) -> u64 {
        self.server_cookie
    }

    /// Current global sequence number.
    pub fn global_seq_val(&self) -> u64 {
        self.global_seq
    }

    /// Current connection sequence number.
    pub fn connect_seq_val(&self) -> u64 {
        self.connect_seq
    }

    /// Current incoming message sequence number.
    pub fn in_seq_val(&self) -> u64 {
        self.in_seq
    }

    /// Preferred connection modes from config.
    pub fn config_preferred_modes(&self) -> &[crate::ConnectionMode] {
        &self.config.preferred_modes
    }

    /// Supported auth methods from config.
    pub fn config_supported_auth_methods(&self) -> &[crate::AuthMethod] {
        &self.config.supported_auth_methods
    }

    /// Service ID from config.
    pub fn config_service_id(&self) -> u32 {
        self.config.service_id
    }

    /// Entity name from config.
    pub fn config_entity_name(&self) -> &denc::EntityName {
        &self.config.entity_name
    }

    /// Take the server-side auth handler (consumed once during `accept_session`).
    pub fn take_server_auth_handler(&mut self) -> Option<auth::CephXServerHandler> {
        self.server_auth_handler.take()
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
            connection_mode: 0,
            connection_secret: None,
            server_addr: None,
            client_addr: None,
            peer_supported_features: 0, // Will be set after banner exchange
            config: crate::ConnectionConfig::default(),
            preserved_auth_provider: None, // Server doesn't use auth provider
            server_auth_handler: auth_handler,
            global_id: 0, // Server doesn't have a global_id (it assigns them to clients)
            negotiated_features: 0,
            client_cookie: 0,              // Will be received from client
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
        tracing::debug!(
            "decrypt_frame_data called, has_decryptor={}, data_len={}",
            self.frame_decryptor.is_some(),
            data.len()
        );
        if let Some(decryptor) = &mut self.frame_decryptor {
            tracing::debug!("Attempting AES-GCM decryption of {} bytes", data.len());
            let result = decryptor.decrypt(data);
            if let Err(ref e) = result {
                tracing::debug!("Decryption FAILED: {:?}", e);
            } else {
                tracing::debug!("Decryption SUCCESS");
            }
            result.map_err(Into::into)
        } else {
            // No encryption, return as-is
            Ok(Bytes::copy_from_slice(data))
        }
    }

    /// Encrypt frame data if in SECURE mode
    pub fn encrypt_frame_data(&mut self, data: &[u8]) -> Result<Bytes> {
        if let Some(encryptor) = &mut self.frame_encryptor {
            encryptor.encrypt(data).map_err(Into::into)
        } else {
            // No encryption, return as-is
            Ok(Bytes::copy_from_slice(data))
        }
    }

    /// Check if frame encryption is active
    pub fn has_encryption(&self) -> bool {
        self.frame_decryptor.is_some()
    }

    /// Check if using msgr2.1 (rev1) — true when the peer supports REVISION_1
    pub fn is_rev1(&self) -> bool {
        let peer = crate::FeatureSet::from(self.peer_supported_features);
        peer.contains(crate::FeatureSet::REVISION_1)
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

    /// Compute HMAC-SHA256 over `data` using `key`.  Returns 32 zero bytes when `key` is `None`.
    fn hmac_sha256(key: Option<&Bytes>, data: &[u8]) -> Result<Bytes> {
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
            None => Ok(Bytes::from(vec![0u8; 32])),
        }
    }

    fn apply_result(&mut self, result: StateResult) -> Result<StateResult> {
        match result {
            StateResult::Transition(new_state, effect) => {
                match effect {
                    ConnectionEffect::None => {
                        self.current_state = new_state;
                    }
                    ConnectionEffect::PreserveAuthProvider(provider) => {
                        tracing::debug!("Preserving auth provider via effect");
                        self.preserved_auth_provider = Some(provider);
                        self.current_state = new_state;
                    }
                    ConnectionEffect::AuthSign {
                        connection_mode,
                        session_key,
                        connection_secret,
                        global_id,
                        auth_provider,
                    } => {
                        // Client side: compute HMAC-SHA256 signatures, set up encryption,
                        // then build the real AuthConnectingSign state.
                        if let Some(p) = auth_provider {
                            self.preserved_auth_provider = Some(p);
                        }
                        self.global_id = global_id;
                        self.connection_mode = connection_mode;
                        self.connection_secret = connection_secret.clone();

                        let our_sig =
                            Self::hmac_sha256(session_key.as_ref(), &self.pre_auth_rxbuf)?;
                        let expected_sig =
                            Self::hmac_sha256(session_key.as_ref(), &self.pre_auth_txbuf)?;

                        tracing::debug!(
                            "Client AUTH_SIGNATURE: {} bytes (rxbuf={} bytes, txbuf={} bytes)",
                            our_sig.len(),
                            self.pre_auth_rxbuf.len(),
                            self.pre_auth_txbuf.len()
                        );

                        self.complete_pre_auth(session_key.clone());
                        if let Some(ref secret) = connection_secret {
                            tracing::debug!("Setting up SECURE-mode encryption (client)");
                            self.setup_encryption(secret)?;
                        }

                        let server_addr = self.server_addr.clone().unwrap_or_default();
                        let client_addr = self.client_addr.clone().unwrap_or_default();
                        self.current_state = Box::new(AuthConnectingSign::new_with_encryption(
                            connection_mode,
                            session_key,
                            connection_secret,
                            our_sig,
                            Some(expected_sig),
                            global_id,
                            server_addr,
                            client_addr,
                            self.peer_supported_features,
                            self.config.supported_features,
                        ));
                    }
                    ConnectionEffect::SessionReady {
                        compression_algorithm,
                    } => {
                        // Client side: optionally install compression, then build
                        // SessionConnecting from machine-level fields set during AuthSign.
                        if let Some(alg) = compression_algorithm {
                            tracing::debug!("Installing compression: {alg:?}");
                            self.setup_compression(alg);
                        }
                        let server_addr = self.server_addr.clone().unwrap_or_default();
                        let client_addr = self.client_addr.clone().unwrap_or_default();
                        self.current_state = Box::new(SessionConnecting::new_with_encryption(
                            self.connection_mode,
                            self.session_key.clone(),
                            self.connection_secret.clone(),
                            None,
                            self.global_id,
                            server_addr,
                            client_addr,
                            self.client_cookie,
                            self.server_cookie,
                            self.global_seq,
                            self.connect_seq,
                            self.in_seq,
                        ));
                    }
                    ConnectionEffect::ServerAuthComplete => {
                        // Server side: route to CompressionAccepting or SessionAccepting.
                        if self.compression_negotiation_needed() {
                            tracing::debug!(
                                "Server: Transitioning to COMPRESSION_ACCEPTING after auth"
                            );
                            self.current_state = Box::new(CompressionAccepting::new());
                        } else {
                            self.current_state = new_state; // SessionAccepting placeholder
                        }
                    }
                    ConnectionEffect::ServerInstallCompression(_) => {
                        // Unexpected in Transition arm — install as-is.
                        self.current_state = new_state;
                    }
                }
                let enter_result = self.current_state.enter()?;
                self.apply_result(enter_result)
            }

            StateResult::SendFrame {
                frame,
                next_state,
                effect,
            } => {
                match effect {
                    ConnectionEffect::None => {
                        if let Some(new_state) = next_state {
                            // Inject server auth handler when transitioning to AuthAccepting.
                            if new_state.kind() == StateKind::AuthAccepting {
                                if let Some(handler) = self.server_auth_handler.take() {
                                    self.current_state =
                                        Box::new(AuthAccepting::with_handler(handler));
                                } else {
                                    self.current_state = new_state;
                                }
                            } else {
                                self.current_state = new_state;
                            }
                        }
                    }
                    ConnectionEffect::PreserveAuthProvider(provider) => {
                        self.preserved_auth_provider = Some(provider);
                        if let Some(new_state) = next_state {
                            self.current_state = new_state;
                        }
                    }
                    ConnectionEffect::AuthSign {
                        connection_mode,
                        session_key,
                        connection_secret,
                        global_id: _,
                        auth_provider: _,
                    } => {
                        // Server side: compute HMAC-SHA256 signatures, set up encryption,
                        // and install AuthAcceptingSign.  protocol.rs calls enter() explicitly.
                        self.connection_mode = connection_mode;
                        self.connection_secret = connection_secret.clone();

                        let our_sig =
                            Self::hmac_sha256(session_key.as_ref(), &self.pre_auth_rxbuf)?;
                        let expected_sig =
                            Self::hmac_sha256(session_key.as_ref(), &self.pre_auth_txbuf)?;

                        tracing::debug!(
                            "Server AUTH_SIGNATURE: {} bytes (rxbuf={} bytes, txbuf={} bytes)",
                            our_sig.len(),
                            self.pre_auth_rxbuf.len(),
                            self.pre_auth_txbuf.len()
                        );

                        self.complete_pre_auth(session_key.clone());
                        if let Some(ref secret) = connection_secret {
                            tracing::debug!("Setting up SECURE-mode encryption (server)");
                            self.setup_encryption(secret)?;
                        }

                        self.current_state = Box::new(AuthAcceptingSign::new_with_signatures(
                            connection_mode,
                            session_key,
                            connection_secret,
                            our_sig,
                            Some(expected_sig),
                        ));
                    }
                    ConnectionEffect::ServerAuthComplete => {
                        // CRC-mode AUTH_DONE: route to CompressionAccepting or SessionAccepting.
                        if self.compression_negotiation_needed() {
                            tracing::debug!(
                                "Server: Transitioning to COMPRESSION_ACCEPTING after CRC-mode auth"
                            );
                            self.current_state = Box::new(CompressionAccepting::new());
                        } else {
                            self.current_state = Box::new(SessionAccepting::new());
                        }
                    }
                    ConnectionEffect::ServerInstallCompression(alg) => {
                        // CompressionAccepting sent COMPRESSION_DONE: install compression.
                        tracing::debug!("Server: Installing compression algorithm {alg:?}");
                        self.setup_compression(alg);
                        self.current_state =
                            next_state.unwrap_or_else(|| Box::new(SessionAccepting::new()));
                    }
                    ConnectionEffect::SessionReady { .. } => {
                        // Not expected in SendFrame arm; install as-is.
                        if let Some(new_state) = next_state {
                            self.current_state = new_state;
                        }
                    }
                }
                Ok(StateResult::SendFrame {
                    frame,
                    next_state: None,
                    effect: ConnectionEffect::None,
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
                self.last_keepalive_ack = Some(timestamp);
                Ok(StateResult::Continue)
            }
            StateResult::Ready => {
                self.negotiated_features = self.current_state.negotiated_features();
                self.current_state = Box::new(Ready);
                Ok(StateResult::Ready)
            }
            StateResult::ReconnectReady { msg_seq } => {
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
    fn test_default_config_does_not_support_compression() {
        let sm = StateMachine::new_client(crate::ConnectionConfig::default());
        assert!(
            !sm.we_support_compression(),
            "Default msgr2 config should not advertise compression"
        );
    }

    #[test]
    fn test_server_only_enters_compression_accepting_when_we_support_it() {
        let mut sm = StateMachine::new_server();
        sm.current_state = Box::new(AuthAcceptingSign::new(0, None, None));
        sm.set_peer_supported_features(crate::FeatureSet::ALL.bits());

        let result = sm
            .apply_result(StateResult::Transition(
                Box::new(SessionAccepting::new()),
                ConnectionEffect::ServerAuthComplete,
            ))
            .expect("server transition should succeed");

        assert!(
            matches!(result, StateResult::Continue),
            "server should not emit extra actions when compression is not negotiated"
        );
        assert_eq!(
            sm.current_state_kind(),
            StateKind::SessionAccepting,
            "server should skip COMPRESSION_ACCEPTING when compression is disabled locally"
        );
    }

    #[test]
    fn test_server_enters_compression_accepting_when_both_sides_support_it() {
        let mut sm = StateMachine::new_server();
        sm.current_state = Box::new(AuthAcceptingSign::new(0, None, None));
        sm.config.supported_features = crate::FeatureSet::ALL.bits();
        sm.set_peer_supported_features(crate::FeatureSet::ALL.bits());

        let result = sm
            .apply_result(StateResult::Transition(
                Box::new(SessionAccepting::new()),
                ConnectionEffect::ServerAuthComplete,
            ))
            .expect("server transition should succeed");

        assert!(
            matches!(result, StateResult::Continue),
            "compression-accepting transition should not emit extra actions"
        );
        assert_eq!(
            sm.current_state_kind(),
            StateKind::CompressionAccepting,
            "server should only enter COMPRESSION_ACCEPTING when both sides advertise compression"
        );
    }

    #[test]
    fn test_server_negotiates_requested_compression_method() {
        let mut sm = StateMachine::new_server();
        sm.current_state = Box::new(CompressionAccepting::new());

        let request = CompressionRequestFrame::new(
            true,
            vec![
                crate::compression::CompressionAlgorithm::Zstd.into(),
                crate::compression::CompressionAlgorithm::Snappy.into(),
            ],
        );
        let frame =
            create_frame_from_trait(&request, Tag::CompressionRequest).expect("request frame");

        let result = sm
            .handle_frame(frame)
            .expect("compression negotiation should succeed");
        let response = match result {
            StateResult::SendFrame { frame, .. } => frame,
            other => panic!("expected SendFrame, got {:?}", other),
        };

        let mut payload = response.segments[0].clone();
        let done = CompressionDoneFrame {
            is_compress: bool::decode(&mut payload, 0).expect("decode is_compress"),
            method: u32::decode(&mut payload, 0).expect("decode method"),
        };
        assert!(done.is_compress, "server should enable compression");
        assert_eq!(
            done.method,
            u32::from(crate::compression::CompressionAlgorithm::Zstd),
            "server should honor the first mutually supported preferred method"
        );
        assert_eq!(sm.current_state_kind(), StateKind::SessionAccepting);

        let compression_ctx = sm
            .compression_ctx()
            .expect("server should install compression context after negotiating it");
        assert_eq!(
            compression_ctx.algorithm(),
            crate::compression::CompressionAlgorithm::Zstd
        );
    }

    #[test]
    fn test_server_declines_unknown_compression_methods() {
        let mut sm = StateMachine::new_server();
        sm.current_state = Box::new(CompressionAccepting::new());

        let request = CompressionRequestFrame::new(true, vec![999]);
        let frame =
            create_frame_from_trait(&request, Tag::CompressionRequest).expect("request frame");

        let result = sm
            .handle_frame(frame)
            .expect("compression negotiation should succeed");
        let response = match result {
            StateResult::SendFrame { frame, .. } => frame,
            other => panic!("expected SendFrame, got {:?}", other),
        };

        let mut payload = response.segments[0].clone();
        let done = CompressionDoneFrame {
            is_compress: bool::decode(&mut payload, 0).expect("decode is_compress"),
            method: u32::decode(&mut payload, 0).expect("decode method"),
        };
        assert!(
            !done.is_compress,
            "server should decline compression when no mutually supported method exists"
        );
        assert_eq!(done.method, 0);
        assert!(
            sm.compression_ctx().is_none(),
            "server should not install compression context when negotiation fails"
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
