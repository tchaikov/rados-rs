//! Authentication phase: AUTH_REQUEST / AUTH_DONE (and optional AUTH_REPLY_MORE).
//!
//! **Client** sends `AUTH_REQUEST`, handles server replies:
//! - `AUTH_BAD_METHOD` → retry with a different auth method (up to 3 tries)
//! - `AUTH_REPLY_MORE` → CephX challenge-response round-trip
//! - `AUTH_DONE`       → authentication complete, phase finishes
//!
//! **Server** waits for `AUTH_REQUEST`, performs authentication (optionally
//! sending `AUTH_REPLY_MORE` for CephX), then replies with `AUTH_DONE`.

use crate::{
    error::{Msgr2Error as Error, Result},
    frames::{AuthDoneFrame, AuthRequestFrame, AuthRequestMoreFrame, Frame, Tag},
    phase::{Phase, Step},
    state_machine::create_frame_from_trait,
    AuthMethod, ConnectionMode,
};
use auth::AuthProvider;
use bytes::Bytes;
use denc::Denc;

// ── Shared output ─────────────────────────────────────────────────────────────

/// Data produced by a completed auth phase.
#[derive(Debug)]
pub struct AuthOutput {
    /// Global ID assigned by the server (or 0 for AUTH_NONE with monitors).
    pub global_id: u64,
    /// Negotiated connection mode (`CRC = 1`, `SECURE = 2`).
    pub connection_mode: u32,
    /// Session key for HMAC-SHA256 auth signature (None for AUTH_NONE).
    pub session_key: Option<Bytes>,
    /// Connection secret for SECURE-mode AES-GCM encryption (None for CRC mode).
    pub connection_secret: Option<Bytes>,
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Client-side authentication phase.
///
/// Drives the full AUTH_REQUEST ↔ AUTH_DONE exchange, including method
/// re-negotiation (`AUTH_BAD_METHOD`) and multi-round CephX challenge
/// (`AUTH_REPLY_MORE`).
pub struct AuthClient {
    method: AuthMethod,
    preferred_modes: Vec<ConnectionMode>,
    supported_methods: Vec<AuthMethod>,
    tried_methods: Vec<AuthMethod>,
    auth_provider: Option<Box<dyn AuthProvider>>,
    service_id: u32,
    entity_name: denc::EntityName,
    global_id: u64,
}

impl Clone for AuthClient {
    fn clone(&self) -> Self {
        Self {
            method: self.method,
            preferred_modes: self.preferred_modes.clone(),
            supported_methods: self.supported_methods.clone(),
            tried_methods: self.tried_methods.clone(),
            auth_provider: self.auth_provider.clone(),
            service_id: self.service_id,
            entity_name: self.entity_name.clone(),
            global_id: self.global_id,
        }
    }
}

impl AuthClient {
    pub fn new(
        preferred_modes: Vec<ConnectionMode>,
        supported_methods: Vec<AuthMethod>,
        auth_provider: Option<Box<dyn AuthProvider>>,
        service_id: u32,
        entity_name: denc::EntityName,
        global_id: u64,
    ) -> Self {
        let method = supported_methods
            .first()
            .copied()
            .unwrap_or(AuthMethod::None);
        tracing::info!(
            "AuthClient: starting with method={method:?}, supported={supported_methods:?}",
        );
        Self {
            method,
            preferred_modes,
            supported_methods,
            tried_methods: Vec::new(),
            auth_provider,
            service_id,
            entity_name,
            global_id,
        }
    }

    fn build_auth_request(&mut self) -> Result<Frame> {
        let modes: Vec<u32> = self.preferred_modes.iter().map(|m| (*m).into()).collect();
        let (method_id, payload) = match self.method {
            AuthMethod::None => {
                let is_service = self.service_id != 0 && self.global_id != 0;
                let pld = if is_service {
                    crate::AuthNonePayload::for_service(self.entity_name.clone(), self.global_id)
                } else {
                    crate::AuthNonePayload::for_monitor(self.entity_name.clone(), 0)
                };
                (AuthMethod::None.into(), pld.encode()?)
            }
            AuthMethod::Cephx => {
                let provider = self
                    .auth_provider
                    .as_mut()
                    .ok_or_else(|| Error::protocol_error("No auth provider for CephX"))?;
                let payload = provider.build_auth_payload(0, self.service_id)?;
                (AuthMethod::Cephx.into(), payload)
            }
            _ => {
                return Err(Error::protocol_error(&format!(
                    "Unsupported auth method: {:?}",
                    self.method
                )))
            }
        };
        let req = AuthRequestFrame::new(method_id, modes, payload);
        create_frame_from_trait(&req, Tag::AuthRequest)
    }

    fn negotiate_method(&self, allowed: &[u32]) -> Result<AuthMethod> {
        let server: Vec<AuthMethod> = allowed
            .iter()
            .filter_map(|&m| AuthMethod::try_from(m).ok())
            .collect();
        self.supported_methods
            .iter()
            .find(|m| server.contains(m))
            .copied()
            .ok_or_else(|| {
                Error::Protocol(format!(
                    "No common auth method. Client: {:?}, Server: {:?}",
                    self.supported_methods, server
                ))
            })
    }

    fn negotiate_modes(&self, allowed: &[u32]) -> Vec<ConnectionMode> {
        let server: Vec<ConnectionMode> = allowed
            .iter()
            .filter_map(|&m| ConnectionMode::try_from(m).ok())
            .collect();
        let negotiated: Vec<_> = self
            .preferred_modes
            .iter()
            .filter(|m| server.contains(m))
            .cloned()
            .collect();
        if !negotiated.is_empty() {
            negotiated
        } else {
            server
                .first()
                .cloned()
                .map(|m| vec![m])
                .unwrap_or_else(|| vec![ConnectionMode::Crc])
        }
    }
}

impl Phase for AuthClient {
    type Output = AuthOutput;

    fn enter(&mut self) -> Result<Option<Frame>> {
        Ok(Some(self.build_auth_request()?))
    }

    fn step(mut self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        match frame.preamble.tag {
            Tag::AuthBadMethod => {
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error("AUTH_BAD_METHOD missing payload"));
                }
                let mut p = frame.segments[0].clone();
                let _rejected: u32 = u32::decode(&mut p, 0)?;
                let _result: i32 = i32::decode(&mut p, 0)?;
                let allowed_methods: Vec<u32> = Vec::decode(&mut p, 0)?;
                let allowed_modes: Vec<u32> = Vec::decode(&mut p, 0)?;

                if self.tried_methods.len() >= 3 {
                    return Err(Error::Protocol(format!(
                        "Auth retry limit exceeded after {:?}",
                        self.tried_methods
                    )));
                }
                let new_method = self.negotiate_method(&allowed_methods)?;
                let mut tried = self.tried_methods.clone();
                tried.push(self.method);
                if tried.contains(&new_method) {
                    return Err(Error::Protocol(format!(
                        "Already tried auth method {new_method:?}"
                    )));
                }
                let new_modes = self.negotiate_modes(&allowed_modes);
                tracing::info!(
                    "AUTH_BAD_METHOD: retrying with {new_method:?}, modes={new_modes:?}"
                );
                let mut next = Self {
                    method: new_method,
                    preferred_modes: new_modes,
                    tried_methods: tried,
                    ..self
                };
                let req = next.build_auth_request()?;
                Ok(Step::Next {
                    state: next,
                    send: Some(req),
                })
            }

            Tag::AuthReplyMore => {
                let provider = self.auth_provider.as_mut().ok_or_else(|| {
                    Error::protocol_error("AUTH_REPLY_MORE but no CephX auth provider")
                })?;
                let payload = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("AUTH_REPLY_MORE missing payload"))?;
                provider.handle_auth_response(payload.clone(), 0, 0)?;
                let response_payload = provider.build_auth_payload(0, self.service_id)?;
                let more = AuthRequestMoreFrame::new(response_payload);
                let resp = create_frame_from_trait(&more, Tag::AuthRequestMore)?;
                Ok(Step::Next {
                    state: self,
                    send: Some(resp),
                })
            }

            Tag::AuthDone => {
                let segment = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("AUTH_DONE missing payload"))?;
                let mut p = segment.clone();
                let global_id = u64::decode(&mut p, 0)?;
                let con_mode = u32::decode(&mut p, 0)?;
                let auth_payload = Bytes::decode(&mut p, 0)?;

                tracing::info!("AUTH_DONE: global_id={global_id}, connection_mode={con_mode}");

                let (session_key, connection_secret) = if self.method == AuthMethod::None {
                    (None, None)
                } else {
                    let provider = self
                        .auth_provider
                        .as_mut()
                        .ok_or_else(|| Error::protocol_error("No auth provider"))?;
                    let (sk, cs) =
                        provider.handle_auth_response(auth_payload, global_id, con_mode)?;
                    (sk, cs)
                };

                Ok(Step::Done(
                    AuthOutput {
                        global_id,
                        connection_mode: con_mode,
                        session_key,
                        connection_secret,
                    },
                    None,
                ))
            }

            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in auth phase (client)",
                frame.preamble.tag
            ))),
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Server-side authentication phase.
///
/// Waits for `AUTH_REQUEST` from the client.  For AUTH_NONE it replies
/// immediately with `AUTH_DONE`.  For CephX it sends `AUTH_REPLY_MORE` first,
/// then processes the response in a second round.
pub struct AuthServer {
    auth_handler: Option<auth::CephXServerHandler>,
    entity_name: Option<denc::EntityName>,
    global_id: Option<u64>,
    /// 0 = initial request, 1 = challenge response
    phase: u8,
    connection_mode: u32,
    client_preferred_modes: Vec<u32>,
}

impl AuthServer {
    pub fn new(auth_handler: Option<auth::CephXServerHandler>) -> Self {
        Self {
            auth_handler,
            entity_name: None,
            global_id: None,
            phase: 0,
            connection_mode: ConnectionMode::Crc as u32,
            client_preferred_modes: Vec::new(),
        }
    }

    fn negotiate_mode(client_modes: &[u32]) -> u32 {
        use crate::{CRC_MODE, SECURE_MODE};
        if client_modes.contains(&SECURE_MODE) {
            SECURE_MODE
        } else {
            CRC_MODE
        }
    }
}

impl Phase for AuthServer {
    type Output = AuthOutput;

    fn step(mut self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        if frame.preamble.tag != Tag::AuthRequest && frame.preamble.tag != Tag::AuthRequestMore {
            return Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in auth phase (server)",
                frame.preamble.tag
            )));
        }

        let auth_request = AuthRequestFrame::from_frame(&frame)?;

        if self.phase == 0 {
            self.client_preferred_modes = auth_request.preferred_modes.clone();
            self.connection_mode = Self::negotiate_mode(&auth_request.preferred_modes);

            if let Some(ref mut handler) = self.auth_handler {
                let (entity_name, global_id, challenge) =
                    handler.handle_initial_request(&auth_request.auth_payload)?;
                tracing::debug!("Server auth phase 0: entity={entity_name}, gid={global_id}");
                self.entity_name = Some(entity_name);
                self.global_id = Some(global_id);
                self.phase = 1;

                let more = AuthRequestMoreFrame::new(challenge);
                let frame = create_frame_from_trait(&more, Tag::AuthReplyMore)?;
                return Ok(Step::Next {
                    state: self,
                    send: Some(frame),
                });
            }

            // No auth handler — accept without authentication
            tracing::warn!("Server: no auth handler, skipping authentication");
            let global_id = 1001u64;
            let done = AuthDoneFrame::new(global_id, self.connection_mode, Bytes::new());
            let done_frame = create_frame_from_trait(&done, Tag::AuthDone)?;
            return Ok(Step::Done(
                AuthOutput {
                    global_id,
                    connection_mode: self.connection_mode,
                    session_key: None,
                    connection_secret: None,
                },
                Some(done_frame),
            ));
        }

        // Phase 1: CephX challenge response
        let handler = self
            .auth_handler
            .as_mut()
            .ok_or_else(|| Error::protocol_error("Missing auth handler in phase 1"))?;
        let entity_name = self
            .entity_name
            .as_ref()
            .ok_or_else(|| Error::protocol_error("Missing entity_name in phase 1"))?;
        let global_id = self
            .global_id
            .ok_or_else(|| Error::protocol_error("Missing global_id in phase 1"))?;

        let (session_key, connection_secret, auth_payload) =
            handler.handle_authenticate(entity_name, global_id, &auth_request.auth_payload)?;
        tracing::info!("Server: {entity_name} authenticated successfully");

        let done_payload = handler.build_auth_done_response(
            global_id,
            self.connection_mode as u8,
            &session_key,
            &connection_secret,
            auth_payload,
        )?;

        let done = AuthDoneFrame::new(global_id, self.connection_mode, done_payload);
        let done_frame = create_frame_from_trait(&done, Tag::AuthDone)?;

        Ok(Step::Done(
            AuthOutput {
                global_id,
                connection_mode: self.connection_mode,
                session_key: Some(session_key.secret.clone()),
                connection_secret: Some(connection_secret.secret.clone()),
            },
            Some(done_frame),
        ))
    }
}
