//! Authentication phase: AUTH_REQUEST / AUTH_DONE (and optional AUTH_REPLY_MORE).
//!
//! **Client** sends `AUTH_REQUEST`, handles server replies:
//! - `AUTH_BAD_METHOD` → retry with a different auth method, using the
//!   ordered-search semantics from Ceph C++ `MonConnection::handle_auth_bad_method`
//!   (`src/mon/MonClient.cc:1870-1894`): anchor at the position of the
//!   rejected method in our preference-ordered supported list and search
//!   forward. The search terminates naturally when no more methods are
//!   viable, so there is no explicit retry counter.
//! - `AUTH_REPLY_MORE` → CephX challenge-response round-trip
//! - `AUTH_DONE`       → authentication complete, phase finishes
//!
//! **Server** waits for `AUTH_REQUEST`, performs authentication (optionally
//! sending `AUTH_REPLY_MORE` for CephX), then replies with `AUTH_DONE`.

use crate::Denc;
use crate::auth::AuthProvider;
use crate::msgr2::{
    AuthMethod, ConnectionMode,
    error::{Msgr2Error as Error, Result},
    frames::{
        AuthDoneFrame, AuthRequestFrame, AuthRequestMoreFrame, Frame, Tag, create_frame_from_trait,
    },
    phase::{Phase, Step},
};
use bytes::Bytes;

/// Render a Ceph result code (negative errno, kernel convention) as a
/// human-readable string by delegating to `std::io::Error`, which calls
/// libc `strerror_r` under the hood. Displays as e.g.
/// `"Permission denied (os error 13)"`.
fn strerror(result: i32) -> std::io::Error {
    std::io::Error::from_raw_os_error(result.unsigned_abs() as i32)
}

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
    entity_name: crate::EntityName,
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
        entity_name: crate::EntityName,
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
                    crate::msgr2::AuthNonePayload::for_service(
                        self.entity_name.clone(),
                        self.global_id,
                    )
                } else {
                    crate::msgr2::AuthNonePayload::for_monitor(self.entity_name.clone(), 0)
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
                )));
            }
        };
        let req = AuthRequestFrame::new(method_id, modes, payload);
        Ok(create_frame_from_trait(&req, Tag::AuthRequest)?)
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
                let mut p = frame.segments[0].as_ref();
                let server_rejected: u32 = u32::decode(&mut p, 0)?;
                let server_result: i32 = i32::decode(&mut p, 0)?;
                let allowed_methods: Vec<u32> = Vec::decode(&mut p, 0)?;
                let allowed_modes: Vec<u32> = Vec::decode(&mut p, 0)?;

                // The server tells us which method it rejected; in the
                // normal case it's the same method we just sent
                // (`self.method`), but decoding it separately lets the
                // ordered search below anchor at the server's position
                // even if the two ever differ. If the server reports a
                // method we don't know about (e.g. a future auth method)
                // we fall back to searching from the start of our
                // supported list.
                let rejected_method = AuthMethod::try_from(server_rejected).ok();

                // Log the rejected method and result errno at info level
                // so operators can correlate a failure with the Ceph
                // monitor's own logs. Previously these fields were
                // decoded and dropped, leaving operators to guess why
                // authentication failed.
                tracing::info!(
                    "AUTH_BAD_METHOD: server rejected method={} ({:?}) with \
                     result={} ({}), allowed_methods={:?}, allowed_modes={:?}",
                    server_rejected,
                    rejected_method,
                    server_result,
                    strerror(server_result),
                    allowed_methods,
                    allowed_modes,
                );

                // Mark the method we sent (and the one the server
                // reports rejecting, if different) as tried so the
                // ordered search below won't pick either.
                let mut tried = self.tried_methods.clone();
                if !tried.contains(&self.method) {
                    tried.push(self.method);
                }
                if let Some(rm) = rejected_method
                    && !tried.contains(&rm)
                {
                    tried.push(rm);
                }

                // Decode the server's allowed-methods list, dropping
                // any values we don't recognise.
                let server_allowed: Vec<AuthMethod> = allowed_methods
                    .iter()
                    .filter_map(|&m| AuthMethod::try_from(m).ok())
                    .collect();

                // Ordered search, mirroring Ceph C++
                // `MonConnection::handle_auth_bad_method`
                // (`src/mon/MonClient.cc:1880-1890`): anchor at the
                // position of the rejected method in our
                // preference-ordered supported list, then search forward
                // for the first method that is (a) also in the server's
                // allowed list and (b) not already in `tried`. Moving
                // forward only prevents falling back to a less-preferred
                // method that we've already moved past.
                //
                // If the server reports a rejected method we don't know,
                // `anchor_pos` is `None` and we search from the start of
                // the supported list. The `tried` set still prevents
                // re-sending any method we've already attempted.
                //
                // The search terminates naturally when no more methods
                // are viable; that's why there's no explicit retry
                // counter here. The `3`-attempt cap that previously
                // lived in this arm was a magic number with no basis in
                // the msgr2 protocol — neither Ceph C++ nor the Linux
                // kernel imposes such a limit. The ordered search
                // bounds the iteration by `supported_methods.len()`
                // naturally.
                let anchor_pos = rejected_method
                    .and_then(|rm| self.supported_methods.iter().position(|&m| m == rm));
                let search_start = anchor_pos.map(|pos| pos + 1).unwrap_or(0);

                let new_method = self.supported_methods[search_start..]
                    .iter()
                    .find(|m| server_allowed.contains(m) && !tried.contains(m))
                    .copied()
                    .ok_or_else(|| {
                        Error::Protocol(format!(
                            "No viable auth method remaining: client supported={:?}, \
                             server allowed={:?}, already tried={:?}; server last \
                             rejected method={} with result={} ({})",
                            self.supported_methods,
                            server_allowed,
                            tried,
                            server_rejected,
                            server_result,
                            strerror(server_result),
                        ))
                    })?;

                let new_modes = self.negotiate_modes(&allowed_modes);
                tracing::info!("Retrying auth with method={new_method:?}, modes={new_modes:?}");
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
    auth_handler: Option<crate::auth::CephXServerHandler>,
    entity_name: Option<crate::EntityName>,
    global_id: Option<u64>,
    /// 0 = initial request, 1 = challenge response
    phase: u8,
    connection_mode: u32,
    client_preferred_modes: Vec<u32>,
}

impl AuthServer {
    pub fn new(auth_handler: Option<crate::auth::CephXServerHandler>) -> Self {
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
        let secure: u32 = crate::msgr2::ConnectionMode::Secure.into();
        if client_modes.contains(&secure) {
            secure
        } else {
            crate::msgr2::ConnectionMode::Crc.into()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgr2::frames::{FrameFlags, MAX_NUM_SEGMENTS, Preamble, SegmentDescriptor};
    use bytes::BytesMut;

    /// Build an AUTH_BAD_METHOD frame's first segment from raw field
    /// values. Payload layout matches what `AuthClient::step` decodes:
    ///
    ///   u32 rejected_method
    ///   i32 result
    ///   Vec<u32> allowed_methods
    ///   Vec<u32> allowed_modes
    fn bad_method_payload(
        rejected: u32,
        result: i32,
        allowed_methods: &[u32],
        allowed_modes: &[u32],
    ) -> Bytes {
        let mut buf = BytesMut::new();
        Denc::encode(&rejected, &mut buf, 0).unwrap();
        Denc::encode(&result, &mut buf, 0).unwrap();
        Denc::encode(&allowed_methods.to_vec(), &mut buf, 0).unwrap();
        Denc::encode(&allowed_modes.to_vec(), &mut buf, 0).unwrap();
        buf.freeze()
    }

    /// Wrap a payload segment into a minimal `Frame` with the
    /// `AuthBadMethod` tag so it can be fed to `AuthClient::step`.
    fn bad_method_frame(payload: Bytes) -> Frame {
        let segments = vec![payload];
        let preamble = Preamble {
            tag: Tag::AuthBadMethod,
            num_segments: 1,
            segments: [SegmentDescriptor::default(); MAX_NUM_SEGMENTS],
            flags: FrameFlags::default(),
            reserved: 0,
            crc: 0,
        };
        Frame { preamble, segments }
    }

    /// Construct an AuthClient with a deterministic set of supported
    /// methods and starting with `method` as the first attempt. No
    /// auth_provider is installed because the tests that exercise the
    /// ordered-search path either fall through to `AuthMethod::None`
    /// (which does not consult the provider) or exit with an error
    /// before `build_auth_request` is called.
    fn client(
        supported: Vec<AuthMethod>,
        method: AuthMethod,
        tried: Vec<AuthMethod>,
    ) -> AuthClient {
        AuthClient {
            method,
            preferred_modes: vec![ConnectionMode::Crc],
            supported_methods: supported,
            tried_methods: tried,
            auth_provider: None,
            service_id: 0,
            entity_name: crate::EntityName::client(""),
            global_id: 0,
        }
    }

    #[test]
    fn bad_method_ordered_search_picks_next_method_after_rejected() {
        // Supported: [Cephx, None] in preference order. Server rejects
        // the current method (Cephx) and advertises [None] as allowed.
        // The ordered search should anchor at Cephx's position (0) and
        // find None at position 1.
        let session = client(
            vec![AuthMethod::Cephx, AuthMethod::None],
            AuthMethod::Cephx,
            vec![],
        );
        let payload = bad_method_payload(
            AuthMethod::Cephx.into(),
            -13,
            &[AuthMethod::None.into()],
            &[ConnectionMode::Crc as u32],
        );
        let frame = bad_method_frame(payload);

        match session.step(frame).unwrap() {
            Step::Next {
                state,
                send: Some(_),
            } => {
                assert_eq!(state.method, AuthMethod::None);
                assert!(
                    state.tried_methods.contains(&AuthMethod::Cephx),
                    "Cephx must be recorded as tried"
                );
            }
            _ => panic!("expected Step::Next with new method and a frame to send"),
        }
    }

    #[test]
    fn bad_method_ordered_search_skips_method_not_in_allowed_list() {
        // Supported: [Cephx, None]. Server rejects Cephx and advertises
        // [Cephx] only (pathological but possible). The ordered search
        // should find no viable method because:
        //   - position 0 (Cephx) is anchor+0, skipped (we start at 1)
        //   - position 1 (None) is not in server_allowed
        // So we get the "no viable method" error.
        let session = client(
            vec![AuthMethod::Cephx, AuthMethod::None],
            AuthMethod::Cephx,
            vec![],
        );
        let payload = bad_method_payload(
            AuthMethod::Cephx.into(),
            -95,
            &[AuthMethod::Cephx.into()],
            &[ConnectionMode::Crc as u32],
        );
        let frame = bad_method_frame(payload);

        let err = match session.step(frame) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected no-viable-method error"),
        };
        assert!(
            err.contains("No viable auth method"),
            "unexpected error: {err}"
        );
        // Error should include the server's result errno for operator
        // debugging — this covers change 2. `strerror` delegates to
        // `std::io::Error::from_raw_os_error`, which formats as
        // "<strerror text> (os error <n>)", so we check for the
        // signed raw code and for the "os error" token rather than a
        // symbolic name whose wording could vary by libc.
        assert!(
            err.contains("-95") && err.contains("(os error 95)"),
            "error should carry result errno: {err}"
        );
    }

    #[test]
    fn bad_method_fails_cleanly_when_only_method_is_rejected() {
        // Supported: [Cephx] (only one method). Server rejects Cephx.
        // The ordered search should fail because nothing comes after
        // position 0.
        let session = client(vec![AuthMethod::Cephx], AuthMethod::Cephx, vec![]);
        let payload = bad_method_payload(
            AuthMethod::Cephx.into(),
            -13,
            &[AuthMethod::Cephx.into()],
            &[ConnectionMode::Crc as u32],
        );
        let frame = bad_method_frame(payload);

        let err = match session.step(frame) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected single-method exhaustion error"),
        };
        assert!(
            err.contains("No viable auth method"),
            "unexpected error: {err}"
        );
        assert!(
            err.contains("(os error 13)"),
            "error should render -13: {err}"
        );
    }

    #[test]
    fn bad_method_unknown_rejected_method_falls_back_to_supported_list_start() {
        // Server reports a rejected method (99) that we don't know.
        // `rejected_method = None`, so `anchor_pos = None`, and
        // `search_start = 0`. The tried set still contains our current
        // method (Cephx) so the search picks the next unvisited
        // supported method — None.
        let session = client(
            vec![AuthMethod::Cephx, AuthMethod::None],
            AuthMethod::Cephx,
            vec![],
        );
        let payload = bad_method_payload(
            99,
            -22,
            &[AuthMethod::None.into(), AuthMethod::Cephx.into()],
            &[ConnectionMode::Crc as u32],
        );
        let frame = bad_method_frame(payload);

        match session.step(frame).unwrap() {
            Step::Next {
                state,
                send: Some(_),
            } => {
                assert_eq!(state.method, AuthMethod::None);
            }
            _ => panic!("expected Step::Next with AuthMethod::None"),
        }
    }

    #[test]
    fn bad_method_error_preserves_result_errno_for_operator_debug() {
        // The main observability improvement (change 2): when the
        // ordered search fails, the error message must carry the
        // server's errno so operators can tell WHY it failed without
        // consulting the Ceph daemon logs.
        let session = client(vec![AuthMethod::Cephx], AuthMethod::Cephx, vec![]);
        let payload = bad_method_payload(
            AuthMethod::Cephx.into(),
            -1,
            &[],
            &[ConnectionMode::Crc as u32],
        );
        let frame = bad_method_frame(payload);

        let err = match session.step(frame) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error"),
        };
        assert!(
            err.contains("-1") && err.contains("(os error 1)"),
            "error should expose errno: {err}"
        );
        assert!(
            err.contains("rejected method"),
            "error should mention the rejected method: {err}"
        );
    }
}
