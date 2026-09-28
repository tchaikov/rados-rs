//! Authentication phase: AUTH_REQUEST / AUTH_DONE (and optional AUTH_REPLY_MORE).
//!
//! **Client** sends `AUTH_REQUEST`, offering the connection modes configured
//! for the peer as C++ `AuthRegistry::get_supported_modes` does, and handles
//! server replies:
//! - `AUTH_BAD_METHOD` → retry with a different auth method, using the
//!   ordered-search semantics from Ceph `MonConnection::handle_auth_bad_method`
//!   (anchor at the rejected method's position in our preference-ordered list,
//!   search forward). Terminates naturally when no method is viable. The
//!   retry offers the configured modes for the new method, whatever modes
//!   the server said it allows.
//! - `AUTH_REPLY_MORE` → CephX challenge-response round-trip
//! - `AUTH_DONE`       → authentication complete, phase finishes, unless it
//!   names a connection mode the client did not offer
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

/// Wrap a Ceph result code (negative errno, kernel convention) as an
/// `io::Error` so its `Display` yields the libc `strerror_r` text, e.g.
/// `"Permission denied (os error 13)"`.
fn os_error(result: i32) -> std::io::Error {
    std::io::Error::from_raw_os_error(result.unsigned_abs() as i32)
}

// ── Shared output ─────────────────────────────────────────────────────────────

/// Data produced by a completed auth phase.
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

impl std::fmt::Debug for AuthOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::auth::types::Redacted;
        f.debug_struct("AuthOutput")
            .field("global_id", &self.global_id)
            .field("connection_mode", &self.connection_mode)
            .field("session_key", &self.session_key.as_deref().map(Redacted))
            .field(
                "connection_secret",
                &self.connection_secret.as_deref().map(Redacted),
            )
            .finish()
    }
}

// ── Client ────────────────────────────────────────────────────────────────────

/// The `ConnectionConfig::service_id` of a monitor connection.
const MON_SERVICE_ID: u32 = 0;

/// Client-side authentication phase.
///
/// Drives the full AUTH_REQUEST ↔ AUTH_DONE exchange, including method
/// re-negotiation (`AUTH_BAD_METHOD`) and multi-round CephX challenge
/// (`AUTH_REPLY_MORE`).
#[derive(Clone)]
pub struct AuthClient {
    method: AuthMethod,
    /// The connection modes configured for this peer, in preference order.
    modes: Vec<ConnectionMode>,
    supported_methods: Vec<AuthMethod>,
    tried_methods: Vec<AuthMethod>,
    auth_provider: Option<Box<dyn AuthProvider>>,
    service_id: u32,
    entity_name: crate::EntityName,
    global_id: u64,
}

impl AuthClient {
    pub fn new(
        modes: Vec<ConnectionMode>,
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
            modes,
            supported_methods,
            tried_methods: Vec::new(),
            auth_provider,
            service_id,
            entity_name,
            global_id,
        }
    }

    /// The modes an `AUTH_REQUEST` with the current method offers: the
    /// configured ones, in order, but only CRC under AUTH_NONE, which has
    /// no connection secret to key SECURE with. C++
    /// `AuthRegistry::get_supported_modes`
    /// (v19.2.6:src/auth/AuthRegistry.cc:289-307).
    fn offered_modes(&self) -> Vec<ConnectionMode> {
        if self.method == AuthMethod::None {
            self.modes
                .iter()
                .copied()
                .filter(|&m| m == ConnectionMode::Crc)
                .collect()
        } else {
            self.modes.clone()
        }
    }

    fn build_auth_request(&mut self) -> Result<Frame> {
        let offered = self.offered_modes();
        // With no mode to offer, a monitor connection fails before asking,
        // as C++ `MonConnection::get_auth_request` does through
        // `handle_auth_failure` (v19.2.6:src/mon/MonClient.cc:1814-1820).
        // Any other peer is sent the empty list, as
        // `MonClient::get_auth_request` does (1503-1507); the server then
        // finds no mode to pick.
        if offered.is_empty() && self.service_id == MON_SERVICE_ID {
            return Err(Error::Auth(format!(
                "no connection mode to offer the monitor with auth method {:?}: \
                 configured modes {:?}",
                self.method, self.modes
            )));
        }
        let modes: Vec<u32> = offered.iter().map(|m| (*m).into()).collect();
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

    fn handle_auth_bad_method(self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        if frame.segments.is_empty() {
            return Err(Error::protocol_error("AUTH_BAD_METHOD missing payload"));
        }
        let mut p = frame.segments[0].as_ref();
        let server_rejected: u32 = u32::decode(&mut p, 0)?;
        let server_result: i32 = i32::decode(&mut p, 0)?;
        let allowed_methods: Vec<u32> = Vec::decode(&mut p, 0)?;
        let allowed_modes: Vec<u32> = Vec::decode(&mut p, 0)?;

        // `self.method` is what we sent; decode the server's echoed
        // rejection independently so the ordered search anchors at the
        // server's position even if the two ever differ. Unknown values
        // (future auth methods) fall back to a full-list search.
        let rejected_method = AuthMethod::try_from(server_rejected).ok();

        tracing::info!(
            "AUTH_BAD_METHOD: server rejected method={} ({:?}) with \
             result={} ({}), allowed_methods={:?}, allowed_modes={:?}",
            server_rejected,
            rejected_method,
            server_result,
            os_error(server_result),
            allowed_methods,
            allowed_modes,
        );

        let mut tried = self.tried_methods.clone();
        if !tried.contains(&self.method) {
            tried.push(self.method);
        }
        if let Some(rm) = rejected_method
            && !tried.contains(&rm)
        {
            tried.push(rm);
        }

        let server_allowed: Vec<AuthMethod> = allowed_methods
            .iter()
            .filter_map(|&m| AuthMethod::try_from(m).ok())
            .collect();

        // Ordered search (Ceph `MonConnection::handle_auth_bad_method`):
        // anchor at the rejected method's position in our preference-
        // ordered supported list and walk forward for the first method
        // that is (a) in the server's allowed list and (b) not in
        // `tried`. Unknown `rejected_method` → anchor at 0. The search
        // is bounded by `supported_methods.len()`, so no retry counter.
        let anchor_pos =
            rejected_method.and_then(|rm| self.supported_methods.iter().position(|&m| m == rm));
        let search_start = anchor_pos.map_or(0, |pos| pos + 1);

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
                    os_error(server_result),
                ))
            })?;

        // The retry offers the configured modes for the new method, not the
        // server's allowed_modes: C++ sends the request again through
        // `get_auth_request` (v19.2.6:src/msg/async/ProtocolV2.cc:1837,
        // src/mon/MonClient.cc:1814).
        let mut next = Self {
            method: new_method,
            tried_methods: tried,
            ..self
        };
        tracing::info!(
            "Retrying auth with method={new_method:?}, modes={:?}",
            next.offered_modes()
        );
        let req = next.build_auth_request()?;
        Ok(Step::Next {
            state: next,
            send: Some(req),
        })
    }

    fn handle_auth_reply_more(mut self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        let provider = self
            .auth_provider
            .as_mut()
            .ok_or_else(|| Error::protocol_error("AUTH_REPLY_MORE but no CephX auth provider"))?;
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

    fn handle_auth_done(mut self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        let segment = frame
            .segments
            .first()
            .ok_or_else(|| Error::protocol_error("AUTH_DONE missing payload"))?;
        let mut p = segment.clone();
        let global_id = u64::decode(&mut p, 0)?;
        let con_mode = u32::decode(&mut p, 0)?;
        let auth_payload = Bytes::decode(&mut p, 0)?;

        tracing::info!("AUTH_DONE: global_id={global_id}, connection_mode={con_mode}");

        // C++ does not check this: its client keeps whatever mode AUTH_DONE
        // names (v19.2.6:src/msg/async/ProtocolV2.cc:1874-1918,
        // src/mon/MonClient.cc:1545-1600). rados-rs refuses a mode it did
        // not offer, so that offering only SECURE guarantees SECURE. It
        // cannot refuse a conforming server, whose `AuthRegistry::pick_mode`
        // picks from the offered list (src/auth/AuthRegistry.cc:309-325), and
        // under CephX an AUTH_DONE rewritten in transit already fails
        // AUTH_SIGNATURE; what it refuses is a server that ignores the list.
        let offered = self.offered_modes();
        if !offered.iter().any(|&m| u32::from(m) == con_mode) {
            return Err(Error::Protocol(format!(
                "server chose connection mode {con_mode}, which the client did not offer \
                 (offered {offered:?})"
            )));
        }

        let (session_key, connection_secret) = if self.method == AuthMethod::None {
            (None, None)
        } else {
            let provider = self
                .auth_provider
                .as_mut()
                .ok_or_else(|| Error::protocol_error("No auth provider"))?;
            provider.handle_auth_response(auth_payload, global_id, con_mode)?
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
}

impl Phase for AuthClient {
    type Output = AuthOutput;

    fn enter(&mut self) -> Result<Option<Frame>> {
        Ok(Some(self.build_auth_request()?))
    }

    fn step(self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        match frame.preamble.tag {
            Tag::AuthBadMethod => self.handle_auth_bad_method(frame),
            Tag::AuthReplyMore => self.handle_auth_reply_more(frame),
            Tag::AuthDone => self.handle_auth_done(frame),
            other => Err(Error::protocol_error(&format!(
                "Unexpected frame {other:?} in auth phase (client)"
            ))),
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Global ID handed out to an unauthenticated client when the server has no
/// auth handler configured. Only reached by the test-scaffolding
/// `ConnectionConfig::with_no_auth()` path, never by real daemons — the
/// specific value is not meaningful, just has to be non-zero so downstream
/// code that distinguishes "no gid yet" from "gid assigned" works.
const NO_AUTH_SERVER_GLOBAL_ID: u64 = 1001;

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
    /// `Some` once phase 0 has negotiated a mode from the client's list.
    connection_mode: Option<u32>,
    client_preferred_modes: Vec<u32>,
}

impl AuthServer {
    pub fn new(auth_handler: Option<crate::auth::CephXServerHandler>) -> Self {
        Self {
            auth_handler,
            entity_name: None,
            global_id: None,
            phase: 0,
            connection_mode: None,
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

    fn step(self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        match (self.phase, frame.preamble.tag) {
            (0, Tag::AuthRequest) => self.handle_auth_request(frame),
            (1, Tag::AuthRequestMore) => self.handle_auth_request_more(frame),
            (phase, tag) => Err(Error::protocol_error(&format!(
                "Unexpected frame {tag:?} in auth phase (server) at phase={phase}"
            ))),
        }
    }
}

impl AuthServer {
    fn handle_auth_request(mut self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        let auth_request = AuthRequestFrame::from_frame(&frame)?;
        self.client_preferred_modes = auth_request.preferred_modes.clone();
        // Without authentication there is no connection secret to key SECURE
        // mode from, so AUTH_NONE only allows CRC, as C++
        // `AuthRegistry::get_supported_modes` does.
        let connection_mode = if self.auth_handler.is_some() {
            Self::negotiate_mode(&auth_request.preferred_modes)
        } else {
            crate::msgr2::ConnectionMode::Crc.into()
        };
        self.connection_mode = Some(connection_mode);

        if let Some(ref mut handler) = self.auth_handler {
            let (entity_name, global_id, challenge) =
                handler.handle_initial_request(&auth_request.auth_payload)?;
            tracing::debug!("Server auth phase 0: entity={entity_name}, gid={global_id}");
            self.entity_name = Some(entity_name);
            self.global_id = Some(global_id);
            self.phase = 1;

            let more = AuthRequestMoreFrame::new(challenge);
            let reply = create_frame_from_trait(&more, Tag::AuthReplyMore)?;
            return Ok(Step::Next {
                state: self,
                send: Some(reply),
            });
        }

        // No auth handler — accept without authentication
        tracing::warn!("Server: no auth handler, skipping authentication");
        let global_id = NO_AUTH_SERVER_GLOBAL_ID;
        let done = AuthDoneFrame::new(global_id, connection_mode, Bytes::new());
        let done_frame = create_frame_from_trait(&done, Tag::AuthDone)?;
        Ok(Step::Done(
            AuthOutput {
                global_id,
                connection_mode,
                session_key: None,
                connection_secret: None,
            },
            Some(done_frame),
        ))
    }

    fn handle_auth_request_more(mut self, frame: Frame) -> Result<Step<Self, AuthOutput>> {
        // `AuthRequestMoreFrame` carries only `auth_payload: Bytes`; there is
        // no public helper on the frame type, so decode the single field
        // inline rather than hand-rolling one. Do NOT reuse
        // `AuthRequestFrame::from_frame` here — that expects the
        // `method + preferred_modes + auth_payload` layout, which is a
        // different wire format and would silently misparse.
        let mut p = frame
            .segments
            .first()
            .ok_or_else(|| Error::protocol_error("AUTH_REQUEST_MORE missing payload"))?
            .clone();
        let auth_payload = Bytes::decode(&mut p, 0)?;

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
        let connection_mode = self
            .connection_mode
            .ok_or_else(|| Error::protocol_error("Missing connection_mode in phase 1"))?;

        let (session_key, connection_secret, auth_payload) = handler.handle_authenticate(
            entity_name,
            global_id,
            &auth_payload,
            crate::auth::protocol::connection_secret_len(connection_mode),
        )?;
        tracing::info!("Server: {entity_name} authenticated successfully");

        let done_payload = handler.build_auth_done_response(
            global_id,
            connection_mode as u8,
            &session_key,
            &connection_secret,
            auth_payload,
        )?;

        let done = AuthDoneFrame::new(global_id, connection_mode, done_payload);
        let done_frame = create_frame_from_trait(&done, Tag::AuthDone)?;

        Ok(Step::Done(
            AuthOutput {
                global_id,
                connection_mode,
                session_key: Some(session_key.secret.clone()),
                connection_secret: (!connection_secret.is_empty()).then_some(connection_secret),
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

    #[test]
    fn auth_output_debug_redacts_the_secrets() {
        let out = format!(
            "{:?}",
            AuthOutput {
                global_id: 9,
                connection_mode: 2,
                session_key: Some(Bytes::from_static(&[0xab; 16])),
                connection_secret: Some(Bytes::from_static(&[0xcd; 64])),
            }
        );
        assert!(out.contains("global_id: 9"), "{out}");
        assert!(out.contains("<16 bytes redacted>"), "{out}");
        assert!(out.contains("<64 bytes redacted>"), "{out}");
        let lower = out.to_lowercase();
        assert!(!lower.contains("xab") && !lower.contains("xcd"), "{out}");
        assert!(!out.contains("171") && !out.contains("205"), "{out}");
    }

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
            modes: vec![ConnectionMode::Crc],
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

    /// Stands in for a CephX provider: a fixed payload out, a session key
    /// and SECURE-sized connection secret back.
    #[derive(Debug, Clone)]
    struct StubProvider;

    impl AuthProvider for StubProvider {
        fn build_auth_payload(
            &mut self,
            _global_id: u64,
            _service_id: u32,
        ) -> crate::auth::error::Result<Bytes> {
            Ok(Bytes::from_static(b"stub"))
        }

        fn handle_auth_response(
            &mut self,
            _payload: Bytes,
            _global_id: u64,
            _con_mode: u32,
        ) -> crate::auth::error::Result<(Option<Bytes>, Option<Bytes>)> {
            Ok((
                Some(Bytes::from_static(&[1; 16])),
                Some(Bytes::from_static(&[2; 64])),
            ))
        }

        fn has_valid_ticket(&self, _service_id: u32) -> bool {
            true
        }

        fn clone_box(&self) -> Box<dyn AuthProvider> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// A client for `service_id` that allows `modes` and starts with the
    /// first of `supported`.
    fn moded_client(
        modes: Vec<ConnectionMode>,
        supported: Vec<AuthMethod>,
        service_id: u32,
    ) -> AuthClient {
        let provider: Option<Box<dyn AuthProvider>> = supported
            .contains(&AuthMethod::Cephx)
            .then(|| Box::new(StubProvider) as Box<dyn AuthProvider>);
        let global_id = if service_id == MON_SERVICE_ID { 0 } else { 42 };
        AuthClient::new(
            modes,
            supported,
            provider,
            service_id,
            crate::EntityName::client("admin"),
            global_id,
        )
    }

    /// The modes an `AUTH_REQUEST` frame offers.
    fn offered(frame: &Frame) -> Vec<u32> {
        assert_eq!(frame.preamble.tag, Tag::AuthRequest);
        AuthRequestFrame::from_frame(frame).unwrap().preferred_modes
    }

    const CRC: u32 = ConnectionMode::Crc as u32;
    const SECURE: u32 = ConnectionMode::Secure as u32;
    const OSD: u32 = crate::EntityType::OSD.bits();

    #[test]
    fn auth_request_offers_the_configured_modes_in_order() {
        for modes in [
            vec![ConnectionMode::Secure, ConnectionMode::Crc],
            vec![ConnectionMode::Crc, ConnectionMode::Secure],
            vec![ConnectionMode::Secure],
        ] {
            for service_id in [MON_SERVICE_ID, OSD] {
                let mut c = moded_client(modes.clone(), vec![AuthMethod::Cephx], service_id);
                let want: Vec<u32> = modes.iter().map(|&m| m.into()).collect();
                assert_eq!(offered(&c.enter().unwrap().unwrap()), want);
            }
        }
    }

    #[test]
    fn auth_none_offers_crc_only() {
        let mut c = moded_client(
            vec![ConnectionMode::Secure, ConnectionMode::Crc],
            vec![AuthMethod::None],
            MON_SERVICE_ID,
        );
        assert_eq!(offered(&c.enter().unwrap().unwrap()), vec![CRC]);
    }

    #[test]
    fn mon_request_with_no_mode_to_offer_fails() {
        // SECURE alone under AUTH_NONE leaves nothing to offer.
        let mut c = moded_client(
            vec![ConnectionMode::Secure],
            vec![AuthMethod::None],
            MON_SERVICE_ID,
        );
        let err = c.enter().expect_err("no mode to offer a monitor");
        assert!(matches!(err, Error::Auth(_)), "{err}");
        assert!(err.to_string().contains("no connection mode"), "{err}");

        let mut c = moded_client(vec![], vec![AuthMethod::Cephx], MON_SERVICE_ID);
        assert!(c.enter().is_err());
    }

    #[test]
    fn service_request_with_no_mode_to_offer_sends_an_empty_list() {
        let mut c = moded_client(vec![ConnectionMode::Secure], vec![AuthMethod::None], OSD);
        assert_eq!(offered(&c.enter().unwrap().unwrap()), Vec::<u32>::new());

        let mut c = moded_client(vec![], vec![AuthMethod::Cephx], OSD);
        assert_eq!(offered(&c.enter().unwrap().unwrap()), Vec::<u32>::new());
    }

    #[test]
    fn bad_method_retry_offers_the_configured_modes_for_the_new_method() {
        // The server rejects AUTH_NONE and allows only CRC. The retry with
        // CephX still offers SECURE alone, as configured; it neither
        // narrows to the server's modes nor falls back to them.
        let mut c = moded_client(
            vec![ConnectionMode::Secure],
            vec![AuthMethod::None, AuthMethod::Cephx],
            MON_SERVICE_ID,
        );
        c.method = AuthMethod::None;
        let payload = bad_method_payload(
            AuthMethod::None.into(),
            -95,
            &[AuthMethod::Cephx.into()],
            &[CRC],
        );
        match c.step(bad_method_frame(payload)).unwrap() {
            Step::Next {
                state,
                send: Some(frame),
            } => {
                assert_eq!(state.method, AuthMethod::Cephx);
                assert_eq!(offered(&frame), vec![SECURE]);
            }
            _ => panic!("expected a retry"),
        }

        // A retry to AUTH_NONE offers CRC only, whatever the server allows.
        let c = moded_client(
            vec![ConnectionMode::Secure, ConnectionMode::Crc],
            vec![AuthMethod::Cephx, AuthMethod::None],
            MON_SERVICE_ID,
        );
        let payload = bad_method_payload(
            AuthMethod::Cephx.into(),
            -13,
            &[AuthMethod::None.into()],
            &[SECURE, CRC],
        );
        match c.step(bad_method_frame(payload)).unwrap() {
            Step::Next {
                state,
                send: Some(frame),
            } => {
                assert_eq!(state.method, AuthMethod::None);
                assert_eq!(offered(&frame), vec![CRC]);
            }
            _ => panic!("expected a retry"),
        }
    }

    /// An AUTH_DONE frame naming `con_mode`.
    fn auth_done_frame(con_mode: u32) -> Frame {
        let done = AuthDoneFrame::new(4242, con_mode, Bytes::new());
        create_frame_from_trait(&done, Tag::AuthDone).unwrap()
    }

    /// Run `c` through AUTH_REQUEST and an AUTH_DONE naming `con_mode`.
    fn finish_with(mut c: AuthClient, con_mode: u32) -> Result<AuthOutput> {
        c.enter()?;
        match c.step(auth_done_frame(con_mode))? {
            Step::Done(out, None) => Ok(out),
            _ => panic!("AUTH_DONE must finish the phase"),
        }
    }

    #[test]
    fn auth_done_with_a_mode_not_offered_is_refused() {
        let secure_only =
            || moded_client(vec![ConnectionMode::Secure], vec![AuthMethod::Cephx], OSD);
        for mode in [CRC, ConnectionMode::Unknown as u32, 7] {
            let err = finish_with(secure_only(), mode)
                .err()
                .unwrap_or_else(|| panic!("mode {mode} was not offered"));
            assert!(err.to_string().contains("did not offer"), "{err}");
        }
        let out = finish_with(secure_only(), SECURE).unwrap();
        assert_eq!(out.connection_mode, SECURE);
        assert!(out.connection_secret.is_some());

        // AUTH_NONE offers CRC alone, so SECURE is refused under it.
        let none = || {
            moded_client(
                vec![ConnectionMode::Secure, ConnectionMode::Crc],
                vec![AuthMethod::None],
                MON_SERVICE_ID,
            )
        };
        assert!(finish_with(none(), SECURE).is_err());
        assert_eq!(finish_with(none(), CRC).unwrap().connection_mode, CRC);

        // A request that offered no mode accepts none, UNKNOWN included.
        for mode in [ConnectionMode::Unknown as u32, CRC, SECURE] {
            let c = moded_client(vec![ConnectionMode::Secure], vec![AuthMethod::None], OSD);
            assert!(finish_with(c, mode).is_err(), "mode {mode}");
        }
    }
}
