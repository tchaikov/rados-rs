//! Session establishment phase: CLIENT_IDENT / SERVER_IDENT exchange.
//!
//! **Client** sends `CLIENT_IDENT` (new session) or `SESSION_RECONNECT`
//! (resuming), then handles the server's response:
//! - `SERVER_IDENT`           → new session complete
//! - `SESSION_RECONNECT_OK`   → reconnect complete
//! - `SESSION_RETRY`          → increment `connect_seq` and retry
//! - `SESSION_RETRY_GLOBAL`   → update `global_seq` and retry
//! - `SESSION_RESET`          → clear cookies and send `CLIENT_IDENT`
//! - `WAIT`                   → keep waiting
//!
//! **Server** waits for `CLIENT_IDENT` (or `SESSION_RECONNECT`), then replies
//! with `SERVER_IDENT` (or `SESSION_RECONNECT_OK`).

use crate::Denc;
use crate::msgr2::{
    error::{Msgr2Error as Error, Result},
    frames::create_frame_from_trait,
    frames::{
        ClientIdentFrame, Frame, IdentMissingFeaturesFrame, ServerIdentFrame,
        SessionReconnectFrame, SessionReconnectOkFrame, Tag,
    },
    phase::{Phase, Step},
};

/// Wire flag set in `ClientIdentFrame.flags` for lossy connections.
/// Mirrors `CEPH_MSG_CONNECT_LOSSY` in `src/include/msgr.h`.
const CEPH_MSG_CONNECT_LOSSY: u64 = 1;

const ALL_LEGACY_CEPH_FEATURE_BITS: u64 = (1u64 << 62) - 1;

fn session_supported_features() -> u64 {
    use crate::denc::features::CephFeatures;

    // The session phase exchanges Ceph feature bits, not banner/msgr2 feature bits.
    // Keep the existing permissive advertisement for now, but centralize it so
    // client and server paths stay in sync.
    (CephFeatures::MSG_ADDR2 | CephFeatures::SERVER_NAUTILUS | CephFeatures::SERVER_OCTOPUS).bits()
        | ALL_LEGACY_CEPH_FEATURE_BITS
}

fn session_required_features() -> u64 {
    crate::denc::features::CephFeatures::MSG_ADDR2.bits()
}

// ── Client output ─────────────────────────────────────────────────────────────

/// Produced when the client-side session phase completes.
#[derive(Debug)]
pub struct SessionClientOutput {
    /// Ceph session features negotiated with the server.
    pub negotiated_features: u64,
    /// Server cookie assigned in `SERVER_IDENT` (needed for reconnection).
    pub server_cookie: u64,
    /// Server-declared lossy flag from `SERVER_IDENT.flags & CEPH_MSG_CONNECT_LOSSY`.
    /// The server's flag is authoritative (Ceph `ProtocolV2::handle_server_ident`,
    /// Linux `process_server_ident`) — the client declares intent in CLIENT_IDENT,
    /// but the server's echo is the policy the connection actually runs with.
    /// `None` when the session was resumed via `SESSION_RECONNECT_OK` (no
    /// SERVER_IDENT arrived, so the existing policy is preserved).
    pub server_is_lossy: Option<bool>,
    /// Set when the session was resumed: the last message sequence the server
    /// acknowledged.  The caller must replay unacknowledged messages.
    pub reconnect_msg_seq: Option<u64>,
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Client-side session establishment phase.
///
/// Constructs and sends the appropriate session frame, then drives the
/// handshake to completion, handling retries as needed.
pub struct SessionClient {
    global_id: u64,
    server_addr: crate::EntityAddr,
    client_addr: crate::EntityAddr,
    client_cookie: u64,
    server_cookie: u64,
    global_seq: u64,
    connect_seq: u64,
    in_seq: u64,
    is_lossy: bool,
}

impl SessionClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        global_id: u64,
        server_addr: crate::EntityAddr,
        client_addr: crate::EntityAddr,
        client_cookie: u64,
        server_cookie: u64,
        global_seq: u64,
        connect_seq: u64,
        in_seq: u64,
        is_lossy: bool,
    ) -> Self {
        Self {
            global_id,
            server_addr,
            client_addr,
            client_cookie,
            server_cookie,
            global_seq,
            connect_seq,
            in_seq,
            is_lossy,
        }
    }

    fn build_client_ident(&self) -> Result<Frame> {
        let addrs = crate::EntityAddrvec::with_addr(self.client_addr);
        let flags = if self.is_lossy {
            CEPH_MSG_CONNECT_LOSSY
        } else {
            0
        };
        let ident = ClientIdentFrame::new(
            addrs,
            self.server_addr,
            self.global_id as i64,
            self.global_seq,
            session_supported_features(),
            session_required_features(),
            flags,
            self.client_cookie,
        );
        tracing::debug!(
            "Sending CLIENT_IDENT: gid={}, cookie={}, global_seq={}",
            self.global_id,
            self.client_cookie,
            self.global_seq
        );
        Ok(create_frame_from_trait(&ident, Tag::ClientIdent)?)
    }

    fn build_session_reconnect(&self) -> Result<Frame> {
        let addrs = crate::EntityAddrvec::with_addr(self.client_addr);
        let reconnect = SessionReconnectFrame::new(
            addrs,
            self.client_cookie,
            self.server_cookie,
            self.global_seq,
            self.connect_seq,
            self.in_seq,
        );
        tracing::debug!(
            "Sending SESSION_RECONNECT: client_cookie={}, server_cookie={}, connect_seq={}",
            self.client_cookie,
            self.server_cookie,
            self.connect_seq
        );
        Ok(create_frame_from_trait(&reconnect, Tag::SessionReconnect)?)
    }

    fn first_segment<'a>(frame: &'a Frame, what: &'static str) -> Result<&'a bytes::Bytes> {
        frame
            .segments
            .first()
            .ok_or_else(|| Error::protocol_error(&format!("{what} missing payload")))
    }

    fn handle_server_ident(self, frame: Frame) -> Result<Step<Self, SessionClientOutput>> {
        let mut p = Self::first_segment(&frame, "SERVER_IDENT")?.clone();
        let server_addrs = crate::EntityAddrvec::decode(&mut p, 0)?;
        let _gid = u64::decode(&mut p, 0)?;
        let _global_seq = u64::decode(&mut p, 0)?;
        let features_supported = u64::decode(&mut p, 0)?;
        let features_required = u64::decode(&mut p, 0)?;
        let flags = u64::decode(&mut p, 0)?;
        let server_cookie = u64::decode(&mut p, 0)?;

        // Fault on misrouted daemons (Ceph `ProtocolV2::handle_server_ident`,
        // Linux `process_server_ident`).
        if !server_addrs.contains(&self.server_addr) {
            return Err(Error::Protocol(format!(
                "SERVER_IDENT peer addr mismatch: dialed {:?}, server advertised {:?}",
                self.server_addr, server_addrs
            )));
        }

        // Server's lossy flag is authoritative. Enforce the protocol
        // invariant `lossy <=> cookie == 0` and warn on a mismatch with
        // our declared policy.
        let server_is_lossy = (flags & CEPH_MSG_CONNECT_LOSSY) != 0;
        if server_is_lossy != self.is_lossy {
            tracing::warn!(
                "SERVER_IDENT lossy flag mismatch: client declared is_lossy={}, \
                 server responded is_lossy={}; adopting server's policy",
                self.is_lossy,
                server_is_lossy,
            );
        }
        if server_is_lossy && server_cookie != 0 {
            return Err(Error::Protocol(format!(
                "SERVER_IDENT protocol violation: lossy mode with non-zero \
                 server_cookie {server_cookie:#x}"
            )));
        }
        if !server_is_lossy && server_cookie == 0 {
            return Err(Error::Protocol(
                "SERVER_IDENT protocol violation: lossless mode with zero server_cookie".into(),
            ));
        }

        let our_features = session_supported_features();
        let missing = features_required & !our_features;
        if missing != 0 {
            return Err(Error::Protocol(format!(
                "Server requires unsupported features: 0x{missing:x}"
            )));
        }

        let negotiated_features = our_features & features_supported;
        tracing::info!(
            "Session established: negotiated_features=0x{negotiated_features:x}, \
             server_cookie={server_cookie}, server_is_lossy={server_is_lossy}"
        );
        Ok(Step::Done(
            SessionClientOutput {
                negotiated_features,
                server_cookie,
                server_is_lossy: Some(server_is_lossy),
                reconnect_msg_seq: None,
            },
            None,
        ))
    }

    fn handle_session_reconnect_ok(self, frame: Frame) -> Result<Step<Self, SessionClientOutput>> {
        let mut p = Self::first_segment(&frame, "SESSION_RECONNECT_OK")?.clone();
        let msg_seq = u64::decode(&mut p, 0)?;
        tracing::info!("Reconnection successful, server acked up to msg_seq={msg_seq}");
        Ok(Step::Done(
            SessionClientOutput {
                negotiated_features: 0,
                server_cookie: self.server_cookie,
                // Resumption skips SERVER_IDENT; preserve existing policy.
                server_is_lossy: None,
                reconnect_msg_seq: Some(msg_seq),
            },
            None,
        ))
    }

    fn handle_session_retry(mut self, frame: Frame) -> Result<Step<Self, SessionClientOutput>> {
        let mut p = Self::first_segment(&frame, "SESSION_RETRY")?.clone();
        let server_connect_seq = u64::decode(&mut p, 0)?;
        tracing::warn!("SESSION_RETRY: server_connect_seq={server_connect_seq}, incrementing ours");
        self.connect_seq += 1;
        let retry_frame = self.build_session_reconnect()?;
        Ok(Step::Next {
            state: self,
            send: Some(retry_frame),
        })
    }

    fn handle_session_retry_global(
        mut self,
        frame: Frame,
    ) -> Result<Step<Self, SessionClientOutput>> {
        let mut p = Self::first_segment(&frame, "SESSION_RETRY_GLOBAL")?.clone();
        let server_global_seq = u64::decode(&mut p, 0)?;
        tracing::warn!("SESSION_RETRY_GLOBAL: updating global_seq to {server_global_seq}");
        self.global_seq = server_global_seq.max(self.global_seq + 1);
        let retry_frame = self.build_session_reconnect()?;
        Ok(Step::Next {
            state: self,
            send: Some(retry_frame),
        })
    }

    fn handle_session_reset(mut self, frame: Frame) -> Result<Step<Self, SessionClientOutput>> {
        let mut p = Self::first_segment(&frame, "SESSION_RESET")?.clone();
        let full = bool::decode(&mut p, 0)?;
        tracing::warn!("SESSION_RESET (full={full}): sending CLIENT_IDENT");
        self.server_cookie = 0;
        if full {
            self.global_seq = 0;
            self.connect_seq = 0;
            self.in_seq = 0;
        }
        let ident_frame = self.build_client_ident()?;
        Ok(Step::Next {
            state: self,
            send: Some(ident_frame),
        })
    }

    fn handle_ident_missing_features(frame: Frame) -> Result<Step<Self, SessionClientOutput>> {
        let mut p = Self::first_segment(&frame, "IDENT_MISSING_FEATURES")?.clone();
        let missing_features = u64::decode(&mut p, 0)?;
        Err(Error::Protocol(format!(
            "Server reported missing required features: 0x{missing_features:x}"
        )))
    }
}

impl Phase for SessionClient {
    type Output = SessionClientOutput;

    fn enter(&mut self) -> Result<Option<Frame>> {
        let frame = if self.server_cookie != 0 {
            self.build_session_reconnect()?
        } else {
            self.build_client_ident()?
        };
        Ok(Some(frame))
    }

    fn step(self, frame: Frame) -> Result<Step<Self, SessionClientOutput>> {
        match frame.preamble.tag {
            Tag::ServerIdent => self.handle_server_ident(frame),
            Tag::SessionReconnectOk => self.handle_session_reconnect_ok(frame),
            Tag::SessionRetry => self.handle_session_retry(frame),
            Tag::SessionRetryGlobal => self.handle_session_retry_global(frame),
            Tag::SessionReset => self.handle_session_reset(frame),
            Tag::Wait => {
                tracing::debug!("Server sent WAIT, continuing to wait");
                Ok(Step::Next {
                    state: self,
                    send: None,
                })
            }
            Tag::IdentMissingFeatures => Self::handle_ident_missing_features(frame),
            other => Err(Error::protocol_error(&format!(
                "Unexpected frame {other:?} in session phase (client)"
            ))),
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Server-side session establishment phase.
///
/// Waits for `CLIENT_IDENT`, then sends `SERVER_IDENT`.
///
/// This implementation is test scaffolding and only covers the subset of
/// the Ceph C++ `handle_client_ident` path that our tests exercise:
///
///   - peer-addr validation (the client said it's trying to reach us)
///   - lossy-flag mirroring (match whatever the client declared)
///   - feature compatibility
///
/// It deliberately does not track per-connection peer identity (`gid`,
/// `global_seq`, `client_cookie`) because no consumer of the server
/// role in this crate uses those. A production server would need them
/// for dispatch routing and reconnection handling.
pub struct SessionServer {
    server_cookie: u64,
    server_addr: crate::EntityAddr,
}

impl SessionServer {
    pub fn new(server_cookie: u64, server_addr: crate::EntityAddr) -> Self {
        Self {
            server_cookie,
            server_addr,
        }
    }
}

impl SessionServer {
    fn handle_client_ident(self, frame: Frame) -> Result<Step<Self, ()>> {
        let mut p = frame
            .segments
            .first()
            .ok_or_else(|| Error::protocol_error("CLIENT_IDENT missing payload"))?
            .clone();
        let _addrs = crate::EntityAddrvec::decode(&mut p, 0)?;
        let client_target_addr = crate::EntityAddr::decode(&mut p, 0)?;
        let _gid = i64::decode(&mut p, 0)?;
        let _global_seq = u64::decode(&mut p, 0)?;
        let client_supported_features = u64::decode(&mut p, 0)?;
        let _client_required_features = u64::decode(&mut p, 0)?;
        let client_flags = u64::decode(&mut p, 0)?;
        let _cookie = u64::decode(&mut p, 0)?;

        // Fault misrouted clients (Ceph `ProtocolV2::handle_client_ident`).
        if client_target_addr != self.server_addr {
            return Err(Error::Protocol(format!(
                "CLIENT_IDENT target addr mismatch: we are {:?}, client is \
                 trying to reach {:?}",
                self.server_addr, client_target_addr
            )));
        }

        let missing_features = session_required_features() & !client_supported_features;
        if missing_features != 0 {
            let ident_missing = IdentMissingFeaturesFrame::new(missing_features);
            let response = create_frame_from_trait(&ident_missing, Tag::IdentMissingFeatures)?;
            return Ok(Step::Abort {
                error: Error::Protocol(format!(
                    "Client missing required features: 0x{missing_features:x}"
                )),
                send: Some(response),
            });
        }

        // Mirror the client's lossy flag so `lossy <=> cookie == 0`
        // holds on the wire (Ceph `ProtocolV2::handle_client_ident`).
        let client_is_lossy = (client_flags & CEPH_MSG_CONNECT_LOSSY) != 0;
        let (response_flags, response_cookie) = if client_is_lossy {
            (CEPH_MSG_CONNECT_LOSSY, 0)
        } else {
            (0, self.server_cookie)
        };

        let addrs = crate::EntityAddrvec::with_addr(self.server_addr);
        let server_ident = ServerIdentFrame::new(
            addrs,
            0,
            0,
            session_supported_features(),
            session_required_features(),
            response_flags,
            response_cookie,
        );
        let response = create_frame_from_trait(&server_ident, Tag::ServerIdent)?;
        tracing::debug!(
            "Server: sending SERVER_IDENT (lossy={}, cookie={:#x})",
            client_is_lossy,
            response_cookie
        );
        Ok(Step::Done((), Some(response)))
    }

    fn handle_session_reconnect(self, _frame: Frame) -> Result<Step<Self, ()>> {
        let reconnect_ok = SessionReconnectOkFrame::new(0);
        let response = create_frame_from_trait(&reconnect_ok, Tag::SessionReconnectOk)?;
        tracing::debug!("Server: sending SESSION_RECONNECT_OK");
        Ok(Step::Done((), Some(response)))
    }
}

impl Phase for SessionServer {
    type Output = ();

    fn step(self, frame: Frame) -> Result<Step<Self, ()>> {
        match frame.preamble.tag {
            Tag::ClientIdent => self.handle_client_ident(frame),
            Tag::SessionReconnect => self.handle_session_reconnect(frame),
            other => Err(Error::protocol_error(&format!(
                "Unexpected frame {other:?} in session phase (server)"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `SessionClient` with a non-default server addr so the
    /// peer-addr validation has something to compare against. Cookies,
    /// seqs, and global_id all default to zero; tests that care about
    /// specific values patch them via the `SessionClient::new`
    /// parameters directly.
    fn client_with_server_addr(server_addr: crate::EntityAddr, is_lossy: bool) -> SessionClient {
        SessionClient::new(
            1,
            server_addr,
            crate::EntityAddr::default(),
            10,
            0,
            0,
            0,
            0,
            is_lossy,
        )
    }

    fn msgr2_addr(port: u16) -> crate::EntityAddr {
        let socket_addr = std::net::SocketAddr::from(([127u8, 0, 0, 1], port));
        crate::EntityAddr::from_socket_addr(crate::EntityAddrType::Msgr2, socket_addr)
    }

    #[test]
    fn client_rejects_server_ident_with_mismatched_peer_addr() {
        // Session was opened against 127.0.0.1:6800 but the server
        // identifies as 127.0.0.1:7000. Ceph `ProtocolV2::handle_server_ident`
        // and Linux `process_server_ident` both reject this case.
        let dialed = msgr2_addr(6800);
        let session = client_with_server_addr(dialed, false);

        let wrong_addrs = crate::EntityAddrvec::with_addr(msgr2_addr(7000));
        let server_ident = ServerIdentFrame::new(
            wrong_addrs,
            0,
            0,
            session_supported_features(),
            0,
            0,
            42, // non-zero cookie so we hit the addr check, not the lossy check
        );
        let frame = create_frame_from_trait(&server_ident, Tag::ServerIdent).unwrap();

        let err = match session.step(frame) {
            Ok(_) => panic!("expected peer-addr validation to fail"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("peer addr mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn client_accepts_server_ident_when_addrvec_contains_dialed_addr() {
        // Multi-address server: we dialed one of them. Ceph
        // `entity_addrvec_t::contains` accepts this case.
        let dialed = msgr2_addr(6800);
        let session = client_with_server_addr(dialed, false);

        let mut addrs = crate::EntityAddrvec::new();
        addrs.addrs.push(msgr2_addr(7000));
        addrs.addrs.push(dialed);
        addrs.addrs.push(msgr2_addr(7001));

        let server_ident =
            ServerIdentFrame::new(addrs, 0, 0, session_supported_features(), 0, 0, 42);
        let frame = create_frame_from_trait(&server_ident, Tag::ServerIdent).unwrap();

        match session.step(frame).unwrap() {
            Step::Done(output, _) => {
                assert_eq!(output.server_cookie, 42);
                assert_eq!(output.server_is_lossy, Some(false));
            }
            _ => panic!("expected Done"),
        }
    }

    #[test]
    fn client_rejects_lossy_invariant_violation_with_nonzero_cookie() {
        // Protocol invariant: lossy mode MUST have server_cookie == 0.
        // Linux `process_server_ident` WARN_ONs on lossy flag set with
        // non-zero cookie.
        let dialed = msgr2_addr(6800);
        let session = client_with_server_addr(dialed, true);

        let addrs = crate::EntityAddrvec::with_addr(dialed);
        let server_ident = ServerIdentFrame::new(
            addrs,
            0,
            0,
            session_supported_features(),
            0,
            CEPH_MSG_CONNECT_LOSSY,
            99, // illegal: lossy + non-zero cookie
        );
        let frame = create_frame_from_trait(&server_ident, Tag::ServerIdent).unwrap();

        let err = match session.step(frame) {
            Ok(_) => panic!("expected lossy invariant violation"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("lossy mode with non-zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn client_rejects_lossless_invariant_violation_with_zero_cookie() {
        // Protocol invariant: lossless mode MUST have server_cookie != 0.
        // Linux `process_server_ident` WARN_ONs on lossless + zero cookie.
        let dialed = msgr2_addr(6800);
        let session = client_with_server_addr(dialed, false);

        let addrs = crate::EntityAddrvec::with_addr(dialed);
        let server_ident = ServerIdentFrame::new(
            addrs,
            0,
            0,
            session_supported_features(),
            0,
            0,
            0, // illegal: lossless + zero cookie
        );
        let frame = create_frame_from_trait(&server_ident, Tag::ServerIdent).unwrap();

        let err = match session.step(frame) {
            Ok(_) => panic!("expected lossless invariant violation"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("lossless mode with zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn client_adopts_server_declared_lossy_flag_when_client_said_otherwise() {
        // Client declared is_lossy=false but the server unilaterally
        // decided this connection is lossy. Ceph `ProtocolV2::handle_server_ident`
        // adopts the server's policy by overwriting connection.policy.lossy;
        // we emit a warning log but still return Success with the
        // server-declared flag plumbed into output.server_is_lossy.
        let dialed = msgr2_addr(6800);
        let session = client_with_server_addr(dialed, false);

        let addrs = crate::EntityAddrvec::with_addr(dialed);
        let server_ident = ServerIdentFrame::new(
            addrs,
            0,
            0,
            session_supported_features(),
            0,
            CEPH_MSG_CONNECT_LOSSY,
            0, // lossy invariant: cookie == 0
        );
        let frame = create_frame_from_trait(&server_ident, Tag::ServerIdent).unwrap();

        match session.step(frame).unwrap() {
            Step::Done(output, _) => {
                assert_eq!(
                    output.server_is_lossy,
                    Some(true),
                    "server's lossy flag should be plumbed into the output"
                );
                assert_eq!(
                    output.server_cookie, 0,
                    "lossy mode must carry server_cookie == 0"
                );
            }
            _ => panic!("expected Done with server-declared lossy policy"),
        }
    }

    #[test]
    fn client_rejects_server_ident_with_unsupported_required_features() {
        let session = SessionClient::new(
            1,
            crate::EntityAddr::default(),
            crate::EntityAddr::default(),
            10,
            0,
            0,
            0,
            0,
            false,
        );
        let addrs = crate::EntityAddrvec::with_addr(crate::EntityAddr::default());
        let server_ident =
            ServerIdentFrame::new(addrs, 0, 0, session_supported_features(), 1u64 << 62, 0, 42);
        let frame = create_frame_from_trait(&server_ident, Tag::ServerIdent).unwrap();

        let err = match session.step(frame) {
            Ok(_) => panic!("expected server-ident feature validation to fail"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("unsupported features"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn client_reports_ident_missing_features_mask() {
        let session = SessionClient::new(
            1,
            crate::EntityAddr::default(),
            crate::EntityAddr::default(),
            10,
            0,
            0,
            0,
            0,
            false,
        );
        let ident_missing = IdentMissingFeaturesFrame::new(0x55);
        let frame = create_frame_from_trait(&ident_missing, Tag::IdentMissingFeatures).unwrap();

        let err = match session.step(frame) {
            Ok(_) => panic!("expected ident-missing-features to fail"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("0x55"), "unexpected error: {err}");
    }

    #[test]
    fn server_sends_ident_missing_features_for_incompatible_client() {
        // Use a real advertised addr for the server and have the client
        // target it, so the new target_addr validation passes and we
        // reach the features check that's actually under test.
        let server_addr = msgr2_addr(6800);
        let server = SessionServer::new(123, server_addr);
        let client_addrs = crate::EntityAddrvec::with_addr(msgr2_addr(6900));
        let client_ident = ClientIdentFrame::new(
            client_addrs,
            server_addr, // target_addr = who the client is trying to reach
            0,
            0,
            0, // client_supported_features = 0 → missing msgr2 required
            0,
            0,
            77,
        );
        let frame = create_frame_from_trait(&client_ident, Tag::ClientIdent).unwrap();

        match server.step(frame).unwrap() {
            Step::Abort { error, send } => {
                assert!(
                    error.to_string().contains("missing required features"),
                    "unexpected error: {error}"
                );
                let response = send.expect("missing response frame");
                assert_eq!(response.preamble.tag, Tag::IdentMissingFeatures);
            }
            _ => panic!("expected abort"),
        }
    }

    #[test]
    fn server_rejects_client_ident_with_wrong_target_addr() {
        // The client says it's trying to reach 6800, but we are 6900.
        // Ceph `ProtocolV2::handle_client_ident` rejects with
        // "peer is trying to reach X which is not us".
        let server_addr = msgr2_addr(6900);
        let server = SessionServer::new(123, server_addr);
        let client_addrs = crate::EntityAddrvec::with_addr(msgr2_addr(7000));
        let client_ident = ClientIdentFrame::new(
            client_addrs,
            msgr2_addr(6800), // target = not-us
            0,
            0,
            session_supported_features(),
            0,
            0,
            77,
        );
        let frame = create_frame_from_trait(&client_ident, Tag::ClientIdent).unwrap();

        let err = match server.step(frame) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected target_addr validation to fail"),
        };
        assert!(
            err.contains("target addr mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn server_mirrors_client_lossy_flag_in_server_ident() {
        // Protocol invariant: the server's SERVER_IDENT response must
        // carry the same lossy bit the client declared (and cookie
        // must obey `lossy <=> cookie == 0`). Our test server enforces
        // this mirroring so a lossy client can complete handshake with
        // a zero cookie and a lossless client with a non-zero cookie.
        let server_addr = msgr2_addr(6900);

        // Case A: lossy client → lossy response with cookie == 0
        let server_lossy = SessionServer::new(123, server_addr);
        let client_ident = ClientIdentFrame::new(
            crate::EntityAddrvec::with_addr(msgr2_addr(7000)),
            server_addr,
            0,
            0,
            session_supported_features(),
            0,
            CEPH_MSG_CONNECT_LOSSY,
            0, // lossy client cookie is always 0
        );
        let frame = create_frame_from_trait(&client_ident, Tag::ClientIdent).unwrap();
        match server_lossy.step(frame).unwrap() {
            Step::Done((), Some(response)) => {
                let segment = response.segments.first().expect("SERVER_IDENT payload");
                let mut p = segment.clone();
                // Skip addrs, gid, global_seq, supported, required
                let _ = crate::EntityAddrvec::decode(&mut p, 0).unwrap();
                let _ = u64::decode(&mut p, 0).unwrap();
                let _ = u64::decode(&mut p, 0).unwrap();
                let _ = u64::decode(&mut p, 0).unwrap();
                let _ = u64::decode(&mut p, 0).unwrap();
                let flags = u64::decode(&mut p, 0).unwrap();
                let cookie = u64::decode(&mut p, 0).unwrap();
                assert_eq!(
                    flags & CEPH_MSG_CONNECT_LOSSY,
                    CEPH_MSG_CONNECT_LOSSY,
                    "server should echo client's lossy flag"
                );
                assert_eq!(cookie, 0, "lossy response must carry zero cookie");
            }
            _ => panic!("expected Done with SERVER_IDENT"),
        }

        // Case B: lossless client → lossless response with cookie != 0
        let server_lossless = SessionServer::new(456, server_addr);
        let client_ident = ClientIdentFrame::new(
            crate::EntityAddrvec::with_addr(msgr2_addr(7000)),
            server_addr,
            0,
            0,
            session_supported_features(),
            0,
            0,  // lossless
            10, // lossless client cookie is non-zero
        );
        let frame = create_frame_from_trait(&client_ident, Tag::ClientIdent).unwrap();
        match server_lossless.step(frame).unwrap() {
            Step::Done((), Some(response)) => {
                let segment = response.segments.first().expect("SERVER_IDENT payload");
                let mut p = segment.clone();
                let _ = crate::EntityAddrvec::decode(&mut p, 0).unwrap();
                let _ = u64::decode(&mut p, 0).unwrap();
                let _ = u64::decode(&mut p, 0).unwrap();
                let _ = u64::decode(&mut p, 0).unwrap();
                let _ = u64::decode(&mut p, 0).unwrap();
                let flags = u64::decode(&mut p, 0).unwrap();
                let cookie = u64::decode(&mut p, 0).unwrap();
                assert_eq!(
                    flags & CEPH_MSG_CONNECT_LOSSY,
                    0,
                    "server should echo client's lossless flag"
                );
                assert_eq!(
                    cookie, 456,
                    "lossless response must carry configured cookie"
                );
            }
            _ => panic!("expected Done with SERVER_IDENT"),
        }
    }

    #[test]
    fn server_replies_to_session_reconnect_with_reconnect_ok() {
        let server = SessionServer::new(123, msgr2_addr(6900));
        let reconnect = SessionReconnectFrame::new(
            crate::EntityAddrvec::with_addr(msgr2_addr(7000)),
            1,
            2,
            3,
            4,
            5,
        );
        let frame = create_frame_from_trait(&reconnect, Tag::SessionReconnect).unwrap();

        match server.step(frame).unwrap() {
            Step::Done((), Some(response)) => {
                assert_eq!(response.preamble.tag, Tag::SessionReconnectOk);
            }
            _ => panic!("expected reconnect response"),
        }
    }
}
