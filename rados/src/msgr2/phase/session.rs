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
        let addrs = crate::EntityAddrvec::with_addr(self.client_addr.clone());
        let flags = if self.is_lossy {
            CEPH_MSG_CONNECT_LOSSY
        } else {
            0
        };
        let ident = ClientIdentFrame::new(
            addrs,
            self.server_addr.clone(),
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
        let addrs = crate::EntityAddrvec::with_addr(self.client_addr.clone());
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

    fn step(mut self, frame: Frame) -> Result<Step<Self, SessionClientOutput>> {
        match frame.preamble.tag {
            Tag::ServerIdent => {
                let segment = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("SERVER_IDENT missing payload"))?;
                let mut p = segment.clone();
                let _addrs = crate::EntityAddrvec::decode(&mut p, 0)?;
                let _gid = u64::decode(&mut p, 0)?;
                let _global_seq = u64::decode(&mut p, 0)?;
                let features_supported = u64::decode(&mut p, 0)?;
                let features_required = u64::decode(&mut p, 0)?;
                let _flags = u64::decode(&mut p, 0)?;
                let server_cookie = u64::decode(&mut p, 0)?;

                let our_features = session_supported_features();
                let missing = features_required & !our_features;
                if missing != 0 {
                    return Err(Error::Protocol(format!(
                        "Server requires unsupported features: 0x{missing:x}"
                    )));
                }

                let negotiated_features = our_features & features_supported;
                tracing::info!(
                    "Session established: negotiated_features=0x{negotiated_features:x}, server_cookie={server_cookie}"
                );
                Ok(Step::Done(
                    SessionClientOutput {
                        negotiated_features,
                        server_cookie,
                        reconnect_msg_seq: None,
                    },
                    None,
                ))
            }

            Tag::SessionReconnectOk => {
                let segment = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("SESSION_RECONNECT_OK missing payload"))?;
                let mut p = segment.clone();
                let msg_seq = u64::decode(&mut p, 0)?;
                tracing::info!("Reconnection successful, server acked up to msg_seq={msg_seq}");
                Ok(Step::Done(
                    SessionClientOutput {
                        negotiated_features: 0,
                        server_cookie: self.server_cookie,
                        reconnect_msg_seq: Some(msg_seq),
                    },
                    None,
                ))
            }

            Tag::SessionRetry => {
                let segment = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("SESSION_RETRY missing payload"))?;
                let mut p = segment.clone();
                let server_connect_seq = u64::decode(&mut p, 0)?;
                tracing::warn!(
                    "SESSION_RETRY: server_connect_seq={server_connect_seq}, incrementing ours"
                );
                self.connect_seq += 1;
                let retry_frame = self.build_session_reconnect()?;
                Ok(Step::Next {
                    state: self,
                    send: Some(retry_frame),
                })
            }

            Tag::SessionRetryGlobal => {
                let segment = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("SESSION_RETRY_GLOBAL missing payload"))?;
                let mut p = segment.clone();
                let server_global_seq = u64::decode(&mut p, 0)?;
                tracing::warn!("SESSION_RETRY_GLOBAL: updating global_seq to {server_global_seq}");
                self.global_seq = server_global_seq.max(self.global_seq + 1);
                let retry_frame = self.build_session_reconnect()?;
                Ok(Step::Next {
                    state: self,
                    send: Some(retry_frame),
                })
            }

            Tag::SessionReset => {
                let segment = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("SESSION_RESET missing payload"))?;
                let mut p = segment.clone();
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

            Tag::Wait => {
                tracing::debug!("Server sent WAIT, continuing to wait");
                Ok(Step::Next {
                    state: self,
                    send: None,
                })
            }

            Tag::IdentMissingFeatures => {
                let segment = frame.segments.first().ok_or_else(|| {
                    Error::protocol_error("IDENT_MISSING_FEATURES missing payload")
                })?;
                let mut p = segment.clone();
                let missing_features = u64::decode(&mut p, 0)?;
                Err(Error::Protocol(format!(
                    "Server reported missing required features: 0x{missing_features:x}"
                )))
            }

            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in session phase (client)",
                frame.preamble.tag
            ))),
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Server-side session establishment phase.
///
/// Waits for `CLIENT_IDENT`, then sends `SERVER_IDENT`.
pub struct SessionServer {
    server_cookie: u64,
}

impl SessionServer {
    pub fn new(server_cookie: u64) -> Self {
        Self { server_cookie }
    }
}

impl Phase for SessionServer {
    type Output = ();

    fn step(self, frame: Frame) -> Result<Step<Self, ()>> {
        match frame.preamble.tag {
            Tag::ClientIdent | Tag::SessionReconnect => {
                if frame.preamble.tag == Tag::ClientIdent {
                    let segment = frame
                        .segments
                        .first()
                        .ok_or_else(|| Error::protocol_error("CLIENT_IDENT missing payload"))?;
                    let mut p = segment.clone();
                    let _addrs = crate::EntityAddrvec::decode(&mut p, 0)?;
                    let _target_addr = crate::EntityAddr::decode(&mut p, 0)?;
                    let _gid = i64::decode(&mut p, 0)?;
                    let _global_seq = u64::decode(&mut p, 0)?;
                    let client_supported_features = u64::decode(&mut p, 0)?;
                    let _client_required_features = u64::decode(&mut p, 0)?;
                    let _flags = u64::decode(&mut p, 0)?;
                    let _cookie = u64::decode(&mut p, 0)?;

                    let missing_features = session_required_features() & !client_supported_features;
                    if missing_features != 0 {
                        let ident_missing = IdentMissingFeaturesFrame::new(missing_features);
                        let response =
                            create_frame_from_trait(&ident_missing, Tag::IdentMissingFeatures)?;
                        return Ok(Step::Abort {
                            error: Error::Protocol(format!(
                                "Client missing required features: 0x{missing_features:x}"
                            )),
                            send: Some(response),
                        });
                    }

                    let addrs = crate::EntityAddrvec::with_addr(crate::EntityAddr::default());
                    let server_ident = ServerIdentFrame::new(
                        addrs,
                        0,
                        0,
                        session_supported_features(),
                        session_required_features(),
                        0,
                        self.server_cookie,
                    );
                    let response = create_frame_from_trait(&server_ident, Tag::ServerIdent)?;
                    tracing::debug!("Server: sending SERVER_IDENT");
                    return Ok(Step::Done((), Some(response)));
                }

                let reconnect_ok = SessionReconnectOkFrame::new(0);
                let response = create_frame_from_trait(&reconnect_ok, Tag::SessionReconnectOk)?;
                tracing::debug!("Server: sending SESSION_RECONNECT_OK");
                Ok(Step::Done((), Some(response)))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in session phase (server)",
                frame.preamble.tag
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let server = SessionServer::new(123);
        let addrs = crate::EntityAddrvec::with_addr(crate::EntityAddr::default());
        let client_ident =
            ClientIdentFrame::new(addrs, crate::EntityAddr::default(), 0, 0, 0, 0, 0, 77);
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
    fn server_replies_to_session_reconnect_with_reconnect_ok() {
        let server = SessionServer::new(123);
        let reconnect = SessionReconnectFrame::new(
            crate::EntityAddrvec::with_addr(crate::EntityAddr::default()),
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
