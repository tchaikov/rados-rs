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

use crate::{
    error::{Msgr2Error as Error, Result},
    frames::{ClientIdentFrame, Frame, ServerIdentFrame, SessionReconnectFrame, Tag},
    phase::{Phase, Step},
    state_machine::create_frame_from_trait,
};
use denc::Denc;

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
    server_addr: denc::EntityAddr,
    client_addr: denc::EntityAddr,
    client_cookie: u64,
    server_cookie: u64,
    global_seq: u64,
    connect_seq: u64,
    in_seq: u64,
}

impl SessionClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        global_id: u64,
        server_addr: denc::EntityAddr,
        client_addr: denc::EntityAddr,
        client_cookie: u64,
        server_cookie: u64,
        global_seq: u64,
        connect_seq: u64,
        in_seq: u64,
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
        }
    }

    fn build_client_ident(&self) -> Result<Frame> {
        use denc::features::CephFeatures;
        let features_supported: u64 = (CephFeatures::MSG_ADDR2
            | CephFeatures::SERVER_NAUTILUS
            | CephFeatures::SERVER_OCTOPUS)
            .bits()
            | 0x3fff_ffff_ffff_ffff;
        let features_required: u64 = CephFeatures::MSG_ADDR2.bits();

        let addrs = denc::EntityAddrvec::with_addr(self.client_addr.clone());
        let ident = ClientIdentFrame::new(
            addrs,
            self.server_addr.clone(),
            self.global_id as i64,
            self.global_seq,
            features_supported,
            features_required,
            0, // flags (non-lossy)
            self.client_cookie,
        );
        tracing::debug!(
            "Sending CLIENT_IDENT: gid={}, cookie={}, global_seq={}",
            self.global_id,
            self.client_cookie,
            self.global_seq
        );
        create_frame_from_trait(&ident, Tag::ClientIdent)
    }

    fn build_session_reconnect(&self) -> Result<Frame> {
        let addrs = denc::EntityAddrvec::with_addr(self.client_addr.clone());
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
        create_frame_from_trait(&reconnect, Tag::SessionReconnect)
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
                let _addrs = denc::EntityAddrvec::decode(&mut p, 0).map_err(|e| {
                    Error::protocol_error(&format!("Failed to decode addrs: {e:?}"))
                })?;
                let _gid = u64::decode(&mut p, 0)?;
                let _global_seq = u64::decode(&mut p, 0)?;
                let features_supported = u64::decode(&mut p, 0)?;
                let features_required = u64::decode(&mut p, 0)?;
                let _flags = u64::decode(&mut p, 0)?;
                let server_cookie = u64::decode(&mut p, 0)?;

                use denc::features::CephFeatures;
                let our_features: u64 = (CephFeatures::MSG_ADDR2
                    | CephFeatures::SERVER_NAUTILUS
                    | CephFeatures::SERVER_OCTOPUS)
                    .bits()
                    | 0x3fff_ffff_ffff_ffff;

                let missing = features_required & !our_features;
                if missing != 0 {
                    return Ok(Step::Done(
                        SessionClientOutput {
                            negotiated_features: 0,
                            server_cookie,
                            reconnect_msg_seq: None,
                        },
                        None,
                    ));
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

            Tag::IdentMissingFeatures => Err(Error::Protocol(
                "Server reported missing required features (IDENT_MISSING_FEATURES)".into(),
            )),

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
                let addrs = denc::EntityAddrvec::with_addr(denc::EntityAddr::default());
                let server_ident = ServerIdentFrame::new(addrs, 0, 0, 0, 0, 0, self.server_cookie);
                let response = create_frame_from_trait(&server_ident, Tag::ServerIdent)?;
                tracing::debug!("Server: sending SERVER_IDENT");
                Ok(Step::Done((), Some(response)))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in session phase (server)",
                frame.preamble.tag
            ))),
        }
    }
}
