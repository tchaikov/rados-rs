//! Hello phase: initial HELLO frame exchange.
//!
//! The client sends HELLO first; the server replies with its own HELLO.
//!
//! Each frame carries the sender's `entity_type` (CLIENT / MON / OSD / …)
//! and the `peer_addr` — the address the sender believes the *receiver*
//! has.  The receiver uses `peer_addr` for address learning: a host behind
//! NAT discovers its externally visible address from the first peer that
//! connects (Ceph `ProtocolV2::handle_hello`, Linux `process_hello`).

use crate::Denc;
use crate::msgr2::{
    error::{Msgr2Error as Error, Result},
    frames::create_frame_from_trait,
    frames::{Frame, HelloFrame, Tag},
    phase::{Phase, Step},
};

// ── Client ────────────────────────────────────────────────────────────────────

/// Client-side HELLO exchange.
///
/// Sends `HELLO(CLIENT, peer_addr)` — where `peer_addr` is the server's
/// address as dialed by the client — and waits for the server's `HELLO`
/// reply.
pub struct HelloClient {
    /// The server's address as we see it. The server uses this to learn
    /// its own externally visible address (NAT traversal).
    peer_addr: crate::EntityAddr,
}

impl HelloClient {
    pub fn new(peer_addr: crate::EntityAddr) -> Self {
        Self { peer_addr }
    }
}

impl Phase for HelloClient {
    type Output = ();

    fn enter(&mut self) -> Result<Option<Frame>> {
        let frame = HelloFrame::new(crate::EntityType::CLIENT.bits() as u8, self.peer_addr);
        Ok(Some(create_frame_from_trait(&frame, Tag::Hello)?))
    }

    fn step(self, frame: Frame) -> Result<Step<Self, ()>> {
        match frame.preamble.tag {
            Tag::Hello => {
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error("HELLO frame missing payload"));
                }
                let mut payload = frame.segments[0].as_ref();
                let entity_type = u8::decode(&mut payload, 0)?;
                tracing::debug!("Received server HELLO: entity_type={entity_type}");

                // Ceph C++ sets connection policy from the server's entity
                // type here. We don't yet have per-type policies, but
                // reject a server that identifies as CLIENT — that's a
                // peer misconfiguration, not a usable daemon.
                if entity_type == crate::EntityType::CLIENT.bits() as u8 {
                    return Err(Error::protocol_error(
                        "server HELLO advertises entity_type=CLIENT; \
                         expected a daemon type (MON/OSD/MDS/MGR)",
                    ));
                }

                Ok(Step::Done((), None))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in Hello phase (client)",
                frame.preamble.tag
            ))),
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Server-side HELLO exchange.
///
/// Waits for the client's `HELLO`, then sends the server's own `HELLO`
/// reply. Each daemon (MON/OSD/MDS/MGR) must advertise its own type in
/// the reply, so the caller supplies it via [`HelloServer::new`].
pub struct HelloServer {
    entity_type: crate::EntityType,
    /// The client's address as the server sees it. The client uses this
    /// to learn its own externally visible address (NAT traversal).
    peer_addr: crate::EntityAddr,
}

impl HelloServer {
    pub fn new(entity_type: crate::EntityType, peer_addr: crate::EntityAddr) -> Self {
        Self {
            entity_type,
            peer_addr,
        }
    }
}

impl Phase for HelloServer {
    type Output = ();

    fn step(self, frame: Frame) -> Result<Step<Self, ()>> {
        match frame.preamble.tag {
            Tag::Hello => {
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error("HELLO frame missing payload"));
                }
                let mut payload = frame.segments[0].as_ref();
                let entity_type = u8::decode(&mut payload, 0)?;
                tracing::debug!("Received client HELLO: entity_type={entity_type}");

                let response = HelloFrame::new(self.entity_type.bits() as u8, self.peer_addr);
                let response_frame = create_frame_from_trait(&response, Tag::Hello)?;
                Ok(Step::Done((), Some(response_frame)))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in Hello phase (server)",
                frame.preamble.tag
            ))),
        }
    }
}
