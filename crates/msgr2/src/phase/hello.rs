//! Hello phase: initial HELLO frame exchange.
//!
//! The client sends HELLO first; the server replies with its own HELLO.
//! Neither frame carries data that is used by later phases — this phase simply
//! validates that both peers speak msgr2.

use crate::{
    error::{Msgr2Error as Error, Result},
    frames::{Frame, HelloFrame, Tag},
    phase::{Phase, Step},
    state_machine::create_frame_from_trait,
};
use denc::Denc;

// ── Client ────────────────────────────────────────────────────────────────────

/// Client-side HELLO exchange.
///
/// Sends `HELLO(CLIENT)` and waits for the server's `HELLO` reply.
pub struct HelloClient;

impl Phase for HelloClient {
    type Output = ();

    fn enter(&mut self) -> Result<Option<Frame>> {
        let frame = HelloFrame::new(
            denc::EntityType::CLIENT.bits() as u8,
            denc::EntityAddr::default(),
        );
        Ok(Some(create_frame_from_trait(&frame, Tag::Hello)?))
    }

    fn step(self, frame: Frame) -> Result<Step<Self, ()>> {
        match frame.preamble.tag {
            Tag::Hello => {
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error("HELLO frame missing payload"));
                }
                let mut payload = frame.segments[0].clone();
                let entity_type = u8::decode(&mut payload, 0)?;
                tracing::debug!("Received server HELLO: entity_type={entity_type}");
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
/// Waits for the client's `HELLO`, then sends the server's own `HELLO` reply.
pub struct HelloServer;

impl Phase for HelloServer {
    type Output = ();

    fn step(self, frame: Frame) -> Result<Step<Self, ()>> {
        match frame.preamble.tag {
            Tag::Hello => {
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error("HELLO frame missing payload"));
                }
                let mut payload = frame.segments[0].clone();
                let entity_type = u8::decode(&mut payload, 0)?;
                tracing::debug!("Received client HELLO: entity_type={entity_type}");

                let response = HelloFrame::new(
                    denc::EntityType::CLIENT.bits() as u8,
                    denc::EntityAddr::default(),
                );
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
