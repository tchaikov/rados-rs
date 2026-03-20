//! Hierarchical State Machine (HSM) for the msgr2 protocol handshake.
//!
//! Each connection establishment proceeds through a fixed sequence of phases.
//! Phases are **pure**: they manipulate state and return frames to send, but
//! never perform I/O themselves.  The [`drive`] function feeds frames in and
//! out via the existing [`FrameIO`] / [`StateMachine`] layer.
//!
//! ## Protocol phases (client)
//! ```text
//! Hello → AuthNegotiate → AuthSign → [Compression] → Session
//! ```
//! ## Protocol phases (server)
//! ```text
//! Hello → AuthNegotiate → AuthSign → [Compression] → Session
//! ```

use crate::msgr2::{
    error::{Msgr2Error as Error, Result},
    frames::Frame,
    protocol::FrameIO,
    state_machine::StateMachine,
};

pub(crate) mod auth;
pub(crate) mod compression;
pub(crate) mod hello;
pub(crate) mod session;
pub(crate) mod sign;

// ── Core types ───────────────────────────────────────────────────────────────

/// Outcome of a single [`Phase::step`] call.
pub enum Step<P, O> {
    /// Stay in this phase.  Optionally send `frame` before waiting for the next one.
    Next { state: P, send: Option<Frame> },
    /// Phase is complete.  Optionally send a final `frame` before returning `output`.
    Done(O, Option<Frame>),
    /// Abort the phase after optionally sending a final protocol error frame.
    Abort { error: Error, send: Option<Frame> },
}

/// A pure state machine for one msgr2 handshake phase.
///
/// Implementors hold only the data they need for their phase.  All I/O is
/// performed by [`drive`]; phases never touch the network directly.
pub trait Phase: Sized {
    /// Value produced when the phase completes.
    type Output;

    /// Optional frame to send when first entering this phase (before any
    /// [`step`] call).  Called exactly once by [`drive`].
    fn enter(&mut self) -> Result<Option<Frame>> {
        Ok(None)
    }

    /// Process one received frame, returning the next state or the phase output.
    fn step(self, frame: Frame) -> Result<Step<Self, Self::Output>>;
}

// ── Drive function ────────────────────────────────────────────────────────────

/// Run `phase` to completion, sending and receiving frames through `frame_io`.
///
/// 1. Calls [`Phase::enter`] and sends the resulting frame (if any).
/// 2. Loops: receives a frame, calls [`Phase::step`], sends any outbound frame.
/// 3. Returns the phase output when [`Step::Done`] is reached.
pub(crate) async fn drive<P: Phase>(
    frame_io: &mut FrameIO,
    sm: &mut StateMachine,
    phase: P,
) -> Result<P::Output> {
    let mut current = phase;

    if let Some(frame) = current.enter()? {
        frame_io.send_frame(&frame, sm).await?;
    }

    loop {
        let frame = frame_io.recv_frame(sm).await?;
        match current.step(frame)? {
            Step::Next { state, send } => {
                if let Some(f) = send {
                    frame_io.send_frame(&f, sm).await?;
                }
                current = state;
            }
            Step::Done(output, send) => {
                if let Some(f) = send {
                    frame_io.send_frame(&f, sm).await?;
                }
                return Ok(output);
            }
            Step::Abort { error, send } => {
                if let Some(f) = send {
                    frame_io.send_frame(&f, sm).await?;
                }
                return Err(error);
            }
        }
    }
}
