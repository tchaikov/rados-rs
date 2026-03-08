//! Compression negotiation phase.
//!
//! Enabled only when both peers advertise the `COMPRESSION` feature bit.
//!
//! **Client** sends `COMPRESSION_REQUEST` and receives `COMPRESSION_DONE`.
//! **Server** waits for `COMPRESSION_REQUEST` and replies with `COMPRESSION_DONE`.

use crate::{
    compression::CompressionAlgorithm,
    error::{Msgr2Error as Error, Result},
    frames::{CompressionDoneFrame, CompressionRequestFrame, Frame, Tag},
    phase::{Phase, Step},
    state_machine::create_frame_from_trait,
};
use denc::Denc;

// ── Shared output ─────────────────────────────────────────────────────────────

/// Produced when the compression phase completes.
#[derive(Debug)]
pub struct CompressionOutput {
    /// The algorithm selected by the server (`None` if compression was declined).
    pub algorithm: CompressionAlgorithm,
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Client-side compression negotiation.
///
/// Sends `COMPRESSION_REQUEST` with an empty preferred-methods list (meaning
/// "any method is acceptable") and awaits the server's `COMPRESSION_DONE`.
pub struct CompressionClient;

impl Phase for CompressionClient {
    type Output = CompressionOutput;

    fn enter(&mut self) -> Result<Option<Frame>> {
        tracing::debug!("Sending COMPRESSION_REQUEST");
        let req = CompressionRequestFrame::new(true, vec![]);
        Ok(Some(create_frame_from_trait(
            &req,
            Tag::CompressionRequest,
        )?))
    }

    fn step(self, frame: Frame) -> Result<Step<Self, CompressionOutput>> {
        match frame.preamble.tag {
            Tag::CompressionDone => {
                let segment = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("COMPRESSION_DONE missing payload"))?;
                let mut p = segment.clone();
                let is_compress = bool::decode(&mut p, 0)?;
                let method: u32 = u32::decode(&mut p, 0)?;

                let algorithm = if is_compress {
                    CompressionAlgorithm::try_from(method).unwrap_or(CompressionAlgorithm::None)
                } else {
                    CompressionAlgorithm::None
                };

                tracing::debug!("COMPRESSION_DONE: algorithm={algorithm:?}");
                Ok(Step::Done(CompressionOutput { algorithm }, None))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in compression phase (client)",
                frame.preamble.tag
            ))),
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Server-side compression negotiation.
///
/// Waits for the client's `COMPRESSION_REQUEST`, selects the best available
/// algorithm from the client's preferred list, and replies with
/// `COMPRESSION_DONE`.
pub struct CompressionServer;

impl CompressionServer {
    fn select_algorithm(request: &CompressionRequestFrame) -> CompressionAlgorithm {
        if !request.is_compress {
            return CompressionAlgorithm::None;
        }
        if request.preferred_methods.is_empty() {
            return CompressionAlgorithm::Snappy;
        }
        request
            .preferred_methods
            .iter()
            .find_map(|&m| {
                CompressionAlgorithm::try_from(m)
                    .ok()
                    .filter(|a| *a != CompressionAlgorithm::None)
            })
            .unwrap_or(CompressionAlgorithm::None)
    }
}

impl Phase for CompressionServer {
    type Output = CompressionOutput;

    fn step(self, frame: Frame) -> Result<Step<Self, CompressionOutput>> {
        match frame.preamble.tag {
            Tag::CompressionRequest => {
                if frame.segments.is_empty() {
                    return Err(Error::protocol_error("COMPRESSION_REQUEST missing payload"));
                }
                let mut p = frame.segments[0].clone();
                let request = CompressionRequestFrame {
                    is_compress: bool::decode(&mut p, 0)?,
                    preferred_methods: Vec::<u32>::decode(&mut p, 0)?,
                };

                let algorithm = Self::select_algorithm(&request);
                let done = match algorithm {
                    CompressionAlgorithm::None => CompressionDoneFrame::new(false, 0u32),
                    alg => CompressionDoneFrame::new(true, alg.into()),
                };
                let done_frame = create_frame_from_trait(&done, Tag::CompressionDone)?;
                tracing::debug!("Server COMPRESSION_DONE: algorithm={algorithm:?}");

                Ok(Step::Done(
                    CompressionOutput { algorithm },
                    Some(done_frame),
                ))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in compression phase (server)",
                frame.preamble.tag
            ))),
        }
    }
}
