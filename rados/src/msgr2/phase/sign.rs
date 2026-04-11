//! Auth-signature phase: AUTH_SIGNATURE exchange.
//!
//! After `AUTH_DONE`, both sides exchange `AUTH_SIGNATURE` frames containing
//! HMAC-SHA256 digests of all bytes sent/received before this point.  This
//! provides mutual authentication in SECURE mode.
//!
//! **Client** sends its signature first, then verifies the server's reply.
//! **Server** sends its signature first (via `enter`), then verifies the
//! client's signature.

use crate::msgr2::{
    error::{Msgr2Error as Error, Result},
    frames::create_frame_from_trait,
    frames::{AuthSignatureFrame, Frame, Tag},
    phase::{Phase, Step},
};
use bytes::Bytes;
use subtle::ConstantTimeEq;

/// Constant-time equality check for HMAC tags. Unequal-length inputs
/// return `false` immediately — length is not secret (HMAC-SHA256 is
/// always 32 bytes, so a length mismatch is a framing bug rather than
/// a MAC forgery attempt). The byte-level comparison must be
/// constant-time to avoid leaking the expected tag via a timing side
/// channel, which is why this routes through `subtle::ConstantTimeEq`
/// instead of `Bytes::PartialEq` / `==`.
fn verify_hmac(got: &[u8], expected: &[u8]) -> bool {
    got.len() == expected.len() && got.ct_eq(expected).into()
}

// ── Client output ─────────────────────────────────────────────────────────────

/// Produced when the client-side signature exchange completes.
#[derive(Debug)]
pub struct AuthSignClientOutput {
    /// True when both sides advertised the `COMPRESSION` feature bit.
    pub needs_compression: bool,
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Client-side AUTH_SIGNATURE exchange.
///
/// Sends the pre-computed HMAC-SHA256 signature of all bytes received so far,
/// then verifies the server's matching signature.
pub struct AuthSignClient {
    /// HMAC(session_key, all_rx_bytes) — what we send to the server.
    our_sig: Bytes,
    /// HMAC(session_key, all_tx_bytes) — what we expect the server to send.
    expected_server_sig: Bytes,
    /// Peer's msgr2 feature bits (from banner).
    peer_features: u64,
    /// Our own advertised msgr2 feature bits.
    our_features: u64,
}

impl AuthSignClient {
    pub fn new(
        our_sig: Bytes,
        expected_server_sig: Bytes,
        peer_features: u64,
        our_features: u64,
    ) -> Self {
        Self {
            our_sig,
            expected_server_sig,
            peer_features,
            our_features,
        }
    }
}

impl Phase for AuthSignClient {
    type Output = AuthSignClientOutput;

    fn enter(&mut self) -> Result<Option<Frame>> {
        tracing::debug!(
            "Sending AUTH_SIGNATURE to server ({} bytes)",
            self.our_sig.len()
        );
        let sig_frame = AuthSignatureFrame::new(self.our_sig.clone());
        Ok(Some(create_frame_from_trait(
            &sig_frame,
            Tag::AuthSignature,
        )?))
    }

    fn step(self, frame: Frame) -> Result<Step<Self, AuthSignClientOutput>> {
        match frame.preamble.tag {
            Tag::AuthSignature => {
                let server_sig = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("AUTH_SIGNATURE frame missing payload"))?;

                if !verify_hmac(server_sig, &self.expected_server_sig) {
                    return Err(Error::protocol_error(
                        "Server AUTH_SIGNATURE verification failed",
                    ));
                }
                tracing::info!(
                    "Server AUTH_SIGNATURE verified ({} bytes)",
                    server_sig.len()
                );

                let peer_fs = crate::msgr2::FeatureSet::from(self.peer_features);
                let our_fs = crate::msgr2::FeatureSet::from(self.our_features);
                let needs_compression = peer_fs.contains(crate::msgr2::FeatureSet::COMPRESSION)
                    && our_fs.contains(crate::msgr2::FeatureSet::COMPRESSION);

                Ok(Step::Done(AuthSignClientOutput { needs_compression }, None))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in AuthSign phase (client)",
                frame.preamble.tag
            ))),
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Server-side AUTH_SIGNATURE exchange.
///
/// Sends the server's pre-computed signature first (via `enter`), then
/// verifies the client's reply.
pub struct AuthSignServer {
    /// HMAC(session_key, all_rx_bytes) — what we send to the client.
    our_sig: Bytes,
    /// HMAC(session_key, all_tx_bytes) — what we expect the client to send.
    expected_client_sig: Bytes,
}

impl AuthSignServer {
    pub fn new(our_sig: Bytes, expected_client_sig: Bytes) -> Self {
        Self {
            our_sig,
            expected_client_sig,
        }
    }
}

impl Phase for AuthSignServer {
    type Output = ();

    fn enter(&mut self) -> Result<Option<Frame>> {
        tracing::debug!(
            "Server: sending AUTH_SIGNATURE to client ({} bytes)",
            self.our_sig.len()
        );
        let sig_frame = AuthSignatureFrame::new(self.our_sig.clone());
        Ok(Some(create_frame_from_trait(
            &sig_frame,
            Tag::AuthSignature,
        )?))
    }

    fn step(self, frame: Frame) -> Result<Step<Self, ()>> {
        match frame.preamble.tag {
            Tag::AuthSignature => {
                let client_sig = frame
                    .segments
                    .first()
                    .ok_or_else(|| Error::protocol_error("AUTH_SIGNATURE frame missing payload"))?;

                if !verify_hmac(client_sig, &self.expected_client_sig) {
                    return Err(Error::protocol_error(
                        "Client AUTH_SIGNATURE verification failed",
                    ));
                }
                tracing::info!(
                    "Client AUTH_SIGNATURE verified ({} bytes)",
                    client_sig.len()
                );

                Ok(Step::Done((), None))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in AuthSign phase (server)",
                frame.preamble.tag
            ))),
        }
    }
}
