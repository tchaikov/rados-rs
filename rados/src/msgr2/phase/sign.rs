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
    expected_server_sig: Option<Bytes>,
    /// Peer's msgr2 feature bits (from banner).
    peer_features: u64,
    /// Our own advertised msgr2 feature bits.
    our_features: u64,
}

impl AuthSignClient {
    pub fn new(
        our_sig: Bytes,
        expected_server_sig: Option<Bytes>,
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
                if let Some(ref expected) = self.expected_server_sig {
                    let server_sig = frame.segments.first().ok_or_else(|| {
                        Error::protocol_error("AUTH_SIGNATURE frame missing payload")
                    })?;

                    if server_sig != expected {
                        tracing::error!(
                            "Server AUTH_SIGNATURE mismatch: expected {} bytes, got {} bytes",
                            expected.len(),
                            server_sig.len()
                        );
                        return Err(Error::protocol_error(
                            "Server AUTH_SIGNATURE verification failed",
                        ));
                    }
                    tracing::info!(
                        "Server AUTH_SIGNATURE verified ({} bytes)",
                        server_sig.len()
                    );
                } else {
                    tracing::debug!("AUTH_SIGNATURE: CRC mode, no verification needed");
                }

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
    expected_client_sig: Option<Bytes>,
}

impl AuthSignServer {
    pub fn new(our_sig: Bytes, expected_client_sig: Option<Bytes>) -> Self {
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
                if let Some(ref expected) = self.expected_client_sig {
                    let client_sig = frame.segments.first().ok_or_else(|| {
                        Error::protocol_error("AUTH_SIGNATURE frame missing payload")
                    })?;

                    if client_sig != expected {
                        tracing::error!(
                            "Client AUTH_SIGNATURE mismatch: expected {} bytes, got {} bytes",
                            expected.len(),
                            client_sig.len()
                        );
                        return Err(Error::protocol_error(
                            "Client AUTH_SIGNATURE verification failed",
                        ));
                    }
                    tracing::info!(
                        "Client AUTH_SIGNATURE verified ({} bytes)",
                        client_sig.len()
                    );
                } else {
                    tracing::debug!("Server AUTH_SIGNATURE: CRC/AUTH_NONE mode, no verification");
                }

                Ok(Step::Done((), None))
            }
            _ => Err(Error::protocol_error(&format!(
                "Unexpected frame {:?} in AuthSign phase (server)",
                frame.preamble.tag
            ))),
        }
    }
}
