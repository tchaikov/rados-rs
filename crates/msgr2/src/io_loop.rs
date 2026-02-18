//! Shared I/O loop for msgr2 connections
//!
//! Both monitor connections (monclient) and OSD sessions (osdclient) need the
//! same select!-based loop: drain an outgoing message channel, receive incoming
//! messages, and send periodic keepalive frames. Only the message routing
//! callback differs between the two.
//!
//! This module provides [`run_io_loop`], a generic async function that
//! implements the common loop mechanics and delegates routing to the caller.

use std::future::Future;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::message::Message;
use crate::protocol::Connection;

/// Keepalive configuration for [`run_io_loop`]
pub struct KeepaliveConfig {
    /// Interval between keepalive frames
    pub interval: Duration,
    /// If `Some`, break the loop when no ACK is received within this duration
    pub timeout: Option<Duration>,
}

/// Run the standard msgr2 I/O loop.
///
/// Drives a msgr2 [`Connection`] in a single async task that multiplexes:
/// - **Outgoing messages**: drains `send_rx` and forwards each to the connection
/// - **Incoming messages**: receives from the connection and calls `route`
/// - **Keepalive**: sends periodic keepalive frames and optionally enforces a
///   timeout if no ACK is received
/// - **Shutdown**: exits cleanly when `shutdown_token` is cancelled
///
/// The `route` callback receives every incoming message and returns `true` to
/// continue the loop or `false` to break it.
///
/// The loop exits when any of the following occur:
/// - `shutdown_token` is cancelled
/// - `send_rx` is closed (all senders dropped)
/// - `connection.recv_message()` returns an error
/// - `connection.send_message()` returns an error
/// - Keepalive timeout fires (when `keepalive.timeout` is `Some`)
/// - `route` returns `false`
pub async fn run_io_loop<R, Fut>(
    mut connection: Connection,
    mut send_rx: mpsc::Receiver<Message>,
    shutdown_token: CancellationToken,
    keepalive: Option<KeepaliveConfig>,
    mut route: R,
) where
    R: FnMut(Message) -> Fut,
    Fut: Future<Output = bool>,
{
    let mut keepalive_interval = keepalive
        .as_ref()
        .map(|k| tokio::time::interval_at(tokio::time::Instant::now() + k.interval, k.interval));
    let mut last_keepalive_sent: Option<std::time::Instant> = None;

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                tracing::debug!("I/O loop: shutdown token cancelled");
                break;
            }

            msg_opt = send_rx.recv() => {
                match msg_opt {
                    Some(msg) => {
                        if let Err(e) = connection.send_message(msg).await {
                            tracing::error!("I/O loop: send error: {}", e);
                            break;
                        }
                    }
                    None => {
                        tracing::debug!("I/O loop: send channel closed");
                        break;
                    }
                }
            }

            result = connection.recv_message() => {
                match result {
                    Ok(msg) => {
                        if !route(msg).await {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("I/O loop: recv error: {}", e);
                        break;
                    }
                }
            }

            _ = async {
                if let Some(ref mut interval) = keepalive_interval {
                    interval.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                // Check for keepalive timeout before sending the next frame
                if let Some(ref k) = keepalive {
                    if let Some(timeout) = k.timeout {
                        if let Some(sent) = last_keepalive_sent {
                            let timed_out = if let Some(last_ack) = connection.last_keepalive_ack() {
                                last_ack.elapsed() > timeout
                            } else {
                                // No ACK yet — measure against when we first sent
                                sent.elapsed() > timeout
                            };
                            if timed_out {
                                tracing::warn!("I/O loop: keepalive timeout ({:?})", timeout);
                                break;
                            }
                        }
                    }
                }

                if let Err(e) = connection.send_keepalive().await {
                    tracing::error!("I/O loop: keepalive send error: {}", e);
                    break;
                }
                last_keepalive_sent = Some(std::time::Instant::now());
            }
        }
    }
}
