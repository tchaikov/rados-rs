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

use crate::msgr2::message::Message;
use crate::msgr2::protocol::{Connection, PriorityQueue};

const MAX_OUTBOUND_SEND_BURST: usize = 32;

/// Keepalive configuration for [`run_io_loop`]
pub struct KeepaliveConfig {
    /// Interval between keepalive frames
    pub interval: Duration,
    /// If `Some`, break the loop when no ACK is received within this duration
    pub timeout: Option<Duration>,
}

fn keepalive_timed_out(
    last_keepalive_sent: Option<std::time::Instant>,
    last_keepalive_ack: Option<std::time::Instant>,
    timeout: Duration,
    now: std::time::Instant,
) -> bool {
    let Some(sent_at) = last_keepalive_sent else {
        return false;
    };

    let Some(elapsed_since_send) = now.checked_duration_since(sent_at) else {
        return false;
    };

    if elapsed_since_send <= timeout {
        return false;
    }

    match last_keepalive_ack {
        Some(ack_at) => ack_at < sent_at,
        None => true,
    }
}

fn should_probe_io_fairness(
    consecutive_outbound_sends: usize,
    outbound: &PriorityQueue<Message>,
) -> bool {
    !outbound.is_empty() && consecutive_outbound_sends >= MAX_OUTBOUND_SEND_BURST
}

async fn wait_for_keepalive_tick(interval: &mut Option<tokio::time::Interval>) {
    if let Some(interval) = interval {
        interval.tick().await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn handle_keepalive_tick(
    connection: &mut Connection,
    keepalive: Option<&KeepaliveConfig>,
    last_keepalive_sent: &mut Option<std::time::Instant>,
) -> bool {
    // Check for keepalive timeout before sending the next frame
    if let Some(k) = keepalive
        && let Some(timeout) = k.timeout
        && keepalive_timed_out(
            *last_keepalive_sent,
            connection.last_keepalive_ack(),
            timeout,
            std::time::Instant::now(),
        )
    {
        tracing::warn!("I/O loop: keepalive timeout ({:?})", timeout);
        return false;
    }

    if let Err(e) = connection.send_keepalive().await {
        tracing::error!("I/O loop: keepalive send error: {}", e);
        return false;
    }

    *last_keepalive_sent = Some(std::time::Instant::now());
    true
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
pub async fn run_io_loop<R, Fut, P>(
    mut connection: Connection,
    mut send_rx: mpsc::Receiver<Message>,
    shutdown_token: CancellationToken,
    keepalive: Option<KeepaliveConfig>,
    mut pre_send: P,
    mut route: R,
) where
    R: FnMut(Message) -> Fut,
    Fut: Future<Output = bool>,
    P: FnMut(&Message) -> bool,
{
    let mut keepalive_interval = keepalive
        .as_ref()
        .map(|k| tokio::time::interval_at(tokio::time::Instant::now() + k.interval, k.interval));
    let mut last_keepalive_sent: Option<std::time::Instant> = None;
    let mut consecutive_outbound_sends = 0usize;
    let mut outbound = PriorityQueue::new();

    loop {
        // Fast-path shutdown: check before draining so that buffered messages
        // for migrated ops (whose sessions were closed by scan_requests) are
        // never sent to the wire.
        if shutdown_token.is_cancelled() {
            tracing::debug!("I/O loop: shutdown token cancelled (pre-drain check)");
            break;
        }

        // Phase 1: Drain all pending messages into PriorityQueue (non-blocking)
        let mut drained_count = 0;
        while let Ok(msg) = send_rx.try_recv() {
            outbound.push_back(msg);
            drained_count += 1;

            // Yield every 100 messages to avoid starving other tasks
            if drained_count % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }

        // Phase 2: After a burst of sends, briefly prioritise receive/keepalive
        // work so sustained outbound traffic does not indefinitely delay it.
        if should_probe_io_fairness(consecutive_outbound_sends, &outbound) {
            tokio::select! {
                biased;

                _ = shutdown_token.cancelled() => {
                    tracing::debug!("I/O loop: shutdown token cancelled");
                    break;
                }

                result = connection.recv_message() => {
                    consecutive_outbound_sends = 0;
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
                    continue;
                }

                _ = wait_for_keepalive_tick(&mut keepalive_interval) => {
                    consecutive_outbound_sends = 0;
                    if !handle_keepalive_tick(
                        &mut connection,
                        keepalive.as_ref(),
                        &mut last_keepalive_sent,
                    )
                    .await
                    {
                        break;
                    }
                    continue;
                }

                _ = tokio::task::yield_now() => {
                    consecutive_outbound_sends = 0;
                }
            }
        }

        // Phase 3: If we have queued messages, send highest priority first.
        // pre_send is checked just before writing to the wire so that callers
        // can drop messages whose backing state has been invalidated (e.g., an
        // OSD op whose pending_op was migrated to a different session while the
        // encoded bytes were already queued here).
        if let Some(msg) = outbound.pop_front() {
            if !pre_send(&msg) {
                tracing::debug!(
                    "I/O loop: dropping message type=0x{:04x} tid={} (pre_send filter)",
                    msg.msg_type(),
                    msg.header.get_tid(),
                );
                consecutive_outbound_sends = 0;
                continue;
            }
            // Second shutdown check: close the TOCTOU window between pre_send
            // returning true and the actual wire write.  scan_requests cancels
            // the session token immediately after removing the pending_op, so
            // this re-check catches the case where cancellation was observed
            // between the contains_key test inside pre_send and here.
            if shutdown_token.is_cancelled() {
                tracing::debug!(
                    "I/O loop: shutdown cancelled between pre_send and send (dropping buffered op)"
                );
                break;
            }
            if let Err(e) = connection.send_message(msg).await {
                tracing::error!("I/O loop: send error: {}", e);
                break;
            }
            consecutive_outbound_sends += 1;
            continue; // Loop back to drain more before blocking
        }

        consecutive_outbound_sends = 0;

        // Phase 4: Nothing pending — block in select!
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                tracing::debug!("I/O loop: shutdown token cancelled");
                break;
            }

            msg_opt = send_rx.recv() => {
                match msg_opt {
                    Some(msg) => {
                        outbound.push_back(msg);
                        // Don't send yet — loop back to drain any additional
                        // messages that arrived, so we can prioritise correctly
                    }
                    None => {
                        tracing::debug!("I/O loop: send channel closed");
                        break;
                    }
                }
            }

            result = connection.recv_message() => {
                consecutive_outbound_sends = 0;
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

            _ = wait_for_keepalive_tick(&mut keepalive_interval) => {
                consecutive_outbound_sends = 0;
                if !handle_keepalive_tick(
                    &mut connection,
                    keepalive.as_ref(),
                    &mut last_keepalive_sent,
                )
                .await
                {
                    break;
                }
            }
        }
    }

    // Graceful close: shut down the TCP write half before `connection`
    // drops so the peer observes a clean FIN instead of an RST. Bounded
    // by a short timeout so a pathologically stuck kernel TCP send buffer
    // cannot hang the task forever — if we can't shut down in 1s we log
    // and proceed with the drop.
    match tokio::time::timeout(Duration::from_secs(1), connection.close()).await {
        Ok(()) => tracing::debug!("I/O loop: graceful close complete"),
        Err(_) => {
            tracing::warn!("I/O loop: graceful close timed out after 1s; dropping connection")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_OUTBOUND_SEND_BURST, keepalive_timed_out, should_probe_io_fairness};
    use crate::msgr2::message::Message;
    use crate::msgr2::protocol::PriorityQueue;
    use std::time::{Duration, Instant};

    #[test]
    fn keepalive_timeout_ignores_old_ack_if_latest_ping_was_acked() {
        let base = Instant::now();
        let sent_at = base + Duration::from_secs(20);
        let ack_at = sent_at + Duration::from_secs(1);
        let now = base + Duration::from_secs(40);

        assert!(
            !keepalive_timed_out(Some(sent_at), Some(ack_at), Duration::from_secs(10), now,),
            "an ACK for the latest keepalive should prevent timeout even if it arrived long ago",
        );
    }

    #[test]
    fn keepalive_timeout_fires_when_latest_ping_is_unacked() {
        let base = Instant::now();
        let sent_at = base;
        let previous_ack_at = base - Duration::from_secs(1);
        let now = base + Duration::from_secs(11);

        assert!(
            keepalive_timed_out(
                Some(sent_at),
                Some(previous_ack_at),
                Duration::from_secs(10),
                now,
            ),
            "an ACK for an older keepalive must not satisfy the current timeout check",
        );
    }

    #[test]
    fn keepalive_timeout_waits_until_timeout_expires() {
        let base = Instant::now();
        let sent_at = base;
        let now = base + Duration::from_secs(9);

        assert!(
            !keepalive_timed_out(Some(sent_at), None, Duration::from_secs(10), now),
            "a keepalive should not time out before the configured timeout elapses",
        );
    }

    #[test]
    fn fairness_probe_waits_for_burst_limit() {
        let mut outbound = PriorityQueue::new();
        outbound.push_back(Message::new(1, bytes::Bytes::new()));

        assert!(
            !should_probe_io_fairness(MAX_OUTBOUND_SEND_BURST - 1, &outbound),
            "fairness probe should not run before the burst limit is reached",
        );
        assert!(
            should_probe_io_fairness(MAX_OUTBOUND_SEND_BURST, &outbound),
            "fairness probe should run once the burst limit is reached",
        );
    }

    #[test]
    fn fairness_probe_skips_empty_outbound_queue() {
        let outbound = PriorityQueue::new();

        assert!(
            !should_probe_io_fairness(MAX_OUTBOUND_SEND_BURST, &outbound),
            "fairness probe is only needed when outbound work is queued",
        );
    }
}
