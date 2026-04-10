//! Split connection types for concurrent send/receive operations
//!
//! This module provides `SendHalf` and `RecvHalf` types that allow concurrent
//! message sending and receiving on a single msgr2 connection. This is useful
//! for implementing message loops where you need to send messages while also
//! waiting for incoming messages.
//!
//! # Example
//!
//! ```no_run
//! use crate::msgr2::protocol::Connection;
//!
//! # async fn example() -> crate::msgr2::Result<()> {
//! # let addr = "127.0.0.1:6789".parse().unwrap();
//! # let config = crate::msgr2::ConnectionConfig::default();
//! let mut conn = Connection::connect(addr, config).await?;
//! conn.establish_session().await?;
//!
//! // Split the connection
//! let (send_half, mut recv_half) = conn.split();
//!
//! // Spawn a task to receive messages
//! let recv_handle = tokio::spawn(async move {
//!     loop {
//!         match recv_half.recv_message().await {
//!             Ok(msg) => println!("Received: {:?}", msg.msg_type()),
//!             Err(_) => break,
//!         }
//!     }
//! });
//!
//! // Send messages from the main task
//! // send_half.send_message(msg).await?;
//! # Ok(())
//! # }
//! ```

use crate::Denc;
use crate::msgr2::error::{Msgr2Error as Error, Result};
use crate::msgr2::frames::create_frame_from_trait;
use crate::msgr2::frames::{Frame, MessageFrame, Tag};
use crate::msgr2::message::Message;
use crate::msgr2::priority_queue::{Prioritized, PriorityQueue};
use crate::msgr2::throttle::MessageThrottle;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

/// Bounded queue of sent messages kept for potential replay on reconnect.
///
/// Shared by `SessionState` (unsplit connection) and `SharedState` (split halves).
#[derive(Debug, Clone, Default)]
pub struct ReplayQueue {
    messages: VecDeque<Message>,
    max: Option<usize>,
}

impl ReplayQueue {
    pub fn new(max: Option<usize>) -> Self {
        Self {
            messages: VecDeque::new(),
            max,
        }
    }

    /// Return an error if the queue is already at its configured limit.
    pub fn check_limit(&self) -> Result<()> {
        if let Some(limit) = self.max
            && self.messages.len() >= limit
        {
            return Err(Error::Protocol(format!(
                "replay queue limit ({limit}) reached; acknowledge or raise ConnectionConfig::max_replay_queue_len before sending more messages"
            )));
        }
        Ok(())
    }

    /// Push a message into the queue.  Caller must have called `check_limit` first
    /// when the seq has already been assigned (split send path).
    pub fn push(&mut self, message: Message) {
        self.messages.push_back(message);
    }

    /// Check limit and push — convenience for the non-split send path.
    pub fn record(&mut self, message: Message) -> Result<()> {
        self.check_limit()?;
        self.messages.push_back(message);
        Ok(())
    }

    /// Drop all messages whose sequence number is ≤ `ack_seq`.
    pub fn discard_acked(&mut self, ack_seq: u64) {
        while self
            .messages
            .front()
            .is_some_and(|m| m.header.get_seq() <= ack_seq)
        {
            self.messages.pop_front();
        }
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Message> {
        self.messages.iter()
    }
}

/// Shared state between SendHalf and RecvHalf
///
/// This contains state that needs to be accessed by both halves,
/// such as sequence numbers and the sent message queue.
#[derive(Debug)]
pub struct SharedState {
    /// Outgoing message sequence number
    pub out_seq: u64,
    /// Incoming message sequence number
    pub in_seq: u64,
    /// Session cookies for reconnection
    pub client_cookie: u64,
    pub server_cookie: u64,
    pub global_seq: u64,
    pub connect_seq: u64,
    /// Queue of sent messages awaiting acknowledgment (for replay on reconnect)
    pub replay: ReplayQueue,
    /// Global ID from authentication
    pub global_id: u64,
}

impl SharedState {
    /// Push a message into the replay queue.
    pub fn record_sent_message(&mut self, message: Message) -> Result<()> {
        self.replay.record(message)
    }

    /// Discard acknowledged messages up to the given sequence number.
    pub fn discard_acknowledged_messages(&mut self, ack_seq: u64) {
        self.replay.discard_acked(ack_seq);
    }
}

/// Internal command for the I/O task
enum IoCommand {
    /// Send a Ceph message (seq assigned at send time for correct priority ordering)
    SendMessage(Message, tokio::sync::oneshot::Sender<Result<()>>),
    /// Send a pre-built frame directly (used for keepalive, not priority-queued)
    SendFrame(Frame, tokio::sync::oneshot::Sender<Result<()>>),
}

/// The sending half of a split connection
///
/// This half can send messages concurrently with the receiving half.
pub struct SendHalf {
    /// Channel to send commands to the I/O task
    cmd_tx: mpsc::Sender<IoCommand>,
    /// Shared state with RecvHalf
    shared: Arc<Mutex<SharedState>>,
    /// Optional message throttle
    throttle: Option<MessageThrottle>,
    /// CancellationToken to signal the I/O task to stop
    shutdown_token: CancellationToken,
}

/// The receiving half of a split connection
///
/// This half can receive messages concurrently with the sending half.
pub struct RecvHalf {
    /// Channel to receive messages from the I/O task
    msg_rx: mpsc::Receiver<Result<Message>>,
    /// Shared state with SendHalf
    shared: Arc<Mutex<SharedState>>,
}

impl SendHalf {
    /// Send a Ceph message
    ///
    /// This method is safe to call concurrently with `RecvHalf::recv_message()`.
    /// Sequence numbers are assigned by the I/O task at actual send time so that
    /// priority reordering produces monotonically increasing seq on the wire.
    pub async fn send_message(&self, msg: Message) -> Result<()> {
        let msg_size = msg.total_len();

        // Wait for throttle if configured
        if let Some(throttle) = &self.throttle {
            throttle.wait_for_send(msg_size).await;
        }

        // Send the message to the I/O task for seq assignment and framing
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(IoCommand::SendMessage(msg, tx))
            .await
            .map_err(|_| Error::Protocol("I/O task closed".into()))?;

        // Wait for result
        rx.await
            .map_err(|_| Error::Protocol("I/O task dropped response".into()))??;

        // Record send with throttle
        if let Some(throttle) = &self.throttle {
            throttle.record_send(msg_size).await;
        }

        Ok(())
    }

    /// Send a keepalive frame
    pub async fn send_keepalive(&self) -> Result<()> {
        use crate::msgr2::frames::Keepalive2Frame;

        let (timestamp_sec, timestamp_nsec) = crate::msgr2::current_keepalive_timestamp()?;

        let keepalive_frame = Keepalive2Frame::new(timestamp_sec, timestamp_nsec);
        let frame = create_frame_from_trait(&keepalive_frame, Tag::Keepalive2)?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(IoCommand::SendFrame(frame, tx))
            .await
            .map_err(|_| Error::Protocol("I/O task closed".into()))?;

        rx.await
            .map_err(|_| Error::Protocol("I/O task dropped response".into()))?
    }

    /// Request shutdown of the connection
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }

    /// Get access to shared state
    pub async fn shared(&self) -> tokio::sync::MutexGuard<'_, SharedState> {
        self.shared.lock().await
    }
}

impl RecvHalf {
    /// Receive a Ceph message
    ///
    /// This method is safe to call concurrently with `SendHalf::send_message()`.
    /// It handles ACK frames internally and only returns Message frames.
    pub async fn recv_message(&mut self) -> Result<Message> {
        match self.msg_rx.recv().await {
            Some(result) => result,
            None => Err(Error::Protocol("Connection closed".into())),
        }
    }

    /// Get access to shared state
    pub async fn shared(&self) -> tokio::sync::MutexGuard<'_, SharedState> {
        self.shared.lock().await
    }
}

impl Drop for SendHalf {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}

/// An outbound message entry waiting to be sent, paired with a reply channel.
struct OutboundEntry {
    msg: Message,
    reply: tokio::sync::oneshot::Sender<Result<()>>,
}

impl Prioritized for OutboundEntry {
    fn priority(&self) -> crate::msgr2::message::MessagePriority {
        self.msg.priority()
    }
}

enum SendOutcome {
    Continue,
    Disconnect,
}

/// Assign seq/ack_seq, record in sent_messages, convert to frame, and send.
/// The result is always forwarded to the caller via the reply channel.
async fn send_outbound_entry(
    entry: OutboundEntry,
    connection_state: &mut crate::msgr2::protocol::ConnectionState,
    shared: &Arc<Mutex<SharedState>>,
) -> SendOutcome {
    let OutboundEntry { mut msg, reply } = entry;
    let is_lossy = connection_state.is_lossy();

    // Assign sequence numbers under lock
    let (seq, ack_seq) = {
        let mut state = shared.lock().await;
        if !is_lossy && let Err(err) = state.replay.check_limit() {
            let _ = reply.send(Err(err));
            return SendOutcome::Continue;
        }
        state.out_seq += 1;
        msg.header.set_seq(state.out_seq);
        msg.header.set_ack_seq(state.in_seq);
        (state.out_seq, state.in_seq)
    };
    let recorded_msg = (!is_lossy).then(|| msg.clone());

    tracing::debug!(
        "I/O task: sending message type=0x{:04x}, seq={}, ack_seq={}",
        msg.msg_type(),
        seq,
        ack_seq
    );

    // Convert to frame and send (move fields — msg is not used after this)
    let msg_frame = MessageFrame::new(msg.header, msg.front, msg.middle, msg.data);
    let frame = match create_frame_from_trait(&msg_frame, Tag::Message) {
        Ok(f) => f,
        Err(e) => {
            let _ = reply.send(Err(e.into()));
            return SendOutcome::Continue;
        }
    };
    let result = connection_state.send_frame(&frame).await;
    let success = result.is_ok();
    if success && let Some(recorded_msg) = recorded_msg {
        let mut state = shared.lock().await;
        if let Err(err) = state.record_sent_message(recorded_msg) {
            let _ = reply.send(Err(err));
            return SendOutcome::Continue;
        }
    }
    let _ = reply.send(result);
    if success {
        SendOutcome::Continue
    } else {
        SendOutcome::Disconnect
    }
}

/// Internal I/O task that handles frame send/receive with priority ordering.
///
/// Messages submitted via `IoCommand::SendMessage` are buffered in a priority
/// queue and sent highest-priority-first. Sequence numbers are assigned at
/// actual send time so the wire order matches the sequence order.
async fn io_task(
    mut connection_state: crate::msgr2::protocol::ConnectionState,
    mut cmd_rx: mpsc::Receiver<IoCommand>,
    msg_tx: mpsc::Sender<Result<Message>>,
    shared: Arc<Mutex<SharedState>>,
    throttle: Option<MessageThrottle>,
    shutdown_token: CancellationToken,
) {
    let mut outbound: PriorityQueue<OutboundEntry> = PriorityQueue::new();

    'outer: loop {
        // Phase 1: Drain all pending commands into the priority queue (non-blocking)
        loop {
            // Check cancellation on each iteration so a large backlog of queued
            // commands (e.g. keepalives) does not delay shutdown.
            if shutdown_token.is_cancelled() {
                tracing::debug!("I/O task: cancelled during drain loop");
                break 'outer;
            }
            match cmd_rx.try_recv() {
                Ok(IoCommand::SendMessage(msg, reply)) => {
                    outbound.push_back(OutboundEntry { msg, reply });
                }
                Ok(IoCommand::SendFrame(frame, reply)) => {
                    // Non-message frames (keepalive) bypass priority queue
                    let result = connection_state.send_frame(&frame).await;
                    let failed = result.is_err();
                    let _ = reply.send(result);
                    if failed {
                        tracing::error!("I/O task: frame send error, shutting down");
                        break 'outer;
                    }
                }
                Err(_) => break, // Channel empty or closed
            }
        }

        // Phase 2: Send the highest-priority queued message
        if let Some(entry) = outbound.pop_front() {
            if matches!(
                send_outbound_entry(entry, &mut connection_state, &shared).await,
                SendOutcome::Disconnect
            ) {
                tracing::error!("I/O task: send error, shutting down");
                break;
            }
            continue; // Loop back to drain more before blocking
        }

        // Phase 3: Nothing pending — block in select!
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                tracing::debug!("I/O task: shutdown requested");
                break;
            }

            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(IoCommand::SendMessage(msg, reply)) => {
                        outbound.push_back(OutboundEntry { msg, reply });
                        // Don't send yet — loop back to drain any additional
                        // commands that arrived, so we can prioritise correctly
                    }
                    Some(IoCommand::SendFrame(frame, reply)) => {
                        let result = connection_state.send_frame(&frame).await;
                        let failed = result.is_err();
                        let _ = reply.send(result);
                        if failed {
                            tracing::error!("I/O task: frame send error, shutting down");
                            break;
                        }
                    }
                    None => {
                        tracing::debug!("I/O task: command channel closed");
                        break;
                    }
                }
            }

            // Handle incoming frames
            frame_result = connection_state.recv_frame() => {
                match frame_result {
                    Ok(frame) => {
                        match frame.preamble.tag {
                            Tag::Ack => {
                                // Handle ACK frame - decode sequence number
                                if let Some(payload) = frame.segments.first() {
                                    let mut buf = payload.as_ref();
                                    if let Ok(ack_seq) = u64::decode(&mut buf, 0) {
                                        tracing::debug!("I/O task: received ACK seq={}", ack_seq);
                                        shared.lock().await.discard_acknowledged_messages(ack_seq);

                                        if let Some(ref throttle) = throttle {
                                            throttle.record_ack().await;
                                        }
                                    }
                                }
                            }
                            Tag::Message => {
                                // Parse message
                                let msg_result = parse_message_frame(&frame, &shared).await;
                                if msg_tx.send(msg_result).await.is_err() {
                                    tracing::debug!("I/O task: message channel closed");
                                    break;
                                }
                            }
                            Tag::Keepalive2Ack => {
                                connection_state.record_keepalive_ack();
                                tracing::trace!("I/O task: received Keepalive2Ack");
                            }
                            Tag::Keepalive2 => {
                                // Peer is checking our liveness — echo back a Keepalive2Ack
                                // with the same timestamp per msgr2 spec.
                                if let Some(payload) = frame.segments.first() {
                                    use crate::msgr2::frames::{Keepalive2AckFrame, create_frame_from_trait};
                                    let mut buf = payload.as_ref();
                                    if let (Ok(ts_sec), Ok(ts_nsec)) = (
                                        u32::decode(&mut buf, 0),
                                        u32::decode(&mut buf, 0),
                                    ) && let Ok(ack) = create_frame_from_trait(
                                        &Keepalive2AckFrame::new(ts_sec, ts_nsec),
                                        crate::msgr2::frames::Tag::Keepalive2Ack,
                                    ) {
                                        let _ = connection_state.send_frame(&ack).await;
                                    }
                                }
                                tracing::trace!("I/O task: received Keepalive2, sent Keepalive2Ack");
                            }
                            _ => {
                                let err = Error::protocol_error(&format!(
                                    "Unexpected frame type: {:?}",
                                    frame.preamble.tag
                                ));
                                if msg_tx.send(Err(err)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Send error to message channel
                        let _ = msg_tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        }
    }

    tracing::debug!("I/O task: exiting");
}

/// Parse a Message frame and update shared state
async fn parse_message_frame(frame: &Frame, shared: &Arc<Mutex<SharedState>>) -> Result<Message> {
    let msg = Message::from_frame_segments(&frame.segments)?;
    let msg_seq = msg.header.get_seq();
    let ack_seq = msg.header.get_ack_seq();

    tracing::debug!(
        "I/O task: received message type={}, seq={}, ack_seq={}",
        msg.msg_type(),
        msg_seq,
        ack_seq
    );

    // Update shared state
    {
        let mut state = shared.lock().await;
        state.in_seq = msg_seq;
        if ack_seq > 0 {
            state.discard_acknowledged_messages(ack_seq);
        }
    }

    Ok(msg)
}

/// Builder for creating split connection halves
///
/// This is used internally by `Connection::split()`.
pub struct SplitBuilder {
    pub(crate) connection_state: crate::msgr2::protocol::ConnectionState,
    pub(crate) shared: SharedState,
    pub(crate) throttle: Option<MessageThrottle>,
}

impl SplitBuilder {
    /// Build the SendHalf and RecvHalf, spawning the I/O task
    pub fn build(self) -> (SendHalf, RecvHalf) {
        let shared = Arc::new(Mutex::new(self.shared));
        let shutdown_token = CancellationToken::new();

        // Create channels
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (msg_tx, msg_rx) = mpsc::channel(32);

        // Spawn I/O task
        let io_shared = shared.clone();
        let io_shutdown = shutdown_token.clone();
        let io_throttle = self.throttle.clone();
        tokio::spawn(async move {
            io_task(
                self.connection_state,
                cmd_rx,
                msg_tx,
                io_shared,
                io_throttle,
                io_shutdown,
            )
            .await;
        });

        let send_half = SendHalf {
            cmd_tx,
            shared: shared.clone(),
            throttle: self.throttle.clone(),
            shutdown_token,
        };

        let recv_half = RecvHalf { msg_rx, shared };

        (send_half, recv_half)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_shared_state_ack_handling() {
        let mut shared = SharedState {
            out_seq: 5,
            in_seq: 3,
            client_cookie: 123,
            server_cookie: 456,
            global_seq: 1,
            connect_seq: 0,
            replay: ReplayQueue::new(None),
            global_id: 100,
        };

        // Create some test messages with proper headers
        for i in 1..=5u64 {
            let msg = Message::new(0, Bytes::new()).with_seq(i);
            shared.record_sent_message(msg).unwrap();
        }

        assert_eq!(shared.replay.len(), 5);

        // ACK up to seq 3
        shared.discard_acknowledged_messages(3);
        assert_eq!(shared.replay.len(), 2);
        // Use get_seq() to avoid unaligned reference to packed field
        assert_eq!(shared.replay.iter().next().unwrap().header.get_seq(), 4);
    }
}
