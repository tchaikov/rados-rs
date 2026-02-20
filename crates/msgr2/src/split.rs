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
//! use msgr2::protocol::Connection;
//!
//! # async fn example() -> msgr2::Result<()> {
//! # let addr = "127.0.0.1:6789".parse().unwrap();
//! # let config = msgr2::ConnectionConfig::default();
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

use crate::error::{Error, Result};
use crate::frames::{Frame, MessageFrame, Tag};
use crate::header::MsgHeader;
use crate::message::Message;
use crate::state_machine::create_frame_from_trait;
use crate::throttle::MessageThrottle;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};

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
    /// Queue of sent messages awaiting acknowledgment
    pub sent_messages: VecDeque<Message>,
    /// Whether the connection is lossy
    pub is_lossy: bool,
    /// Global ID from authentication
    pub global_id: u64,
}

impl SharedState {
    /// Record a sent message for potential replay
    pub fn record_sent_message(&mut self, message: Message) {
        if !self.is_lossy {
            self.sent_messages.push_back(message);
        }
    }

    /// Discard acknowledged messages up to the given sequence number
    pub fn discard_acknowledged_messages(&mut self, ack_seq: u64) {
        while let Some(msg) = self.sent_messages.front() {
            if msg.header.get_seq() <= ack_seq {
                self.sent_messages.pop_front();
            } else {
                break;
            }
        }
    }
}

/// Internal command for the I/O task
enum IoCommand {
    /// Send a Ceph message (seq assigned at send time for correct priority ordering)
    SendMessage(Message, tokio::sync::oneshot::Sender<Result<()>>),
    /// Send a pre-built frame directly (used for keepalive, not priority-queued)
    SendFrame(Frame, tokio::sync::oneshot::Sender<Result<()>>),
    Shutdown,
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
    /// Notify when shutdown is requested
    shutdown: Arc<Notify>,
}

/// The receiving half of a split connection
///
/// This half can receive messages concurrently with the sending half.
pub struct RecvHalf {
    /// Channel to receive messages from the I/O task
    msg_rx: mpsc::Receiver<Result<Message>>,
    /// Shared state with SendHalf
    shared: Arc<Mutex<SharedState>>,
    /// Optional message throttle (for ACK handling)
    #[allow(dead_code)] // Kept for future use in reconnection logic
    throttle: Option<MessageThrottle>,
}

impl SendHalf {
    /// Send a Ceph message
    ///
    /// This method is safe to call concurrently with `RecvHalf::recv_message()`.
    /// Sequence numbers are assigned by the I/O task at actual send time so that
    /// priority reordering produces monotonically increasing seq on the wire.
    pub async fn send_message(&self, msg: Message) -> Result<()> {
        let msg_size = msg.total_size() as usize;

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
        use crate::frames::Keepalive2Frame;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let timestamp_sec = now.as_secs() as u32;
        let timestamp_nsec = now.subsec_nanos();

        let keepalive_frame = Keepalive2Frame::new(timestamp_sec, timestamp_nsec);
        let frame = create_frame_from_trait(&keepalive_frame, Tag::Keepalive2);

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
        self.shutdown.notify_one();
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
        // Try to send shutdown command, ignore errors if channel is closed
        let _ = self.cmd_tx.try_send(IoCommand::Shutdown);
    }
}

/// An outbound message entry waiting to be sent, paired with a reply channel.
struct OutboundEntry {
    msg: Message,
    reply: tokio::sync::oneshot::Sender<Result<()>>,
}

/// Priority queue for outbound message entries (high → normal → low).
struct OutboundQueue {
    high: VecDeque<OutboundEntry>,
    normal: VecDeque<OutboundEntry>,
    low: VecDeque<OutboundEntry>,
}

impl OutboundQueue {
    fn new() -> Self {
        Self {
            high: VecDeque::new(),
            normal: VecDeque::new(),
            low: VecDeque::new(),
        }
    }

    fn push(&mut self, entry: OutboundEntry) {
        match entry.msg.priority() {
            crate::message::MessagePriority::High => self.high.push_back(entry),
            crate::message::MessagePriority::Normal => self.normal.push_back(entry),
            crate::message::MessagePriority::Low => self.low.push_back(entry),
        }
    }

    fn pop(&mut self) -> Option<OutboundEntry> {
        self.high
            .pop_front()
            .or_else(|| self.normal.pop_front())
            .or_else(|| self.low.pop_front())
    }
}

/// Assign seq/ack_seq, record in sent_messages, convert to frame, and send.
///
/// Returns `true` on success, `false` on send failure (connection broken).
/// The result is always forwarded to the caller via the reply channel.
async fn send_outbound_entry(
    entry: OutboundEntry,
    connection_state: &mut crate::protocol::ConnectionState,
    shared: &Arc<Mutex<SharedState>>,
) -> bool {
    let OutboundEntry { mut msg, reply } = entry;

    // Assign sequence numbers under lock
    let (seq, ack_seq) = {
        let mut state = shared.lock().await;
        state.out_seq += 1;
        msg.header.set_seq(state.out_seq);
        msg.header.set_ack_seq(state.in_seq);
        state.record_sent_message(msg.clone());
        (state.out_seq, state.in_seq)
    };

    tracing::debug!(
        "I/O task: sending message type=0x{:04x}, seq={}, ack_seq={}",
        msg.msg_type(),
        seq,
        ack_seq
    );

    // Convert to frame and send (move fields — msg is not used after this)
    let msg_frame = MessageFrame::new(msg.header, msg.front, msg.middle, msg.data);
    let frame = create_frame_from_trait(&msg_frame, Tag::Message);
    let result = connection_state.send_frame(&frame).await;
    let success = result.is_ok();
    let _ = reply.send(result);
    success
}

/// Internal I/O task that handles frame send/receive with priority ordering.
///
/// Messages submitted via `IoCommand::SendMessage` are buffered in a priority
/// queue and sent highest-priority-first. Sequence numbers are assigned at
/// actual send time so the wire order matches the sequence order.
async fn io_task(
    mut connection_state: crate::protocol::ConnectionState,
    mut cmd_rx: mpsc::Receiver<IoCommand>,
    msg_tx: mpsc::Sender<Result<Message>>,
    shared: Arc<Mutex<SharedState>>,
    throttle: Option<MessageThrottle>,
    shutdown: Arc<Notify>,
) {
    let mut outbound = OutboundQueue::new();

    'outer: loop {
        // Phase 1: Drain all pending commands into the priority queue (non-blocking)
        loop {
            match cmd_rx.try_recv() {
                Ok(IoCommand::SendMessage(msg, reply)) => {
                    outbound.push(OutboundEntry { msg, reply });
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
                Ok(IoCommand::Shutdown) => {
                    tracing::debug!("I/O task: shutdown command received");
                    return;
                }
                Err(_) => break, // Channel empty or closed
            }
        }

        // Phase 2: Send the highest-priority queued message
        if let Some(entry) = outbound.pop() {
            if !send_outbound_entry(entry, &mut connection_state, &shared).await {
                tracing::error!("I/O task: send error, shutting down");
                break;
            }
            continue; // Loop back to drain more before blocking
        }

        // Phase 3: Nothing pending — block in select!
        tokio::select! {
            _ = shutdown.notified() => {
                tracing::debug!("I/O task: shutdown requested");
                break;
            }

            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(IoCommand::SendMessage(msg, reply)) => {
                        outbound.push(OutboundEntry { msg, reply });
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
                    Some(IoCommand::Shutdown) | None => {
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
                                // Handle ACK frame
                                if let Some(payload) = frame.segments.first() {
                                    if payload.len() >= 8 {
                                        let ack_seq = u64::from_le_bytes([
                                            payload[0], payload[1], payload[2], payload[3],
                                            payload[4], payload[5], payload[6], payload[7],
                                        ]);

                                        tracing::debug!("I/O task: received ACK seq={}", ack_seq);

                                        // Update shared state
                                        {
                                            let mut state = shared.lock().await;
                                            state.discard_acknowledged_messages(ack_seq);
                                        }

                                        // Record ACK with throttle
                                        if let Some(ref throttle) = throttle {
                                            throttle.record_ack().await;
                                        }
                                    }
                                }
                                // Don't send ACK to message channel, continue loop
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
                                tracing::trace!("I/O task: received Keepalive2Ack");
                                // Don't send to message channel
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
    if frame.segments.is_empty() {
        return Err(Error::protocol_error("Message frame missing header"));
    }

    let mut header_buf = frame.segments[0].clone();
    let header = MsgHeader::decode(&mut header_buf)?;

    let front = frame.segments.get(1).cloned().unwrap_or_else(Bytes::new);
    let middle = frame.segments.get(2).cloned().unwrap_or_else(Bytes::new);
    let data = frame.segments.get(3).cloned().unwrap_or_else(Bytes::new);

    let msg_seq = header.get_seq();
    let ack_seq = header.get_ack_seq();

    let msg = Message {
        header,
        front,
        middle,
        data,
        footer: None,
    };

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
    pub(crate) connection_state: crate::protocol::ConnectionState,
    pub(crate) shared: SharedState,
    pub(crate) throttle: Option<MessageThrottle>,
}

impl SplitBuilder {
    /// Build the SendHalf and RecvHalf, spawning the I/O task
    pub fn build(self) -> (SendHalf, RecvHalf) {
        let shared = Arc::new(Mutex::new(self.shared));
        let shutdown = Arc::new(Notify::new());

        // Create channels
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (msg_tx, msg_rx) = mpsc::channel(32);

        // Spawn I/O task
        let io_shared = shared.clone();
        let io_shutdown = shutdown.clone();
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
            shutdown,
        };

        let recv_half = RecvHalf {
            msg_rx,
            shared,
            throttle: self.throttle,
        };

        (send_half, recv_half)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_state_ack_handling() {
        let mut shared = SharedState {
            out_seq: 5,
            in_seq: 3,
            client_cookie: 123,
            server_cookie: 456,
            global_seq: 1,
            connect_seq: 0,
            sent_messages: VecDeque::new(),
            is_lossy: false,
            global_id: 100,
        };

        // Create some test messages with proper headers
        for i in 1..=5u64 {
            let mut msg = Message {
                header: MsgHeader::new_default(0, 0), // msg_type=0, priority=0
                front: Bytes::new(),
                middle: Bytes::new(),
                data: Bytes::new(),
                footer: None,
            };
            msg.header.set_seq(i);
            shared.record_sent_message(msg);
        }

        assert_eq!(shared.sent_messages.len(), 5);

        // ACK up to seq 3
        shared.discard_acknowledged_messages(3);
        assert_eq!(shared.sent_messages.len(), 2);
        // Use get_seq() to avoid unaligned reference to packed field
        assert_eq!(shared.sent_messages.front().unwrap().header.get_seq(), 4);
    }
}
