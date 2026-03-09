//! msgr2 Protocol V2 implementation
//!
//! This module provides a high-level async API for the msgr2 protocol,
//! handling frame I/O, encryption, and state machine coordination.

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::banner::Banner;
use crate::error::{Msgr2Error as Error, Result};
use crate::frames::{Frame, MessageFrame, Preamble, Tag};
use crate::header::MsgHeader;
use crate::message::Message;
use crate::state_machine::{create_frame_from_trait, StateKind, StateMachine};
use crate::FeatureSet;
use denc::Denc;
use std::borrow::Cow;

/// Zero padding buffer — avoids heap allocation for small alignment padding.
/// Large enough to cover INLINE_SIZE (48) padding in one slice.
const ZEROS: [u8; 64] = [0u8; 64];

/// Priority-based message queue.
///
/// Higher-priority messages are sent first, matching Ceph's ProtocolV2 behaviour.
/// Within a priority level, ordering is FIFO.
///
/// Intended for the **outgoing** queue (deciding what to send next). Do not use
/// for `sent_messages` (ACK replay buffer), which must preserve insertion/sequence
/// order for correct `discard_acknowledged_messages()` processing.
#[derive(Debug, Clone, Default)]
pub struct PriorityQueue {
    high: VecDeque<Message>,
    normal: VecDeque<Message>,
    low: VecDeque<Message>,
}

impl PriorityQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a message into the appropriate priority sub-queue.
    pub fn push_back(&mut self, msg: Message) {
        match msg.priority() {
            crate::message::MessagePriority::High => self.high.push_back(msg),
            crate::message::MessagePriority::Normal => self.normal.push_back(msg),
            crate::message::MessagePriority::Low => self.low.push_back(msg),
        }
    }

    /// Pop the highest-priority message available.
    pub fn pop_front(&mut self) -> Option<Message> {
        self.high
            .pop_front()
            .or_else(|| self.normal.pop_front())
            .or_else(|| self.low.pop_front())
    }

    /// Peek at the next message to be sent (highest-priority queue's front).
    pub fn front(&self) -> Option<&Message> {
        self.high
            .front()
            .or_else(|| self.normal.front())
            .or_else(|| self.low.front())
    }

    /// Iterate over all messages in priority order (high → normal → low).
    pub fn iter(&self) -> impl Iterator<Item = &Message> {
        self.high
            .iter()
            .chain(self.normal.iter())
            .chain(self.low.iter())
    }

    /// Clear all queues.
    pub fn clear(&mut self) {
        self.high.clear();
        self.normal.clear();
        self.low.clear();
    }

    /// Total number of queued messages across all priority levels.
    pub fn len(&self) -> usize {
        self.high.len() + self.normal.len() + self.low.len()
    }

    /// Returns `true` if all priority queues are empty.
    pub fn is_empty(&self) -> bool {
        self.high.is_empty() && self.normal.is_empty() && self.low.is_empty()
    }
}

/// Action to take after successful reconnection in retry_with_reconnect
enum ReconnectAction<T> {
    /// Return success immediately with the given value
    ReturnSuccess(T),
    /// Continue retrying the operation
    Retry,
}

/// Frame I/O layer - handles encryption-aware frame send/recv
pub(crate) struct FrameIO {
    stream: TcpStream,
}

impl FrameIO {
    const PREAMBLE_SIZE: usize = 32;
    const INLINE_SIZE: usize = 48;
    const GCM_TAG_SIZE: usize = 16;

    /// Create a new FrameIO from an existing TCP stream
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    fn normalized_num_segments(frame: &Frame) -> usize {
        let mut num_segments = frame.segments.len();
        while num_segments > 1 && frame.segments[num_segments - 1].is_empty() {
            num_segments -= 1;
        }
        num_segments
    }

    fn build_secure_preamble(frame: &Frame, num_segments: usize) -> Result<Bytes> {
        let mut preamble = Preamble {
            tag: frame.preamble.tag,
            num_segments: num_segments as u8,
            segments: [crate::frames::SegmentDescriptor::default();
                crate::frames::MAX_NUM_SEGMENTS],
            flags: frame.preamble.flags,
            reserved: 0,
            crc: 0,
        };

        for (index, segment) in frame.segments.iter().take(num_segments).enumerate() {
            preamble.segments[index] = crate::frames::SegmentDescriptor {
                logical_len: segment.len() as u32,
                align: frame.preamble.segments[index].align,
            };
        }

        Ok(preamble.encode()?)
    }

    fn build_secure_payload(frame: &Frame, num_segments: usize) -> BytesMut {
        use crate::frames::FRAME_LATE_STATUS_COMPLETE;

        let total_len = frame
            .segments
            .iter()
            .take(num_segments)
            .filter(|segment| !segment.is_empty())
            .map(|segment| {
                (segment.len() + crate::frames::CRYPTO_BLOCK_SIZE - 1)
                    & !(crate::frames::CRYPTO_BLOCK_SIZE - 1)
            })
            .sum::<usize>()
            + if num_segments > 1 {
                crate::frames::CRYPTO_BLOCK_SIZE
            } else {
                0
            };

        let mut payload_bytes = BytesMut::with_capacity(total_len);
        for segment in frame.segments.iter().take(num_segments) {
            let segment_len = segment.len();
            if segment_len == 0 {
                continue;
            }

            payload_bytes.extend_from_slice(segment);
            let aligned_len = (segment_len + crate::frames::CRYPTO_BLOCK_SIZE - 1)
                & !(crate::frames::CRYPTO_BLOCK_SIZE - 1);
            let padding_needed = aligned_len - segment_len;
            if padding_needed > 0 {
                payload_bytes.extend_from_slice(&ZEROS[..padding_needed]);
            }
        }

        if num_segments > 1 {
            payload_bytes.extend_from_slice(&[FRAME_LATE_STATUS_COMPLETE]);
            payload_bytes.extend_from_slice(&ZEROS[..crate::frames::CRYPTO_BLOCK_SIZE - 1]);
        }

        payload_bytes
    }

    /// Send a frame with optional compression and encryption
    ///
    /// Processing order: compression → encryption
    /// - If compression is enabled, compress frame segments first
    /// - If encryption is enabled, encrypt the (possibly compressed) frame
    ///
    /// If encryption is enabled in the state machine, this implements msgr2.1 inline optimization:
    /// - First 48 bytes of payload are encrypted together with preamble (32+48=80 → 96 bytes with GCM tag)
    /// - Remaining payload bytes are encrypted separately (N bytes → N+16 bytes with GCM tag)
    pub async fn send_frame(
        &mut self,
        frame: &Frame,
        state_machine: &mut StateMachine,
    ) -> Result<()> {
        // Step 1: Apply compression if enabled
        // Use Cow to avoid cloning in the common case where compression fails/disabled
        let frame_to_send: Cow<Frame> = if let Some(ctx) = state_machine.compression_ctx() {
            match frame.compress(ctx) {
                Ok(compressed_frame) if compressed_frame.preamble.is_compressed() => {
                    tracing::debug!(
                        "Frame compressed: tag={:?}, algorithm={:?}",
                        frame.preamble.tag,
                        ctx.algorithm()
                    );
                    Cow::Owned(compressed_frame)
                }
                Ok(_) => {
                    // Compression didn't help or failed, borrow original
                    Cow::Borrowed(frame)
                }
                Err(e) => {
                    tracing::warn!("Compression failed, sending uncompressed: {:?}", e);
                    Cow::Borrowed(frame)
                }
            }
        } else {
            Cow::Borrowed(frame)
        };

        // Step 2: Apply encryption if enabled
        let is_compressed = frame_to_send.preamble.is_compressed();
        let final_bytes = if !state_machine.has_encryption() {
            // Convert to wire format with proper preamble, segments, epilogue, and CRCs.
            // Use msgr2.1 (is_rev1 = true) to match the banner we sent.
            let wire_bytes = frame_to_send.to_wire(true)?;
            tracing::debug!(
                "Sending frame: tag={:?}, {} segments, {} wire bytes, encrypted=false, compressed={}",
                frame_to_send.preamble.tag,
                frame_to_send.preamble.num_segments,
                wire_bytes.len(),
                is_compressed,
            );
            wire_bytes
        } else {
            let num_segments = Self::normalized_num_segments(&frame_to_send);
            let preamble_bytes = Self::build_secure_preamble(&frame_to_send, num_segments)?;
            let payload_bytes = Self::build_secure_payload(&frame_to_send, num_segments);

            tracing::debug!(
                "Sending frame: tag={:?}, {} segments, secure payload {} bytes, compressed={}",
                frame_to_send.preamble.tag,
                num_segments,
                payload_bytes.len(),
                is_compressed,
            );

            let inline_size = payload_bytes.len().min(Self::INLINE_SIZE);
            let remaining_size = payload_bytes.len().saturating_sub(Self::INLINE_SIZE);

            // Build preamble block (preamble + inline data + padding if needed)
            let mut preamble_block =
                BytesMut::with_capacity(Self::PREAMBLE_SIZE + Self::INLINE_SIZE);
            preamble_block.extend_from_slice(&preamble_bytes);
            preamble_block.extend_from_slice(&payload_bytes[..inline_size]);

            // Pad to INLINE_SIZE if inline data is shorter
            if inline_size < Self::INLINE_SIZE {
                preamble_block.extend_from_slice(&ZEROS[..Self::INLINE_SIZE - inline_size]);
            }

            // Encrypt preamble block (32 + 48 = 80 bytes -> 96 bytes with GCM tag)
            let encrypted_preamble = state_machine.encrypt_frame_data(&preamble_block)?;

            let mut result =
                BytesMut::with_capacity(encrypted_preamble.len() + remaining_size + 16);
            result.extend_from_slice(&encrypted_preamble);

            // Encrypt remaining payload if any (starts after first 48 bytes)
            if remaining_size > 0 {
                let remaining_data = &payload_bytes[Self::INLINE_SIZE..];
                let encrypted_remaining = state_machine.encrypt_frame_data(remaining_data)?;
                result.extend_from_slice(&encrypted_remaining);
            }

            tracing::trace!(
                "Encrypted frame: {} bytes (preamble_block={}, remaining={})",
                result.len(),
                encrypted_preamble.len(),
                if remaining_size > 0 {
                    remaining_size + 16
                } else {
                    0
                }
            );

            result.freeze()
        };

        // Record data for AUTH_SIGNATURE computation (after encryption)
        state_machine.record_sent(&final_bytes);
        tracing::trace!(
            "Recorded sent frame tag={:?}, {} bytes",
            frame.preamble.tag,
            final_bytes.len()
        );

        self.stream.write_all(&final_bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Receive a frame with optional decryption and decompression
    ///
    /// Processing order: decryption → decompression
    /// - If encryption is enabled, decrypt the frame first
    /// - If FRAME_EARLY_DATA_COMPRESSED flag is set, decompress the frame
    ///
    /// If encryption is enabled, this handles msgr2.1 inline optimization:
    /// - Reads 96 bytes (preamble + inline + GCM tag) and decrypts to 80 bytes
    /// - Reads remaining encrypted payload if needed
    pub async fn recv_frame(&mut self, state_machine: &mut StateMachine) -> Result<Frame> {
        let has_encryption = state_machine.has_encryption();

        // Read preamble block
        let preamble_block_size = if has_encryption {
            Self::PREAMBLE_SIZE + Self::INLINE_SIZE + Self::GCM_TAG_SIZE // 96 bytes
        } else {
            Self::PREAMBLE_SIZE // 32 bytes
        };

        let mut preamble_block_buf = vec![0u8; preamble_block_size];
        self.stream.read_exact(&mut preamble_block_buf).await?;

        // Record data for AUTH_SIGNATURE computation (before decryption)
        state_machine.record_received(&preamble_block_buf);

        // Decrypt preamble block if encrypted
        let preamble_and_inline = if has_encryption {
            let decrypted = state_machine.decrypt_frame_data(&preamble_block_buf)?;
            tracing::debug!(
                "Decrypted preamble block: {} bytes → {} bytes",
                preamble_block_buf.len(),
                decrypted.len()
            );
            decrypted
        } else {
            Bytes::from(preamble_block_buf)
        };

        // Extract preamble (first 32 bytes)
        let preamble_bytes = preamble_and_inline.slice(..Self::PREAMBLE_SIZE);
        let inline_data = if has_encryption {
            preamble_and_inline.slice(Self::PREAMBLE_SIZE..Self::PREAMBLE_SIZE + Self::INLINE_SIZE)
        } else {
            Bytes::new()
        };

        // Parse preamble
        let preamble = Preamble::decode(preamble_bytes.clone())?;

        tracing::debug!(
            "Received preamble: tag={:?}, num_segments={}, segments={:?}",
            preamble.tag,
            preamble.num_segments,
            &preamble.segments[0..preamble.num_segments as usize]
        );

        // Calculate total segment size
        let total_segment_size: usize = preamble.segments[..preamble.num_segments as usize]
            .iter()
            .map(|s| s.logical_len as usize)
            .sum();

        // Calculate total payload size based on encryption mode and number of segments
        const CRYPTO_BLOCK_SIZE: usize = 16;
        let total_payload_size = if has_encryption && total_segment_size > 0 {
            if preamble.num_segments == 1 {
                // Secure mode single segment: align to 16-byte boundary
                (total_segment_size + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1)
            } else {
                // Secure mode multi-segment: pad each segment + add epilogue
                let mut padded_size = 0;
                for i in 0..preamble.num_segments as usize {
                    let seg_len = preamble.segments[i].logical_len as usize;
                    if seg_len > 0 {
                        let aligned = (seg_len + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1);
                        padded_size += aligned;
                    }
                }
                padded_size + CRYPTO_BLOCK_SIZE // epilogue
            }
        } else {
            // Plaintext (CRC mode)
            if total_segment_size == 0 {
                0
            } else if preamble.num_segments == 1 {
                // Single segment (rev1): segment + 4-byte CRC inline, no epilogue
                total_segment_size + crate::frames::FRAME_CRC_SIZE
            } else {
                // Multi-segment: 17 extra bytes total
                // Rev1: CRC(4) after seg0 + epilogue_rev1(1 + 3×4 = 13) = 17
                // Rev0: epilogue_rev0(1 + 4×4 = 17) = 17
                total_segment_size
                    + crate::frames::FRAME_CRC_SIZE
                    + 1
                    + (crate::frames::MAX_NUM_SEGMENTS - 1) * crate::frames::FRAME_CRC_SIZE
            }
        };

        if total_payload_size == 0 {
            tracing::debug!("Empty frame, returning immediately");
            return Ok(Frame {
                preamble,
                segments: vec![],
            });
        }

        // Read remaining segment data if any
        let remaining_needed = if has_encryption {
            // We already have first 48 bytes inline
            if total_payload_size > Self::INLINE_SIZE {
                let remaining_plaintext = total_payload_size - Self::INLINE_SIZE;
                remaining_plaintext + Self::GCM_TAG_SIZE
            } else {
                0
            }
        } else {
            total_payload_size
        };

        let full_payload = if remaining_needed > 0 {
            let mut remaining_buf = vec![0u8; remaining_needed];
            self.stream.read_exact(&mut remaining_buf).await?;

            // Record data for AUTH_SIGNATURE computation (before decryption)
            state_machine.record_received(&remaining_buf);

            if has_encryption {
                // Decrypt remaining data
                let decrypted_remaining = state_machine.decrypt_frame_data(&remaining_buf)?;

                // Combine inline + decrypted remaining
                let mut combined =
                    BytesMut::with_capacity(Self::INLINE_SIZE + decrypted_remaining.len());
                combined.extend_from_slice(&inline_data);
                combined.extend_from_slice(&decrypted_remaining);
                combined.freeze()
            } else {
                Bytes::from(remaining_buf)
            }
        } else if has_encryption {
            // All data was inline
            inline_data.slice(..total_payload_size)
        } else {
            Bytes::new()
        };

        // Parse segments from the full payload
        let is_rev1 = state_machine.is_rev1();
        let is_multi = preamble.num_segments > 1;
        let mut segments = Vec::new();
        let mut offset = 0;
        for i in 0..preamble.num_segments as usize {
            let segment_len = preamble.segments[i].logical_len as usize;
            if segment_len > 0 {
                // Extract only the logical length of data (not the padding)
                let segment_data = full_payload.slice(offset..offset + segment_len);
                segments.push(segment_data.clone());

                if has_encryption {
                    // Secure frames: skip 16-byte alignment padding
                    let aligned_len =
                        (segment_len + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1);
                    offset += aligned_len;
                } else {
                    offset += segment_len;
                    // Plaintext rev1: 4-byte CRC follows segment 0 (both single and multi-segment)
                    if is_rev1 && i == 0 {
                        if let Some(crc_bytes) = full_payload
                            .get(offset..offset + crate::frames::FRAME_CRC_SIZE)
                            .and_then(|s| <[u8; 4]>::try_from(s).ok())
                        {
                            let received_crc = u32::from_le_bytes(crc_bytes);
                            let expected_crc = !crc32c::crc32c(&segment_data);
                            if received_crc != expected_crc {
                                return Err(Error::Protocol(format!(
                                    "Segment 0 CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                                    expected_crc, received_crc
                                )));
                            }
                        }
                        offset += crate::frames::FRAME_CRC_SIZE;
                    }
                }
            } else {
                // Empty segment - still add it to maintain segment indices
                segments.push(Bytes::new());
            }
        }

        // Verify epilogue for plaintext multi-segment frames
        if !has_encryption && is_multi && total_segment_size > 0 {
            // Epilogue: first byte is late_status (rev1) or late_flags (rev0)
            if offset < full_payload.len() {
                let late_status = full_payload[offset];
                if (late_status & crate::frames::FRAME_LATE_STATUS_ABORTED_MASK)
                    == crate::frames::FRAME_LATE_STATUS_ABORTED
                {
                    return Err(crate::Msgr2Error::Protocol(
                        "Frame transmission aborted by sender".to_string(),
                    ));
                }
                offset += 1;

                // Verify epilogue segment CRCs.
                // Rev1: 3 CRCs for segments 1-3 (segment 0 was verified inline above).
                //   Stored as !crc32c(segment_data).
                // Rev0: 4 CRCs for segments 0-3.
                //   Stored as crc32c(segment_data).
                let (crc_start_seg, num_epilogue_crcs, invert) = if is_rev1 {
                    (1usize, crate::frames::MAX_NUM_SEGMENTS - 1, true)
                } else {
                    (0usize, crate::frames::MAX_NUM_SEGMENTS, false)
                };
                for seg_idx in crc_start_seg..crc_start_seg + num_epilogue_crcs {
                    let Some(crc_bytes) = full_payload
                        .get(offset..offset + crate::frames::FRAME_CRC_SIZE)
                        .and_then(|s| <[u8; 4]>::try_from(s).ok())
                    else {
                        break;
                    };
                    let received_crc = u32::from_le_bytes(crc_bytes);
                    offset += crate::frames::FRAME_CRC_SIZE;

                    if seg_idx < segments.len() && !segments[seg_idx].is_empty() {
                        let computed = crc32c::crc32c(&segments[seg_idx]);
                        let expected_crc = if invert { !computed } else { computed };
                        if received_crc != expected_crc {
                            return Err(Error::Protocol(format!(
                                "Segment {} CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                                seg_idx, expected_crc, received_crc
                            )));
                        }
                    }
                }
            }
        }

        let mut frame = Frame { preamble, segments };

        // Apply decompression if frame is compressed
        if frame.preamble.is_compressed() {
            if let Some(ctx) = state_machine.compression_ctx() {
                frame = frame.decompress(ctx).map_err(|e| {
                    Error::protocol_error(&format!("Decompression failed: {:?}", e))
                })?;
                tracing::debug!(
                    "Frame decompressed: tag={:?}, algorithm={:?}",
                    frame.preamble.tag,
                    ctx.algorithm()
                );
            } else {
                tracing::warn!(
                    "Frame has FRAME_EARLY_DATA_COMPRESSED flag but no compression context"
                );
            }
        }

        Ok(frame)
    }
}

/// Connection state coordination layer
pub(crate) struct ConnectionState {
    state_machine: StateMachine,
    frame_io: FrameIO,
    /// Outgoing message sequence number (incremented for each message sent)
    out_seq: u64,
    /// Incoming message sequence number (last received from peer)
    in_seq: u64,
    /// Session cookies for reconnection
    session: SessionState,
}

/// Session state preserved across reconnections
#[derive(Debug, Clone)]
struct SessionState {
    /// Client cookie - generated on first connection, identifies this client instance
    client_cookie: u64,
    /// Server cookie - assigned by server, identifies the session at server side
    server_cookie: u64,
    /// Global sequence number - monotonically increasing across all connections
    global_seq: u64,
    /// Connection sequence number - incremented on each reconnection attempt
    connect_seq: u64,
    /// Queue of sent messages awaiting acknowledgment (for replay on reconnect)
    sent_messages: std::collections::VecDeque<Message>,
    /// Optional cap on the replay queue size
    max_sent_messages: Option<usize>,
    /// Connection policy - if true, connection is lossy (no reconnection support)
    is_lossy: bool,
}

#[allow(dead_code)]
impl ConnectionState {
    /// Create a new connection state with the given stream and state machine
    fn new(stream: TcpStream, state_machine: StateMachine) -> Self {
        let frame_io = FrameIO::new(stream);

        // Generate a random client cookie for this connection instance
        let client_cookie = rand::random::<u64>();

        Self {
            state_machine,
            frame_io,
            out_seq: 0, // Starts at 0, first message will get seq=1
            in_seq: 0,
            session: SessionState {
                client_cookie,
                server_cookie: 0, // Will be assigned by server
                global_seq: 0,
                connect_seq: 0,
                sent_messages: std::collections::VecDeque::new(),
                max_sent_messages: None,
                is_lossy: false, // Default to lossless (reconnectable)
            },
        }
    }

    /// Send a frame through the state machine and frame I/O
    pub(crate) async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        self.frame_io
            .send_frame(frame, &mut self.state_machine)
            .await
    }

    /// Receive a frame through the frame I/O and state machine
    pub(crate) async fn recv_frame(&mut self) -> Result<Frame> {
        self.frame_io.recv_frame(&mut self.state_machine).await
    }

    /// Drive a phase to completion, handling the enter / recv / send loop.
    pub(crate) async fn drive_phase<P: crate::phase::Phase>(
        &mut self,
        phase: P,
    ) -> Result<P::Output> {
        crate::phase::drive(&mut self.frame_io, &mut self.state_machine, phase).await
    }

    /// Get the current state name
    fn current_state_name(&self) -> &str {
        self.state_machine.current_state_name()
    }

    /// Get the current state kind
    fn current_state_kind(&self) -> StateKind {
        self.state_machine.current_state_kind()
    }

    // Session state management methods

    /// Get the client cookie for this connection
    pub(crate) fn client_cookie(&self) -> u64 {
        self.session.client_cookie
    }

    /// Get the server cookie (0 if not yet assigned)
    pub(crate) fn server_cookie(&self) -> u64 {
        self.session.server_cookie
    }

    /// Set the server cookie (called when SERVER_IDENT is received)
    fn set_server_cookie(&mut self, cookie: u64) {
        self.session.server_cookie = cookie;
    }

    /// Check if this connection has a valid session that can be reconnected
    pub(crate) fn can_reconnect(&self) -> bool {
        !self.session.is_lossy && self.session.server_cookie != 0
    }

    /// Get the current global sequence number
    fn global_seq(&self) -> u64 {
        self.session.global_seq
    }

    /// Set the global sequence number
    fn set_global_seq(&mut self, seq: u64) {
        self.session.global_seq = seq;
    }

    /// Get the current connection sequence number
    fn connect_seq(&self) -> u64 {
        self.session.connect_seq
    }

    /// Increment the connection sequence number (for reconnection attempts)
    fn increment_connect_seq(&mut self) {
        self.session.connect_seq += 1;
    }

    /// Get the last received message sequence number
    fn in_seq(&self) -> u64 {
        self.in_seq
    }

    /// Set the incoming message sequence number
    pub(crate) fn set_in_seq(&mut self, seq: u64) {
        self.in_seq = seq;
    }

    /// Record a sent message for potential replay
    pub(crate) fn record_sent_message(&mut self, message: Message) -> Result<()> {
        if self.session.is_lossy {
            return Ok(());
        }
        if let Some(limit) = self.session.max_sent_messages {
            if self.session.sent_messages.len() >= limit {
                return Err(Error::Protocol(format!(
                    "replay queue limit ({limit}) reached; acknowledge or raise ConnectionConfig::max_replay_queue_len before sending more messages"
                )));
            }
        }
        self.session.sent_messages.push_back(message);
        Ok(())
    }

    /// Discard acknowledged messages up to the given sequence number
    pub(crate) fn discard_acknowledged_messages(&mut self, ack_seq: u64) {
        while let Some(msg) = self.session.sent_messages.front() {
            if msg.header.get_seq() <= ack_seq {
                self.session.sent_messages.pop_front();
            } else {
                break;
            }
        }
    }

    /// Get all unacknowledged messages for replay
    pub(crate) fn get_unacknowledged_messages(&self) -> &std::collections::VecDeque<Message> {
        &self.session.sent_messages
    }

    /// Clear the sent message queue
    pub(crate) fn clear_sent_messages(&mut self) {
        self.session.sent_messages.clear();
    }

    /// Reset session state for a new connection
    pub(crate) fn reset_session(&mut self) {
        self.session.server_cookie = 0;
        self.session.global_seq = 0;
        self.session.connect_seq = 0;
        self.session.sent_messages.clear();
        self.in_seq = 0;
        self.out_seq = 0;
        // Note: client_cookie is preserved across resets
    }
}

/// High-level msgr2 Protocol V2 connection
///
/// This provides an async API for establishing msgr2 connections,
/// handling authentication, and exchanging messages.
pub struct Connection {
    state: ConnectionState,
    /// Server address for reconnection
    server_addr: SocketAddr,
    /// Target entity address (for connect_with_target), if applicable
    target_entity_addr: Option<denc::EntityAddr>,
    /// Connection configuration
    config: crate::ConnectionConfig,
    /// Optional message throttle for rate limiting
    throttle: Option<crate::throttle::MessageThrottle>,
}

/// Snapshot of runtime connection state for diagnostics and debugging.
///
/// Obtained via [`Connection::diagnostics`]. All fields reflect the state at
/// the moment the snapshot is taken; they are not updated live.
#[derive(Debug, Clone)]
pub struct ConnectionDiagnostics {
    /// Current state-machine phase
    pub state_kind: crate::state_machine::StateKind,
    /// Whether frames are encrypted (secure mode)
    pub encryption_enabled: bool,
    /// Active compression algorithm, or `None` if compression is not in use
    pub compression_algorithm: Option<crate::compression::CompressionAlgorithm>,
    /// Outgoing sequence number (number of messages sent so far)
    pub out_seq: u64,
    /// Incoming sequence number (last acknowledged from peer)
    pub in_seq: u64,
    /// Number of sent messages awaiting acknowledgment (for reconnect replay)
    pub sent_messages_pending: usize,
    /// Client cookie generated at connection creation
    pub client_cookie: u64,
    /// Server cookie assigned by the peer (0 if session not yet established)
    pub server_cookie: u64,
    /// Global sequence number (monotonically increasing across reconnections)
    pub global_seq: u64,
    /// Connection sequence number (incremented on each reconnect attempt)
    pub connect_seq: u64,
    /// Global ID assigned by the monitor during CephX authentication (0 if not authenticated)
    pub global_id: u64,
    /// Timestamp of the most recent keepalive acknowledgment received
    pub last_keepalive_ack: Option<std::time::Instant>,
    /// Whether the connection policy is lossy (no reconnection support)
    pub is_lossy: bool,
    /// Whether a transparent reconnect is possible (non-lossy + server cookie present)
    pub can_reconnect: bool,
}

impl Connection {
    /// Convert a SocketAddr to an EntityAddr in sockaddr_storage format
    ///
    /// This helper function converts Rust's SocketAddr type to Ceph's EntityAddr
    /// format, which uses the sockaddr_storage binary representation.
    ///
    /// # Format
    /// - IPv4: ss_family (2 bytes LE) + port (2 bytes BE) + IP (4 bytes) + padding (8 bytes)
    /// - IPv6: ss_family (2 bytes LE) + port (2 bytes BE) + flowinfo (4 bytes BE) + IP (16 bytes) + scope_id (4 bytes BE)
    fn socket_to_entity_addr(addr: SocketAddr) -> denc::EntityAddr {
        let mut entity_addr = denc::EntityAddr::new();
        entity_addr.addr_type = denc::EntityAddrType::Msgr2;

        match addr {
            SocketAddr::V4(v4) => {
                let mut data = Vec::with_capacity(16);
                data.extend_from_slice(&2u16.to_le_bytes()); // AF_INET = 2
                data.extend_from_slice(&v4.port().to_be_bytes()); // port in network byte order
                data.extend_from_slice(&v4.ip().octets()); // IP address
                data.extend_from_slice(&[0u8; 8]); // padding
                entity_addr.sockaddr_data = data;
            }
            SocketAddr::V6(v6) => {
                let mut data = Vec::with_capacity(28);
                data.extend_from_slice(&10u16.to_le_bytes()); // AF_INET6 = 10
                data.extend_from_slice(&v6.port().to_be_bytes());
                data.extend_from_slice(&0u32.to_be_bytes()); // flowinfo
                data.extend_from_slice(&v6.ip().octets());
                data.extend_from_slice(&v6.scope_id().to_be_bytes());
                entity_addr.sockaddr_data = data;
            }
        }

        entity_addr
    }

    /// Configure a state machine with server and client addresses
    ///
    /// This helper is used by both connect() and reconnect() to set up
    /// the state machine with proper EntityAddr values.
    ///
    /// # Arguments
    /// * `state_machine` - The state machine to configure
    /// * `server_addr` - The server's TCP address
    /// * `local_addr` - The local TCP address
    /// * `target_entity_addr` - Optional target EntityAddr (for OSD connections with nonce)
    fn configure_state_machine_addresses(
        state_machine: &mut StateMachine,
        server_addr: SocketAddr,
        local_addr: SocketAddr,
        target_entity_addr: Option<&denc::EntityAddr>,
    ) {
        // Set server address
        if let Some(target_addr) = target_entity_addr {
            state_machine.set_server_addr(target_addr.clone());
        } else {
            state_machine.set_server_addr(Self::socket_to_entity_addr(server_addr));
        }

        // Set client address
        state_machine.set_client_addr(Self::socket_to_entity_addr(local_addr));
    }

    /// Connect to a Ceph server with a specific target EntityAddr
    ///
    /// This establishes a TCP connection and performs the msgr2 banner exchange.
    /// Use this when connecting to an OSD where you need the exact EntityAddr (with nonce)
    /// from the OSDMap for CLIENT_IDENT.
    ///
    /// # Arguments
    /// * `addr` - The TCP socket address to connect to
    /// * `target_entity_addr` - The full EntityAddr (with nonce) to use as target in CLIENT_IDENT
    /// * `config` - Connection configuration (features and connection modes)
    pub async fn connect_with_target(
        addr: SocketAddr,
        target_entity_addr: denc::EntityAddr,
        config: crate::ConnectionConfig,
    ) -> Result<Self> {
        tracing::debug!(
            "Connection::connect_with_target() called with addr: {}, target nonce: {}",
            addr,
            target_entity_addr.nonce
        );
        Self::connect_inner(addr, Some(target_entity_addr), config).await
    }

    /// Connect to a Ceph server at the given address
    ///
    /// This establishes a TCP connection and performs the msgr2 banner exchange.
    ///
    /// # Arguments
    /// * `addr` - The server address to connect to
    /// * `config` - Connection configuration (features and connection modes)
    pub async fn connect(addr: SocketAddr, config: crate::ConnectionConfig) -> Result<Self> {
        tracing::debug!("Connection::connect() called with addr: {}", addr);
        Self::connect_inner(addr, None, config).await
    }

    /// Shared implementation for `connect()` and `connect_with_target()`.
    async fn connect_inner(
        addr: SocketAddr,
        target_entity_addr: Option<denc::EntityAddr>,
        config: crate::ConnectionConfig,
    ) -> Result<Self> {
        // Validate config
        if let Err(e) = config.validate() {
            return Err(Error::Protocol(format!("Invalid config: {}", e)));
        }

        // Establish TCP connection
        let mut stream = TcpStream::connect(addr).await?;
        let local_addr = stream.local_addr()?;
        tracing::info!(
            "TCP connection established to {} (local: {})",
            addr,
            local_addr
        );

        // Create state machine for client BEFORE banner exchange
        // This is important because we need to track banner bytes in pre-auth buffers
        let mut state_machine = StateMachine::new_client(config.clone());

        // Configure addresses
        Self::configure_state_machine_addresses(
            &mut state_machine,
            addr,
            local_addr,
            target_entity_addr.as_ref(),
        );

        // Perform msgr2 banner exchange and record bytes in pre-auth buffers
        Self::exchange_banner(&mut stream, &mut state_machine, &config).await?;

        let mut state = ConnectionState::new(stream, state_machine);
        state.session.max_sent_messages = config.max_replay_queue_len;

        // Initialize throttle from config if present
        let throttle = config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::throttle::MessageThrottle::new(cfg.clone()));

        Ok(Self {
            state,
            server_addr: addr,
            target_entity_addr,
            config,
            throttle,
        })
    }

    /// Perform msgr2 banner exchange (client-side)
    ///
    /// Client sends banner first, then receives server banner.
    async fn exchange_banner(
        stream: &mut TcpStream,
        state_machine: &mut StateMachine,
        config: &crate::ConnectionConfig,
    ) -> Result<()> {
        // Send our banner with configured features
        let banner = Banner::new_with_features(
            FeatureSet::from(config.supported_features),
            FeatureSet::from(config.required_features),
        );

        let mut buf = BytesMut::with_capacity(64);
        banner.encode(&mut buf)?;

        // Record and send banner bytes (pre-auth signature tracking)
        state_machine.record_sent(&buf);
        stream.write_all(&buf).await?;
        stream.flush().await?;
        tracing::info!(
            "Sent msgr2 banner: supported={:x}, required={:x}",
            u64::from(banner.supported_features),
            u64::from(banner.required_features)
        );

        // Read server banner response (26 bytes: 8 prefix + 2 length + 16 payload)
        let mut buf = vec![0u8; 26];
        stream.read_exact(&mut buf).await?;
        state_machine.record_received(&buf);

        let mut bytes = BytesMut::from(&buf[..]);
        let server_banner = Banner::decode(&mut bytes)?;

        tracing::info!(
            "Received server banner: supported={:x}, required={:x}",
            u64::from(server_banner.supported_features),
            u64::from(server_banner.required_features)
        );

        // Check if we can meet server requirements
        let our_features = FeatureSet::from(config.supported_features);
        let missing = server_banner.required_features & !our_features;
        if !missing.is_empty() {
            return Err(Error::Protocol(format!(
                "Missing required features: {:x}",
                u64::from(missing)
            )));
        }

        // Store peer's supported features for later use (e.g., compression negotiation)
        state_machine.set_peer_supported_features(u64::from(server_banner.supported_features));

        Ok(())
    }

    /// Perform msgr2 banner exchange (server-side)
    ///
    /// Server receives client banner first, then sends server banner.
    async fn exchange_banner_server(
        stream: &mut TcpStream,
        state_machine: &mut StateMachine,
        config: &crate::ConnectionConfig,
    ) -> Result<()> {
        // Read client banner (26 bytes: 8 prefix + 2 length + 16 payload)
        let mut buf = vec![0u8; 26];
        stream.read_exact(&mut buf).await?;
        state_machine.record_received(&buf);

        let mut bytes = BytesMut::from(&buf[..]);
        let client_banner = Banner::decode(&mut bytes)?;

        tracing::info!(
            "Received client banner: supported={:x}, required={:x}",
            u64::from(client_banner.supported_features),
            u64::from(client_banner.required_features)
        );

        // Check if we can meet client requirements
        let our_features = FeatureSet::from(config.supported_features);
        let missing = client_banner.required_features & !our_features;
        if !missing.is_empty() {
            return Err(Error::Protocol(format!(
                "Missing required features: {:x}",
                u64::from(missing)
            )));
        }

        // Store peer's supported features for later use (e.g., compression negotiation)
        state_machine.set_peer_supported_features(u64::from(client_banner.supported_features));

        // Send our banner response
        let banner = Banner::new_with_features(
            FeatureSet::from(config.supported_features),
            FeatureSet::from(config.required_features),
        );

        let mut buf = BytesMut::with_capacity(64);
        banner.encode(&mut buf)?;

        state_machine.record_sent(&buf);
        stream.write_all(&buf).await?;
        stream.flush().await?;
        tracing::info!(
            "Sent msgr2 banner: supported={:x}, required={:x}",
            u64::from(banner.supported_features),
            u64::from(banner.required_features)
        );

        Ok(())
    }

    /// Accept an incoming msgr2 connection (server-side)
    ///
    /// This accepts a TCP connection from a client and performs the msgr2 banner exchange.
    /// After this, call `accept_session()` to complete the handshake.
    ///
    /// # Arguments
    /// * `stream` - The accepted TCP stream from TcpListener
    /// * `config` - Connection configuration (features and connection modes)
    /// * `auth_handler` - Optional server-side authentication handler
    pub async fn accept(
        stream: TcpStream,
        config: crate::ConnectionConfig,
        auth_handler: Option<auth::CephXServerHandler>,
    ) -> Result<Self> {
        let peer_addr = stream.peer_addr()?;
        let local_addr = stream.local_addr()?;

        tracing::info!("Accepting connection from {}", peer_addr);
        tracing::debug!("Local address: {}", local_addr);

        // Validate config
        if let Err(e) = config.validate() {
            return Err(Error::Protocol(format!("Invalid config: {}", e)));
        }

        let mut stream = stream;

        // Create state machine for server BEFORE banner exchange
        // This is important because we need to track banner bytes in pre-auth buffers
        let mut state_machine = StateMachine::new_server_with_auth(auth_handler);

        // Set addresses using the shared conversion helper
        state_machine.set_client_addr(Self::socket_to_entity_addr(peer_addr));
        state_machine.set_server_addr(Self::socket_to_entity_addr(local_addr));

        tracing::debug!("Created server state machine");

        // Perform msgr2 banner exchange (server-side: receive first, then send)
        Self::exchange_banner_server(&mut stream, &mut state_machine, &config).await?;

        let mut state = ConnectionState::new(stream, state_machine);
        state.session.max_sent_messages = config.max_replay_queue_len;

        // Initialize throttle from config if present
        let throttle = config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::throttle::MessageThrottle::new(cfg.clone()));

        Ok(Self {
            state,
            server_addr: local_addr, // For server, this is our local address
            target_entity_addr: None,
            config,
            throttle,
        })
    }

    /// Accept a session by completing the full msgr2 handshake (server-side)
    ///
    /// Drives the following phase sequence:
    /// 1. `HelloServer`  — receive HELLO, send HELLO
    /// 2. `AuthServer`   — receive AUTH_REQUEST, send AUTH_DONE (with optional CephX round)
    /// 3. Coordinator    — compute HMAC-SHA256, install encryption
    /// 4. `AuthSignServer` — send server signature, verify client signature
    /// 5. `CompressionServer` (optional) — negotiate compression
    /// 6. `SessionServer` — receive CLIENT_IDENT, send SERVER_IDENT
    ///
    /// Returns when the connection is ready for message exchange.
    pub async fn accept_session(&mut self) -> Result<()> {
        use crate::{
            compression::CompressionAlgorithm,
            phase::{
                auth::AuthServer, compression::CompressionServer, hello::HelloServer,
                session::SessionServer, sign::AuthSignServer,
            },
        };

        tracing::info!("Accepting msgr2 session...");

        // Phase 1: HELLO exchange (server receives, then sends)
        self.state.drive_phase(HelloServer).await?;

        // Phase 2: Authentication
        let auth_handler = self.state.state_machine.take_server_auth_handler();
        let auth_out = self
            .state
            .drive_phase(AuthServer::new(auth_handler))
            .await?;

        // Coordinator: compute HMAC signatures
        let our_sig = {
            let sm = &self.state.state_machine;
            let rx_buf = Bytes::copy_from_slice(sm.get_pre_auth_rxbuf());
            StateMachine::hmac_sha256_pub(auth_out.session_key.as_ref(), &rx_buf)?
        };
        let exp_sig = {
            let sm = &self.state.state_machine;
            let tx_buf = Bytes::copy_from_slice(sm.get_pre_auth_txbuf());
            Some(StateMachine::hmac_sha256_pub(
                auth_out.session_key.as_ref(),
                &tx_buf,
            )?)
        };

        // Coordinator: commit auth state and install encryption
        {
            let sm = &mut self.state.state_machine;
            sm.complete_pre_auth(auth_out.session_key.clone());
            sm.set_connection_mode(auth_out.connection_mode);
            sm.set_connection_secret(auth_out.connection_secret.clone());
            if let Some(ref secret) = auth_out.connection_secret {
                sm.setup_encryption(secret)?;
            }
        }

        // Phase 3: Auth-signature exchange (server sends first)
        self.state
            .drive_phase(AuthSignServer::new(our_sig, exp_sig))
            .await?;

        // Phase 4: Compression (only when both peers advertise it)
        if self.state.state_machine.compression_negotiation_needed() {
            let comp_out = self.state.drive_phase(CompressionServer).await?;
            if comp_out.algorithm != CompressionAlgorithm::None {
                self.state
                    .state_machine
                    .setup_compression(comp_out.algorithm);
            }
        }

        // Phase 5: Session establishment
        let server_cookie = self.state.state_machine.server_cookie_val();
        self.state
            .drive_phase(SessionServer::new(server_cookie))
            .await?;

        self.state.state_machine.transition_to_ready(0);
        tracing::info!("Session established (server-side)");
        Ok(())
    }

    /// Establish a session by completing the full msgr2 handshake (client-side)
    ///
    /// Drives the following phase sequence:
    /// 1. `HelloClient`  — send HELLO, receive HELLO
    /// 2. `AuthClient`   — AUTH_REQUEST ↔ AUTH_DONE exchange (with optional retries)
    /// 3. Coordinator    — compute HMAC-SHA256, install encryption
    /// 4. `AuthSignClient` — send client signature, verify server signature
    /// 5. `CompressionClient` (optional) — negotiate compression
    /// 6. `SessionClient` — send CLIENT_IDENT / SESSION_RECONNECT, receive SERVER_IDENT
    ///
    /// Returns when the connection is ready for message exchange.
    pub async fn establish_session(&mut self) -> Result<()> {
        use crate::{
            compression::CompressionAlgorithm,
            phase::{
                auth::{AuthClient, AuthOutput},
                compression::CompressionClient,
                hello::HelloClient,
                session::SessionClient,
                sign::AuthSignClient,
            },
        };

        tracing::info!("Establishing msgr2 session...");

        // Snapshot config data before any mutable operations.
        let preferred_modes = self.state.state_machine.config_preferred_modes().to_vec();
        let supported_auth_methods = self
            .state
            .state_machine
            .config_supported_auth_methods()
            .to_vec();
        let auth_provider = self.state.state_machine.get_auth_provider();
        let service_id = self.state.state_machine.config_service_id();
        let entity_name = self.state.state_machine.config_entity_name().clone();
        let client_cookie = self.state.session.client_cookie;

        // Phase 1: HELLO exchange
        self.state.drive_phase(HelloClient).await?;

        // Phase 2: Authentication
        let initial_global_id = self.state.state_machine.global_id();
        let auth_out: AuthOutput = self
            .state
            .drive_phase(AuthClient::new(
                preferred_modes,
                supported_auth_methods,
                auth_provider,
                service_id,
                entity_name,
                initial_global_id,
            ))
            .await?;

        // Coordinator: compute HMAC-SHA256 signatures over pre-auth byte streams.
        let our_sig = {
            let rx_buf = Bytes::copy_from_slice(self.state.state_machine.get_pre_auth_rxbuf());
            StateMachine::hmac_sha256_pub(auth_out.session_key.as_ref(), &rx_buf)?
        };
        let exp_sig = {
            let tx_buf = Bytes::copy_from_slice(self.state.state_machine.get_pre_auth_txbuf());
            Some(StateMachine::hmac_sha256_pub(
                auth_out.session_key.as_ref(),
                &tx_buf,
            )?)
        };

        // Coordinator: commit auth state and install encryption.
        self.state
            .state_machine
            .complete_pre_auth(auth_out.session_key.clone());
        self.state.state_machine.set_global_id(auth_out.global_id);
        self.state
            .state_machine
            .set_connection_mode(auth_out.connection_mode);
        self.state
            .state_machine
            .set_connection_secret(auth_out.connection_secret.clone());
        if let Some(ref secret) = auth_out.connection_secret {
            self.state.state_machine.setup_encryption(secret)?;
        }

        // Phase 3: Auth-signature exchange (client sends first)
        let peer_features = self.state.state_machine.peer_supported_features_val();
        let our_features = self.state.state_machine.our_supported_features();
        let sign_out = self
            .state
            .drive_phase(AuthSignClient::new(
                our_sig,
                exp_sig,
                peer_features,
                our_features,
            ))
            .await?;

        // Phase 4: Compression (only when both peers advertise it)
        if sign_out.needs_compression {
            let comp_out = self.state.drive_phase(CompressionClient).await?;
            if comp_out.algorithm != CompressionAlgorithm::None {
                self.state
                    .state_machine
                    .setup_compression(comp_out.algorithm);
            }
        }

        // Phase 5: Session establishment (CLIENT_IDENT or SESSION_RECONNECT)
        let server_cookie = self.state.session.server_cookie;
        let global_seq = self.state.session.global_seq;
        let connect_seq = self.state.session.connect_seq;
        let in_seq = self.state.in_seq;
        let global_id = self.state.state_machine.global_id();
        let server_addr = self.state.state_machine.server_addr_clone();
        let client_addr = self.state.state_machine.client_addr_clone();

        let sess_out = self
            .state
            .drive_phase(SessionClient::new(
                global_id,
                server_addr,
                client_addr,
                client_cookie,
                server_cookie,
                global_seq,
                connect_seq,
                in_seq,
            ))
            .await?;

        // Apply session results.
        self.state.session.server_cookie = sess_out.server_cookie;
        self.state
            .state_machine
            .transition_to_ready(sess_out.negotiated_features);

        if let Some(msg_seq) = sess_out.reconnect_msg_seq {
            tracing::info!("Session reconnected, server acked up to msg_seq={msg_seq}");
            self.state.discard_acknowledged_messages(msg_seq);
            let messages_to_replay: Vec<_> =
                self.state.session.sent_messages.iter().cloned().collect();
            if !messages_to_replay.is_empty() {
                tracing::info!(
                    "Replaying {} unacknowledged messages",
                    messages_to_replay.len()
                );
                for msg in messages_to_replay {
                    let msg_frame = MessageFrame::new(
                        msg.header,
                        msg.front.clone(),
                        msg.middle.clone(),
                        msg.data.clone(),
                    );
                    let frame = create_frame_from_trait(&msg_frame, Tag::Message)?;
                    self.state.send_frame(&frame).await?;
                }
            }
        }

        tracing::info!("Session established!");
        Ok(())
    }

    /// Attempt to reconnect after TCP connection failure
    ///
    /// This preserves the session state (cookies, sequences, sent messages)
    /// and attempts to resume the session using SESSION_RECONNECT.
    async fn reconnect(&mut self) -> Result<()> {
        // Check if we can reconnect (need server_cookie and non-lossy policy)
        if !self.state.can_reconnect() {
            return Err(Error::Protocol(
                "Cannot reconnect: lossy connection or no session established".to_string(),
            ));
        }

        tracing::warn!(
            "Connection lost, attempting to reconnect to {}",
            self.server_addr
        );

        // Preserve session state from current connection
        let client_cookie = self.state.client_cookie();
        let server_cookie = self.state.server_cookie();
        let global_seq = self.state.session.global_seq;
        let connect_seq = self.state.session.connect_seq + 1; // Increment for new attempt
        let in_seq = self.state.in_seq;
        let out_seq = self.state.out_seq;
        let sent_messages = self.state.session.sent_messages.clone();
        let max_sent_messages = self.state.session.max_sent_messages;

        tracing::debug!(
            "Reconnecting with: client_cookie={}, server_cookie={}, global_seq={}, connect_seq={}, in_seq={}, out_seq={}, queued_messages={}",
            client_cookie, server_cookie, global_seq, connect_seq, in_seq, out_seq, sent_messages.len()
        );

        // Establish new TCP connection
        let mut stream = TcpStream::connect(self.server_addr).await?;
        tracing::info!("TCP reconnection established to {}", self.server_addr);

        // Get local address for CLIENT_IDENT
        let local_addr = stream.local_addr()?;

        // Create new state machine with preserved session cookies
        let mut state_machine = StateMachine::new_client(self.config.clone());

        // Restore session cookies so reconnection handshake uses SESSION_RECONNECT
        state_machine.set_session_cookies(
            client_cookie,
            server_cookie,
            global_seq,
            connect_seq,
            in_seq,
        );

        // Set server and client addresses
        Self::configure_state_machine_addresses(
            &mut state_machine,
            self.server_addr,
            local_addr,
            self.target_entity_addr.as_ref(),
        );

        // Perform banner exchange
        Self::exchange_banner(&mut stream, &mut state_machine, &self.config).await?;

        // Create new ConnectionState
        let mut state = ConnectionState::new(stream, state_machine);

        // Restore session state
        state.out_seq = out_seq;
        state.in_seq = in_seq;
        state.session.sent_messages = sent_messages;
        state.session.max_sent_messages = max_sent_messages;

        // Replace current state with new reconnected state
        self.state = state;

        // Reinitialize throttle for new connection
        self.throttle = self
            .config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::throttle::MessageThrottle::new(cfg.clone()));

        tracing::info!("Session state restored, initiating reconnection handshake");

        // Run the full session establishment (this will use SESSION_RECONNECT since server_cookie != 0)
        self.establish_session().await?;

        tracing::info!("Reconnection complete, session resumed");

        Ok(())
    }

    /// Execute an operation with automatic retry and reconnection on recoverable errors
    ///
    /// This generic method handles the common pattern of:
    /// 1. Try the operation
    /// 2. If it fails with a recoverable error, attempt reconnection
    /// 3. Retry the operation after successful reconnection
    /// 4. Repeat up to MAX_RECONNECT_ATTEMPTS times
    ///
    /// # Arguments
    /// * `operation` - The async operation to execute
    /// * `operation_name` - Name of the operation for logging (e.g., "send", "receive")
    /// * `on_reconnect` - Callback after successful reconnection, returns whether to continue retrying
    ///
    /// # Returns
    /// The result of the operation, or an error if all retries are exhausted
    async fn retry_with_reconnect<F, T, R>(
        &mut self,
        mut operation: F,
        operation_name: &str,
        on_reconnect: R,
    ) -> Result<T>
    where
        F: FnMut(
            &mut Self,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + '_>>,
        R: Fn() -> ReconnectAction<T>,
    {
        const MAX_RECONNECT_ATTEMPTS: usize = 3;
        const INITIAL_BACKOFF_MS: u64 = 100;
        const MAX_BACKOFF_MS: u64 = 1000;
        const JITTER_PERCENT: f64 = 0.3; // ±30% jitter

        for attempt in 0..MAX_RECONNECT_ATTEMPTS {
            match operation(self).await {
                Ok(result) => return Ok(result),
                Err(e) if e.is_recoverable() && self.state.can_reconnect() => {
                    tracing::warn!(
                        "{} failed due to connection error (attempt {}/{}): {}",
                        operation_name,
                        attempt + 1,
                        MAX_RECONNECT_ATTEMPTS,
                        e
                    );

                    // Calculate exponential backoff with jitter before reconnection attempt
                    // Skip backoff on first attempt (attempt == 0)
                    if attempt > 0 {
                        // Exponential backoff: 100ms, 200ms, 400ms, ...
                        let base_backoff_ms =
                            (INITIAL_BACKOFF_MS * (1_u64 << (attempt - 1))).min(MAX_BACKOFF_MS);

                        // Add jitter: ±30% randomness to prevent thundering herd
                        // Following best practices for distributed systems
                        use rand::Rng;
                        let jitter = rand::thread_rng().gen_range(-JITTER_PERCENT..=JITTER_PERCENT);
                        let jittered_backoff_ms = (base_backoff_ms as f64 * (1.0 + jitter)) as u64;

                        tracing::debug!(
                            "Backing off for {}ms (base: {}ms, jitter: {:.1}%) before attempt {}",
                            jittered_backoff_ms,
                            base_backoff_ms,
                            jitter * 100.0,
                            attempt + 1
                        );

                        tokio::time::sleep(tokio::time::Duration::from_millis(jittered_backoff_ms))
                            .await;
                    }

                    // Attempt reconnection
                    if let Err(reconnect_err) = self.reconnect().await {
                        tracing::error!("Reconnection failed: {}", reconnect_err);
                        // If this was the last attempt, return the reconnection error
                        if attempt + 1 == MAX_RECONNECT_ATTEMPTS {
                            return Err(reconnect_err);
                        }
                        // Otherwise, continue to next attempt
                        continue;
                    }

                    tracing::info!("Reconnection successful after {}", operation_name);

                    // Let caller decide what to do after reconnection
                    match on_reconnect() {
                        ReconnectAction::ReturnSuccess(value) => return Ok(value),
                        ReconnectAction::Retry => continue,
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::Protocol(format!(
            "Failed to {} after {} attempts",
            operation_name, MAX_RECONNECT_ATTEMPTS
        )))
    }

    /// Send a Ceph message over the established session
    ///
    /// This method automatically handles connection failures by attempting
    /// reconnection and message replay according to the msgr2 protocol.
    pub async fn send_message(&mut self, msg: Message) -> Result<()> {
        self.retry_with_reconnect(
            |conn| Box::pin(conn.send_message_inner(msg.clone())),
            "send",
            || {
                // After reconnection, the message will be replayed automatically
                // from the sent_messages queue, so we can return success immediately
                ReconnectAction::ReturnSuccess(())
            },
        )
        .await
    }

    /// Internal implementation of send_message without reconnection logic
    async fn send_message_inner(&mut self, mut msg: Message) -> Result<()> {
        let msg_type = msg.msg_type();
        let msg_size = msg.total_size() as usize;

        // Wait for throttle if configured
        if let Some(throttle) = &self.throttle {
            throttle.wait_for_send(msg_size).await;
        }

        // Assign sequence numbers (pre-increment, like C++ does with ++out_seq)
        self.state.out_seq += 1;
        msg.header.set_seq(self.state.out_seq);
        msg.header.set_ack_seq(self.state.in_seq);

        let seq = msg.seq();
        let ack_seq = msg.header.get_ack_seq();
        tracing::debug!(
            "Sending message: type=0x{:04x}, seq={}, ack_seq={}, front={}, middle={}, data={}",
            msg_type,
            seq,
            ack_seq,
            msg.front.len(),
            msg.middle.len(),
            msg.data.len(),
        );

        // Record message in sent queue for potential replay (before sending)
        self.state.record_sent_message(msg.clone())?;

        // Convert Message to MessageFrame and send
        let msg_frame = MessageFrame::new(
            msg.header,
            msg.front.clone(),
            msg.middle.clone(),
            msg.data.clone(),
        );
        let frame = create_frame_from_trait(&msg_frame, Tag::Message)?;
        self.state.send_frame(&frame).await?;

        // Record send with throttle
        if let Some(throttle) = &self.throttle {
            throttle.record_send(msg_size).await;
        }

        Ok(())
    }

    /// Receive a Ceph message from the established session
    ///
    /// This method automatically handles connection failures by attempting
    /// reconnection according to the msgr2 protocol.
    pub async fn recv_message(&mut self) -> Result<Message> {
        self.retry_with_reconnect(
            |conn| Box::pin(conn.recv_message_inner()),
            "receive",
            || {
                // After reconnection, we need to retry receiving the message
                ReconnectAction::Retry
            },
        )
        .await
    }

    /// Internal implementation of recv_message without reconnection logic
    async fn recv_message_inner(&mut self) -> Result<Message> {
        // Loop until we get a Message frame, handling ACKs along the way
        loop {
            // Receive a frame
            let frame = self.state.recv_frame().await?;

            match frame.preamble.tag {
                Tag::Ack => {
                    // Handle ACK frame - decode sequence number and continue
                    if frame.segments.is_empty() {
                        return Err(Error::protocol_error("ACK frame missing payload"));
                    }

                    // ACK frame contains a single uint64_t sequence number
                    let mut payload = frame.segments[0].as_ref();
                    let ack_seq = u64::decode(&mut payload, 0)?;

                    tracing::debug!("Received ACK frame: seq={}", ack_seq);

                    // Discard acknowledged messages from sent queue (following Ceph's practice)
                    self.state.discard_acknowledged_messages(ack_seq);

                    // Record ACK with throttle to release queue depth slot
                    if let Some(throttle) = &self.throttle {
                        throttle.record_ack().await;
                    }

                    // Continue loop to wait for Message frame
                    continue;
                }
                Tag::Message => {
                    // Convert Frame to MessageFrame
                    // MessageFrame has 4 segments: header, front, middle, data
                    if frame.segments.is_empty() {
                        return Err(Error::protocol_error(
                            "Message frame missing header segment",
                        ));
                    }

                    let mut header_buf = frame.segments[0].clone();
                    let header = MsgHeader::decode(&mut header_buf)?;

                    let front = frame.segments.get(1).cloned().unwrap_or_default();
                    let middle = frame.segments.get(2).cloned().unwrap_or_default();
                    let data = frame.segments.get(3).cloned().unwrap_or_default();

                    // Use safe accessors for packed field values
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
                        "Received message: type={}, seq={}, ack_seq={}",
                        msg.msg_type(),
                        msg_seq,
                        ack_seq
                    );

                    // Update in_seq to the received message's sequence (following Ceph's practice)
                    self.state.set_in_seq(msg_seq);

                    // Process piggybacked ACK in message header (following Ceph's practice)
                    if ack_seq > 0 {
                        self.state.discard_acknowledged_messages(ack_seq);
                    }

                    return Ok(msg);
                }
                Tag::Keepalive2Ack => {
                    // Handle Keepalive2Ack frame - just continue waiting for Message
                    // The state machine already updated last_keepalive_ack timestamp
                    tracing::trace!("Received Keepalive2Ack frame (processed by state machine)");
                    continue;
                }
                _ => {
                    return Err(Error::protocol_error(&format!(
                        "Unexpected frame type in recv_message: {:?}",
                        frame.preamble.tag
                    )));
                }
            }
        }
    }

    /// Get a clone of the authenticated auth provider
    ///
    /// This should be called after `establish_session()` to get an auth provider
    /// that contains the session and service tickets obtained during authentication.
    pub fn get_auth_provider(&self) -> Option<Box<dyn auth::AuthProvider>> {
        self.state.state_machine.get_auth_provider()
    }

    /// Get the global_id assigned during authentication
    /// Returns 0 if authentication hasn't completed yet
    pub fn global_id(&self) -> u64 {
        self.state.state_machine.global_id()
    }

    /// Get the negotiated Ceph session features from the ident exchange.
    pub fn negotiated_features(&self) -> u64 {
        self.state.state_machine.negotiated_features()
    }

    /// Send a keepalive frame to the peer
    ///
    /// This sends a Keepalive2 frame with the current timestamp.
    /// The peer should respond with a Keepalive2Ack frame.
    pub async fn send_keepalive(&mut self) -> Result<()> {
        use crate::frames::Keepalive2Frame;
        use crate::state_machine::create_frame_from_trait;

        // Get current timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let timestamp_sec = now.as_secs() as u32;
        let timestamp_nsec = now.subsec_nanos();

        // Create Keepalive2 frame
        let keepalive_frame = Keepalive2Frame::new(timestamp_sec, timestamp_nsec);
        let frame = create_frame_from_trait(&keepalive_frame, crate::frames::Tag::Keepalive2)?;

        tracing::trace!(
            "Sending KEEPALIVE2 with timestamp {}.{:09}",
            timestamp_sec,
            timestamp_nsec
        );

        // Send the frame
        self.state.send_frame(&frame).await
    }

    /// Get the last keepalive ACK timestamp
    ///
    /// Returns None if no keepalive ACK has been received yet.
    /// This can be used to detect connection timeouts.
    pub fn last_keepalive_ack(&self) -> Option<std::time::Instant> {
        self.state.state_machine.last_keepalive_ack()
    }

    /// Get the current state name
    pub fn current_state_name(&self) -> &str {
        self.state.current_state_name()
    }

    /// Get the current state kind
    pub fn current_state_kind(&self) -> StateKind {
        self.state.current_state_kind()
    }

    /// Split the connection into separate send and receive halves
    ///
    /// This allows concurrent sending and receiving of messages from different
    /// tasks. The connection is consumed and cannot be used directly after splitting.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use msgr2::protocol::Connection;
    /// # async fn example() -> msgr2::Result<()> {
    /// # let addr = "127.0.0.1:6789".parse().unwrap();
    /// # let config = msgr2::ConnectionConfig::default();
    /// let mut conn = Connection::connect(addr, config).await?;
    /// conn.establish_session().await?;
    ///
    /// // Split the connection
    /// let (send_half, mut recv_half) = conn.split();
    ///
    /// // Spawn a task to receive messages
    /// tokio::spawn(async move {
    ///     loop {
    ///         match recv_half.recv_message().await {
    ///             Ok(msg) => println!("Received: {:?}", msg.msg_type()),
    ///             Err(_) => break,
    ///         }
    ///     }
    /// });
    ///
    /// // Send messages from the main task
    /// // send_half.send_message(msg).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Note
    ///
    /// The split halves do not support automatic reconnection. If the connection
    /// fails, both halves will return errors and you'll need to establish a new
    /// connection.
    pub fn split(self) -> (crate::split::SendHalf, crate::split::RecvHalf) {
        use crate::split::{SharedState, SplitBuilder};

        let shared = SharedState {
            out_seq: self.state.out_seq,
            in_seq: self.state.in_seq,
            client_cookie: self.state.session.client_cookie,
            server_cookie: self.state.session.server_cookie,
            global_seq: self.state.session.global_seq,
            connect_seq: self.state.session.connect_seq,
            sent_messages: self.state.session.sent_messages.clone(),
            max_sent_messages: self.state.session.max_sent_messages,
            is_lossy: self.state.session.is_lossy,
            global_id: self.state.state_machine.global_id(),
        };

        let builder = SplitBuilder {
            connection_state: self.state,
            shared,
            throttle: self.throttle,
        };

        builder.build()
    }

    /// Close the connection and discard all pending messages
    ///
    /// This matches Ceph's `discard_out_queue()` behavior:
    /// - Discards all sent messages (they won't be retransmitted)
    /// - Closes the TCP connection
    /// - Resets the session state
    ///
    /// Use this when permanently closing a connection (not reconnecting).
    pub async fn close(&mut self) {
        tracing::info!("Closing connection to {}", self.server_addr);

        // Discard all sent messages (won't be retransmitted)
        self.state.clear_sent_messages();

        // Reset session state
        self.state.reset_session();

        // TCP stream will be dropped when ConnectionState is dropped
        tracing::debug!("Connection closed, all pending messages discarded");
    }

    /// Snapshot of runtime connection state for diagnostics and debugging.
    ///
    /// The returned [`ConnectionDiagnostics`] is a point-in-time copy; it is
    /// not updated as the connection state evolves.
    pub fn diagnostics(&self) -> ConnectionDiagnostics {
        ConnectionDiagnostics {
            state_kind: self.state.current_state_kind(),
            encryption_enabled: self.state.state_machine.has_encryption(),
            compression_algorithm: self
                .state
                .state_machine
                .compression_ctx()
                .map(|c| c.algorithm()),
            out_seq: self.state.out_seq,
            in_seq: self.state.in_seq,
            sent_messages_pending: self.state.session.sent_messages.len(),
            client_cookie: self.state.session.client_cookie,
            server_cookie: self.state.session.server_cookie,
            global_seq: self.state.session.global_seq,
            connect_seq: self.state.session.connect_seq,
            global_id: self.state.state_machine.global_id(),
            last_keepalive_ack: self.state.state_machine.last_keepalive_ack(),
            is_lossy: self.state.session.is_lossy,
            can_reconnect: self.state.can_reconnect(),
        }
    }

    /// Current throttle statistics, or `None` if no throttle is configured.
    pub async fn throttle_stats(&self) -> Option<crate::throttle::ThrottleStats> {
        let t = self.throttle.as_ref()?;
        Some(t.stats().await)
    }

    /// Mark connection as down (for reconnection)
    ///
    /// This matches Ceph's `mark_down()` behavior:
    /// - Keeps sent messages for potential replay on reconnection
    /// - Closes the TCP connection
    /// - Does NOT reset session state (preserves cookies, sequences)
    ///
    /// Use this when the connection failed but you plan to reconnect.
    /// The sent messages will be available for retransmission after reconnection.
    pub async fn mark_down(&mut self) {
        tracing::info!(
            "Marking connection down to {} (keeping {} sent messages for replay)",
            self.server_addr,
            self.state.get_unacknowledged_messages().len()
        );

        // Keep sent messages for potential replay
        // Do NOT call clear_sent_messages() or reset_session()

        // TCP stream will be dropped when ConnectionState is dropped
        tracing::debug!("Connection marked down, sent messages preserved for reconnection");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MessagePriority;
    use crate::state_machine::StateMachine;

    /// Create a minimal Connection for unit tests using a loopback TCP socket pair.
    async fn make_test_connection() -> Connection {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let sm = StateMachine::new_client(crate::ConnectionConfig::with_no_auth());
        let state = ConnectionState::new(stream, sm);
        Connection {
            state,
            server_addr: addr,
            target_entity_addr: None,
            config: crate::ConnectionConfig::with_no_auth(),
            throttle: None,
        }
    }

    #[tokio::test]
    async fn test_diagnostics_initial_state() {
        let conn = make_test_connection().await;
        let diag = conn.diagnostics();

        assert_eq!(diag.out_seq, 0);
        assert_eq!(diag.in_seq, 0);
        assert_eq!(diag.sent_messages_pending, 0);
        assert_eq!(diag.global_id, 0);
        assert_eq!(diag.global_seq, 0);
        assert_eq!(diag.connect_seq, 0);
        assert_eq!(diag.server_cookie, 0);
        assert!(!diag.encryption_enabled);
        assert!(diag.compression_algorithm.is_none());
        assert!(!diag.can_reconnect); // no server_cookie yet
        assert!(diag.last_keepalive_ack.is_none());
        assert!(!diag.is_lossy);
    }

    #[tokio::test]
    async fn test_throttle_stats_none_without_config() {
        let conn = make_test_connection().await;
        assert!(conn.throttle_stats().await.is_none());
    }

    #[test]
    fn test_priority_queue_ordering() {
        let mut queue = PriorityQueue::new();

        let low_msg = Message::new(1, Bytes::from("low")).with_priority(MessagePriority::Low);
        let normal_msg =
            Message::new(2, Bytes::from("normal")).with_priority(MessagePriority::Normal);
        let high_msg = Message::new(3, Bytes::from("high")).with_priority(MessagePriority::High);

        // Add in random order
        queue.push_back(normal_msg);
        queue.push_back(low_msg);
        queue.push_back(high_msg);

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.pop_front().unwrap().msg_type(), 3); // High first
        assert_eq!(queue.pop_front().unwrap().msg_type(), 2); // Normal second
        assert_eq!(queue.pop_front().unwrap().msg_type(), 1); // Low last
        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_fifo_within_priority() {
        let mut queue = PriorityQueue::new();

        for i in 1u16..=3 {
            queue.push_back(
                Message::new(i, Bytes::from(format!("msg{i}")))
                    .with_priority(MessagePriority::Normal),
            );
        }

        assert_eq!(queue.pop_front().unwrap().msg_type(), 1);
        assert_eq!(queue.pop_front().unwrap().msg_type(), 2);
        assert_eq!(queue.pop_front().unwrap().msg_type(), 3);
    }

    #[test]
    fn test_priority_queue_iter() {
        let mut queue = PriorityQueue::new();

        queue.push_back(Message::new(1, Bytes::new()).with_priority(MessagePriority::Low));
        queue.push_back(Message::new(3, Bytes::new()).with_priority(MessagePriority::High));
        queue.push_back(Message::new(2, Bytes::new()).with_priority(MessagePriority::Normal));

        let types: Vec<u16> = queue.iter().map(|m| m.msg_type()).collect();
        assert_eq!(types, vec![3, 2, 1]); // High, Normal, Low
    }

    #[test]
    fn test_priority_queue_clear() {
        let mut queue = PriorityQueue::new();
        queue.push_back(Message::new(1, Bytes::new()));
        queue.push_back(Message::new(2, Bytes::new()).with_priority(MessagePriority::High));
        queue.push_back(Message::new(3, Bytes::new()).with_priority(MessagePriority::Normal));

        assert_eq!(queue.len(), 3);
        queue.clear();
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
        assert!(queue.pop_front().is_none());
    }

    #[test]
    fn test_priority_queue_front() {
        let mut queue = PriorityQueue::new();

        queue.push_back(Message::new(1, Bytes::from("low")).with_priority(MessagePriority::Low));
        queue.push_back(Message::new(2, Bytes::from("high")).with_priority(MessagePriority::High));

        assert_eq!(queue.front().unwrap().msg_type(), 2);
        assert_eq!(queue.len(), 2); // front() does not remove
    }
}
