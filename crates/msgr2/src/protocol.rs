//! msgr2 Protocol V2 implementation
//!
//! This module provides a high-level async API for the msgr2 protocol,
//! handling frame I/O, encryption, and state machine coordination.

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::banner::Banner;
use crate::error::{Error, Result};
use crate::frames::{Frame, MessageFrame, Preamble, Tag};
use crate::header::MsgHeader;
use crate::message::Message;
use crate::message_bus::{Dispatcher, MessageBus};
use crate::state_machine::{create_frame_from_trait, StateKind, StateMachine, StateResult};
use crate::FeatureSet;
use std::borrow::Cow;

/// Action to take after successful reconnection in retry_with_reconnect
enum ReconnectAction<T> {
    /// Return success immediately with the given value
    ReturnSuccess(T),
    /// Continue retrying the operation
    Retry,
}

/// Frame I/O layer - handles encryption-aware frame send/recv
pub struct FrameIO {
    stream: TcpStream,
}

impl Drop for FrameIO {
    fn drop(&mut self) {}
}

impl FrameIO {
    const PREAMBLE_SIZE: usize = 32;
    const INLINE_SIZE: usize = 48;
    const GCM_TAG_SIZE: usize = 16;

    /// Create a new FrameIO from an existing TCP stream
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
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
                Ok(_) | Err(_) => {
                    // Compression didn't help or failed, borrow original
                    if let Err(e) = frame.compress(ctx) {
                        tracing::warn!("Compression failed, sending uncompressed: {:?}", e);
                    }
                    Cow::Borrowed(frame)
                }
            }
        } else {
            Cow::Borrowed(frame)
        };

        // Step 2: Convert to wire format with proper preamble, segments, epilogue, and CRCs
        // Use msgr2.1 (is_rev1 = true) to match the banner we sent
        let wire_bytes = frame_to_send.to_wire(true);

        tracing::debug!(
            "Sending frame: tag={:?}, {} segments, {} wire bytes, compressed={}",
            frame_to_send.preamble.tag,
            frame_to_send.preamble.num_segments,
            wire_bytes.len(),
            (frame_to_send.preamble.flags & crate::frames::FRAME_EARLY_DATA_COMPRESSED) != 0
        );

        // Print segment info
        for (i, seg) in frame_to_send.segments.iter().enumerate() {
            tracing::debug!("  Segment {}: {} bytes", i, seg.len());
        }

        // Split into preamble (32 bytes) and payload (rest)
        let preamble_bytes = &wire_bytes[..Self::PREAMBLE_SIZE];
        let mut payload_bytes = wire_bytes[Self::PREAMBLE_SIZE..].to_vec();

        tracing::trace!(
            "send_frame: tag={:?}, has_encryption={}, num_segments={}, payload_len={}",
            frame_to_send.preamble.tag,
            state_machine.has_encryption(),
            frame_to_send.preamble.num_segments,
            payload_bytes.len()
        );

        // Step 3: Apply encryption if enabled
        let final_bytes = if !state_machine.has_encryption() {
            // No encryption - send as-is
            tracing::debug!(
                "Frame payload: plaintext={} bytes (no encryption)",
                payload_bytes.len()
            );
            wire_bytes
        } else {
            // SECURE mode with msgr2.1 inline optimization
            tracing::debug!(
                "SECURE mode: num_segments={}, payload_bytes.len()={}",
                frame.preamble.num_segments,
                payload_bytes.len()
            );

            // Rebuild payload from segments with proper alignment for secure mode
            // Secure frames don't use CRC; they use GCM auth tags instead
            const CRYPTO_BLOCK_SIZE: usize = 16;
            const FRAME_LATE_STATUS_COMPLETE: u8 = 0x0e;

            payload_bytes.clear();

            // Add all segments with 16-byte alignment padding (skip empty segments)
            for segment in &frame.segments {
                let segment_len = segment.len();
                if segment_len == 0 {
                    continue; // Don't add or pad empty segments
                }

                payload_bytes.extend_from_slice(segment);

                // Pad to 16-byte boundary
                let aligned_len = (segment_len + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1);
                let padding_needed = aligned_len - segment_len;
                if padding_needed > 0 {
                    payload_bytes.extend_from_slice(&vec![0u8; padding_needed]);
                }
            }

            // For multi-segment frames, add epilogue
            if frame.preamble.num_segments > 1 {
                // epilogue_secure_rev1_block_t: 1 byte late_status + 15 bytes padding = 16 bytes
                let mut epilogue = vec![FRAME_LATE_STATUS_COMPLETE];
                epilogue.extend_from_slice(&[0u8; CRYPTO_BLOCK_SIZE - 1]);
                payload_bytes.extend_from_slice(&epilogue);

                tracing::trace!(
                    "Secure multi-segment: num_segments={}, total_payload={} (includes {} byte epilogue)",
                    frame.preamble.num_segments,
                    payload_bytes.len(),
                    CRYPTO_BLOCK_SIZE
                );
            } else {
                tracing::trace!("Secure single-segment: payload_len={}", payload_bytes.len());
            }

            let inline_size = payload_bytes.len().min(Self::INLINE_SIZE);
            let remaining_size = payload_bytes.len().saturating_sub(Self::INLINE_SIZE);

            // Build preamble block (preamble + inline data + padding if needed)
            let mut preamble_block =
                BytesMut::with_capacity(Self::PREAMBLE_SIZE + Self::INLINE_SIZE);
            preamble_block.extend_from_slice(preamble_bytes);
            preamble_block.extend_from_slice(&payload_bytes[..inline_size]);

            // Pad to 48 bytes if inline data < 48 bytes
            if inline_size < Self::INLINE_SIZE {
                preamble_block.extend_from_slice(&vec![0u8; Self::INLINE_SIZE - inline_size]);
            }

            // Encrypt preamble block (32 + 48 = 80 bytes → 96 bytes with GCM tag)
            let encrypted_preamble = state_machine.encrypt_frame_data(&preamble_block)?;
            tracing::debug!(
                "Encrypted preamble block: {} bytes → {} bytes (includes {} inline)",
                preamble_block.len(),
                encrypted_preamble.len(),
                inline_size
            );

            let mut result =
                BytesMut::with_capacity(encrypted_preamble.len() + remaining_size + 16);
            result.extend_from_slice(&encrypted_preamble);

            // Encrypt remaining payload if any (starts after first 48 bytes)
            if remaining_size > 0 {
                let remaining_data = &payload_bytes[Self::INLINE_SIZE..];
                tracing::trace!(
                    "About to encrypt remaining: remaining_data.len()={}, payload_bytes.len()={}, inline_size={}, remaining_size={}",
                    remaining_data.len(),
                    payload_bytes.len(),
                    inline_size,
                    remaining_size
                );
                let encrypted_remaining = state_machine.encrypt_frame_data(remaining_data)?;
                result.extend_from_slice(&encrypted_remaining);
            }

            tracing::debug!(
                "Total encrypted frame: {} bytes (preamble_block={}, remaining={})",
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

        // Print first 128 bytes in hex for debugging
        let preview_len = final_bytes.len().min(128);
        let hex_str: String = final_bytes[..preview_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("");
        tracing::debug!("Wire bytes (hex): {}", hex_str);

        // Record ENCRYPTED data for AUTH_SIGNATURE computation (after encryption)
        state_machine.record_sent(&final_bytes);
        if state_machine.is_pre_auth_enabled() {
            tracing::debug!(
                "Pre-auth: recorded sent frame tag={:?}, {} bytes (encrypted)",
                frame.preamble.tag,
                final_bytes.len()
            );
        }

        tracing::debug!(
            "DEBUG: About to write_all {} bytes for frame tag={:?}",
            final_bytes.len(),
            frame.preamble.tag
        );
        if frame.preamble.tag == Tag::ClientIdent {
            tracing::debug!(
                "DEBUG: CLIENT_IDENT hex (first 128 bytes): {}",
                final_bytes
                    .iter()
                    .take(128)
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>()
                    .join("")
            );
        }
        self.stream.write_all(&final_bytes).await?;
        tracing::debug!("DEBUG: write_all succeeded, flushing...");
        self.stream.flush().await?;
        tracing::debug!("DEBUG: flush succeeded for tag={:?}", frame.preamble.tag);
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
        tracing::debug!("DEBUG: recv_frame called");
        let has_encryption = state_machine.has_encryption();
        tracing::debug!("DEBUG: recv_frame: has_encryption={}", has_encryption);

        // Read preamble block
        let preamble_block_size = if has_encryption {
            Self::PREAMBLE_SIZE + Self::INLINE_SIZE + Self::GCM_TAG_SIZE // 96 bytes
        } else {
            Self::PREAMBLE_SIZE // 32 bytes
        };

        let mut preamble_block_buf = vec![0u8; preamble_block_size];
        tracing::debug!(
            "About to read {} bytes for preamble block",
            preamble_block_size
        );

        // Try to peek first to see if data is available
        let mut peek_buf = [0u8; 1];
        match self.stream.peek(&mut peek_buf).await {
            Ok(n) => if n == 0 {},
            Err(_e) => {}
        }

        match self.stream.read_exact(&mut preamble_block_buf).await {
            Ok(_) => {
                tracing::debug!("Successfully read preamble block");
                tracing::debug!(
                    "DEBUG: recv_frame read {} bytes, has_encryption={}, first 32 bytes: {}",
                    preamble_block_buf.len(),
                    has_encryption,
                    preamble_block_buf
                        .iter()
                        .take(32)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join("")
                );
            }
            Err(e) => {
                tracing::error!("Failed to read preamble block: {:?}", e);
                return Err(e.into());
            }
        }

        // Record ENCRYPTED data for AUTH_SIGNATURE computation (before decryption)
        state_machine.record_received(&preamble_block_buf);
        tracing::debug!(
            "Pre-auth: recorded received preamble block {} bytes (encrypted)",
            preamble_block_buf.len()
        );

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
            Bytes::copy_from_slice(&preamble_block_buf)
        };

        // Extract preamble (first 32 bytes)
        let preamble_bytes = &preamble_and_inline[..Self::PREAMBLE_SIZE];
        let inline_data = if has_encryption {
            &preamble_and_inline[Self::PREAMBLE_SIZE..Self::PREAMBLE_SIZE + Self::INLINE_SIZE]
        } else {
            &[]
        };

        // Parse preamble
        let preamble = Preamble::decode(Bytes::copy_from_slice(preamble_bytes))?;

        // Print raw preamble bytes for debugging
        let preamble_hex: String = preamble_bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("");
        tracing::debug!("Raw preamble hex: {}", preamble_hex);
        tracing::debug!(
            "DEBUG: Decoded preamble: tag={:?} (raw=0x{:02x}), num_segments={}",
            preamble.tag,
            preamble_bytes[0],
            preamble.num_segments
        );
        tracing::debug!(
            "Preamble: tag={:?} (raw={}), num_segments={}, segments={:?}",
            preamble.tag,
            preamble_bytes[0],
            preamble.num_segments,
            &preamble.segments[0..preamble.num_segments as usize]
        );

        // Calculate total segment size
        let mut total_segment_size = 0;
        for i in 0..preamble.num_segments as usize {
            total_segment_size += preamble.segments[i].logical_len as usize;
        }

        // Calculate total payload size based on encryption mode and number of segments
        const CRYPTO_BLOCK_SIZE: usize = 16;
        let total_payload_size = if has_encryption && total_segment_size > 0 {
            if preamble.num_segments == 1 {
                // Secure mode single segment: align to 16-byte boundary
                let aligned_len =
                    (total_segment_size + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1);
                tracing::debug!(
                    "Secure single-segment RX: logical={}, aligned={}",
                    total_segment_size,
                    aligned_len
                );
                aligned_len
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
                // Add epilogue for multi-segment frames
                padded_size += CRYPTO_BLOCK_SIZE;
                tracing::debug!(
                    "Secure multi-segment RX: logical={}, padded={}",
                    total_segment_size,
                    padded_size
                );
                padded_size
            }
        } else {
            // Plaintext: add CRC for single-segment frames
            let needs_crc = preamble.num_segments == 1 && total_segment_size > 0;
            total_segment_size + if needs_crc { 4 } else { 0 }
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

            // Record ENCRYPTED data for AUTH_SIGNATURE computation (before decryption)
            state_machine.record_received(&remaining_buf);
            tracing::debug!(
                "Pre-auth: recorded received remaining {} bytes (encrypted)",
                remaining_buf.len()
            );

            if has_encryption {
                // Decrypt remaining data
                let decrypted_remaining = state_machine.decrypt_frame_data(&remaining_buf)?;
                tracing::debug!(
                    "Decrypted remaining: {} bytes → {} bytes",
                    remaining_buf.len(),
                    decrypted_remaining.len()
                );

                // Combine inline + decrypted remaining
                let mut combined =
                    BytesMut::with_capacity(Self::INLINE_SIZE + decrypted_remaining.len());
                combined.extend_from_slice(inline_data);
                combined.extend_from_slice(&decrypted_remaining);
                combined.freeze()
            } else {
                Bytes::copy_from_slice(&remaining_buf)
            }
        } else if has_encryption {
            // All data was inline
            Bytes::copy_from_slice(&inline_data[..total_payload_size])
        } else {
            Bytes::new()
        };

        // Parse segments from the full payload
        let mut segments = Vec::new();
        let mut offset = 0;
        for i in 0..preamble.num_segments as usize {
            let segment_len = preamble.segments[i].logical_len as usize;
            if segment_len > 0 {
                // Extract only the logical length of data (not the padding)
                let segment_data = &full_payload[offset..offset + segment_len];
                segments.push(Bytes::copy_from_slice(segment_data));

                // For secure frames, skip 16-byte alignment padding
                if has_encryption {
                    const CRYPTO_BLOCK_SIZE: usize = 16;
                    let aligned_len =
                        (segment_len + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1);
                    offset += aligned_len;
                } else {
                    offset += segment_len;
                }
            } else {
                // Empty segment - still add it to maintain segment indices
                segments.push(Bytes::new());
            }
        }

        // For secure multi-segment frames, skip the epilogue (already at the end of full_payload)
        // The epilogue was included in total_payload_size but we don't need to parse it

        tracing::debug!("Parsed {} segments", segments.len());

        let mut frame = Frame { preamble, segments };

        // Step 4: Apply decompression if frame is compressed
        if frame.preamble.is_compressed() {
            if let Some(ctx) = state_machine.compression_ctx() {
                // Calculate original size from all segment lengths
                // For compressed frames, we need to know the original uncompressed size
                // This should be stored somewhere - for now, we'll use a heuristic
                // In practice, Ceph stores this in the frame metadata
                let compressed_size: usize = frame.segments.iter().map(|s| s.len()).sum();
                // Estimate original size as 2x compressed size (conservative estimate)
                // TODO: Get actual original size from frame metadata
                let estimated_original_size = compressed_size * 3;

                match frame.decompress(ctx, estimated_original_size) {
                    Ok(decompressed_frame) => {
                        tracing::debug!(
                            "Frame decompressed: tag={:?}, algorithm={:?}",
                            frame.preamble.tag,
                            ctx.algorithm()
                        );
                        frame = decompressed_frame;
                    }
                    Err(e) => {
                        tracing::error!("Decompression failed: {:?}", e);
                        return Err(Error::protocol_error(&format!(
                            "Decompression failed: {:?}",
                            e
                        )));
                    }
                }
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
pub struct ConnectionState {
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
    /// Connection policy - if true, connection is lossy (no reconnection support)
    is_lossy: bool,
}

impl ConnectionState {
    /// Create a new connection state with the given stream and state machine
    pub fn new(stream: TcpStream, state_machine: StateMachine) -> Self {
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
                is_lossy: false, // Default to lossless (reconnectable)
            },
        }
    }

    /// Send a frame through the state machine and frame I/O
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        self.frame_io
            .send_frame(frame, &mut self.state_machine)
            .await
    }

    /// Receive a frame through the frame I/O and state machine
    pub async fn recv_frame(&mut self) -> Result<Frame> {
        self.frame_io.recv_frame(&mut self.state_machine).await
    }

    /// Handle a frame with the state machine
    pub fn handle_frame(&mut self, frame: Frame) -> Result<StateResult> {
        self.state_machine.handle_frame(frame)
    }

    /// Enter the state machine
    pub fn enter(&mut self) -> Result<StateResult> {
        self.state_machine.enter()
    }

    /// Get the current state name
    pub fn current_state_name(&self) -> &str {
        self.state_machine.current_state_name()
    }

    /// Get the current state kind
    pub fn current_state_kind(&self) -> StateKind {
        self.state_machine.current_state_kind()
    }

    // Session state management methods

    /// Get the client cookie for this connection
    pub fn client_cookie(&self) -> u64 {
        self.session.client_cookie
    }

    /// Get the server cookie (0 if not yet assigned)
    pub fn server_cookie(&self) -> u64 {
        self.session.server_cookie
    }

    /// Set the server cookie (called when SERVER_IDENT is received)
    pub fn set_server_cookie(&mut self, cookie: u64) {
        self.session.server_cookie = cookie;
    }

    /// Check if this connection has a valid session that can be reconnected
    pub fn can_reconnect(&self) -> bool {
        !self.session.is_lossy && self.session.server_cookie != 0
    }

    /// Get the current global sequence number
    pub fn global_seq(&self) -> u64 {
        self.session.global_seq
    }

    /// Set the global sequence number
    pub fn set_global_seq(&mut self, seq: u64) {
        self.session.global_seq = seq;
    }

    /// Get the current connection sequence number
    pub fn connect_seq(&self) -> u64 {
        self.session.connect_seq
    }

    /// Increment the connection sequence number (for reconnection attempts)
    pub fn increment_connect_seq(&mut self) {
        self.session.connect_seq += 1;
    }

    /// Get the last received message sequence number
    pub fn in_seq(&self) -> u64 {
        self.in_seq
    }

    /// Set the incoming message sequence number
    pub fn set_in_seq(&mut self, seq: u64) {
        self.in_seq = seq;
    }

    /// Record a sent message for potential replay
    pub fn record_sent_message(&mut self, message: Message) {
        if !self.session.is_lossy {
            self.session.sent_messages.push_back(message);
        }
    }

    /// Discard acknowledged messages up to the given sequence number
    pub fn discard_acknowledged_messages(&mut self, ack_seq: u64) {
        while let Some(msg) = self.session.sent_messages.front() {
            if msg.header.seq <= ack_seq {
                self.session.sent_messages.pop_front();
            } else {
                break;
            }
        }
    }

    /// Get all unacknowledged messages for replay
    pub fn get_unacknowledged_messages(&self) -> &std::collections::VecDeque<Message> {
        &self.session.sent_messages
    }

    /// Clear the sent message queue
    pub fn clear_sent_messages(&mut self) {
        self.session.sent_messages.clear();
    }

    /// Reset session state for a new connection
    pub fn reset_session(&mut self) {
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
    /// Local message handlers registered on this connection
    local_handlers: HashMap<u16, Arc<dyn Dispatcher>>,
    /// Global message bus for inter-component messages
    message_bus: Option<Arc<MessageBus>>,
    /// Background task handle for message loop
    #[allow(dead_code)] // Will be used when message loop is implemented
    recv_task: Option<JoinHandle<()>>,
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
            "DEBUG: Connection::connect_with_target() called with addr: {}, target nonce: {}",
            addr,
            target_entity_addr.nonce
        );

        // Validate config
        if let Err(e) = config.validate() {
            return Err(Error::Protocol(format!("Invalid config: {}", e)));
        }

        tracing::debug!("DEBUG: About to TcpStream::connect({})", addr);
        // Establish TCP connection
        let mut stream = TcpStream::connect(addr).await?;
        let peer_addr = stream.peer_addr()?;
        tracing::debug!(
            "DEBUG: ✓ TCP connection established - peer: {}, local: {}",
            peer_addr,
            stream.local_addr()?
        );
        tracing::info!("✓ TCP connection established to {}", addr);

        // Get our local address from the connection
        let local_addr = stream.local_addr()?;
        tracing::debug!("Local address: {}", local_addr);

        // Create state machine for client BEFORE banner exchange
        // This is important because we need to track banner bytes in pre-auth buffers
        let mut state_machine = StateMachine::new_client(config.clone());

        // Configure addresses
        Self::configure_state_machine_addresses(
            &mut state_machine,
            addr,
            local_addr,
            Some(&target_entity_addr),
        );

        tracing::debug!("✓ Created client state machine");

        // Perform msgr2 banner exchange and record bytes in pre-auth buffers
        Self::exchange_banner(&mut stream, &mut state_machine, &config).await?;

        let state = ConnectionState::new(stream, state_machine);

        // Initialize throttle from config if present
        let throttle = config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::throttle::MessageThrottle::new(cfg.clone()));

        Ok(Self {
            state,
            server_addr: addr,
            target_entity_addr: Some(target_entity_addr),
            config,
            throttle,
            local_handlers: HashMap::new(),
            message_bus: None,
            recv_task: None,
        })
    }

    /// Connect to a Ceph server at the given address
    ///
    /// This establishes a TCP connection and performs the msgr2 banner exchange.
    ///
    /// # Arguments
    /// * `addr` - The server address to connect to
    /// * `config` - Connection configuration (features and connection modes)
    pub async fn connect(addr: SocketAddr, config: crate::ConnectionConfig) -> Result<Self> {
        tracing::debug!("DEBUG: Connection::connect() called with addr: {}", addr);

        // Validate config
        if let Err(e) = config.validate() {
            return Err(Error::Protocol(format!("Invalid config: {}", e)));
        }

        tracing::debug!("DEBUG: About to TcpStream::connect({})", addr);
        // Establish TCP connection
        let mut stream = TcpStream::connect(addr).await?;
        let peer_addr = stream.peer_addr()?;
        tracing::debug!(
            "DEBUG: ✓ TCP connection established - peer: {}, local: {}",
            peer_addr,
            stream.local_addr()?
        );
        tracing::info!("✓ TCP connection established to {}", addr);

        // Get our local address from the connection
        let local_addr = stream.local_addr()?;
        tracing::debug!("Local address: {}", local_addr);

        // Create state machine for client BEFORE banner exchange
        // This is important because we need to track banner bytes in pre-auth buffers
        let mut state_machine = StateMachine::new_client(config.clone());

        // Configure addresses
        Self::configure_state_machine_addresses(&mut state_machine, addr, local_addr, None);

        tracing::debug!("✓ Created client state machine");

        // Perform msgr2 banner exchange and record bytes in pre-auth buffers
        Self::exchange_banner(&mut stream, &mut state_machine, &config).await?;

        let state = ConnectionState::new(stream, state_machine);

        // Initialize throttle from config if present
        let throttle = config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::throttle::MessageThrottle::new(cfg.clone()));

        Ok(Self {
            state,
            server_addr: addr,
            target_entity_addr: None,
            config,
            throttle,
            local_handlers: HashMap::new(),
            message_bus: None,
            recv_task: None,
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
            FeatureSet::new(config.supported_features),
            FeatureSet::new(config.required_features),
        );

        let mut buf = BytesMut::with_capacity(64);
        banner.encode(&mut buf)?;

        // Record sent banner bytes for pre-auth signature
        state_machine.record_sent(&buf);
        tracing::debug!("Pre-auth: recorded sent banner {} bytes", buf.len());

        stream.write_all(&buf).await?;
        stream.flush().await?;
        tracing::info!(
            "✓ Sent msgr2 banner with features: supported={:x}, required={:x}",
            banner.supported_features.value(),
            banner.required_features.value()
        );

        // Read server banner response
        // Banner is "ceph v2\n" (8 bytes) + length (2 bytes) + payload (16 bytes) = 26 bytes total
        let mut buf = vec![0u8; 26];
        stream.read_exact(&mut buf).await?;

        // Record received banner bytes for pre-auth signature
        state_machine.record_received(&buf);
        tracing::debug!("Pre-auth: recorded received banner {} bytes", buf.len());

        let mut bytes = BytesMut::from(&buf[..]);
        let server_banner = Banner::decode(&mut bytes)?;

        tracing::info!(
            "✓ Received server banner: supported={:x}, required={:x}",
            server_banner.supported_features.value(),
            server_banner.required_features.value()
        );

        // Check if we can meet server requirements
        let our_features = FeatureSet::new(config.supported_features);
        let missing = server_banner.required_features & !our_features;
        if !missing.is_empty() {
            return Err(Error::Protocol(format!(
                "Missing required features: {:x}",
                missing.value()
            )));
        }

        // Store peer's supported features for later use (e.g., compression negotiation)
        state_machine.set_peer_supported_features(server_banner.supported_features.value());

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
        // Read client banner first
        // Banner is "ceph v2\n" (8 bytes) + length (2 bytes) + payload (16 bytes) = 26 bytes total
        let mut buf = vec![0u8; 26];
        stream.read_exact(&mut buf).await?;

        // Record received banner bytes for pre-auth signature
        state_machine.record_received(&buf);
        tracing::debug!("Pre-auth: recorded received banner {} bytes", buf.len());

        let mut bytes = BytesMut::from(&buf[..]);
        let client_banner = Banner::decode(&mut bytes)?;

        tracing::info!(
            "✓ Received client banner: supported={:x}, required={:x}",
            client_banner.supported_features.value(),
            client_banner.required_features.value()
        );

        // Check if we can meet client requirements
        let our_features = FeatureSet::new(config.supported_features);
        let missing = client_banner.required_features & !our_features;
        if !missing.is_empty() {
            return Err(Error::Protocol(format!(
                "Missing required features: {:x}",
                missing.value()
            )));
        }

        // Store peer's supported features for later use (e.g., compression negotiation)
        state_machine.set_peer_supported_features(client_banner.supported_features.value());

        // Send our banner response
        let banner = Banner::new_with_features(
            FeatureSet::new(config.supported_features),
            FeatureSet::new(config.required_features),
        );

        let mut buf = BytesMut::with_capacity(64);
        banner.encode(&mut buf)?;

        // Record sent banner bytes for pre-auth signature
        state_machine.record_sent(&buf);
        tracing::debug!("Pre-auth: recorded sent banner {} bytes", buf.len());

        stream.write_all(&buf).await?;
        stream.flush().await?;
        tracing::info!(
            "✓ Sent msgr2 banner with features: supported={:x}, required={:x}",
            banner.supported_features.value(),
            banner.required_features.value()
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

        tracing::info!("✓ Accepting connection from {}", peer_addr);
        tracing::debug!("Local address: {}", local_addr);

        // Validate config
        if let Err(e) = config.validate() {
            return Err(Error::Protocol(format!("Invalid config: {}", e)));
        }

        let mut stream = stream;

        // Create state machine for server BEFORE banner exchange
        // This is important because we need to track banner bytes in pre-auth buffers
        let mut state_machine = StateMachine::new_server_with_auth(auth_handler);

        // Set the client address (peer) for SERVER_IDENT
        let mut client_entity_addr = denc::EntityAddr::new();
        client_entity_addr.addr_type = denc::EntityAddrType::Msgr2;
        match peer_addr {
            SocketAddr::V4(v4) => {
                // IPv4: ss_family (2 bytes, little-endian) + port (2 bytes, big-endian) + IP (4 bytes) + padding (8 bytes)
                let mut data = Vec::with_capacity(16);
                data.extend_from_slice(&2u16.to_le_bytes()); // AF_INET = 2
                data.extend_from_slice(&v4.port().to_be_bytes()); // port in network byte order
                data.extend_from_slice(&v4.ip().octets()); // IP address
                data.extend_from_slice(&[0u8; 8]); // padding
                client_entity_addr.sockaddr_data = data;
            }
            SocketAddr::V6(v6) => {
                // IPv6: ss_family (2 bytes, little-endian) + port (2 bytes) + flowinfo (4 bytes) + IP (16 bytes) + scope_id (4 bytes)
                let mut data = Vec::with_capacity(28);
                data.extend_from_slice(&10u16.to_le_bytes()); // AF_INET6 = 10
                data.extend_from_slice(&v6.port().to_be_bytes());
                data.extend_from_slice(&0u32.to_be_bytes()); // flowinfo
                data.extend_from_slice(&v6.ip().octets());
                data.extend_from_slice(&v6.scope_id().to_be_bytes());
                client_entity_addr.sockaddr_data = data;
            }
        }
        state_machine.set_client_addr(client_entity_addr);

        // Set our local server address for SERVER_IDENT
        let mut server_entity_addr = denc::EntityAddr::new();
        server_entity_addr.addr_type = denc::EntityAddrType::Msgr2;
        match local_addr {
            SocketAddr::V4(v4) => {
                let mut data = Vec::with_capacity(16);
                data.extend_from_slice(&2u16.to_le_bytes()); // AF_INET = 2
                data.extend_from_slice(&v4.port().to_be_bytes());
                data.extend_from_slice(&v4.ip().octets());
                data.extend_from_slice(&[0u8; 8]); // padding
                server_entity_addr.sockaddr_data = data;
            }
            SocketAddr::V6(v6) => {
                let mut data = Vec::with_capacity(28);
                data.extend_from_slice(&10u16.to_le_bytes()); // AF_INET6 = 10
                data.extend_from_slice(&v6.port().to_be_bytes());
                data.extend_from_slice(&0u32.to_be_bytes()); // flowinfo
                data.extend_from_slice(&v6.ip().octets());
                data.extend_from_slice(&v6.scope_id().to_be_bytes());
                server_entity_addr.sockaddr_data = data;
            }
        }
        state_machine.set_server_addr(server_entity_addr);

        tracing::debug!("✓ Created server state machine");

        // Perform msgr2 banner exchange (server-side: receive first, then send)
        Self::exchange_banner_server(&mut stream, &mut state_machine, &config).await?;

        let state = ConnectionState::new(stream, state_machine);

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
            local_handlers: HashMap::new(),
            message_bus: None,
            recv_task: None,
        })
    }

    /// Accept a session by completing the full msgr2 handshake (server-side)
    ///
    /// This goes through:
    /// 1. Receive HELLO from client, send HELLO response
    /// 2. Receive AUTH_REQUEST, send AUTH_DONE
    /// 3. Receive CLIENT_IDENT, send SERVER_IDENT
    ///
    /// Returns when the connection is ready for message exchange.
    pub async fn accept_session(&mut self) -> Result<()> {
        tracing::info!("Accepting msgr2 session...");

        // Server starts by waiting for HELLO from client
        tracing::debug!("Waiting for HELLO frame from client...");
        let hello_frame = self.state.recv_frame().await?;
        tracing::debug!(
            "✓ Received HELLO frame (tag: {:?})",
            hello_frame.preamble.tag
        );

        // Process HELLO and send response
        match self.state.handle_frame(hello_frame)? {
            StateResult::SendFrame { frame, .. } => {
                tracing::debug!("✓ Sending HELLO response");
                self.state.send_frame(&frame).await?;
            }
            result => {
                return Err(Error::Protocol(format!(
                    "Unexpected HELLO processing result: {:?}",
                    result
                )));
            }
        }

        // Wait for AUTH_REQUEST
        tracing::debug!("Waiting for AUTH_REQUEST from client...");
        let auth_request = self.state.recv_frame().await?;
        tracing::debug!(
            "✓ Received AUTH_REQUEST (tag: {:?})",
            auth_request.preamble.tag
        );

        // Process AUTH_REQUEST and send AUTH_DONE
        match self.state.handle_frame(auth_request)? {
            StateResult::SendFrame { frame, .. } => {
                tracing::debug!("✓ Sending AUTH_DONE");
                self.state.send_frame(&frame).await?;
            }
            result => {
                return Err(Error::Protocol(format!(
                    "Unexpected AUTH processing result: {:?}",
                    result
                )));
            }
        }

        // Wait for CLIENT_IDENT
        tracing::debug!("Waiting for CLIENT_IDENT from client...");
        let client_ident = self.state.recv_frame().await?;
        tracing::debug!(
            "✓ Received CLIENT_IDENT (tag: {:?})",
            client_ident.preamble.tag
        );

        // Process CLIENT_IDENT and send SERVER_IDENT
        match self.state.handle_frame(client_ident)? {
            StateResult::SendFrame { frame, .. } => {
                tracing::debug!("✓ Sending SERVER_IDENT");
                self.state.send_frame(&frame).await?;
            }
            StateResult::Ready => {
                tracing::info!("✓ Session established (server-side)");
                return Ok(());
            }
            result => {
                return Err(Error::Protocol(format!(
                    "Unexpected SESSION processing result: {:?}",
                    result
                )));
            }
        }

        tracing::info!("✓ Session established (server-side)");
        Ok(())
    }

    /// Establish a session by completing the full msgr2 handshake (client-side)
    ///
    /// This goes through:
    /// 1. HELLO exchange
    /// 2. CephX authentication
    /// 3. SESSION_CONNECTING with CLIENT_IDENT/SERVER_IDENT
    ///
    /// Returns when the connection is ready for message exchange.
    pub async fn establish_session(&mut self) -> Result<()> {
        tracing::info!("Establishing msgr2 session...");

        // Enter state machine and send HELLO
        match self.state.enter()? {
            StateResult::SendAndWait { frame, .. } => {
                tracing::debug!("✓ Sending HELLO frame");
                self.state.send_frame(&frame).await?;
            }
            result => {
                return Err(Error::Protocol(format!(
                    "Unexpected initial state result: {:?}",
                    result
                )));
            }
        }

        // Read HELLO response
        tracing::debug!("Reading HELLO response from server...");
        let hello_response = self.state.recv_frame().await?;
        tracing::debug!(
            "✓ Received HELLO response (tag: {:?})",
            hello_response.preamble.tag
        );

        // Process HELLO response
        match self.state.handle_frame(hello_response)? {
            StateResult::SendAndWait { frame, .. } => {
                tracing::debug!("✓ Sending AUTH_REQUEST frame");
                self.state.send_frame(&frame).await?;
            }
            result => {
                return Err(Error::Protocol(format!(
                    "Unexpected HELLO response result: {:?}",
                    result
                )));
            }
        }

        // Handle AUTH exchange (may be multiple rounds)
        tracing::info!("Processing authentication...");
        let mut auth_rounds = 0;
        loop {
            auth_rounds += 1;
            if auth_rounds > 5 {
                return Err(Error::Protocol("Too many auth rounds".to_string()));
            }

            tracing::debug!("Auth round {}", auth_rounds);

            let auth_response = self.state.recv_frame().await?;
            tracing::debug!(
                "✓ Received auth frame (tag: {:?})",
                auth_response.preamble.tag
            );

            match self.state.handle_frame(auth_response)? {
                StateResult::SendAndWait { frame, .. } | StateResult::SendFrame { frame, .. } => {
                    tracing::debug!("  → Sending next auth frame");
                    self.state.send_frame(&frame).await?;

                    // Check if we've transitioned past auth states
                    let state_kind = self.state.current_state_kind();
                    if state_kind.is_authenticated() {
                        tracing::info!(
                            "✓ Authentication and signature exchange completed, now in state: {}",
                            state_kind.as_str()
                        );
                        break;
                    } else if !state_kind.is_auth_state() {
                        tracing::debug!("✓ Transitioned to state: {}", state_kind.as_str());
                        break;
                    }
                    // Continue loop for AUTH_CONNECTING and AUTH_CONNECTING_SIGN states
                }
                StateResult::Transition(_) => {
                    let state_kind = self.state.current_state_kind();
                    tracing::debug!("✓ Transitioned to state: {}", state_kind.as_str());
                    // Only break if we're past the auth states
                    if !state_kind.is_auth_state() {
                        break;
                    }
                }
                result => {
                    return Err(Error::Protocol(format!(
                        "Unexpected auth result: {:?}",
                        result
                    )));
                }
            }
        }

        // Handle compression negotiation if we're in COMPRESSION_CONNECTING state
        if self.state.current_state_kind() == StateKind::CompressionConnecting {
            tracing::debug!(
                "✓ Now in COMPRESSION_CONNECTING state, handling compression negotiation"
            );

            // Read COMPRESSION_DONE response
            let compression_response = self.state.recv_frame().await?;
            tracing::debug!(
                "✓ Received frame (tag: {:?})",
                compression_response.preamble.tag
            );

            // Process compression response and transition to SESSION_CONNECTING
            match self.state.handle_frame(compression_response)? {
                StateResult::SendAndWait { frame, .. } => {
                    tracing::debug!("✓ Transitioned to SESSION_CONNECTING, sending CLIENT_IDENT");
                    self.state.send_frame(&frame).await?;
                }
                result => {
                    return Err(Error::Protocol(format!(
                        "Unexpected compression result: {:?}",
                        result
                    )));
                }
            }
        }

        // Now we should be in SESSION_CONNECTING state and CLIENT_IDENT has been sent
        if self.state.current_state_kind() == StateKind::SessionConnecting {
            tracing::debug!("✓ CLIENT_IDENT sent, waiting for SERVER_IDENT");

            // Loop to handle potential AUTH_SIGNATURE followed by SERVER_IDENT
            loop {
                tracing::debug!("Reading response frame...");
                let response_frame = self.state.recv_frame().await?;
                tracing::debug!("✓ Received frame (tag: {:?})", response_frame.preamble.tag);

                // Process frame
                match self.state.handle_frame(response_frame)? {
                    StateResult::Ready => {
                        tracing::info!("🎉 Session established! Ready for message exchange");
                        break;
                    }
                    StateResult::ReconnectReady { msg_seq } => {
                        tracing::info!(
                            "🎉 Session reconnected! Server acknowledged up to msg_seq={}",
                            msg_seq
                        );

                        // Discard acknowledged messages from sent queue (following Ceph's practice)
                        self.state.discard_acknowledged_messages(msg_seq);

                        // Replay unacknowledged messages (following Ceph's practice)
                        let messages_to_replay: Vec<_> =
                            self.state.session.sent_messages.iter().cloned().collect();

                        if !messages_to_replay.is_empty() {
                            tracing::info!(
                                "Replaying {} unacknowledged messages",
                                messages_to_replay.len()
                            );

                            for msg in messages_to_replay {
                                tracing::debug!(
                                    "Replaying message: type={}, seq={}",
                                    msg.msg_type(),
                                    msg.seq()
                                );

                                // Resend the message with its original sequence number
                                // (following Ceph's practice of preserving message sequence)
                                let msg_frame = MessageFrame::new(
                                    msg.header.clone(),
                                    msg.front.clone(),
                                    msg.middle.clone(),
                                    msg.data.clone(),
                                );

                                let frame = create_frame_from_trait(&msg_frame, Tag::Message);
                                self.state.send_frame(&frame).await?;
                            }

                            tracing::info!("✓ Message replay complete");
                        }

                        break;
                    }
                    StateResult::Continue => {
                        // Frame was handled (e.g., AUTH_SIGNATURE), continue to next frame
                        tracing::debug!("Frame handled, continuing to read next frame");
                        continue;
                    }
                    StateResult::Fault(msg) => {
                        return Err(Error::Protocol(format!("Session setup fault: {}", msg)));
                    }
                    result => {
                        return Err(Error::Protocol(format!(
                            "Unexpected session setup result: {:?}",
                            result
                        )));
                    }
                }
            }
        } else {
            return Err(Error::Protocol(format!(
                "Expected SESSION_CONNECTING, but in: {}",
                self.state.current_state_name()
            )));
        }

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

        tracing::debug!(
            "Reconnecting with: client_cookie={}, server_cookie={}, global_seq={}, connect_seq={}, in_seq={}, out_seq={}, queued_messages={}",
            client_cookie, server_cookie, global_seq, connect_seq, in_seq, out_seq, sent_messages.len()
        );

        // Establish new TCP connection
        let mut stream = TcpStream::connect(self.server_addr).await?;
        tracing::info!("✓ TCP reconnection established to {}", self.server_addr);

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

        // Replace current state with new reconnected state
        self.state = state;

        // Reinitialize throttle for new connection
        self.throttle = self
            .config
            .throttle_config
            .as_ref()
            .map(|cfg| crate::throttle::MessageThrottle::new(cfg.clone()));

        tracing::info!("✓ Session state restored, initiating reconnection handshake");

        // Run the full session establishment (this will use SESSION_RECONNECT since server_cookie != 0)
        self.establish_session().await?;

        tracing::info!("✓ Reconnection complete, session resumed");

        Ok(())
    }

    /// Check if an error is a connection error that warrants reconnection
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

    /// Register a local handler for a message type
    ///
    /// The handler will be called when messages of this type are received on this
    /// connection. Local handlers are checked first before falling back to the global
    /// message bus.
    ///
    /// # Arguments
    ///
    /// * `msg_type` - The message type to register for (e.g., CEPH_MSG_OSD_OPREPLY)
    /// * `dispatcher` - The dispatcher to invoke for this message type
    pub fn register_local_handler(&mut self, msg_type: u16, dispatcher: Arc<dyn Dispatcher>) {
        self.local_handlers.insert(msg_type, dispatcher);
    }

    /// Set the global message bus
    ///
    /// Messages that are not handled by local handlers will be dispatched through
    /// the global message bus. This allows inter-component communication without
    /// tight coupling.
    ///
    /// # Arguments
    ///
    /// * `bus` - The message bus to use for unhandled messages
    pub fn set_message_bus(&mut self, bus: Arc<MessageBus>) {
        self.message_bus = Some(bus);
    }

    /// Start the embedded message loop
    ///
    /// This spawns a background task that continuously receives messages from the
    /// connection and dispatches them to registered handlers. Local handlers are
    /// tried first, then the global message bus if no local handler is found.
    ///
    /// # Implementation Status
    ///
    /// **TODO**: Not yet implemented. Requires architectural refactoring of Connection
    /// to support interior mutability or split send/receive paths. The current design
    /// with `recv_message(&mut self)` cannot be called from a spawned task without
    /// wrapping Connection in Arc<Mutex<...>>.
    ///
    /// Possible approaches:
    /// 1. Wrap ConnectionState in Arc<Mutex<...>> for shared access
    /// 2. Split Connection into send/receive halves (like tokio::net::TcpStream::split)
    /// 3. Use channels between the task and Connection methods
    ///
    /// For now, applications should call recv_message() manually in their own loops.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No handlers (local or global) are configured
    /// - A message is received with no handler registered
    pub fn start(&mut self) -> Result<()> {
        // TODO: Implement message loop once Connection architecture supports it
        Err(Error::Protocol(
            "Embedded message loop not yet implemented - use recv_message() manually".into(),
        ))
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

        // Calculate message size for throttling
        let msg_size = msg.total_size() as usize;

        // Wait for throttle if configured
        if let Some(throttle) = &self.throttle {
            throttle.wait_for_send(msg_size).await;
            tracing::trace!("Throttle check passed for message size {}", msg_size);
        }

        // Increment sequence number (pre-increment, like C++ does with ++out_seq)
        self.state.out_seq += 1;
        msg.header.seq = self.state.out_seq;

        // Piggyback ACK in message header (like Ceph does)
        msg.header.ack_seq = self.state.in_seq;

        let seq = msg.seq();
        let ack_seq = msg.header.get_ack_seq(); // Safe accessor for packed field
        let version = msg.header.get_version();
        let compat_version = msg.header.get_compat_version();
        tracing::debug!(
            "DEBUG: send_message() type=0x{:04x}, seq={}, ack_seq={}, front={}, middle={}, data={}, version={}, compat_version={}",
            msg_type,
            seq,
            ack_seq,
            msg.front.len(),
            msg.middle.len(),
            msg.data.len(),
            version,
            compat_version
        );

        // Record message in sent queue for potential replay (before sending)
        // This follows Ceph's practice of recording before transmission
        self.state.record_sent_message(msg.clone());

        // Convert Message to MessageFrame
        let msg_frame = MessageFrame::new(
            msg.header.clone(),
            msg.front.clone(),
            msg.middle.clone(),
            msg.data.clone(),
        );

        // Create Frame from MessageFrame
        let frame = create_frame_from_trait(&msg_frame, Tag::Message);

        tracing::debug!(
            "DEBUG: Created frame with {} segments",
            frame.segments.len()
        );
        for (i, seg) in frame.segments.iter().enumerate() {
            tracing::debug!("DEBUG:   Segment {}: {} bytes", i, seg.len());
        }

        // Send the frame
        self.state.send_frame(&frame).await?;

        // Record send with throttle
        if let Some(throttle) = &self.throttle {
            throttle.record_send(msg_size).await;
        }

        tracing::debug!(
            "Sent message: type={}, seq={}, ack_seq={}, size={}",
            msg_type,
            seq,
            ack_seq,
            msg_size
        );
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
                    let payload = frame.segments[0].clone();
                    if payload.len() < 8 {
                        return Err(Error::protocol_error(&format!(
                            "ACK frame payload too short: {} bytes",
                            payload.len()
                        )));
                    }

                    let ack_seq = u64::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ]);

                    tracing::debug!("✓ Received ACK frame: seq={}", ack_seq);

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
                        header: header.clone(),
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
                    tracing::trace!("✓ Received Keepalive2Ack frame (processed by state machine)");
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
        let frame = create_frame_from_trait(&keepalive_frame, crate::frames::Tag::Keepalive2);

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
