//! msgr2 Protocol V2 implementation
//!
//! This module provides a high-level async API for the msgr2 protocol,
//! handling frame I/O, encryption, and state machine coordination.

use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::banner::Banner;
use crate::error::{Error, Result};
use crate::frames::{Frame, MessageFrame, Preamble, Tag};
use crate::header::MsgHeader;
use crate::message::Message;
use crate::state_machine::{create_frame_from_trait, StateKind, StateMachine, StateResult};
use crate::FeatureSet;

/// Frame I/O layer - handles encryption-aware frame send/recv
pub struct FrameIO {
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

    /// Send a frame with optional encryption
    ///
    /// If encryption is enabled in the state machine, this implements msgr2.1 inline optimization:
    /// - First 48 bytes of payload are encrypted together with preamble (32+48=80 → 96 bytes with GCM tag)
    /// - Remaining payload bytes are encrypted separately (N bytes → N+16 bytes with GCM tag)
    pub async fn send_frame(
        &mut self,
        frame: &Frame,
        state_machine: &mut StateMachine,
    ) -> Result<()> {
        // Convert Frame to wire format with proper preamble, segments, epilogue, and CRCs
        // Use msgr2.1 (is_rev1 = true) to match the banner we sent
        let wire_bytes = frame.to_wire(true);

        tracing::debug!(
            "Sending frame: tag={:?}, {} segments, {} wire bytes",
            frame.preamble.tag,
            frame.preamble.num_segments,
            wire_bytes.len()
        );

        // Print segment info
        for (i, seg) in frame.segments.iter().enumerate() {
            tracing::debug!("  Segment {}: {} bytes", i, seg.len());
        }

        // Split into preamble (32 bytes) and payload (rest)
        let preamble_bytes = &wire_bytes[..Self::PREAMBLE_SIZE];
        let mut payload_bytes = wire_bytes[Self::PREAMBLE_SIZE..].to_vec();

        tracing::info!(
            "send_frame: tag={:?}, has_encryption={}, num_segments={}, payload_len={}",
            frame.preamble.tag,
            state_machine.has_encryption(),
            frame.preamble.num_segments,
            payload_bytes.len()
        );

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
            // For single-segment frames in secure mode:
            // 1. Remove CRC + late_status (5 bytes total) added by assemble_crc_rev1
            // 2. Add alignment padding to CRYPTO_BLOCK_SIZE (16 bytes), not segment align
            // Secure frames don't use CRC; they use GCM auth tags instead
            if frame.preamble.num_segments == 1 {
                let segment_len = frame.segments[0].len();
                const CRYPTO_BLOCK_SIZE: usize = 16;

                // Remove CRC (4 bytes) + late_status (1 byte) = 5 bytes total
                // assemble_crc_rev1 adds: segment + CRC (4 bytes) + late_status in some cases
                // But for single segment it's: segment + CRC (4 bytes)
                // Let me just rebuild the payload from scratch
                payload_bytes.clear();
                payload_bytes.extend_from_slice(&frame.segments[0]);

                // Add alignment padding to 16-byte boundary (CRYPTO_BLOCK_SIZE)
                let aligned_len = (segment_len + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1);
                let padding_needed = aligned_len - segment_len;
                if padding_needed > 0 {
                    payload_bytes.extend_from_slice(&vec![0u8; padding_needed]);
                }

                tracing::info!(
                    "Secure single-segment: original={}, aligned={}, padding={}, payload_bytes.len()={}",
                    segment_len,
                    aligned_len,
                    padding_needed,
                    payload_bytes.len()
                );
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
                tracing::info!(
                    "About to encrypt remaining: remaining_data.len()={}, payload_bytes.len()={}, inline_size={}, remaining_size={}",
                    remaining_data.len(),
                    payload_bytes.len(),
                    inline_size,
                    remaining_size
                );
                let encrypted_remaining = state_machine.encrypt_frame_data(remaining_data)?;
                tracing::debug!(
                    "RUST_DEBUG: Encrypted remaining: {} bytes → {} bytes",
                    remaining_data.len(),
                    encrypted_remaining.len()
                );
                tracing::debug!(
                    "RUST_DEBUG: Encrypted remaining hex (first 64 bytes): {}",
                    encrypted_remaining[..encrypted_remaining.len().min(64)]
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join("")
                );
                result.extend_from_slice(&encrypted_remaining);
                tracing::debug!(
                    "RUST_DEBUG: After extending result, total length: {}",
                    result.len()
                );
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
            "RUST_DEBUG: About to write {} bytes to TCP stream for frame tag={:?}",
            final_bytes.len(),
            frame.preamble.tag
        );
        self.stream.write_all(&final_bytes).await?;
        tracing::debug!(
            "RUST_DEBUG: Successfully wrote {} bytes to TCP stream",
            final_bytes.len()
        );
        self.stream.flush().await?;
        tracing::debug!("RUST_DEBUG: Successfully flushed TCP stream");
        Ok(())
    }

    /// Receive a frame with optional decryption
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
        tracing::debug!(
            "About to read {} bytes for preamble block",
            preamble_block_size
        );

        // Try to peek first to see if data is available
        let mut peek_buf = [0u8; 1];
        match self.stream.peek(&mut peek_buf).await {
            Ok(n) => {
                tracing::debug!("Peek successful: {} bytes available", n);
            }
            Err(e) => {
                tracing::error!("Peek failed: {:?}", e);
            }
        }

        match self.stream.read_exact(&mut preamble_block_buf).await {
            Ok(_) => {
                tracing::debug!("Successfully read preamble block");
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

        tracing::debug!("Total segment size: {} bytes", total_segment_size);

        // For secure mode single-segment frames, use 16-byte aligned size (CRYPTO_BLOCK_SIZE)
        // For plaintext or multi-segment frames, add CRC
        let total_payload_size =
            if has_encryption && preamble.num_segments == 1 && total_segment_size > 0 {
                // Secure mode single segment: align to 16-byte boundary (CRYPTO_BLOCK_SIZE)
                const CRYPTO_BLOCK_SIZE: usize = 16;
                let aligned_len =
                    (total_segment_size + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1);
                tracing::debug!(
                    "Secure single-segment RX: logical={}, aligned={}",
                    total_segment_size,
                    aligned_len
                );
                aligned_len
            } else {
                // Plaintext or multi-segment: add CRC
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

        tracing::debug!(
            "Reading additional {} bytes (total_payload={}, inline={}, encryption={})",
            remaining_needed,
            total_payload_size,
            if has_encryption { Self::INLINE_SIZE } else { 0 },
            has_encryption
        );

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

                // For secure single-segment frames, skip 16-byte alignment padding
                if has_encryption && preamble.num_segments == 1 {
                    const CRYPTO_BLOCK_SIZE: usize = 16;
                    let aligned_len =
                        (segment_len + CRYPTO_BLOCK_SIZE - 1) & !(CRYPTO_BLOCK_SIZE - 1);
                    offset += aligned_len;
                } else {
                    offset += segment_len;
                }
            }
        }

        tracing::debug!("Parsed {} segments", segments.len());

        Ok(Frame { preamble, segments })
    }
}

/// Connection state coordination layer
pub struct ConnectionState {
    state_machine: StateMachine,
    frame_io: FrameIO,
}

impl ConnectionState {
    /// Create a new connection state with the given stream and state machine
    pub fn new(stream: TcpStream, state_machine: StateMachine) -> Self {
        let frame_io = FrameIO::new(stream);
        Self {
            state_machine,
            frame_io,
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
}

/// High-level msgr2 Protocol V2 connection
///
/// This provides an async API for establishing msgr2 connections,
/// handling authentication, and exchanging messages.
pub struct Connection {
    state: ConnectionState,
}

impl Connection {
    /// Connect to a Ceph server at the given address
    ///
    /// This establishes a TCP connection and performs the msgr2 banner exchange.
    ///
    /// # Arguments
    /// * `addr` - The server address to connect to
    /// * `config` - Connection configuration (features and connection modes)
    pub async fn connect(addr: SocketAddr, config: crate::ConnectionConfig) -> Result<Self> {
        // Establish TCP connection
        let mut stream = TcpStream::connect(addr).await?;
        tracing::info!("✓ TCP connection established to {}", addr);

        // Get our local address from the connection
        let local_addr = stream.local_addr()?;
        tracing::debug!("Local address: {}", local_addr);

        // Create state machine for client BEFORE banner exchange
        // This is important because we need to track banner bytes in pre-auth buffers
        let mut state_machine = StateMachine::new_client(config.clone());

        // Set the server address for CLIENT_IDENT
        // Convert SocketAddr to EntityAddr with proper sockaddr_storage format
        let mut server_entity_addr = denc::EntityAddr::new();
        server_entity_addr.addr_type = denc::EntityAddrType::Msgr2;
        // Store the socket address in sockaddr_storage format
        match addr {
            SocketAddr::V4(v4) => {
                // IPv4: ss_family (2 bytes, little-endian) + port (2 bytes, big-endian) + IP (4 bytes) + padding (8 bytes)
                let mut data = Vec::with_capacity(16);
                data.extend_from_slice(&2u16.to_le_bytes()); // AF_INET = 2 (native byte order)
                data.extend_from_slice(&v4.port().to_be_bytes()); // port in network byte order
                data.extend_from_slice(&v4.ip().octets()); // IP address
                data.extend_from_slice(&[0u8; 8]); // padding
                server_entity_addr.sockaddr_data = data;
            }
            SocketAddr::V6(v6) => {
                // IPv6: ss_family (2 bytes, little-endian) + port (2 bytes) + flowinfo (4 bytes) + IP (16 bytes) + scope_id (4 bytes)
                let mut data = Vec::with_capacity(28);
                data.extend_from_slice(&10u16.to_le_bytes()); // AF_INET6 = 10 (native byte order)
                data.extend_from_slice(&v6.port().to_be_bytes());
                data.extend_from_slice(&0u32.to_be_bytes()); // flowinfo
                data.extend_from_slice(&v6.ip().octets());
                data.extend_from_slice(&v6.scope_id().to_be_bytes());
                server_entity_addr.sockaddr_data = data;
            }
        }
        state_machine.set_server_addr(server_entity_addr);

        // Set our local client address for CLIENT_IDENT
        let mut client_entity_addr = denc::EntityAddr::new();
        client_entity_addr.addr_type = denc::EntityAddrType::Msgr2;
        match local_addr {
            SocketAddr::V4(v4) => {
                // IPv4: ss_family (2 bytes, little-endian) + port (2 bytes, big-endian) + IP (4 bytes) + padding (8 bytes)
                let mut data = Vec::with_capacity(16);
                data.extend_from_slice(&2u16.to_le_bytes()); // AF_INET = 2 (native byte order)
                data.extend_from_slice(&v4.port().to_be_bytes()); // port in network byte order
                data.extend_from_slice(&v4.ip().octets()); // IP address
                data.extend_from_slice(&[0u8; 8]); // padding
                client_entity_addr.sockaddr_data = data;
            }
            SocketAddr::V6(v6) => {
                // IPv6: ss_family (2 bytes, little-endian) + port (2 bytes) + flowinfo (4 bytes) + IP (16 bytes) + scope_id (4 bytes)
                let mut data = Vec::with_capacity(28);
                data.extend_from_slice(&10u16.to_le_bytes()); // AF_INET6 = 10 (native byte order)
                data.extend_from_slice(&v6.port().to_be_bytes());
                data.extend_from_slice(&0u32.to_be_bytes()); // flowinfo
                data.extend_from_slice(&v6.ip().octets());
                data.extend_from_slice(&v6.scope_id().to_be_bytes());
                client_entity_addr.sockaddr_data = data;
            }
        }
        state_machine.set_client_addr(client_entity_addr);

        tracing::info!("✓ Created client state machine");

        // Perform msgr2 banner exchange and record bytes in pre-auth buffers
        Self::exchange_banner(&mut stream, &mut state_machine, &config).await?;

        let state = ConnectionState::new(stream, state_machine);

        Ok(Self { state })
    }

    /// Perform msgr2 banner exchange
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
        banner.encode(&mut buf);

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

    /// Establish a session by completing the full msgr2 handshake
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
                tracing::info!("✓ Sending HELLO frame");
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
        tracing::info!("Reading HELLO response from server...");
        let hello_response = self.state.recv_frame().await?;
        tracing::info!(
            "✓ Received HELLO response (tag: {:?})",
            hello_response.preamble.tag
        );

        // Process HELLO response
        match self.state.handle_frame(hello_response)? {
            StateResult::SendAndWait { frame, .. } => {
                tracing::info!("✓ Sending AUTH_REQUEST frame");
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

            tracing::info!("Auth round {}", auth_rounds);

            let auth_response = self.state.recv_frame().await?;
            tracing::info!(
                "✓ Received auth frame (tag: {:?})",
                auth_response.preamble.tag
            );

            match self.state.handle_frame(auth_response)? {
                StateResult::SendAndWait { frame, .. } | StateResult::SendFrame { frame, .. } => {
                    tracing::info!("  → Sending next auth frame");
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
                        tracing::info!("✓ Transitioned to state: {}", state_kind.as_str());
                        break;
                    }
                    // Continue loop for AUTH_CONNECTING and AUTH_CONNECTING_SIGN states
                }
                StateResult::Transition(_) => {
                    let state_kind = self.state.current_state_kind();
                    tracing::info!("✓ Transitioned to state: {}", state_kind.as_str());
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
            tracing::info!(
                "✓ Now in COMPRESSION_CONNECTING state, handling compression negotiation"
            );

            // Read COMPRESSION_DONE response
            let compression_response = self.state.recv_frame().await?;
            tracing::info!(
                "✓ Received frame (tag: {:?})",
                compression_response.preamble.tag
            );

            // Process compression response and transition to SESSION_CONNECTING
            match self.state.handle_frame(compression_response)? {
                StateResult::SendAndWait { frame, .. } => {
                    tracing::info!("✓ Transitioned to SESSION_CONNECTING, sending CLIENT_IDENT");
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
            tracing::info!("✓ CLIENT_IDENT sent, waiting for SERVER_IDENT");

            // Loop to handle potential AUTH_SIGNATURE followed by SERVER_IDENT
            loop {
                tracing::info!("Reading response frame...");
                let response_frame = self.state.recv_frame().await?;
                tracing::info!("✓ Received frame (tag: {:?})", response_frame.preamble.tag);

                // Process frame
                match self.state.handle_frame(response_frame)? {
                    StateResult::Ready => {
                        tracing::info!("🎉 Session established! Ready for message exchange");
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

    /// Send a Ceph message over the established session
    pub async fn send_message(&mut self, msg: Message) -> Result<()> {
        let msg_type = msg.msg_type();
        let seq = msg.seq();

        // Convert Message to MessageFrame
        let msg_frame = MessageFrame::new(
            msg.header.clone(),
            msg.front.clone(),
            msg.middle.clone(),
            msg.data.clone(),
        );

        // Create Frame from MessageFrame
        let frame = create_frame_from_trait(&msg_frame, Tag::Message);

        // Send the frame
        self.state.send_frame(&frame).await?;

        tracing::debug!("Sent message: type={}, seq={}", msg_type, seq);
        Ok(())
    }

    /// Receive a Ceph message from the established session
    pub async fn recv_message(&mut self) -> Result<Message> {
        // Receive a frame
        let frame = self.state.recv_frame().await?;

        // Verify it's a Message frame
        if frame.preamble.tag != Tag::Message {
            return Err(Error::protocol_error(&format!(
                "Expected Message frame, got {:?}",
                frame.preamble.tag
            )));
        }

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

        let msg = Message {
            header,
            front,
            middle,
            data,
            footer: None,
        };

        tracing::debug!(
            "Received message: type={}, seq={}",
            msg.msg_type(),
            msg.seq()
        );
        Ok(msg)
    }
}
