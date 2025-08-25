use bytes::{Bytes, BytesMut};
use crate::auth::{CephXClientAuth, SimpleCephXProtocol};
use crate::config::CephContext;
use crate::error::{Error, Result};
use crate::messenger::{Banner, Frame, FrameTag, Message, ConnectMessage, ConnectReplyMessage, CEPH_BANNER_LEN};
use crate::messenger::frames::{MsgFrame, Tag, HelloFrame, AuthRequestFrame, ClientIdentFrame, Preamble};
use crate::types::{ConnectionInfo, ConnectionState, EntityAddr, EntityName, FeatureSet};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};
use rand;

const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

pub struct Connection {
    stream: Arc<Mutex<TcpStream>>,
    state: Arc<Mutex<ConnectionState>>,
    info: Arc<Mutex<ConnectionInfo>>,
    outgoing_tx: mpsc::UnboundedSender<Message>,
    incoming_rx: Arc<Mutex<mpsc::UnboundedReceiver<Message>>>,
    local_addr: EntityAddr,
    local_name: EntityName,
    features: FeatureSet,
    auth_session: Arc<Mutex<Option<CephXClientAuth>>>,
}

impl Connection {
    pub async fn connect(
        remote_addr: EntityAddr,
        local_addr: EntityAddr,
        local_name: EntityName,
        features: FeatureSet,
        context: Option<Arc<CephContext>>,
    ) -> Result<Self> {
        info!("Connecting to {} as {}", remote_addr, local_name);
        
        let stream = timeout(
            Duration::from_secs(10),
            TcpStream::connect(remote_addr.addr)
        ).await
        .map_err(|_| Error::Timeout)?
        .map_err(Error::from)?;

        let stream = Arc::new(Mutex::new(stream));
        let state = Arc::new(Mutex::new(ConnectionState::Connecting));
        let info = Arc::new(Mutex::new(ConnectionInfo {
            peer_addr: remote_addr,
            peer_name: None,
            features: FeatureSet::EMPTY,
            lossy: false,
        }));

        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let incoming_rx = Arc::new(Mutex::new(incoming_rx));

        let mut connection = Self {
            stream: stream.clone(),
            state: state.clone(),
            info: info.clone(),
            outgoing_tx,
            incoming_rx,
            local_addr,
            local_name,
            features,
            auth_session: Arc::new(Mutex::new(None)),
        };

        // Perform handshake
        connection.handshake(context).await?;

        // Start I/O tasks
        connection.start_io_tasks(outgoing_rx, incoming_tx).await;

        Ok(connection)
    }

    pub async fn accept(
        stream: TcpStream,
        local_addr: EntityAddr,
        local_name: EntityName,
        features: FeatureSet,
    ) -> Result<Self> {
        let peer_addr = stream.peer_addr()
            .map_err(Error::from)?;
        
        info!("Accepting connection from {} as {}", peer_addr, local_name);
        
        let stream = Arc::new(Mutex::new(stream));
        let state = Arc::new(Mutex::new(ConnectionState::Accepting));
        let info = Arc::new(Mutex::new(ConnectionInfo {
            peer_addr: EntityAddr::new_with_random_nonce(peer_addr),
            peer_name: None,
            features: FeatureSet::EMPTY,
            lossy: false,
        }));

        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let incoming_rx = Arc::new(Mutex::new(incoming_rx));

        let mut connection = Self {
            stream: stream.clone(),
            state: state.clone(),
            info: info.clone(),
            outgoing_tx,
            incoming_rx,
            local_addr,
            local_name,
            features,
            auth_session: Arc::new(Mutex::new(None)),
        };

        // Perform handshake (server side)
        connection.accept_handshake().await?;

        // Start I/O tasks
        connection.start_io_tasks(outgoing_rx, incoming_tx).await;

        Ok(connection)
    }

    async fn handshake(&mut self, context: Option<Arc<CephContext>>) -> Result<()> {
        debug!("Starting client handshake");
        
        // Send banner with our supported features
        let banner = Banner::new_with_features(self.features, FeatureSet::EMPTY);
        let mut buf = BytesMut::new();
        banner.encode(&mut buf);
        debug!("Sending banner with features: supported={:x}, required={:x} ({} bytes)", 
               banner.supported_features.value(), banner.required_features.value(), buf.len());
        debug!("Banner content: {:02x?}", &buf[..]);
        
        {
            let mut stream = self.stream.lock().await;
            timeout(WRITE_TIMEOUT, stream.write_all(&buf)).await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from)?;
        }
        debug!("Banner sent successfully");

        // Receive complete banner including payload
        // Banner format: "ceph v2\n" + payload_size (uint16_t) + payload data
        // First, read the prefix and payload size to determine total size
        let mut prefix_and_size_buf = vec![0u8; CEPH_BANNER_LEN + 1 + 2]; // "ceph v2\n" + payload_size
        debug!("Waiting for banner prefix and size ({} bytes)", prefix_and_size_buf.len());
        {
            let mut stream = self.stream.lock().await;
            match timeout(READ_TIMEOUT, stream.read_exact(&mut prefix_and_size_buf)).await {
                Ok(Ok(_)) => {
                    debug!("Received banner prefix and size ({} bytes)", prefix_and_size_buf.len());
                    debug!("Banner prefix and size: {:02x?}", &prefix_and_size_buf[..]);
                }
                Ok(Err(e)) => {
                    error!("Failed to read banner prefix and size: {}", e);
                    return Err(Error::from(e));
                }
                Err(_) => {
                    error!("Timeout waiting for banner prefix and size");
                    return Err(Error::Timeout);
                }
            }
        }
        
        // Extract payload size from the last 2 bytes
        let payload_size = u16::from_le_bytes([
            prefix_and_size_buf[CEPH_BANNER_LEN + 1], 
            prefix_and_size_buf[CEPH_BANNER_LEN + 1 + 1]
        ]) as usize;
        debug!("Banner payload size: {}", payload_size);
        
        // Read the payload data
        let mut payload_buf = vec![0u8; payload_size];
        debug!("Waiting for banner payload ({} bytes)", payload_size);
        {
            let mut stream = self.stream.lock().await;
            match timeout(READ_TIMEOUT, stream.read_exact(&mut payload_buf)).await {
                Ok(Ok(_)) => {
                    debug!("Received banner payload ({} bytes)", payload_buf.len());
                    debug!("Banner payload: {:02x?}", &payload_buf[..]);
                }
                Ok(Err(e)) => {
                    error!("Failed to read banner payload: {}", e);
                    return Err(Error::from(e));
                }
                Err(_) => {
                    error!("Timeout waiting for banner payload");
                    return Err(Error::Timeout);
                }
            }
        }
        
        // Combine all banner data for decoding
        let mut complete_banner = Vec::new();
        complete_banner.extend_from_slice(&prefix_and_size_buf);
        complete_banner.extend_from_slice(&payload_buf);
        
        let mut banner_bytes = &complete_banner[..];
        let peer_banner = Banner::decode(&mut banner_bytes)?;
        debug!("Decoded peer banner: supported_features={:x}, required_features={:x}", 
               peer_banner.supported_features.value(), peer_banner.required_features.value());
        
        let local_addr = self.stream.lock().await.local_addr()?;
        
        // 2. HELLO_CONNECTING: Exchange HELLO frames
        debug!("State: HELLO_CONNECTING - Sending HELLO frame");
        
        let hello_frame = HelloFrame::new(self.local_name.entity_type, EntityAddr::new_with_random_nonce(local_addr));
        let hello_msg_frame = hello_frame.to_frame().map_err(|e| Error::Protocol(e.to_string()))?;
        let hello_encoded = hello_msg_frame.encode();
        
        debug!("Sending HELLO frame ({} bytes)", hello_encoded.len());
        debug!("HELLO frame content: {:02x?}", &hello_encoded[..]);
        {
            let mut stream = self.stream.lock().await;
            timeout(WRITE_TIMEOUT, stream.write_all(&hello_encoded)).await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from)?;
        }
        debug!("HELLO frame sent successfully");

        // Read HELLO response
        debug!("Waiting for HELLO response frame");
        let hello_response = self.read_frame().await?;
        match hello_response.preamble.tag {
            Tag::Hello => {
                debug!("Received HELLO frame - proceeding to auth");
                let server_hello = HelloFrame::decode(hello_response.payload)?;
                debug!("Server hello: entity_type={:?}, peer_addr={}", server_hello.entity_type, server_hello.peer_addr);
            }
            _ => {
                error!("Expected HELLO frame, got: {:?}", hello_response.preamble.tag);
                return Err(Error::Protocol(format!("Expected HELLO frame, got: {:?}", hello_response.preamble.tag)));
            }
        }
        
        // 3. AUTH_CONNECTING: Send AUTH_REQUEST frame after hello exchange
        if let Some(ctx) = &context {
            if ctx.config.is_cephx_enabled() {
                debug!("State: AUTH_CONNECTING - Sending AUTH_REQUEST frame for CephX");
                
                let auth_request = SimpleCephXProtocol::create_auth_request(&ctx.entity_name);
                let auth_frame = AuthRequestFrame::new(
                    1, // CEPHX method
                    vec![1], // CEPHX preferred modes  
                    auth_request,
                );
                let auth_msg_frame = auth_frame.to_frame().map_err(|e| Error::Protocol(e.to_string()))?;
                let auth_encoded = auth_msg_frame.encode();
                
                debug!("Sending AUTH_REQUEST frame ({} bytes)", auth_encoded.len());
                debug!("AUTH_REQUEST frame content: {:02x?}", &auth_encoded[..]);
                {
                    let mut stream = self.stream.lock().await;
                    timeout(WRITE_TIMEOUT, stream.write_all(&auth_encoded)).await
                        .map_err(|_| Error::Timeout)?
                        .map_err(Error::from)?;
                }
                debug!("AUTH_REQUEST frame sent successfully");

                // 4. AUTH_CONNECTING_SIGN: Exchange AUTH_SIGNATURE frames
                debug!("State: AUTH_CONNECTING_SIGN - Processing auth signatures");
                
                // Read AUTH response frames
                loop {
                    let auth_response = self.read_frame().await?;
                    debug!("Received auth response frame: {:?}", auth_response.preamble.tag);
                    
                    match auth_response.preamble.tag {
                        Tag::AuthReplyMore => {
                            debug!("Received AUTH_REPLY_MORE - continuing auth");
                            // TODO: Handle more auth steps if needed
                        }
                        Tag::AuthSignature => {
                            debug!("Received AUTH_SIGNATURE - verifying HMAC");
                            // TODO: Verify signature against session HMAC
                            // For now, assume success and send our signature back
                            
                            // Create and send our auth signature
                            let auth_sig_frame = MsgFrame::new(Tag::AuthSignature, Bytes::new());
                            let auth_sig_encoded = auth_sig_frame.encode();
                            
                            debug!("Sending AUTH_SIGNATURE frame ({} bytes)", auth_sig_encoded.len());
                            {
                                let mut stream = self.stream.lock().await;
                                timeout(WRITE_TIMEOUT, stream.write_all(&auth_sig_encoded)).await
                                    .map_err(|_| Error::Timeout)?
                                    .map_err(Error::from)?;
                            }
                            debug!("AUTH_SIGNATURE frame sent successfully");
                        }
                        Tag::AuthDone => {
                            debug!("Received AUTH_DONE - authentication successful");
                            break;
                        }
                        Tag::AuthBadMethod => {
                            error!("Authentication failed: bad method");
                            return Err(Error::Authentication("Auth bad method".to_string()));
                        }
                        _ => {
                            warn!("Unexpected auth response frame: {:?}", auth_response.preamble.tag);
                        }
                    }
                }
            }
        }

        // 5. COMPRESSION_CONNECTING: Handle compression negotiation if supported
        if self.features.has_compression() {
            debug!("State: COMPRESSION_CONNECTING - Negotiating compression");
            // TODO: Implement compression negotiation
            // For now, skip compression
        }

        // 6. SESSION_CONNECTING: Send CLIENT_IDENT frame
        debug!("State: SESSION_CONNECTING - Sending CLIENT_IDENT frame");
        let peer_addr = {
            let info = self.info.lock().await;
            info.peer_addr.clone()
        };
        let client_ident_frame = ClientIdentFrame {
            addrs: vec![EntityAddr::new_with_random_nonce(local_addr)],
            target_addr: peer_addr,
            gid: 0, // Will be set during auth
            global_seq: 0,
            supported_features: self.features.value(),
            required_features: 0,
            flags: 0,
            cookie: rand::random::<u64>(),
        };
        
        let client_ident_msg_frame = client_ident_frame.to_frame().map_err(|e| Error::Protocol(e.to_string()))?;
        let ident_encoded = client_ident_msg_frame.encode();
        
        debug!("Sending CLIENT_IDENT frame ({} bytes)", ident_encoded.len());
        {
            let mut stream = self.stream.lock().await;
            timeout(WRITE_TIMEOUT, stream.write_all(&ident_encoded)).await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from)?;
        }
        debug!("CLIENT_IDENT frame sent successfully");

        // Read SERVER_IDENT response
        debug!("Waiting for SERVER_IDENT frame");
        let server_ident_frame = self.read_frame().await?;
        match server_ident_frame.preamble.tag {
            Tag::ServerIdent => {
                debug!("Received SERVER_IDENT - connection established successfully");
            }
            Tag::IdentMissingFeatures => {
                error!("Connection failed: missing features");
                return Err(Error::Protocol("Missing required features".to_string()));
            }
            Tag::Wait => {
                debug!("Server requested wait");
                // Handle wait if needed
            }
            _ => {
                warn!("Unexpected server response: {:?}", server_ident_frame.preamble.tag);
            }
        }

        // Prepare connect message (with potential auth)
        let connect_msg = ConnectMessage::new(self.features, self.local_name.entity_type.value());
        debug!("Created connect message with features: {:?}, entity_type: {}", 
               self.features, self.local_name.entity_type.value());

        // Now that clock skew is fixed, let's try proper CephX auth
        let (final_connect_msg, auth_data): (ConnectMessage, Option<bytes::Bytes>) = if let Some(ctx) = &context {
            if ctx.config.is_cephx_enabled() {
                debug!("CephX is required, preparing auth data for connect message");
                
                // Create initial auth request data that will be sent as authorizer
                let auth_request = SimpleCephXProtocol::create_auth_request(&ctx.entity_name);
                debug!("Created auth request: {:02x?}", &auth_request[..]);
                let connect_with_auth = connect_msg.with_auth(1, auth_request.len() as u32);
                debug!("Added CephX auth to connect message (auth_len={})", auth_request.len());
                
                (connect_with_auth, Some(auth_request))
            } else {
                debug!("CephX not enabled in config");
                (connect_msg, None)
            }
        } else {
            debug!("No context provided - connecting without auth");
            (connect_msg, None)
        };

        // Original auth implementation - commented out for now
        // let (final_connect_msg, auth_data) = if let Some(ctx) = &context {
        //     if ctx.config.is_cephx_enabled() {
        //         debug!("CephX is required, preparing auth data for connect message");
        //         
        //         // Create initial auth request data that will be sent as authorizer
        //         let auth_request = SimpleCephXProtocol::create_auth_request(&ctx.entity_name);
        //         debug!("Created auth request: {:02x?}", &auth_request[..]);
        //         let connect_with_auth = connect_msg.with_auth(1, auth_request.len() as u32);
        //         debug!("Added CephX auth to connect message (auth_len={})", auth_request.len());
        //         
        //         (connect_with_auth, Some(auth_request))
        //     } else {
        //         debug!("CephX not enabled in config");
        //         (connect_msg, None)
        //     }
        // } else {
        //     debug!("No context provided - connecting without auth");
        //     (connect_msg, None)
        // };

        // Send connect message
        let mut buf = BytesMut::new();
        final_connect_msg.encode(&mut buf)?;
        
        // Add auth data if present
        if let Some(auth_bytes) = &auth_data {
            buf.extend_from_slice(auth_bytes);
            debug!("Appended {} bytes of auth data", auth_bytes.len());
        }
        
        debug!("Sending connect message ({} bytes), expected {}", buf.len(), ConnectMessage::LENGTH + auth_data.as_ref().map(|d| d.len()).unwrap_or(0));
        debug!("Connect message content: {:02x?}", &buf[..]);
        
        {
            let mut stream = self.stream.lock().await;
            timeout(WRITE_TIMEOUT, stream.write_all(&buf)).await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from)?;
        }
        debug!("Connect message sent successfully");

        // Give the server a moment to process our request
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Receive connect reply
        let mut reply_buf = vec![0u8; ConnectReplyMessage::LENGTH];
        debug!("Waiting for connect reply (expected {} bytes)", ConnectReplyMessage::LENGTH);
        {
            let mut stream = self.stream.lock().await;
            match timeout(READ_TIMEOUT, stream.read_exact(&mut reply_buf)).await {
                Ok(Ok(_)) => {
                    debug!("Received connect reply ({} bytes)", reply_buf.len());
                }
                Ok(Err(e)) => {
                    error!("Failed to read connect reply: {}", e);
                    return Err(Error::from(e));
                }
                Err(_) => {
                    error!("Timeout waiting for connect reply");
                    return Err(Error::Timeout);
                }
            }
        }

        let mut reply_bytes = &reply_buf[..];
        let reply = ConnectReplyMessage::decode(&mut reply_bytes)?;
        debug!("Decoded connect reply: {:?}", reply);

        // Handle different reply types
        match reply.tag {
            ConnectReplyMessage::REPLY_TAG_READY => {
                debug!("Connect reply indicates ready state");
            }
            ConnectReplyMessage::REPLY_TAG_BADAUTHORIZER => {
                debug!("Server requires authentication - this is expected");
                return Err(Error::Authentication("Server requires authentication - need to implement proper CephX handshake".into()));
            }
            _ => {
                error!("Connect reply not ready: {}", reply);
                return Err(Error::HandshakeFailed(format!(
                    "Connect reply not ready: {}", reply
                )));
            }
        }

        // Authentication is now handled in the connect message with authorizer data
        // No separate auth phase needed for basic CephX
        debug!("Authentication completed via connect message authorizer");

        // TODO: Handle more complex auth scenarios if connect reply requires it
        // if reply.tag == ConnectReplyMessage::REPLY_TAG_BADAUTHORIZER {
        //     // Handle auth failure
        // }

        // Update connection info
        {
            let mut info = self.info.lock().await;
            info.features = reply.features;
        }

        {
            let mut state = self.state.lock().await;
            *state = ConnectionState::Connected;
        }

        info!("Client handshake completed successfully");
        Ok(())
    }

    /// Read a complete msgr2 frame from the stream
    async fn read_frame(&self) -> Result<MsgFrame> {
        // Read frame preamble (36 bytes)
        let mut preamble_buf = vec![0u8; 36];
        {
            let mut stream = self.stream.lock().await;
            timeout(READ_TIMEOUT, stream.read_exact(&mut preamble_buf)).await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from)?;
        }
        
        // Parse preamble to get payload size
        let preamble = Preamble::decode(Bytes::from(preamble_buf))
            .map_err(|e| Error::Protocol(e.to_string()))?;
        let payload_size = preamble.segments[0].length as usize;
        
        // Read payload if present
        let payload = if payload_size > 0 {
            let mut payload_buf = vec![0u8; payload_size];
            {
                let mut stream = self.stream.lock().await;
                timeout(READ_TIMEOUT, stream.read_exact(&mut payload_buf)).await
                    .map_err(|_| Error::Timeout)?
                    .map_err(Error::from)?;
            }
            Bytes::from(payload_buf)
        } else {
            Bytes::new()
        };
        
        debug!("Read frame: tag={:?}, payload_size={}", preamble.tag, payload_size);
        Ok(MsgFrame { preamble, payload })
    }

    async fn authenticate(
        &mut self,
        context: &CephContext,
    ) -> Result<()> {
        debug!("Starting CephX authentication with new handshake");

        // Get our key from keyring
        let entity_key = context.get_entity_key()
            .ok_or_else(|| Error::Authentication("No key found for entity".into()))?
            .clone();
        debug!("Retrieved entity key for authentication");

        // Create CephX client authenticator
        let mut auth = CephXClientAuth::new(context.entity_name, entity_key);
        debug!("Created CephX client auth for entity: {}", context.entity_name);
        
        // Step 1: Send initial auth request
        let auth_data = SimpleCephXProtocol::create_auth_request(&context.entity_name);
        debug!("Created auth request data ({} bytes)", auth_data.len());
        let auth_frame = Frame::auth_request(auth_data);
        let mut frame_buf = BytesMut::new();
        auth_frame.encode(&mut frame_buf)?;
        debug!("Encoded auth frame ({} bytes)", frame_buf.len());
        
        {
            let mut stream = self.stream.lock().await;
            timeout(WRITE_TIMEOUT, stream.write_all(&frame_buf)).await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from)?;
        }

        debug!("Sent initial auth request");

        // Step 2: Read auth response
        debug!("Reading auth response frame...");
        let auth_reply_data = self.read_auth_frame().await?;
        debug!("Received auth reply data ({} bytes)", auth_reply_data.len());
        
        // Parse the auth reply to get global ID
        let (result, global_id) = SimpleCephXProtocol::parse_auth_reply(&auth_reply_data)?;
        debug!("Parsed auth reply: result={}, global_id={:?}", result, global_id);
        
        if result != 0 {
            error!("Initial auth failed with result: {}", result);
            return Err(Error::Authentication(format!("Initial auth failed: {}", result)));
        }
        
        if let Some(gid) = global_id {
            auth.global_id = Some(gid);
            debug!("Received global ID: {}", gid);
        } else {
            debug!("No global ID received in auth reply");
        }

        // Step 3: Start CephX key exchange
        let mut next_request = auth.start_auth()?;
        
        // Continue the authentication handshake
        loop {
            // Send auth request
            let auth_frame = Frame::auth_request(next_request);
            let mut frame_buf = BytesMut::new();
            auth_frame.encode(&mut frame_buf)?;
            
            {
                let mut stream = self.stream.lock().await;
                timeout(WRITE_TIMEOUT, stream.write_all(&frame_buf)).await
                    .map_err(|_| Error::Timeout)?
                    .map_err(Error::from)?;
            }

            debug!("Sent auth request in state: {:?}", auth.state);

            // Read response
            let response_data = self.read_auth_frame().await?;
            
            // Handle response
            match auth.handle_auth_reply(&response_data)? {
                Some(next_req) => {
                    next_request = next_req;
                    debug!("Continuing auth handshake");
                }
                None => {
                    debug!("Auth handshake completed");
                    break;
                }
            }
            
            if auth.is_failed() {
                return Err(Error::Authentication("CephX handshake failed".into()));
            }
        }

        if !auth.is_completed() {
            return Err(Error::Authentication("CephX handshake not completed".into()));
        }

        // Send auth done
        let auth_done_data = SimpleCephXProtocol::create_auth_done();
        let done_frame = Frame::auth_done();
        let mut done_buf = BytesMut::new();
        done_frame.encode(&mut done_buf)?;
        
        {
            let mut stream = self.stream.lock().await;
            timeout(WRITE_TIMEOUT, stream.write_all(&done_buf)).await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from)?;
        }

        // Store the authentication session
        *self.auth_session.lock().await = Some(auth);

        debug!("CephX authentication completed successfully");
        Ok(())
    }

    async fn read_auth_frame(&self) -> Result<Bytes> {
        debug!("Reading auth frame header...");
        // Read frame header
        let mut frame_header_buf = vec![0u8; Frame::HEADER_SIZE];
        {
            let mut stream = self.stream.lock().await;
            match timeout(READ_TIMEOUT, stream.read_exact(&mut frame_header_buf)).await {
                Ok(Ok(_)) => {
                    debug!("Read auth frame header ({} bytes)", frame_header_buf.len());
                }
                Ok(Err(e)) => {
                    error!("Failed to read auth frame header: {}", e);
                    return Err(Error::from(e));
                }
                Err(_) => {
                    error!("Timeout reading auth frame header");
                    return Err(Error::Timeout);
                }
            }
        }

        let mut header_bytes = &frame_header_buf[..];
        let (tag, payload_len) = Frame::decode_header(&mut header_bytes)?;
        debug!("Decoded auth frame header: tag={:?}, payload_len={}", tag, payload_len);

        // Validate frame type
        match tag {
            FrameTag::AuthReply | FrameTag::AuthBad => {
                debug!("Received auth frame: {:?}", tag);
            }
            _ => {
                error!("Unexpected auth frame type: {:?}", tag);
                return Err(Error::Authentication(format!("Unexpected auth frame type: {:?}", tag)));
            }
        }

        // Read frame payload
        debug!("Reading auth frame payload ({} bytes)...", payload_len);
        let mut payload_buf = vec![0u8; payload_len as usize];
        {
            let mut stream = self.stream.lock().await;
            match timeout(READ_TIMEOUT, stream.read_exact(&mut payload_buf)).await {
                Ok(Ok(_)) => {
                    debug!("Read auth frame payload ({} bytes)", payload_buf.len());
                }
                Ok(Err(e)) => {
                    error!("Failed to read auth frame payload: {}", e);
                    return Err(Error::from(e));
                }
                Err(_) => {
                    error!("Timeout reading auth frame payload");
                    return Err(Error::Timeout);
                }
            }
        }

        if tag == FrameTag::AuthBad {
            error!("Server rejected authentication");
            return Err(Error::Authentication("Server rejected authentication".into()));
        }

        debug!("Auth frame read successfully");
        Ok(Bytes::from(payload_buf))
    }

    async fn accept_handshake(&mut self) -> Result<()> {
        debug!("Starting server handshake");
        
        let mut stream = self.stream.lock().await;
        
        // Receive banner
        let mut banner_buf = vec![0u8; Banner::new().banner.len()];
        timeout(READ_TIMEOUT, stream.read_exact(&mut banner_buf)).await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::from)?;

        let mut banner_bytes = &banner_buf[..];
        let _peer_banner = Banner::decode(&mut banner_bytes)?;

        // Send banner
        let banner = Banner::new();
        let mut buf = BytesMut::new();
        banner.encode(&mut buf);
        
        timeout(WRITE_TIMEOUT, stream.write_all(&buf)).await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::from)?;

        // Receive connect message
        let mut connect_buf = vec![0u8; ConnectMessage::LENGTH];
        timeout(READ_TIMEOUT, stream.read_exact(&mut connect_buf)).await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::from)?;

        let mut connect_bytes = &connect_buf[..];
        let connect_msg = ConnectMessage::decode(&mut connect_bytes)?;

        // Send connect reply
        let negotiated_features = self.features.intersection(connect_msg.features);
        let reply = ConnectReplyMessage::ready(negotiated_features, 0, 0);
        
        let mut buf = BytesMut::new();
        reply.encode(&mut buf)?;
        
        timeout(WRITE_TIMEOUT, stream.write_all(&buf)).await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::from)?;

        // Update connection info
        {
            let mut info = self.info.lock().await;
            info.features = negotiated_features;
        }

        {
            let mut state = self.state.lock().await;
            *state = ConnectionState::Connected;
        }

        info!("Server handshake completed successfully");
        Ok(())
    }

    async fn start_io_tasks(
        &self,
        outgoing_rx: mpsc::UnboundedReceiver<Message>,
        incoming_tx: mpsc::UnboundedSender<Message>,
    ) {
        let stream_read = self.stream.clone();
        let stream_write = self.stream.clone();
        let state = self.state.clone();

        // Spawn reader task
        let state_read = state.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::read_loop(stream_read, incoming_tx, state_read).await {
                error!("Read loop error: {}", e);
            }
        });

        // Spawn writer task
        let state_write = state.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::write_loop(stream_write, outgoing_rx, state_write).await {
                error!("Write loop error: {}", e);
            }
        });

        // Spawn keepalive task
        let keepalive_tx = self.outgoing_tx.clone();
        tokio::spawn(async move {
            Self::keepalive_loop(keepalive_tx).await;
        });
    }

    async fn read_loop(
        stream: Arc<Mutex<TcpStream>>,
        incoming_tx: mpsc::UnboundedSender<Message>,
        state: Arc<Mutex<ConnectionState>>,
    ) -> Result<()> {
        let mut buf = BytesMut::with_capacity(8192);
        
        loop {
            {
                let current_state = *state.lock().await;
                if current_state == ConnectionState::Closed {
                    break;
                }
            }

            let mut stream = stream.lock().await;
            
            // Read frame header
            buf.reserve(Frame::HEADER_SIZE);
            if buf.len() < Frame::HEADER_SIZE {
                let bytes_read = timeout(READ_TIMEOUT, stream.read_buf(&mut buf)).await
                    .map_err(|_| Error::Timeout)?
                    .map_err(Error::from)?;
                
                if bytes_read == 0 {
                    debug!("Connection closed by peer");
                    break;
                }
            }

            if buf.len() < Frame::HEADER_SIZE {
                continue;
            }

            let (tag, payload_len) = Frame::decode_header(&mut buf.clone())?;
            
            // Read complete frame
            let total_frame_len = Frame::HEADER_SIZE + payload_len as usize;
            if buf.len() < total_frame_len {
                buf.reserve(total_frame_len - buf.len());
                while buf.len() < total_frame_len {
                    let bytes_read = timeout(READ_TIMEOUT, stream.read_buf(&mut buf)).await
                        .map_err(|_| Error::Timeout)?
                        .map_err(Error::from)?;
                    
                    if bytes_read == 0 {
                        return Err(Error::Connection("Unexpected EOF".into()));
                    }
                }
            }

            // Parse frame
            let frame = Frame::decode_payload(&mut buf, tag, payload_len)?;
            
            match frame.tag {
                FrameTag::MessageFrame => {
                    // Decode message from frame payload
                    let msg = Message::decode(&mut frame.payload.as_ref())?;
                    debug!("Received: {}", msg);
                    
                    if let Err(_) = incoming_tx.send(msg) {
                        warn!("Failed to send message to handler (receiver dropped)");
                        break;
                    }
                }
                FrameTag::KeepAlive2 => {
                    debug!("Received keepalive");
                    // TODO: Send keepalive ack
                }
                FrameTag::KeepAlive2Ack => {
                    debug!("Received keepalive ack");
                }
                _ => {
                    debug!("Received frame: {}", frame);
                }
            }
        }

        {
            let mut current_state = state.lock().await;
            *current_state = ConnectionState::Closed;
        }

        Ok(())
    }

    async fn write_loop(
        stream: Arc<Mutex<TcpStream>>,
        mut outgoing_rx: mpsc::UnboundedReceiver<Message>,
        state: Arc<Mutex<ConnectionState>>,
    ) -> Result<()> {
        while let Some(msg) = outgoing_rx.recv().await {
            {
                let current_state = *state.lock().await;
                if current_state == ConnectionState::Closed {
                    break;
                }
            }

            debug!("Sending: {}", msg);

            let mut msg_buf = BytesMut::new();
            msg.encode(&mut msg_buf)?;

            let frame = Frame::new(FrameTag::MessageFrame, msg_buf.freeze());
            let mut frame_buf = BytesMut::new();
            frame.encode(&mut frame_buf)?;

            let mut stream = stream.lock().await;
            timeout(WRITE_TIMEOUT, stream.write_all(&frame_buf)).await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from)?;
        }

        Ok(())
    }

    async fn keepalive_loop(outgoing_tx: mpsc::UnboundedSender<Message>) {
        let mut interval = tokio::time::interval(KEEPALIVE_INTERVAL);
        
        loop {
            interval.tick().await;
            
            let ping = Message::ping();
            if let Err(_) = outgoing_tx.send(ping) {
                break; // Connection closed
            }
            
            debug!("Sent keepalive ping");
        }
    }

    pub async fn send_message(&self, msg: Message) -> Result<()> {
        self.outgoing_tx.send(msg)
            .map_err(|_| Error::Connection("Connection closed".into()))
    }

    pub async fn recv_message(&self) -> Result<Message> {
        let mut rx = self.incoming_rx.lock().await;
        rx.recv().await.ok_or_else(|| Error::Connection("Connection closed".into()))
    }

    pub async fn close(&self) -> Result<()> {
        {
            let mut state = self.state.lock().await;
            *state = ConnectionState::Closed;
        }
        
        info!("Connection closed");
        Ok(())
    }

    pub async fn state(&self) -> ConnectionState {
        *self.state.lock().await
    }

    pub async fn info(&self) -> ConnectionInfo {
        self.info.lock().await.clone()
    }

    pub fn local_addr(&self) -> EntityAddr {
        self.local_addr.clone()
    }

    pub fn local_name(&self) -> EntityName {
        self.local_name
    }
}