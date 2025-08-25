use rados_rs::{
    auth::{CephXKey, SimpleCephXProtocol},
    config::{CephConfig, CephContext},
    messenger::{Banner, ConnectReplyMessage, Frame, FrameTag, Messenger, MessageHandler, Message, CEPH_MSG_PING, CEPH_MSG_PING_ACK},
    types::{EntityAddr, EntityName, EntityType, FeatureSet},
    Result,
};
use bytes::{BufMut, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, info, Level};

// Mock server that implements basic Ceph messenger protocol for testing
pub struct MockCephServer {
    listener: TcpListener,
    addr: SocketAddr,
}

impl MockCephServer {
    pub async fn new() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        
        Ok(Self { listener, addr })
    }
    
    pub fn address(&self) -> SocketAddr {
        self.addr
    }
    
    pub async fn run(self) -> std::io::Result<()> {
        info!("Mock Ceph server running on {}", self.addr);
        
        loop {
            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("Mock server accepted connection from {}", peer_addr);
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream).await {
                            tracing::error!("Error handling client {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Error accepting connection: {}", e);
                    break;
                }
            }
        }
        
        Ok(())
    }
}

async fn handle_client(mut stream: TcpStream) -> Result<()> {
    debug!("Handling new client connection");
    
    // Step 1: Banner exchange
    let mut banner_buf = vec![0u8; 7]; // "ceph v2" length
    stream.read_exact(&mut banner_buf).await?;
    
    let banner_str = String::from_utf8_lossy(&banner_buf);
    debug!("Received banner: {}", banner_str);
    
    // Send banner back
    stream.write_all(b"ceph v2").await?;
    debug!("Sent banner response");
    
    // Step 2: Connect message
    let mut connect_buf = vec![0u8; 32]; // ConnectMessage::LENGTH
    stream.read_exact(&mut connect_buf).await?;
    debug!("Received connect message");
    
    // Send connect reply
    let reply = ConnectReplyMessage::ready(FeatureSet::MSGR2, 0, 0);
    let mut reply_buf = BytesMut::new();
    reply.encode(&mut reply_buf)?;
    stream.write_all(&reply_buf).await?;
    debug!("Sent connect reply");
    
    // Step 3: Authentication flow (simplified)
    let mut frame_buf = vec![0u8; 1024];
    
    loop {
        // Try to read frame header first
        let mut header_buf = vec![0u8; Frame::HEADER_SIZE];
        match timeout(Duration::from_secs(5), stream.read_exact(&mut header_buf)).await {
            Ok(Ok(_)) => {
                let (tag, payload_len) = Frame::decode_header(&mut &header_buf[..])?;
                debug!("Received frame: {:?}, payload_len: {}", tag, payload_len);
                
                // Read payload
                if payload_len > 0 {
                    let mut payload = vec![0u8; payload_len as usize];
                    stream.read_exact(&mut payload).await?;
                    debug!("Read payload of {} bytes", payload_len);
                }
                
                match tag {
                    FrameTag::Auth => {
                        debug!("Handling auth request");
                        
                        // Send mock auth reply with global ID
                        let mut auth_reply = BytesMut::new();
                        auth_reply.put_i32_le(0); // Success
                        auth_reply.put_u64_le(12345); // Global ID
                        
                        let auth_frame = Frame::auth_reply(auth_reply.freeze());
                        let mut frame_buf = BytesMut::new();
                        auth_frame.encode(&mut frame_buf)?;
                        
                        stream.write_all(&frame_buf).await?;
                        debug!("Sent auth reply");
                    }
                    FrameTag::AuthDone => {
                        debug!("Received auth done - authentication completed");
                        break;
                    }
                    FrameTag::MessageFrame => {
                        debug!("Received message frame - connection established");
                        // Echo back any messages for testing
                        let echo_frame = Frame::new(FrameTag::MessageFrame, bytes::Bytes::from("echo"));
                        let mut echo_buf = BytesMut::new();
                        echo_frame.encode(&mut echo_buf)?;
                        stream.write_all(&echo_buf).await?;
                    }
                    _ => {
                        debug!("Unhandled frame type: {:?}", tag);
                    }
                }
            }
            Ok(Err(e)) => {
                debug!("Connection closed: {}", e);
                break;
            }
            Err(_) => {
                debug!("Timeout waiting for data, closing connection");
                break;
            }
        }
    }
    
    debug!("Client connection finished");
    Ok(())
}

struct TestPingHandler;

#[async_trait::async_trait]
impl MessageHandler for TestPingHandler {
    fn handle_message(&self, msg: Message, sender: EntityName) -> Result<Option<Message>> {
        match msg.msg_type() {
            CEPH_MSG_PING => {
                info!("Test handler received ping from {}", sender);
                Ok(Some(Message::ping_ack().with_seq(msg.seq() + 1)))
            }
            CEPH_MSG_PING_ACK => {
                info!("Test handler received pong from {}", sender);
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let args: Vec<String> = std::env::args().collect();
    
    match args.get(1).map(|s| s.as_str()) {
        Some("mock-server") => {
            run_mock_server().await
        }
        Some("test-auth") => {
            run_auth_test().await
        }
        Some("full-test") => {
            run_full_integration_test().await
        }
        _ => {
            eprintln!("Usage: {} <command>", args[0]);
            eprintln!("Commands:");
            eprintln!("  mock-server    - Run mock Ceph server for testing");
            eprintln!("  test-auth      - Test authentication with mock server");
            eprintln!("  full-test      - Run full integration test");
            std::process::exit(1);
        }
    }
}

async fn run_mock_server() -> Result<()> {
    let server = MockCephServer::new().await?;
    let addr = server.address();
    
    println!("Mock Ceph server starting on {}", addr);
    println!("Connect with: cargo run --bin test_runner test-auth");
    println!("Press Ctrl+C to stop");
    
    server.run().await?;
    Ok(())
}

async fn run_auth_test() -> Result<()> {
    info!("Starting authentication test");
    
    // Create a simple config for testing
    let entity_name = EntityName::new(EntityType::TYPE_CLIENT, 0);
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    
    // Create messenger without real config (for testing)
    let mut messenger = Messenger::new(entity_name, bind_addr);
    messenger.register_handler(CEPH_MSG_PING_ACK, Box::new(TestPingHandler)).await;
    messenger.start().await?;
    
    info!("Test messenger started");
    
    // Try to connect to mock server on default port
    let mock_server_addr = SocketAddr::from(([127, 0, 0, 1], 6789));
    let peer_addr = EntityAddr::new_with_random_nonce(mock_server_addr);
    let peer_name = EntityName::new(EntityType::TYPE_MON, 0);
    
    match messenger.connect_to(peer_addr, Some(peer_name)).await {
        Ok(_) => {
            info!("✓ Successfully connected to mock server!");
            
            // Send test ping
            let ping = Message::ping().with_seq(1);
            if let Err(e) = messenger.send_message(peer_name, ping).await {
                tracing::error!("Failed to send ping: {}", e);
            } else {
                info!("✓ Ping sent successfully");
            }
            
            // Wait a bit
            sleep(Duration::from_secs(2)).await;
        }
        Err(e) => {
            tracing::error!("✗ Failed to connect to mock server: {}", e);
            println!("Make sure to start the mock server first: cargo run --bin test_runner mock-server");
        }
    }
    
    messenger.shutdown().await?;
    Ok(())
}

async fn run_full_integration_test() -> Result<()> {
    info!("Starting full integration test");
    
    // Start mock server
    let server = MockCephServer::new().await?;
    let server_addr = server.address();
    
    info!("Started mock server on {}", server_addr);
    
    // Run server in background
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            tracing::error!("Mock server error: {}", e);
        }
    });
    
    // Give server time to start
    sleep(Duration::from_millis(100)).await;
    
    // Start client
    let entity_name = EntityName::new(EntityType::TYPE_CLIENT, 0);
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    
    let mut messenger = Messenger::new(entity_name, bind_addr);
    messenger.register_handler(CEPH_MSG_PING_ACK, Box::new(TestPingHandler)).await;
    messenger.start().await?;
    
    info!("Started test client");
    
    // Connect to mock server
    let peer_addr = EntityAddr::new_with_random_nonce(server_addr);
    let peer_name = EntityName::new(EntityType::TYPE_MON, 0);
    
    match messenger.connect_to(peer_addr, Some(peer_name)).await {
        Ok(_) => {
            info!("✓ Integration test: Connection successful");
            
            // Send test message
            let ping = Message::ping().with_seq(42);
            messenger.send_message(peer_name, ping).await?;
            info!("✓ Integration test: Message sent");
            
            // Wait for response
            sleep(Duration::from_secs(1)).await;
            
            println!("✓ Full integration test completed successfully!");
        }
        Err(e) => {
            tracing::error!("✗ Integration test failed: {}", e);
        }
    }
    
    messenger.shutdown().await?;
    server_handle.abort();
    
    Ok(())
}