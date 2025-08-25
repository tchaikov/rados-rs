use rados_rs::{
    config::{CephConfig, CephContext},
    error::Result,
    messenger::{Message, Messenger, MessageHandler, CEPH_MSG_PING, CEPH_MSG_PING_ACK},
    types::{EntityAddr, EntityName, EntityType},
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, Level};
use tracing_subscriber;

struct PingHandler;

#[async_trait::async_trait]
impl MessageHandler for PingHandler {
    fn handle_message(&self, msg: Message, sender: EntityName) -> Result<Option<Message>> {
        match msg.msg_type() {
            CEPH_MSG_PING => {
                info!("Received ping from {}, sending pong", sender);
                Ok(Some(Message::ping_ack().with_seq(msg.seq() + 1)))
            }
            CEPH_MSG_PING_ACK => {
                info!("Received pong from {}", sender);
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
    
    if args.len() < 2 {
        eprintln!("Usage: {} <mode> [options]", args[0]);
        eprintln!("Modes:");
        eprintln!("  server <bind_port>                    - Start as server");
        eprintln!("  client <server_addr:port>             - Connect as client");
        eprintln!("  ping <server_addr:port>               - Send ping and exit");
        eprintln!("  ceph-test                             - Test with local Ceph cluster");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "server" => {
            let port = args.get(2)
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(6789);
            
            run_server(port).await
        }
        "client" => {
            let server_addr = args.get(2)
                .expect("Server address required for client mode")
                .parse::<SocketAddr>()
                .expect("Invalid server address format");
            
            run_client(server_addr).await
        }
        "ping" => {
            let server_addr = args.get(2)
                .expect("Server address required for ping mode")
                .parse::<SocketAddr>()
                .expect("Invalid server address format");
            
            run_ping_client(server_addr).await
        }
        "ceph-test" => {
            run_ceph_test().await
        }
        _ => {
            eprintln!("Invalid mode: {}", args[1]);
            std::process::exit(1);
        }
    }
}

async fn run_server(port: u16) -> Result<()> {
    info!("Starting RADOS-RS server on port {}", port);
    
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let local_name = EntityName::new(EntityType::TYPE_MON, 0);
    
    let mut messenger = Messenger::new(local_name, bind_addr);
    
    // Register ping handler
    messenger.register_handler(CEPH_MSG_PING, Box::new(PingHandler)).await;
    
    messenger.start().await?;
    
    info!("Server started, listening on {}", bind_addr);
    info!("Press Ctrl+C to shutdown");
    
    // Wait for shutdown signal
    tokio::signal::ctrl_c().await
        .expect("Failed to listen for ctrl-c");
    
    info!("Shutdown signal received");
    messenger.shutdown().await?;
    
    Ok(())
}

async fn run_client(server_addr: SocketAddr) -> Result<()> {
    info!("Starting RADOS-RS client, connecting to {}", server_addr);
    
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], 0)); // Bind to random port
    let local_name = EntityName::new(EntityType::TYPE_CLIENT, rand::random::<u64>());
    
    let mut messenger = Messenger::new(local_name, bind_addr);
    
    // Register ping handler for pongs
    messenger.register_handler(CEPH_MSG_PING_ACK, Box::new(PingHandler)).await;
    
    messenger.start().await?;
    
    let peer_addr = EntityAddr::new_with_random_nonce(server_addr);
    let peer_name = EntityName::new(EntityType::TYPE_MON, 0);
    
    // Connect to server
    messenger.connect_to(peer_addr, Some(peer_name)).await?;
    
    info!("Connected to server, sending pings every 5 seconds");
    info!("Press Ctrl+C to shutdown");
    
    // Start ping loop
    let ping_task = {
        let messenger = Arc::new(messenger);
        let ping_messenger = messenger.clone();
        
        tokio::spawn(async move {
            let mut seq = 1u64;
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                let ping = Message::ping().with_seq(seq);
                if let Err(e) = ping_messenger.send_message(peer_name, ping).await {
                    tracing::error!("Failed to send ping: {}", e);
                    break;
                }
                
                info!("Sent ping #{}", seq);
                seq += 1;
            }
        })
    };
    
    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received");
        }
        _ = ping_task => {
            info!("Ping task terminated");
        }
    }
    
    // Note: messenger is wrapped in Arc, so we can't call shutdown directly
    info!("Client shutting down");
    Ok(())
}

async fn run_ping_client(server_addr: SocketAddr) -> Result<()> {
    info!("Sending single ping to {}", server_addr);
    
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], 0)); // Bind to random port  
    let local_name = EntityName::new(EntityType::TYPE_CLIENT, rand::random::<u64>());
    
    let mut messenger = Messenger::new(local_name, bind_addr);
    
    messenger.start().await?;
    
    let peer_addr = EntityAddr::new_with_random_nonce(server_addr);
    let peer_name = EntityName::new(EntityType::TYPE_MON, 0);
    
    // Connect to server
    messenger.connect_to(peer_addr, Some(peer_name)).await?;
    
    // Send single ping
    let ping = Message::ping().with_seq(1);
    messenger.send_message(peer_name, ping).await?;
    
    info!("Ping sent, waiting 2 seconds for response...");
    sleep(Duration::from_secs(2)).await;
    
    messenger.shutdown().await?;
    info!("Ping client finished");
    
    Ok(())
}

async fn run_ceph_test() -> Result<()> {
    info!("Testing with local Ceph cluster");
    
    // Load Ceph configuration
    let config_path = "/home/kefu/dev/rust-app-ceres/docker/ceph-config/ceph.conf";
    info!("Loading config from: {}", config_path);
    
    let config = CephConfig::from_file(config_path)?;
    info!("Config loaded: FSID={}", config.fsid);
    info!("Monitor hosts: {:?}", config.get_mon_addresses());
    
    // Load keyring
    let keyring = config.load_keyring()?;
    info!("Keyring loaded with {} entries", keyring.entries().len());
    
    // Create context for client.admin
    let entity_name = EntityName::new(EntityType::TYPE_CLIENT, 0); // client.admin = client.0
    let context = Arc::new(CephContext::from_config_and_keyring(
        config,
        keyring,
        entity_name,
    ));
    
    info!("Created context for entity: {}", entity_name);
    
    // Create messenger with context
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], 0));
    let mut messenger = Messenger::new(entity_name, bind_addr)
        .with_context(context.clone());
    
    // Register handlers
    messenger.register_handler(CEPH_MSG_PING_ACK, Box::new(PingHandler)).await;
    
    messenger.start().await?;
    info!("Messenger started");
    
    // Connect to first monitor
    let mon_addresses = context.get_mon_addresses();
    if let Some(mon_addr) = mon_addresses.first() {
        info!("Connecting to monitor at {}", mon_addr);
        
        let peer_addr = EntityAddr::new_with_random_nonce(*mon_addr);
        let peer_name = EntityName::new(EntityType::TYPE_MON, 0);
        
        match messenger.connect_to(peer_addr, Some(peer_name)).await {
            Ok(_) => {
                info!("✓ Successfully connected to Ceph monitor!");
                
                // Send a ping to test the connection
                let ping = Message::ping().with_seq(1);
                if let Err(e) = messenger.send_message(peer_name, ping).await {
                    tracing::error!("Failed to send ping: {}", e);
                } else {
                    info!("✓ Ping sent to monitor");
                }
                
                // Wait a bit to see if we get a response
                sleep(Duration::from_secs(3)).await;
                
            }
            Err(e) => {
                tracing::error!("✗ Failed to connect to monitor: {}", e);
            }
        }
    } else {
        tracing::error!("No monitor addresses found in configuration");
    }
    
    messenger.shutdown().await?;
    info!("Ceph test completed");
    Ok(())
}