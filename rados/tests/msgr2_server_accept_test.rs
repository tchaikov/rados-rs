//! Integration tests for msgr2 server-side connection acceptance
//!
//! These tests verify that the server-side implementation can accept
//! incoming connections and complete the msgr2 handshake.

use rados::msgr2::Connection;
use rados::msgr2::{ConnectionConfig, ConnectionMode};
use std::time::Duration;
use tokio::net::TcpListener;

/// Test basic server-side connection acceptance
#[tokio::test]
async fn test_server_accept_basic() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    // Bind to a random port on localhost
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    tracing::info!("Server listening on {}", server_addr);

    // Spawn server task
    let server_handle = tokio::spawn(async move {
        tracing::info!("Server: Waiting for connection...");
        let (stream, peer_addr) = listener.accept().await.unwrap();
        tracing::info!("Server: Accepted connection from {}", peer_addr);

        // Create server config with no authentication for simplicity
        let server_config = ConnectionConfig::with_no_auth();

        // Accept the connection (no auth handler for this test)
        let mut server_conn = Connection::accept(stream, server_config, None)
            .await
            .unwrap();
        tracing::info!("Server: Banner exchange complete");

        // Complete the handshake
        server_conn.accept_session().await.unwrap();
        tracing::info!("Server: Session established");

        server_conn
    });

    // Give server time to start listening
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Spawn client task
    let client_handle = tokio::spawn(async move {
        tracing::info!("Client: Connecting to {}", server_addr);

        // Create client config with no authentication
        let client_config = ConnectionConfig::with_no_auth();

        // Connect to server
        let mut client_conn = Connection::connect(server_addr, client_config)
            .await
            .unwrap();
        tracing::info!("Client: Banner exchange complete");

        // Complete the handshake
        client_conn.establish_session().await.unwrap();
        tracing::info!("Client: Session established");

        client_conn
    });

    // Wait for both to complete
    let (server_result, client_result) = tokio::join!(server_handle, client_handle);

    let server_conn = server_result.unwrap();
    let client_conn = client_result.unwrap();

    // Verify both connections are in Ready state
    tracing::info!("Server state: {}", server_conn.current_state_name());
    tracing::info!("Client state: {}", client_conn.current_state_name());

    // Both should be in Ready state
    assert_eq!(
        server_conn.current_state_kind(),
        rados::msgr2::StateKind::Ready
    );
    assert_eq!(
        client_conn.current_state_kind(),
        rados::msgr2::StateKind::Ready
    );

    tracing::info!("✓ Test passed: Server and client both reached Ready state");
}

/// Accept one connection with no auth handler, returning how its session
/// setup ended.
async fn serve_one(listener: TcpListener) -> Result<(), String> {
    let (stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
    let mut conn = Connection::accept(stream, ConnectionConfig::with_no_auth(), None)
        .await
        .map_err(|e| e.to_string())?;
    conn.accept_session().await.map_err(|e| e.to_string())
}

/// Connect with `config` and set up a session, returning how it ended.
async fn connect_one(addr: std::net::SocketAddr, config: ConnectionConfig) -> Result<(), String> {
    let mut conn = Connection::connect(addr, config)
        .await
        .map_err(|e| e.to_string())?;
    conn.establish_session().await.map_err(|e| e.to_string())
}

/// A client that allows only SECURE has no mode to offer a monitor
/// without authentication, since AUTH_NONE offers CRC alone, so it fails
/// before it sends AUTH_REQUEST, as C++ `MonConnection::get_auth_request`
/// does.
#[tokio::test]
async fn test_secure_only_client_without_auth_fails() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_one(listener));

    let client_config = ConnectionConfig {
        preferred_modes: vec![ConnectionMode::Secure],
        ..ConnectionConfig::with_no_auth()
    };
    let client = tokio::time::timeout(
        Duration::from_secs(10),
        connect_one(server_addr, client_config),
    )
    .await
    .expect("client timed out");
    let err = client.expect_err("a SECURE-only client must not connect without auth");
    assert!(err.contains("no connection mode"), "{err}");

    let served = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("server timed out")
        .unwrap();
    assert!(served.is_err(), "the server must not reach a session");
}

/// A client that allows only SECURE offers an OSD no mode without
/// authentication. The rados-rs server, unlike a C++ one, answers with
/// CRC, which the client refuses because it did not offer it.
#[tokio::test]
async fn test_service_client_refuses_a_mode_it_did_not_offer() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_one(listener));

    let client_config = ConnectionConfig {
        preferred_modes: vec![ConnectionMode::Secure],
        service_id: rados::EntityType::OSD.bits(),
        global_id: 4242,
        ..ConnectionConfig::with_no_auth()
    };
    let client = tokio::time::timeout(
        Duration::from_secs(10),
        connect_one(server_addr, client_config),
    )
    .await
    .expect("client timed out");
    let err = client.expect_err("the client must refuse a mode it did not offer");
    assert!(err.contains("did not offer"), "{err}");

    let served = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("server timed out")
        .unwrap();
    assert!(served.is_err(), "the server must not reach a session");
}
