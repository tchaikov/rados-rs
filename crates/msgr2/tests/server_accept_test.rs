//! Integration tests for msgr2 server-side connection acceptance
//!
//! These tests verify that the server-side implementation can accept
//! incoming connections and complete the msgr2 handshake.

use msgr2::protocol::Connection;
use msgr2::ConnectionConfig;
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
        msgr2::state_machine::StateKind::Ready
    );
    assert_eq!(
        client_conn.current_state_kind(),
        msgr2::state_machine::StateKind::Ready
    );

    tracing::info!("✓ Test passed: Server and client both reached Ready state");
}

/// Test server-side connection acceptance with Cephx authentication
#[tokio::test]
#[ignore] // Requires keyring setup
async fn test_server_accept_with_auth() {
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

        // Create server config with Cephx authentication
        // Note: This test requires proper auth provider setup
        let server_config = ConnectionConfig::prefer_secure_mode();

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

        // Create client config with Cephx authentication
        // Note: This test requires proper auth provider setup
        let client_config = ConnectionConfig::prefer_secure_mode();

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
    assert_eq!(
        server_conn.current_state_kind(),
        msgr2::state_machine::StateKind::Ready
    );
    assert_eq!(
        client_conn.current_state_kind(),
        msgr2::state_machine::StateKind::Ready
    );

    tracing::info!("✓ Test passed: Server and client both reached Ready state with Cephx auth");
}
