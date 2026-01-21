//! Monitor connection wrapper
//!
//! Wraps a msgr2 connection with monitor-specific state.

use crate::error::{MonClientError, Result};
use crate::types::EntityAddrVec;
use msgr2::protocol::Connection as Msgr2Connection;
use msgr2::ConnectionConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Monitor connection state
pub struct MonConnection {
    /// Underlying msgr2 connection (needs interior mutability for send/recv)
    connection: Arc<Mutex<Msgr2Connection>>,

    /// Monitor rank
    rank: usize,

    /// Monitor addresses
    addrs: EntityAddrVec,

    /// Authentication state
    state: Arc<Mutex<ConnectionState>>,
}

#[derive(Debug)]
struct ConnectionState {
    /// Authentication state
    auth_state: AuthState,

    /// Global ID assigned by monitor
    global_id: u64,

    /// Whether we have an active session
    has_session: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthState {
    None,
    Authenticating,
    Authenticated,
}

impl MonConnection {
    /// Create a new monitor connection by connecting to the given address
    pub async fn connect(
        addr: SocketAddr,
        rank: usize,
        addrs: EntityAddrVec,
        keyring_path: Option<String>,
    ) -> Result<Self> {
        tracing::info!("Connecting to monitor rank {} at {}", rank, addr);

        // Create connection config with authentication
        let config = if let Some(keyring) = keyring_path {
            // Load keyring and create auth provider
            let mut mon_auth =
                auth::MonitorAuthProvider::new("client.admin".to_string()).map_err(|e| {
                    MonClientError::MessageError(msgr2::error::Error::Auth(e.to_string()))
                })?;
            mon_auth
                .set_secret_key_from_keyring(&keyring)
                .map_err(|e| {
                    MonClientError::MessageError(msgr2::error::Error::Auth(e.to_string()))
                })?;

            ConnectionConfig::with_auth_provider(Box::new(mon_auth))
        } else {
            // No keyring, use no authentication
            ConnectionConfig::with_no_auth()
        };

        // Connect using msgr2 (banner exchange only)
        let mut connection = Msgr2Connection::connect(addr, config)
            .await
            .map_err(MonClientError::MessageError)?;

        tracing::info!("Banner exchange complete, establishing session...");

        // Complete the full handshake (HELLO, AUTH, SESSION)
        connection
            .establish_session()
            .await
            .map_err(MonClientError::MessageError)?;

        tracing::info!("✓ Session established with monitor rank {}", rank);

        let mon_conn = Self {
            connection: Arc::new(Mutex::new(connection)),
            rank,
            addrs,
            state: Arc::new(Mutex::new(ConnectionState {
                auth_state: AuthState::Authenticated,
                global_id: 0,
                has_session: true,
            })),
        };

        tracing::info!("✓ MonConnection created, connection is wrapped in Arc<Mutex>");

        Ok(mon_conn)
    }

    /// Get the monitor rank
    pub fn rank(&self) -> usize {
        self.rank
    }

    /// Get the monitor addresses
    pub fn addrs(&self) -> &EntityAddrVec {
        &self.addrs
    }

    /// Get a reference to the underlying msgr2 connection
    pub fn connection(&self) -> Arc<Mutex<Msgr2Connection>> {
        Arc::clone(&self.connection)
    }

    /// Check if authenticated
    pub async fn is_authenticated(&self) -> bool {
        let state = self.state.lock().await;
        state.auth_state == AuthState::Authenticated
    }

    /// Check if we have a session
    pub async fn has_session(&self) -> bool {
        let state = self.state.lock().await;
        state.has_session
    }

    /// Get the global ID
    pub async fn global_id(&self) -> u64 {
        let state = self.state.lock().await;
        state.global_id
    }

    /// Set authentication state
    pub async fn set_auth_state(&self, auth_state: AuthState) {
        let mut state = self.state.lock().await;
        state.auth_state = auth_state;
    }

    /// Set global ID
    pub async fn set_global_id(&self, global_id: u64) {
        let mut state = self.state.lock().await;
        state.global_id = global_id;
    }

    /// Mark session as established
    pub async fn set_session(&self, has_session: bool) {
        let mut state = self.state.lock().await;
        state.has_session = has_session;
    }

    /// Send a message to the monitor
    pub async fn send_message(&self, msg: msgr2::message::Message) -> Result<()> {
        tracing::debug!(
            "Sending message type {} to mon.{}",
            msg.msg_type(),
            self.rank
        );
        let mut conn = self.connection.lock().await;
        conn.send_message(msg)
            .await
            .map_err(MonClientError::MessageError)?;
        Ok(())
    }

    /// Receive a message from the monitor
    pub async fn receive_message(&self) -> Result<msgr2::message::Message> {
        let mut conn = self.connection.lock().await;
        let msg = conn
            .recv_message()
            .await
            .map_err(MonClientError::MessageError)?;
        tracing::debug!(
            "Received message type {} from mon.{}",
            msg.msg_type(),
            self.rank
        );
        Ok(msg)
    }

    /// Close the connection
    pub async fn close(&self) -> Result<()> {
        tracing::debug!("Closing connection to mon.{}", self.rank);
        // TODO: Implement proper connection closing
        Ok(())
    }
}

impl std::fmt::Display for MonConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MonConnection(rank={}, addrs={:?})",
            self.rank, self.addrs
        )
    }
}
