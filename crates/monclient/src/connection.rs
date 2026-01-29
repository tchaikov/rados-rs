//! Monitor connection wrapper
//!
//! Wraps a msgr2 connection with monitor-specific state.

use crate::error::{MonClientError, Result};
use crate::types::EntityAddrVec;
use msgr2::protocol::Connection as Msgr2Connection;
use msgr2::ConnectionConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex};

/// Monitor connection state
pub struct MonConnection {
    /// Underlying msgr2 connection (not used, kept for API compatibility)
    #[allow(dead_code)]
    connection: Arc<Mutex<Option<Msgr2Connection>>>,

    /// Monitor rank
    rank: usize,

    /// Monitor addresses
    addrs: EntityAddrVec,

    /// Authentication state
    state: Arc<Mutex<ConnectionState>>,

    /// Authentication provider (stored for creating service auth providers)
    auth_provider: Option<auth::MonitorAuthProvider>,

    /// Channel for sending messages (to avoid deadlock with receive loop)
    send_tx: mpsc::UnboundedSender<msgr2::message::Message>,

    /// Channel for receiving messages
    recv_rx: Arc<Mutex<broadcast::Receiver<msgr2::message::Message>>>,
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
        entity_name: String,
        keyring_path: Option<String>,
    ) -> Result<Self> {
        tracing::info!("Connecting to monitor rank {} at {}", rank, addr);

        // Create connection config with authentication
        let config = if let Some(keyring) = keyring_path {
            // Load keyring and create auth provider
            let mut mon_auth =
                auth::MonitorAuthProvider::new(entity_name.clone()).map_err(|e| {
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

        tracing::debug!("Banner exchange complete, establishing session...");

        // Complete the full handshake (HELLO, AUTH, SESSION)
        connection
            .establish_session()
            .await
            .map_err(MonClientError::MessageError)?;

        tracing::info!("✓ Session established with monitor rank {}", rank);

        // Get the global_id from the connection
        let global_id = connection.global_id();
        tracing::debug!("Retrieved global_id {} from connection", global_id);

        // Get the authenticated auth provider from the connection
        // This provider now contains the session and service tickets
        let auth_provider = connection.get_auth_provider().and_then(|provider| {
            eprintln!("DEBUG: Retrieved auth provider from connection after authentication");
            tracing::debug!("Retrieved auth provider from connection after authentication");
            // Downcast to MonitorAuthProvider
            provider
                .as_any()
                .downcast_ref::<auth::MonitorAuthProvider>()
                .map(|mon_auth| {
                    eprintln!("DEBUG: Successfully downcast to MonitorAuthProvider");
                    tracing::debug!("Successfully downcast to MonitorAuthProvider");
                    mon_auth.clone()
                })
        });

        if auth_provider.is_none() {
            eprintln!("DEBUG: Auth provider is None after authentication!");
            tracing::warn!("Auth provider is None after authentication! Need to fix state machine to preserve auth_provider");
        } else {
            eprintln!("DEBUG: Auth provider is Some after authentication!");
        }

        // Create channels for sending and receiving messages (to avoid deadlock)
        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<msgr2::message::Message>();
        let (recv_tx, recv_rx) = broadcast::channel::<msgr2::message::Message>(100);

        let mut connection_for_task = connection;

        // Spawn a unified task to handle both sending and receiving
        // This avoids deadlock by ensuring only one task accesses the connection
        tokio::spawn(async move {
            tracing::debug!("Send/Receive task started");
            loop {
                tokio::select! {
                    // Handle outgoing messages
                    Some(msg) = send_rx.recv() => {
                        tracing::trace!("Send/Recv task: Sending message");
                        if let Err(e) = connection_for_task.send_message(msg).await {
                            tracing::error!("Failed to send message: {}", e);
                            break;
                        }
                        tracing::trace!("Send/Recv task: Message sent successfully");
                    }
                    // Handle incoming messages
                    result = connection_for_task.recv_message() => {
                        match result {
                            Ok(msg) => {
                                tracing::trace!("Send/Recv task: Received message, broadcasting");
                                let _ = recv_tx.send(msg);
                            }
                            Err(e) => {
                                tracing::error!("Failed to receive message: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
            tracing::debug!("Send/Receive task terminated");
        });

        let mon_conn = Self {
            connection: Arc::new(Mutex::new(None)), // Not used anymore
            rank,
            addrs,
            state: Arc::new(Mutex::new(ConnectionState {
                auth_state: AuthState::Authenticated,
                global_id, // Use the global_id from authentication
                has_session: true,
            })),
            auth_provider,
            send_tx,
            recv_rx: Arc::new(Mutex::new(recv_rx)),
        };

        tracing::debug!("✓ MonConnection created, connection is wrapped in Arc<Mutex>");

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
    /// Note: This returns None now since the connection is managed by a background task
    pub fn connection(&self) -> Arc<Mutex<Option<Msgr2Connection>>> {
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
        // Copy fields from packed struct to avoid alignment issues
        let msg_type = msg.msg_type();
        let tid = msg.header.tid;
        tracing::debug!(
            "MonConnection::send_message: Sending message type 0x{:04x} (tid={}) to mon.{}",
            msg_type,
            tid,
            self.rank
        );
        self.send_tx
            .send(msg)
            .map_err(|_| MonClientError::Other("Send channel closed".into()))?;
        tracing::trace!("MonConnection::send_message: Message queued for sending");
        Ok(())
    }

    /// Receive a message from the monitor
    /// Note: This should only be called by the message receive loop
    pub async fn receive_message(&self) -> Result<msgr2::message::Message> {
        // Receive from the broadcast channel (sent by the unified send/recv task)
        let mut rx = self.recv_rx.lock().await;
        let msg = rx
            .recv()
            .await
            .map_err(|e| MonClientError::Other(format!("Receive channel error: {}", e)))?;
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

    /// Create a ServiceAuthProvider for OSD/MDS/MGR connections
    ///
    /// This creates an authorizer-based auth provider using the service tickets
    /// obtained during monitor authentication. Returns None if no authentication
    /// was used (no-auth cluster).
    pub fn create_service_auth_provider(&self) -> Option<auth::ServiceAuthProvider> {
        eprintln!(
            "DEBUG: create_service_auth_provider called, auth_provider.is_some() = {}",
            self.auth_provider.is_some()
        );
        self.auth_provider.as_ref().map(|mon_auth| {
            eprintln!("DEBUG: Creating ServiceAuthProvider from MonitorAuthProvider");
            auth::ServiceAuthProvider::from_authenticated_handler(mon_auth.handler().clone())
        })
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
