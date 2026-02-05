//! Monitor connection wrapper
//!
//! Wraps a msgr2 connection with monitor-specific state.

use crate::error::{MonClientError, Result};
use crate::types::EntityAddrVec;
use msgr2::protocol::Connection as Msgr2Connection;
use msgr2::{ConnectionConfig, MessageBus};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// Keepalive policy for the connection
///
/// This determines when the protocol layer sends keepalive frames and
/// when it should notify the upper layer of keepalive timeouts.
#[derive(Debug, Clone)]
pub struct KeepalivePolicy {
    /// Interval between keepalive sends
    /// Set to None to disable keepalive
    pub interval: Option<Duration>,

    /// Timeout for keepalive ACK
    /// If no ACK is received within this duration, the upper layer is notified
    /// Set to None to disable timeout checking
    pub timeout: Option<Duration>,
}

impl KeepalivePolicy {
    /// Create a keepalive policy with specified interval and timeout
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            interval: Some(interval),
            timeout: Some(timeout),
        }
    }

    /// Create a keepalive policy with no keepalive
    pub fn disabled() -> Self {
        Self {
            interval: None,
            timeout: None,
        }
    }
}

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
    /// Wrapped in Mutex to allow mutable access for ticket renewal
    auth_provider: Option<Arc<Mutex<auth::MonitorAuthProvider>>>,

    /// Channel for sending messages (to avoid deadlock with receive loop)
    send_tx: mpsc::UnboundedSender<msgr2::message::Message>,

    /// Channel for receiving keepalive timeout notifications
    /// The background task sends () when a keepalive timeout occurs
    timeout_rx: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,

    /// Message bus for routing ALL messages
    /// ALL messages received from the monitor are dispatched to the MessageBus
    /// Note: Used by the background task closure, so #[allow(dead_code)] is needed
    #[allow(dead_code)]
    message_bus: Arc<MessageBus>,
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
        keepalive_policy: KeepalivePolicy,
        message_bus: Arc<MessageBus>,
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

        // Create channels for sending messages and timeouts
        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<msgr2::message::Message>();
        let (timeout_tx, timeout_rx) = mpsc::unbounded_channel::<()>();

        let mut connection_for_task = connection;

        // Clone message_bus for the background task
        let message_bus_for_task = Arc::clone(&message_bus);

        // Spawn a unified task to handle sending, receiving, and keepalive
        // ALL received messages are forwarded to the MessageBus
        tokio::spawn(async move {
            tracing::debug!("Send/Receive/Keepalive task started");

            // Set up keepalive timer if enabled
            let mut keepalive_interval = keepalive_policy.interval.map(|interval| {
                let mut ticker = tokio::time::interval(interval);
                // Don't fire immediately on first tick
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                ticker
            });

            // Track when we last sent a keepalive (for timeout checking when no ACK received yet)
            let mut last_keepalive_sent: Option<std::time::Instant> = None;

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

                    // Handle incoming messages - forward ALL to MessageBus
                    result = connection_for_task.recv_message() => {
                        match result {
                            Ok(msg) => {
                                let msg_type = msg.header.msg_type;
                                tracing::trace!("Received message type 0x{:04x}, forwarding to MessageBus", msg_type);

                                // Forward ALL messages to MessageBus
                                if let Err(e) = message_bus_for_task.dispatch(msg).await {
                                    tracing::error!("Failed to dispatch message 0x{:04x} to MessageBus: {}", msg_type, e);
                                    // Continue processing other messages even if one fails
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to receive message: {}", e);
                                break;
                            }
                        }
                    }

                    // Handle keepalive timing
                    _ = async {
                        if let Some(ref mut interval) = keepalive_interval {
                            interval.tick().await;
                        } else {
                            // If keepalive is disabled, wait forever
                            std::future::pending::<()>().await;
                        }
                    } => {
                        // First check for keepalive timeout (before sending new one)
                        if let Some(timeout_duration) = keepalive_policy.timeout {
                            // Only check timeout if we've sent at least one keepalive
                            if let Some(sent_time) = last_keepalive_sent {
                                if let Some(last_ack) = connection_for_task.last_keepalive_ack() {
                                    // We have received at least one ACK - check if it's stale
                                    let elapsed = last_ack.elapsed();
                                    if elapsed > timeout_duration {
                                        tracing::warn!(
                                            "Keepalive timeout: no ACK for {:?} (threshold: {:?})",
                                            elapsed,
                                            timeout_duration
                                        );
                                        // Notify upper layer of timeout
                                        let _ = timeout_tx.send(());
                                        // Break the loop to close the connection
                                        break;
                                    }
                                } else {
                                    // No ACK received yet - check if we've been waiting too long
                                    let elapsed_since_send = sent_time.elapsed();
                                    if elapsed_since_send > timeout_duration {
                                        tracing::warn!(
                                            "Keepalive timeout: no initial ACK for {:?} (threshold: {:?})",
                                            elapsed_since_send,
                                            timeout_duration
                                        );
                                        // Notify upper layer of timeout
                                        let _ = timeout_tx.send(());
                                        // Break the loop to close the connection
                                        break;
                                    }
                                }
                            }
                        }

                        // Now send keepalive
                        tracing::trace!("Keepalive interval elapsed, sending keepalive");
                        if let Err(e) = connection_for_task.send_keepalive().await {
                            tracing::error!("Failed to send keepalive: {}", e);
                            break;
                        }
                        last_keepalive_sent = Some(std::time::Instant::now());
                    }
                }
            }
            tracing::debug!("Send/Receive/Keepalive task terminated");
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
            auth_provider: auth_provider.map(|p| Arc::new(Mutex::new(p))),
            send_tx,
            timeout_rx: Arc::new(Mutex::new(timeout_rx)),
            message_bus,
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

    /// Close the connection
    ///
    /// Properly closes the underlying msgr2 connection and clears the session state.
    /// This is a graceful shutdown that allows pending messages to be sent.
    pub async fn close(&self) -> Result<()> {
        tracing::debug!("Closing connection to mon.{}", self.rank);

        // Mark session as closed
        let mut state = self.state.lock().await;
        state.has_session = false;
        state.auth_state = AuthState::None;
        drop(state);

        // Close the underlying msgr2 connection
        let mut connection = self.connection.lock().await;
        if let Some(conn) = connection.take() {
            // The msgr2 Connection will be dropped here, which closes the TCP connection
            tracing::debug!("Closed msgr2 connection to mon.{}", self.rank);
            drop(conn);
        }

        Ok(())
    }

    /// Send a keepalive ping message
    pub async fn send_keepalive(&self) -> Result<()> {
        let ping_msg = msgr2::message::Message::ping();
        self.send_message(ping_msg).await?;
        tracing::trace!("Sent keepalive ping to mon.{}", self.rank);
        Ok(())
    }

    /// Get a reference to the authentication provider
    ///
    /// Returns None if no authentication was used (no-auth cluster).
    pub fn get_auth_provider(&self) -> Option<Arc<Mutex<auth::MonitorAuthProvider>>> {
        self.auth_provider.as_ref().map(Arc::clone)
    }

    /// Reopen the session after a timeout
    ///
    /// This marks the connection as closed and returns an error,
    /// which will trigger the client to reconnect.
    pub async fn reopen_session(&self) -> Result<()> {
        tracing::warn!(
            "Keepalive timeout on mon.{}, marking connection as closed",
            self.rank
        );

        // Mark session as lost
        let mut state = self.state.lock().await;
        state.has_session = false;

        // Close the underlying connection
        let mut connection = self.connection.lock().await;
        *connection = None;

        // Return an error to trigger reconnection at the MonClient level
        Err(MonClientError::Other(
            "Connection timed out, need to reconnect".into(),
        ))
    }

    /// Try to receive a keepalive timeout notification (non-blocking)
    ///
    /// Returns Some(()) if a keepalive timeout has occurred, None otherwise.
    /// The upper layer should call reopen_session() when this returns Some(()).
    pub async fn try_recv_timeout(&self) -> Option<()> {
        let mut rx = self.timeout_rx.lock().await;
        rx.try_recv().ok()
    }

    /// Create a ServiceAuthProvider for OSD/MDS/MGR connections
    ///
    /// This creates an authorizer-based auth provider using the service tickets
    /// obtained during monitor authentication. The handler is shared via Arc<Mutex<>>
    /// so that when MonClient renews tickets, all ServiceAuthProviders automatically
    /// see the updated tickets. Returns None if no authentication was used (no-auth cluster).
    pub async fn create_service_auth_provider(&self) -> Option<auth::ServiceAuthProvider> {
        if let Some(mon_auth_arc) = &self.auth_provider {
            let mon_auth = mon_auth_arc.lock().await;
            // Share the handler with ServiceAuthProvider via Arc<Mutex<>>
            // This ensures ticket renewals are automatically visible
            Some(auth::ServiceAuthProvider::from_shared_handler(
                std::sync::Arc::clone(mon_auth.handler()),
            ))
        } else {
            None
        }
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
