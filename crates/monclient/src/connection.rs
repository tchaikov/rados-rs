//! Monitor connection wrapper
//!
//! Wraps a msgr2 connection with monitor-specific state.

use crate::error::{MonClientError, Result};
use crate::messages::{MOSDMap, CEPH_MSG_OSD_MAP};
use msgr2::io_loop::{run_io_loop, KeepaliveConfig};
use msgr2::protocol::Connection as Msgr2Connection;
use msgr2::{ConnectionConfig, MapSender};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Send channel capacity for connection messages
const CONNECTION_SEND_CHANNEL_CAPACITY: usize = 256;

/// Parameters for creating a monitor connection
#[derive(Clone)]
pub struct MonConnectionParams {
    /// Monitor socket address
    pub addr: SocketAddr,
    /// Monitor rank
    pub rank: usize,
    /// Entity name (e.g., "client.admin")
    pub entity_name: String,
    /// Path to keyring file
    pub keyring_path: Option<String>,
    /// Keepalive policy
    pub keepalive_policy: KeepalivePolicy,
    /// Channel for routing MOSDMap messages to OSDClient
    pub osdmap_tx: Option<MapSender<MOSDMap>>,
    /// Channel for routing monitor messages
    pub mon_msg_tx: mpsc::Sender<msgr2::message::Message>,
}

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

/// Monitor connection
pub struct MonConnection {
    /// Monitor rank
    rank: usize,

    /// Global ID assigned during authentication (immutable after connect)
    global_id: u64,

    /// Authentication provider (stored for creating service auth providers)
    /// Wrapped in Mutex to allow mutable access for ticket renewal
    auth_provider: Option<Arc<Mutex<auth::MonitorAuthProvider>>>,

    /// Channel for sending messages (to avoid deadlock with receive loop)
    send_tx: mpsc::Sender<msgr2::message::Message>,

    /// Background task handle for graceful shutdown and death detection
    task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,

    /// Token for signaling shutdown to background task
    shutdown_token: CancellationToken,
}

impl MonConnection {
    /// Create a new monitor connection by connecting to the given address
    pub async fn connect(params: MonConnectionParams) -> Result<Self> {
        tracing::info!(
            "Connecting to monitor rank {} at {}",
            params.rank,
            params.addr
        );

        // Create connection config with authentication
        let config = if let Some(keyring) = params.keyring_path {
            // Load keyring and create auth provider
            let mut mon_auth =
                auth::MonitorAuthProvider::new(&params.entity_name).map_err(|e| {
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
        let mut connection = Msgr2Connection::connect(params.addr, config)
            .await
            .map_err(MonClientError::MessageError)?;

        tracing::debug!("Banner exchange complete, establishing session...");

        // Complete the full handshake (HELLO, AUTH, SESSION)
        connection
            .establish_session()
            .await
            .map_err(MonClientError::MessageError)?;

        tracing::info!("Session established with monitor rank {}", params.rank);

        // Get the global_id from the connection
        let global_id = connection.global_id();
        tracing::debug!("Retrieved global_id {} from connection", global_id);

        // Get the authenticated auth provider from the connection
        let auth_provider = connection.get_auth_provider().and_then(|provider| {
            tracing::debug!("Retrieved auth provider from connection after authentication");
            provider
                .as_any()
                .downcast_ref::<auth::MonitorAuthProvider>()
                .map(|mon_auth| {
                    tracing::debug!("Successfully downcast to MonitorAuthProvider");
                    mon_auth.clone()
                })
        });

        if auth_provider.is_none() {
            tracing::warn!("Auth provider is None after authentication! Need to fix state machine to preserve auth_provider");
        }

        // Create bounded send channel (monitors are low-rate senders)
        let (send_tx, send_rx) =
            mpsc::channel::<msgr2::message::Message>(CONNECTION_SEND_CHANNEL_CAPACITY);

        let osdmap_tx_for_task = params.osdmap_tx.clone();
        let mon_msg_tx_for_task = params.mon_msg_tx.clone();
        let keepalive_policy = params.keepalive_policy;

        // Convert KeepalivePolicy → KeepaliveConfig for run_io_loop
        let keepalive = keepalive_policy.interval.map(|interval| KeepaliveConfig {
            interval,
            timeout: keepalive_policy.timeout,
        });

        // Create cancellation token for background task
        let shutdown_token = CancellationToken::new();
        let task_shutdown_token = shutdown_token.clone();

        // Spawn unified I/O task using the shared loop
        let handle = tokio::spawn(async move {
            tracing::debug!("MonConnection I/O task started for rank {}", params.rank);

            run_io_loop(
                connection,
                send_rx,
                task_shutdown_token,
                keepalive,
                move |msg| {
                    let osdmap_tx = osdmap_tx_for_task.clone();
                    let mon_msg_tx = mon_msg_tx_for_task.clone();
                    async move {
                        let msg_type = msg.msg_type();
                        match msg_type {
                            CEPH_MSG_OSD_MAP => {
                                // Send to OSDClient if configured
                                if let Some(ref tx) = osdmap_tx {
                                    if let Err(e) = tx.send(msg.clone()).await {
                                        tracing::error!(
                                            "Failed to send MOSDMap to OSDClient: {:?}",
                                            e
                                        );
                                    }
                                }
                                // Also send to MonClient for caching
                                if let Err(e) = mon_msg_tx.send(msg).await {
                                    tracing::error!("Failed to send MOSDMap to MonClient: {}", e);
                                    return false;
                                }
                            }
                            _ => {
                                if let Err(e) = mon_msg_tx.send(msg).await {
                                    tracing::error!(
                                        "Failed to send message 0x{:04x} to MonClient: {}",
                                        msg_type,
                                        e
                                    );
                                    return false;
                                }
                            }
                        }
                        true
                    }
                },
            )
            .await;

            tracing::debug!("MonConnection I/O task terminated for rank {}", params.rank);
        });

        let mon_conn = Self {
            rank: params.rank,
            global_id,
            auth_provider: auth_provider.map(|p| Arc::new(Mutex::new(p))),
            send_tx,
            task_handle: Arc::new(Mutex::new(Some(handle))),
            shutdown_token,
        };

        tracing::debug!("MonConnection created for rank {}", params.rank);

        Ok(mon_conn)
    }

    /// Get the monitor rank
    pub fn rank(&self) -> usize {
        self.rank
    }

    /// Get the global ID assigned during authentication
    pub fn global_id(&self) -> u64 {
        self.global_id
    }

    /// Check if the background I/O task has finished
    ///
    /// Returns true if the task exited for any reason (including unexpected failure).
    /// Used by the tick loop to detect dead connections.
    pub async fn is_task_finished(&self) -> bool {
        let guard = self.task_handle.lock().await;
        guard.as_ref().is_none_or(|h| h.is_finished())
    }

    /// Send a message to the monitor
    pub async fn send_message(&self, msg: msgr2::message::Message) -> Result<()> {
        // Copy fields from packed struct to avoid alignment issues
        tracing::debug!(
            "MonConnection::send_message: Sending message type 0x{:04x} (tid={}) to mon.{}",
            msg.msg_type(),
            msg.tid(),
            self.rank
        );
        self.send_tx
            .send(msg)
            .await
            .map_err(|_| MonClientError::Other("Send channel closed".into()))?;
        tracing::trace!("MonConnection::send_message: Message queued for sending");
        Ok(())
    }

    /// Close the connection
    ///
    /// Signals the background task to terminate and waits for it to exit.
    pub async fn close(&self) -> Result<()> {
        tracing::debug!("Closing connection to mon.{}", self.rank);

        // Signal background task to shutdown
        self.shutdown_token.cancel();

        // Await the background task to ensure it has stopped
        let handle = self.task_handle.lock().await.take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }

        Ok(())
    }

    /// Get a reference to the authentication provider
    ///
    /// Returns None if no authentication was used (no-auth cluster).
    pub fn get_auth_provider(&self) -> Option<Arc<Mutex<auth::MonitorAuthProvider>>> {
        self.auth_provider.as_ref().map(Arc::clone)
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
        write!(f, "MonConnection(rank={})", self.rank)
    }
}
