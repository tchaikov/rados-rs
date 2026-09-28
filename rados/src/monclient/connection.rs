//! Monitor connection wrapper
//!
//! Wraps a msgr2 connection with monitor-specific state.

use crate::monclient::error::{MonClientError, Result};
use crate::monclient::messages::MOSDMap;
use crate::msgr2::io_loop::{KeepaliveConfig, run_io_loop};
use crate::msgr2::protocol::Connection as Msgr2Connection;
use crate::msgr2::{ConnectionConfig, MapSender};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
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
    /// Optional authentication provider (CephX, None, etc.)
    pub auth_provider: Option<crate::auth::MonitorAuthProvider>,
    /// Keepalive policy
    pub keepalive_policy: KeepalivePolicy,
    /// Channel for routing MOSDMap messages to OSDClient
    pub osdmap_tx: Option<MapSender<MOSDMap>>,
    /// Channel for routing monitor messages
    pub mon_msg_tx: mpsc::Sender<crate::msgr2::message::Message>,
    /// Connection modes to offer the monitor, in order of preference
    pub connection_modes: Vec<crate::msgr2::ConnectionMode>,
}

impl MonConnectionParams {
    fn connection_config(&self) -> ConnectionConfig {
        let mut config = if let Some(auth_provider) = &self.auth_provider {
            ConnectionConfig::with_auth_provider(Box::new(auth_provider.clone()))
        } else {
            ConnectionConfig::with_no_auth()
        };
        config.preferred_modes = self.connection_modes.clone();
        // Monitor connections are lossy (mirrors librados Policy::lossy_client):
        // the client does not replay messages on reconnect; it reconnects fresh.
        config.is_lossy = true;
        config
    }
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
    rank: usize,
    /// Immutable after connect
    global_id: u64,
    /// Ceph session features negotiated during the ident exchange.
    peer_supported_features: u64,
    auth_provider: Option<crate::auth::MonitorAuthProvider>,
    send_tx: mpsc::Sender<crate::msgr2::message::Message>,
    /// I/O task handle for graceful shutdown.
    ///
    /// `MonConnection` is always stored behind an outer `Arc<MonConnection>`,
    /// so the field itself does not need a second refcount — a bare `Mutex`
    /// suffices to coordinate the set-at-spawn / take-in-shutdown handoff.
    /// The `Option` marks "already taken" so double-shutdowns are no-ops.
    task_handle: Mutex<Option<JoinHandle<()>>>,
    shutdown_token: CancellationToken,
    /// Used for blocklist detection.
    client_addr: crate::EntityAddr,
}

impl MonConnection {
    /// Create a new monitor connection by connecting to the given address
    pub async fn connect(params: MonConnectionParams) -> Result<Self> {
        tracing::info!(
            "Connecting to monitor rank {} at {}",
            params.rank,
            params.addr
        );

        // Connect using msgr2 (banner exchange only)
        let mut connection =
            Msgr2Connection::connect(params.addr, params.connection_config()).await?;

        tracing::debug!("Banner exchange complete, establishing session...");

        // Complete the full handshake (HELLO, AUTH, SESSION)
        connection.establish_session().await?;

        tracing::info!("Session established with monitor rank {}", params.rank);

        // Get the global_id and our own address from the connection
        let global_id = connection.global_id();
        let peer_supported_features = connection.negotiated_features();
        let client_addr = connection.client_addr();
        tracing::debug!("Retrieved global_id {} from connection", global_id);
        tracing::debug!(
            "Negotiated monitor features 0x{:x} (stateful_sub={})",
            peer_supported_features,
            (peer_supported_features & crate::denc::features::CephFeatures::SERVER_JEWEL.bits())
                != 0
        );

        // Extract the MonitorAuthProvider from the dynamic auth provider
        let auth_provider = connection.get_auth_provider().and_then(|provider| {
            provider
                .as_any()
                .downcast_ref::<crate::auth::MonitorAuthProvider>()
                .cloned()
        });

        if auth_provider.is_none() {
            tracing::debug!(
                "No MonitorAuthProvider available (no-auth cluster or auth not configured)"
            );
        }

        // Create bounded send channel (monitors are low-rate senders)
        let (send_tx, send_rx) =
            mpsc::channel::<crate::msgr2::message::Message>(CONNECTION_SEND_CHANNEL_CAPACITY);

        let rank = params.rank;
        let osdmap_tx_for_task = params.osdmap_tx;
        let mon_msg_tx_for_task = params.mon_msg_tx;
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
            tracing::debug!("MonConnection I/O task started for rank {}", rank);

            run_io_loop(
                connection,
                send_rx,
                task_shutdown_token,
                keepalive,
                |_| true, // monitor messages are never filtered
                move |msg| {
                    let osdmap_tx = osdmap_tx_for_task.clone();
                    let mon_msg_tx = mon_msg_tx_for_task.clone();
                    async move {
                        // Forward OSDMap messages to OSDClient if configured
                        if msg.msg_type() == crate::msgr2::message::CEPH_MSG_OSD_MAP
                            && let Some(ref tx) = osdmap_tx
                            && let Err(e) = tx.send(msg.clone()).await
                        {
                            tracing::error!("Failed to send MOSDMap to OSDClient: {:?}", e);
                        }

                        // All messages (including OSDMap) are sent to MonClient
                        if let Err(e) = mon_msg_tx.send(msg).await {
                            tracing::error!("Failed to send message to MonClient: {}", e);
                            return false;
                        }
                        true
                    }
                },
            )
            .await;

            tracing::debug!("MonConnection I/O task terminated for rank {}", rank);
        });

        let mon_conn = Self {
            rank,
            global_id,
            peer_supported_features,
            auth_provider,
            send_tx,
            task_handle: Mutex::new(Some(handle)),
            shutdown_token,
            client_addr,
        };

        tracing::debug!("MonConnection created for rank {}", rank);

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

    /// Whether the connected monitor supports stateful subscriptions.
    pub fn supports_stateful_subscriptions(&self) -> bool {
        (self.peer_supported_features & crate::denc::features::CephFeatures::SERVER_JEWEL.bits())
            != 0
    }

    /// Get our own entity address (as sent in CLIENT_IDENT).
    /// Used to check against the OSDMap blocklist.
    pub fn client_addr(&self) -> &crate::EntityAddr {
        &self.client_addr
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
    pub async fn send_message(&self, msg: crate::msgr2::message::Message) -> Result<()> {
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

    /// Returns a clone of the inner handler Arc, shared with ServiceAuthProviders.
    ///
    /// Ticket renewals written through this Arc are visible to all
    /// ServiceAuthProviders because they hold the same Arc.
    pub fn get_auth_handler(
        &self,
    ) -> Option<std::sync::Arc<std::sync::Mutex<crate::auth::client::CephXClientHandler>>> {
        self.auth_provider
            .as_ref()
            .map(|p| std::sync::Arc::clone(p.handler()))
    }

    /// Create a ServiceAuthProvider for OSD/MDS/MGR connections
    ///
    /// Returns None if no authentication was used (no-auth cluster).
    pub fn create_service_auth_provider(&self) -> Option<crate::auth::ServiceAuthProvider> {
        self.auth_provider.as_ref().map(|p| {
            crate::auth::ServiceAuthProvider::from_shared_handler(std::sync::Arc::clone(
                p.handler(),
            ))
        })
    }
}

impl std::fmt::Display for MonConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MonConnection(rank={})", self.rank)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgr2::ConnectionMode::{Crc, Secure};

    #[test]
    fn connection_config_offers_the_configured_modes() {
        let (mon_msg_tx, _mon_msg_rx) = mpsc::channel(1);
        let auth_provider = crate::auth::MonitorAuthProvider::new("client.admin").unwrap();
        for auth_provider in [None, Some(auth_provider)] {
            for modes in [vec![Secure], vec![Secure, Crc]] {
                let params = MonConnectionParams {
                    addr: "127.0.0.1:3300".parse().unwrap(),
                    rank: 0,
                    auth_provider: auth_provider.clone(),
                    keepalive_policy: KeepalivePolicy::disabled(),
                    osdmap_tx: None,
                    mon_msg_tx: mon_msg_tx.clone(),
                    connection_modes: modes.clone(),
                };
                assert_eq!(params.connection_config().preferred_modes, modes);
            }
        }
    }
}
