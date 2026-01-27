//! OSD session management
//!
//! This module handles per-OSD connection and request tracking.
//!
//! ## Design
//!
//! Following the Linux kernel and Ceph C++ async messenger pattern:
//! - `submit_op()` quickly queues messages to a channel (no blocking I/O)
//! - A dedicated `io_task()` owns the Connection and multiplexes send/receive
//! - No locks held during I/O operations

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, error, info, warn};

use crate::error::{OSDClientError, Result};
use crate::messages::{MOSDOp, MOSDOpReply};
use crate::types::{OpResult, RequestId};
use msgr2::ceph_message::{CephMessage, CrcFlags};

/// Per-OSD connection and request tracking
pub struct OSDSession {
    pub osd_id: i32,
    // Channel for sending messages - like Linux kernel's out_queue
    send_tx: mpsc::Sender<msgr2::message::Message>,
    pending_ops: Arc<RwLock<HashMap<u64, PendingOp>>>,
    next_tid: AtomicU64,
    #[allow(dead_code)]
    entity_name: String,
    #[allow(dead_code)]
    client_inc: u32,
    auth_provider: Option<Box<dyn auth::AuthProvider>>,
}

/// Tracking information for a pending operation
pub struct PendingOp {
    pub tid: u64,
    pub reqid: RequestId,
    pub result_tx: oneshot::Sender<Result<OpResult>>,
    pub submitted_at: Instant,
    /// Number of times this operation has been attempted
    /// Used to validate retry_attempt in replies
    pub attempts: i32,
}

impl OSDSession {
    /// Create a new OSD session
    pub fn new(
        osd_id: i32,
        entity_name: String,
        client_inc: u32,
        auth_provider: Option<Box<dyn auth::AuthProvider>>,
    ) -> Self {
        // Create channel for outgoing messages (like Linux kernel's out_queue)
        // Buffer size of 100 messages should be plenty
        let (send_tx, _) = mpsc::channel(100);

        Self {
            osd_id,
            send_tx,
            pending_ops: Arc::new(RwLock::new(HashMap::new())),
            next_tid: AtomicU64::new(1),
            entity_name,
            client_inc,
            auth_provider,
        }
    }

    /// Get the next transaction ID
    pub fn next_tid(&self) -> u64 {
        self.next_tid.fetch_add(1, Ordering::SeqCst)
    }

    /// Connect to the OSD and start I/O task
    ///
    /// This establishes a msgr2 connection to the OSD at the given address
    /// and spawns an I/O task that owns the connection.
    pub async fn connect(&mut self, entity_addr: denc::EntityAddr) -> Result<()> {
        // Extract SocketAddr for TCP connection
        let addr = entity_addr
            .to_socket_addr()
            .ok_or_else(|| OSDClientError::Connection("Invalid OSD address".to_string()))?;

        info!(
            "Connecting to OSD {} at {} (nonce={})",
            self.osd_id, addr, entity_addr.nonce
        );

        // Create connection config with authentication provider
        let config = if let Some(auth_provider) = &self.auth_provider {
            msgr2::ConnectionConfig::with_auth_provider_and_service(auth_provider.clone_box(), 4)
        } else {
            msgr2::ConnectionConfig::with_no_auth()
        };

        // Connect using msgr2
        let mut connection =
            msgr2::protocol::Connection::connect_with_target(addr, entity_addr, config)
                .await
                .map_err(|e| OSDClientError::Connection(format!("Failed to connect: {}", e)))?;

        info!(
            "Banner exchange complete, establishing session with OSD {}",
            self.osd_id
        );

        // Complete the full handshake
        connection.establish_session().await.map_err(|e| {
            OSDClientError::Connection(format!("Failed to establish session: {}", e))
        })?;

        info!("✓ Session established with OSD {}", self.osd_id);

        // Create new channel for this connection
        let (send_tx, send_rx) = mpsc::channel(100);
        self.send_tx = send_tx;

        // Spawn I/O task that owns the connection
        // This task multiplexes send/receive using tokio::select!
        let pending_ops = Arc::clone(&self.pending_ops);
        let osd_id = self.osd_id;
        tokio::spawn(async move {
            Self::io_task(osd_id, connection, send_rx, pending_ops).await;
        });

        info!("✓ I/O task started for OSD {}", self.osd_id);

        Ok(())
    }

    /// I/O task that owns the Connection
    ///
    /// This task multiplexes:
    /// - Receiving messages from send_rx channel and sending to OSD
    /// - Receiving messages from OSD and routing to pending ops
    ///
    /// Like Linux kernel's con_work() function, but using tokio::select!
    async fn io_task(
        osd_id: i32,
        mut connection: msgr2::protocol::Connection,
        mut send_rx: mpsc::Receiver<msgr2::message::Message>,
        pending_ops: Arc<RwLock<HashMap<u64, PendingOp>>>,
    ) {
        info!("I/O task started for OSD {}", osd_id);

        loop {
            tokio::select! {
                // Handle outgoing messages (like try_write in Linux kernel)
                Some(msg) = send_rx.recv() => {
                    if let Err(e) = connection.send_message(msg).await {
                        error!("Failed to send message to OSD {}: {}", osd_id, e);
                        break;
                    }
                }

                // Handle incoming messages (like try_read in Linux kernel)
                result = connection.recv_message() => {
                    match result {
                        Ok(msg) => {
                            if msg.msg_type() == crate::messages::CEPH_MSG_OSD_OPREPLY {
                                let tid = msg.tid();

                                // Use the unified CephMessage framework for decoding
                                // Note: msgr2::Message doesn't include header/footer, so we decode directly
                                match MOSDOpReply::decode(&msg.front, &msg.data) {
                                    Ok(reply) => {
                                        Self::handle_reply(tid, reply, &pending_ops).await;
                                    }
                                    Err(e) => {
                                        error!("Failed to decode MOSDOpReply: {}", e);
                                    }
                                }
                            } else {
                                warn!("Received unexpected message type: 0x{:04x}", msg.msg_type());
                            }
                        }
                        Err(e) => {
                            error!("Failed to receive message from OSD {}: {}", osd_id, e);
                            break;
                        }
                    }
                }
            }
        }

        info!("I/O task exiting for OSD {}", osd_id);
    }

    /// Submit an operation to the OSD
    ///
    /// This queues the message for sending (non-blocking, like ceph_con_send)
    pub async fn submit_op(&self, op: MOSDOp) -> Result<oneshot::Receiver<Result<OpResult>>> {
        let tid = op.reqid.tid;

        // Create channel for result
        let (tx, rx) = oneshot::channel();

        // Track the operation
        {
            let mut pending = self.pending_ops.write().await;
            pending.insert(
                tid,
                PendingOp {
                    tid,
                    reqid: op.reqid.clone(),
                    result_tx: tx,
                    submitted_at: Instant::now(),
                    attempts: 1, // First attempt (matches Linux kernel: r_attempts starts at 1)
                },
            );
        }

        // Encode the operation using the unified CephMessage framework
        let ceph_msg = CephMessage::from_payload(&op, 0, CrcFlags::ALL)
            .map_err(|e| OSDClientError::Encoding(format!("Failed to encode MOSDOp: {}", e)))?;

        // Convert CephMessage to msgr2::Message for sending
        // The msgr2::Message is used by the protocol layer for framing
        // Use the version from the CephMessage header (set by CephMessagePayload trait)
        let mut msg =
            msgr2::message::Message::new(crate::messages::CEPH_MSG_OSD_OP, ceph_msg.front)
                .with_version(ceph_msg.header.version)
                .with_tid(tid);
        msg.header.compat_version = ceph_msg.header.compat_version;
        msg.data = ceph_msg.data;

        // Send to channel (non-blocking, like Linux kernel's list_add_tail + queue_con)
        self.send_tx
            .send(msg)
            .await
            .map_err(|_| OSDClientError::Connection("I/O task has exited".into()))?;

        debug!("Submitted operation tid={} to OSD {}", tid, self.osd_id);
        Ok(rx)
    }

    /// Handle an operation reply
    async fn handle_reply(
        tid: u64,
        reply: MOSDOpReply,
        pending_ops: &Arc<RwLock<HashMap<u64, PendingOp>>>,
    ) {
        let pending_op = {
            let mut pending = pending_ops.write().await;
            pending.remove(&tid)
        };

        if let Some(pending_op) = pending_op {
            // Validate retry_attempt matches our attempt count
            // See: ~/dev/linux/net/ceph/osd_client.c handle_reply()
            if reply.retry_attempt >= 0 {
                // retry_attempt is 0-based, but our attempts counter is 1-based
                // So retry_attempt should equal (attempts - 1)
                if reply.retry_attempt != pending_op.attempts - 1 {
                    warn!(
                        "Ignoring stale reply for tid {}: retry_attempt {} != expected {} (attempts={})",
                        tid, reply.retry_attempt, pending_op.attempts - 1, pending_op.attempts
                    );
                    return;
                }
            } else {
                // Old server that doesn't support retry_attempt
                warn!("Received reply without retry_attempt for tid {}", tid);
            }

            let result = reply.to_op_result();
            let _ = pending_op.result_tx.send(Ok(result));
        } else {
            warn!("Received reply for unknown tid: {}", tid);
        }
    }

    /// Check if connected to OSD
    pub async fn is_connected(&self) -> bool {
        // Connection is alive if the send channel is not closed
        !self.send_tx.is_closed()
    }
}
