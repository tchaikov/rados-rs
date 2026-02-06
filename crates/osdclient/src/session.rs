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
use crate::types::{OpResult, RequestId, StripedPgId};
use msgr2::ceph_message::{CephMessage, CrcFlags};

/// Context passed to the I/O task
struct IoTaskContext {
    osd_id: i32,
    pending_ops: Arc<RwLock<HashMap<u64, PendingOp>>>,
    #[allow(dead_code)]
    backoffs: Arc<RwLock<HashMap<StripedPgId, HashMap<denc::HObject, OSDBackoff>>>>,
    message_bus: Arc<msgr2::MessageBus>,
    client: std::sync::Weak<crate::client::OSDClient>,
}

/// OSD backoff tracking
///
/// Represents an active backoff from the OSD requesting the client
/// to pause operations on a specific object range.
#[derive(Debug, Clone)]
pub struct OSDBackoff {
    pub pgid: StripedPgId,
    pub id: u64,
    pub begin: denc::HObject,
    pub end: denc::HObject,
}

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
    /// Active backoffs from OSD
    /// Outer map: pgid -> inner map
    /// Inner map: begin hobject -> backoff info
    backoffs: Arc<RwLock<HashMap<StripedPgId, HashMap<denc::HObject, OSDBackoff>>>>,
    /// Message bus for broadcasting messages (OSDMAP)
    message_bus: Arc<msgr2::MessageBus>,
    /// Weak reference to OSDClient for session-specific message dispatch
    client: std::sync::Weak<crate::client::OSDClient>,
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
    /// Pool ID for OSDMap rescanning
    pub pool_id: u64,
    /// Object ID for OSDMap rescanning
    pub object_id: String,
    /// OSDMap epoch this operation was submitted against
    pub osdmap_epoch: u32,
    /// Full operation for potential resubmission
    pub op: MOSDOp,
}

impl OSDSession {
    /// Create a new OSD session
    pub fn new(
        osd_id: i32,
        entity_name: String,
        client_inc: u32,
        auth_provider: Option<Box<dyn auth::AuthProvider>>,
        message_bus: Arc<msgr2::MessageBus>,
        client: std::sync::Weak<crate::client::OSDClient>,
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
            backoffs: Arc::new(RwLock::new(HashMap::new())),
            message_bus,
            client,
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
        let context = IoTaskContext {
            osd_id: self.osd_id,
            pending_ops: Arc::clone(&self.pending_ops),
            backoffs: Arc::clone(&self.backoffs),
            message_bus: Arc::clone(&self.message_bus),
            client: self.client.clone(),
        };
        // IMPORTANT: Keep a clone of send_tx alive in the io_task to prevent premature channel closure.
        // The mpsc channel closes when all Senders are dropped. By keeping this clone alive for the
        // io_task's lifetime, we ensure the channel stays open even if there are timing issues with
        // session lifecycle management. This matches the previous implementation's behavior.
        let send_tx_keepalive = self.send_tx.clone();
        tokio::spawn(async move {
            // Capture the keepalive by value to ensure it lives for the io_task's lifetime
            let _keepalive = send_tx_keepalive;
            Self::io_task(context, connection, send_rx).await;
            drop(_keepalive); // Explicit drop for clarity
        });

        info!("✓ I/O task started for OSD {}", self.osd_id);

        Ok(())
    }

    /// I/O task that owns the Connection
    ///
    /// This task multiplexes:
    /// - Receiving messages from send_rx channel and sending to OSD
    /// - Receiving messages from OSD and routing based on message type
    ///
    /// Message routing follows a hybrid pattern inspired by both implementations:
    /// - Ceph C++ (Objecter): Central dispatcher with connection context preservation
    /// - Linux Kernel: Explicit OSD context passing to handlers
    ///
    /// We route messages based on their semantic scope:
    /// - OSDMAP: Broadcast message → MessageBus (shared with MonClient)
    /// - OPREPLY/BACKOFF: Session-specific → Direct dispatch with osd_id context
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc ms_dispatch2() and ~/dev/linux/net/ceph/osd_client.c
    async fn io_task(
        ctx: IoTaskContext,
        mut connection: msgr2::protocol::Connection,
        mut send_rx: mpsc::Receiver<msgr2::message::Message>,
    ) {
        debug!("I/O task started for OSD {}", ctx.osd_id);

        loop {
            tokio::select! {
                // Handle outgoing messages (like try_write in Linux kernel)
                msg_opt = send_rx.recv() => {
                    match msg_opt {
                        Some(msg) => {
                            debug!("OSD {} sending message type 0x{:04x}, tid={}", ctx.osd_id, msg.msg_type(), msg.tid());
                            if let Err(e) = connection.send_message(msg).await {
                                error!("Failed to send message to OSD {}: {}", ctx.osd_id, e);
                                break;
                            }
                        }
                        None => {
                            debug!("OSD {} send_rx channel closed", ctx.osd_id);
                            break;
                        }
                    }
                }

                // Handle incoming messages - route based on message semantics
                result = connection.recv_message() => {
                    match result {
                        Ok(msg) => {
                            let msg_type = msg.msg_type();
                            debug!("OSD {} received message type 0x{:04x}", ctx.osd_id, msg_type);

                            // Route messages based on their scope (broadcast vs session-specific)
                            match msg_type {
                                msgr2::message::CEPH_MSG_OSD_MAP => {
                                    // Broadcast message: Forward to MessageBus
                                    // Multiple components (MonClient, OSDClient) may need this
                                    if let Err(e) = ctx.message_bus.dispatch(msg).await {
                                        error!("Failed to dispatch OSDMap to MessageBus: {}", e);
                                    }
                                }
                                crate::messages::CEPH_MSG_OSD_OPREPLY | crate::messages::CEPH_MSG_OSD_BACKOFF => {
                                    // Session-specific message: Dispatch directly with OSD context
                                    // Like Linux kernel's explicit osd parameter
                                    if let Some(client_arc) = ctx.client.upgrade() {
                                        if let Err(e) = client_arc.dispatch_from_osd(ctx.osd_id, msg).await {
                                            error!("Failed to dispatch message 0x{:04x} from OSD {}: {}", msg_type, ctx.osd_id, e);
                                        }
                                    } else {
                                        debug!("OSDClient dropped, ignoring message from OSD {}", ctx.osd_id);
                                        break;
                                    }
                                }
                                _ => {
                                    warn!("OSD {} sent unexpected message type: 0x{:04x}", ctx.osd_id, msg_type);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to receive message from OSD {}: {}", ctx.osd_id, e);
                            break;
                        }
                    }
                }
            }
        }

        info!("I/O task exiting for OSD {}", ctx.osd_id);

        // Clean up pending operations on disconnect
        let mut pending = ctx.pending_ops.write().await;
        for (tid, pending_op) in pending.drain() {
            let _ = pending_op
                .result_tx
                .send(Err(OSDClientError::Connection(format!(
                    "OSD {} disconnected",
                    ctx.osd_id
                ))));
            debug!(
                "Cancelled pending operation tid={} due to OSD {} disconnect",
                tid, ctx.osd_id
            );
        }
    }

    // ===========================================================================
    // Public accessor methods for OSDClient to use
    // ===========================================================================

    /// Check if this session has a pending operation with the given tid
    pub async fn has_pending_op(&self, tid: u64) -> bool {
        let pending = self.pending_ops.read().await;
        pending.contains_key(&tid)
    }

    /// Get a clone of pending_ops for iteration (used by OSDClient for message routing)
    pub fn pending_ops(&self) -> Arc<RwLock<HashMap<u64, PendingOp>>> {
        Arc::clone(&self.pending_ops)
    }

    /// Get a clone of backoffs for management (used by OSDClient for backoff handling)
    pub fn backoffs(
        &self,
    ) -> Arc<RwLock<HashMap<StripedPgId, HashMap<denc::HObject, OSDBackoff>>>> {
        Arc::clone(&self.backoffs)
    }

    /// Get the send channel for sending messages (used by OSDClient for ACKs)
    pub fn send_tx(&self) -> mpsc::Sender<msgr2::message::Message> {
        self.send_tx.clone()
    }

    /// Handle an operation reply and check if retry is needed
    ///
    /// Returns Some((pending_op, modified_flags)) if the operation should be retried with modified flags
    /// This is called by OSDClient when it receives OPREPLY messages from MessageBus
    pub async fn handle_osd_op_reply(
        &self,
        tid: u64,
        reply: MOSDOpReply,
    ) -> Option<(PendingOp, u32)> {
        Self::handle_reply(tid, reply, &self.pending_ops).await
    }

    /// Resubmit an operation for retry
    ///
    /// This is called by OSDClient after handle_osd_op_reply indicates a retry is needed
    pub async fn resubmit_with_new_flags(
        &self,
        tid: u64,
        mut pending_op: PendingOp,
        new_flags: u32,
    ) -> Result<()> {
        // Update operation for retry
        pending_op.op.flags = new_flags;
        pending_op.attempts += 1;
        pending_op.op.retry_attempt = pending_op.attempts - 1;

        // Resubmit the operation
        Self::resubmit_operation(tid, pending_op, &self.send_tx, &self.pending_ops).await
    }

    /// Resubmit an operation (for retries)
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc _op_submit()
    async fn resubmit_operation(
        tid: u64,
        mut pending_op: PendingOp,
        send_tx: &mpsc::Sender<msgr2::message::Message>,
        pending_ops: &Arc<RwLock<HashMap<u64, PendingOp>>>,
    ) -> Result<()> {
        // Encode the operation
        let msg = Self::encode_operation(&pending_op.op, tid)?;

        // Update tid in pending_op
        pending_op.tid = tid;

        // IMPORTANT: Use try_send() instead of send().await to avoid deadlock
        // This function is called from io_task which is the consumer of send_rx
        // If we await on send(), we could block the io_task and prevent it from
        // draining the channel, causing a deadlock
        match send_tx.try_send(msg) {
            Ok(()) => {
                // Send succeeded - add back to pending ops
                let mut pending = pending_ops.write().await;
                pending.insert(tid, pending_op);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Channel is full - this shouldn't happen with our 100-message buffer
                // but if it does, notify the client rather than deadlocking
                error!("Send channel full when retrying operation tid {}", tid);
                let _ = pending_op
                    .result_tx
                    .send(Err(OSDClientError::Connection("Send channel full".into())));
                Err(OSDClientError::Connection("Send channel full".into()))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Channel closed - notify client
                let _ = pending_op.result_tx.send(Err(OSDClientError::Connection(
                    "Send channel closed".into(),
                )));
                Err(OSDClientError::Connection("Send channel closed".into()))
            }
        }
    }

    /// Encode an operation into a msgr2 message
    ///
    /// Helper to eliminate duplication between submit_op and retry logic
    fn encode_operation(op: &MOSDOp, tid: u64) -> Result<msgr2::message::Message> {
        let ceph_msg = CephMessage::from_payload(op, 0, CrcFlags::ALL)
            .map_err(|e| OSDClientError::Encoding(format!("Failed to encode MOSDOp: {}", e)))?;

        let mut msg =
            msgr2::message::Message::new(crate::messages::CEPH_MSG_OSD_OP, ceph_msg.front)
                .with_version(ceph_msg.header.version)
                .with_tid(tid);
        msg.header.compat_version = ceph_msg.header.compat_version;
        msg.data = ceph_msg.data;

        Ok(msg)
    }

    /// Submit an operation to the OSD
    ///
    /// This queues the message for sending (non-blocking, like ceph_con_send)
    pub async fn submit_op(&self, mut op: MOSDOp) -> Result<oneshot::Receiver<Result<OpResult>>> {
        let tid = op.reqid.tid;

        // Create HObject from operation for backoff checking
        let hobj = denc::HObject {
            key: op.object.key.clone(),
            oid: op.object.oid.clone(),
            snapid: op.snapid,
            hash: op.object.hash,
            max: false,
            nspace: op.object.namespace.clone(),
            pool: op.object.pool,
        };

        // Check if operation is blocked by backoff
        if self.is_blocked_by_backoff(&op.pgid, &hobj).await {
            return Err(OSDClientError::Backoff(format!(
                "Operation blocked by OSD backoff: pgid={:?}, object={}",
                op.pgid, op.object.oid
            )));
        }

        // Set retry_attempt for this operation
        // retry_attempt is 0-based: 0 for first attempt, 1 for first retry, etc.
        // Our attempts counter is 1-based, so retry_attempt = attempts - 1
        // See: ~/dev/linux/net/ceph/osd_client.c (encodes req->r_attempts)
        const FIRST_ATTEMPT: i32 = 1;
        op.retry_attempt = FIRST_ATTEMPT - 1; // 0 for first send

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
                    pool_id: op.object.pool,
                    object_id: op.object.oid.clone(),
                    osdmap_epoch: op.osdmap_epoch,
                    op: op.clone(),
                },
            );
        }

        // Encode the operation using shared helper
        let msg = Self::encode_operation(&op, tid)?;

        // Send to channel (non-blocking, like Linux kernel's list_add_tail + queue_con)
        debug!("Submitting operation tid={} to OSD {}", tid, self.osd_id);

        self.send_tx
            .send(msg)
            .await
            .map_err(|_| OSDClientError::Connection("I/O task has exited".into()))?;

        Ok(rx)
    }

    /// Handle an operation reply
    ///
    /// Returns Some((pending_op, modified_flags)) if the operation should be retried with modified flags
    async fn handle_reply(
        tid: u64,
        reply: MOSDOpReply,
        pending_ops: &Arc<RwLock<HashMap<u64, PendingOp>>>,
    ) -> Option<(PendingOp, u32)> {
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
                    return None;
                }
            } else {
                // Server doesn't support retry_attempt field
                // This should only happen with very old Ceph versions (pre-v3, from 2012)
                debug!(
                    "Reply for tid {} has retry_attempt=-1 (old server or unset field)",
                    tid
                );
            }

            // Check for EAGAIN on replica reads
            // See: ~/dev/ceph/src/osdc/Objecter.cc handle_reply()
            const EAGAIN: i32 = -11;
            if reply.result == EAGAIN {
                // Check if this was a replica read
                use crate::types::OsdOpFlags;
                let replica_flags = (OsdOpFlags::BALANCE_READS | OsdOpFlags::LOCALIZE_READS).bits();

                if pending_op.op.flags & replica_flags != 0 {
                    debug!(
                        "Got EAGAIN for replica read tid {}, resubmitting to primary",
                        tid
                    );

                    // Remove replica read flags and retry (will go to primary)
                    let new_flags = pending_op.op.flags & !replica_flags;
                    return Some((pending_op, new_flags));
                }
            }

            let result = reply.to_op_result();
            let _ = pending_op.result_tx.send(Ok(result));
        } else {
            warn!("Received reply for unknown tid: {}", tid);
        }

        None
    }

    /// Resend operations in the specified backoff range
    ///
    /// When a backoff is lifted, we need to resend any pending operations
    /// whose hobject falls within the [begin, end) range for the given PG.
    ///
    /// This is called by OSDClient when handling UNBLOCK backoff messages
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc handle_osd_backoff() UNBLOCK case
    pub async fn resend_ops_in_range(
        &self,
        pgid: &StripedPgId,
        begin: &denc::HObject,
        end: &denc::HObject,
    ) {
        let osd_id = self.osd_id;
        let send_tx = &self.send_tx;
        let pending_ops = &self.pending_ops;
        let pending = pending_ops.read().await;

        let mut ops_to_resend = Vec::new();

        // Find all operations in this PG that fall within the backoff range
        for (tid, pending_op) in pending.iter() {
            // Check if this operation is for the same PG
            if pending_op.op.pgid != *pgid {
                continue;
            }

            // Create hobject for this operation
            let hobj = denc::HObject {
                key: pending_op.op.object.key.clone(),
                oid: pending_op.op.object.oid.clone(),
                snapid: pending_op.op.snapid,
                hash: pending_op.op.object.hash,
                max: false,
                nspace: pending_op.op.object.namespace.clone(),
                pool: pending_op.op.object.pool,
            };

            // Check if hobject is contained in [begin, end)
            // This matches Ceph's contained_by() logic
            if hobj >= *begin && hobj < *end {
                debug!(
                    "OSD {} backoff lifted: will resend operation tid={} for object={} in range [{:?}, {:?})",
                    osd_id, tid, pending_op.op.object.oid, begin, end
                );
                ops_to_resend.push((*tid, pending_op.op.clone()));
            }
        }

        drop(pending);

        // Resend the operations
        // IMPORTANT: Use try_send() to avoid deadlock (we're in the io_task)
        for (tid, op) in ops_to_resend {
            match Self::encode_operation(&op, tid) {
                Ok(msg) => match send_tx.try_send(msg) {
                    Ok(()) => {
                        info!(
                            "OSD {} resent operation tid={} for object={} after backoff lifted",
                            osd_id, tid, op.object.oid
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!(
                            "OSD {} send channel full, cannot resend tid={} after backoff",
                            osd_id, tid
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        error!(
                            "OSD {} send channel closed, cannot resend tid={} after backoff",
                            osd_id, tid
                        );
                        break;
                    }
                },
                Err(e) => {
                    error!(
                        "OSD {} failed to encode operation tid={} for resend: {}",
                        osd_id, tid, e
                    );
                }
            }
        }
    }

    /// Check if connected to OSD
    pub async fn is_connected(&self) -> bool {
        // Connection is alive if the send channel is not closed
        !self.send_tx.is_closed()
    }

    /// Check if an operation is blocked by an active backoff
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc _send_op()
    async fn is_blocked_by_backoff(&self, pgid: &StripedPgId, hobj: &denc::HObject) -> bool {
        let backoffs_map = self.backoffs.read().await;

        // Check if this PG has any backoffs
        if let Some(pg_backoffs) = backoffs_map.get(pgid) {
            // Find the backoff range that might contain this object
            // We need to check if hobj falls in [begin, end) for any backoff
            for backoff in pg_backoffs.values() {
                // Check if hobj is in [begin, end)
                if hobj >= &backoff.begin && hobj < &backoff.end {
                    debug!(
                        "Operation blocked by backoff: pgid={:?}, hobj={:?}, backoff=[{:?}, {:?})",
                        pgid, hobj, backoff.begin, backoff.end
                    );
                    return true;
                }
            }
        }

        false
    }

    // === OSDMap Rescanning Support ===

    /// Get metadata for all pending operations
    ///
    /// Returns (tid, pool_id, object_id, osdmap_epoch) for each pending operation.
    /// Used by OSDClient to determine which operations need rescanning.
    pub async fn get_pending_ops_metadata(&self) -> Vec<(u64, u64, String, u32)> {
        let pending = self.pending_ops.read().await;
        pending
            .iter()
            .map(|(tid, op)| (*tid, op.pool_id, op.object_id.clone(), op.osdmap_epoch))
            .collect()
    }

    /// Remove and return a pending operation
    ///
    /// Used during OSDMap rescanning to extract operations that need to be
    /// migrated to a different OSD. Returns None if the operation doesn't exist.
    pub async fn remove_pending_op(&self, tid: u64) -> Option<PendingOp> {
        let mut pending = self.pending_ops.write().await;
        pending.remove(&tid)
    }

    /// Insert a migrated operation from another session
    ///
    /// Used during OSDMap rescanning to insert an operation that was removed
    /// from another session. The operation's MOSDOp, attempts counter, and
    /// osdmap_epoch are updated before insertion.
    ///
    /// Returns an error if the operation is blocked by backoff or if sending fails.
    pub async fn insert_migrated_op(
        &self,
        mut pending_op: PendingOp,
        new_osdmap_epoch: u32,
    ) -> Result<()> {
        let tid = pending_op.tid;

        // Update the operation's OSDMap epoch
        pending_op.osdmap_epoch = new_osdmap_epoch;
        pending_op.op.osdmap_epoch = new_osdmap_epoch;

        // Increment attempts counter (matching C++ Objecter behavior)
        pending_op.attempts += 1;

        // Create HObject for backoff checking
        let hobj = denc::HObject {
            key: pending_op.op.object.key.clone(),
            oid: pending_op.op.object.oid.clone(),
            snapid: pending_op.op.snapid,
            hash: pending_op.op.object.hash,
            max: false,
            nspace: pending_op.op.object.namespace.clone(),
            pool: pending_op.op.object.pool,
        };

        // Check if operation is blocked by backoff
        if self.is_blocked_by_backoff(&pending_op.op.pgid, &hobj).await {
            // Cancel the operation with backoff error
            let _ = pending_op
                .result_tx
                .send(Err(OSDClientError::Backoff(format!(
                    "Migrated operation blocked by OSD backoff: pgid={:?}, object={}",
                    pending_op.op.pgid, pending_op.op.object.oid
                ))));
            return Err(OSDClientError::Backoff(format!(
                "Migrated operation blocked by backoff: pgid={:?}",
                pending_op.op.pgid
            )));
        }

        // Insert into pending operations
        {
            let mut pending = self.pending_ops.write().await;
            pending.insert(tid, pending_op);
        }

        // Get a reference to the op for sending
        let pending = self.pending_ops.read().await;
        let op = match pending.get(&tid) {
            Some(p) => p.op.clone(),
            None => {
                return Err(OSDClientError::Internal(
                    "Operation disappeared after insertion".into(),
                ))
            }
        };
        drop(pending);

        // Encode and send the operation using shared helper
        let msg = Self::encode_operation(&op, tid)?;

        // Send to channel
        self.send_tx
            .send(msg)
            .await
            .map_err(|_| OSDClientError::Connection("Send channel closed".into()))?;

        info!(
            "Migrated operation {} to OSD {} (attempt {}, epoch {})",
            tid,
            self.osd_id,
            op.reqid.inc + 1,
            new_osdmap_epoch
        );

        Ok(())
    }
}
