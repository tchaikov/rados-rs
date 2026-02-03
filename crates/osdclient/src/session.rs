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
    pub pool_id: i64,
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
        let backoffs = Arc::clone(&self.backoffs);
        let osd_id = self.osd_id;
        let send_tx_clone = self.send_tx.clone();
        tokio::spawn(async move {
            Self::io_task(
                osd_id,
                connection,
                send_tx_clone,
                send_rx,
                pending_ops,
                backoffs,
            )
            .await;
        });

        info!("✓ I/O task started for OSD {}", self.osd_id);

        Ok(())
    }

    /// I/O task that owns the Connection
    ///
    /// This task multiplexes:
    /// - Receiving messages from send_rx channel and sending to OSD
    /// - Receiving messages from OSD and dispatching to handlers
    ///
    /// Like Linux kernel's con_work() function, but using tokio::select!
    /// Reference: ~/dev/ceph/src/msg/async/ProtocolV2.cc and ~/dev/linux/net/ceph/messenger.c
    async fn io_task(
        osd_id: i32,
        mut connection: msgr2::protocol::Connection,
        send_tx: mpsc::Sender<msgr2::message::Message>,
        mut send_rx: mpsc::Receiver<msgr2::message::Message>,
        pending_ops: Arc<RwLock<HashMap<u64, PendingOp>>>,
        backoffs: Arc<RwLock<HashMap<StripedPgId, HashMap<denc::HObject, OSDBackoff>>>>,
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
                            // Dispatch to appropriate handler (like Ceph's ms_dispatch2)
                            Self::dispatch_message(
                                osd_id,
                                msg,
                                &send_tx,
                                &pending_ops,
                                &backoffs,
                                &mut connection,
                            )
                            .await;
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

    /// Dispatch incoming message to appropriate handler
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc ms_dispatch2()
    async fn dispatch_message(
        osd_id: i32,
        msg: msgr2::message::Message,
        send_tx: &mpsc::Sender<msgr2::message::Message>,
        pending_ops: &Arc<RwLock<HashMap<u64, PendingOp>>>,
        backoffs: &Arc<RwLock<HashMap<StripedPgId, HashMap<denc::HObject, OSDBackoff>>>>,
        connection: &mut msgr2::protocol::Connection,
    ) {
        match msg.msg_type() {
            crate::messages::CEPH_MSG_OSD_OPREPLY => {
                let tid = msg.tid();
                match MOSDOpReply::decode(&msg.front, &msg.data) {
                    Ok(reply) => {
                        Self::handle_osd_op_reply(osd_id, tid, reply, send_tx, pending_ops).await;
                    }
                    Err(e) => {
                        error!("Failed to decode MOSDOpReply: {}", e);
                    }
                }
            }
            crate::messages::CEPH_MSG_OSD_BACKOFF => {
                match crate::messages::MOSDBackoff::decode(&msg.front) {
                    Ok(backoff_msg) => {
                        Self::handle_backoff(osd_id, backoff_msg, backoffs, connection).await;
                    }
                    Err(e) => {
                        error!("Failed to decode MOSDBackoff: {}", e);
                    }
                }
            }
            _ => {
                warn!(
                    "OSD {} sent unexpected message type: 0x{:04x}",
                    osd_id,
                    msg.msg_type()
                );
            }
        }
    }

    /// Handle OSD operation reply
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc handle_osd_op_reply()
    async fn handle_osd_op_reply(
        osd_id: i32,
        tid: u64,
        reply: MOSDOpReply,
        send_tx: &mpsc::Sender<msgr2::message::Message>,
        pending_ops: &Arc<RwLock<HashMap<u64, PendingOp>>>,
    ) {
        // Check if retry is needed (returns Some if EAGAIN on replica read)
        if let Some((mut pending_op, new_flags)) = Self::handle_reply(tid, reply, pending_ops).await
        {
            debug!(
                "OSD {} EAGAIN on replica read tid {}, retrying to primary (flags: 0x{:x} -> 0x{:x})",
                osd_id, tid, pending_op.op.flags, new_flags
            );

            // Update operation for retry
            pending_op.op.flags = new_flags;
            pending_op.attempts += 1;
            pending_op.op.retry_attempt = pending_op.attempts - 1;

            // Resubmit the operation
            if let Err(e) = Self::resubmit_operation(tid, pending_op, send_tx, pending_ops).await {
                error!("Failed to resubmit operation tid {}: {}", tid, e);
            }
        }
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

        // Try to send first, only add to pending if successful
        if let Err(e) = send_tx.send(msg).await {
            // Send failed - notify client
            let _ = pending_op
                .result_tx
                .send(Err(OSDClientError::Connection(format!(
                    "Failed to send retry: {}",
                    e
                ))));
            return Err(OSDClientError::Connection("Failed to send retry".into()));
        }

        // Send succeeded - add back to pending ops
        let mut pending = pending_ops.write().await;
        pending.insert(tid, pending_op);

        Ok(())
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
        self.send_tx
            .send(msg)
            .await
            .map_err(|_| OSDClientError::Connection("I/O task has exited".into()))?;

        debug!("Submitted operation tid={} to OSD {}", tid, self.osd_id);
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

    /// Handle an OSD backoff message
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc handle_osd_backoff()
    async fn handle_backoff(
        osd_id: i32,
        backoff: crate::messages::MOSDBackoff,
        backoffs: &Arc<RwLock<HashMap<StripedPgId, HashMap<denc::HObject, OSDBackoff>>>>,
        connection: &mut msgr2::protocol::Connection,
    ) {
        use crate::messages::{
            CEPH_OSD_BACKOFF_OP_ACK_BLOCK, CEPH_OSD_BACKOFF_OP_BLOCK, CEPH_OSD_BACKOFF_OP_UNBLOCK,
        };

        match backoff.op {
            CEPH_OSD_BACKOFF_OP_BLOCK => {
                info!(
                    "OSD {} requests backoff: pgid={}:{}.{}, id={}, range=[{:?}, {:?})",
                    osd_id,
                    backoff.pgid.pool,
                    backoff.pgid.seed,
                    backoff.pgid.shard,
                    backoff.id,
                    backoff.begin,
                    backoff.end
                );

                // Register backoff
                {
                    let mut backoffs_map = backoffs.write().await;
                    let pg_backoffs = backoffs_map
                        .entry(backoff.pgid)
                        .or_insert_with(HashMap::new);

                    let backoff_entry = OSDBackoff {
                        pgid: backoff.pgid,
                        id: backoff.id,
                        begin: backoff.begin.clone(),
                        end: backoff.end.clone(),
                    };

                    pg_backoffs.insert(backoff.begin.clone(), backoff_entry);
                }

                // Send ACK_BLOCK reply
                let ack = crate::messages::MOSDBackoff::new(
                    backoff.pgid,
                    backoff.map_epoch,
                    CEPH_OSD_BACKOFF_OP_ACK_BLOCK,
                    backoff.id,
                    backoff.begin,
                    backoff.end,
                );

                match ack.encode() {
                    Ok(payload) => {
                        let msg = msgr2::message::Message::new(
                            crate::messages::CEPH_MSG_OSD_BACKOFF,
                            payload,
                        )
                        .with_version(crate::messages::MOSDBackoff::VERSION);

                        if let Err(e) = connection.send_message(msg).await {
                            error!("Failed to send ACK_BLOCK to OSD {}: {}", osd_id, e);
                        } else {
                            debug!("Sent ACK_BLOCK for backoff id={} to OSD {}", ack.id, osd_id);
                        }
                    }
                    Err(e) => {
                        error!("Failed to encode ACK_BLOCK message: {}", e);
                    }
                }
            }

            CEPH_OSD_BACKOFF_OP_UNBLOCK => {
                info!(
                    "OSD {} lifts backoff: pgid={}:{}.{}, id={}",
                    osd_id, backoff.pgid.pool, backoff.pgid.seed, backoff.pgid.shard, backoff.id
                );

                // Remove backoff
                {
                    let mut backoffs_map = backoffs.write().await;
                    if let Some(pg_backoffs) = backoffs_map.get_mut(&backoff.pgid) {
                        pg_backoffs.remove(&backoff.begin);

                        // Remove PG entry if no more backoffs
                        if pg_backoffs.is_empty() {
                            backoffs_map.remove(&backoff.pgid);
                        }
                    }
                }

                // TODO: Resend queued operations in this range
                // This requires tracking which operations were blocked
                // For now, operations will timeout and be retried naturally
                debug!(
                    "Backoff lifted for pgid={:?}, future operations will proceed",
                    backoff.pgid
                );
            }

            _ => {
                warn!(
                    "Received unknown backoff operation {} from OSD {}",
                    backoff.op, osd_id
                );
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
    pub async fn get_pending_ops_metadata(&self) -> Vec<(u64, i64, String, u32)> {
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
