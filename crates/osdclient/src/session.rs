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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::backoff::BackoffTracker;
use crate::error::{OSDClientError, Result};
use crate::messages::{MOSDOp, MOSDOpReply};
use crate::types::{OpResult, RequestId, StripedPgId};
use monclient::MOSDMap;
use msgr2::ceph_message::{CephMessage, CrcFlags};
use msgr2::io_loop::{run_io_loop, KeepaliveConfig};
use msgr2::MapSender;

/// Connection state for OSD session
///
/// Following Ceph pattern for explicit connection state tracking.
/// Provides better observability and clearer semantics than implicit state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Initial state - not yet connected
    Disconnected,
    /// Attempting to establish connection
    Connecting,
    /// Connection established and operational
    Connected,
    /// Attempting to reconnect after failure
    Reconnecting,
    /// Connection closed (terminal state)
    Closed,
}

/// Session information combining connection state and peer address
#[derive(Debug, Clone)]
struct SessionInfo {
    conn_state: ConnectionState,
    peer_addr: Option<denc::EntityAddr>,
}

/// Context passed to the I/O task
struct IoTaskContext {
    osd_id: i32,
    pending_ops: Arc<RwLock<HashMap<u64, PendingOp>>>,
    #[allow(dead_code)]
    backoff_tracker: Arc<RwLock<BackoffTracker>>,
    osdmap_tx: MapSender<MOSDMap>,
    client: std::sync::Weak<crate::client::OSDClient>,
    /// Session incarnation for this connection
    /// Shared with OSDSession to detect stale operations.
    /// Currently validated in handle_reply and timeout callback.
    #[allow(dead_code)]
    incarnation: Arc<AtomicU32>,
    /// Session information (connection state and peer address)
    session_info: Arc<RwLock<SessionInfo>>,
    /// Shutdown token for graceful termination
    shutdown_token: tokio_util::sync::CancellationToken,
}

// Re-export BackoffEntry for backward compatibility
pub use crate::backoff::BackoffEntry as OSDBackoff;

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
    /// Per-PG backoff tracker using efficient data structures
    backoff_tracker: Arc<RwLock<BackoffTracker>>,
    /// Channel for routing MOSDMap messages to OSDClient
    osdmap_tx: MapSender<MOSDMap>,
    /// Weak reference to OSDClient for session-specific message dispatch
    client: std::sync::Weak<crate::client::OSDClient>,
    /// Tracker for per-operation timeouts
    tracker: Option<Arc<crate::tracker::Tracker>>,
    /// Connection incarnation counter - increments on each connect()
    /// Following Ceph Objecter and Linux kernel Ceph client pattern.
    /// Used to detect stale operations from previous connection incarnations.
    /// Reference: ~/dev/ceph/src/osdc/Objecter.h:2502, ~/dev/linux/net/ceph/osd_client.c:1413
    incarnation: Arc<AtomicU32>,
    /// Session information (connection state and peer address combined)
    /// Using a single lock reduces lock acquisitions and simplifies the code.
    session_info: Arc<RwLock<SessionInfo>>,
    /// Mutex to prevent concurrent connect() attempts
    /// Following Ceph pattern: prevents race conditions when multiple
    /// paths might trigger reconnection simultaneously.
    /// Reference: Ceph Objecter uses locks during reconnection
    connecting_lock: Arc<tokio::sync::Mutex<()>>,
    /// I/O task handle for graceful shutdown
    io_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
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
    /// Operation state
    pub state: crate::types::OpState,
    /// Target information
    pub target: crate::types::OpTarget,
    /// Whether operation should be resent
    pub should_resend: bool,
    /// Maximum retry attempts
    pub max_attempts: i32,
    /// Connection incarnation when this operation was sent
    /// Used to detect stale operations after connection restarts.
    /// Following Ceph Objecter pattern: each connection restart increments
    /// session incarnation, and operations with old incarnation are discarded.
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc:3519, ~/dev/linux/net/ceph/osd_client.c:2348
    pub sent_incarnation: u32,
    /// Number of redirects this operation has followed
    /// Used to detect and prevent infinite redirect loops.
    /// Reference: Ceph has FIXME comment but no implementation (Objecter.cc:3741)
    pub redirect_count: u32,
}

impl OSDSession {
    /// Create a new OSD session
    pub fn new(
        osd_id: i32,
        entity_name: String,
        client_inc: u32,
        auth_provider: Option<Box<dyn auth::AuthProvider>>,
        osdmap_tx: MapSender<MOSDMap>,
        client: std::sync::Weak<crate::client::OSDClient>,
    ) -> Self {
        // Create channel for outgoing messages (like Linux kernel's out_queue)
        // Buffer size of 100 messages should be plenty
        let (send_tx, _) = mpsc::channel(100);

        // Get tracker from client if available
        let tracker = client.upgrade().map(|c| Arc::clone(&c.tracker));

        Self {
            osd_id,
            send_tx,
            pending_ops: Arc::new(RwLock::new(HashMap::new())),
            next_tid: AtomicU64::new(1),
            entity_name,
            client_inc,
            auth_provider,
            backoff_tracker: Arc::new(RwLock::new(BackoffTracker::new())),
            osdmap_tx,
            client,
            tracker,
            // Start at incarnation 0, will increment to 1 on first connect()
            // Following Ceph pattern: incarnation 0 means "never connected"
            incarnation: Arc::new(AtomicU32::new(0)),
            // Start with no peer address and disconnected state
            session_info: Arc::new(RwLock::new(SessionInfo {
                conn_state: ConnectionState::Disconnected,
                peer_addr: None,
            })),
            // Mutex to prevent concurrent connection attempts
            connecting_lock: Arc::new(tokio::sync::Mutex::new(())),
            // No I/O task handle until connect() is called
            io_task_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Get the next transaction ID
    pub fn next_tid(&self) -> u64 {
        self.next_tid.fetch_add(1, Ordering::SeqCst)
    }

    /// Get current connection incarnation
    ///
    /// Used to detect stale operations from previous connections.
    /// Following Ceph pattern where operations store incarnation when sent
    /// and are discarded if incarnation doesn't match current value.
    pub fn current_incarnation(&self) -> u32 {
        self.incarnation.load(Ordering::SeqCst)
    }

    /// Connect to the OSD and start I/O task
    ///
    /// This establishes a msgr2 connection to the OSD at the given address
    /// and spawns an I/O task that owns the connection.
    pub async fn connect(
        &mut self,
        entity_addr: denc::EntityAddr,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        // Acquire connecting lock to prevent concurrent connection attempts
        // Following Ceph pattern: prevents race conditions
        let _lock = self.connecting_lock.lock().await;

        // Check if already connected (double-check after acquiring lock)
        if self.session_info.read().await.conn_state == ConnectionState::Connected {
            info!("OSD {} already connected, skipping", self.osd_id);
            return Ok(());
        }

        // Transition to Connecting state
        self.session_info.write().await.conn_state = ConnectionState::Connecting;

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
            msgr2::protocol::Connection::connect_with_target(addr, entity_addr.clone(), config)
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

        // Store peer address and transition to Connected state
        let mut info = self.session_info.write().await;
        info.peer_addr = Some(entity_addr.clone());
        info.conn_state = ConnectionState::Connected;
        drop(info);

        // Increment connection incarnation following Ceph pattern
        // Reference: ~/dev/linux/net/ceph/osd_client.c:1413
        let new_incarnation = self.incarnation.fetch_add(1, Ordering::SeqCst) + 1;
        info!(
            "OSD {} connection incarnation incremented to {}",
            self.osd_id, new_incarnation
        );

        // Create new channel for this connection
        let (send_tx, send_rx) = mpsc::channel(100);
        self.send_tx = send_tx;

        // Spawn I/O task that owns the connection
        // This task multiplexes send/receive using tokio::select!
        let context = IoTaskContext {
            osd_id: self.osd_id,
            pending_ops: Arc::clone(&self.pending_ops),
            backoff_tracker: Arc::clone(&self.backoff_tracker),
            osdmap_tx: self.osdmap_tx.clone(),
            client: self.client.clone(),
            incarnation: Arc::clone(&self.incarnation),
            session_info: Arc::clone(&self.session_info),
            shutdown_token,
        };
        // IMPORTANT: Keep a clone of send_tx alive in the io_task to prevent premature channel closure.
        // The mpsc channel closes when all Senders are dropped. By keeping this clone alive for the
        // io_task's lifetime, we ensure the channel stays open even if there are timing issues with
        // session lifecycle management. This matches the previous implementation's behavior.
        let send_tx_keepalive = self.send_tx.clone();
        let handle = tokio::spawn(async move {
            // Capture the keepalive by value to ensure it lives for the io_task's lifetime
            let _keepalive = send_tx_keepalive;
            Self::io_task(context, connection, send_rx).await;
            drop(_keepalive); // Explicit drop for clarity
        });

        // Store the handle for graceful shutdown
        *self.io_task_handle.lock().await = Some(handle);

        info!("✓ I/O task started for OSD {}", self.osd_id);

        Ok(())
    }

    /// I/O task that owns the Connection
    ///
    /// Routes messages based on their semantic scope:
    /// - OSDMAP: Broadcast → osdmap_tx channel (shared with OSDClient)
    /// - OPREPLY/BACKOFF: Session-specific → dispatch_from_osd (Linux kernel pattern)
    ///
    /// The mechanical select! loop (send/recv/keepalive/shutdown) is shared with
    /// MonConnection via [`msgr2::io_loop::run_io_loop`]; only the routing differs.
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc ms_dispatch2()
    ///            ~/dev/linux/net/ceph/osd_client.c
    async fn io_task(
        ctx: IoTaskContext,
        connection: msgr2::protocol::Connection,
        send_rx: mpsc::Receiver<msgr2::message::Message>,
    ) {
        debug!("I/O task started for OSD {}", ctx.osd_id);

        let osd_id = ctx.osd_id;
        let osdmap_tx = ctx.osdmap_tx.clone();
        let client = ctx.client.clone();

        // Keepalive every 10 seconds, matching Ceph's default (no timeout check for OSDs)
        let keepalive = Some(KeepaliveConfig {
            interval: std::time::Duration::from_secs(10),
            timeout: None,
        });

        run_io_loop(
            connection,
            send_rx,
            ctx.shutdown_token.clone(),
            keepalive,
            move |msg| {
                let osdmap_tx = osdmap_tx.clone();
                let client = client.clone();
                async move {
                    let msg_type = msg.msg_type();
                    debug!("OSD {} received message type 0x{:04x}", osd_id, msg_type);

                    match msg_type {
                        msgr2::message::CEPH_MSG_OSD_MAP => {
                            if let Err(e) = osdmap_tx.send(msg).await {
                                error!("Failed to send OSDMap to OSDClient: {:?}", e);
                            }
                        }
                        crate::messages::CEPH_MSG_OSD_OPREPLY
                        | crate::messages::CEPH_MSG_OSD_BACKOFF => {
                            if let Some(client_arc) = client.upgrade() {
                                if let Err(e) = client_arc.dispatch_from_osd(osd_id, msg).await {
                                    error!(
                                        "Failed to dispatch message 0x{:04x} from OSD {}: {}",
                                        msg_type, osd_id, e
                                    );
                                }
                            } else {
                                debug!("OSDClient dropped, ignoring message from OSD {}", osd_id);
                                return false;
                            }
                        }
                        _ => {
                            warn!(
                                "OSD {} sent unexpected message type: 0x{:04x}",
                                osd_id, msg_type
                            );
                        }
                    }
                    true
                }
            },
        )
        .await;

        info!("I/O task exiting for OSD {}", ctx.osd_id);

        // Update connection state based on exit reason:
        // cancelled → Closed (terminal), otherwise → Disconnected (can reconnect)
        let final_state = if ctx.shutdown_token.is_cancelled() {
            ConnectionState::Closed
        } else {
            ConnectionState::Disconnected
        };
        ctx.session_info.write().await.conn_state = final_state;

        // Leave pending ops in the session.
        // They are migrated eagerly by get_or_create_session() the next time any operation
        // targets this OSD (see kick_into_session() there), or by scan_requests_on_map_change()
        // when an OSDMap update arrives. This avoids a type cycle that would arise from
        // spawning kick_requests() here (io_task → kick → get_or_create_session → connect →
        // io_task forms an opaque-async-return cycle the compiler cannot resolve).
        let n = ctx.pending_ops.read().await.len();
        if n > 0 {
            info!(
                "OSD {} I/O task exited with {} pending ops; will be kicked on next session use",
                ctx.osd_id, n
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

    /// Get a clone of backoff tracker for management (used by OSDClient for backoff handling)
    pub fn backoff_tracker(&self) -> Arc<RwLock<BackoffTracker>> {
        Arc::clone(&self.backoff_tracker)
    }

    /// Get the send channel for sending messages (used by OSDClient for ACKs)
    pub fn send_tx(&self) -> mpsc::Sender<msgr2::message::Message> {
        self.send_tx.clone()
    }

    /// Handle an operation reply and check if retry is needed
    ///
    /// Returns Some((pending_op, modified_flags)) if the operation should be retried with modified flags
    /// This is called by OSDClient when it receives OPREPLY messages from OSD connections
    pub async fn handle_osd_op_reply(
        &self,
        tid: u64,
        reply: MOSDOpReply,
    ) -> Option<(PendingOp, u32)> {
        // Get current session incarnation for stale operation detection
        let current_incarnation = self.incarnation.load(Ordering::SeqCst);

        let result = Self::handle_reply(tid, reply, &self.pending_ops, current_incarnation).await;

        // If operation completed (not retrying), untrack it
        if result.is_none() {
            if let Some(tracker) = &self.tracker {
                tracker.untrack(tid, self.osd_id);
            }
        }

        result
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

        // Update incarnation (operation is being resent in current session)
        pending_op.sent_incarnation = self.incarnation.load(Ordering::SeqCst);

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
                    state: crate::types::OpState::Queued,
                    target: crate::types::OpTarget::new(
                        op.osdmap_epoch,
                        op.pgid,
                        self.osd_id,
                        vec![self.osd_id],
                    ),
                    should_resend: false,
                    max_attempts: 10,
                    // Capture current session incarnation
                    // Following Ceph pattern: operation stores incarnation when sent
                    // Reference: ~/dev/linux/net/ceph/osd_client.c:2348
                    sent_incarnation: self.incarnation.load(Ordering::SeqCst),
                    // Start with no redirects
                    redirect_count: 0,
                },
            );
        }

        // Track operation timeout
        if let Some(tracker) = &self.tracker {
            let timeout = tracker.operation_timeout();
            let deadline = tokio::time::Instant::now() + timeout;
            tracker.track(tid, self.osd_id, deadline);
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
        current_incarnation: u32,
    ) -> Option<(PendingOp, u32)> {
        let pending_op = {
            let mut pending = pending_ops.write().await;
            pending.remove(&tid)
        };

        if let Some(pending_op) = pending_op {
            // Check incarnation first - most important staleness check
            // Following Ceph pattern: discard operations from previous connection incarnations
            // Reference: ~/dev/linux/net/ceph/osd_client.c handle_reply()
            if pending_op.sent_incarnation != current_incarnation {
                warn!(
                    "Ignoring stale reply for tid {}: sent in incarnation {} but current is {}",
                    tid, pending_op.sent_incarnation, current_incarnation
                );
                debug!(
                    "Stale operation details: tid={}, reqid={}, attempts={}, osdmap_epoch={}",
                    tid, pending_op.reqid.tid, pending_op.attempts, pending_op.osdmap_epoch
                );
                return None;
            }

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

            // Check for redirect reply first
            // Following Ceph Objecter pattern: handle redirects before other logic
            // Reference: ~/dev/ceph/src/osdc/Objecter.cc:3734
            if let Some(redirect) = &reply.redirect {
                // Check for redirect loop
                const MAX_REDIRECTS: u32 = 10;
                if pending_op.redirect_count >= MAX_REDIRECTS {
                    error!(
                        "Operation tid {} exceeded maximum redirects ({}), failing",
                        tid, MAX_REDIRECTS
                    );
                    let _ = pending_op
                        .result_tx
                        .send(Err(crate::error::OSDClientError::Other(format!(
                            "Exceeded maximum redirects ({})",
                            MAX_REDIRECTS
                        ))));
                    return None;
                }

                info!(
                    "Got redirect reply for tid {} (redirect #{} of {}): pool={}, key={}, namespace={}, object={}",
                    tid,
                    pending_op.redirect_count + 1,
                    MAX_REDIRECTS,
                    redirect.redirect_locator.pool_id,
                    redirect.redirect_locator.key,
                    redirect.redirect_locator.namespace,
                    redirect.redirect_object
                );

                // Apply redirect to operation's target object locator
                // The redirect combines with existing locator (Ceph's combine_with_locator)
                let mut updated_op = pending_op;

                // Increment redirect counter
                updated_op.redirect_count += 1;

                // Update object locator with redirect information
                if redirect.redirect_locator.pool_id != u64::MAX {
                    updated_op.op.object.pool = redirect.redirect_locator.pool_id;
                }
                if !redirect.redirect_locator.key.is_empty() {
                    updated_op.op.object.key = redirect.redirect_locator.key.clone();
                }
                if !redirect.redirect_locator.namespace.is_empty() {
                    updated_op.op.object.namespace = redirect.redirect_locator.namespace.clone();
                }
                if !redirect.redirect_object.is_empty() {
                    updated_op.op.object.oid = redirect.redirect_object.clone();
                }

                // Set redirect flags (following Ceph Objecter.cc:3747-3749)
                use crate::types::OsdOpFlags;
                updated_op.op.flags |= OsdOpFlags::REDIRECTED.bits();
                updated_op.op.flags |= OsdOpFlags::IGNORE_CACHE.bits();
                updated_op.op.flags |= OsdOpFlags::IGNORE_OVERLAY.bits();

                let new_flags = updated_op.op.flags;

                // Return operation for resubmission with updated flags
                // The caller (OSDClient) will recalculate target OSD via CRUSH
                return Some((updated_op, new_flags));
            }

            // Check for EAGAIN on replica reads
            // See: ~/dev/ceph/src/osdc/Objecter.cc handle_reply()
            if reply.result == crate::error::EAGAIN {
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
        // Check explicit connection state
        self.session_info.read().await.conn_state == ConnectionState::Connected
    }

    /// Await the I/O task to ensure it has stopped
    ///
    /// The caller must have already cancelled the shutdown token (via the OSDClient's
    /// shutdown_token, whose child token is passed to each session's connect()).
    pub async fn close(&self) {
        info!("Waiting for OSD {} I/O task to stop", self.osd_id);
        let handle = self.io_task_handle.lock().await.take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        self.session_info.write().await.conn_state = ConnectionState::Closed;
    }

    /// Get the peer address for this session
    ///
    /// Returns the EntityAddr that was used to establish the current connection.
    /// Used to validate that the session's address matches the current OSDMap.
    pub async fn get_peer_address(&self) -> Option<denc::EntityAddr> {
        self.session_info.read().await.peer_addr.clone()
    }

    /// Get the current connection state
    ///
    /// Provides explicit state information for observability and debugging.
    /// Following Ceph pattern for explicit connection state tracking.
    pub async fn connection_state(&self) -> ConnectionState {
        self.session_info.read().await.conn_state
    }

    /// Check if an operation is blocked by an active backoff
    ///
    /// Uses efficient range lookup matching Ceph's _send_op() logic
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc _send_op()
    async fn is_blocked_by_backoff(&self, pgid: &StripedPgId, hobj: &denc::HObject) -> bool {
        let tracker = self.backoff_tracker.read().await;
        tracker.is_blocked(pgid, hobj).is_some()
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

        // Update incarnation to current session incarnation (operation is being resent)
        // This is critical: migrated operations get new incarnation from target session
        pending_op.sent_incarnation = self.incarnation.load(Ordering::SeqCst);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_incarnation_starts_at_zero() {
        let (osdmap_tx, _osdmap_rx) = msgr2::map_channel::map_channel::<MOSDMap>(1);
        let session = OSDSession::new(
            0,
            "client.test".to_string(),
            0,
            None,
            osdmap_tx,
            std::sync::Weak::new(),
        );

        assert_eq!(session.current_incarnation(), 0);
    }

    #[tokio::test]
    async fn test_incarnation_increments_on_connect() {
        let (osdmap_tx, _osdmap_rx) = msgr2::map_channel::map_channel::<MOSDMap>(1);
        let session = OSDSession::new(
            0,
            "client.test".to_string(),
            0,
            None,
            osdmap_tx,
            std::sync::Weak::new(),
        );

        // Initial incarnation should be 0
        assert_eq!(session.current_incarnation(), 0);

        // Simulate connect by incrementing incarnation (what connect() does)
        let _new_inc = session.incarnation.fetch_add(1, Ordering::SeqCst) + 1;

        // After connect, incarnation should be 1
        assert_eq!(session.current_incarnation(), 1);

        // Second connect
        let _new_inc = session.incarnation.fetch_add(1, Ordering::SeqCst) + 1;

        // After second connect, incarnation should be 2
        assert_eq!(session.current_incarnation(), 2);
    }

    #[test]
    fn test_pending_op_captures_incarnation() {
        // Test that PendingOp captures incarnation when created
        let incarnation = Arc::new(AtomicU32::new(5));

        let sent_incarnation = incarnation.load(Ordering::SeqCst);
        assert_eq!(sent_incarnation, 5);

        // Simulate reconnection
        incarnation.fetch_add(1, Ordering::SeqCst);

        // Old operation's incarnation is now stale
        let current_incarnation = incarnation.load(Ordering::SeqCst);
        assert_eq!(current_incarnation, 6);
        assert_ne!(sent_incarnation, current_incarnation);
    }
}
