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

use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::auth::EntityType;
use crate::osdclient::backoff::BackoffTracker;
use crate::osdclient::error::{OSDClientError, Result};
use crate::osdclient::messages::{MOSDOp, MOSDOpReply};
use crate::osdclient::types::{OpResult, StripedPgId};

/// Build an HObject from MOSDOp fields for backoff range checking
///
/// Note: This function clones strings because HObject owns its data.
/// The clones only happen when checking backoff (rare) or during resend operations.
fn hobj_from_op(op: &MOSDOp) -> crate::HObject {
    crate::HObject {
        key: op.object.key.clone(),
        oid: op.object.oid.clone(),
        snapid: op.snapid,
        hash: op.object.hash,
        max: false,
        nspace: op.object.namespace.clone(),
        pool: op.object.pool,
    }
}
use crate::monclient::MOSDMap;
use crate::msgr2::MapSender;
use crate::msgr2::ceph_message::{CephMessage, CrcFlags};
use crate::msgr2::io_loop::{KeepaliveConfig, run_io_loop};
use crate::msgr2::message::MessagePriority;

/// Send channel buffer size (messages)
const SEND_CHANNEL_BUFFER_SIZE: usize = 100;
/// Maximum redirect loop count before failing an operation
const MAX_REDIRECTS: u32 = 10;
/// Default keepalive interval for OSD connections (seconds)
const DEFAULT_KEEPALIVE_INTERVAL_SECS: u64 = 10;

/// Connection state for OSD session
///
/// Following Ceph pattern for explicit connection state tracking.
/// Provides better observability and clearer semantics than implicit state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionState {
    /// Initial state - not yet connected
    Disconnected,
    /// Attempting to establish connection
    Connecting,
    /// Connection established and operational
    Connected,
    /// Connection closed (terminal state)
    Closed,
}

/// Session information combining connection state and peer address
#[derive(Debug, Clone)]
struct SessionInfo {
    conn_state: ConnectionState,
    peer_addr: Option<crate::EntityAddr>,
}

/// Context passed to the I/O task
struct IoTaskContext {
    osd_id: i32,
    pending_ops: Arc<DashMap<u64, PendingOp>>,
    osdmap_tx: MapSender<MOSDMap>,
    client: std::sync::Weak<crate::osdclient::client::OSDClient>,
    /// Session information (connection state and peer address)
    session_info: Arc<tokio::sync::RwLock<SessionInfo>>,
    /// Shutdown token for graceful termination
    shutdown_token: tokio_util::sync::CancellationToken,
}

/// Per-OSD connection and request tracking
pub struct OSDSession {
    pub osd_id: i32,
    // Channel for sending messages - like Linux kernel's out_queue
    send_tx: mpsc::Sender<crate::msgr2::message::Message>,
    pending_ops: Arc<DashMap<u64, PendingOp>>,
    next_tid: AtomicU64,
    auth_provider: Option<Box<dyn crate::auth::AuthProvider>>,
    /// Global ID from monitor authentication (for AUTH_NONE authorizers)
    global_id: u64,
    /// Per-PG backoff tracker using efficient data structures
    backoff_tracker: Arc<tokio::sync::RwLock<BackoffTracker>>,
    /// Channel for routing MOSDMap messages to OSDClient
    osdmap_tx: MapSender<MOSDMap>,
    /// Weak reference to OSDClient for session-specific message dispatch
    client: std::sync::Weak<crate::osdclient::client::OSDClient>,
    /// Tracker for per-operation timeouts
    tracker: Option<Arc<crate::osdclient::tracker::Tracker>>,
    /// Connection incarnation counter - increments on each connect()
    /// Following Ceph Objecter and Linux kernel Ceph client pattern.
    /// Used to detect stale operations from previous connection incarnations.
    /// Reference: ~/dev/ceph/src/osdc/Objecter.h:2502, ~/dev/linux/net/ceph/osd_client.c:1413
    incarnation: Arc<AtomicU32>,
    /// Session information (connection state and peer address combined)
    /// Using a single lock reduces lock acquisitions and simplifies the code.
    session_info: Arc<tokio::sync::RwLock<SessionInfo>>,
    /// Mutex to prevent concurrent connect() attempts
    /// Following Ceph pattern: prevents race conditions when multiple
    /// paths might trigger reconnection simultaneously.
    /// Reference: Ceph Objecter uses locks during reconnection
    connecting_lock: Arc<tokio::sync::Mutex<()>>,
    /// I/O task handle for graceful shutdown
    io_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

/// Tracking information for a pending operation
pub(crate) struct PendingOp {
    pub tid: u64,
    pub result_tx: oneshot::Sender<Result<OpResult>>,
    /// Number of times this operation has been attempted
    /// Used to validate retry_attempt in replies
    pub attempts: i32,
    /// OSDMap epoch this operation was submitted against
    pub osdmap_epoch: u32,
    /// Full operation for potential resubmission
    /// Wrapped in Arc for cheap cloning during retries
    pub op: Arc<MOSDOp>,
    /// Operation state
    pub state: crate::osdclient::types::OpState,
    /// Target information
    pub target: crate::osdclient::types::OpTarget,
    /// Operation priority (set in message header)
    /// Default is CEPH_MSG_PRIO_DEFAULT (127)
    pub priority: i32,
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
        auth_provider: Option<Box<dyn crate::auth::AuthProvider>>,
        global_id: u64,
        osdmap_tx: MapSender<MOSDMap>,
        client: std::sync::Weak<crate::osdclient::client::OSDClient>,
    ) -> Self {
        // Create channel for outgoing messages (like Linux kernel's out_queue)
        let (send_tx, _) = mpsc::channel(SEND_CHANNEL_BUFFER_SIZE);

        // Get tracker from client if available
        let tracker = client.upgrade().map(|c| Arc::clone(&c.tracker));

        Self {
            osd_id,
            send_tx,
            pending_ops: Arc::new(DashMap::new()),
            next_tid: AtomicU64::new(1),
            auth_provider,
            global_id,
            backoff_tracker: Arc::new(tokio::sync::RwLock::new(BackoffTracker::new())),
            osdmap_tx,
            client,
            tracker,
            // Start at incarnation 0, will increment to 1 on first connect()
            // Following Ceph pattern: incarnation 0 means "never connected"
            incarnation: Arc::new(AtomicU32::new(0)),
            // Start with no peer address and disconnected state
            session_info: Arc::new(tokio::sync::RwLock::new(SessionInfo {
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
        entity_addr: crate::EntityAddr,
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

        // Create connection config with authentication provider and global_id
        let mut config = if let Some(auth_provider) = &self.auth_provider {
            crate::msgr2::ConnectionConfig::with_auth_provider_and_service(
                auth_provider.clone_box(),
                EntityType::OSD.bits(),
            )
        } else {
            crate::msgr2::ConnectionConfig::with_no_auth()
        };
        config.service_id = EntityType::OSD.bits();
        config.global_id = self.global_id;

        // Connect using msgr2
        let mut connection = crate::msgr2::protocol::Connection::connect_with_target(
            addr,
            entity_addr.clone(),
            config,
        )
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

        info!("Session established with OSD {}", self.osd_id);

        // Store peer address and transition to Connected state
        {
            let mut info = self.session_info.write().await;
            info.peer_addr = Some(entity_addr.clone());
            info.conn_state = ConnectionState::Connected;
        }

        // Increment connection incarnation following Ceph pattern
        // Reference: ~/dev/linux/net/ceph/osd_client.c:1413
        let new_incarnation = self.incarnation.fetch_add(1, Ordering::SeqCst) + 1;
        info!(
            "OSD {} connection incarnation incremented to {}",
            self.osd_id, new_incarnation
        );

        // Create new channel for this connection
        let (send_tx, send_rx) = mpsc::channel(SEND_CHANNEL_BUFFER_SIZE);
        self.send_tx = send_tx;

        // Spawn I/O task that owns the connection
        // This task multiplexes send/receive using tokio::select!
        let context = IoTaskContext {
            osd_id: self.osd_id,
            pending_ops: Arc::clone(&self.pending_ops),
            osdmap_tx: self.osdmap_tx.clone(),
            client: self.client.clone(),
            session_info: Arc::clone(&self.session_info),
            shutdown_token,
        };
        // IMPORTANT: Keep a clone of send_tx alive in the io_task to prevent premature channel closure.
        // The mpsc channel closes when all Senders are dropped. By keeping this clone alive for the
        // io_task's lifetime, we ensure the channel stays open even if there are timing issues with
        // session lifecycle management. This matches the previous implementation's behavior.
        let send_tx_keepalive = self.send_tx.clone();
        let handle = tokio::spawn(async move {
            // Keep a clone of send_tx alive to prevent premature channel closure.
            let _keepalive = send_tx_keepalive;
            Self::io_task(context, connection, send_rx).await;
        });

        // Store the handle for graceful shutdown
        *self.io_task_handle.lock().await = Some(handle);

        info!("I/O task started for OSD {}", self.osd_id);

        Ok(())
    }

    /// I/O task that owns the Connection
    ///
    /// Routes messages based on their semantic scope:
    /// - OSDMAP: Broadcast → osdmap_tx channel (shared with OSDClient)
    /// - OPREPLY/BACKOFF: Session-specific → dispatch_from_osd (Linux kernel pattern)
    ///
    /// The mechanical select! loop (send/recv/keepalive/shutdown) is shared with
    /// MonConnection via [`crate::msgr2::io_loop::run_io_loop`]; only the routing differs.
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc ms_dispatch2()
    ///            ~/dev/linux/net/ceph/osd_client.c
    async fn io_task(
        ctx: IoTaskContext,
        connection: crate::msgr2::protocol::Connection,
        send_rx: mpsc::Receiver<crate::msgr2::message::Message>,
    ) {
        debug!("I/O task started for OSD {}", ctx.osd_id);

        let osd_id = ctx.osd_id;
        let osdmap_tx = ctx.osdmap_tx.clone();
        let client = ctx.client.clone();

        // Keepalive matching Ceph's default (no timeout check for OSDs)
        let keepalive = Some(KeepaliveConfig {
            interval: std::time::Duration::from_secs(DEFAULT_KEEPALIVE_INTERVAL_SECS),
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
                        crate::msgr2::message::CEPH_MSG_OSD_MAP => {
                            if let Err(e) = osdmap_tx.send(msg).await {
                                error!("Failed to send OSDMap to OSDClient: {:?}", e);
                            }
                        }
                        crate::osdclient::messages::CEPH_MSG_OSD_OPREPLY
                        | crate::osdclient::messages::CEPH_MSG_OSD_BACKOFF => {
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
        let n = ctx.pending_ops.len();
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

    /// Get a clone of backoff tracker for management (used by OSDClient for backoff handling)
    pub fn backoff_tracker(&self) -> Arc<tokio::sync::RwLock<BackoffTracker>> {
        Arc::clone(&self.backoff_tracker)
    }

    /// Get the send channel for sending messages (used by OSDClient for ACKs)
    pub fn send_tx(&self) -> mpsc::Sender<crate::msgr2::message::Message> {
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
        if result.is_none()
            && let Some(tracker) = &self.tracker
        {
            tracker.untrack(tid, self.osd_id);
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
        // Create new MOSDOp with updated fields for retry
        let mut new_mosdop = (*pending_op.op).clone();
        new_mosdop.flags = new_flags;

        // Update attempts and retry_attempt
        pending_op.attempts += 1;
        new_mosdop.retry_attempt = pending_op.attempts - 1;

        // Update incarnation (operation is being resent in current session)
        pending_op.sent_incarnation = self.incarnation.load(Ordering::SeqCst);

        // Replace the Arc with updated operation
        pending_op.op = Arc::new(new_mosdop);

        // Resubmit the operation
        Self::resubmit_operation(tid, pending_op, &self.send_tx, &self.pending_ops).await
    }

    /// Resubmit an operation (for retries)
    ///
    /// Reference: ~/dev/ceph/src/osdc/Objecter.cc _op_submit()
    async fn resubmit_operation(
        tid: u64,
        mut pending_op: PendingOp,
        send_tx: &mpsc::Sender<crate::msgr2::message::Message>,
        pending_ops: &Arc<DashMap<u64, PendingOp>>,
    ) -> Result<()> {
        // Encode the operation (priority is preserved from original op)
        let msg = Self::encode_operation(&pending_op.op, tid, pending_op.priority)?;

        // Update tid in pending_op
        pending_op.tid = tid;

        // IMPORTANT: Use try_send() instead of send().await to avoid deadlock
        // This function is called from io_task which is the consumer of send_rx
        // If we await on send(), we could block the io_task and prevent it from
        // draining the channel, causing a deadlock
        match send_tx.try_send(msg) {
            Ok(()) => {
                // Send succeeded - add back to pending ops
                pending_ops.insert(tid, pending_op);
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
    /// Priority is set in the message header (not the MOSDOp payload)
    fn encode_operation(
        op: &MOSDOp,
        tid: u64,
        priority: i32,
    ) -> Result<crate::msgr2::message::Message> {
        let ceph_msg = CephMessage::from_payload(op, 0, CrcFlags::ALL)?;

        // Convert i32 priority to u16 for message header (clamped to valid range)
        let priority_u16 = priority.clamp(0, u16::MAX as i32) as u16;

        let mut msg = crate::msgr2::message::Message::new(
            crate::osdclient::messages::CEPH_MSG_OSD_OP,
            ceph_msg.front,
        )
        .with_version(ceph_msg.header.version.get())
        .with_tid(tid)
        .with_priority(MessagePriority::from(priority_u16));
        msg.header.compat_version = ceph_msg.header.compat_version;
        msg.data = ceph_msg.data;

        Ok(msg)
    }

    /// Submit an operation to the OSD
    ///
    /// This queues the message for sending (non-blocking, like ceph_con_send)
    /// Priority is set in the message header (not the MOSDOp payload)
    pub async fn submit_op(
        &self,
        mut op: MOSDOp,
        priority: i32,
    ) -> Result<oneshot::Receiver<Result<OpResult>>> {
        let tid = op.reqid.tid;

        // Check if operation is blocked by backoff
        if self.is_blocked_by_backoff(&op).await {
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
        // Note: We store Arc<MOSDOp> to avoid cloning the entire operation on retries.
        // Wrap op in Arc first to avoid partial move issues
        let op_arc = Arc::new(op);

        {
            self.pending_ops.insert(
                tid,
                PendingOp {
                    tid,
                    result_tx: tx,
                    attempts: 1, // First attempt (matches Linux kernel: r_attempts starts at 1)
                    osdmap_epoch: op_arc.osdmap_epoch,
                    op: op_arc.clone(),
                    state: crate::osdclient::types::OpState::Queued,
                    target: crate::osdclient::types::OpTarget::new(
                        op_arc.osdmap_epoch,
                        op_arc.pgid,
                        self.osd_id,
                        vec![self.osd_id],
                    ),
                    // Store priority for retries (set in message header, not MOSDOp payload)
                    priority,
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

        // Encode the operation using shared helper (priority goes in message header)
        let msg = Self::encode_operation(&op_arc, tid, priority)?;

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
        pending_ops: &Arc<DashMap<u64, PendingOp>>,
        current_incarnation: u32,
    ) -> Option<(PendingOp, u32)> {
        let pending_op = pending_ops.remove(&tid).map(|(_, v)| v)?;

        // Validate operation staleness
        if !Self::validate_reply_freshness(tid, &reply, &pending_op, current_incarnation) {
            return None;
        }

        // Handle redirect if present
        if let Some(redirect) = &reply.redirect {
            return Self::handle_redirect(tid, pending_op, redirect);
        }

        // Handle EAGAIN on replica reads
        if reply.result == crate::osdclient::error::EAGAIN
            && let Some((new_mosdop, new_flags)) =
                Self::handle_eagain_retry(tid, &reply, &pending_op)
        {
            let mut updated_op = pending_op;
            updated_op.op = Arc::new(new_mosdop);
            return Some((updated_op, new_flags));
        }

        // Normal completion
        let result = reply.into_op_result();
        let _ = pending_op.result_tx.send(Ok(result));
        None
    }

    /// Validate that reply is not stale
    fn validate_reply_freshness(
        tid: u64,
        reply: &MOSDOpReply,
        pending_op: &PendingOp,
        current_incarnation: u32,
    ) -> bool {
        // Check incarnation first
        if pending_op.sent_incarnation != current_incarnation {
            warn!(
                "Ignoring stale reply for tid {}: sent in incarnation {} but current is {}",
                tid, pending_op.sent_incarnation, current_incarnation
            );
            debug!(
                "Stale operation details: tid={}, attempts={}, osdmap_epoch={}",
                tid, pending_op.attempts, pending_op.osdmap_epoch
            );
            return false;
        }

        // Validate retry_attempt
        if reply.retry_attempt >= 0 {
            if reply.retry_attempt != pending_op.attempts - 1 {
                warn!(
                    "Ignoring stale reply for tid {}: retry_attempt {} != expected {} (attempts={})",
                    tid,
                    reply.retry_attempt,
                    pending_op.attempts - 1,
                    pending_op.attempts
                );
                return false;
            }
        } else {
            debug!(
                "Reply for tid {} has retry_attempt=-1 (old server or unset field)",
                tid
            );
        }

        true
    }

    /// Handle redirect reply
    fn handle_redirect(
        tid: u64,
        mut pending_op: PendingOp,
        redirect: &crate::osdclient::types::RequestRedirect,
    ) -> Option<(PendingOp, u32)> {
        // Check for redirect loop
        if pending_op.redirect_count >= MAX_REDIRECTS {
            error!(
                "Operation tid {} exceeded maximum redirects ({}), failing",
                tid, MAX_REDIRECTS
            );
            let _ = pending_op
                .result_tx
                .send(Err(crate::osdclient::error::OSDClientError::Other(
                    format!("Exceeded maximum redirects ({})", MAX_REDIRECTS),
                )));
            return None;
        }

        debug!(
            "Got redirect reply for tid {} (redirect #{} of {}): pool={}, key={}, namespace={}, object={}",
            tid,
            pending_op.redirect_count + 1,
            MAX_REDIRECTS,
            redirect.redirect_locator.pool_id,
            redirect.redirect_locator.key,
            redirect.redirect_locator.namespace,
            redirect.redirect_object
        );

        // Increment redirect counter
        pending_op.redirect_count += 1;

        // Apply redirect to operation
        let new_mosdop = Self::apply_redirect_to_op(&pending_op.op, redirect);
        let new_flags = new_mosdop.flags;
        pending_op.op = Arc::new(new_mosdop);

        Some((pending_op, new_flags))
    }

    /// Apply redirect information to operation.
    ///
    /// Mirrors C++ `request_redirect_t::combine_with_locator()` which
    /// unconditionally assigns `redirect_locator` → `target_oloc`.
    fn apply_redirect_to_op(
        op: &MOSDOp,
        redirect: &crate::osdclient::types::RequestRedirect,
    ) -> MOSDOp {
        let mut new_mosdop = op.clone();

        // Unconditionally overwrite locator — matches C++ combine_with_locator().
        new_mosdop.object.pool = redirect.redirect_locator.pool_id;
        new_mosdop
            .object
            .key
            .clone_from(&redirect.redirect_locator.key);
        new_mosdop
            .object
            .namespace
            .clone_from(&redirect.redirect_locator.namespace);

        if !redirect.redirect_object.is_empty() {
            new_mosdop.object.oid.clone_from(&redirect.redirect_object);
        }

        // Set redirect flags (Objecter.cc:3744)
        use crate::osdclient::types::OsdOpFlags;
        let redirect_flags =
            OsdOpFlags::REDIRECTED | OsdOpFlags::IGNORE_CACHE | OsdOpFlags::IGNORE_OVERLAY;
        new_mosdop.flags |= redirect_flags.bits();

        new_mosdop
    }

    /// Handle EAGAIN retry for replica reads
    fn handle_eagain_retry(
        tid: u64,
        _reply: &MOSDOpReply,
        pending_op: &PendingOp,
    ) -> Option<(MOSDOp, u32)> {
        use crate::osdclient::types::OsdOpFlags;
        let replica_flags = (OsdOpFlags::BALANCE_READS | OsdOpFlags::LOCALIZE_READS).bits();

        if pending_op.op.flags & replica_flags != 0 {
            debug!(
                "Got EAGAIN for replica read tid {}, resubmitting to primary",
                tid
            );

            // Create new MOSDOp with replica read flags removed
            let mut new_mosdop = (*pending_op.op).clone();
            new_mosdop.flags &= !replica_flags;
            let new_flags = new_mosdop.flags;

            return Some((new_mosdop, new_flags));
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
        begin: &crate::HObject,
        end: &crate::HObject,
    ) {
        let osd_id = self.osd_id;
        let send_tx = &self.send_tx;
        let pending_ops = &self.pending_ops;

        let mut ops_to_resend = Vec::with_capacity(pending_ops.len());

        // Find all operations in this PG that fall within the backoff range
        for entry in pending_ops.iter() {
            let (tid, pending_op) = entry.pair();
            // Check if this operation is for the same PG
            if pending_op.op.pgid != *pgid {
                continue;
            }

            // Check if hobject is contained in [begin, end)
            let hobj = hobj_from_op(&pending_op.op);
            // This matches Ceph's contained_by() logic
            if hobj >= *begin && hobj < *end {
                debug!(
                    "OSD {} backoff lifted: will resend operation tid={} for object={} in range [{:?}, {:?})",
                    osd_id, tid, pending_op.op.object.oid, begin, end
                );
                ops_to_resend.push((*tid, Arc::clone(&pending_op.op), pending_op.priority));
            }
        }

        // Resend the operations
        // IMPORTANT: Use try_send() to avoid deadlock (we're in the io_task)
        for (tid, op_arc, priority) in ops_to_resend {
            match Self::encode_operation(&op_arc, tid, priority) {
                Ok(msg) => match send_tx.try_send(msg) {
                    Ok(()) => {
                        info!(
                            "OSD {} resent operation tid={} for object={} after backoff lifted",
                            osd_id, tid, op_arc.object.oid
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
    pub async fn get_peer_address(&self) -> Option<crate::EntityAddr> {
        self.session_info.read().await.peer_addr.clone()
    }

    /// Check if an operation is blocked by an active backoff
    ///
    /// Fast path: skips hobj construction entirely when no backoffs are active
    /// (the common case on a healthy cluster).
    /// Reference: Ceph Objecter _send_op()
    async fn is_blocked_by_backoff(&self, op: &MOSDOp) -> bool {
        let tracker = self.backoff_tracker.read().await;
        if tracker.is_empty() {
            return false;
        }
        let hobj = hobj_from_op(op);
        tracker.is_blocked(&op.pgid, &hobj).is_some()
    }

    // === OSDMap Rescanning Support ===

    /// Get metadata for all pending operations
    ///
    /// Returns (tid, pool_id, object_id, osdmap_epoch) for each pending operation.
    /// Used by OSDClient to determine which operations need rescanning.
    /// Note: object_id is cloned here, but this is only called during OSDMap updates (infrequent).
    pub async fn get_pending_ops_metadata(&self) -> Vec<(u64, u64, String, u32)> {
        self.pending_ops
            .iter()
            .map(|entry| {
                let (tid, op) = entry.pair();
                (
                    *tid,
                    op.op.object.pool,
                    op.op.object.oid.clone(),
                    op.osdmap_epoch,
                )
            })
            .collect()
    }

    /// Remove and return a pending operation
    ///
    /// Used during OSDMap rescanning to extract operations that need to be
    /// migrated to a different OSD. Returns None if the operation doesn't exist.
    pub async fn remove_pending_op(&self, tid: u64) -> Option<PendingOp> {
        self.pending_ops.remove(&tid).map(|(_, v)| v)
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

        // Update the operation's OSDMap epoch, cloning only if the Arc is shared.
        Arc::make_mut(&mut pending_op.op).osdmap_epoch = new_osdmap_epoch;
        pending_op.osdmap_epoch = new_osdmap_epoch;

        // Increment attempts counter (matching C++ Objecter behavior)
        pending_op.attempts += 1;

        // Update incarnation to current session incarnation (operation is being resent)
        // This is critical: migrated operations get new incarnation from target session
        pending_op.sent_incarnation = self.incarnation.load(Ordering::SeqCst);

        // Check if operation is blocked by backoff
        if self.is_blocked_by_backoff(&pending_op.op).await {
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

        // Extract op and priority before inserting (avoids re-acquiring lock)
        let op = Arc::clone(&pending_op.op);
        let priority = pending_op.priority;

        // Insert into pending operations
        self.pending_ops.insert(tid, pending_op);

        // Encode and send the operation using shared helper (priority preserved from original)
        let msg = Self::encode_operation(&op, tid, priority)?;

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
        let (osdmap_tx, _osdmap_rx) = crate::msgr2::map_channel::map_channel::<MOSDMap>(1);
        let session = OSDSession::new(
            0,
            None,
            0, // global_id
            osdmap_tx,
            std::sync::Weak::new(),
        );

        assert_eq!(session.current_incarnation(), 0);
    }

    #[tokio::test]
    async fn test_incarnation_increments_on_connect() {
        let (osdmap_tx, _osdmap_rx) = crate::msgr2::map_channel::map_channel::<MOSDMap>(1);
        let session = OSDSession::new(
            0,
            None,
            0, // global_id
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
