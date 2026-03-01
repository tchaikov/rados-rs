//! RADOS OSD Client
//!
//! Main entry point for performing object operations against a Ceph cluster.

use monclient::MOSDMap;
use msgr2::{MapReceiver, MapSender};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use denc::{Denc, VersionedEncode};

use crate::backoff::BackoffEntry;
use crate::error::{OSDClientError, Result};
use crate::messages::{MOSDOp, HASH_CALCULATE_FROM_NAME};
use crate::session::OSDSession;
use crate::throttle::Throttle;
use crate::tracker::{Tracker, TrackerConfig};
use crate::types::{
    calc_op_budget, ListObjectEntry, ListResult, OSDOp, ObjectId, OsdOpFlags, PoolFlags,
    ReadResult, RequestRedirect, StatResult, StripedPgId, WriteResult,
};

/// Configuration for OSD client
#[derive(Debug, Clone)]
pub struct OSDClientConfig {
    /// Operation timeout configuration
    pub tracker_config: TrackerConfig,
    /// Client incarnation number
    /// This should be unique per client instance to avoid OSD duplicate request detection.
    /// In Ceph C++, this is typically 0, with uniqueness provided by the global_id
    /// in the entity_name instead.
    pub client_inc: u32,
    /// Maximum in-flight operations (default: 1024, matches objecter_inflight_ops)
    pub max_inflight_ops: usize,
    /// Maximum in-flight bytes (default: 100MB, matches objecter_inflight_op_bytes)
    pub max_inflight_bytes: usize,
}

impl Default for OSDClientConfig {
    fn default() -> Self {
        Self {
            tracker_config: TrackerConfig::default(),
            client_inc: 0,
            max_inflight_ops: crate::throttle::DEFAULT_MAX_OPS,
            max_inflight_bytes: crate::throttle::DEFAULT_MAX_BYTES,
        }
    }
}

/// Main OSD client for performing object operations
pub struct OSDClient {
    config: OSDClientConfig,
    mon_client: Arc<monclient::MonClient>,
    sessions: Arc<RwLock<HashMap<i32, Arc<OSDSession>>>>,
    pub(crate) tracker: Arc<Tracker>,
    /// Request throttle to prevent resource exhaustion
    throttle: Arc<Throttle>,
    /// Entity name (e.g., "client.admin") from MonClient auth config
    entity_name: String,
    /// Global ID from monitor authentication (used in entity_name for request IDs)
    global_id: u64,
    /// Cluster FSID for OSDMap validation
    fsid: denc::UuidD,
    /// Current OSDMap (watch channel for efficient distribution)
    osdmap_tx: watch::Sender<Option<Arc<crate::osdmap::OSDMap>>>,
    osdmap_rx: watch::Receiver<Option<Arc<crate::osdmap::OSDMap>>>,
    /// Channel for routing MOSDMap messages to sessions
    map_tx: MapSender<MOSDMap>,
    /// Shutdown token for graceful termination
    shutdown_token: CancellationToken,
    /// Weak self-reference for session creation
    self_weak: std::sync::Weak<Self>,
}

impl OSDClient {
    /// Create a new OSD client
    pub async fn new(
        config: OSDClientConfig,
        fsid: denc::UuidD,
        mon_client: Arc<monclient::MonClient>,
        osdmap_tx: MapSender<MOSDMap>,
        mut osdmap_rx: MapReceiver<MOSDMap>,
    ) -> Result<Arc<Self>> {
        // Get entity_name and global_id from MonClient
        let entity_name = mon_client.get_entity_name_string();
        let global_id = mon_client.get_global_id().await;

        info!("Creating OSDClient for entity_name={}", entity_name);
        info!("OSDClient using global_id {} from monitor", global_id);

        // Create watch channel for OSDMap distribution
        let (osdmap_tx_watch, osdmap_rx_watch) = watch::channel(None);

        // Create throttle with configured limits
        let throttle = Arc::new(Throttle::new(
            config.max_inflight_ops,
            config.max_inflight_bytes,
        ));
        info!(
            "OSDClient throttle: max_ops={}, max_bytes={}",
            throttle.max_ops(),
            throttle.max_bytes()
        );

        let client = Arc::new_cyclic(|weak: &std::sync::Weak<Self>| {
            // Create timeout callback that cancels operations in sessions
            let weak_for_callback = weak.clone();
            let timeout_callback: crate::tracker::TimeoutCallback = Arc::new(move |osd_id, tid| {
                if let Some(client) = weak_for_callback.upgrade() {
                    let sessions = client.sessions.clone();
                    tokio::spawn(async move {
                        let sessions_guard = sessions.read().await;
                        if let Some(session) = sessions_guard.get(&osd_id) {
                            if let Some(pending_op) = session.remove_pending_op(tid).await {
                                // Check incarnation - operation might be from a previous connection
                                // that has since reconnected. In that case, silently drop the timeout.
                                let current_incarnation = session.current_incarnation();
                                if pending_op.sent_incarnation != current_incarnation {
                                    debug!(
                                        "Ignoring timeout for stale operation: OSD {} tid={} (sent in incarnation {} but current is {})",
                                        osd_id, tid, pending_op.sent_incarnation, current_incarnation
                                    );
                                    return;
                                }

                                let _ = pending_op.result_tx.send(Err(OSDClientError::Timeout(
                                    client.tracker.operation_timeout(),
                                )));
                                warn!("Cancelled timed-out operation: OSD {} tid={}", osd_id, tid);
                            }
                        }
                    });
                }
            });

            let tracker = Arc::new(Tracker::new(
                config.tracker_config.clone(),
                timeout_callback,
            ));

            Self {
                config,
                mon_client: Arc::clone(&mon_client),
                sessions: Arc::new(RwLock::new(HashMap::new())),
                tracker,
                throttle,
                entity_name: entity_name.clone(),
                global_id,
                fsid,
                osdmap_tx: osdmap_tx_watch,
                osdmap_rx: osdmap_rx_watch,
                map_tx: osdmap_tx,
                shutdown_token: CancellationToken::new(),
                self_weak: weak.clone(),
            }
        });

        // Spawn drain task for OSDMap messages
        let client_weak = Arc::downgrade(&client);
        tokio::spawn(async move {
            while let Some(msg) = osdmap_rx.recv().await {
                if let Some(client_arc) = client_weak.upgrade() {
                    if let Err(e) = client_arc.handle_osdmap(msg).await {
                        error!("Failed to handle OSDMap: {}", e);
                    }
                } else {
                    info!("OSDClient dropped, terminating OSDMap drain task");
                    break;
                }
            }
            info!("OSDClient OSDMap drain task terminated");
        });

        Ok(client)
    }

    /// Dispatch a session-specific message from an OSD
    ///
    /// This is called directly from the OSD session's io_task with explicit OSD context,
    /// following the Linux kernel pattern of passing `struct ceph_osd *osd` to handlers.
    ///
    /// Reference: ~/dev/linux/net/ceph/osd_client.c handle_backoff(struct ceph_osd *osd, ...)
    pub async fn dispatch_from_osd(&self, osd_id: i32, msg: msgr2::message::Message) -> Result<()> {
        let msg_type = msg.msg_type();
        debug!("Dispatching message 0x{:04x} from OSD {}", msg_type, osd_id);

        match msg_type {
            crate::messages::CEPH_MSG_OSD_OPREPLY => {
                self.handle_osd_op_reply_from_osd(osd_id, msg).await
            }
            crate::messages::CEPH_MSG_OSD_BACKOFF => {
                self.handle_backoff_from_osd(osd_id, msg).await
            }
            _ => {
                warn!(
                    "Unexpected session-specific message type 0x{:04x} from OSD {}",
                    msg_type, osd_id
                );
                Ok(())
            }
        }
    }

    /// Get the current OSDMap
    pub async fn get_osdmap(&self) -> Result<Arc<crate::osdmap::OSDMap>> {
        let osdmap = self.osdmap_rx.borrow().clone();
        match osdmap {
            Some(map) => Ok(map),
            None => Err(OSDClientError::Connection(
                "OSDMap not available".to_string(),
            )),
        }
    }

    /// Wait for OSDMap to be received
    ///
    /// This waits until the OSDClient has received an OSDMap from the monitor cluster.
    /// The OSDMap is required for all object operations as it contains the cluster topology.
    ///
    /// Should be called after subscribing to osdmap to ensure the map is available
    /// before performing object operations.
    pub async fn wait_for_osdmap(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdmap::OSDMap>> {
        let mut rx = self.osdmap_rx.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(map) = rx.borrow_and_update().as_ref() {
                    info!("OSDMap received via watch channel");
                    return Ok(Arc::clone(map));
                }
                rx.changed()
                    .await
                    .map_err(|_| OSDClientError::Connection("watch channel closed".into()))?;
            }
        })
        .await
        .map_err(|_| OSDClientError::Timeout(timeout))?
    }

    /// Wait for a specific OSDMap epoch
    pub async fn wait_for_epoch(
        &self,
        epoch: u32,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdmap::OSDMap>> {
        let mut rx = self.osdmap_rx.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(map) = rx.borrow_and_update().as_ref() {
                    if map.epoch.as_u32() >= epoch {
                        return Ok(Arc::clone(map));
                    }
                }
                rx.changed()
                    .await
                    .map_err(|_| OSDClientError::Connection("watch channel closed".into()))?;
            }
        })
        .await
        .map_err(|_| OSDClientError::Timeout(timeout))?
    }

    /// Get or create a session for an OSD
    async fn get_or_create_session(&self, osd_id: i32) -> Result<Arc<OSDSession>> {
        // Get OSD address early (before acquiring any locks)
        // This reduces lock contention by doing I/O outside critical section
        let current_addr = self.get_osd_address(osd_id).await?;

        // Check if we already have a session
        {
            let sessions = self.sessions.read().await;
            if let Some(session) = sessions.get(&osd_id) {
                if session.is_connected().await {
                    // Validate that session's address matches current OSDMap
                    // This prevents wasted reconnection attempts to stale addresses
                    // Reference: Ceph Objecter checks OSDMap during reconnect
                    let session_addr = session.get_peer_address().await;

                    // Compare addresses (ignoring nonce which can change)
                    if let Some(session_addr) = session_addr {
                        if session_addr.to_socket_addr() == current_addr.to_socket_addr() {
                            return Ok(Arc::clone(session));
                        } else {
                            info!(
                                "OSD {} address changed in OSDMap (was {:?}, now {:?}), creating new session",
                                osd_id,
                                session_addr.to_socket_addr(),
                                current_addr.to_socket_addr()
                            );
                            // Address changed, fall through to create new session
                        }
                    }
                }
            }
        }

        // Prepare session outside the write lock to minimize critical section
        // Get service auth provider from monitor client
        let auth_provider = self
            .mon_client
            .get_service_auth_provider()
            .await
            .map(|provider| Box::new(provider) as Box<dyn auth::AuthProvider>);

        let mut session = OSDSession::new(
            osd_id,
            self.entity_name.clone(),
            self.config.client_inc,
            auth_provider,
            self.global_id,
            self.map_tx.clone(),
            self.self_weak.clone(),
        );

        // Connect to OSD BEFORE acquiring write lock - this is the expensive operation
        // that can take seconds and should not block other session lookups
        info!("Creating new session for OSD {}", osd_id);
        session
            .connect(current_addr.clone(), self.shutdown_token.child_token())
            .await?;

        let session = Arc::new(session);

        // Now acquire write lock only to insert the connected session
        let mut sessions = self.sessions.write().await;

        // Double-check after acquiring write lock to avoid race condition
        if let Some(existing) = sessions.get(&osd_id) {
            if existing.is_connected().await {
                // Re-validate address with write lock held
                let session_addr = existing.get_peer_address().await;

                if let Some(session_addr) = session_addr {
                    if session_addr.to_socket_addr() == current_addr.to_socket_addr() {
                        // Another task created a session while we were connecting
                        // Close our redundant session and return existing one
                        let existing_clone = Arc::clone(existing);
                        drop(sessions);
                        session.close().await;
                        return Ok(existing_clone);
                    }
                }

                // Address changed, remove old session
                info!("Removing old session for OSD {} with stale address", osd_id);
                sessions.remove(&osd_id);
            }
            // Disconnected session stays in the map; it will be replaced below and
            // its pending ops kicked into the new session after creation.
        }

        // Insert the new session, replacing any disconnected session.
        // insert() returns the old value so we can kick its pending ops.
        let old_session = sessions.insert(osd_id, Arc::clone(&session));
        drop(sessions);

        // Kick ops from old disconnected session into new session.
        // Matches C++ Objecter::_kick_requests() called after _reopen_session():
        // session is opened first, then ops are resent via the new connection.
        // CRUSH remapping is handled separately by scan_requests_on_map_change().
        if let Some(old) = old_session {
            self.kick_into_session(&old, &session).await;
        }

        Ok(session)
    }

    /// Get OSD address from OSDMap
    async fn get_osd_address(&self, osd_id: i32) -> Result<denc::EntityAddr> {
        let osdmap = self.get_osdmap().await?;

        // Check if OSD exists
        if osd_id < 0 || osd_id as usize >= osdmap.osd_addrs_client.len() {
            return Err(OSDClientError::Connection(format!(
                "OSD {} not found in OSDMap",
                osd_id
            )));
        }

        // Get the address vector for this OSD
        let addrvec = &osdmap.osd_addrs_client[osd_id as usize];

        // Find a v2 address (msgr2 protocol)
        addrvec
            .addrs
            .iter()
            .find(|addr| matches!(addr.addr_type, denc::EntityAddrType::Msgr2))
            .cloned()
            .ok_or_else(|| {
                OSDClientError::Connection(format!("No msgr2 address found for OSD {}", osd_id))
            })
    }

    /// Map an object to OSDs using CRUSH
    async fn object_to_osds(&self, pool: u64, oid: &str) -> Result<(StripedPgId, Vec<i32>)> {
        let osdmap = self.get_osdmap().await?;

        // Find pool info
        let pool_info = osdmap
            .pools
            .get(&pool)
            .ok_or(OSDClientError::PoolNotFound(pool))?;

        // Create object locator
        let locator = crush::placement::ObjectLocator {
            pool_id: pool,
            key: String::new(),
            namespace: String::new(),
            hash: HASH_CALCULATE_FROM_NAME,
        };

        // Get CRUSH map
        let crush_map = osdmap
            .crush
            .as_ref()
            .ok_or_else(|| OSDClientError::Crush("No CRUSH map in OSDMap".into()))?;

        // Map object to PG and OSDs using CRUSH
        debug!(
            "OSD weights from map (max_osd={}): {:?}",
            osdmap.max_osd, osdmap.osd_weight
        );

        // Check if pool has hashpspool flag (modern pools)
        let hashpspool =
            PoolFlags::from_bits_truncate(pool_info.flags).contains(PoolFlags::HASHPSPOOL);

        let (pg, mut osds) = crush::placement::object_to_osds(
            crush_map,
            oid,
            &locator,
            pool_info.pg_num,
            pool_info.crush_rule as u32,
            &osdmap.osd_weight, // Use actual OSD weights from OSDMap (16.16 fixed-point format)
            pool_info.size as usize,
            hashpspool,
        )
        .map_err(|e| OSDClientError::Crush(format!("CRUSH placement failed: {}", e)))?;

        if osds.is_empty() {
            return Err(OSDClientError::NoOSDs);
        }

        // Check for PG overrides in OSDMap
        // These override CRUSH placement for various reasons (rebalancing, failures, manual overrides)
        // Note: crush::PgId and denc::PgId are now the same type (crush::PgId is re-exported by denc)
        let pgid = pg;

        // 1. Check pg_upmap (complete acting set override)
        if let Some(upmap_osds) = osdmap.pg_upmap.get(&pgid) {
            debug!(
                "Using pg_upmap override for PG {:?}: {:?}",
                pgid, upmap_osds
            );
            osds = upmap_osds.clone();
        }

        // 2. Check pg_temp (temporary override during recovery)
        if let Some(temp_osds) = osdmap.pg_temp.get(&pgid) {
            debug!("Using pg_temp override for PG {:?}: {:?}", pgid, temp_osds);
            osds = temp_osds.clone();
        }

        // 3. Check pg_upmap_items (fine-grained OSD replacements)
        if let Some(upmap_items) = osdmap.pg_upmap_items.get(&pgid) {
            debug!(
                "Applying pg_upmap_items to PG {:?}: {:?}",
                pgid, upmap_items
            );
            for &(from_osd, to_osd) in upmap_items {
                if let Some(pos) = osds.iter().position(|&osd| osd == from_osd) {
                    osds[pos] = to_osd;
                }
            }
        }

        // 4. Check pg_upmap_primaries (primary OSD override)
        if let Some(&primary_osd) = osdmap.pg_upmap_primaries.get(&pgid) {
            debug!(
                "Using pg_upmap_primaries override for PG {:?}: primary={}",
                pgid, primary_osd
            );
            // Move the primary to front if it's in the set
            if let Some(pos) = osds.iter().position(|&osd| osd == primary_osd) {
                osds.swap(0, pos);
            }
        }

        // Convert to StripedPgId
        let spgid = StripedPgId::from_pg(pg.pool, pg.seed);

        debug!("Mapped {}/{} to PG {:?}, OSDs: {:?}", pool, oid, pg, osds);

        Ok((spgid, osds))
    }

    /// Apply redirect to an operation
    ///
    /// This modifies the operation's target object and flags based on the redirect
    /// information from an EC pool. Matches the behavior of `combine_with_locator()`
    /// in C++ Objecter (~/dev/ceph/src/osd/osd_types.h).
    fn apply_redirect(op: &mut MOSDOp, redirect: &RequestRedirect) {
        // Update object locator from redirect
        op.object.pool = redirect.redirect_locator.pool_id;
        op.object.key = redirect.redirect_locator.key.clone();
        op.object.namespace = redirect.redirect_locator.namespace.clone();

        // If redirect specifies a different object name, use it
        if !redirect.redirect_object.is_empty() {
            op.object.oid = redirect.redirect_object.clone();
        }

        // Set redirect flags (from ~/dev/ceph/src/osdc/Objecter.cc:3744)
        let redirect_flags =
            OsdOpFlags::REDIRECTED | OsdOpFlags::IGNORE_CACHE | OsdOpFlags::IGNORE_OVERLAY;
        op.flags |= redirect_flags.bits();

        debug!(
            "Applied redirect: pool={}, oid={}, key={}, nspace={}",
            op.object.pool, op.object.oid, op.object.key, op.object.namespace
        );
    }

    /// Execute a built operation created with OpBuilder
    ///
    /// This bridges the OpBuilder API to the internal execute_op implementation.
    /// Provides a convenient way to execute operations built with the fluent OpBuilder API.
    ///
    /// # Arguments
    /// * `pool` - Pool ID
    /// * `oid` - Object name
    /// * `built_op` - Built operation from OpBuilder
    ///
    /// # Returns
    /// Returns the OpResult after handling all redirects
    ///
    /// # Example
    /// ```ignore
    /// use osdclient::OpBuilder;
    ///
    /// let op = OpBuilder::new()
    ///     .read(0, 4096)
    ///     .balance_reads()
    ///     .build();
    ///
    /// let result = client.execute_built_op(pool_id, "my-object", op).await?;
    /// ```
    pub async fn execute_built_op(
        &self,
        pool: u64,
        oid: &str,
        built_op: crate::operation::BuiltOp,
    ) -> Result<crate::types::OpResult> {
        let timeout = built_op.timeout;
        let priority = if built_op.priority < 0 {
            crate::messages::CEPH_MSG_PRIO_DEFAULT
        } else {
            built_op.priority
        };
        self.execute_op(pool, oid, built_op.into_ops(), timeout, priority)
            .await
    }

    /// Execute an OSD operation with automatic redirect handling
    ///
    /// This is the common pattern for all OSD operations:
    /// 1. Create object ID and calculate hash
    /// 2. Acquire throttle permit
    /// 3. Get OSDMap epoch
    /// 4. Build MOSDOp message
    /// 5. Redirect retry loop with automatic session management
    /// 6. Return OpResult for caller to process
    ///
    /// # Arguments
    /// * `pool` - Pool ID
    /// * `oid` - Object name
    /// * `ops` - Operations to execute
    ///
    /// # Returns
    /// Returns the OpResult after handling all redirects
    async fn execute_op(
        &self,
        pool: u64,
        oid: &str,
        ops: Vec<OSDOp>,
        timeout: Option<std::time::Duration>,
        priority: i32,
    ) -> Result<crate::types::OpResult> {
        // Create initial object ID
        let mut object = ObjectId::new(pool, oid);
        object.calculate_hash();

        // Calculate operation budget and acquire throttle permit
        let budget = calc_op_budget(&ops);
        let _throttle_permit = self.throttle.acquire(budget).await;
        debug!(
            "Acquired throttle permit: budget={} bytes, current_ops={}, current_bytes={}",
            budget,
            self.throttle.current_ops(),
            self.throttle.current_bytes()
        );

        // Get OSDMap epoch from OSDClient's own osdmap (not from MonClient)
        let osdmap = self.get_osdmap().await?;

        // Build initial message
        let flags = MOSDOp::calculate_flags(&ops);
        let mut msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch.as_u32(),
            flags,
            object.clone(),
            StripedPgId::from_pg(object.pool, 0), // Will be set in loop
            ops.clone(),
            crate::types::RequestId::new(&self.entity_name, 0, self.config.client_inc as i32),
            self.global_id,
        );

        // Redirect retry loop
        loop {
            // Map to OSDs based on current object
            let (spgid, osds) = self
                .object_to_osds(msg.object.pool, &msg.object.oid)
                .await?;
            let primary_osd = osds[0];
            msg.pgid = spgid;

            // Get session
            let session = self.get_or_create_session(primary_osd).await?;

            // Build request ID with fresh TID
            let tid = session.next_tid();
            msg.reqid =
                crate::types::RequestId::new(&self.entity_name, tid, self.config.client_inc as i32);

            // Submit operation (priority is set in message header)
            let result_rx = session.submit_op(msg.clone(), priority).await?;

            // Wait for result with timeout (per-op override or default from tracker)
            let effective_timeout = timeout.unwrap_or_else(|| self.tracker.operation_timeout());
            let result = tokio::time::timeout(effective_timeout, result_rx)
                .await
                .map_err(|_| OSDClientError::Timeout(effective_timeout))?
                .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

            // Check for redirect
            if let Some(redirect) = result.redirect {
                debug!(
                    "Received redirect to pool={}, object={}, retrying",
                    redirect.redirect_locator.pool_id,
                    if redirect.redirect_object.is_empty() {
                        msg.object.oid.as_str()
                    } else {
                        redirect.redirect_object.as_str()
                    }
                );
                Self::apply_redirect(&mut msg, &redirect);
                continue;
            }

            // No redirect, return result for caller to process
            return Ok(result);
        }
    }

    /// Check an OpResult for errors and return the first op's outdata
    ///
    /// Validates both the overall result code and the per-op return code,
    /// returning the outdata from the first operation on success.
    fn check_op_result(result: &crate::types::OpResult, op_name: &str) -> Result<()> {
        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: format!("{} failed", op_name),
            });
        }
        if let Some(op) = result.ops.first() {
            if op.return_code != 0 {
                return Err(OSDClientError::OSDError {
                    code: op.return_code,
                    message: format!("{} failed", op_name),
                });
            }
        }
        Ok(())
    }

    /// Read data from an object
    pub async fn read(&self, pool: u64, oid: &str, offset: u64, len: u64) -> Result<ReadResult> {
        debug!(
            "read pool={} oid={} offset={} len={}",
            pool, oid, offset, len
        );

        let ops = vec![OSDOp::read(offset, len)];
        let result = self
            .execute_op(pool, oid, ops, None, crate::messages::CEPH_MSG_PRIO_DEFAULT)
            .await?;

        Self::check_op_result(&result, "Read")?;

        let outdata = result
            .ops
            .first()
            .map(|op| op.outdata.clone())
            .unwrap_or_default();

        Ok(ReadResult {
            data: outdata,
            version: result.version,
        })
    }

    /// Sparse read data from an object
    ///
    /// Sparse read returns a map of extents indicating which regions of the object
    /// contain data, along with the actual data. This is useful for efficiently
    /// reading sparse objects (e.g., VM disk images with holes).
    pub async fn sparse_read(
        &self,
        pool: u64,
        oid: &str,
        offset: u64,
        len: u64,
    ) -> Result<crate::types::SparseReadResult> {
        debug!(
            "sparse_read pool={} oid={} offset={} len={}",
            pool, oid, offset, len
        );

        let ops = vec![OSDOp::sparse_read(offset, len)];
        let result = self
            .execute_op(pool, oid, ops, None, crate::messages::CEPH_MSG_PRIO_DEFAULT)
            .await?;

        Self::check_op_result(&result, "Sparse read")?;

        // Parse sparse read result from outdata
        // The outdata contains:
        // 1. Encoded map<uint64_t, uint64_t> (extent map: offset -> length)
        // 2. Encoded bufferlist (actual data)
        use crate::types::{SparseExtent, SparseReadResult};
        use denc::denc::Denc;

        let outdata = result
            .ops
            .first()
            .map(|op| op.outdata.clone())
            .unwrap_or_default();

        if outdata.is_empty() {
            return Ok(SparseReadResult {
                extents: vec![],
                data: bytes::Bytes::new(),
                version: result.version,
            });
        }

        let mut buf = outdata;

        // Decode extent map directly as Vec<SparseExtent>
        let extents = Vec::<SparseExtent>::decode(&mut buf, 0)
            .map_err(|e| OSDClientError::Internal(format!("Failed to decode extent map: {}", e)))?;

        // Decode bufferlist (actual data) using Denc
        let data = bytes::Bytes::decode(&mut buf, 0).map_err(|e| {
            OSDClientError::Internal(format!("Failed to decode bufferlist data: {}", e))
        })?;

        debug!(
            "Sparse read decoded: {} extents, {} data bytes",
            extents.len(),
            data.len()
        );

        Ok(SparseReadResult {
            extents,
            data,
            version: result.version,
        })
    }

    /// Write data to an object
    pub async fn write(
        &self,
        pool: u64,
        oid: &str,
        offset: u64,
        data: bytes::Bytes,
    ) -> Result<WriteResult> {
        debug!(
            "write pool={} oid={} offset={} len={}",
            pool,
            oid,
            offset,
            data.len()
        );

        let ops = vec![OSDOp::write(offset, data)];
        let result = self
            .execute_op(pool, oid, ops, None, crate::messages::CEPH_MSG_PRIO_DEFAULT)
            .await?;

        Self::check_op_result(&result, "Write")?;

        Ok(WriteResult {
            version: result.version,
        })
    }

    /// Write full object (overwrite)
    pub async fn write_full(
        &self,
        pool: u64,
        oid: &str,
        data: bytes::Bytes,
    ) -> Result<WriteResult> {
        debug!("write_full pool={} oid={} len={}", pool, oid, data.len());

        let ops = vec![OSDOp::write_full(data)];
        let result = self
            .execute_op(pool, oid, ops, None, crate::messages::CEPH_MSG_PRIO_DEFAULT)
            .await?;

        Self::check_op_result(&result, "Write")?;

        Ok(WriteResult {
            version: result.version,
        })
    }

    /// Get object statistics
    pub async fn stat(&self, pool: u64, oid: &str) -> Result<StatResult> {
        debug!("stat pool={} oid={}", pool, oid);
        let ops = vec![OSDOp::stat()];
        let result = self
            .execute_op(pool, oid, ops, None, crate::messages::CEPH_MSG_PRIO_DEFAULT)
            .await?;

        Self::check_op_result(&result, "Stat")?;

        // Parse stat data from outdata using OsdStatData's Denc implementation
        use denc::Denc;
        let outdata = result.ops.first().map(|op| &op.outdata[..]).unwrap_or(&[]);
        let stat_data = crate::denc_types::OsdStatData::decode(&mut &outdata[..], 0)
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode stat data: {}", e)))?;

        Ok(StatResult {
            size: stat_data.size,
            mtime: stat_data.mtime,
        })
    }

    /// Delete an object
    pub async fn delete(&self, pool: u64, oid: &str) -> Result<()> {
        debug!("delete pool={} oid={}", pool, oid);
        let ops = vec![OSDOp::delete()];
        let result = self
            .execute_op(pool, oid, ops, None, crate::messages::CEPH_MSG_PRIO_DEFAULT)
            .await?;

        Self::check_op_result(&result, "Delete")?;

        Ok(())
    }

    /// List objects in a pool
    ///
    /// Lists objects in the specified pool, iterating through all PGs.
    /// Returns a list of objects and an optional continuation cursor for pagination.
    ///
    /// # Arguments
    /// * `pool` - Pool ID to list objects from
    /// * `cursor` - Optional cursor for pagination (format: "pg:hash")
    /// * `max_entries` - Maximum number of entries to return
    pub async fn list(
        &self,
        pool: u64,
        cursor: Option<String>,
        max_entries: u64,
    ) -> Result<ListResult> {
        debug!(
            "list pool={} cursor={:?} max_entries={}",
            pool, cursor, max_entries
        );

        // Parse cursor to extract starting PG and hobject position
        // Format: "pg:hash" where hash is the hobject hash
        let (mut current_pg, mut hobject_cursor) = if let Some(ref c) = cursor {
            if let Some((pg_str, hash_str)) = c.split_once(':') {
                let pg = pg_str.parse::<u32>().unwrap_or(0);
                let hash = hash_str.parse::<u32>().unwrap_or(0);
                (pg, denc::HObject::new(pool, String::new(), hash))
            } else {
                let pg = c.parse::<u32>().unwrap_or(0);
                (pg, denc::HObject::empty_cursor(pool))
            }
        } else {
            (0, denc::HObject::empty_cursor(pool))
        };

        // Get OSDMap to look up pool info and pg_num
        let osdmap = self.get_osdmap().await?;

        // Find pool info
        let pool_info = osdmap
            .pools
            .get(&pool)
            .ok_or(OSDClientError::PoolNotFound(pool))?;

        // Get number of PGs in this pool
        let pg_num = pool_info.pg_num;
        let starting_pg = current_pg;

        // Check if pool has hashpspool flag
        let hashpspool =
            PoolFlags::from_bits_truncate(pool_info.flags).contains(PoolFlags::HASHPSPOOL);

        let crush_map = osdmap
            .crush
            .as_ref()
            .ok_or_else(|| OSDClientError::Crush("No CRUSH map in OSDMap".into()))?;

        let osdmap_epoch = osdmap.epoch;

        // Collect entries from PGs until we have enough or reach the end
        let mut all_entries = Vec::new();

        // Loop through PGs starting from current_pg
        loop {
            debug!(
                "Querying PG {}/{}, collected {} entries so far",
                current_pg,
                pg_num,
                all_entries.len()
            );

            // Create StripedPgId for the current PG
            let spgid = StripedPgId::from_pg(pool, current_pg);

            // Look up which OSDs handle this PG using CRUSH
            let pg = crush::placement::PgId {
                pool,
                seed: current_pg,
            };

            let osds = crush::placement::pg_to_osds(
                crush_map,
                pg,
                pool_info.crush_rule as u32,
                &osdmap.osd_weight,
                pool_info.size as usize,
                hashpspool,
            )
            .map_err(|e| OSDClientError::Crush(format!("PG->OSD mapping failed: {}", e)))?;

            if osds.is_empty() {
                // Skip this PG if no OSDs
                current_pg += 1;
                if current_pg >= pg_num {
                    break;
                }
                hobject_cursor = denc::HObject::empty_cursor(pool);
                continue;
            }

            let primary_osd = osds[0];

            // Get session
            let session = self.get_or_create_session(primary_osd).await?;

            // Create object ID (empty for PGLS)
            let mut object = ObjectId::new(pool, "");
            object.hash = 0;

            // Create pgls operation
            // Request up to max_entries total, but we may get less per PG
            let remaining = max_entries.saturating_sub(all_entries.len() as u64);
            let ops = vec![OSDOp::pgls(
                remaining,
                hobject_cursor.clone(),
                osdmap_epoch.as_u32(),
            )];

            // Acquire throttle permit
            let _throttle_permit = self.throttle.acquire(calc_op_budget(&ops)).await;

            // Build request ID
            let tid = session.next_tid();
            let reqid =
                crate::types::RequestId::new(&self.entity_name, tid, self.config.client_inc as i32);

            // Build message
            let flags = MOSDOp::calculate_flags(&ops);
            let msg = MOSDOp::new(
                self.config.client_inc,
                osdmap.epoch.as_u32(),
                flags,
                object,
                spgid,
                ops,
                reqid,
                self.global_id,
            );

            // Submit operation (use default priority for internal PGLS operations)
            let result_rx = session
                .submit_op(msg, crate::messages::CEPH_MSG_PRIO_DEFAULT)
                .await?;

            // Wait for result with timeout
            let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
                .await
                .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
                .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

            // For PGLS: result = 1 means "reached end of PG" (success)
            //           result = 0 means "more objects available" (success)
            //           result < 0 means error
            if result.result < 0 {
                return Err(OSDClientError::OSDError {
                    code: result.result,
                    message: format!("List operation failed for PG {}", current_pg),
                });
            }

            // Parse the pg_nls_response from outdata
            if result.ops.is_empty() {
                return Err(OSDClientError::Internal(
                    "No operation reply in list response".into(),
                ));
            }

            let outdata = &result.ops[0].outdata;

            // Decode pg_nls_response_t using proper Denc
            let response = denc::PgNlsResponse::decode(&mut outdata.as_ref(), 0).map_err(|e| {
                OSDClientError::Decoding(format!("Failed to decode pg_nls_response: {}", e))
            })?;

            debug!(
                "PG {} returned {} entries, handle.hash={:#x}, result={}",
                current_pg,
                response.entries.len(),
                response.handle.hash,
                result.result
            );

            // Convert entries to ListObjectEntry
            for entry in response.entries {
                all_entries.push(ListObjectEntry::new(
                    &entry.nspace,
                    &entry.oid,
                    &entry.locator,
                ));
            }

            // Check if we have enough entries
            if all_entries.len() >= max_entries as usize {
                // Return with cursor pointing to current position
                let cursor = if response.handle.hash == u32::MAX {
                    // End of this PG, next request should start at next PG
                    let next_pg = current_pg + 1;
                    if next_pg >= pg_num {
                        None // End of pool
                    } else {
                        Some(format!("{}:0", next_pg))
                    }
                } else {
                    // More objects in this PG
                    Some(format!("{}:{}", current_pg, response.handle.hash))
                };
                return Ok(ListResult {
                    entries: all_entries,
                    cursor,
                });
            }

            // Check if we reached the end of this PG
            if response.handle.hash == u32::MAX || result.result == 1 {
                // Move to next PG
                current_pg += 1;
                if current_pg >= pg_num {
                    // Reached end of all PGs
                    debug!(
                        "Reached end of pool at PG {}, total {} entries",
                        current_pg,
                        all_entries.len()
                    );
                    return Ok(ListResult {
                        entries: all_entries,
                        cursor: None, // End of pool
                    });
                }
                // Check if we've wrapped around to where we started
                if current_pg == starting_pg && all_entries.is_empty() {
                    // Avoid infinite loop if pool is empty
                    return Ok(ListResult {
                        entries: all_entries,
                        cursor: None,
                    });
                }
                // Reset cursor for next PG
                hobject_cursor = denc::HObject::empty_cursor(pool);
            } else {
                // Continue within same PG using the returned handle as cursor
                hobject_cursor = response.handle;
            }
        }

        // Shouldn't reach here, but return what we have
        Ok(ListResult {
            entries: all_entries,
            cursor: None,
        })
    }

    /// List all pools in the cluster
    ///
    /// Returns a list of all pools with their IDs and names.
    pub async fn list_pools(&self) -> Result<Vec<crate::types::PoolInfo>> {
        debug!("Listing pools");

        let osdmap = self.get_osdmap().await?;

        // Extract pool information from OSDMap
        let mut pools = Vec::new();
        for pool_id in osdmap.pools.keys() {
            if let Some(pool_name) = osdmap.pool_name.get(pool_id) {
                pools.push(crate::types::PoolInfo {
                    pool_id: *pool_id,
                    pool_name: pool_name.clone(),
                });
            }
        }

        Ok(pools)
    }

    /// Handle pool operation result: request OSDMap update if needed
    ///
    /// After a successful pool create/delete, the OSDMap epoch advances.
    /// This ensures we receive the updated map.
    async fn handle_pool_op_result(&self, result: &monclient::PoolOpResult) -> Result<()> {
        if !result.is_success() {
            return Err(OSDClientError::Other(format!(
                "Pool operation failed with code {}",
                result.reply_code
            )));
        }

        let target_epoch = result.epoch;
        let current_epoch = self
            .osdmap_rx
            .borrow()
            .as_ref()
            .map(|m| m.epoch.as_u32())
            .unwrap_or(0);

        if target_epoch > current_epoch {
            debug!(
                "Requesting OSDMap update from epoch {} to {}",
                current_epoch, target_epoch
            );
            self.mon_client
                .subscribe("osdmap", current_epoch as u64, 0)
                .await
                .ok();
        }

        Ok(())
    }

    /// Create a new pool
    ///
    /// This is a simplified interface for pool creation using MPoolOp messages.
    /// For advanced pool creation with custom parameters (pg_num, pgp_num, pool_type, etc.),
    /// use MonClient's invoke() method with a mon_command instead.
    ///
    /// # Arguments
    /// * `pool_name` - Name of the pool to create
    /// * `crush_rule` - Optional CRUSH rule ID (uses cluster default if None)
    ///
    /// # Returns
    /// * `Ok(())` if the pool was created successfully
    /// * `Err(OSDClientError)` if the operation failed
    pub async fn create_pool(&self, pool_name: &str, crush_rule: Option<i16>) -> Result<()> {
        debug!("Creating pool: {}", pool_name);

        // Get current OSDMap epoch
        let version = {
            let osdmap = self.osdmap_rx.borrow().clone();
            osdmap.map(|map| map.epoch.as_u32() as u64).unwrap_or(0)
        };

        // Create MPoolOp message
        let msg = monclient::MPoolOp::create_pool(
            self.fsid.bytes,
            pool_name.to_string(),
            crush_rule,
            version,
        );

        // Send via MonClient
        let result = self
            .mon_client
            .send_poolop(msg)
            .await
            .map_err(|e| OSDClientError::Connection(format!("Pool operation failed: {}", e)))?;

        if result.is_success() {
            debug!("Pool created successfully: {}", pool_name);
            self.handle_pool_op_result(&result).await
        } else {
            Err(OSDClientError::Other(format!(
                "Pool operation failed with code {}",
                result.reply_code
            )))
        }
    }

    /// Delete a pool
    ///
    /// This operation uses the MPoolOp binary message protocol to delete a pool,
    /// matching the official librados implementation.
    ///
    /// # Arguments
    /// * `pool_name` - Name of the pool to delete
    /// * `confirm` - Must be set to true to confirm deletion (safety check)
    ///
    /// # Returns
    /// * `Ok(())` if the pool was deleted successfully
    /// * `Err(OSDClientError)` if the operation failed
    ///
    /// # Safety
    /// This operation is destructive and will delete all data in the pool.
    /// The `confirm` parameter must be explicitly set to `true`.
    pub async fn delete_pool(&self, pool_name: &str, confirm: bool) -> Result<()> {
        if !confirm {
            return Err(OSDClientError::Other(
                "Pool deletion requires explicit confirmation".into(),
            ));
        }

        debug!("Deleting pool: {}", pool_name);

        // Look up pool ID and epoch from OSDMap
        let (pool_id, version) = {
            let osdmap = self
                .osdmap_rx
                .borrow()
                .clone()
                .ok_or_else(|| OSDClientError::Other("OSD map not available yet".into()))?;

            let pool_id = osdmap
                .pool_name
                .iter()
                .find(|(_, name)| name.as_str() == pool_name)
                .map(|(id, _)| *id as u32)
                .ok_or_else(|| OSDClientError::Other(format!("Pool '{}' not found", pool_name)))?;

            let version = osdmap.epoch.as_u32() as u64;

            (pool_id, version)
        };

        debug!(
            "Deleting pool '{}' with ID {} (OSDMap epoch {})",
            pool_name, pool_id, version
        );

        // Create and send MPoolOp delete message
        let msg = monclient::MPoolOp::delete_pool(self.fsid.bytes, pool_id, version);

        let result = self
            .mon_client
            .send_poolop(msg)
            .await
            .map_err(|e| OSDClientError::Connection(format!("Pool operation failed: {}", e)))?;

        if result.is_success() {
            debug!("Pool deleted successfully: {}", pool_name);
            self.handle_pool_op_result(&result).await
        } else {
            Err(OSDClientError::Other(format!(
                "Pool operation failed with code {}",
                result.reply_code
            )))
        }
    }

    /// Scan pending ops after OSDMap update, resend if target changed
    ///
    /// This implements Ceph's `_scan_requests` pattern from Objecter.
    async fn scan_requests_on_map_change(&self, new_epoch: u32) -> Result<()> {
        let osdmap = self.get_osdmap().await?;

        // Collect session metadata under read lock, then release immediately
        // This minimizes lock contention by avoiding I/O operations while holding the lock
        let session_snapshot: Vec<(i32, Arc<OSDSession>)> = {
            let sessions = self.sessions.read().await;
            sessions
                .iter()
                .map(|(id, sess)| (*id, Arc::clone(sess)))
                .collect()
        };

        let mut need_resend = Vec::new();
        // Sessions whose OSD is down or whose address changed need to be
        // closed so that the next operation creates a fresh connection.
        // Follows Ceph Objecter::_scan_requests() which closes sessions for
        // down or address-changed OSDs.
        let mut sessions_to_close: Vec<i32> = Vec::new();

        // Process sessions without holding the lock
        for (osd_id, session) in session_snapshot {
            // --- Phase 1: Check session-level OSD health ---
            // Reference: Ceph Objecter::_scan_requests() checks is_up() for
            // every open session and closes sessions to down OSDs.
            let should_close = if osdmap.is_down(osd_id) {
                info!(
                    "OSD {} is DOWN in epoch {}, closing session and migrating pending ops",
                    osd_id, new_epoch
                );
                true
            } else if session.is_connected().await {
                // --- Phase 2: Check if the OSD address changed ---
                // Reference: Ceph Objecter checks the OSD address in the OSDMap
                // against the session's peer address and re-opens the session
                // when they diverge (e.g. OSD restarted on a different port).
                self.session_address_stale(&session, osd_id, &osdmap, new_epoch)
                    .await
            } else {
                false
            };

            if should_close {
                sessions_to_close.push(osd_id);
                self.drain_session_ops(&session, &osdmap, new_epoch, &mut need_resend)
                    .await;
                continue;
            }

            // --- Phase 3: Per-operation target check (existing logic) ---
            let metadata = session.get_pending_ops_metadata().await;
            for (tid, pool_id, object_id, _osdmap_epoch) in metadata {
                // Check if pool deleted
                if !osdmap.pools.contains_key(&pool_id) {
                    if let Some(pending_op) = session.remove_pending_op(tid).await {
                        let _ = pending_op
                            .result_tx
                            .send(Err(OSDClientError::PoolNotFound(pool_id)));
                    }
                    continue;
                }

                // Check if target changed
                let (_, new_osds) = self.object_to_osds(pool_id, &object_id).await?;
                let new_primary = new_osds.first().copied().unwrap_or(-1);

                if new_primary != osd_id {
                    if let Some(mut op) = session.remove_pending_op(tid).await {
                        // Mark as needing resend, then update target
                        op.state = crate::types::OpState::NeedsResend;
                        op.target.update(new_epoch, new_primary, new_osds.clone());
                        // Transition back to Queued for resubmission
                        op.state = crate::types::OpState::Queued;
                        need_resend.push((new_primary, op));
                    }
                }
            }
        }

        // Close sessions to down or address-changed OSDs.
        // This ensures the next get_or_create_session() creates a fresh
        // connection with the updated address.
        if !sessions_to_close.is_empty() {
            let mut sessions = self.sessions.write().await;
            for osd_id in &sessions_to_close {
                if let Some(session) = sessions.remove(osd_id) {
                    info!("Removing stale session for OSD {}", osd_id);
                    // Release write lock before closing session (which may do I/O)
                    drop(sessions);
                    session.close().await;
                    // Re-acquire write lock if there are more sessions to close
                    sessions = self.sessions.write().await;
                }
            }
        }

        // Resend to new targets
        for (new_osd, pending_op) in need_resend {
            let session = self.get_or_create_session(new_osd).await?;
            if let Err(e) = session.insert_migrated_op(pending_op, new_epoch).await {
                warn!("Failed to migrate operation to OSD {}: {}", new_osd, e);
            }
        }
        Ok(())
    }

    /// Check if a session's address no longer matches the OSDMap.
    ///
    /// Returns `true` when the session's peer address does not appear in the
    /// OSDMap's client address vector for the given OSD, indicating the OSD
    /// restarted on a different address.
    async fn session_address_stale(
        &self,
        session: &OSDSession,
        osd_id: i32,
        osdmap: &crate::osdmap::OSDMap,
        new_epoch: u32,
    ) -> bool {
        let Some(session_addr) = session.get_peer_address().await else {
            return false;
        };
        let Some(map_addrvec) = osdmap.get_osd_addr(osd_id) else {
            return false;
        };
        let session_sockaddr = session_addr.to_socket_addr();
        let map_has_match = map_addrvec.addrs.iter().any(|a| {
            matches!(a.addr_type, denc::EntityAddrType::Msgr2)
                && a.to_socket_addr() == session_sockaddr
        });
        if !map_has_match {
            info!(
                "OSD {} address changed in epoch {} (session: {:?}, map: {:?}), closing session",
                osd_id, new_epoch, session_sockaddr, map_addrvec
            );
        }
        !map_has_match
    }

    /// Drain all pending operations from a session that is about to be closed.
    ///
    /// Each operation is either failed (if its pool was deleted) or re-targeted
    /// via CRUSH and appended to `need_resend` for resubmission.
    async fn drain_session_ops(
        &self,
        session: &OSDSession,
        osdmap: &crate::osdmap::OSDMap,
        new_epoch: u32,
        need_resend: &mut Vec<(i32, crate::session::PendingOp)>,
    ) {
        let metadata = session.get_pending_ops_metadata().await;
        for (tid, pool_id, object_id, _osdmap_epoch) in metadata {
            if !osdmap.pools.contains_key(&pool_id) {
                if let Some(pending_op) = session.remove_pending_op(tid).await {
                    let _ = pending_op
                        .result_tx
                        .send(Err(OSDClientError::PoolNotFound(pool_id)));
                }
                continue;
            }
            if let Ok((_, new_osds)) = self.object_to_osds(pool_id, &object_id).await {
                let new_primary = new_osds.first().copied().unwrap_or(-1);
                if let Some(mut op) = session.remove_pending_op(tid).await {
                    op.state = crate::types::OpState::NeedsResend;
                    op.target.update(new_epoch, new_primary, new_osds.clone());
                    op.state = crate::types::OpState::Queued;
                    need_resend.push((new_primary, op));
                }
            }
        }
    }

    /// Resend pending ops from a disconnected session into a freshly created one.
    ///
    /// Called after the new session for an OSD has been connected, matching the
    /// C++ pattern in `Objecter::_kick_requests()` which runs after
    /// `_reopen_session()`: the session is opened first, then ops are resent via
    /// the new connection.
    ///
    /// CRUSH remapping (ops moving to a *different* OSD) is handled separately by
    /// `scan_requests_on_map_change()` when OSDMap updates arrive.  This function
    /// only resubmits to the same OSD's new session, so there is no mutual
    /// recursion with `get_or_create_session` and no boxing is required.
    async fn kick_into_session(&self, old_session: &OSDSession, new_session: &Arc<OSDSession>) {
        if self.shutdown_token.is_cancelled() {
            return;
        }

        let osdmap = match self.get_osdmap().await {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "kick_into_session: OSD {} has pending ops but no OSDMap: {}",
                    new_session.osd_id, e
                );
                return;
            }
        };

        let metadata = old_session.get_pending_ops_metadata().await;
        if metadata.is_empty() {
            return;
        }

        info!(
            "kick_into_session: resending {} ops to OSD {}",
            metadata.len(),
            new_session.osd_id
        );

        let new_epoch = osdmap.epoch.as_u32();

        for (tid, pool_id, _, _) in metadata {
            let Some(mut pending_op) = old_session.remove_pending_op(tid).await else {
                continue;
            };

            // Pool deleted — fail immediately.
            if !osdmap.pools.contains_key(&pool_id) {
                let _ = pending_op
                    .result_tx
                    .send(Err(OSDClientError::PoolNotFound(pool_id)));
                continue;
            }

            pending_op.state = crate::types::OpState::Queued;

            if let Err(e) = new_session.insert_migrated_op(pending_op, new_epoch).await {
                warn!(
                    "kick_into_session: failed to resubmit tid {} to OSD {}: {}",
                    tid, new_session.osd_id, e
                );
            }
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) {
        info!("Shutting down OSDClient");

        // Shutdown tracker first to stop timeout callbacks
        self.tracker.shutdown();

        // Cancel all I/O tasks (child tokens are cancelled automatically)
        self.shutdown_token.cancel();

        // Await all I/O tasks to ensure they have stopped
        let sessions = self.sessions.read().await;
        for (_, session) in sessions.iter() {
            session.close().await;
        }

        info!("OSDClient shutdown complete");
    }

    /// Handle OSDMap message (moved from MonClient)
    async fn handle_osdmap(&self, msg: msgr2::message::Message) -> Result<()> {
        use monclient::messages::MOSDMap;

        info!("Handling OSDMap message ({} bytes)", msg.front.len());

        // Decode MOSDMap
        use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
        let header = CephMsgHeader::new(MOSDMap::msg_type(), MOSDMap::msg_version(0));
        let mosdmap = MOSDMap::decode_payload(&header, &msg.front, &[], &[])
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode MOSDMap: {}", e)))?;
        info!(
            "Received MOSDMap: epochs [{}..{}], {} full maps, {} incremental maps",
            mosdmap.get_first(),
            mosdmap.get_last(),
            mosdmap.maps.len(),
            mosdmap.incremental_maps.len()
        );

        // 1. Validate FSID
        if mosdmap.fsid != self.fsid.bytes {
            warn!(
                "Ignoring OSDMap with wrong fsid (expected {:?}, got {:?})",
                self.fsid.bytes, mosdmap.fsid
            );
            return Ok(());
        }

        // 2. Check if we've already processed these epochs
        let current_epoch = self
            .osdmap_rx
            .borrow()
            .as_ref()
            .map(|m| m.epoch)
            .unwrap_or(denc::Epoch::new(0));

        if mosdmap.get_last() <= current_epoch.as_u32() {
            info!(
                "Ignoring OSDMap epochs [{}..{}] <= current epoch {}",
                mosdmap.get_first(),
                mosdmap.get_last(),
                current_epoch
            );
            return Ok(());
        }

        debug!(
            "Processing OSDMap epochs [{}..{}] > current epoch {}",
            mosdmap.get_first(),
            mosdmap.get_last(),
            current_epoch
        );

        // 3. Process epochs sequentially
        let mut updated = false;
        let mut new_map: Option<Arc<crate::osdmap::OSDMap>> = None;
        {
            let current_map = self.osdmap_rx.borrow().clone();

            if current_epoch.as_u32() > 0 {
                // We have a current map, apply updates sequentially
                let mut working_map = current_map;
                for e in (current_epoch.as_u32() + 1)..=mosdmap.get_last() {
                    let current_map_epoch = working_map
                        .as_ref()
                        .map(|m| m.epoch)
                        .unwrap_or(denc::Epoch::new(0));

                    if current_map_epoch == denc::Epoch::new(e - 1)
                        && mosdmap.incremental_maps.contains_key(&e)
                    {
                        // Apply incremental
                        debug!("Applying incremental OSDMap for epoch {}", e);
                        let inc_bl = mosdmap.incremental_maps.get(&e).unwrap();

                        match crate::osdmap::OSDMapIncremental::decode_versioned(
                            &mut inc_bl.as_ref(),
                            0,
                        ) {
                            Ok(inc_map) => {
                                debug!(
                                    "Decoded incremental: epoch={}, {} new pools, {} old pools",
                                    inc_map.epoch,
                                    inc_map.new_pools.len(),
                                    inc_map.old_pools.len()
                                );

                                if let Some(current_map) = &working_map {
                                    let mut updated_map = (**current_map).clone();
                                    if let Err(err) = inc_map.apply_to(&mut updated_map) {
                                        warn!("Failed to apply incremental epoch {}: {}", e, err);
                                    } else {
                                        working_map = Some(Arc::new(updated_map));
                                        updated = true;
                                    }
                                }
                            }
                            Err(err) => {
                                warn!("Failed to decode incremental epoch {}: {}", e, err);
                            }
                        }
                    } else if mosdmap.maps.contains_key(&e) {
                        // Use full map
                        debug!("Using full OSDMap for epoch {}", e);
                        let full_bl = mosdmap.maps.get(&e).unwrap();
                        match crate::osdmap::OSDMap::decode_versioned(&mut full_bl.as_ref(), 0) {
                            Ok(full_map) => {
                                debug!("Decoded full OSDMap: epoch={}", full_map.epoch);
                                working_map = Some(Arc::new(full_map));
                                updated = true;
                            }
                            Err(err) => {
                                warn!("Failed to decode full map epoch {}: {}", e, err);
                            }
                        }
                    } else {
                        warn!("Missing epoch {} (incremental and full)", e);
                    }
                }
                new_map = working_map;
            } else {
                // No current map, use latest full map
                if let Some((&latest_epoch, full_bl)) = mosdmap.maps.iter().max_by_key(|(e, _)| **e)
                {
                    debug!("Using latest full OSDMap (epoch {})", latest_epoch);
                    match crate::osdmap::OSDMap::decode_versioned(&mut full_bl.as_ref(), 0) {
                        Ok(full_map) => {
                            info!("Initial OSDMap loaded: epoch={}", full_map.epoch);
                            new_map = Some(Arc::new(full_map));
                            updated = true;
                        }
                        Err(err) => {
                            warn!("Failed to decode initial full map: {}", err);
                        }
                    }
                }
            }
        }

        // 4. Update watch channel and rescan pending operations if map updated
        if updated {
            if let Some(map) = new_map {
                let final_epoch = map.epoch;
                self.osdmap_tx.send(Some(map)).ok();

                info!(
                    "OSDMap updated to epoch {}, rescanning pending operations",
                    final_epoch
                );

                // Notify MonClient that we received this osdmap epoch
                // This allows MonClient to track subscription state and renew if needed
                if let Err(e) = self
                    .mon_client
                    .notify_map_received("osdmap", u64::from(u32::from(final_epoch)))
                    .await
                {
                    warn!(
                        "Failed to notify MonClient of osdmap epoch {}: {}",
                        final_epoch, e
                    );
                }

                // Call scan_requests_on_map_change
                if let Err(e) = self.scan_requests_on_map_change(final_epoch.as_u32()).await {
                    warn!("Failed to rescan requests after OSDMap update: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Handle OSD operation reply from a specific OSD
    ///
    /// Called with explicit OSD context (Linux kernel pattern)
    async fn handle_osd_op_reply_from_osd(
        &self,
        osd_id: i32,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        use crate::messages::MOSDOpReply;

        let tid = msg.tid();

        // Decode the reply
        use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
        let header = CephMsgHeader::new(MOSDOpReply::msg_type(), MOSDOpReply::msg_version(0));
        let reply =
            MOSDOpReply::decode_payload(&header, &msg.front, &[], &msg.data).map_err(|e| {
                OSDClientError::Decoding(format!("Failed to decode MOSDOpReply: {}", e))
            })?;

        debug!("OSD {} sent OSDOpReply for tid={}", osd_id, tid);

        // Get the session for this OSD
        let sessions = self.sessions.read().await;
        let session = sessions.get(&osd_id).ok_or_else(|| {
            OSDClientError::Connection(format!("No session found for OSD {}", osd_id))
        })?;

        // Check if retry is needed (returns Some if EAGAIN on replica read)
        if let Some((pending_op, new_flags)) = session.handle_osd_op_reply(tid, reply).await {
            debug!(
                "OSD {} EAGAIN on replica read tid {}, retrying to primary (flags: 0x{:x} -> 0x{:x})",
                osd_id, tid, pending_op.op.flags, new_flags
            );

            // Resubmit with new flags
            if let Err(e) = session
                .resubmit_with_new_flags(tid, pending_op, new_flags)
                .await
            {
                error!("Failed to resubmit operation tid {}: {}", tid, e);
            }
        }

        Ok(())
    }

    /// Handle OSD backoff from a specific OSD
    ///
    /// Called with explicit OSD context (Linux kernel pattern)
    /// The ACK must be sent back to the same OSD that sent the backoff
    async fn handle_backoff_from_osd(
        &self,
        osd_id: i32,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        use crate::messages::{
            MOSDBackoff, CEPH_OSD_BACKOFF_OP_ACK_BLOCK, CEPH_OSD_BACKOFF_OP_BLOCK,
            CEPH_OSD_BACKOFF_OP_UNBLOCK,
        };

        // Decode the backoff message
        use msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
        let header = CephMsgHeader::new(MOSDBackoff::msg_type(), MOSDBackoff::msg_version(0));
        let backoff = MOSDBackoff::decode_payload(&header, &msg.front, &[], &[]).map_err(|e| {
            OSDClientError::Decoding(format!("Failed to decode MOSDBackoff: {}", e))
        })?;

        debug!(
            "OSD {} sent backoff op={} for pgid={}:{}.{}",
            osd_id, backoff.op, backoff.pgid.pool, backoff.pgid.seed, backoff.pgid.shard
        );

        // Get the session for this OSD (the one that sent the backoff)
        let sessions = self.sessions.read().await;
        let session = sessions.get(&osd_id).ok_or_else(|| {
            OSDClientError::Connection(format!("No session found for OSD {}", osd_id))
        })?;

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

                // Register backoff in session
                {
                    let tracker = session.backoff_tracker();
                    let mut tracker = tracker.write().await;
                    let entry = BackoffEntry {
                        pgid: backoff.pgid,
                        id: backoff.id,
                        begin: backoff.begin.clone(),
                        end: backoff.end.clone(),
                    };
                    tracker.register(entry);
                }

                // Send ACK_BLOCK reply through session's send channel
                let ack = MOSDBackoff::new(
                    backoff.pgid,
                    backoff.map_epoch,
                    CEPH_OSD_BACKOFF_OP_ACK_BLOCK,
                    backoff.id,
                    backoff.begin,
                    backoff.end,
                );

                use msgr2::ceph_message::CephMessagePayload;
                match ack.encode_payload(0) {
                    Ok(payload) => {
                        let msg = msgr2::message::Message::new(
                            crate::messages::CEPH_MSG_OSD_BACKOFF,
                            payload,
                        )
                        .with_version(MOSDBackoff::VERSION);

                        let send_tx = session.send_tx();
                        if let Err(e) = send_tx.send(msg).await {
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
                    "OSD {} lifts backoff: pgid={}:{}.{}, id={}, range=[{:?}, {:?})",
                    osd_id,
                    backoff.pgid.pool,
                    backoff.pgid.seed,
                    backoff.pgid.shard,
                    backoff.id,
                    backoff.begin,
                    backoff.end
                );

                // Remove backoff from session
                {
                    let tracker = session.backoff_tracker();
                    let mut tracker = tracker.write().await;
                    tracker.remove_by_id(backoff.id, &backoff.begin, &backoff.end);
                }

                // Resend operations that were in the backoff range
                session
                    .resend_ops_in_range(&backoff.pgid, &backoff.begin, &backoff.end)
                    .await;
            }

            _ => {
                warn!(
                    "Received unknown backoff operation {} from OSD {}",
                    backoff.op, osd_id
                );
            }
        }

        Ok(())
    }
}
