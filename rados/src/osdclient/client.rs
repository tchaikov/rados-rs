//! RADOS OSD Client
//!
//! Main entry point for performing object operations against a Ceph cluster.

use crate::monclient::MOSDMap;
use crate::msgr2::{MapReceiver, MapSender};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::denc::{Denc, VersionedEncode};

use crate::osdclient::backoff::BackoffEntry;
use crate::osdclient::error::{OSDClientError, Result};
use crate::osdclient::messages::MOSDOp;
use crate::osdclient::session::OSDSession;
use crate::osdclient::throttle::Throttle;
use crate::osdclient::tracker::{Tracker, TrackerConfig};
use crate::osdclient::types::{
    ListObjectEntry, ListResult, OSDOp, ObjectId, ObjectLocator, OsdOpFlags, PoolFlags, ReadResult,
    RequestRedirect, StatResult, StripedPgId, WriteResult, calc_op_budget,
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
            max_inflight_ops: crate::osdclient::throttle::DEFAULT_MAX_OPS,
            max_inflight_bytes: crate::osdclient::throttle::DEFAULT_MAX_BYTES,
        }
    }
}

/// Main OSD client for performing object operations
pub struct OSDClient {
    config: OSDClientConfig,
    mon_client: Arc<crate::monclient::MonClient>,
    sessions: Arc<RwLock<HashMap<i32, Arc<OSDSession>>>>,
    pub(crate) tracker: Arc<Tracker>,
    /// Request throttle to prevent resource exhaustion
    throttle: Arc<Throttle>,
    /// Entity name (e.g., "client.admin") from MonClient auth config
    entity_name: String,
    /// Global ID from monitor authentication (used in entity_name for request IDs)
    global_id: u64,
    /// Cluster FSID for OSDMap validation
    fsid: crate::UuidD,
    /// Current OSDMap (watch channel for efficient distribution)
    osdmap_tx: watch::Sender<Option<Arc<crate::osdclient::osdmap::OSDMap>>>,
    osdmap_rx: watch::Receiver<Option<Arc<crate::osdclient::osdmap::OSDMap>>>,
    /// Channel for routing MOSDMap messages to sessions
    map_tx: MapSender<MOSDMap>,
    /// Shutdown token for graceful termination
    shutdown_token: CancellationToken,
    /// Weak self-reference for session creation
    self_weak: std::sync::Weak<Self>,
    /// Set to true once we detect our address in the OSDMap blocklist.
    /// After that, all new operations fail immediately with `Blocklisted`.
    blocklisted: AtomicBool,
    /// Minimum OSDMap epoch required before any op is sent.
    ///
    /// A one-way ratchet (only advances forward).  When non-zero, ops are
    /// held in the pause-wait loop until `osdmap.epoch >= epoch_barrier`.
    /// Mirrors `Objecter::epoch_barrier` / `set_epoch_barrier()` in C++.
    epoch_barrier: AtomicU32,
}

/// Await an OSD operation result with a timeout, mapping all error layers to `OSDClientError`.
async fn await_op_result(
    rx: tokio::sync::oneshot::Receiver<Result<crate::osdclient::types::OpResult>>,
    timeout: std::time::Duration,
) -> Result<crate::osdclient::types::OpResult> {
    tokio::time::timeout(timeout, rx)
        .await
        .map_err(|_| OSDClientError::Timeout(timeout))?
        .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))?
}

impl OSDClient {
    /// Create a new OSD client
    pub async fn new(
        config: OSDClientConfig,
        fsid: crate::UuidD,
        mon_client: Arc<crate::monclient::MonClient>,
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
            let timeout_callback: crate::osdclient::tracker::TimeoutCallback = Arc::new(
                move |osd_id, tid| {
                    if let Some(client) = weak_for_callback.upgrade() {
                        let sessions = client.sessions.clone();
                        tokio::spawn(async move {
                            let session = {
                                let sessions_guard = sessions.read().await;
                                sessions_guard.get(&osd_id).cloned()
                            };

                            if let Some(session) = session
                                && let Some(pending_op) = session.remove_pending_op(tid).await
                            {
                                // Check incarnation - operation might be from a previous connection
                                // that has since reconnected. In that case, silently drop the timeout.
                                let current_incarnation = session.current_incarnation();
                                if pending_op.sent_incarnation != current_incarnation {
                                    debug!(
                                        "Ignoring timeout for stale operation: OSD {} tid={} (sent in incarnation {} but current is {})",
                                        osd_id,
                                        tid,
                                        pending_op.sent_incarnation,
                                        current_incarnation
                                    );
                                    return;
                                }

                                let _ = pending_op.result_tx.send(Err(OSDClientError::Timeout(
                                    client.tracker.operation_timeout(),
                                )));
                                warn!("Cancelled timed-out operation: OSD {} tid={}", osd_id, tid);
                            }
                        });
                    }
                },
            );

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
                blocklisted: AtomicBool::new(false),
                epoch_barrier: AtomicU32::new(0),
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
    pub async fn dispatch_from_osd(
        &self,
        osd_id: i32,
        msg: crate::msgr2::message::Message,
    ) -> Result<()> {
        let msg_type = msg.msg_type();
        debug!("Dispatching message 0x{:04x} from OSD {}", msg_type, osd_id);

        match msg_type {
            crate::osdclient::messages::CEPH_MSG_OSD_OPREPLY => {
                self.handle_osd_op_reply_from_osd(osd_id, msg).await
            }
            crate::osdclient::messages::CEPH_MSG_OSD_BACKOFF => {
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
    pub async fn get_osdmap(&self) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
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
    ) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        // epoch=0: any map with epoch ≥ 0 (always true) satisfies the condition.
        self.wait_for_epoch(0, timeout).await
    }

    /// Wait for a specific OSDMap epoch
    pub async fn wait_for_epoch(
        &self,
        epoch: u32,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        let mut rx = self.osdmap_rx.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(map) = rx.borrow_and_update().as_ref()
                    && map.epoch.as_u32() >= epoch
                {
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

    /// Wait for any OSDMap with an epoch strictly greater than `current`'s epoch.
    ///
    /// Used to block an operation that is paused (pool-pause / pool-full) until
    /// the cluster state changes.  Bounded by `timeout` to prevent infinite waits.
    async fn wait_for_newer_osdmap(
        &self,
        current: &Arc<crate::osdclient::osdmap::OSDMap>,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        let target_epoch = current.epoch.as_u32() + 1;
        self.wait_for_epoch(target_epoch, timeout).await
    }

    /// Subscribe to OSDMap updates and wait until the monitor's current epoch arrives.
    pub async fn wait_for_latest_osdmap(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        self.mon_client
            .subscribe(crate::monclient::MonService::OsdMap, 0, 0)
            .await
            .map_err(OSDClientError::MonClient)?;

        let (epoch, _) = self
            .mon_client
            .get_version(crate::monclient::MonService::OsdMap)
            .await
            .map_err(OSDClientError::MonClient)?;

        self.wait_for_epoch(epoch as u32, timeout).await
    }

    /// Get or create a session for an OSD
    async fn get_or_create_session(&self, osd_id: i32) -> Result<Arc<OSDSession>> {
        // Get OSD address early (before acquiring any locks)
        // This reduces lock contention by doing I/O outside critical section
        let current_addr = self.get_osd_address(osd_id).await?;

        // Check if we already have a session
        // Clone Arc before releasing lock to avoid holding lock across await
        let existing_session = {
            let sessions = self.sessions.read().await;
            sessions.get(&osd_id).map(Arc::clone)
        };

        if let Some(session) = existing_session
            && session.is_connected().await
        {
            // Validate that session's address matches current OSDMap
            // This prevents wasted reconnection attempts to stale addresses
            // Reference: Ceph Objecter checks OSDMap during reconnect
            let session_addr = session.get_peer_address().await;

            // Compare addresses (ignoring nonce which can change)
            if let Some(session_addr) = session_addr {
                if session_addr.to_socket_addr() == current_addr.to_socket_addr() {
                    return Ok(session);
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

        // Prepare session outside the write lock to minimize critical section
        // Get service auth provider from monitor client
        let auth_provider = self
            .mon_client
            .get_service_auth_provider()
            .await
            .map(|provider| Box::new(provider) as Box<dyn crate::auth::AuthProvider>);

        let mut session = OSDSession::new(
            osd_id,
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
        // Double-check after acquiring write lock to avoid race condition
        // Clone existing session before releasing lock to check it outside critical section
        let existing_to_check = {
            let sessions = self.sessions.read().await;
            sessions.get(&osd_id).map(Arc::clone)
        };

        if let Some(existing) = existing_to_check
            && existing.is_connected().await
        {
            // Re-validate address outside lock
            let session_addr = existing.get_peer_address().await;

            if let Some(session_addr) = session_addr
                && session_addr.to_socket_addr() == current_addr.to_socket_addr()
            {
                // Another task created a session while we were connecting
                // Close our redundant session and return existing one
                session.close().await;
                return Ok(existing);
            }

            // Address changed, remove old session
            info!("Removing old session for OSD {} with stale address", osd_id);
            let mut sessions = self.sessions.write().await;
            sessions.remove(&osd_id);
            drop(sessions);
        }
        // Disconnected session stays in the map; it will be replaced below and
        // its pending ops kicked into the new session after creation.

        let old_session = {
            let mut sessions = self.sessions.write().await;

            // Insert the new session, replacing any disconnected session.
            // insert() returns the old value so we can kick its pending ops.
            sessions.insert(osd_id, Arc::clone(&session))
        };

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
    async fn get_osd_address(&self, osd_id: i32) -> Result<crate::EntityAddr> {
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
            .find(|addr| matches!(addr.addr_type, crate::EntityAddrType::Msgr2))
            .cloned()
            .ok_or_else(|| {
                OSDClientError::Connection(format!("No msgr2 address found for OSD {}", osd_id))
            })
    }

    /// Map an object to OSDs using CRUSH
    async fn object_to_osds(&self, pool: u64, oid: &str) -> Result<(StripedPgId, Vec<i32>)> {
        let osdmap = self.get_osdmap().await?;
        self.object_to_osds_in_map(&osdmap, pool, oid)
    }

    fn object_to_osds_in_map(
        &self,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        pool: u64,
        oid: &str,
    ) -> Result<(StripedPgId, Vec<i32>)> {
        // Find pool info
        let pool_info = osdmap
            .pools
            .get(&pool)
            .ok_or(OSDClientError::PoolNotFound(pool))?;

        // Create object locator
        let locator = ObjectLocator::new(pool);

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

        let (pg, mut osds) = crate::crush::placement::object_to_osds(
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
        // Note: crate::crush::PgId and crate::PgId are now the same type (crate::crush::PgId is re-exported by denc)

        // 1. Check pg_upmap (complete acting set override)
        if let Some(upmap_osds) = osdmap.pg_upmap.get(&pg) {
            debug!("Using pg_upmap override for PG {:?}: {:?}", pg, upmap_osds);
            osds = upmap_osds.clone();
        }

        // 2. Check pg_temp (temporary override during recovery)
        if let Some(temp_osds) = osdmap.pg_temp.get(&pg) {
            debug!("Using pg_temp override for PG {:?}: {:?}", pg, temp_osds);
            osds = temp_osds.clone();
        }

        // 3. Check pg_upmap_items (fine-grained OSD replacements)
        if let Some(upmap_items) = osdmap.pg_upmap_items.get(&pg) {
            debug!("Applying pg_upmap_items to PG {:?}: {:?}", pg, upmap_items);
            for &(from_osd, to_osd) in upmap_items {
                if let Some(pos) = osds.iter().position(|&osd| osd == from_osd) {
                    osds[pos] = to_osd;
                }
            }
        }

        // 4. Check pg_upmap_primaries (primary OSD override)
        if let Some(&primary_osd) = osdmap.pg_upmap_primaries.get(&pg) {
            debug!(
                "Using pg_upmap_primaries override for PG {:?}: primary={}",
                pg, primary_osd
            );
            // Move the primary to front if it's in the set
            if let Some(pos) = osds.iter().position(|&osd| osd == primary_osd) {
                osds.swap(0, pos);
            }
        }

        // Convert to StripedPgId
        let spg = StripedPgId::from_pg(pg.pool, pg.seed);

        debug!("Mapped {}/{} to PG {:?}, OSDs: {:?}", pool, oid, pg, osds);

        Ok((spg, osds))
    }

    fn cached_rescan_osds(
        &self,
        cache: &mut HashMap<(u64, String), Vec<i32>>,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        pool_id: u64,
        object_id: &str,
    ) -> Result<Vec<i32>> {
        let key = (pool_id, object_id.to_owned());
        if let Some(osds) = cache.get(&key) {
            return Ok(osds.clone());
        }

        let (_, osds) = self.object_to_osds_in_map(osdmap, pool_id, object_id)?;
        cache.insert(key, osds.clone());
        Ok(osds)
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
    /// use crate::osdclient::OpBuilder;
    ///
    /// let op = OpBuilder::new()
    ///     .read(0, 4096)
    ///     .balance_reads()
    ///     .build();
    ///
    /// let result = client.execute_built_op(pool_id, "my-object", op).await?;
    /// ```
    /// Execute a pre-built operation with a plain pool/oid address.
    pub async fn execute_built_op(
        &self,
        pool: u64,
        oid: &str,
        built_op: crate::osdclient::operation::BuiltOp,
    ) -> Result<crate::osdclient::types::OpResult> {
        self.execute_built_op_with_id(ObjectId::new(pool, oid), built_op)
            .await
    }

    /// Execute a pre-built operation with a full [`ObjectId`] (supports namespace and locator key).
    pub async fn execute_built_op_with_id(
        &self,
        object: ObjectId,
        built_op: crate::osdclient::operation::BuiltOp,
    ) -> Result<crate::osdclient::types::OpResult> {
        let timeout = built_op.timeout;
        let priority = if built_op.priority < 0 {
            crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT
        } else {
            built_op.priority
        };
        let extra_flags = built_op.flags;
        self.execute_op(object, built_op.into_ops(), timeout, priority, extra_flags)
            .await
    }

    /// Execute an OSD operation with automatic redirect handling
    ///
    /// This is the common pattern for all OSD operations:
    /// 1. Acquire throttle permit
    /// 2. Get OSDMap epoch
    /// 3. Build MOSDOp message
    /// 4. Redirect retry loop with automatic session management
    /// 5. Return OpResult for caller to process
    ///
    /// # Arguments
    /// * `object` - Full object address (pool, oid, namespace, locator key)
    /// * `ops` - Operations to execute
    ///
    /// # Returns
    /// Returns the OpResult after handling all redirects
    async fn execute_op(
        &self,
        mut object: ObjectId,
        ops: Vec<OSDOp>,
        timeout: Option<std::time::Duration>,
        priority: i32,
        extra_flags: crate::osdclient::types::OsdOpFlags,
    ) -> Result<crate::osdclient::types::OpResult> {
        // Fail immediately if the client has been fenced by the cluster.
        if self.blocklisted.load(Ordering::Relaxed) {
            return Err(OSDClientError::Blocklisted);
        }

        object.calculate_hash();

        // Calculate operation budget and acquire throttle permit
        let budget = calc_op_budget(&ops);
        let _throttle_permit = self.throttle.acquire(budget).await?;
        debug!(
            "Acquired throttle permit: budget={} bytes, current_ops={}, current_bytes={}",
            budget,
            self.throttle.current_ops(),
            self.throttle.current_bytes()
        );

        // Compute an absolute deadline once so that pause-wait loops don't
        // inadvertently extend the total operation time beyond the timeout.
        let effective_timeout = timeout.unwrap_or_else(|| self.tracker.operation_timeout());
        let deadline = std::time::Instant::now() + effective_timeout;

        // Get OSDMap epoch from OSDClient's own osdmap (not from MonClient)
        let mut osdmap = self.get_osdmap().await?;

        // Build initial message
        // OR in caller-supplied extra flags (e.g. BALANCE_READS, FULL_TRY, FULL_FORCE).
        let flags = MOSDOp::calculate_flags(&ops) | extra_flags.bits();
        let mut msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch.as_u32(),
            flags,
            object.clone(),
            StripedPgId::from_pg(object.pool, 0), // Will be set in loop
            ops.clone(),
            crate::osdclient::types::RequestId::new(
                &self.entity_name,
                0,
                self.config.client_inc as i32,
            ),
            self.global_id,
        );

        // Initialise snap context from pool (Objecter::_calc_target snapc setup).
        // For pools without snaps this is a no-op (snap_seq=0, snaps=[]).
        (msg.snap_seq, msg.snaps) = osdmap.pool_snap_context(msg.object.pool);

        // Pre-compute flag-derived booleans that are constant across iterations.
        use crate::osdclient::types::OsdOpFlags;
        let is_write = flags & OsdOpFlags::WRITE.bits() != 0;
        let is_read = !is_write;
        let respects_full = flags & (OsdOpFlags::FULL_TRY | OsdOpFlags::FULL_FORCE).bits() == 0;

        // Set mtime for write ops — mirrors librados setting real_clock::now() when
        // the caller provides no explicit mtime. Reads carry UTime::zero().
        if is_write {
            msg.mtime = crate::UTime::now();
        }

        // Redirect/pause retry loop
        loop {
            // Refresh the epoch in the message to reflect the current OSDMap.
            // C++ _prepare_osd_op reads osdmap->get_epoch() at send time; after a
            // pause-wait that loads a newer map we must do the same.
            msg.osdmap_epoch = osdmap.epoch.as_u32();

            // Check pool EIO flag — hard fail, mirrors RECALC_OP_TARGET_POOL_EIO.
            if osdmap.is_pool_eio(msg.object.pool) {
                return Err(OSDClientError::OSDError {
                    code: -libc::EIO,
                    message: format!("pool {} has EIO flag set", msg.object.pool),
                });
            }

            // Check epoch barrier — block until osdmap.epoch >= barrier.
            // Mirrors Objecter::_calc_target RECALC_OP_TARGET_BARRIER_NEWER path.
            let barrier = self.epoch_barrier.load(Ordering::Relaxed);
            let behind_barrier = barrier != 0 && osdmap.epoch.as_u32() < barrier;

            // Check pool-pause and pool-full state before sending.
            // Mirrors Objecter::_calc_target() pauserd/pausewr checks.
            let pauserd = osdmap.is_pauserd();
            let pausewr = osdmap.is_pausewr();
            let pool_full = respects_full && osdmap.is_pool_full(msg.object.pool);
            let paused =
                behind_barrier || (is_read && pauserd) || (is_write && (pausewr || pool_full));
            if paused {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err(OSDClientError::Timeout(effective_timeout));
                }
                info!(
                    "Op on pool {} is paused (barrier={}, behind_barrier={}, pauserd={}, \
                     pausewr={}, pool_full={}); waiting for OSDMap update",
                    msg.object.pool, barrier, behind_barrier, pauserd, pausewr, pool_full,
                );
                osdmap = self.wait_for_newer_osdmap(&osdmap, remaining).await?;
                // Prune snap context: remove any snap IDs that were purged in
                // the new OSDMap — mirrors Objecter::_prune_snapc().
                osdmap.prune_snap_context(msg.object.pool, &mut msg.snaps);
                continue;
            }

            // Map to OSDs based on current object
            let (spg, osds) = self
                .object_to_osds(msg.object.pool, &msg.object.oid)
                .await?;
            let primary_osd = osds[0];
            msg.pgid = spg;

            // Get session
            let session = self.get_or_create_session(primary_osd).await?;

            // Build request ID with fresh TID
            let tid = session.next_tid();
            msg.reqid = crate::osdclient::types::RequestId::new(
                &self.entity_name,
                tid,
                self.config.client_inc as i32,
            );

            // Submit operation (priority is set in message header)
            let result_rx = session.submit_op(msg.clone(), priority).await?;

            // Wait for result using the remaining time towards the deadline.
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(OSDClientError::Timeout(effective_timeout));
            }
            let result = await_op_result(result_rx, remaining).await?;

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

    /// Check an OpResult for errors.
    ///
    /// Validates both the overall result code and the per-op return code.
    pub(crate) fn check_op_result(
        result: &crate::osdclient::types::OpResult,
        op_name: &str,
    ) -> Result<()> {
        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: format!("{} failed", op_name),
            });
        }
        if let Some(op) = result.ops.first()
            && op.return_code != 0
        {
            return Err(OSDClientError::OSDError {
                code: op.return_code,
                message: format!("{} failed", op_name),
            });
        }
        Ok(())
    }

    /// Advance the epoch barrier to `epoch` (no-op if `epoch` ≤ current barrier).
    ///
    /// Ops will be held in the pause-wait loop until the current OSDMap epoch
    /// is at least `epoch`.  This is a one-way ratchet — calling it with a
    /// lower epoch than the current barrier has no effect.
    ///
    /// Mirrors `Objecter::set_epoch_barrier()` in C++.
    pub fn set_epoch_barrier(&self, epoch: u32) {
        self.epoch_barrier.fetch_max(epoch, Ordering::Relaxed);
    }

    /// Read data from an object
    pub async fn read(&self, pool: u64, oid: &str, offset: u64, len: u64) -> Result<ReadResult> {
        debug!(
            "read pool={} oid={} offset={} len={}",
            pool, oid, offset, len
        );

        let ops = vec![OSDOp::read(offset, len)];
        let result = self
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
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
    ) -> Result<crate::osdclient::types::SparseReadResult> {
        self.sparse_read_with_id(ObjectId::new(pool, oid), offset, len)
            .await
    }

    /// Sparse read with a full [`ObjectId`] (supports namespace and locator key).
    pub async fn sparse_read_with_id(
        &self,
        object: ObjectId,
        offset: u64,
        len: u64,
    ) -> Result<crate::osdclient::types::SparseReadResult> {
        debug!(
            "sparse_read pool={} oid={} offset={} len={}",
            object.pool, object.oid, offset, len
        );

        let ops = vec![OSDOp::sparse_read(offset, len)];
        let result = self
            .execute_op(
                object,
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "sparse_read")?;

        let sparse = crate::osdclient::types::SparseReadResult::from_op_result(&result)?;

        debug!(
            "Sparse read decoded: {} extents, {} data bytes",
            sparse.extents.len(),
            sparse.data.len()
        );

        Ok(sparse)
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
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
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
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
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
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "Stat")?;

        // Parse stat data from outdata using OsdStatData's Denc implementation
        let outdata = result.ops.first().map(|op| &op.outdata[..]).unwrap_or(&[]);
        let stat_data = crate::osdclient::denc_types::OsdStatData::decode(&mut &outdata[..], 0)?;

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
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "Delete")?;

        Ok(())
    }

    /// Compute the PG id that an object hash belongs to.
    ///
    /// This mirrors Ceph's `ceph_stable_mod(hash, pg_num, pg_num_mask)`:
    /// - If `(hash & mask) < pg_num`  →  `hash & mask`
    /// - Otherwise                    →  `hash & (mask >> 1)`
    ///
    /// `pg_num_mask = (1 << ceil_log2(pg_num)) - 1`; for power-of-2 pg_num it equals
    /// `pg_num - 1`.
    fn hash_to_pg(hash: u32, pg_num: u32) -> u32 {
        if pg_num == 0 {
            return 0;
        }
        // pg_num_mask = (1 << cbits(pg_num - 1)) - 1
        let mask: u32 = if pg_num <= 1 {
            0
        } else {
            (1u32 << (32 - (pg_num - 1).leading_zeros())) - 1
        };
        if (hash & mask) < pg_num {
            hash & mask
        } else {
            hash & (mask >> 1)
        }
    }

    /// Parse a list cursor string into an `HObject` starting position.
    ///
    /// Cursor format: decimal representation of the hobject raw hash.
    /// `None` means "start from the very beginning" (hash=0).
    fn parse_list_cursor(pool: u64, cursor: Option<String>) -> crate::HObject {
        match cursor {
            None => crate::HObject::empty_cursor(pool),
            Some(s) => {
                let hash: u32 = s.parse().unwrap_or(0);
                crate::HObject::new(pool, String::new(), hash)
            }
        }
    }

    /// Query objects from the PG that contains `hobject_cursor`.
    ///
    /// The target PG is derived from `hobject_cursor.hash` using `ceph_stable_mod`, matching
    /// exactly how Ceph's Objecter routes PGNLS requests.  Using the cursor hash as the
    /// object hash in the MOSDOp satisfies the OSD's `pgid.contains(head)` assertion.
    ///
    /// Returns the decoded response and result code from the OSD.
    async fn query_pg_objects(
        &self,
        pool: u64,
        hobject_cursor: &crate::HObject,
        max_entries: u64,
        pool_info: &crate::osdclient::PgPool,
        osdmap: &Arc<crate::osdclient::osdmap::OSDMap>,
    ) -> Result<(crate::osdclient::PgNlsResponse, i32)> {
        // Derive the target PG from the cursor hash — mirrors Objecter::pg_read(current_pg,…)
        let current_pg = Self::hash_to_pg(hobject_cursor.hash, pool_info.pg_num);

        // Create StripedPgId for the current PG
        let spg = StripedPgId::from_pg(pool, current_pg);
        let hashpspool =
            PoolFlags::from_bits_truncate(pool_info.flags).contains(PoolFlags::HASHPSPOOL);
        let crush_map = osdmap
            .crush
            .as_ref()
            .ok_or_else(|| OSDClientError::Crush("No CRUSH map in OSDMap".into()))?;

        // Look up which OSDs handle this PG using CRUSH
        let pg = crate::crush::placement::PgId {
            pool,
            seed: current_pg,
        };

        let osds = crate::crush::placement::pg_to_osds(
            crush_map,
            pg,
            pool_info.crush_rule as u32,
            &osdmap.osd_weight,
            pool_info.size as usize,
            hashpspool,
        )
        .map_err(|e| OSDClientError::Crush(format!("PG->OSD mapping failed: {}", e)))?;

        if osds.is_empty() {
            return Err(OSDClientError::NoOSDs);
        }

        let primary_osd = osds[0];

        // Get session
        let session = self.get_or_create_session(primary_osd).await?;

        // Create object ID (empty for PGNLS).
        // The hash must match the cursor's hash so the OSD's pgid.contains(head)
        // check passes — hash=0 only belongs to PG 0.
        let mut object = ObjectId::new(pool, "");
        object.hash = hobject_cursor.hash;

        // Create pgls operation
        let ops = vec![OSDOp::pgls(
            max_entries,
            hobject_cursor.clone(),
            osdmap.epoch.as_u32(),
        )?];

        // Acquire throttle permit
        let _throttle_permit = self.throttle.acquire(calc_op_budget(&ops)).await?;

        // Build request ID
        let tid = session.next_tid();
        let reqid = crate::osdclient::types::RequestId::new(
            &self.entity_name,
            tid,
            self.config.client_inc as i32,
        );

        // Build message
        let flags = MOSDOp::calculate_flags(&ops);
        let msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch.as_u32(),
            flags,
            object,
            spg,
            ops,
            reqid,
            self.global_id,
        );

        // Submit operation (use default priority for internal PGLS operations)
        let result_rx = session
            .submit_op(msg, crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT)
            .await?;

        // Wait for result with timeout
        let result = await_op_result(result_rx, self.tracker.operation_timeout()).await?;

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
        let response = crate::osdclient::PgNlsResponse::decode(&mut outdata.as_ref(), 0)?;

        debug!(
            "PG {} returned {} entries, handle.hash={:#x}, result={}",
            current_pg,
            response.entries.len(),
            response.handle.hash,
            result.result
        );

        Ok((response, result.result))
    }

    /// Build a `ListResult` from collected entries and the next cursor handle.
    ///
    /// `next_handle` is the `response.handle` returned by the last PGNLS call.
    /// When `next_handle.max` is true the pool is exhausted; otherwise encode
    /// the handle's hash as the cursor string for the next `list` call.
    fn build_list_result(
        entries: Vec<ListObjectEntry>,
        next_handle: &crate::HObject,
    ) -> ListResult {
        let cursor = if next_handle.max {
            None
        } else {
            Some(next_handle.hash.to_string())
        };
        ListResult { entries, cursor }
    }

    /// List objects in a pool
    ///
    /// Lists objects in the specified pool using PGNLS, following the cursor returned
    /// by each OSD reply (bitwise-sorted order).  The cursor is the raw hobject hash
    /// encoded as a decimal string; `None` starts from the beginning of the pool.
    ///
    /// # Arguments
    /// * `pool`        - Pool ID to list objects from
    /// * `cursor`      - Optional pagination cursor (decimal hash string from a prior call)
    /// * `max_entries` - Maximum number of entries to return per call
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

        // Parse cursor → starting hobject position
        let mut hobject_cursor = Self::parse_list_cursor(pool, cursor);

        // Get OSDMap to look up pool info
        let osdmap = self.get_osdmap().await?;
        let pool_info = osdmap
            .pools
            .get(&pool)
            .ok_or(OSDClientError::PoolNotFound(pool))?;

        let mut all_entries: Vec<ListObjectEntry> = Vec::new();

        loop {
            // Pool end reached (cursor returned from a previous call was handle.max)
            if hobject_cursor.max {
                return Ok(ListResult {
                    entries: all_entries,
                    cursor: None,
                });
            }

            let current_pg = Self::hash_to_pg(hobject_cursor.hash, pool_info.pg_num);
            debug!(
                "Querying PG {} (hash={:#x}), collected {} entries so far",
                current_pg,
                hobject_cursor.hash,
                all_entries.len()
            );

            let remaining = max_entries.saturating_sub(all_entries.len() as u64);
            let (response, _result_code) = match self
                .query_pg_objects(pool, &hobject_cursor, remaining, pool_info, &osdmap)
                .await
            {
                Ok(result) => result,
                Err(OSDClientError::NoOSDs) => {
                    // No OSDs for this PG — advance to the next PG in sorted order
                    // by synthesising the end-of-PG handle that the OSD would have returned.
                    hobject_cursor = Self::pg_hobj_end(pool, current_pg, pool_info.pg_num);
                    continue;
                }
                Err(e) => return Err(e),
            };

            // Collect entries
            for entry in response.entries {
                all_entries.push(ListObjectEntry::new(
                    &entry.nspace,
                    &entry.oid,
                    &entry.locator,
                ));
            }

            // The OSD always returns the correct next cursor — use it directly.
            // This is the "SORTBITWISE" path used by modern Ceph (Quincy+):
            //   list_context->pos = response.handle
            let next_handle = response.handle;

            // Return early once we have enough entries
            if all_entries.len() >= max_entries as usize {
                return Ok(Self::build_list_result(all_entries, &next_handle));
            }

            // Advance cursor for next iteration
            hobject_cursor = next_handle;
        }
    }

    /// Compute the end-of-PG hobject handle for a given PG, matching `pg_t::get_hobj_end`.
    ///
    /// This is used to skip over a PG that has no OSDs so we can advance to the
    /// next PG in bitwise-sorted order without issuing an OSD request.
    fn pg_hobj_end(pool: u64, pg_seed: u32, pg_num: u32) -> crate::HObject {
        let bits = if pg_num <= 1 {
            0u32
        } else {
            32 - (pg_num - 1).leading_zeros()
        };
        let rev_start = pg_seed.reverse_bits();
        let rev_end: u64 = (rev_start as u64) | (0xffff_ffffu64 >> bits);
        let rev_end = rev_end + 1;
        if rev_end >= 0x1_0000_0000 {
            // This PG extends to the very end of the hash space → pool end
            let mut h = crate::HObject::empty_cursor(pool);
            h.max = true;
            h
        } else {
            crate::HObject::new(pool, String::new(), (rev_end as u32).reverse_bits())
        }
    }

    /// List all pools in the cluster
    ///
    /// Returns a list of all pools with their IDs and names.
    pub async fn list_pools(&self) -> Result<Vec<crate::osdclient::types::PoolInfo>> {
        debug!("Listing pools");

        let osdmap = self.get_osdmap().await?;

        // Extract pool information from OSDMap
        let mut pools = Vec::with_capacity(osdmap.pools.len());
        for pool_id in osdmap.pools.keys() {
            if let Some(pool_name) = osdmap.pool_name.get(pool_id) {
                pools.push(crate::osdclient::types::PoolInfo {
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
    async fn handle_pool_op_result(&self, result: &crate::monclient::PoolOpResult) -> Result<()> {
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
                .subscribe(
                    crate::monclient::MonService::OsdMap,
                    current_epoch as u64,
                    0,
                )
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
        let msg = crate::monclient::MPoolOp::create_pool(
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
            .map_err(OSDClientError::MonClient)?;

        self.handle_pool_op_result(&result).await?;
        debug!("Pool created successfully: {}", pool_name);
        Ok(())
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
        let msg = crate::monclient::MPoolOp::delete_pool(self.fsid.bytes, pool_id, version);

        let result = self
            .mon_client
            .send_poolop(msg)
            .await
            .map_err(OSDClientError::MonClient)?;

        self.handle_pool_op_result(&result).await?;
        debug!("Pool deleted successfully: {}", pool_name);
        Ok(())
    }

    /// Scan pending ops after OSDMap update, resend if target changed
    ///
    /// This implements Ceph's `_scan_requests` pattern from Objecter.
    async fn scan_requests_on_map_change(&self, new_epoch: u32) -> Result<()> {
        let osdmap = self.get_osdmap().await?;

        // Collect session snapshot
        let session_snapshot = self.collect_session_snapshot().await;

        let mut need_resend = Vec::new();
        let mut sessions_to_close: Vec<i32> = Vec::with_capacity(session_snapshot.len());

        // Process each session
        for (osd_id, session) in session_snapshot {
            let should_close = self
                .check_session_health(osd_id, &session, &osdmap, new_epoch)
                .await;

            if should_close {
                sessions_to_close.push(osd_id);
                self.drain_session_ops(&session, &osdmap, new_epoch, &mut need_resend)
                    .await;
                continue;
            }

            // Check per-operation targets
            self.scan_session_operations(osd_id, &session, &osdmap, new_epoch, &mut need_resend)
                .await?;
        }

        // Close stale sessions
        self.close_stale_sessions(sessions_to_close).await;

        // Resend operations to new targets
        self.resend_migrated_operations(need_resend, new_epoch)
            .await?;

        Ok(())
    }

    /// Collect snapshot of all sessions
    async fn collect_session_snapshot(&self) -> Vec<(i32, Arc<OSDSession>)> {
        let sessions = self.sessions.read().await;
        sessions
            .iter()
            .map(|(id, sess)| (*id, Arc::clone(sess)))
            .collect()
    }

    /// Check if session should be closed due to OSD down or address change
    async fn check_session_health(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        osdmap: &Arc<crate::osdclient::osdmap::OSDMap>,
        new_epoch: u32,
    ) -> bool {
        if osdmap.is_down(osd_id) {
            info!(
                "OSD {} is DOWN in epoch {}, closing session and migrating pending ops",
                osd_id, new_epoch
            );
            return true;
        }

        if session.is_connected().await {
            return self
                .session_address_stale(session, osd_id, osdmap, new_epoch)
                .await;
        }

        false
    }

    /// Scan operations in a session and collect those needing resend
    async fn scan_session_operations(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        osdmap: &Arc<crate::osdclient::osdmap::OSDMap>,
        new_epoch: u32,
        need_resend: &mut Vec<(i32, crate::osdclient::session::PendingOp)>,
    ) -> Result<()> {
        let metadata = session.get_pending_ops_metadata().await;
        let mut placement_cache = HashMap::new();

        for (tid, pool_id, object_id, op_osdmap_epoch) in metadata {
            // Check if pool deleted
            if !osdmap.pools.contains_key(&pool_id) {
                if let Some(pending_op) = session.remove_pending_op(tid).await {
                    let _ = pending_op
                        .result_tx
                        .send(Err(OSDClientError::PoolNotFound(pool_id)));
                }
                continue;
            }

            // Check if the pool's admin-forced resend epoch falls after this op
            // was sent.  Mirrors Objecter::_calc_target step 4: if
            // last_force_op_resend is in (op_epoch, new_epoch] the op must be
            // resubmitted even if its primary OSD has not changed.
            let force_resend = osdmap.pools.get(&pool_id).is_some_and(|p| {
                let lf = p.canonical_last_force_op_resend().as_u32();
                lf > op_osdmap_epoch && lf <= new_epoch
            });

            // Check if target changed
            let new_osds =
                self.cached_rescan_osds(&mut placement_cache, osdmap, pool_id, &object_id)?;
            let new_primary = new_osds.first().copied().unwrap_or(-1);

            if (new_primary != osd_id || force_resend)
                && let Some(mut op) = session.remove_pending_op(tid).await
            {
                op.state = crate::osdclient::types::OpState::NeedsResend;
                op.target.update(new_epoch, new_primary, new_osds.clone());
                op.state = crate::osdclient::types::OpState::Queued;
                need_resend.push((new_primary, op));
            }
        }

        Ok(())
    }

    /// Close sessions that are stale
    async fn close_stale_sessions(&self, sessions_to_close: Vec<i32>) {
        if sessions_to_close.is_empty() {
            return;
        }

        for osd_id in &sessions_to_close {
            let session = {
                let mut sessions = self.sessions.write().await;
                sessions.remove(osd_id)
            };

            if let Some(session) = session {
                info!("Removing stale session for OSD {}", osd_id);
                session.close().await;
            }
        }
    }

    /// Resend migrated operations to their new target OSDs
    async fn resend_migrated_operations(
        &self,
        need_resend: Vec<(i32, crate::osdclient::session::PendingOp)>,
        new_epoch: u32,
    ) -> Result<()> {
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
        osdmap: &crate::osdclient::osdmap::OSDMap,
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
            matches!(a.addr_type, crate::EntityAddrType::Msgr2)
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
        osdmap: &crate::osdclient::osdmap::OSDMap,
        new_epoch: u32,
        need_resend: &mut Vec<(i32, crate::osdclient::session::PendingOp)>,
    ) {
        let metadata = session.get_pending_ops_metadata().await;
        let mut placement_cache = HashMap::new();
        for (tid, pool_id, object_id, _osdmap_epoch) in metadata {
            if !osdmap.pools.contains_key(&pool_id) {
                if let Some(pending_op) = session.remove_pending_op(tid).await {
                    let _ = pending_op
                        .result_tx
                        .send(Err(OSDClientError::PoolNotFound(pool_id)));
                }
                continue;
            }
            if let Ok(new_osds) =
                self.cached_rescan_osds(&mut placement_cache, osdmap, pool_id, &object_id)
            {
                let new_primary = new_osds.first().copied().unwrap_or(-1);
                if let Some(mut op) = session.remove_pending_op(tid).await {
                    op.state = crate::osdclient::types::OpState::NeedsResend;
                    op.target.update(new_epoch, new_primary, new_osds.clone());
                    op.state = crate::osdclient::types::OpState::Queued;
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

            pending_op.state = crate::osdclient::types::OpState::Queued;

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
        // Clone sessions before releasing lock to avoid holding lock across await
        let sessions_to_close = {
            let sessions = self.sessions.read().await;
            sessions.values().map(Arc::clone).collect::<Vec<_>>()
        };

        for session in sessions_to_close {
            session.close().await;
        }

        info!("OSDClient shutdown complete");
    }

    /// Decode and validate MOSDMap message
    ///
    /// Returns the decoded MOSDMap and current epoch if validation passes
    fn decode_and_validate_osdmap(
        &self,
        msg: &crate::msgr2::message::Message,
    ) -> Result<(crate::monclient::messages::MOSDMap, crate::Epoch)> {
        use crate::monclient::messages::MOSDMap;
        use crate::msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};

        info!("Handling OSDMap message ({} bytes)", msg.front.len());

        // Decode MOSDMap
        let header = CephMsgHeader::new(MOSDMap::msg_type(), MOSDMap::msg_version(0));
        let mosdmap = MOSDMap::decode_payload(&header, &msg.front, &[], &[])?;
        info!(
            "Received MOSDMap: epochs [{}..{}], {} full maps, {} incremental maps",
            mosdmap.get_first(),
            mosdmap.get_last(),
            mosdmap.maps.len(),
            mosdmap.incremental_maps.len()
        );

        // Validate FSID
        if mosdmap.fsid != self.fsid.bytes {
            warn!(
                "Ignoring OSDMap with wrong fsid (expected {:?}, got {:?})",
                self.fsid.bytes, mosdmap.fsid
            );
            return Err(OSDClientError::Internal("FSID mismatch".into()));
        }

        // Check if we've already processed these epochs
        let current_epoch = self
            .osdmap_rx
            .borrow()
            .as_ref()
            .map(|m| m.epoch)
            .unwrap_or(crate::Epoch::new(0));

        if mosdmap.get_last() <= current_epoch.as_u32() {
            info!(
                "Ignoring OSDMap epochs [{}..{}] <= current epoch {}",
                mosdmap.get_first(),
                mosdmap.get_last(),
                current_epoch
            );
            return Err(OSDClientError::Internal("Stale epoch".into()));
        }

        debug!(
            "Processing OSDMap epochs [{}..{}] > current epoch {}",
            mosdmap.get_first(),
            mosdmap.get_last(),
            current_epoch
        );

        Ok((mosdmap, current_epoch))
    }

    /// Process OSDMap updates (incremental and full maps)
    ///
    /// Returns the updated map if any updates were successfully applied
    fn process_osdmap_updates(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        current_epoch: crate::Epoch,
    ) -> Option<Arc<crate::osdclient::osdmap::OSDMap>> {
        let current_map = self.osdmap_rx.borrow().clone();

        if current_epoch.as_u32() > 0 {
            // We have a current map, apply updates sequentially
            self.apply_sequential_updates(mosdmap, current_epoch, current_map)
        } else {
            // No current map, use latest full map
            self.load_initial_map(mosdmap)
        }
    }

    /// Apply sequential updates to existing map
    fn apply_sequential_updates(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        current_epoch: crate::Epoch,
        current_map: Option<Arc<crate::osdclient::osdmap::OSDMap>>,
    ) -> Option<Arc<crate::osdclient::osdmap::OSDMap>> {
        let mut working_map = current_map;
        let mut updated = false;

        for e in (current_epoch.as_u32() + 1)..=mosdmap.get_last() {
            let current_map_epoch = working_map
                .as_ref()
                .map(|m| m.epoch)
                .unwrap_or(crate::Epoch::new(0));

            if current_map_epoch == crate::Epoch::new(e - 1)
                && mosdmap.incremental_maps.contains_key(&e)
            {
                // Apply incremental
                if let Some(new_map) = self.apply_incremental_map(mosdmap, e, &working_map) {
                    working_map = Some(new_map);
                    updated = true;
                }
            } else if mosdmap.maps.contains_key(&e) {
                // Use full map
                if let Some(new_map) = self.apply_full_map(mosdmap, e) {
                    working_map = Some(new_map);
                    updated = true;
                }
            } else {
                warn!("Missing epoch {} (incremental and full)", e);
            }
        }

        if updated { working_map } else { None }
    }

    /// Apply incremental map update
    fn apply_incremental_map(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        epoch: u32,
        working_map: &Option<Arc<crate::osdclient::osdmap::OSDMap>>,
    ) -> Option<Arc<crate::osdclient::osdmap::OSDMap>> {
        debug!("Applying incremental OSDMap for epoch {}", epoch);
        let inc_bl = mosdmap.incremental_maps.get(&epoch)?;

        match crate::osdclient::osdmap::OSDMapIncremental::decode_versioned(&mut inc_bl.as_ref(), 0)
        {
            Ok(inc_map) => {
                debug!(
                    "Decoded incremental: epoch={}, {} new pools, {} old pools",
                    inc_map.epoch,
                    inc_map.new_pools.len(),
                    inc_map.old_pools.len()
                );

                if let Some(current_map) = working_map {
                    let mut updated_map = (**current_map).clone();
                    if let Err(err) = inc_map.apply_to(&mut updated_map) {
                        warn!("Failed to apply incremental epoch {}: {}", epoch, err);
                        None
                    } else {
                        Some(Arc::new(updated_map))
                    }
                } else {
                    None
                }
            }
            Err(err) => {
                warn!("Failed to decode incremental epoch {}: {}", epoch, err);
                None
            }
        }
    }

    /// Apply full map update
    fn apply_full_map(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        epoch: u32,
    ) -> Option<Arc<crate::osdclient::osdmap::OSDMap>> {
        debug!("Using full OSDMap for epoch {}", epoch);
        let full_bl = mosdmap.maps.get(&epoch)?;

        match crate::osdclient::osdmap::OSDMap::decode_versioned(&mut full_bl.as_ref(), 0) {
            Ok(full_map) => {
                debug!("Decoded full OSDMap: epoch={}", full_map.epoch);
                Some(Arc::new(full_map))
            }
            Err(err) => {
                warn!("Failed to decode full map epoch {}: {}", epoch, err);
                None
            }
        }
    }

    /// Load initial map when no current map exists
    fn load_initial_map(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
    ) -> Option<Arc<crate::osdclient::osdmap::OSDMap>> {
        let (&latest_epoch, full_bl) = mosdmap.maps.iter().max_by_key(|(e, _)| **e)?;

        debug!("Using latest full OSDMap (epoch {})", latest_epoch);
        match crate::osdclient::osdmap::OSDMap::decode_versioned(&mut full_bl.as_ref(), 0) {
            Ok(full_map) => {
                info!("Initial OSDMap loaded: epoch={}", full_map.epoch);
                Some(Arc::new(full_map))
            }
            Err(err) => {
                warn!("Failed to decode initial full map: {}", err);
                None
            }
        }
    }

    /// Update OSDMap state and notify subscribers
    async fn update_osdmap_state(
        &self,
        new_map: Arc<crate::osdclient::osdmap::OSDMap>,
    ) -> Result<()> {
        let final_epoch = new_map.epoch;

        // Check blocklist before publishing the new map so that
        // `scan_requests_on_map_change` below can skip normal resend logic when
        // we are fenced (all ops will be failed unconditionally).
        if !self.blocklisted.load(Ordering::Relaxed)
            && let Some(client_addr) = self.mon_client.get_client_addr().await
            && new_map.is_blocklisted(&client_addr)
        {
            error!(
                "OSDClient is blocklisted at epoch {} (addr={}), failing all pending ops",
                final_epoch, client_addr
            );
            self.blocklisted.store(true, Ordering::Relaxed);
            self.fail_all_pending_ops_blocklisted().await;
        }

        self.osdmap_tx.send(Some(new_map)).ok();

        info!(
            "OSDMap updated to epoch {}, rescanning pending operations",
            final_epoch
        );

        // Notify MonClient that we received this osdmap epoch
        if let Err(e) = self
            .mon_client
            .notify_map_received(
                crate::monclient::MonService::OsdMap,
                u64::from(u32::from(final_epoch)),
            )
            .await
        {
            warn!(
                "Failed to notify MonClient of osdmap epoch {}: {}",
                final_epoch, e
            );
        }

        // Skip the normal rescan if we just got blocklisted — all ops were
        // already failed above.
        if !self.blocklisted.load(Ordering::Relaxed)
            && let Err(e) = self.scan_requests_on_map_change(final_epoch.as_u32()).await
        {
            warn!("Failed to rescan requests after OSDMap update: {}", e);
        }

        Ok(())
    }

    /// Fail all pending ops across every session with `Blocklisted`.
    ///
    /// Called once when the client detects its address in the OSDMap blocklist
    /// so callers receive an immediate error rather than a timeout.
    async fn fail_all_pending_ops_blocklisted(&self) {
        let session_snapshot = self.collect_session_snapshot().await;
        for (_osd_id, session) in session_snapshot {
            let tids: Vec<u64> = session
                .get_pending_ops_metadata()
                .await
                .into_iter()
                .map(|(tid, _, _, _)| tid)
                .collect();
            for tid in tids {
                if let Some(op) = session.remove_pending_op(tid).await {
                    let _ = op.result_tx.send(Err(OSDClientError::Blocklisted));
                }
            }
        }
    }

    /// Handle OSDMap message (moved from MonClient)
    async fn handle_osdmap(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        // Decode and validate
        let (mosdmap, current_epoch) = match self.decode_and_validate_osdmap(&msg) {
            Ok(result) => result,
            Err(_) => return Ok(()), // Already logged, skip processing
        };

        // Process map updates
        let new_map = self.process_osdmap_updates(&mosdmap, current_epoch);

        // Update state if we got a new map
        if let Some(map) = new_map {
            self.update_osdmap_state(map).await?;
        }

        Ok(())
    }

    /// Handle OSD operation reply from a specific OSD
    ///
    /// Called with explicit OSD context (Linux kernel pattern)
    async fn handle_osd_op_reply_from_osd(
        &self,
        osd_id: i32,
        msg: crate::msgr2::message::Message,
    ) -> Result<()> {
        use crate::osdclient::messages::MOSDOpReply;

        let tid = msg.tid();

        // Decode the reply
        use crate::msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
        let header = CephMsgHeader::new(MOSDOpReply::msg_type(), MOSDOpReply::msg_version(0));
        let reply = MOSDOpReply::decode_payload(&header, &msg.front, &[], &msg.data)?;

        debug!("OSD {} sent OSDOpReply for tid={}", osd_id, tid);

        // Get the session for this OSD
        // Clone Arc before releasing lock to avoid holding lock across await
        let session = {
            let sessions = self.sessions.read().await;
            sessions.get(&osd_id).cloned().ok_or_else(|| {
                OSDClientError::Connection(format!("No session found for OSD {}", osd_id))
            })?
        };

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
        msg: crate::msgr2::message::Message,
    ) -> Result<()> {
        use crate::osdclient::messages::{CEPH_OSD_BACKOFF_OP_BLOCK, CEPH_OSD_BACKOFF_OP_UNBLOCK};

        // Decode the backoff message
        let backoff = Self::decode_backoff_message(&msg)?;

        debug!(
            "OSD {} sent backoff op={} for pg={}:{}.{}",
            osd_id, backoff.op, backoff.pgid.pool, backoff.pgid.seed, backoff.pgid.shard
        );

        // Get the session for this OSD
        let session = self.get_session_for_osd(osd_id).await?;

        match backoff.op {
            CEPH_OSD_BACKOFF_OP_BLOCK => self.handle_backoff_block(osd_id, &session, backoff).await,
            CEPH_OSD_BACKOFF_OP_UNBLOCK => {
                self.handle_backoff_unblock(osd_id, &session, backoff).await
            }
            _ => {
                warn!(
                    "Received unknown backoff operation {} from OSD {}",
                    backoff.op, osd_id
                );
                Ok(())
            }
        }
    }

    /// Decode backoff message from raw message
    fn decode_backoff_message(
        msg: &crate::msgr2::message::Message,
    ) -> Result<crate::osdclient::messages::MOSDBackoff> {
        use crate::msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
        use crate::osdclient::messages::MOSDBackoff;

        let header = CephMsgHeader::new(MOSDBackoff::msg_type(), MOSDBackoff::msg_version(0));
        MOSDBackoff::decode_payload(&header, &msg.front, &[], &[]).map_err(Into::into)
    }

    /// Get session for OSD, returning error if not found
    async fn get_session_for_osd(&self, osd_id: i32) -> Result<Arc<OSDSession>> {
        let sessions = self.sessions.read().await;
        sessions.get(&osd_id).cloned().ok_or_else(|| {
            OSDClientError::Connection(format!("No session found for OSD {}", osd_id))
        })
    }

    /// Handle BLOCK backoff operation
    async fn handle_backoff_block(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        backoff: crate::osdclient::messages::MOSDBackoff,
    ) -> Result<()> {
        info!(
            "OSD {} requests backoff: pg={}:{}.{}, id={}, range=[{:?}, {:?})",
            osd_id,
            backoff.pgid.pool,
            backoff.pgid.seed,
            backoff.pgid.shard,
            backoff.id,
            backoff.begin,
            backoff.end
        );

        // Register backoff in session
        self.register_backoff(session, &backoff).await;

        // Send ACK_BLOCK reply
        self.send_backoff_ack(osd_id, session, backoff).await
    }

    /// Register backoff entry in session tracker
    async fn register_backoff(
        &self,
        session: &Arc<OSDSession>,
        backoff: &crate::osdclient::messages::MOSDBackoff,
    ) {
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

    /// Send ACK_BLOCK message to OSD
    async fn send_backoff_ack(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        backoff: crate::osdclient::messages::MOSDBackoff,
    ) -> Result<()> {
        use crate::msgr2::ceph_message::CephMessagePayload;
        use crate::osdclient::messages::{CEPH_OSD_BACKOFF_OP_ACK_BLOCK, MOSDBackoff};

        let ack = MOSDBackoff::new(
            backoff.pgid,
            backoff.map_epoch,
            CEPH_OSD_BACKOFF_OP_ACK_BLOCK,
            backoff.id,
            backoff.begin,
            backoff.end,
        );

        match ack.encode_payload(0) {
            Ok(payload) => {
                let msg = crate::msgr2::message::Message::new(
                    crate::osdclient::messages::CEPH_MSG_OSD_BACKOFF,
                    payload,
                )
                .with_version(MOSDBackoff::VERSION);

                let send_tx = session.send_tx();
                if let Err(e) = send_tx.send(msg).await {
                    error!("Failed to send ACK_BLOCK to OSD {}: {}", osd_id, e);
                } else {
                    debug!("Sent ACK_BLOCK for backoff id={} to OSD {}", ack.id, osd_id);
                }
                Ok(())
            }
            Err(e) => {
                error!("Failed to encode ACK_BLOCK message: {}", e);
                Err(OSDClientError::Encoding(format!(
                    "Failed to encode ACK_BLOCK: {}",
                    e
                )))
            }
        }
    }

    /// Handle UNBLOCK backoff operation
    async fn handle_backoff_unblock(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        backoff: crate::osdclient::messages::MOSDBackoff,
    ) -> Result<()> {
        info!(
            "OSD {} lifts backoff: pg={}:{}.{}, id={}, range=[{:?}, {:?})",
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

        Ok(())
    }
}
