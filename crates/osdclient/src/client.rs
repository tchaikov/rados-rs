//! RADOS OSD Client
//!
//! Main entry point for performing object operations against a Ceph cluster.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use denc::{Denc, VersionedEncode};
use msgr2::{Dispatcher, MessageBus};

use crate::error::{OSDClientError, Result};
use crate::messages::MOSDOp;
use crate::session::OSDSession;
use crate::throttle::Throttle;
use crate::tracker::{Tracker, TrackerConfig};
use crate::types::{
    calc_op_budget, ListObjectEntry, ListResult, OSDOp, ObjectId, OsdOpFlags, PoolFlags,
    ReadResult, RequestRedirect, StatResult, StripedPgId, WriteResult,
};

/// Configuration for OSD client
#[derive(Debug, Clone, Default)]
pub struct OSDClientConfig {
    /// Entity name (e.g., "client.admin")
    pub entity_name: String,
    /// Path to keyring file for authentication
    pub keyring_path: Option<String>,
    /// Operation timeout configuration
    pub tracker_config: TrackerConfig,
    /// Client incarnation number
    /// This should be unique per client instance to avoid OSD duplicate request detection.
    /// In Ceph C++, this is typically 0, with uniqueness provided by the global_id
    /// in the entity_name instead.
    pub client_inc: u32,
}

/// Main OSD client for performing object operations
pub struct OSDClient {
    config: OSDClientConfig,
    mon_client: Arc<monclient::MonClient>,
    sessions: Arc<RwLock<HashMap<i32, Arc<OSDSession>>>>,
    tracker: Arc<Tracker>,
    /// Request throttle to prevent resource exhaustion
    throttle: Arc<Throttle>,
    /// Global ID from monitor authentication (used in entity_name for request IDs)
    global_id: u64,
    /// Cluster FSID for OSDMap validation
    fsid: denc::UuidD,
    /// Current OSDMap
    osdmap: Arc<RwLock<Option<Arc<denc::osdmap::OSDMap>>>>,
    /// Global message bus for inter-component messaging
    message_bus: Arc<MessageBus>,
    /// Notification for OSDMap arrival
    osdmap_notify: Arc<tokio::sync::Notify>,
}

impl OSDClient {
    /// Create a new OSD client
    pub async fn new(
        config: OSDClientConfig,
        fsid: denc::UuidD,
        mon_client: Arc<monclient::MonClient>,
        message_bus: Arc<MessageBus>,
    ) -> Result<Self> {
        info!("Creating OSDClient for {}", config.entity_name);

        // Get global_id from MonClient (assigned during authentication)
        let global_id = mon_client.get_global_id().await;
        info!("OSDClient using global_id {} from monitor", global_id);

        let tracker = Arc::new(Tracker::new(config.tracker_config.clone()));

        // Create throttle with default limits (1024 ops, 100MB)
        let throttle = Arc::new(Throttle::default_limits());
        info!(
            "OSDClient throttle: max_ops={}, max_bytes={}",
            throttle.max_ops(),
            throttle.max_bytes()
        );

        let client = Self {
            config,
            mon_client: Arc::clone(&mon_client),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            tracker,
            throttle,
            global_id,
            fsid,
            osdmap: Arc::new(RwLock::new(None)),
            message_bus,
            osdmap_notify: Arc::new(tokio::sync::Notify::new()),
        };

        Ok(client)
    }

    /// Register this OSDClient as a handler for OSDMap messages on the message bus
    pub async fn register_handlers(self: Arc<Self>) -> Result<()> {
        info!("Registering OSDClient as OSDMap handler on message bus");
        self.message_bus
            .register(msgr2::message::CEPH_MSG_OSD_MAP, self.clone())
            .await;
        Ok(())
    }

    /// Get the current OSDMap
    pub async fn get_osdmap(&self) -> Result<Arc<denc::osdmap::OSDMap>> {
        let osdmap_guard = self.osdmap.read().await;
        match osdmap_guard.as_ref() {
            Some(map) => Ok(Arc::clone(map)),
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
    ) -> Result<Arc<denc::osdmap::OSDMap>> {
        // Check if we already have an OSDMap
        {
            let osdmap_guard = self.osdmap.read().await;
            if let Some(map) = osdmap_guard.as_ref() {
                return Ok(Arc::clone(map));
            }
        }

        // Wait for OSDMap notification with timeout
        tokio::select! {
            _ = self.osdmap_notify.notified() => {
                // After notification, get the map
                let osdmap_guard = self.osdmap.read().await;
                match osdmap_guard.as_ref() {
                    Some(map) => {
                        info!("OSDMap received via event notification");
                        Ok(Arc::clone(map))
                    }
                    None => Err(OSDClientError::Connection("OSDMap not available after notification".to_string()))
                }
            }
            _ = tokio::time::sleep(timeout) => {
                Err(OSDClientError::Connection("Timeout waiting for OSDMap".to_string()))
            }
        }
    }

    /// Get or create a session for an OSD
    async fn get_or_create_session(&self, osd_id: i32) -> Result<Arc<OSDSession>> {
        // Check if we already have a session
        {
            let sessions = self.sessions.read().await;
            if let Some(session) = sessions.get(&osd_id) {
                if session.is_connected().await {
                    return Ok(Arc::clone(session));
                }
            }
        }

        // Create a new session
        let mut sessions = self.sessions.write().await;

        // Double-check after acquiring write lock
        if let Some(session) = sessions.get(&osd_id) {
            if session.is_connected().await {
                return Ok(Arc::clone(session));
            }
        }

        // Create new session
        info!("Creating new session for OSD {}", osd_id);

        // Get service auth provider from monitor client
        let auth_provider = self
            .mon_client
            .get_service_auth_provider()
            .await
            .map(|provider| Box::new(provider) as Box<dyn auth::AuthProvider>);

        let mut session = OSDSession::new(
            osd_id,
            self.config.entity_name.clone(),
            self.config.client_inc,
            auth_provider,
        );

        // Get OSD address from OSDMap
        let osd_addr = self.get_osd_address(osd_id).await?;

        // Connect to OSD - this spawns the I/O task
        session.connect(osd_addr).await?;

        // Wrap in Arc and store
        let session = Arc::new(session);
        sessions.insert(osd_id, Arc::clone(&session));

        Ok(session)
    }

    /// Get OSD address from OSDMap
    async fn get_osd_address(&self, osd_id: i32) -> Result<denc::EntityAddr> {
        let osdmap = self
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::Connection(format!("Failed to get OSDMap: {}", e)))?;

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
        for addr in &addrvec.addrs {
            if matches!(addr.addr_type, denc::EntityAddrType::Msgr2) {
                // Return the full EntityAddr with nonce intact
                return Ok(addr.clone());
            }
        }

        Err(OSDClientError::Connection(format!(
            "No msgr2 address found for OSD {}",
            osd_id
        )))
    }

    /// Map an object to OSDs using CRUSH
    async fn object_to_osds(&self, pool: i64, oid: &str) -> Result<(StripedPgId, Vec<i32>)> {
        // Get current OSDMap from MonClient
        let osdmap = self
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::Connection(format!("Failed to get OSDMap: {}", e)))?;

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
            hash: -1,
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
        // Convert crush::PgId to denc::PgId for looking up in OSDMap
        let pgid = denc::PgId {
            pool: pg.pool as u64,
            seed: pg.seed,
        };

        // 1. Check pg_upmap (complete acting set override)
        if let Some(upmap_osds) = osdmap.pg_upmap.get(&pgid) {
            info!(
                "Using pg_upmap override for PG {:?}: {:?}",
                pgid, upmap_osds
            );
            osds = upmap_osds.clone();
        }

        // 2. Check pg_temp (temporary override during recovery)
        if let Some(temp_osds) = osdmap.pg_temp.get(&pgid) {
            info!("Using pg_temp override for PG {:?}: {:?}", pgid, temp_osds);
            osds = temp_osds.clone();
        }

        // 3. Check pg_upmap_items (fine-grained OSD replacements)
        if let Some(upmap_items) = osdmap.pg_upmap_items.get(&pgid) {
            info!(
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
            info!(
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
        op.object.pool = redirect.redirect_locator.pool;
        op.object.key = redirect.redirect_locator.key.clone();
        op.object.namespace = redirect.redirect_locator.nspace.clone();

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
        pool: i64,
        oid: &str,
        ops: Vec<OSDOp>,
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
        let osdmap = self
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::Connection(format!("Failed to get OSDMap: {}", e)))?;

        // Build initial message
        let flags = MOSDOp::calculate_flags(&ops);
        let mut msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch,
            flags,
            object.clone(),
            StripedPgId::from_pg(object.pool, 0), // Will be set in loop
            ops.clone(),
            crate::types::RequestId::new(
                &self.config.entity_name,
                0,
                self.config.client_inc as i32,
            ),
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
            msg.reqid = crate::types::RequestId::new(
                &self.config.entity_name,
                tid,
                self.config.client_inc as i32,
            );

            // Submit operation
            let result_rx = session.submit_op(msg.clone()).await?;

            // Wait for result with timeout
            let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
                .await
                .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
                .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

            // Check for redirect
            if let Some(redirect) = result.redirect {
                debug!(
                    "Received redirect to pool={}, object={}, retrying",
                    redirect.redirect_locator.pool,
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

    /// Read data from an object
    pub async fn read(&self, pool: i64, oid: &str, offset: u64, len: u64) -> Result<ReadResult> {
        debug!(
            "read pool={} oid={} offset={} len={}",
            pool, oid, offset, len
        );

        let ops = vec![OSDOp::read(offset, len)];
        let result = self.execute_op(pool, oid, ops).await?;

        // Check overall result code
        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "Read operation failed".into(),
            });
        }

        // Extract read data
        if result.ops.is_empty() {
            return Err(OSDClientError::Internal("No operation results".into()));
        }

        let read_op = &result.ops[0];
        if read_op.return_code != 0 {
            return Err(OSDClientError::OSDError {
                code: read_op.return_code,
                message: "Read operation failed".into(),
            });
        }

        Ok(ReadResult {
            data: read_op.outdata.clone(),
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
        pool: i64,
        oid: &str,
        offset: u64,
        len: u64,
    ) -> Result<crate::types::SparseReadResult> {
        debug!(
            "sparse_read pool={} oid={} offset={} len={}",
            pool, oid, offset, len
        );

        let ops = vec![OSDOp::sparse_read(offset, len)];
        let result = self.execute_op(pool, oid, ops).await?;

        // Check overall result code
        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "Sparse read operation failed".into(),
            });
        }

        // Extract sparse read data
        if result.ops.is_empty() {
            return Err(OSDClientError::Internal("No operation results".into()));
        }

        let read_op = &result.ops[0];
        if read_op.return_code != 0 {
            return Err(OSDClientError::OSDError {
                code: read_op.return_code,
                message: "Sparse read operation failed".into(),
            });
        }

        // Parse sparse read result from outdata
        // The outdata contains:
        // 1. Encoded map<uint64_t, uint64_t> (extent map: offset -> length)
        // 2. Encoded bufferlist (actual data)
        use crate::types::{SparseExtent, SparseReadResult};
        use denc::denc::Denc;

        if read_op.outdata.is_empty() {
            return Ok(SparseReadResult {
                extents: vec![],
                data: bytes::Bytes::new(),
                version: result.version,
            });
        }

        let mut buf = read_op.outdata.clone();

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
        pool: i64,
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
        let result = self.execute_op(pool, oid, ops).await?;

        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "Write operation failed".into(),
            });
        }

        Ok(WriteResult {
            version: result.version,
        })
    }

    /// Write full object (overwrite)
    pub async fn write_full(
        &self,
        pool: i64,
        oid: &str,
        data: bytes::Bytes,
    ) -> Result<WriteResult> {
        debug!("write_full pool={} oid={} len={}", pool, oid, data.len());

        let ops = vec![OSDOp::write_full(data)];
        let result = self.execute_op(pool, oid, ops).await?;

        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "Write operation failed".into(),
            });
        }

        Ok(WriteResult {
            version: result.version,
        })
    }

    /// Get object statistics
    pub async fn stat(&self, pool: i64, oid: &str) -> Result<StatResult> {
        debug!("stat pool={} oid={}", pool, oid);
        let ops = vec![OSDOp::stat()];
        let result = self.execute_op(pool, oid, ops).await?;

        // Check overall result code first
        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "Stat operation failed".into(),
            });
        }

        // Extract stat data
        if result.ops.is_empty() {
            return Err(OSDClientError::Internal("No operation results".into()));
        }

        let stat_op = &result.ops[0];
        if stat_op.return_code != 0 {
            return Err(OSDClientError::OSDError {
                code: stat_op.return_code,
                message: "Stat operation failed".into(),
            });
        }

        // Parse stat data from outdata using OsdStatData's Denc implementation
        use denc::Denc;
        let stat_data = crate::denc_types::OsdStatData::decode(&mut &stat_op.outdata[..], 0)
            .map_err(|e| OSDClientError::Decoding(format!("Failed to decode stat data: {}", e)))?;

        Ok(StatResult {
            size: stat_data.size,
            mtime: stat_data.mtime,
        })
    }

    /// Delete an object
    pub async fn delete(&self, pool: i64, oid: &str) -> Result<()> {
        debug!("delete pool={} oid={}", pool, oid);
        let ops = vec![OSDOp::delete()];
        let result = self.execute_op(pool, oid, ops).await?;

        // Check overall result code
        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "Delete operation failed".into(),
            });
        }

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
        pool: i64,
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
        let osdmap = self
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::Connection(format!("Failed to get OSDMap: {}", e)))?;

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
            let ops = vec![OSDOp::pgls(remaining, hobject_cursor.clone(), osdmap_epoch)];

            // Acquire throttle permit
            let _throttle_permit = self.throttle.acquire(calc_op_budget(&ops)).await;

            // Build request ID
            let tid = session.next_tid();
            let reqid = crate::types::RequestId::new(
                &self.config.entity_name,
                tid,
                self.config.client_inc as i32,
            );

            // Build message
            let flags = MOSDOp::calculate_flags(&ops);
            let msg = MOSDOp::new(
                self.config.client_inc,
                osdmap.epoch,
                flags,
                object,
                spgid,
                ops,
                reqid,
                self.global_id,
            );

            // Submit operation
            let result_rx = session.submit_op(msg).await?;

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
                all_entries.push(ListObjectEntry::new(entry.nspace, entry.oid, entry.locator));
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

        // Get OSDMap from monitor
        let osdmap = self
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::Connection(format!("Failed to get OSDMap: {}", e)))?;

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
            let osdmap_guard = self.osdmap.read().await;
            osdmap_guard
                .as_ref()
                .map(|map| map.epoch as u64)
                .unwrap_or(0)
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
            .send_poolop(pool_name.to_string(), msg)
            .await
            .map_err(|e| OSDClientError::Connection(format!("Pool operation failed: {}", e)))?;

        if result.is_success() {
            debug!("Pool created successfully: {}", pool_name);
            Ok(())
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
            let osdmap_guard = self.osdmap.read().await;
            let osdmap = osdmap_guard
                .as_ref()
                .ok_or_else(|| OSDClientError::Other("OSD map not available yet".into()))?;

            let pool_id = osdmap
                .pool_name
                .iter()
                .find(|(_, name)| name.as_str() == pool_name)
                .map(|(id, _)| *id as u32)
                .ok_or_else(|| OSDClientError::Other(format!("Pool '{}' not found", pool_name)))?;

            let version = osdmap.epoch as u64;

            (pool_id, version)
        };

        debug!(
            "Deleting pool '{}' with ID {} (OSDMap epoch {})",
            pool_name, pool_id, version
        );

        // Create and send MPoolOp delete message
        let msg = monclient::MPoolOp::delete_pool(
            self.fsid.bytes,
            pool_id,
            pool_name.to_string(),
            version,
        );

        let result = self
            .mon_client
            .send_poolop(pool_name.to_string(), msg)
            .await
            .map_err(|e| OSDClientError::Connection(format!("Pool operation failed: {}", e)))?;

        if result.is_success() {
            debug!("Pool deleted successfully: {}", pool_name);
            Ok(())
        } else {
            Err(OSDClientError::Other(format!(
                "Pool operation failed with code {}",
                result.reply_code
            )))
        }
    }

    /// Handle OSDMap message (moved from MonClient)
    async fn handle_osdmap(&self, msg: msgr2::message::Message) -> Result<()> {
        use monclient::messages::MOSDMap;

        info!("Handling OSDMap message ({} bytes)", msg.front.len());

        // Decode MOSDMap
        let mosdmap = MOSDMap::decode(&msg.front)
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
            .osdmap
            .read()
            .await
            .as_ref()
            .map(|m| m.epoch)
            .unwrap_or(0);

        if mosdmap.get_last() <= current_epoch {
            info!(
                "Ignoring OSDMap epochs [{}..{}] <= current epoch {}",
                mosdmap.get_first(),
                mosdmap.get_last(),
                current_epoch
            );
            return Ok(());
        }

        info!(
            "Processing OSDMap epochs [{}..{}] > current epoch {}",
            mosdmap.get_first(),
            mosdmap.get_last(),
            current_epoch
        );

        // 3. Process epochs sequentially
        let mut updated = false;
        {
            let mut osdmap_guard = self.osdmap.write().await;

            if current_epoch > 0 {
                // We have a current map, apply updates sequentially
                for e in (current_epoch + 1)..=mosdmap.get_last() {
                    let current_map_epoch = osdmap_guard.as_ref().map(|m| m.epoch).unwrap_or(0);

                    if current_map_epoch == e - 1 && mosdmap.incremental_maps.contains_key(&e) {
                        // Apply incremental
                        info!("Applying incremental OSDMap for epoch {}", e);
                        let inc_bl = mosdmap.incremental_maps.get(&e).unwrap();

                        match denc::osdmap::OSDMapIncremental::decode_versioned(
                            &mut inc_bl.as_ref(),
                            0,
                        ) {
                            Ok(inc_map) => {
                                info!(
                                    "✓ Decoded incremental: epoch={}, {} new pools, {} old pools",
                                    inc_map.epoch,
                                    inc_map.new_pools.len(),
                                    inc_map.old_pools.len()
                                );

                                if let Some(current_map) = &*osdmap_guard {
                                    let mut updated_map = (**current_map).clone();
                                    if let Err(err) = inc_map.apply_to(&mut updated_map) {
                                        warn!("Failed to apply incremental epoch {}: {}", e, err);
                                    } else {
                                        *osdmap_guard = Some(Arc::new(updated_map));
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
                        info!("Using full OSDMap for epoch {}", e);
                        let full_bl = mosdmap.maps.get(&e).unwrap();
                        match denc::osdmap::OSDMap::decode_versioned(&mut full_bl.as_ref(), 0) {
                            Ok(full_map) => {
                                info!("✓ Decoded full OSDMap: epoch={}", full_map.epoch);
                                *osdmap_guard = Some(Arc::new(full_map));
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
            } else {
                // No current map, use latest full map
                if let Some((&latest_epoch, full_bl)) = mosdmap.maps.iter().max_by_key(|(e, _)| **e)
                {
                    info!("Using latest full OSDMap (epoch {})", latest_epoch);
                    match denc::osdmap::OSDMap::decode_versioned(&mut full_bl.as_ref(), 0) {
                        Ok(full_map) => {
                            info!("✓ Initial OSDMap loaded: epoch={}", full_map.epoch);
                            *osdmap_guard = Some(Arc::new(full_map));
                            updated = true;
                        }
                        Err(err) => {
                            warn!("Failed to decode initial full map: {}", err);
                        }
                    }
                }
            }
        }

        // 4. Rescan pending operations if map updated
        if updated {
            let final_epoch = self
                .osdmap
                .read()
                .await
                .as_ref()
                .map(|m| m.epoch)
                .unwrap_or(0);
            info!(
                "OSDMap updated to epoch {}, rescanning pending operations",
                final_epoch
            );

            // Notify waiters that OSDMap is available
            self.osdmap_notify.notify_waiters();

            // TODO: Rescan all sessions for pending operations
            // This will be implemented when we add rescan() method to OSDSession
        }

        Ok(())
    }
}

/// Implement Dispatcher trait for OSDClient to handle OSDMap messages
#[async_trait]
impl Dispatcher for OSDClient {
    async fn dispatch(
        &self,
        msg: msgr2::message::Message,
    ) -> std::result::Result<(), denc::RadosError> {
        // Convert OSDClientError to RadosError
        self.handle_osdmap(msg)
            .await
            .map_err(|e| denc::RadosError::Protocol(format!("OSDClient error: {}", e)))
    }
}
