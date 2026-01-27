//! RADOS OSD Client
//!
//! Main entry point for performing object operations against a Ceph cluster.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::error::{OSDClientError, Result};
use crate::messages::MOSDOp;
use crate::session::OSDSession;
use crate::tracker::{Tracker, TrackerConfig};
use crate::types::{
    ListObjectEntry, ListResult, OSDOp, ObjectId, ReadResult, StatResult, StripedPgId, WriteResult,
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
    pub client_inc: u32,
}

/// Main OSD client for performing object operations
pub struct OSDClient {
    config: OSDClientConfig,
    mon_client: Arc<monclient::MonClient>,
    sessions: Arc<RwLock<HashMap<i32, Arc<OSDSession>>>>,
    tracker: Arc<Tracker>,
}

impl OSDClient {
    /// Create a new OSD client
    pub async fn new(
        config: OSDClientConfig,
        mon_client: Arc<monclient::MonClient>,
    ) -> Result<Self> {
        info!("Creating OSDClient for {}", config.entity_name);

        let tracker = Arc::new(Tracker::new(config.tracker_config.clone()));

        Ok(Self {
            config,
            mon_client,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            tracker,
        })
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
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

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
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

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
        // FLAG_HASHPSPOOL = 1<<0 = 1 (from ~/dev/ceph/src/osd/osd_types.h)
        const FLAG_HASHPSPOOL: u64 = 1;
        let hashpspool = (pool_info.flags & FLAG_HASHPSPOOL) != 0;

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

    /// Read data from an object
    pub async fn read(&self, pool: i64, oid: &str, offset: u64, len: u64) -> Result<ReadResult> {
        debug!(
            "read pool={} oid={} offset={} len={}",
            pool, oid, offset, len
        );

        // Map to OSDs
        let (spgid, osds) = self.object_to_osds(pool, oid).await?;
        let primary_osd = osds[0];

        // Get session
        let session = self.get_or_create_session(primary_osd).await?;

        // Create object ID
        let mut object = ObjectId::new(pool, oid);
        object.calculate_hash();

        // Create read operation
        let ops = vec![OSDOp::read(offset, len)];

        // Build request ID
        let tid = session.next_tid();
        let reqid = crate::types::RequestId::new(
            &self.config.entity_name,
            tid,
            self.config.client_inc as i32,
        );

        // Get OSDMap epoch
        let osdmap = self
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

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
        );

        // Submit operation
        let result_rx = session.submit_op(msg).await?;

        // Wait for result with timeout
        let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
            .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

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

        // Map to OSDs
        let (spgid, osds) = self.object_to_osds(pool, oid).await?;
        let primary_osd = osds[0];

        // Get session
        let session = self.get_or_create_session(primary_osd).await?;

        // Create object ID
        let mut object = ObjectId::new(pool, oid);
        object.calculate_hash();

        // Create write operation
        let ops = vec![OSDOp::write(offset, data)];

        // Build request ID
        let tid = session.next_tid();
        let reqid = crate::types::RequestId::new(
            &self.config.entity_name,
            tid,
            self.config.client_inc as i32,
        );

        // Get OSDMap epoch
        let osdmap = self
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

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
        );

        // Submit operation
        let result_rx = session.submit_op(msg).await?;

        // Wait for result with timeout
        let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
            .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

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

        // Map to OSDs
        let (spgid, osds) = self.object_to_osds(pool, oid).await?;
        let primary_osd = osds[0];

        // Get session
        let session = self.get_or_create_session(primary_osd).await?;

        // Create object ID
        let mut object = ObjectId::new(pool, oid);
        object.calculate_hash();

        // Create write_full operation
        let ops = vec![OSDOp::write_full(data)];

        // Build request ID
        let tid = session.next_tid();
        let reqid = crate::types::RequestId::new(
            &self.config.entity_name,
            tid,
            self.config.client_inc as i32,
        );

        // Get OSDMap epoch
        let osdmap = self
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

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
        );

        // Submit operation
        let result_rx = session.submit_op(msg).await?;

        // Wait for result with timeout
        let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
            .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

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

        // Map to OSDs
        let (spgid, osds) = self.object_to_osds(pool, oid).await?;
        let primary_osd = osds[0];

        // Get session
        let session = self.get_or_create_session(primary_osd).await?;

        // Create object ID
        let mut object = ObjectId::new(pool, oid);
        object.calculate_hash();

        // Create stat operation
        let ops = vec![OSDOp::stat()];

        // Build request ID
        let tid = session.next_tid();
        let reqid = crate::types::RequestId::new(
            &self.config.entity_name,
            tid,
            self.config.client_inc as i32,
        );

        // Get OSDMap epoch
        let osdmap = self
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

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
        );

        // Submit operation
        let result_rx = session.submit_op(msg).await?;

        // Wait for result with timeout
        let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
            .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

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

        // Parse stat data from outdata
        // Format: u64 size + u32 tv_sec + u32 tv_nsec (utime_t)
        use bytes::Buf;
        let mut data = &stat_op.outdata[..];

        if data.remaining() < 16 {
            return Err(OSDClientError::Decoding(format!(
                "Incomplete stat data: expected 16 bytes, got {}",
                data.remaining()
            )));
        }

        let size = data.get_u64_le();
        let tv_sec = data.get_u32_le();
        let tv_nsec = data.get_u32_le();

        // Convert to SystemTime
        let mtime = std::time::UNIX_EPOCH + std::time::Duration::new(tv_sec as u64, tv_nsec);

        Ok(StatResult { size, mtime })
    }

    /// Delete an object
    pub async fn delete(&self, pool: i64, oid: &str) -> Result<()> {
        debug!("delete pool={} oid={}", pool, oid);

        // Map to OSDs
        let (spgid, osds) = self.object_to_osds(pool, oid).await?;
        let primary_osd = osds[0];

        // Get session
        let session = self.get_or_create_session(primary_osd).await?;

        // Create object ID
        let mut object = ObjectId::new(pool, oid);
        object.calculate_hash();

        // Create delete operation
        let ops = vec![OSDOp::delete()];

        // Build request ID
        let tid = session.next_tid();
        let reqid = crate::types::RequestId::new(
            &self.config.entity_name,
            tid,
            self.config.client_inc as i32,
        );

        // Get OSDMap epoch
        let osdmap = self
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

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
        );

        // Submit operation
        let result_rx = session.submit_op(msg).await?;

        // Wait for result with timeout
        let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
            .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

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
    /// Lists objects in the specified pool, optionally starting from a cursor.
    /// Returns a list of objects and an optional continuation cursor for pagination.
    ///
    /// # Arguments
    /// * `pool` - Pool ID to list objects from
    /// * `cursor` - Optional cursor for pagination (None for first page)
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

        // For listing, we use a special object name (empty string)
        // and rely on the PGLS operation
        let oid = "";

        // Map to OSDs
        let (spgid, osds) = self.object_to_osds(pool, oid).await?;
        let primary_osd = osds[0];

        // Get session
        let session = self.get_or_create_session(primary_osd).await?;

        // Create object ID
        let object = ObjectId::new(pool, oid);
        // For PGLS operations, don't calculate hash from object name
        // The hash should match the PG we're querying
        // object.calculate_hash();

        // Get OSDMap epoch for start_epoch parameter
        let osdmap = self
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

        // Parse cursor (default to 0 for start)
        let cursor_value = cursor
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        // Create pgls operation
        let ops = vec![OSDOp::pgls(max_entries, cursor_value, osdmap.epoch)];

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
        );

        // Submit operation
        let result_rx = session.submit_op(msg).await?;

        // Wait for result with timeout
        let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
            .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "List operation failed".into(),
            });
        }

        // Parse the pg_nls_response from outdata
        if result.ops.is_empty() {
            return Err(OSDClientError::Internal(
                "No operation reply in list response".into(),
            ));
        }

        let outdata = &result.ops[0].outdata;

        // Decode pg_nls_response_t
        // Format: handle (u64) + count (u32) + entries
        use bytes::Buf;

        if outdata.len() < 12 {
            return Err(OSDClientError::Internal("List response too short".into()));
        }

        let mut buf = outdata.as_ref();

        // Read handle (cursor for next iteration)
        let handle = buf.get_u64_le();

        // Read entry count
        let count = buf.get_u32_le();

        // Parse entries
        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            // Each entry: namespace (string), oid (string), locator (string)
            // String format: length (u32) + bytes

            // namespace
            if buf.remaining() < 4 {
                return Err(OSDClientError::Internal(
                    "Incomplete entry in list response".into(),
                ));
            }
            let nspace_len = buf.get_u32_le() as usize;
            if buf.remaining() < nspace_len {
                return Err(OSDClientError::Internal(
                    "Incomplete namespace in list response".into(),
                ));
            }
            let nspace = String::from_utf8(buf.copy_to_bytes(nspace_len).to_vec())
                .map_err(|_| OSDClientError::Internal("Invalid UTF-8 in namespace".into()))?;

            // oid
            if buf.remaining() < 4 {
                return Err(OSDClientError::Internal(
                    "Incomplete entry in list response".into(),
                ));
            }
            let oid_len = buf.get_u32_le() as usize;
            if buf.remaining() < oid_len {
                return Err(OSDClientError::Internal(
                    "Incomplete oid in list response".into(),
                ));
            }
            let oid = String::from_utf8(buf.copy_to_bytes(oid_len).to_vec())
                .map_err(|_| OSDClientError::Internal("Invalid UTF-8 in oid".into()))?;

            // locator
            if buf.remaining() < 4 {
                return Err(OSDClientError::Internal(
                    "Incomplete entry in list response".into(),
                ));
            }
            let locator_len = buf.get_u32_le() as usize;
            if buf.remaining() < locator_len {
                return Err(OSDClientError::Internal(
                    "Incomplete locator in list response".into(),
                ));
            }
            let locator = String::from_utf8(buf.copy_to_bytes(locator_len).to_vec())
                .map_err(|_| OSDClientError::Internal("Invalid UTF-8 in locator".into()))?;

            entries.push(ListObjectEntry::new(nspace, oid, locator));
        }

        // Convert handle to cursor
        // Handle of u64::MAX means we've reached the end
        let cursor = if handle == u64::MAX {
            None
        } else {
            Some(handle.to_string())
        };

        Ok(ListResult { entries, cursor })
    }

    /// List all pools in the cluster
    ///
    /// Returns a list of all pools with their IDs and names.
    pub async fn list_pools(&self) -> Result<Vec<crate::types::PoolInfo>> {
        debug!("Listing pools");

        // Get OSDMap from monitor
        let osdmap = self
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

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
}
