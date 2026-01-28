//! RADOS OSD Client
//!
//! Main entry point for performing object operations against a Ceph cluster.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use denc::Denc;

use crate::error::{OSDClientError, Result};
use crate::messages::MOSDOp;
use crate::session::OSDSession;
use crate::tracker::{Tracker, TrackerConfig};
use crate::types::{
    ListObjectEntry, ListResult, OSDOp, ObjectId, ReadResult, StatResult, StripedPgId, WriteResult,
};

/// Configuration for OSD client
#[derive(Debug, Clone)]
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

impl Default for OSDClientConfig {
    fn default() -> Self {
        Self {
            entity_name: String::new(),
            keyring_path: None,
            tracker_config: TrackerConfig::default(),
            // Use 0 like Ceph C++ clients
            // Uniqueness comes from global_id in entity_name, not from client_inc
            client_inc: 0,
        }
    }
}

/// Main OSD client for performing object operations
pub struct OSDClient {
    config: OSDClientConfig,
    mon_client: Arc<monclient::MonClient>,
    sessions: Arc<RwLock<HashMap<i32, Arc<OSDSession>>>>,
    tracker: Arc<Tracker>,
    /// Global ID from monitor authentication (used in entity_name for request IDs)
    global_id: u64,
}

impl OSDClient {
    /// Create a new OSD client
    pub async fn new(
        config: OSDClientConfig,
        mon_client: Arc<monclient::MonClient>,
    ) -> Result<Self> {
        info!("Creating OSDClient for {}", config.entity_name);

        // Get global_id from MonClient (assigned during authentication)
        let global_id = mon_client.get_global_id().await;
        info!("OSDClient using global_id {} from monitor", global_id);

        let tracker = Arc::new(Tracker::new(config.tracker_config.clone()));

        Ok(Self {
            config,
            mon_client,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            tracker,
            global_id,
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
            self.global_id,
        );

        // Submit operation
        let result_rx = session.submit_op(msg).await?;

        // Wait for result with timeout
        let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
            .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

        // Check overall result code first
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
            self.global_id,
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
        eprintln!(
            "DEBUG write_full: object='{}' mapped to PG={:?}, primary_osd={}, all_osds={:?}",
            oid, spgid, primary_osd, osds
        );

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
            self.global_id,
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
        eprintln!(
            "DEBUG stat: object='{}' mapped to PG={:?}, primary_osd={}, all_osds={:?}",
            oid, spgid, primary_osd, osds
        );

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
            self.global_id,
        );

        // Submit operation
        let result_rx = session.submit_op(msg).await?;

        // Wait for result with timeout
        let result = tokio::time::timeout(self.tracker.operation_timeout(), result_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(self.tracker.operation_timeout()))?
            .map_err(|_| OSDClientError::Internal("Operation cancelled".into()))??;

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
            self.global_id,
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
            .mon_client
            .get_osdmap()
            .await
            .map_err(|e| OSDClientError::MonClient(format!("Failed to get OSDMap: {}", e)))?;

        // Find pool info
        let pool_info = osdmap
            .pools
            .get(&pool)
            .ok_or(OSDClientError::PoolNotFound(pool))?;

        // Get number of PGs in this pool
        let pg_num = pool_info.pg_num;
        let starting_pg = current_pg;

        // Check if pool has hashpspool flag
        const FLAG_HASHPSPOOL: u64 = 1;
        let hashpspool = (pool_info.flags & FLAG_HASHPSPOOL) != 0;

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
