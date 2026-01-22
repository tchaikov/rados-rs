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
use crate::types::{OSDOp, ObjectId, ReadResult, StatResult, StripedPgId, WriteResult};

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
    pub client_inc: u32,
}

impl Default for OSDClientConfig {
    fn default() -> Self {
        Self {
            entity_name: "client.admin".to_string(),
            keyring_path: None,
            tracker_config: TrackerConfig::default(),
            client_inc: 1,
        }
    }
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

        // Map object to PG and OSDs
        debug!(
            "OSD weights from map (max_osd={}): {:?}",
            osdmap.max_osd, osdmap.osd_weight
        );
        let (pg, osds) = crush::placement::object_to_osds(
            crush_map,
            oid,
            &locator,
            pool_info.pg_num,
            pool_info.crush_rule as u32,
            &osdmap.osd_weight, // Use actual OSD weights from OSDMap (16.16 fixed-point format)
            pool_info.size as usize,
        )
        .map_err(|e| OSDClientError::Crush(format!("CRUSH placement failed: {}", e)))?;

        if osds.is_empty() {
            return Err(OSDClientError::NoOSDs);
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
        let msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch,
            0, // flags
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
        let msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch,
            0, // flags
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
        let msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch,
            0, // flags
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
        let msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch,
            0, // flags
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
        // TODO: Proper parsing of stat structure
        // For now, return simplified result
        Ok(StatResult {
            size: 0, // Would be parsed from outdata
            mtime: std::time::SystemTime::now(),
        })
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
        let msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch,
            0, // flags
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
}
