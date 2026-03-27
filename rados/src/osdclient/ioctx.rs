//! I/O Context for pool-specific operations
//!
//! IoCtx represents an I/O context for a specific Ceph pool, providing
//! object operations (create, read, write, stat, delete) and object listing.

use bytes::Bytes;
use std::sync::Arc;
use tracing::{debug, info};

use crate::osdclient::client::OSDClient;
use crate::osdclient::error::{OSDClientError, Result};
use crate::osdclient::operation::OpBuilder;
use crate::osdclient::snapshot::SnapId;
use crate::osdclient::types::{OSDOp, ReadResult, SparseReadResult, StatResult, WriteResult};

/// Maximum entries per PGLS request for object listing pagination
const MAX_ENTRIES_PER_REQUEST: usize = 100;

/// I/O Context for a specific pool
///
/// IoCtx provides an interface for performing object operations within a specific
/// Ceph pool. It tracks pending write operations and provides async methods for
/// all object operations.
///
/// # Example
///
/// ```no_run
/// # use crate::osdclient::{OSDClient, IoCtx};
/// # use std::sync::Arc;
/// # async fn example(client: Arc<OSDClient>) -> Result<(), Box<dyn std::error::Error>> {
/// // Create an IoCtx for a specific pool
/// let ioctx = IoCtx::new(client, 1).await?;
///
/// // Write an object
/// ioctx.write_full("my-object", b"Hello, Ceph!".to_vec()).await?;
///
/// // Read it back
/// let data = ioctx.read("my-object", 0, 0).await?;
///
/// // Get stats
/// let stats = ioctx.stat("my-object").await?;
///
/// // Remove it
/// ioctx.remove("my-object").await?;
/// # Ok(())
/// # }
/// ```
pub struct IoCtx {
    /// OSD client for performing operations
    client: Arc<OSDClient>,

    /// Pool ID this context is associated with
    pool_id: u64,

    /// Pool name (cached, initialized on first access)
    pool_name: tokio::sync::OnceCell<String>,
}

impl IoCtx {
    /// Create a new I/O context for a specific pool
    ///
    /// # Arguments
    ///
    /// * `client` - The OSD client to use for operations
    /// * `pool_id` - The ID of the pool this context operates on
    ///
    /// # Returns
    ///
    /// Returns a new IoCtx instance
    pub async fn new(client: Arc<OSDClient>, pool_id: u64) -> Result<Self> {
        info!("Creating IoCtx for pool {}", pool_id);

        Ok(Self {
            client,
            pool_id,
            pool_name: tokio::sync::OnceCell::new(),
        })
    }

    /// Get the pool ID
    pub fn pool_id(&self) -> u64 {
        self.pool_id
    }

    /// Get the pool name (cached)
    pub async fn pool_name(&self) -> Result<String> {
        self.pool_name
            .get_or_try_init(|| async {
                let osdmap = self.client.get_osdmap().await?;
                osdmap
                    .get_pool_name(self.pool_id)
                    .cloned()
                    .ok_or(OSDClientError::PoolNotFound(self.pool_id))
            })
            .await
            .cloned()
    }

    /// Create an object (optionally exclusive)
    ///
    /// # Arguments
    ///
    /// * `oid` - Object name
    /// * `exclusive` - If true, fail with `-EEXIST` if the object already exists.
    ///   If false, the operation is a no-op when the object already exists.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success, or an error if the operation fails.
    pub async fn create(&self, oid: &str, exclusive: bool) -> Result<()> {
        debug!("Creating object {} (exclusive={})", oid, exclusive);

        let op = OpBuilder::new().create(exclusive).build();
        let result = self.client.execute_built_op(self.pool_id, oid, op).await?;

        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "create failed".into(),
            });
        }
        Ok(())
    }

    /// Write data to an object, replacing its contents
    ///
    /// # Arguments
    ///
    /// * `oid` - Object name
    /// * `data` - Data to write
    ///
    /// # Returns
    ///
    /// Returns the write result (version) on success
    pub async fn write_full(&self, oid: &str, data: impl Into<Bytes>) -> Result<WriteResult> {
        let data = data.into();
        debug!("Writing {} bytes to object {}", data.len(), oid);

        self.client.write_full(self.pool_id, oid, data).await
    }

    /// Atomically append data to the end of an object.
    ///
    /// The OSD determines the actual write offset; the client does not need to
    /// know or track the current object size.  Matches `IoCtx::append()` /
    /// `rados_append()` in librados.
    pub async fn append(&self, oid: &str, data: impl Into<Bytes>) -> Result<WriteResult> {
        let data = data.into();
        debug!("Appending {} bytes to object {}", data.len(), oid);

        let op = OpBuilder::new().append(data).build();
        let result = self.client.execute_built_op(self.pool_id, oid, op).await?;

        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: "append failed".into(),
            });
        }
        Ok(WriteResult {
            version: result.version,
        })
    }

    /// Write data to an object at a specific byte offset
    ///
    /// Unlike [`write_full`], this does not truncate the object; it overwrites
    /// only the bytes `[offset, offset + data.len())`.
    pub async fn write(
        &self,
        oid: &str,
        offset: u64,
        data: impl Into<Bytes>,
    ) -> Result<WriteResult> {
        let data = data.into();
        debug!(
            "Writing {} bytes to object {} at offset {}",
            data.len(),
            oid,
            offset
        );

        self.client.write(self.pool_id, oid, offset, data).await
    }

    /// Read data from an object
    ///
    /// # Arguments
    ///
    /// * `oid` - Object name
    /// * `offset` - Byte offset to start reading from
    /// * `length` - Number of bytes to read (0 = read to end)
    ///
    /// # Returns
    ///
    /// Returns the data read from the object
    pub async fn read(&self, oid: &str, offset: u64, length: u64) -> Result<ReadResult> {
        debug!(
            "Reading from object {} (offset={}, length={})",
            oid, offset, length
        );

        self.client.read(self.pool_id, oid, offset, length).await
    }

    /// Sparse read data from an object
    ///
    /// Sparse read returns a map of extents indicating which regions of the object
    /// contain data, along with the actual data. This is useful for efficiently
    /// reading sparse objects (e.g., VM disk images with holes).
    ///
    /// # Arguments
    ///
    /// * `oid` - Object name
    /// * `offset` - Byte offset to start reading from
    /// * `length` - Number of bytes to read (0 = read to end)
    ///
    /// # Returns
    ///
    /// Returns the sparse read result with extents and data
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crate::osdclient::IoCtx;
    /// # async fn example(ioctx: IoCtx) -> Result<(), Box<dyn std::error::Error>> {
    /// // Read 1MB starting at offset 0
    /// let result = ioctx.sparse_read("myobject", 0, 1024*1024).await?;
    ///
    /// // Check which regions have data
    /// for extent in &result.extents {
    ///     println!("Data at offset {} for {} bytes", extent.offset, extent.length);
    /// }
    ///
    /// // Access the actual data
    /// println!("Total data bytes: {}", result.data.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sparse_read(
        &self,
        oid: &str,
        offset: u64,
        length: u64,
    ) -> Result<SparseReadResult> {
        debug!(
            "Sparse reading from object {} (offset={}, length={})",
            oid, offset, length
        );

        self.client
            .sparse_read(self.pool_id, oid, offset, length)
            .await
    }

    /// Get object metadata (size and mtime)
    ///
    /// # Arguments
    ///
    /// * `oid` - Object name
    ///
    /// # Returns
    ///
    /// Returns object size and modification time
    pub async fn stat(&self, oid: &str) -> Result<StatResult> {
        debug!("Getting stats for object {}", oid);

        self.client.stat(self.pool_id, oid).await
    }

    /// Remove an object
    ///
    /// # Arguments
    ///
    /// * `oid` - Object name
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success (or if object didn't exist)
    pub async fn remove(&self, oid: &str) -> Result<()> {
        info!("Removing object {}", oid);

        self.client.delete(self.pool_id, oid).await?;
        Ok(())
    }

    /// List objects in the pool with pagination
    ///
    /// # Arguments
    ///
    /// * `cursor` - Pagination cursor (None to start from beginning)
    /// * `max_entries` - Maximum number of objects to return
    ///
    /// # Returns
    ///
    /// Returns a tuple of (objects, next_cursor) where next_cursor is None if no more objects
    pub async fn list_objects(
        &self,
        cursor: Option<String>,
        max_entries: usize,
    ) -> Result<(Vec<String>, Option<String>)> {
        debug!(
            "Listing objects in pool {} (cursor={:?}, max={})",
            self.pool_id, cursor, max_entries
        );

        let result = self
            .client
            .list(self.pool_id, cursor, max_entries as u64)
            .await?;

        // Extract object names from entries
        let object_names: Vec<String> = result.entries.into_iter().map(|entry| entry.oid).collect();

        Ok((object_names, result.cursor))
    }

    /// List all objects in the pool
    ///
    /// This method automatically handles pagination and returns all objects in the pool.
    /// It's equivalent to the C++ librados `ls` command.
    ///
    /// # Returns
    ///
    /// Returns a vector of all object names in the pool
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crate::osdclient::IoCtx;
    /// # async fn example(ioctx: IoCtx) -> Result<(), Box<dyn std::error::Error>> {
    /// let objects = ioctx.ls().await?;
    /// for obj in objects {
    ///     println!("{}", obj);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ls(&self) -> Result<Vec<String>> {
        use futures::StreamExt;
        info!("Listing all objects in pool {}", self.pool_id);
        let all_objects =
            crate::osdclient::list_objects_stream(self.clone(), MAX_ENTRIES_PER_REQUEST)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
        info!(
            "Found {} total objects in pool {}",
            all_objects.len(),
            self.pool_id
        );
        Ok(all_objects)
    }

    /// Acquire an exclusive lock on an object
    ///
    /// # Arguments
    ///
    /// * `oid` - Object identifier
    /// * `name` - Lock name (can have multiple named locks per object)
    /// * `cookie` - Unique lock identifier for this client
    /// * `description` - Human-readable description
    /// * `duration` - Lock duration (None = infinite)
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is already held by another client
    pub async fn lock_exclusive(
        &self,
        oid: impl Into<String>,
        name: &str,
        cookie: &str,
        description: &str,
        duration: Option<std::time::Duration>,
    ) -> Result<()> {
        let oid_str = oid.into();
        debug!(
            "Acquiring exclusive lock '{}' on object '{}' in pool {}",
            name, oid_str, self.pool_id
        );

        let op = OpBuilder::new()
            .op(OSDOp::lock_exclusive(name, cookie, description, duration)?)
            .build();

        self.client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;
        Ok(())
    }

    /// Acquire a shared lock on an object
    ///
    /// # Arguments
    ///
    /// * `oid` - Object identifier
    /// * `name` - Lock name
    /// * `cookie` - Unique lock identifier for this client
    /// * `tag` - Shared lock tag (all shared locks with same tag are compatible)
    /// * `description` - Human-readable description
    /// * `duration` - Lock duration (None = infinite)
    ///
    /// # Errors
    ///
    /// Returns an error if an incompatible lock is held
    pub async fn lock_shared(
        &self,
        oid: impl Into<String>,
        name: &str,
        cookie: &str,
        tag: &str,
        description: &str,
        duration: Option<std::time::Duration>,
    ) -> Result<()> {
        let oid_str = oid.into();
        debug!(
            "Acquiring shared lock '{}' on object '{}' in pool {}",
            name, oid_str, self.pool_id
        );

        let op = OpBuilder::new()
            .op(OSDOp::lock_shared(
                name,
                cookie,
                tag,
                description,
                duration,
            )?)
            .build();

        self.client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;
        Ok(())
    }

    /// Release a lock on an object
    ///
    /// # Arguments
    ///
    /// * `oid` - Object identifier
    /// * `name` - Lock name to unlock
    /// * `cookie` - Lock identifier that was used when acquiring the lock
    pub async fn unlock(&self, oid: impl Into<String>, name: &str, cookie: &str) -> Result<()> {
        let oid_str = oid.into();
        debug!(
            "Releasing lock '{}' on object '{}' in pool {}",
            name, oid_str, self.pool_id
        );

        let op = OpBuilder::new().op(OSDOp::unlock(name, cookie)?).build();

        self.client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;
        Ok(())
    }

    /// Get an extended attribute value
    ///
    /// # Arguments
    ///
    /// * `oid` - Object identifier
    /// * `name` - Attribute name
    ///
    /// # Returns
    ///
    /// The attribute value as bytes
    pub async fn get_xattr(
        &self,
        oid: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Bytes> {
        let oid_str = oid.into();
        let name_str = name.into();
        debug!(
            "Getting xattr '{}' from object '{}' in pool {}",
            name_str, oid_str, self.pool_id
        );

        let op = OpBuilder::new().op(OSDOp::get_xattr(name_str)?).build();

        let result = self
            .client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;

        Ok(result.first_outdata()?.clone())
    }

    /// Set an extended attribute
    ///
    /// # Arguments
    ///
    /// * `oid` - Object identifier
    /// * `name` - Attribute name
    /// * `value` - Attribute value
    pub async fn set_xattr(
        &self,
        oid: impl Into<String>,
        name: impl Into<String>,
        value: Bytes,
    ) -> Result<()> {
        let oid_str = oid.into();
        let name_str = name.into();
        debug!(
            "Setting xattr '{}' on object '{}' in pool {}",
            name_str, oid_str, self.pool_id
        );

        let op = OpBuilder::new()
            .op(OSDOp::set_xattr(name_str, value)?)
            .build();

        self.client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;
        Ok(())
    }

    /// Remove an extended attribute
    ///
    /// # Arguments
    ///
    /// * `oid` - Object identifier
    /// * `name` - Attribute name to remove
    pub async fn remove_xattr(
        &self,
        oid: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<()> {
        let oid_str = oid.into();
        let name_str = name.into();
        debug!(
            "Removing xattr '{}' from object '{}' in pool {}",
            name_str, oid_str, self.pool_id
        );

        let op = OpBuilder::new().op(OSDOp::remove_xattr(name_str)?).build();

        self.client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;
        Ok(())
    }

    /// List all extended attribute names for an object
    ///
    /// # Arguments
    ///
    /// * `oid` - Object identifier
    ///
    /// # Returns
    ///
    /// Vector of attribute names
    pub async fn list_xattrs(&self, oid: impl Into<String>) -> Result<Vec<String>> {
        use crate::Denc;

        let oid_str = oid.into();
        debug!(
            "Listing xattrs for object '{}' in pool {}",
            oid_str, self.pool_id
        );

        let op = OpBuilder::new().op(OSDOp::list_xattrs()).build();

        let result = self
            .client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;

        // Parse outdata as list of strings
        let mut data = &result.first_outdata()?[..];
        Ok(Vec::<String>::decode(&mut data, 0)?)
    }

    // ---- Pool-level snapshot operations ----

    /// Fetch the current `PgPool` descriptor for this IoCtx's pool.
    async fn get_pool_info(&self) -> Result<crate::osdclient::osdmap::PgPool> {
        let osdmap = self.client.get_osdmap().await?;
        osdmap
            .get_pool(self.pool_id)
            .cloned()
            .ok_or(OSDClientError::PoolNotFound(self.pool_id))
    }

    /// List all pool-level snapshots, ordered by snap_id.
    pub async fn pool_snap_list(&self) -> Result<Vec<crate::osdclient::osdmap::PoolSnapInfo>> {
        let pool = self.get_pool_info().await?;
        Ok(pool.snaps.values().cloned().collect())
    }

    /// Resolve a snapshot name to its snap_id.
    ///
    /// Returns an error if no snapshot with the given name exists in this pool.
    pub async fn pool_snap_lookup(&self, name: &str) -> Result<SnapId> {
        let pool = self.get_pool_info().await?;
        pool.snaps
            .values()
            .find(|s| s.name == name)
            .map(|s| SnapId::from(s.snapid))
            .ok_or_else(|| {
                OSDClientError::Other(format!(
                    "snapshot '{}' not found in pool {}",
                    name, self.pool_id
                ))
            })
    }

    /// Resolve a snap_id to its [`PoolSnapInfo`](crate::osdclient::osdmap::PoolSnapInfo).
    ///
    /// Returns an error if the snap_id does not exist in this pool.
    pub async fn pool_snap_get_info(
        &self,
        snap_id: impl Into<SnapId>,
    ) -> Result<crate::osdclient::osdmap::PoolSnapInfo> {
        let snap_id = snap_id.into();
        let pool = self.get_pool_info().await?;
        pool.snaps.get(&snap_id.as_u64()).cloned().ok_or_else(|| {
            OSDClientError::Other(format!(
                "snap_id {} not found in pool {}",
                snap_id, self.pool_id
            ))
        })
    }

    /// Invoke an object class (CLS) method on an object.
    ///
    /// Calls a server-side Ceph object class plugin method co-located with the OSD.
    /// Matches `IoCtx::exec()` / `rados_exec()` in librados.
    ///
    /// # Arguments
    /// * `oid` - Object identifier
    /// * `class` - Object class name (e.g., `"lock"`, `"rbd"`, `"cas"`)
    /// * `method` - Method name within the class (e.g., `"lock"`, `"unlock"`)
    /// * `indata` - Serialised input data for the method (may be empty)
    ///
    /// # Returns
    /// The raw output bytes returned by the class method.
    pub async fn exec(
        &self,
        oid: impl Into<String>,
        class: &str,
        method: &str,
        indata: Bytes,
    ) -> Result<Bytes> {
        let oid_str = oid.into();
        debug!(
            "exec '{}::{}' on object '{}' in pool {}",
            class, method, oid_str, self.pool_id
        );

        let op = OpBuilder::new()
            .op(OSDOp::call(class, method, indata)?)
            .build();

        let result = self
            .client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;

        if result.result != 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: format!("exec {}::{} failed", class, method),
            });
        }

        let outdata = result
            .ops
            .first()
            .map(|op| op.outdata.clone())
            .unwrap_or_default();

        if let Some(op) = result.ops.first()
            && op.return_code != 0
        {
            return Err(OSDClientError::OSDError {
                code: op.return_code,
                message: format!("exec {}::{} failed", class, method),
            });
        }

        Ok(outdata)
    }

    /// Roll back an object to a pool snapshot (equivalent to `rados_ioctx_snap_rollback`).
    ///
    /// # Arguments
    /// * `oid` - Object name
    /// * `snap_id` - Snapshot ID to roll back to
    pub async fn snap_rollback(
        &self,
        oid: impl Into<String>,
        snap_id: impl Into<SnapId>,
    ) -> Result<()> {
        let snap_id = snap_id.into();
        let oid_str = oid.into();
        debug!(
            "Rolling back object '{}' in pool {} to snap_id {}",
            oid_str, self.pool_id, snap_id
        );

        let op = OpBuilder::new().rollback(snap_id).build();
        self.client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;
        Ok(())
    }
}

impl Clone for IoCtx {
    fn clone(&self) -> Self {
        // Create new OnceCell - clones will independently cache pool name
        Self {
            client: Arc::clone(&self.client),
            pool_id: self.pool_id,
            pool_name: tokio::sync::OnceCell::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_ioctx_pool_id() {
        // Basic test that will be expanded
    }
}
