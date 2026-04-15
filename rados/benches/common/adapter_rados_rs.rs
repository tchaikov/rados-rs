use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use rados::{Client, IoCtx};

use super::client::{BenchClient, BenchResult, StatInfo};
use super::fixture::BenchEnv;

pub struct RadosRsClient {
    ioctx: Arc<IoCtx>,
    _client: Client,
}

impl RadosRsClient {
    pub async fn connect(env: &BenchEnv, pool_name: &str) -> Result<Self> {
        let client = Client::builder()
            .entity_name(env.ceph_user.clone())
            .config_file(std::path::PathBuf::from(&env.ceph_conf))
            .build()
            .await
            .context("rados::Client::builder().build()")?;

        let ioctx = client
            .open_pool(pool_name)
            .await
            .with_context(|| format!("rados-rs open_pool({pool_name})"))?;

        Ok(Self {
            ioctx: Arc::new(ioctx),
            _client: client,
        })
    }
}

#[async_trait]
impl BenchClient for RadosRsClient {
    fn name(&self) -> &'static str {
        "rados-rs"
    }

    async fn write_full(&self, oid: &str, data: Bytes) -> BenchResult<()> {
        self.ioctx
            .write_full(oid, data)
            .await
            .with_context(|| format!("rados-rs write_full {oid}"))?;
        Ok(())
    }

    async fn read(&self, oid: &str, len: usize) -> BenchResult<Bytes> {
        let result = self
            .ioctx
            .read(oid, 0, len as u64)
            .await
            .with_context(|| format!("rados-rs read {oid}"))?;
        Ok(result.data)
    }

    async fn stat(&self, oid: &str) -> BenchResult<StatInfo> {
        let stat = self
            .ioctx
            .stat(oid)
            .await
            .with_context(|| format!("rados-rs stat {oid}"))?;
        let mtime_secs = stat
            .mtime
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        Ok(StatInfo {
            size: stat.size,
            mtime_secs,
        })
    }

    async fn remove(&self, oid: &str) -> BenchResult<()> {
        self.ioctx
            .remove(oid)
            .await
            .with_context(|| format!("rados-rs remove {oid}"))?;
        Ok(())
    }
}
