use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use super::client::{BenchClient, BenchResult, StatInfo};
use super::fixture::{BenchEnv, PoolFixture};
use super::librados::IoCtx;

pub struct LibradosClient {
    io_ctx: Arc<IoCtx>,
}

impl LibradosClient {
    // `_env` is unused: the librados Rados handle and all its config were
    // already consumed by `PoolFixture`. The parameter is kept so both
    // adapters share the same `connect(env, ...)` shape at call sites.
    pub async fn connect(_env: &BenchEnv, pool: &PoolFixture) -> Result<Self> {
        let io_ctx = IoCtx::from_rados(Arc::clone(pool.rados()), pool.pool_name())
            .with_context(|| format!("IoCtx::from_rados({})", pool.pool_name()))?;
        Ok(Self {
            io_ctx: Arc::new(io_ctx),
        })
    }
}

#[async_trait]
impl BenchClient for LibradosClient {
    fn name(&self) -> &'static str {
        "librados"
    }

    async fn write_full(&self, oid: &str, data: Bytes) -> BenchResult<()> {
        self.io_ctx
            .write_full(oid, data.as_ref())
            .await
            .with_context(|| format!("librados write_full {oid}"))
    }

    async fn read(&self, oid: &str, len: usize) -> BenchResult<Bytes> {
        // Trait returns Bytes. librados fills a caller buffer, so we pay
        // one allocation + truncate here. Documented in the plan.
        let mut buf = vec![0u8; len];
        let n = self
            .io_ctx
            .read(oid, 0, &mut buf)
            .await
            .with_context(|| format!("librados read {oid}"))?;
        buf.truncate(n);
        Ok(Bytes::from(buf))
    }

    async fn stat(&self, oid: &str) -> BenchResult<StatInfo> {
        let (size, mtime) = self
            .io_ctx
            .stat(oid)
            .await
            .with_context(|| format!("librados stat {oid}"))?;
        // `mtime` is `time_t`; `i64` on Linux x86_64 but narrower on
        // 32-bit targets, so coerce portably.
        #[allow(clippy::unnecessary_cast)]
        let mtime_secs = mtime as i64;
        Ok(StatInfo { size, mtime_secs })
    }

    async fn remove(&self, oid: &str) -> BenchResult<()> {
        self.io_ctx
            .remove(oid)
            .await
            .with_context(|| format!("librados remove {oid}"))
    }
}
