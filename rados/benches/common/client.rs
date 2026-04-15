use async_trait::async_trait;
use bytes::Bytes;

pub type BenchResult<T> = anyhow::Result<T>;

#[async_trait]
pub trait BenchClient: Send + Sync + 'static {
    /// Library name used for reporting (e.g. "rados-rs", "librados").
    fn name(&self) -> &'static str;

    async fn write_full(&self, oid: &str, data: Bytes) -> BenchResult<()>;

    async fn read(&self, oid: &str, len: usize) -> BenchResult<Bytes>;

    async fn stat(&self, oid: &str) -> BenchResult<StatInfo>;

    async fn remove(&self, oid: &str) -> BenchResult<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct StatInfo {
    pub size: u64,
    pub mtime_secs: i64,
}
