//! `AsyncRead` + `AsyncWrite` + `AsyncSeek` adaptor for a single RADOS object.
//!
//! # Performance Notes
//!
//! - **No read-ahead**: each `poll_read` issues one OSD read RPC. For high-throughput
//!   streaming, wrap with [`tokio::io::BufReader`] using a large internal buffer.
//! - **No write coalescing**: each `poll_write` issues one OSD write RPC. For many
//!   small writes, wrap with [`tokio::io::BufWriter`].
//! - **`SeekFrom::End` requires a stat RPC**: avoid in hot-path loops; prefer
//!   `SeekFrom::Start` or `SeekFrom::Current` which resolve without network I/O.
//! - **Append mode**: RADOS append (`append_object`) is atomic but position-unknown.
//!   Use [`IoCtx::append`] directly for append-only workflows rather than this adaptor.

use crate::osdclient::error::OSDClientError;
use crate::osdclient::ioctx::IoCtx;
use bytes::Bytes;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use tokio::task::JoinHandle;

/// Poll a `JoinHandle<io::Result<T>>`, clearing it on completion.
///
/// Flattens the outer `JoinError` (task panicked or was aborted) and the inner
/// `io::Error` into a single `Poll<io::Result<T>>`.
fn poll_join<T>(
    cx: &mut Context<'_>,
    handle: &mut Option<JoinHandle<io::Result<T>>>,
) -> Poll<io::Result<T>> {
    let Some(h) = handle.as_mut() else {
        return Poll::Ready(Err(io::Error::other("no in-flight task")));
    };
    match Pin::new(h).poll(cx) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Err(join_err)) => {
            *handle = None;
            Poll::Ready(Err(io::Error::other(join_err)))
        }
        Poll::Ready(Ok(result)) => {
            *handle = None;
            Poll::Ready(result)
        }
    }
}

/// Tokio-compatible async adaptor over a single RADOS object.
///
/// Implements [`AsyncRead`], [`AsyncWrite`], and [`AsyncSeek`].
/// Each operation spawns the underlying [`IoCtx`] call in a [`tokio::spawn`] task,
/// bridging the poll-based trait interface with rados-rs's native async API.
///
/// # Notes
///
/// - `flush` is a no-op: RADOS writes are immediately durable on the OSD.
/// - Seeking past end-of-object is valid; subsequent reads return empty bytes.
/// - Only one read or write operation may be in-flight at a time. This is
///   consistent with how `tokio::io::BufReader`/`BufWriter` work.
///
/// # Example
///
/// ```no_run
/// use crate::osdclient::{IoCtx, RadosObject};
/// use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
/// use std::io::SeekFrom;
///
/// # async fn example(ioctx: IoCtx) -> std::io::Result<()> {
/// let mut obj = RadosObject::new(ioctx, "my-object".to_string());
///
/// obj.write_all(b"hello world").await?;
/// obj.seek(SeekFrom::Start(0)).await?;
///
/// let mut buf = Vec::new();
/// obj.read_to_end(&mut buf).await?;
/// assert_eq!(buf, b"hello world");
/// # Ok(())
/// # }
/// ```
pub struct RadosObject {
    ioctx: IoCtx,
    name: String,
    offset: u64,
    read_fut: Option<JoinHandle<io::Result<Bytes>>>,
    write_fut: Option<JoinHandle<io::Result<usize>>>,
    /// The SeekFrom target stored between `start_seek` and `poll_complete`.
    seek_target: Option<SeekFrom>,
    seek_fut: Option<JoinHandle<io::Result<u64>>>,
}

impl RadosObject {
    /// Create a new adaptor. The object need not exist yet.
    pub fn new(ioctx: IoCtx, name: String) -> Self {
        Self {
            ioctx,
            name,
            offset: 0,
            read_fut: None,
            write_fut: None,
            seek_target: None,
            seek_fut: None,
        }
    }

    /// Current byte offset within the object.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl AsyncRead for RadosObject {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = buf.remaining();
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        if self.read_fut.is_none() {
            let ioctx = self.ioctx.clone();
            let name = self.name.clone();
            let offset = self.offset;
            self.read_fut = Some(tokio::spawn(async move {
                match ioctx.read(&name, offset, remaining as u64).await {
                    Ok(result) => Ok(result.data),
                    // Object doesn't exist yet → return empty (EOF).
                    // The OSD returns ENOENT (-2) as OSDError; ObjectNotFound is
                    // used for higher-level lookups. Match both defensively.
                    Err(OSDClientError::ObjectNotFound(_)) => Ok(Bytes::new()),
                    Err(OSDClientError::OSDError {
                        code: crate::osdclient::error::ENOENT,
                        ..
                    }) => Ok(Bytes::new()),
                    Err(e) => Err(io::Error::other(e.to_string())),
                }
            }));
        }

        match poll_join(cx, &mut self.read_fut) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(data)) => {
                let n = data.len().min(buf.remaining());
                buf.put_slice(&data[..n]);
                self.offset += n as u64;
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl AsyncWrite for RadosObject {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.write_fut.is_none() {
            let ioctx = self.ioctx.clone();
            let name = self.name.clone();
            let offset = self.offset;
            let bytes = Bytes::copy_from_slice(data);
            let len = bytes.len();
            self.write_fut = Some(tokio::spawn(async move {
                ioctx
                    .write(&name, offset, bytes)
                    .await
                    .map(|_result| len)
                    .map_err(|e| io::Error::other(e.to_string()))
            }));
        }

        match poll_join(cx, &mut self.write_fut) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(n)) => {
                self.offset += n as u64;
                Poll::Ready(Ok(n))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Writes are immediately durable on the OSD — no client-side buffer to flush.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // No persistent connection to tear down per-object.
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for RadosObject {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        // Drop any in-flight stat task from a previous SeekFrom::End so its
        // result is not mistakenly applied to this new seek target.
        self.seek_fut = None;
        self.seek_target = Some(position);
        Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let target = match self.seek_target.take() {
            Some(t) => t,
            // No pending seek — return current offset.
            None => return Poll::Ready(Ok(self.offset)),
        };

        match target {
            SeekFrom::Start(n) => {
                self.offset = n;
                Poll::Ready(Ok(self.offset))
            }
            SeekFrom::Current(delta) => {
                let new_offset = (self.offset as i64).saturating_add(delta).max(0) as u64;
                self.offset = new_offset;
                Poll::Ready(Ok(self.offset))
            }
            // SeekFrom::End requires a stat RPC to learn the object size.
            SeekFrom::End(delta) => {
                if self.seek_fut.is_none() {
                    let ioctx = self.ioctx.clone();
                    let name = self.name.clone();
                    self.seek_fut = Some(tokio::spawn(async move {
                        ioctx
                            .stat(&name)
                            .await
                            .map(|stat| ((stat.size as i64).saturating_add(delta)).max(0) as u64)
                            .map_err(|e| io::Error::other(e.to_string()))
                    }));
                }

                match poll_join(cx, &mut self.seek_fut) {
                    Poll::Pending => {
                        // Restore target so poll_complete can retry next wake.
                        self.seek_target = Some(SeekFrom::End(delta));
                        Poll::Pending
                    }
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                    Poll::Ready(Ok(new_offset)) => {
                        self.offset = new_offset;
                        Poll::Ready(Ok(self.offset))
                    }
                }
            }
        }
    }
}
