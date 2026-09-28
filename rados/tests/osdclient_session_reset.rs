//! Session reset cluster test: an op whose connection drops after the
//! OSD applied it, with its reply unread, is sent again under its tid,
//! and the OSD answers it from its log rather than applying it again
//! (v19.2.6:src/osd/PrimaryLogPG.cc:2218-2244). Run with:
//!   CEPH_CONF=... cargo test -p rados --test osdclient_session_reset -- --ignored --nocapture

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::create_ioctx;

const BASE: &[u8] = b"base";
const APPENDED: &[u8] = b"+once";

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

/// The log lines of the thread it is installed on, kept to search.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log")).into_owned()
    }
}

/// Watch `oid` from a second client, on a thread and runtime of its own,
/// so it runs while the test's runtime is blocked, until the object's
/// size is `want` or `bound` passes. Returns once that client is up; the
/// thread's result is the size it saw last.
fn size_seen_by_another_client(
    oid: String,
    want: u64,
    bound: Duration,
) -> std::thread::JoinHandle<u64> {
    let (up_tx, up_rx) = std::sync::mpsc::channel();
    let observer = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async move {
                let b = create_ioctx().await.expect("client B");
                up_tx.send(()).expect("up");
                let deadline = Instant::now() + bound;
                loop {
                    let size = b.stat(&oid).await.expect("stat").size;
                    if size == want || Instant::now() >= deadline {
                        return size;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
    });
    up_rx.recv().expect("client B up");
    observer
}

/// `#[tokio::test]`'s runtime is single-threaded: blocking it keeps the
/// append's reply unread.
#[tokio::test]
#[ignore]
async fn an_append_whose_reply_was_lost_is_applied_once() {
    let log = Captured::default();
    let _log = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer({
                let log = log.clone();
                move || log.clone()
            })
            .finish(),
    );
    let a = create_ioctx().await.expect("client A");
    let oid = unique("reset-append");
    a.write_full(&oid, Bytes::from_static(BASE))
        .await
        .expect("create");
    let want = (BASE.len() + APPENDED.len()) as u64;
    let observer = size_seen_by_another_client(oid.clone(), want, Duration::from_secs(10));

    let append = {
        let a = a.clone();
        let oid = oid.clone();
        tokio::spawn(async move { a.append(&oid, Bytes::from_static(APPENDED)).await })
    };
    // Let the append go out. The OSD takes far longer to apply it and
    // reply than these few turns of the runtime. The append is small so
    // that its frame is out within them: what the socket has not taken
    // of a larger one when the runtime blocks is never sent.
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    // Block the runtime, so nothing reads the reply, until the other
    // client sees the append applied.
    let seen = observer.join().expect("client B");
    assert_eq!(seen, want, "the OSD applied the append");

    // The session leaves the client's map before its I/O task stops, so
    // the reply, read or not, reaches no op, and the append goes out
    // again on a new session.
    a.close_primary_session_for_test(oid.as_str())
        .await
        .expect("close the primary's session");
    tokio::time::timeout(Duration::from_secs(30), append)
        .await
        .expect("the append completes")
        .expect("ran")
        .expect("append");
    let log = log.text();
    assert!(
        log.contains("mid-flight"),
        "the append's reply was read before its session closed, so it was \
         not sent again:\n{log}"
    );

    let stat = a.stat(&oid).await.expect("stat");
    let data = a.read(&oid, 0, 64).await.expect("read").data;
    a.remove(&oid).await.expect("remove");
    assert_eq!(String::from_utf8_lossy(&data), "base+once", "appended once");
    assert_eq!(stat.size, want);
}
