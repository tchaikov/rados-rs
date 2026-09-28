//! PG split and merge cluster test against Ceph v19.2.2: ops in flight
//! while a pool's PGs merge and then split must all complete, as the OSD
//! drops an op sent before its PG split or merged and only the client's
//! resend recovers it. Run with:
//!   CEPH_CONF=... cargo test -p rados --test osdclient_split_merge -- --ignored --nocapture
//!
//! `CEPH_EXEC` is the command prefix that runs the cluster's `ceph` binary
//! (default `docker exec -i ceph-mon`); see `common` for a rooket cluster's.

mod common;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::build_test_client;
use futures::FutureExt;
use rados::{Client, IoCtx, WatchEvent, Watcher};

/// Every op must complete within this, below the Tracker's 30 s
/// operation timeout, so an op the OSD dropped fails the test rather
/// than recovering through the timeout.
const OP_BOUND: Duration = Duration::from_secs(20);
/// How long a pg_num change may take to land in the client's map.
const PG_NUM_BOUND: Duration = Duration::from_secs(75);
const OBJECTS: usize = 96;
/// Each write's size: large enough that ops queue at the OSD.
const PAYLOAD: usize = 64 * 1024;
const WATCHED: usize = 8;
/// The OSD's own notify timeout; a watcher caught mid-reconnect is
/// reported missed after it rather than failing the notify.
const NOTIFY_TIMEOUT_MS: u64 = 5000;

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

/// Run the v19.2.2 C++ CLI as `$CEPH_EXEC <args>` and return its stdout.
async fn ceph_cli(args: &[&str]) -> Vec<u8> {
    let exec = std::env::var("CEPH_EXEC").unwrap_or_else(|_| "docker exec -i ceph-mon".into());
    let mut words = exec.split_whitespace();
    let out = tokio::process::Command::new(words.next().expect("CEPH_EXEC is empty"))
        .args(words)
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .expect("CEPH_EXEC output");
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

async fn mon_command(client: &Client, cmd: serde_json::Value) {
    let result = client
        .mon_client()
        .invoke(vec![cmd.to_string()], Bytes::new())
        .await
        .expect("mon command");
    assert_eq!(result.retval, 0, "{cmd}: {}", result.outs);
}

/// The pool's pg_num in the client's latest map.
async fn pg_num(client: &Client, pool: &str) -> u32 {
    let osdmap = client
        .osd_client()
        .wait_for_latest_osdmap(Duration::from_secs(5))
        .await
        .expect("osdmap");
    osdmap
        .pool_id_by_name(pool)
        .and_then(|id| osdmap.get_pool(id))
        .map(|p| p.pg_num)
        .expect("pool in map")
}

/// Wait until the pool's pg_num in the client's map is `want`.
async fn wait_for_pg_num(client: &Client, pool: &str, want: u32) {
    let deadline = Instant::now() + PG_NUM_BOUND;
    loop {
        let now = pg_num(client, pool).await;
        if now == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "pool {pool} still at pg_num {now}, not {want}, after {PG_NUM_BOUND:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until all `pgs` PGs of the pool are active.
async fn wait_for_active(pool: &str, pgs: usize) {
    let deadline = Instant::now() + PG_NUM_BOUND;
    loop {
        let out = ceph_cli(&["ceph", "pg", "ls-by-pool", pool, "-f", "json"]).await;
        let listed: serde_json::Value = serde_json::from_slice(&out).expect("pg ls json");
        let stats = listed["pg_stats"].as_array().cloned().unwrap_or_default();
        let active = stats
            .iter()
            .filter(|pg| pg["state"].as_str().is_some_and(|s| s.contains("active")))
            .count();
        if stats.len() == pgs && active == pgs {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "pool {pool}: {active} of {pgs} PGs active"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Ops that failed or did not complete within `OP_BOUND`, described.
type Failures = std::sync::Mutex<Vec<String>>;

/// Run `fut` within `OP_BOUND`, recording a failure if it errs or runs
/// out of time.
async fn bounded<T, E: std::fmt::Debug>(
    failures: &Failures,
    op: &'static str,
    object: &str,
    fut: impl Future<Output = Result<T, E>>,
) -> Option<T> {
    let started = Instant::now();
    let error = match tokio::time::timeout(OP_BOUND, fut).await {
        Ok(Ok(v)) => return Some(v),
        Ok(Err(e)) => format!("{e:?}"),
        Err(_) => "no reply".to_string(),
    };
    let after = started.elapsed();
    failures
        .lock()
        .unwrap()
        .push(format!("{op} {object} after {after:?}: {error}"));
    None
}

/// Object `i`'s handle: in the test's namespace, under a locator key of
/// its own so the objects spread over the PGs.
fn object_ctx(base: &IoCtx, i: usize) -> (IoCtx, String) {
    let mut ioctx = base.clone();
    ioctx.set_locator_key(format!("key-{i}"));
    (ioctx, format!("obj-{i}"))
}

/// Ack every notify the watch sees until `stop`.
async fn ack_notifies(mut watcher: Watcher, stop: Arc<AtomicBool>, disconnects: Arc<AtomicU64>) {
    while !stop.load(Ordering::Relaxed) {
        match tokio::time::timeout(Duration::from_millis(500), watcher.recv()).await {
            Ok(Some(WatchEvent::Notify { notify_id, .. })) => {
                let _ = watcher.notify_ack(notify_id, Bytes::new()).await;
            }
            Ok(Some(WatchEvent::Disconnect { code })) => {
                eprintln!("watch {} disconnected: {code}", watcher.cookie());
                disconnects.fetch_add(1, Ordering::Relaxed);
            }
            Ok(None) => return,
            Err(_) => {}
        }
    }
    let _ = watcher.unwatch().await;
}

/// Write, read back and, on a watched object, notify object `i` until
/// `stop`. Returns the number of rounds.
async fn worker(base: IoCtx, i: usize, stop: Arc<AtomicBool>, failures: Arc<Failures>) -> u64 {
    let (ioctx, name) = object_ctx(&base, i);
    let mut rounds = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let tag = format!("{name}-{rounds}-");
        let payload: Vec<u8> = tag.bytes().cycle().take(PAYLOAD).collect();
        let wrote = bounded(
            &failures,
            "write_full",
            &name,
            ioctx.write_full(&name, Bytes::from(payload.clone())),
        )
        .await;
        if wrote.is_some()
            && let Some(read) = bounded(
                &failures,
                "read",
                &name,
                ioctx.read(&name, 0, PAYLOAD as u64),
            )
            .await
            && read.data != payload
        {
            failures.lock().unwrap().push(format!(
                "read {name}: got {} bytes, not the {} of round {rounds}",
                read.data.len(),
                payload.len()
            ));
        }
        if i < WATCHED {
            bounded(
                &failures,
                "notify",
                &name,
                ioctx.notify(&name, Bytes::from(tag), NOTIFY_TIMEOUT_MS),
            )
            .await;
        }
        rounds += 1;
    }
    rounds
}

#[tokio::test]
#[ignore]
async fn ops_complete_across_a_merge_and_a_split() {
    common::init_tracing();
    let client = build_test_client().await.expect("client");
    let pool = unique("split-merge");
    mon_command(
        &client,
        serde_json::json!({
            "prefix": "osd pool create",
            "pool": pool,
            "pg_num": 8,
            "pgp_num": 8,
            "autoscale_mode": "off",
        }),
    )
    .await;

    let body = AssertUnwindSafe(async {
        wait_for_pg_num(&client, &pool, 8).await;
        wait_for_active(&pool, 8).await;
        let mut base = client.open_pool(&pool).await.expect("open pool");
        base.set_namespace(unique("sm-ns"));

        let failures = Arc::new(Failures::default());
        let stop = Arc::new(AtomicBool::new(false));
        let disconnects = Arc::new(AtomicU64::new(0));
        let mut ackers = Vec::new();
        for i in 0..WATCHED {
            let (ioctx, name) = object_ctx(&base, i);
            ioctx
                .write_full(&name, Bytes::from_static(b"seed"))
                .await
                .expect("create watched object");
            let watcher = ioctx.watch(&name).await.expect("watch");
            ackers.push(tokio::spawn(ack_notifies(
                watcher,
                Arc::clone(&stop),
                Arc::clone(&disconnects),
            )));
        }
        let workers: Vec<_> = (0..OBJECTS)
            .map(|i| {
                tokio::spawn(worker(
                    base.clone(),
                    i,
                    Arc::clone(&stop),
                    Arc::clone(&failures),
                ))
            })
            .collect();

        let started = Instant::now();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let set_pg_num = |n: u32| {
            serde_json::json!({
                "prefix": "osd pool set",
                "pool": pool,
                "var": "pg_num",
                "val": n.to_string(),
            })
        };
        mon_command(&client, set_pg_num(6)).await;
        wait_for_pg_num(&client, &pool, 6).await;
        eprintln!("merged to 6 PGs after {:?}", started.elapsed());
        tokio::time::sleep(Duration::from_secs(2)).await;
        mon_command(&client, set_pg_num(64)).await;
        wait_for_pg_num(&client, &pool, 64).await;
        eprintln!("split to 64 PGs after {:?}", started.elapsed());
        tokio::time::sleep(Duration::from_secs(5)).await;

        stop.store(true, Ordering::Relaxed);
        let mut rounds = 0;
        for w in workers {
            rounds += w.await.expect("worker");
        }
        for a in ackers {
            a.await.expect("acker");
        }
        let failures = failures.lock().unwrap();
        eprintln!(
            "{rounds} rounds over {OBJECTS} objects in {:?}, {} watch disconnects, {} failures",
            started.elapsed(),
            disconnects.load(Ordering::Relaxed),
            failures.len()
        );
        assert!(rounds > 0, "no op ran");
        assert!(
            failures.is_empty(),
            "ops that failed or took over {OP_BOUND:?}: {failures:#?}"
        );
    })
    .catch_unwind()
    .await;

    if let Err(err) = client.osd_client().delete_pool(&pool, true).await {
        eprintln!("cleanup: deleting pool {pool}: {err:?}");
    }
    if let Err(panic) = body {
        std::panic::resume_unwind(panic);
    }
}

const LISTED: usize = 200;
/// Entries per listing call: more than there are, so one call walks
/// every PG, one PGNLS page each, and a split can land between its pages.
const LIST_PAGE: usize = 2 * LISTED;

/// List the whole namespace of `ioctx`, each call within `OP_BOUND`.
/// Returns the names, or `None` on a failed call.
async fn list_all(ioctx: &IoCtx, failures: &Failures) -> Option<Vec<String>> {
    let mut names = Vec::new();
    let mut cursor = None;
    loop {
        let (page, next) = bounded(
            failures,
            "list",
            "the namespace",
            ioctx.list_objects(cursor.clone(), LIST_PAGE),
        )
        .await?;
        names.extend(page);
        match next {
            Some(next) => cursor = Some(next),
            None => return Some(names),
        }
    }
}

#[tokio::test]
#[ignore]
async fn a_listing_completes_across_a_split() {
    common::init_tracing();
    let client = build_test_client().await.expect("client");
    let pool = unique("split-list");
    mon_command(
        &client,
        serde_json::json!({
            "prefix": "osd pool create",
            "pool": pool,
            "pg_num": 8,
            "pgp_num": 8,
            "autoscale_mode": "off",
        }),
    )
    .await;

    let lister_abort = std::sync::Mutex::new(None);
    let body = AssertUnwindSafe(async {
        wait_for_pg_num(&client, &pool, 8).await;
        wait_for_active(&pool, 8).await;
        let mut ioctx = client.open_pool(&pool).await.expect("open pool");
        ioctx.set_namespace(unique("sl-ns"));
        let mut want = std::collections::BTreeSet::new();
        for i in 0..LISTED {
            let name = format!("obj-{i}");
            ioctx
                .write_full(&name, Bytes::from_static(b"x"))
                .await
                .expect("write");
            want.insert(name);
        }

        // List the namespace over and over while the pool splits.
        let failures = Arc::new(Failures::default());
        let stop = Arc::new(AtomicBool::new(false));
        let lister = {
            let (ioctx, failures, stop, want) = (
                ioctx.clone(),
                Arc::clone(&failures),
                Arc::clone(&stop),
                want.clone(),
            );
            tokio::spawn(async move {
                let mut listings = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let Some(names) = list_all(&ioctx, &failures).await else {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };
                    let count = names.len();
                    let got: std::collections::BTreeSet<_> = names.into_iter().collect();
                    if got != want || count != want.len() {
                        failures.lock().unwrap().push(format!(
                            "listing {listings}: {count} names, {} missing, {} extra",
                            want.difference(&got).count(),
                            got.difference(&want).count()
                        ));
                    }
                    listings += 1;
                }
                listings
            })
        };
        *lister_abort.lock().unwrap() = Some(lister.abort_handle());

        let started = Instant::now();
        tokio::time::sleep(Duration::from_secs(2)).await;
        mon_command(
            &client,
            serde_json::json!({
                "prefix": "osd pool set",
                "pool": pool,
                "var": "pg_num",
                "val": "64",
            }),
        )
        .await;
        wait_for_pg_num(&client, &pool, 64).await;
        eprintln!("split to 64 PGs after {:?}", started.elapsed());
        tokio::time::sleep(Duration::from_secs(5)).await;
        stop.store(true, Ordering::Relaxed);
        let listings = lister.await.expect("lister");

        // And once more on the split pool.
        let names = list_all(&ioctx, &failures).await;
        let failures = failures.lock().unwrap();
        eprintln!(
            "{listings} listings in {:?}, {} failures",
            started.elapsed(),
            failures.len()
        );
        assert!(listings > 0, "no listing ran");
        assert!(failures.is_empty(), "{failures:#?}");
        let names = names.expect("listed");
        assert_eq!(names.len(), want.len(), "duplicates listed");
        let got: std::collections::BTreeSet<_> = names.into_iter().collect();
        assert_eq!(got, want);
    })
    .catch_unwind()
    .await;

    // A panic leaves the lister running; stop it before the pool goes.
    if let Some(lister) = lister_abort.lock().unwrap().take() {
        lister.abort();
    }

    if let Err(err) = client.osd_client().delete_pool(&pool, true).await {
        eprintln!("cleanup: deleting pool {pool}: {err:?}");
    }
    if let Err(panic) = body {
        std::panic::resume_unwind(panic);
    }
}
