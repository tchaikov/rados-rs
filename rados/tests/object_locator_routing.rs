//! Object locator routing cluster tests against Ceph v19.2.2: placement
//! by namespace, locator key and `ceph_stable_mod`, checked against the
//! monitor and the C++ `rados` CLI. Run with:
//!   CEPH_CONF=... cargo test -p rados --test object_locator_routing -- --ignored --nocapture
//!
//! `CEPH_EXEC` is the command prefix that runs the v19.2.2 `ceph` and
//! `rados` binaries (default `docker exec -i ceph-mon`).

mod common;

use std::collections::BTreeSet;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{build_test_client, create_ioctx, test_pool_name};
use futures::FutureExt;
use rados::crush::ObjectLocator;
use rados::{ALL_NSPACES, Client, IoCtx, ListObjectEntry, OSDClientError, WatchEvent, Watcher};

const ENOENT: i32 = 2;
const OP_BOUND: Duration = Duration::from_secs(10);

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

fn names(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("obj-{i:04}")).collect()
}

fn is_osd_error(err: &OSDClientError, errno: i32) -> bool {
    matches!(err, OSDClientError::OSDError { code, .. } if *code == -errno)
}

/// Await `fut`, failing within `OP_BOUND`: an op the OSD drops as
/// misplaced gets no reply, and would otherwise hang.
async fn within<T>(ns: &str, key: &str, name: &str, fut: impl Future<Output = T>) -> T {
    match tokio::time::timeout(OP_BOUND, fut).await {
        Ok(v) => v,
        Err(_) => panic!("no reply in {OP_BOUND:?} for nspace {ns:?} key {key:?} name {name:?}"),
    }
}

/// Run `body`, then remove every `(ioctx, name)` in `created` whether it
/// passed or panicked, then re-raise any panic.
async fn guarded(created: &[(IoCtx, String)], body: impl Future<Output = ()>) {
    let result = AssertUnwindSafe(body).catch_unwind().await;
    for (ioctx, name) in created {
        match tokio::time::timeout(OP_BOUND, ioctx.remove(name)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) if is_osd_error(&err, ENOENT) => {}
            Ok(Err(err)) => eprintln!("cleanup: removing {name}: {err:?}"),
            Err(_) => eprintln!("cleanup: removing {name}: no reply"),
        }
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn in_namespace(ioctx: &IoCtx, ns: &str) -> IoCtx {
    let mut ioctx = ioctx.clone();
    ioctx.set_namespace(ns);
    ioctx
}

fn with_key(ioctx: &IoCtx, key: &str) -> IoCtx {
    let mut ioctx = ioctx.clone();
    ioctx.set_locator_key(key);
    ioctx
}

fn parse_pgid(s: &str) -> u32 {
    let (_, seed) = s.split_once('.').expect("pgid is <pool>.<hex>");
    u32::from_str_radix(seed, 16).expect("hex pg seed")
}

/// The monitor's `osd map` answer for `object` in `nspace`: the map
/// epoch it answered from and (raw ps, pg).
async fn mon_osd_map(client: &Client, pool: &str, object: &str, nspace: &str) -> (u32, (u32, u32)) {
    let mut cmd = serde_json::json!({
        "prefix": "osd map",
        "pool": pool,
        "object": object,
        "format": "json",
    });
    if !nspace.is_empty() {
        cmd["nspace"] = nspace.into();
    }
    let result = client
        .mon_client()
        .invoke(vec![cmd.to_string()], Bytes::new())
        .await
        .expect("osd map");
    assert_eq!(result.retval, 0, "osd map: {}", result.outs);
    let reply: serde_json::Value = serde_json::from_slice(&result.outbl).expect("osd map json");
    let epoch = reply["epoch"].as_u64().expect("epoch");
    (
        u32::try_from(epoch).expect("epoch fits u32"),
        (
            parse_pgid(reply["raw_pgid"].as_str().expect("raw_pgid")),
            parse_pgid(reply["pgid"].as_str().expect("pgid")),
        ),
    )
}

/// The client's placement for `name` under `loc`: the epoch of the map
/// it placed it in and (raw ps, pg).
async fn client_map(client: &Client, name: &str, loc: &ObjectLocator) -> (u32, (u32, u32)) {
    let osdmap = client.osd_client().get_osdmap().await.expect("osdmap");
    let raw = osdmap.object_locator_to_pg(name, loc).expect("raw pg");
    (
        osdmap.epoch.as_u32(),
        (raw.seed, osdmap.raw_pg_to_pg(raw).expect("pg").seed),
    )
}

/// The monitor's placement of `mon_object` in `nspace` and the client's
/// of `name` under `loc`, as (want, got), both taken from one OSDMap
/// epoch: the client waits for the monitor's epoch, and the monitor is
/// asked again when the client's map has moved past its answer.
async fn placements(
    client: &Client,
    pool: &str,
    mon_object: &str,
    nspace: &str,
    name: &str,
    loc: &ObjectLocator,
) -> ((u32, u32), (u32, u32)) {
    let deadline = Instant::now() + OP_BOUND;
    loop {
        let (mon_epoch, want) = mon_osd_map(client, pool, mon_object, nspace).await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _ = client
            .osd_client()
            .wait_for_epoch(mon_epoch, remaining)
            .await;
        let (epoch, got) = client_map(client, name, loc).await;
        if epoch == mon_epoch {
            return (want, got);
        }
        assert!(
            Instant::now() < deadline,
            "{nspace:?}/{mon_object}: client epoch {epoch} never met the monitor's {mon_epoch}"
        );
    }
}

fn locator(pool: u64, key: &str, ns: &str) -> ObjectLocator {
    ObjectLocator {
        pool_id: pool,
        key: key.to_string(),
        namespace: ns.to_string(),
        hash: -1,
    }
}

/// Run the v19.2.2 C++ CLI as `$CEPH_EXEC <args>`, feeding it `stdin`.
async fn ceph_cli(args: &[&str], stdin: &[u8]) -> Vec<u8> {
    use tokio::io::AsyncWriteExt;
    let exec = std::env::var("CEPH_EXEC").unwrap_or_else(|_| "docker exec -i ceph-mon".into());
    let mut words = exec.split_whitespace();
    let mut cmd = tokio::process::Command::new(words.next().expect("CEPH_EXEC is empty"));
    cmd.args(words)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().expect("spawn CEPH_EXEC");
    let mut input = child.stdin.take().expect("stdin");
    input.write_all(stdin).await.expect("write stdin");
    drop(input);
    let out = child.wait_with_output().await.expect("CEPH_EXEC output");
    assert!(
        out.status.success(),
        "{exec} {args:?}: {}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

/// Every entry `ioctx` lists, across all pages.
async fn all_entries(ioctx: &IoCtx) -> Vec<ListObjectEntry> {
    let mut entries = Vec::new();
    let mut cursor = None;
    loop {
        let page = within(&format!("list {cursor:?}"), "", "", async {
            ioctx.list_object_entries(cursor.clone(), 1000).await
        })
        .await
        .expect("list_object_entries");
        entries.extend(page.entries);
        match page.cursor {
            Some(next) => cursor = Some(next),
            None => return entries,
        }
    }
}

async fn ack_next(watcher: &mut Watcher) {
    let event = tokio::time::timeout(OP_BOUND, watcher.recv())
        .await
        .expect("a watch event in time")
        .expect("the watch channel is open");
    match event {
        WatchEvent::Notify { notify_id, .. } => watcher
            .notify_ack(notify_id, Bytes::from_static(b"ack"))
            .await
            .expect("notify_ack"),
        other => panic!("expected a notify, got {other:?}"),
    }
}

/// Notify `name` from `notifier` and ack it on `watcher`: exactly one
/// ack, from `watcher`, and nobody missed.
async fn notify_acked(notifier: &IoCtx, watcher: &mut Watcher, ns: &str, key: &str, name: &str) {
    let cookie = watcher.cookie();
    let (result, ()) = within(ns, key, name, async {
        tokio::join!(
            notifier.notify(name, Bytes::from_static(b"ping"), 0),
            ack_next(watcher),
        )
    })
    .await;
    let result = result.expect("notify");
    assert!(!result.timed_out, "{name}: {result:?}");
    assert_eq!(result.acks.len(), 1, "{name}: {result:?}");
    assert_eq!(result.acks[0].cookie, cookie, "{name}");
    assert!(result.missed.is_empty(), "{name}: {result:?}");
}

#[tokio::test]
#[ignore]
async fn placement_matches_the_monitor() {
    let client = build_test_client().await.expect("client");
    let pool_name = test_pool_name();
    let pool = client
        .osd_client()
        .get_osdmap()
        .await
        .expect("osdmap")
        .pool_id_by_name(&pool_name)
        .expect("test pool");
    let own_ns = unique("olr-ns");
    for ns in ["", "ns1", own_ns.as_str()] {
        for name in names(64) {
            let (want, got) = placements(
                &client,
                &pool_name,
                &name,
                ns,
                &name,
                &locator(pool, "", ns),
            )
            .await;
            assert_eq!(got, want, "{ns:?}/{name}");
        }
    }
    for ns in ["", "ns1"] {
        for key in ["obj", "key-a", "key-b", "_shadow_obj.1", "e"] {
            let (want, got) = placements(
                &client,
                &pool_name,
                key,
                ns,
                "_multipart_x.1",
                &locator(pool, key, ns),
            )
            .await;
            assert_eq!(got, want, "{ns:?} key {key}");
        }
    }
}

#[tokio::test]
#[ignore]
async fn namespaced_objects_round_trip() {
    let base = create_ioctx().await.expect("ioctx");
    let ns = unique("olr-rt");
    let other_ns = unique("olr-rt-other");
    let ioctx = in_namespace(&base, &ns);
    let other = in_namespace(&base, &other_ns);
    let names = names(64);
    let created: Vec<(IoCtx, String)> = names
        .iter()
        .flat_map(|n| [(ioctx.clone(), n.clone()), (other.clone(), n.clone())])
        .collect();
    guarded(&created, async {
        for name in &names {
            within(
                &ns,
                "",
                name,
                ioctx.write_full(name, Bytes::from(name.clone())),
            )
            .await
            .expect("write_full");
            let read = within(&ns, "", name, ioctx.read(name, 0, 4096))
                .await
                .expect("read");
            assert_eq!(read.data, name.as_bytes());
            let stat = within(&ns, "", name, ioctx.stat(name)).await.expect("stat");
            assert_eq!(stat.size, name.len() as u64);
            let err = within("", "", name, base.stat(name))
                .await
                .expect_err("not in the default namespace");
            assert!(is_osd_error(&err, ENOENT), "{err:?}");
        }
        for name in &names {
            let payload = format!("other-{name}");
            within(
                &other_ns,
                "",
                name,
                other.write_full(name, Bytes::from(payload)),
            )
            .await
            .expect("write_full");
        }
        for name in &names {
            let mine = within(&ns, "", name, ioctx.read(name, 0, 4096))
                .await
                .expect("read");
            assert_eq!(mine.data, name.as_bytes());
            let theirs = within(&other_ns, "", name, other.read(name, 0, 4096))
                .await
                .expect("read");
            assert_eq!(theirs.data, format!("other-{name}").as_bytes());
        }
        for (ctx, name) in &created {
            within("", "", name, ctx.remove(name))
                .await
                .expect("remove");
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn keyed_objects_round_trip() {
    let client = build_test_client().await.expect("client");
    let pool_name = test_pool_name();
    let base = client.open_pool(&pool_name).await.expect("ioctx");
    let pool = base.pool_id();
    let key = unique("olr-key");
    let own_ns = unique("olr-key-ns");
    for ns in ["", own_ns.as_str()] {
        let ioctx = with_key(&in_namespace(&base, ns), &key);
        let names = names(64);
        let created: Vec<(IoCtx, String)> =
            names.iter().map(|n| (ioctx.clone(), n.clone())).collect();
        guarded(&created, async {
            for name in &names {
                let (want, got) = placements(
                    &client,
                    &pool_name,
                    &key,
                    ns,
                    name,
                    &locator(pool, &key, ns),
                )
                .await;
                assert_eq!(got, want, "{ns:?} key {key} name {name}");
                within(
                    ns,
                    &key,
                    name,
                    ioctx.write_full(name, Bytes::from(name.clone())),
                )
                .await
                .expect("write_full");
                let read = within(ns, &key, name, ioctx.read(name, 0, 4096))
                    .await
                    .expect("read");
                assert_eq!(read.data, name.as_bytes());
            }
            let first = &names[0];
            within(
                ns,
                &key,
                first,
                ioctx.set_xattr(first, "attr", Bytes::from_static(b"v")),
            )
            .await
            .expect("set_xattr");
            let value = within(ns, &key, first, ioctx.get_xattr(first, "attr"))
                .await
                .expect("get_xattr");
            assert_eq!(value, Bytes::from_static(b"v"));
            for name in &names {
                within(ns, &key, name, ioctx.remove(name))
                    .await
                    .expect("remove");
            }
        })
        .await;
    }
}

#[tokio::test]
#[ignore]
async fn watch_and_notify_in_a_namespace() {
    let ns = unique("olr-wn");
    let a = in_namespace(&create_ioctx().await.expect("client A"), &ns);
    let b = in_namespace(&create_ioctx().await.expect("client B"), &ns);
    let key = "olr-wn-key";
    let a_keyed = with_key(&a, key);
    let b_keyed = with_key(&b, key);
    let names = names(16);
    let keyed_name = "keyed-watch".to_string();
    let mut created: Vec<(IoCtx, String)> = names.iter().map(|n| (a.clone(), n.clone())).collect();
    created.push((a_keyed.clone(), keyed_name.clone()));
    guarded(&created, async {
        let mut watchers = Vec::new();
        for name in &names {
            within(&ns, "", name, a.write_full(name, Bytes::from_static(b"x")))
                .await
                .expect("write_full");
            let watcher = within(&ns, "", name, a.watch(name.as_str()))
                .await
                .expect("watch");
            watchers.push(watcher);
        }
        for (name, watcher) in names.iter().zip(watchers.iter_mut()) {
            notify_acked(&b, watcher, &ns, "", name).await;
            let listed = within(&ns, "", name, a.list_watchers(name.as_str()))
                .await
                .expect("list_watchers");
            assert!(
                listed.iter().any(|w| w.cookie == watcher.cookie()),
                "{name}: {listed:?}"
            );
        }

        within(
            &ns,
            key,
            &keyed_name,
            a_keyed.write_full(&keyed_name, Bytes::from_static(b"x")),
        )
        .await
        .expect("write_full");
        let mut keyed = within(&ns, key, &keyed_name, a_keyed.watch(keyed_name.as_str()))
            .await
            .expect("watch");
        notify_acked(&b_keyed, &mut keyed, &ns, key, &keyed_name).await;
        watchers.push(keyed);

        for watcher in watchers {
            within(&ns, "", "unwatch", watcher.unwatch())
                .await
                .expect("unwatch");
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn namespaced_watch_survives_a_session_reset() {
    let ns = unique("olr-reset");
    let a = in_namespace(&create_ioctx().await.expect("client A"), &ns);
    let b = in_namespace(&create_ioctx().await.expect("client B"), &ns);
    let name = "reset-watch".to_string();
    let created = vec![(a.clone(), name.clone())];
    guarded(&created, async {
        within(
            &ns,
            "",
            &name,
            a.write_full(&name, Bytes::from_static(b"x")),
        )
        .await
        .expect("write_full");
        let mut watcher = within(&ns, "", &name, a.watch(name.as_str()))
            .await
            .expect("watch");
        a.close_primary_session_for_test(name.as_str())
            .await
            .expect("close the primary's session");
        let started = Instant::now();
        notify_acked(&b, &mut watcher, &ns, "", &name).await;
        assert!(started.elapsed() < OP_BOUND, "{:?}", started.elapsed());
        watcher.check().expect("watch healthy after the reset");
        within(&ns, "", &name, watcher.unwatch())
            .await
            .expect("unwatch");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn listing_is_per_namespace() {
    let base = create_ioctx().await.expect("ioctx");
    let ns_a = unique("olr-list-a");
    let ns_b = unique("olr-list-b");
    let a = in_namespace(&base, &ns_a);
    let b = in_namespace(&base, &ns_b);
    let key = "olr-list-key";
    let a_keyed = with_key(&a, key);
    let keyed_name = "keyed-entry".to_string();

    let shared: Vec<String> = (0..4).map(|i| format!("shared-{i}")).collect();
    let a_names: Vec<String> = shared
        .iter()
        .cloned()
        .chain((0..4).map(|i| format!("a-only-{i}")))
        .collect();
    let b_names: Vec<String> = shared
        .iter()
        .cloned()
        .chain((0..4).map(|i| format!("b-only-{i}")))
        .collect();
    let mut created: Vec<(IoCtx, String)> =
        a_names.iter().map(|n| (a.clone(), n.clone())).collect();
    created.extend(b_names.iter().map(|n| (b.clone(), n.clone())));
    created.push((a_keyed.clone(), keyed_name.clone()));

    guarded(&created, async {
        for (ctx, name) in &created {
            within(
                "",
                "",
                name,
                ctx.write_full(name, Bytes::from(name.clone())),
            )
            .await
            .expect("write_full");
        }

        let mut want_a: BTreeSet<String> = a_names.iter().cloned().collect();
        want_a.insert(keyed_name.clone());
        let got_a: BTreeSet<String> = within(&ns_a, "", "ls", a.ls())
            .await
            .expect("ls")
            .into_iter()
            .collect();
        assert_eq!(got_a, want_a);
        for entry in all_entries(&a).await {
            assert_eq!(entry.nspace, ns_a, "{entry:?}");
            let want_key = if entry.oid == keyed_name { key } else { "" };
            assert_eq!(entry.locator, want_key, "{entry:?}");
        }

        let want_b: BTreeSet<String> = b_names.iter().cloned().collect();
        let got_b: BTreeSet<String> = within(&ns_b, "", "ls", b.ls())
            .await
            .expect("ls")
            .into_iter()
            .collect();
        assert_eq!(got_b, want_b);
        for entry in all_entries(&b).await {
            assert_eq!(entry.nspace, ns_b, "{entry:?}");
            assert_eq!(entry.locator, "", "{entry:?}");
        }

        let default: BTreeSet<String> = within("", "", "ls", base.ls())
            .await
            .expect("ls")
            .into_iter()
            .collect();
        for name in want_a.iter().chain(&want_b) {
            assert!(
                !default.contains(name),
                "{name} listed in the default namespace"
            );
        }

        let everything: BTreeSet<(String, String, String)> =
            all_entries(&in_namespace(&base, ALL_NSPACES))
                .await
                .into_iter()
                .map(|e| (e.nspace, e.oid, e.locator))
                .collect();
        for name in &a_names {
            let entry = (ns_a.clone(), name.clone(), String::new());
            assert!(everything.contains(&entry), "{entry:?}");
        }
        for name in &b_names {
            let entry = (ns_b.clone(), name.clone(), String::new());
            assert!(everything.contains(&entry), "{entry:?}");
        }
        let keyed_entry = (ns_a.clone(), keyed_name.clone(), key.to_string());
        assert!(everything.contains(&keyed_entry), "{keyed_entry:?}");
    })
    .await;
}

/// Wait (bounded) until the client's map has `pool` at `pg_num` 12 and
/// the mgr reports all twelve of its PGs active.
async fn wait_for_pool(client: &Client, pool: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let osdmap = client
            .osd_client()
            .wait_for_latest_osdmap(Duration::from_secs(5))
            .await
            .expect("osdmap");
        let in_map = osdmap
            .pool_id_by_name(pool)
            .and_then(|id| osdmap.get_pool(id))
            .is_some_and(|p| p.pg_num == 12 && p.pgp_num == 12);
        if in_map {
            let out = ceph_cli(&["ceph", "pg", "ls-by-pool", pool, "-f", "json"], b"").await;
            let pgs: serde_json::Value = serde_json::from_slice(&out).expect("pg ls json");
            let stats = pgs["pg_stats"].as_array().cloned().unwrap_or_default();
            let active = stats
                .iter()
                .filter(|pg| pg["state"].as_str().is_some_and(|s| s.contains("active")))
                .count();
            if stats.len() == 12 && active == 12 {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "pool {pool} not active at pg_num 12"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
#[ignore]
async fn stable_mod_matches_the_cpp_client() {
    let client = build_test_client().await.expect("client");
    let pool = unique("olr-pg12");
    let cmd = serde_json::json!({
        "prefix": "osd pool create",
        "pool": pool,
        "pg_num": 12,
        "pgp_num": 12,
        "autoscale_mode": "off",
    });
    let created = client
        .mon_client()
        .invoke(vec![cmd.to_string()], Bytes::new())
        .await
        .expect("osd pool create");
    assert_eq!(created.retval, 0, "osd pool create: {}", created.outs);

    let body = AssertUnwindSafe(async {
        wait_for_pool(&client, &pool).await;
        let ioctx = client.open_pool(&pool).await.expect("open pool");
        let pool_id = ioctx.pool_id();
        let mut all: Vec<String> = ["bar", "obj", "_shadow_obj.1", "e", "c"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        all.extend(names(64));

        for name in &all {
            let (want, got) =
                placements(&client, &pool, name, "", name, &locator(pool_id, "", "")).await;
            assert_eq!(got, want, "{name}");
        }
        assert_eq!(
            client_map(&client, "e", &locator(pool_id, "", ""))
                .await
                .1
                .1,
            6
        );

        for name in &all {
            within(
                "",
                "",
                name,
                ioctx.write_full(name, Bytes::from(name.clone())),
            )
            .await
            .expect("write_full");
            let got = ceph_cli(&["rados", "-p", &pool, "get", name, "-"], b"").await;
            assert_eq!(got, name.as_bytes(), "C++ read of {name}");
        }
        for name in &all {
            let cli_name = format!("cli-{name}");
            ceph_cli(
                &["rados", "-p", &pool, "put", &cli_name, "-"],
                name.as_bytes(),
            )
            .await;
            let read = within("", "", &cli_name, ioctx.read(&cli_name, 0, 4096))
                .await
                .expect("read");
            assert_eq!(read.data, name.as_bytes(), "rados-rs read of {cli_name}");
        }
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
