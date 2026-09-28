//! Captures the cephx known answers `cephx_transcript_kat` checks: a fresh
//! aes256k entity authenticates to the mon in CRC and SECURE mode and to
//! `osd.0` over raw msgr2 connections, every auth payload is recorded on the
//! wire, and the session keys are taken from the mon's own log, so the
//! expected keys do not come from rados-rs.
//!
//! Ignored and in no CI list. It skips unless these are set:
//! - `CEPHX_CAPTURE_OUT`: the directory the fixtures are written to;
//! - `CEPHX_CAPTURE_RELEASE`: the cluster's Ceph release, e.g. `19.2.6`;
//! - `CEPHX_MON_LOG_CMD`: a command prefix, split on whitespace, that prints
//!   the mon's log, e.g.
//!   `rooket k -n rook-ceph logs deploy/rook-ceph-mon-a -c mon --since=10m`.
//!
//! The mon logs session keys only at `debug_auth` 10, and at that level it
//! also logs the long-term key of every entity that authenticates to it, so
//! raise the level only around the run and restore it afterwards. This tool
//! never prints, stores or returns a log line: it keeps the session keys of
//! its own entity's sessions and nothing else. See `common` for running it
//! against a rooket cluster.
//!
//!   CEPH_CONF=... CEPHX_CAPTURE_OUT=... CEPHX_CAPTURE_RELEASE=... \
//!   CEPHX_MON_LOG_CMD=... cargo test -p rados --test cephx_capture -- --ignored --nocapture

mod cephx;
mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::FutureExt;
use rados::auth::protocol::{CEPH_CON_MODE_CRC, CEPH_CON_MODE_SECURE};
use rados::auth::{
    AuthProvider, CephXClientHandler, MonitorAuthProvider, Result as AuthResult,
    ServiceAuthProvider,
};
use rados::msgr2::{Connection, ConnectionConfig, ConnectionMode};
use rados::{CephConfig, Client, EntityType};
use serde_json::json;

const LOG_WAIT: Duration = Duration::from_secs(20);
const LOG_POLL: Duration = Duration::from_secs(2);
const LOG_MARKER: &str = "build_service_ticket service session_auth_info(";

/// One auth payload on the wire: `out` from `build_auth_payload`, `in` into
/// `handle_auth_response`.
#[derive(Debug, Clone)]
struct Recorded {
    direction: &'static str,
    con_mode: u32,
    global_id: u64,
    payload: Bytes,
}

type Log = Arc<Mutex<Vec<Recorded>>>;

/// An `AuthProvider` that delegates to `P` and records every payload. Its
/// clones share the log.
#[derive(Debug, Clone)]
struct Recorder<P> {
    inner: P,
    log: Log,
}

impl<P> Recorder<P> {
    fn new(inner: P) -> (Self, Log) {
        let log = Log::default();
        (
            Self {
                inner,
                log: Arc::clone(&log),
            },
            log,
        )
    }

    fn record(&self, direction: &'static str, con_mode: u32, global_id: u64, payload: &Bytes) {
        self.log.lock().expect("recorder log").push(Recorded {
            direction,
            con_mode,
            global_id,
            payload: payload.clone(),
        });
    }
}

impl<P: AuthProvider + Clone + 'static> AuthProvider for Recorder<P> {
    fn build_auth_payload(&mut self, global_id: u64, service_id: u32) -> AuthResult<Bytes> {
        let payload = self.inner.build_auth_payload(global_id, service_id)?;
        self.record("out", 0, global_id, &payload);
        Ok(payload)
    }

    fn handle_auth_response(
        &mut self,
        payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> AuthResult<(Option<Bytes>, Option<Bytes>)> {
        self.record("in", con_mode, global_id, &payload);
        self.inner
            .handle_auth_response(payload, global_id, con_mode)
    }

    fn has_valid_ticket(&self, service_id: u32) -> bool {
        self.inner.has_valid_ticket(service_id)
    }

    fn clone_box(&self) -> Box<dyn AuthProvider> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

type SharedHandler = Arc<Mutex<CephXClientHandler>>;

/// A completed mon session: its handler, global_id, mode and transcript.
struct MonSession {
    handler: SharedHandler,
    global_id: u64,
    con_mode: u32,
    transcript: Vec<Recorded>,
}

struct Capture {
    out: PathBuf,
    release: String,
    log_cmd: String,
}

impl Capture {
    fn from_env() -> Option<Self> {
        let (Ok(out), Ok(log_cmd)) = (
            std::env::var("CEPHX_CAPTURE_OUT"),
            std::env::var("CEPHX_MON_LOG_CMD"),
        ) else {
            return None;
        };
        let release = std::env::var("CEPHX_CAPTURE_RELEASE")
            .expect("CEPHX_CAPTURE_RELEASE is required, e.g. 19.2.6");
        Some(Self {
            out: out.into(),
            release,
            log_cmd,
        })
    }
}

fn service_name(service: EntityType) -> &'static str {
    match service {
        EntityType::AUTH => "auth",
        EntityType::MON => "mon",
        EntityType::OSD => "osd",
        EntityType::MGR => "mgr",
        EntityType::MDS => "mds",
        other => panic!("no service name for {other:?}"),
    }
}

/// The services `handler`'s session holds tickets for, by C++ entity type
/// name.
fn stored_services(handler: &SharedHandler) -> BTreeSet<&'static str> {
    let handler = handler.lock().expect("handler lock");
    handler
        .get_session()
        .expect("session after AUTH_DONE")
        .ticket_handlers
        .keys()
        .map(|service| service_name(*service))
        .collect()
}

fn mon_addr() -> SocketAddr {
    let conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".to_owned());
    let addr = CephConfig::from_file(&conf)
        .expect("ceph.conf")
        .first_v2_mon_addr_for("client.admin")
        .expect("a v2 mon address in ceph.conf");
    addr.strip_prefix("v2:")
        .expect("v2: prefix")
        .parse()
        .expect("mon address")
}

/// The base64 `key = ` value of `entity`'s keyring.
fn entity_key(entity: &cephx::Entity) -> String {
    let text = std::fs::read_to_string(entity.keyring.path()).expect("read keyring");
    text.lines()
        .filter_map(|line| line.trim().strip_prefix("key"))
        .filter_map(|rest| rest.trim_start().strip_prefix('='))
        .map(|key| key.trim().to_owned())
        .next()
        .expect("key in keyring")
}

async fn mon_session(entity: &cephx::Entity, key: &str, mode: ConnectionMode) -> MonSession {
    let mut provider = MonitorAuthProvider::new(&entity.name).expect("mon auth provider");
    provider
        .set_secret_key_from_base64(key)
        .expect("entity key");
    let handler = Arc::clone(provider.handler());
    let (recorder, log) = Recorder::new(provider);
    let config = ConnectionConfig {
        preferred_modes: vec![mode],
        entity_name: entity.name.parse().expect("entity name"),
        ..ConnectionConfig::with_auth_provider(Box::new(recorder))
    };
    let mut conn = Connection::connect(mon_addr(), config)
        .await
        .expect("connect to the mon");
    conn.establish_session()
        .await
        .unwrap_or_else(|e| panic!("mon session in {mode:?} mode: {e}"));
    let global_id = conn.global_id();
    conn.close().await;

    let transcript = log.lock().expect("recorder log").clone();
    let done = transcript.last().expect("a recorded AUTH_DONE");
    assert_eq!(done.direction, "in", "the transcript ends with AUTH_DONE");
    assert_eq!(done.global_id, global_id, "AUTH_DONE's global_id");
    assert_eq!(
        done.con_mode,
        u32::from(mode),
        "AUTH_DONE's connection mode"
    );
    MonSession {
        handler,
        global_id,
        con_mode: done.con_mode,
        transcript,
    }
}

/// Authenticate to `osd.0` with `session`'s tickets and return its
/// transcript: authorizer 1, the challenge, authorizer 2 and the reply.
async fn osd_session(
    admin: &Client,
    entity: &cephx::Entity,
    session: &MonSession,
) -> Vec<Recorded> {
    let osdmap = admin.osd_client().get_osdmap().await.expect("osdmap");
    let entity_addr = osdmap
        .osd_addrs_client
        .first()
        .and_then(|addrs| addrs.get_msgr2())
        .cloned()
        .expect("osd.0's v2 address");
    let addr = entity_addr.to_socket_addr().expect("osd.0 socket address");

    let provider = ServiceAuthProvider::from_shared_handler(Arc::clone(&session.handler));
    let (recorder, log) = Recorder::new(provider);
    let mut config = ConnectionConfig::with_auth_provider_and_service(
        Box::new(recorder),
        EntityType::OSD.bits(),
    );
    config.preferred_modes = vec![ConnectionMode::Secure];
    config.global_id = session.global_id;
    config.entity_name = entity.name.parse().expect("entity name");
    config.is_lossy = true;
    let mut conn = Connection::connect_with_target(addr, entity_addr, config)
        .await
        .expect("connect to osd.0");
    conn.establish_session()
        .await
        .unwrap_or_else(|e| panic!("osd.0 session: {e}"));
    conn.close().await;

    let transcript = log.lock().expect("recorder log").clone();
    let shape: Vec<(&str, bool)> = transcript
        .iter()
        .map(|r| (r.direction, r.con_mode != 0))
        .collect();
    assert_eq!(
        shape,
        [("out", false), ("in", false), ("out", false), ("in", true)],
        "osd.0 transcript: authorizer, challenge, authorizer, reply"
    );
    transcript
}

/// `(service, session_key)` of every `build_service_ticket` line the mon
/// logged for `entity`, by global_id. Everything else in the log is dropped.
fn logged_session_keys(
    log: &[u8],
    entity: &str,
    global_ids: &[u64],
) -> BTreeMap<u64, Vec<(String, String)>> {
    let mut found: BTreeMap<u64, Vec<(String, String)>> = BTreeMap::new();
    for line in String::from_utf8_lossy(log).lines() {
        let Some(start) = line.find(LOG_MARKER) else {
            continue;
        };
        let rest = &line[start + LOG_MARKER.len()..];
        let Some(info) = rest.split(')').next() else {
            continue;
        };
        let mut words = info.split_whitespace();
        let Some(service) = words.next() else {
            continue;
        };
        let (mut session_key, mut name, mut global_id) = (None, None, None);
        for word in words {
            if let Some(v) = word.strip_prefix("session_key=") {
                session_key = Some(v);
            } else if let Some(v) = word.strip_prefix("ticket.name=") {
                name = Some(v);
            } else if let Some(v) = word.strip_prefix("ticket.global_id=") {
                global_id = v.parse::<u64>().ok();
            }
        }
        if let (Some(key), Some(n), Some(gid)) = (session_key, name, global_id)
            && n == entity
            && global_ids.contains(&gid)
        {
            found
                .entry(gid)
                .or_default()
                .push((service.to_owned(), key.to_owned()));
        }
    }
    found
}

/// Run the mon log command until it shows, for every session, a session key
/// for each service the session stored, and return them by global_id. Its
/// output is parsed in memory only, and failures name counts and global_ids.
async fn mon_logged_keys(
    log_cmd: &str,
    entity: &str,
    sessions: &[&MonSession],
) -> BTreeMap<u64, BTreeMap<String, String>> {
    let global_ids: Vec<u64> = sessions.iter().map(|s| s.global_id).collect();
    let start = Instant::now();
    loop {
        let mut words = log_cmd.split_whitespace();
        let out = tokio::process::Command::new(words.next().expect("CEPHX_MON_LOG_CMD is empty"))
            .args(words)
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .expect("run CEPHX_MON_LOG_CMD");
        let mut progress = Vec::new();
        if out.status.success() {
            let found = logged_session_keys(&out.stdout, entity, &global_ids);
            let mut complete = BTreeMap::new();
            for session in sessions {
                let lines = found.get(&session.global_id).map_or(&[][..], Vec::as_slice);
                let mut keys = BTreeMap::new();
                for (service, key) in lines {
                    let previous = keys.insert(service.clone(), key.clone());
                    assert!(
                        previous.is_none_or(|p| p == *key),
                        "global_id {}: the mon logged two session keys for one service",
                        session.global_id
                    );
                }
                let stored = stored_services(&session.handler);
                let logged: BTreeSet<&str> = keys.keys().map(String::as_str).collect();
                if logged.is_superset(&stored) {
                    complete.insert(session.global_id, keys);
                } else {
                    progress.push(format!(
                        "global_id {}: {} of {} services",
                        session.global_id,
                        logged.intersection(&stored).count(),
                        stored.len()
                    ));
                }
            }
            if progress.is_empty() {
                return complete;
            }
        } else {
            progress.push(format!("the log command exited {}", out.status));
        }
        assert!(
            start.elapsed() < LOG_WAIT,
            "no session keys in the mon log after {LOG_WAIT:?} (is debug_auth 10?): {}",
            progress.join("; ")
        );
        tokio::time::sleep(LOG_POLL).await;
    }
}

fn write_fixture(dir: &Path, file: &str, value: &serde_json::Value) {
    let text = serde_json::to_string_pretty(value).expect("fixture JSON") + "\n";
    std::fs::write(dir.join(file), text).unwrap_or_else(|e| panic!("write {file}: {e}"));
}

/// C++ `AuthConnectionMeta::get_connection_secret_length` (`Auth.h`).
fn connection_secret_len(con_mode: u32) -> usize {
    if con_mode == CEPH_CON_MODE_SECURE {
        64
    } else {
        0
    }
}

async fn capture(admin: &Client, entity: &cephx::Entity, cfg: &Capture) {
    let key = entity_key(entity);
    let crc = mon_session(entity, &key, ConnectionMode::Crc).await;
    let secure = mon_session(entity, &key, ConnectionMode::Secure).await;
    assert_eq!(crc.con_mode, CEPH_CON_MODE_CRC);
    assert_eq!(secure.con_mode, CEPH_CON_MODE_SECURE);
    let osd = osd_session(admin, entity, &secure).await;

    let logged = mon_logged_keys(&cfg.log_cmd, &entity.name, &[&crc, &secure]).await;

    std::fs::create_dir_all(&cfg.out).expect("CEPHX_CAPTURE_OUT");
    for (session, mode) in [(&crc, "crc"), (&secure, "secure")] {
        let auth_done = session.transcript.last().expect("AUTH_DONE");
        write_fixture(
            &cfg.out,
            &format!("v{}-mon-{mode}.json", cfg.release),
            &json!({
                "release": cfg.release,
                "entity": entity.name,
                "client_key": key,
                "con_mode": session.con_mode,
                "global_id": session.global_id,
                "auth_done_hex": hex::encode(&auth_done.payload),
                "connection_secret_len": connection_secret_len(session.con_mode),
                "mon_logged_session_keys": logged[&session.global_id],
            }),
        );
    }
    let osd_key = logged[&secure.global_id]
        .get("osd")
        .expect("the mon logged the OSD session key");
    write_fixture(
        &cfg.out,
        &format!("v{}-osd.json", cfg.release),
        &json!({
            "release": cfg.release,
            "entity": entity.name,
            "con_mode": osd[3].con_mode,
            "global_id": secure.global_id,
            "osd_session_key": osd_key,
            "authorizer1_hex": hex::encode(&osd[0].payload),
            "challenge_hex": hex::encode(&osd[1].payload),
            "authorizer2_hex": hex::encode(&osd[2].payload),
            "reply_hex": hex::encode(&osd[3].payload),
        }),
    );
    println!(
        "wrote v{0}-mon-crc.json, v{0}-mon-secure.json and v{0}-osd.json to {1}",
        cfg.release,
        cfg.out.display()
    );
}

#[tokio::test]
#[ignore]
async fn capture_transcripts() {
    let Some(cfg) = Capture::from_env() else {
        println!("skipped: set CEPHX_CAPTURE_OUT and CEPHX_MON_LOG_CMD to capture");
        return;
    };
    common::init_tracing();

    let admin = common::build_test_client().await.expect("admin client");
    let entity = cephx::create_entity(&admin, "kat", "aes256k").await;
    let result = AssertUnwindSafe(capture(&admin, &entity, &cfg))
        .catch_unwind()
        .await;
    if let Err(e) = cephx::remove_entity(&admin, &entity.name).await {
        eprintln!("cleanup: {e}");
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
