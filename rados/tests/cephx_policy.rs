//! Cephx policy scenarios on a live cluster: they change the cluster's auth
//! policy through the admin client's mon commands and restore Rook v1.20.7's
//! (`auth_allowed_ciphers aes,aes256k`, `auth_preferred_cipher aes256k`,
//! `auth_service_cipher aes256k`) on the way out, wiping the rotating keys
//! after every service-cipher change.
//!
//! The rules:
//! - Opt-in. Every test is ignored and skips unless
//!   `CEPH_TEST_ALLOW_AUTH_POLICY=1` and the monmap has a cephx policy
//!   (Ceph v19.2.6 or v20.2.4 and later).
//! - Serial and alone. Each test holds `POLICY` for its whole run, and
//!   `--test-threads=1` keeps the output readable. Nothing else may use the
//!   cluster meanwhile: while a policy is changed, other clients' keys can be
//!   refused and their service tickets are AES.
//! - Never in CI.
//! - A test that finds the cluster off Rook's policy fails without touching
//!   it. Only a killed run (Ctrl-C, SIGKILL) skips the restore, and the next
//!   run then fails that way. Restore the policy by hand with the cluster's
//!   `ceph` CLI (on a rooket cluster, the toolbox's), then remove any
//!   `client.rados-rs-*` entity with `ceph auth rm`:
//!
//!   ceph mon set auth_allowed_ciphers aes,aes256k
//!   ceph mon set auth_preferred_cipher aes256k
//!   ceph mon set auth_service_cipher aes256k
//!   ceph auth wipe-rotating-service-keys
//!
//! Run with (see `common` for a rooket cluster):
//!   CEPH_CONF=... CEPH_TEST_ALLOW_AUTH_POLICY=1 \
//!   cargo test -p rados --test cephx_policy -- --ignored --nocapture --test-threads=1

mod cephx;
mod common;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{build_test_client, test_pool_name};
use futures::FutureExt;
use rados::{Client, ClientError, EntityType, IoCtx, MonClientError, Msgr2Error};

static POLICY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A hung body fails into its cleanup instead of hanging the run.
const BODY_TIMEOUT: Duration = Duration::from_secs(300);
/// Retry budget of one policy change: a monmap commit makes the mons
/// re-elect, which can delay the next command.
const POLICY_WAIT: Duration = Duration::from_secs(30);
/// After an unanswered wipe, how long to wait for the auth epoch to rise
/// before sending it again: a wipe is not idempotent.
const WIPE_RESEND_WAIT: Duration = Duration::from_secs(10);
/// Bound of one object operation.
const OP_BOUND: Duration = Duration::from_secs(10);

const RECOVERY: &str = "ceph mon set auth_allowed_ciphers aes,aes256k\n\
                        ceph mon set auth_preferred_cipher aes256k\n\
                        ceph mon set auth_service_cipher aes256k\n\
                        ceph auth wipe-rotating-service-keys";

const AES: u16 = rados::auth::CEPH_CRYPTO_AES;
const AES256K: u16 = rados::auth::CEPH_CRYPTO_AES256KRB5;

/// Take `POLICY` and build the admin client, or `None` with a printed
/// reason when the test must not run.
async fn setup() -> Option<(tokio::sync::MutexGuard<'static, ()>, Client)> {
    if std::env::var("CEPH_TEST_ALLOW_AUTH_POLICY").as_deref() != Ok("1") {
        println!(
            "skipped: set CEPH_TEST_ALLOW_AUTH_POLICY=1 to let it change the cluster's auth policy"
        );
        return None;
    }
    let serial = POLICY.lock().await;
    common::init_tracing();
    let admin = build_test_client().await.expect("admin client");
    if cephx::mon_auth(&admin).await.is_none() {
        println!("skipped: the monmap has no cephx policy (Ceph before v19.2.6)");
        return None;
    }
    Some((serial, admin))
}

fn cipher_number(name: &str) -> u16 {
    match name {
        "aes" => AES,
        "aes256k" => AES256K,
        other => panic!("no cipher number for {other:?}"),
    }
}

/// Retry `check` every `poll` until it succeeds or `bound` has passed.
async fn try_eventually<T, F, Fut>(
    what: &str,
    bound: Duration,
    poll: Duration,
    mut check: F,
) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let start = Instant::now();
    loop {
        let last = match check().await {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        if start.elapsed() >= bound {
            return Err(format!("{what}: not within {bound:?}: {last}"));
        }
        tokio::time::sleep(poll).await;
    }
}

/// [`try_eventually`], panicking naming `what` when `bound` passes.
async fn eventually<T, F, Fut>(what: &str, bound: Duration, poll: Duration, check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    try_eventually(what, bound, poll, check)
        .await
        .unwrap_or_else(|e| panic!("{e}"))
}

async fn policy(admin: &Client) -> Result<cephx::MonAuth, String> {
    cephx::try_mon_auth(admin)
        .await?
        .ok_or_else(|| "the monmap has no cephx policy".to_owned())
}

/// `mon set name value`, retried while the mon does not answer (an
/// unchanged value answers `already set`), then polled until `mon dump`
/// shows it.
async fn mon_set(admin: &Client, name: &str, value: &str) -> Result<(), String> {
    let cmd = serde_json::json!({"prefix": "mon set", "name": name, "value": value});
    let mut want: Vec<u16> = value
        .split([',', ' '])
        .filter(|c| !c.is_empty())
        .map(cipher_number)
        .collect();
    want.sort_unstable();
    let what = format!("mon set {name} {value}");
    try_eventually(&what, POLICY_WAIT, Duration::from_secs(1), || async {
        match cephx::mon_command(admin, cmd.clone()).await {
            Ok(res) if res.retval == 0 => Ok(()),
            Ok(res) => Err(format!("retval {}: {}", res.retval, res.outs)),
            Err(e) => Err(format!("{e}")),
        }
    })
    .await?;
    try_eventually(&what, POLICY_WAIT, Duration::from_secs(1), || async {
        let auth = policy(admin).await?;
        let shown = match name {
            "auth_service_cipher" => vec![auth.service_cipher],
            "auth_preferred_cipher" => vec![auth.preferred],
            "auth_allowed_ciphers" => auth.allowed.clone(),
            other => panic!("no mon dump field for {other}"),
        };
        if shown == want {
            Ok(())
        } else {
            Err(format!("mon dump shows {shown:?}"))
        }
    })
    .await
}

/// `auth wipe-rotating-service-keys`, then wait for the monmap's auth epoch
/// to rise. A wipe is not idempotent, so one the mon did not answer is sent
/// again only if the epoch has not risen within `WIPE_RESEND_WAIT`.
async fn wipe(admin: &Client) -> Result<(), String> {
    let before = try_eventually("mon dump", POLICY_WAIT, Duration::from_secs(1), || {
        policy(admin)
    })
    .await?
    .epoch;
    let cmd = serde_json::json!({"prefix": "auth wipe-rotating-service-keys"});
    let start = Instant::now();
    loop {
        let (bound, sent) = match cephx::mon_command(admin, cmd.clone()).await {
            Ok(res) if res.retval == 0 => (POLICY_WAIT, Ok(())),
            Ok(res) => (
                WIPE_RESEND_WAIT,
                Err(format!("retval {}: {}", res.retval, res.outs)),
            ),
            Err(e) => (WIPE_RESEND_WAIT, Err(format!("{e}"))),
        };
        let rose = try_eventually(
            "the auth epoch rises",
            bound,
            Duration::from_secs(1),
            || async {
                let epoch = policy(admin).await?.epoch;
                if epoch > before {
                    Ok(())
                } else {
                    Err(format!("auth_epoch {epoch}"))
                }
            },
        )
        .await;
        match (rose, sent) {
            (Ok(()), _) => return Ok(()),
            (Err(e), Ok(())) => return Err(format!("auth wipe-rotating-service-keys: {e}")),
            (Err(e), Err(send)) if start.elapsed() >= POLICY_WAIT => {
                return Err(format!("auth wipe-rotating-service-keys: {send}; {e}"));
            }
            (Err(_), Err(_)) => {}
        }
    }
}

/// Why `auth` is not Rook's policy, if it is not.
fn off_rook_policy(auth: Option<&cephx::MonAuth>) -> Option<String> {
    let Some(auth) = auth else {
        return Some("the monmap has no cephx policy".to_owned());
    };
    let rook = (AES256K, vec![AES, AES256K], AES256K);
    let found = (auth.service_cipher, auth.allowed.clone(), auth.preferred);
    (found != rook).then(|| {
        format!(
            "service cipher {}, allowed {:?}, preferred {}; Rook's is {}, {:?}, {}",
            found.0, found.1, found.2, rook.0, rook.1, rook.2
        )
    })
}

/// The precondition, checked before a test arms its cleanup: a cluster off
/// Rook's policy is reported with the recovery commands and left alone.
async fn assert_rook_policy(admin: &Client) {
    if let Some(problem) = off_rook_policy(cephx::mon_auth(admin).await.as_ref()) {
        panic!(
            "the cluster is off Rook's cephx policy ({problem}), so the test leaves it alone; \
             restore it with the cluster's ceph CLI:\n{RECOVERY}"
        );
    }
}

/// Put Rook's policy back and wipe the rotating keys, each step retried and
/// run whatever the others did; returns the failures, followed by the
/// recovery commands if there are any.
async fn restore(admin: &Client) -> Vec<String> {
    let mut failures = Vec::new();
    for (name, value) in [
        ("auth_allowed_ciphers", "aes,aes256k"),
        ("auth_preferred_cipher", "aes256k"),
        ("auth_service_cipher", "aes256k"),
    ] {
        if let Err(e) = mon_set(admin, name, value).await {
            failures.push(e);
        }
    }
    if let Err(e) = wipe(admin).await {
        failures.push(e);
    }
    match policy(admin).await {
        Ok(auth) => failures.extend(off_rook_policy(Some(&auth))),
        Err(e) => failures.push(e),
    }
    if !failures.is_empty() {
        failures.push(format!(
            "the cluster may be off Rook's cephx policy; restore it with the cluster's ceph \
             CLI:\n{RECOVERY}"
        ));
    }
    failures
}

/// What a test created, for its cleanup.
#[derive(Default)]
struct Created {
    entities: Vec<cephx::Entity>,
    objects: Vec<String>,
    client: Option<Client>,
}

/// Remove `created`'s objects and entities, adding failures to `failures`.
async fn remove_created(admin: &Client, created: &Created, failures: &mut Vec<String>) {
    if !created.objects.is_empty() {
        match admin.open_pool(&test_pool_name()).await {
            Ok(ioctx) => {
                for oid in &created.objects {
                    let removed = try_eventually(
                        "remove",
                        Duration::from_secs(60),
                        Duration::from_secs(2),
                        || cephx::remove_object(&ioctx, oid),
                    )
                    .await;
                    failures.extend(removed.err());
                }
            }
            Err(e) => failures.push(format!("opening the pool: {e}")),
        }
    }
    for entity in &created.entities {
        failures.extend(cephx::remove_entity(admin, &entity.name).await.err());
    }
}

type BodyResult = Result<std::thread::Result<()>, tokio::time::error::Elapsed>;

/// Run `body` under `BODY_TIMEOUT`, catching a panic.
async fn run_body(body: impl Future<Output = ()>) -> BodyResult {
    tokio::time::timeout(BODY_TIMEOUT, AssertUnwindSafe(body).catch_unwind()).await
}

/// Assert last: re-raise the body's panic, or fail with every cleanup
/// failure. Cleanup failures are printed as one block before a re-raise,
/// since the panic's own message was printed before the cleanup ran.
fn finish(body: BodyResult, failures: Vec<String>) {
    let cleanup = failures.join("\n");
    match body {
        Ok(Ok(())) => assert!(failures.is_empty(), "cleanup failed:\n{cleanup}"),
        Ok(Err(panic)) => {
            if !failures.is_empty() {
                eprintln!("the test failed, and so did its cleanup:\n{cleanup}");
            }
            std::panic::resume_unwind(panic);
        }
        Err(_) if failures.is_empty() => {
            panic!("the test body did not finish within {BODY_TIMEOUT:?}")
        }
        Err(_) => panic!(
            "the test body did not finish within {BODY_TIMEOUT:?}, and its cleanup failed:\n\
             {cleanup}"
        ),
    }
}

/// Print the outcome of a check the cleanup records but does not assert.
fn record(what: &str, outcome: Result<(), String>) {
    match outcome {
        Ok(()) => println!("recorded: {what}: yes"),
        Err(e) => println!("recorded: {what}: NO: {e}"),
    }
}

async fn bounded<T, E: std::fmt::Display>(
    what: &str,
    op: impl Future<Output = Result<T, E>>,
) -> Result<T, String> {
    match tokio::time::timeout(OP_BOUND, op).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(format!("{what}: {e}")),
        Err(_) => Err(format!("{what}: no reply within {OP_BOUND:?}")),
    }
}

async fn read_is(ioctx: &IoCtx, oid: &str, data: &'static [u8]) -> Result<(), String> {
    let read = bounded("read", ioctx.read(oid, 0, 64)).await?;
    if read.data == data {
        Ok(())
    } else {
        Err(format!("read {:?}, wrote {data:?}", read.data))
    }
}

async fn write_read(ioctx: &IoCtx, oid: &str, data: &'static [u8]) -> Result<(), String> {
    bounded("write", ioctx.write_full(oid, Bytes::from_static(data))).await?;
    read_is(ioctx, oid, data).await
}

/// Close `ioctx`'s session to `oid`'s primary, then write and read `oid`
/// on a new session, retried while the OSD refuses the new ticket (it must
/// first fetch the new rotating keys).
async fn io_on_new_session(ioctx: &IoCtx, oid: &str, data: &'static [u8]) -> Result<(), String> {
    bounded("close the primary's session", async {
        ioctx.close_primary_session_for_test(oid).await
    })
    .await?;
    try_eventually(
        "write and read on a new OSD session",
        Duration::from_secs(60),
        Duration::from_secs(2),
        || write_read(ioctx, oid, data),
    )
    .await
}

async fn mixed_body(admin: &Client, created: &mut Created) {
    let Created {
        entities,
        objects,
        client,
    } = created;
    entities.push(cephx::create_entity(admin, "mixed", "aes256k").await);
    let entity = &entities[0];
    let oid = cephx::unique("mixed");
    objects.push(oid.clone());
    let pool = test_pool_name();

    let c: &Client = client.insert(cephx::client_for(entity).await.expect("client C"));
    assert_eq!(
        cephx::ticket_types(c).await,
        (AES256K, AES256K),
        "C's AUTH and OSD tickets on Rook's policy"
    );
    let c_io = c.open_pool(&pool).await.expect("C's pool");
    write_read(&c_io, &oid, b"before")
        .await
        .expect("C writes and reads o");
    let before = c.mon_client().get_monmap().await.auth_epoch;

    mon_set(admin, "auth_service_cipher", "aes")
        .await
        .expect("service cipher aes");
    wipe(admin).await.expect("wipe");

    eventually(
        "C sees the auth epoch rise and holds an AES OSD ticket",
        Duration::from_secs(60),
        Duration::from_secs(1),
        || async move {
            let epoch = c.mon_client().get_monmap().await.auth_epoch;
            let types = cephx::try_ticket_types(c).await;
            if epoch > before && types == Some((AES256K, AES)) {
                Ok(())
            } else {
                Err(format!(
                    "auth_epoch {before} -> {epoch}, ticket types {types:?}"
                ))
            }
        },
    )
    .await;
    io_on_new_session(&c_io, &oid, b"mixed")
        .await
        .unwrap_or_else(|e| panic!("C's I/O with an AES OSD ticket: {e}"));

    let d = cephx::client_for(entity).await.expect("client D");
    assert_eq!(
        cephx::ticket_types(&d).await,
        (AES256K, AES),
        "D's AUTH and OSD tickets while the service cipher is aes"
    );
    let d_io = d.open_pool(&pool).await.expect("D's pool");
    eventually(
        "D reads o",
        Duration::from_secs(60),
        Duration::from_secs(2),
        || read_is(&d_io, &oid, b"mixed"),
    )
    .await;
}

/// One aes256k client across a service-cipher change and a wipe: its AUTH
/// ticket keeps its type and its OSD ticket turns AES, Rook's upgrade window
/// in one client.
#[tokio::test]
#[ignore]
async fn mixed_ticket_types_in_a_long_lived_client() {
    let Some((_serial, admin)) = setup().await else {
        return;
    };
    assert_rook_policy(&admin).await;

    let mut created = Created::default();
    let body = run_body(mixed_body(&admin, &mut created)).await;

    let mut failures = restore(&admin).await;
    if let (Some(c), Some(oid)) = (&created.client, created.objects.first()) {
        record(
            "C holds aes256k AUTH and OSD tickets after the restore",
            try_eventually(
                "ticket types",
                Duration::from_secs(60),
                Duration::from_secs(1),
                || async {
                    match cephx::try_ticket_types(c).await {
                        Some(t) if t == (AES256K, AES256K) => Ok(()),
                        other => Err(format!("{other:?}")),
                    }
                },
            )
            .await,
        );
        let reads = async {
            let c_io = bounded("open the pool", c.open_pool(&test_pool_name())).await?;
            io_on_new_session(&c_io, oid, b"after").await
        };
        record(
            "C writes and reads o on a new OSD session after the restore",
            reads.await,
        );
    }
    remove_created(&admin, &created, &mut failures).await;
    finish(body, failures);
}

async fn refused_body(admin: &Client, created: &mut Created) {
    created
        .entities
        .push(cephx::create_entity(admin, "refused", "aes").await);
    let entity = &created.entities[0];
    let sanity = cephx::client_for(entity)
        .await
        .expect("the AES client connects on Rook's policy");
    let _ = sanity.shutdown().await;

    mon_set(admin, "auth_allowed_ciphers", "aes256k")
        .await
        .expect("allowed ciphers aes256k");
    // The mon refuses the entity at CEPHX_GET_AUTH_SESSION_KEY with -EACCES,
    // which msgr2 answers with AUTH_BAD_METHOD; cephx is the only method.
    match tokio::time::timeout(Duration::from_secs(30), cephx::client_for(entity)).await {
        Ok(Err(ClientError::MonClient(MonClientError::MessageError(Msgr2Error::Protocol(
            msg,
        )))))
            if msg.contains("result=-13 (") && msg.contains("(os error 13)") =>
        {
            println!("refused: {msg}");
        }
        Ok(Err(e)) => panic!("the AES key failed, but not with EACCES: {e:?}"),
        Ok(Ok(_)) => panic!("an AES key authenticated with aes dropped from the allowed ciphers"),
        Err(_) => panic!("no answer within 30 s, which is not a refusal"),
    }
}

/// An AES key is refused once aes is dropped from the allowed ciphers.
#[tokio::test]
#[ignore]
async fn key_type_not_allowed_is_refused() {
    let Some((_serial, admin)) = setup().await else {
        return;
    };
    assert_rook_policy(&admin).await;

    let mut created = Created::default();
    let body = run_body(refused_body(&admin, &mut created)).await;

    let mut failures = restore(&admin).await;
    if let Some(entity) = created.entities.first() {
        record(
            "a fresh AES client connects after the restore",
            try_eventually(
                "connect",
                Duration::from_secs(30),
                Duration::from_secs(2),
                || async {
                    let client = cephx::client_for(entity)
                        .await
                        .map_err(|e| format!("{e}"))?;
                    let _ = client.shutdown().await;
                    Ok(())
                },
            )
            .await,
        );
    }
    remove_created(&admin, &created, &mut failures).await;
    finish(body, failures);
}

async fn refresh_body(admin: &Client, created: &mut Created) {
    let Created {
        entities,
        objects,
        client,
    } = created;
    entities.push(cephx::create_entity(admin, "refresh", "aes256k").await);
    let entity = &entities[0];
    let oid = cephx::unique("refresh");
    objects.push(oid.clone());

    let c: &Client = client.insert(cephx::client_for(entity).await.expect("client C"));
    let c_io = c.open_pool(&test_pool_name()).await.expect("C's pool");
    write_read(&c_io, &oid, b"before")
        .await
        .expect("C writes and reads o");
    let (key_before, blob_before) = cephx::ticket_bytes(c, EntityType::OSD).await;
    let (key_before, blob_before) = (&key_before, &blob_before);
    let epoch_before = c.mon_client().get_monmap().await.auth_epoch;

    wipe(admin).await.expect("wipe");

    // secret_id is not compared: a wipe restarts the rotating keys' numbering.
    eventually(
        "C sees the auth epoch rise and holds a new OSD ticket",
        Duration::from_secs(30),
        Duration::from_secs(1),
        || async move {
            let epoch = c.mon_client().get_monmap().await.auth_epoch;
            let (key, blob) = cephx::ticket_bytes(c, EntityType::OSD).await;
            let (new_key, new_blob) = (key != *key_before, blob != *blob_before);
            if epoch > epoch_before && new_key && new_blob {
                Ok(())
            } else {
                Err(format!(
                    "auth_epoch {epoch_before} -> {epoch}, new session key {new_key}, \
                     new blob {new_blob}"
                ))
            }
        },
    )
    .await;
    io_on_new_session(&c_io, &oid, b"after")
        .await
        .unwrap_or_else(|e| panic!("C's I/O with the refreshed OSD ticket: {e}"));
}

/// A wipe raises the monmap's auth epoch; a connected client fetches new
/// service tickets and does I/O on a new OSD session with them.
#[tokio::test]
#[ignore]
async fn tickets_refresh_when_auth_epoch_rises() {
    let Some((_serial, admin)) = setup().await else {
        return;
    };
    assert_rook_policy(&admin).await;

    let mut created = Created::default();
    let body = run_body(refresh_body(&admin, &mut created)).await;

    let mut failures = Vec::new();
    remove_created(&admin, &created, &mut failures).await;
    match policy(&admin).await {
        Ok(auth) => failures.extend(
            off_rook_policy(Some(&auth))
                .map(|problem| format!("off Rook's policy after a wipe: {problem}\n{RECOVERY}")),
        ),
        Err(e) => failures.push(e),
    }
    finish(body, failures);
}
