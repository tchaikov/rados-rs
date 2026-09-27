//! Cluster tests for the otp class, against Ceph v19.2.2. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_otp -- --ignored --nocapture
//!
//! Codes are computed from the OSD's clock (`get_current_time`), never the
//! host's, with a 300 s step so no test straddles a step boundary.

#[path = "../../rados/tests/common/mod.rs"]
mod common;
mod totp;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use common::create_ioctx;
use rados::{IoCtx, OSDClientError, UTime};
use rados_cls::otp::{self, CheckResult, OtpInfo, SeedType};

const ENOENT: i32 = 2;
const EINVAL: i32 = 22;
const STEP: u32 = 300;
const KEY: &[u8] = b"12345678901234567890";
/// Hex of `KEY`, so no base32 encoder is needed.
const SEED_HEX: &str = "3132333435363738393031323334353637383930";

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

fn is_osd_error(err: &OSDClientError, errno: i32) -> bool {
    matches!(err, OSDClientError::OSDError { code, .. } if *code == -errno)
}

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).expect("ascii"), 16).expect("hex"))
        .collect()
}

/// Run `body`, then remove `oids` whether it passed or panicked, so a
/// failing test leaves nothing in the pool.
async fn guarded(ioctx: &IoCtx, oids: &[&str], body: impl Future<Output = ()>) {
    let mut body = std::pin::pin!(body);
    let result = std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| body.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    })
    .await;
    for oid in oids {
        if let Err(err) = ioctx.remove(oid).await
            && !is_osd_error(&err, ENOENT)
        {
            eprintln!("cleanup: removing {oid}: {err:?}");
        }
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn device(id: &str) -> OtpInfo {
    OtpInfo {
        step_size: STEP,
        window: 2,
        ..OtpInfo::totp(id, SEED_HEX, SeedType::HEX)
    }
}

/// The OSD's clock and its TOTP counter; waits out the last 15 s of a
/// step so the counter holds for the rest of the test.
async fn counter(ioctx: &IoCtx, oid: &str) -> (UTime, u64) {
    let mut now = otp::get_current_time(ioctx, oid).await.expect("time");
    if now.sec % STEP >= 285 {
        let wait = STEP - now.sec % STEP + 1;
        eprintln!("waiting {wait}s for the next step");
        tokio::time::sleep(Duration::from_secs(u64::from(wait))).await;
        now = otp::get_current_time(ioctx, oid).await.expect("time");
    }
    (now, u64::from(now.sec / STEP))
}

fn code(t: u64) -> String {
    totp::hotp(KEY, t, 6)
}

/// A six-digit code no counter in the window accepts.
fn wrong(t: u64) -> String {
    let valid: Vec<String> = (t - 2..=t + 2).map(code).collect();
    (0..)
        .map(|n| format!("{n:06}"))
        .find(|c| !valid.contains(c))
        .expect("a wrong code")
}

async fn verdict(ioctx: &IoCtx, oid: &str, id: &str, val: &str, token: &str) -> CheckResult {
    otp::check_and_get_result(ioctx, oid, id, val, token)
        .await
        .expect("check_and_get_result")
        .result
}

fn ids(entries: &[OtpInfo]) -> Vec<&str> {
    entries.iter().map(|e| e.id.as_str()).collect()
}

#[tokio::test]
#[ignore]
async fn otp_set_get_list() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-set-get");
    guarded(&ioctx, &[&oid], async {
        let b32 = OtpInfo {
            time_ofs: -30,
            step_size: 60,
            window: 1,
            ..OtpInfo::totp("b-b32", "GEZDGNBV", SeedType::BASE32)
        };
        otp::set(&ioctx, &oid, &[device("a-hex"), b32])
            .await
            .expect("set");

        let all = otp::get_all(&ioctx, &oid).await.expect("get_all");
        assert_eq!(ids(&all), ["a-hex", "b-b32"]);
        let (a, b) = (&all[0], &all[1]);
        assert_eq!(&a.seed_bin[..], KEY, "the server fills seed_bin");
        assert_eq!(a.seed, SEED_HEX);
        assert_eq!(a.seed_type, SeedType::HEX);
        assert_eq!((a.time_ofs, a.step_size, a.window), (0, STEP, 2));
        assert_eq!(&b.seed_bin[..], b"12345");
        assert_eq!(b.seed, "GEZDGNBV");
        assert_eq!(b.seed_type, SeedType::BASE32);
        assert_eq!((b.time_ofs, b.step_size, b.window), (-30, 60, 1));
        let json = serde_json::to_value(a).expect("json");
        assert_eq!(json["seed"], SEED_HEX, "the seed dumps in cleartext");

        let got = otp::get(&ioctx, &oid, &["b-b32", "missing"])
            .await
            .expect("get");
        assert_eq!(ids(&got), ["b-b32"]);
        let got = otp::get(&ioctx, &oid, &["missing"])
            .await
            .expect("get missing");
        assert!(got.is_empty(), "{got:?}");
        let err = otp::get_one(&ioctx, &oid, "missing")
            .await
            .expect_err("get_one missing");
        assert!(is_osd_error(&err, ENOENT), "{err:?}");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_set_bad_seed() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-bad-seed");
    guarded(&ioctx, &[&oid], async {
        let bad = OtpInfo {
            seed: "zz".to_owned(),
            ..device("x")
        };
        let err = otp::set(&ioctx, &oid, &[bad]).await.expect_err("bad seed");
        assert!(is_osd_error(&err, EINVAL), "{err:?}");
        let err = otp::get_all(&ioctx, &oid)
            .await
            .expect_err("nothing was created");
        assert!(is_osd_error(&err, ENOENT), "{err:?}");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_remove() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-remove");
    guarded(&ioctx, &[&oid], async {
        otp::set(&ioctx, &oid, &[device("a"), device("b")])
            .await
            .expect("set");
        otp::remove(&ioctx, &oid, &["a"]).await.expect("remove");
        otp::remove(&ioctx, &oid, &["a"])
            .await
            .expect("remove an absent id");
        let all = otp::get_all(&ioctx, &oid).await.expect("get_all");
        assert_eq!(ids(&all), ["b"]);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_missing_object() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-missing");
    let oid2 = unique("cls-otp-missing-id");
    guarded(&ioctx, &[&oid, &oid2], async {
        let err = otp::get_all(&ioctx, &oid).await.expect_err("get_all");
        assert!(is_osd_error(&err, ENOENT), "get_all: {err:?}");
        let err = otp::get_current_time(&ioctx, &oid)
            .await
            .expect_err("get_current_time");
        assert!(is_osd_error(&err, ENOENT), "get_current_time: {err:?}");
        let err = otp::get_result(&ioctx, &oid, "a", "t")
            .await
            .expect_err("get_result");
        assert!(is_osd_error(&err, ENOENT), "get_result: {err:?}");
        otp::remove(&ioctx, &oid, &["a"])
            .await
            .expect("remove on a missing object");

        otp::set(&ioctx, &oid2, &[device("a")]).await.expect("set");
        let err = otp::check(&ioctx, &oid2, "nope", "123456", "t")
            .await
            .expect_err("check of an unknown id");
        assert!(is_osd_error(&err, ENOENT), "check: {err:?}");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_check_totp() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-check");
    guarded(&ioctx, &[&oid], async {
        otp::set(&ioctx, &oid, &[device("d")]).await.expect("set");
        let (now, t) = counter(&ioctx, &oid).await;

        let res = otp::check_and_get_result(&ioctx, &oid, "d", &code(t), "t1")
            .await
            .expect("check t1");
        assert_eq!(res.result, CheckResult::SUCCESS);
        assert_eq!(res.token, "t1");
        assert!(
            res.timestamp.sec.abs_diff(now.sec) <= 5,
            "{res:?} vs {now:?}"
        );

        otp::check(&ioctx, &oid, "d", &wrong(t), "t2")
            .await
            .expect("a wrong code still returns 0");
        let res = otp::get_result(&ioctx, &oid, "d", "t2")
            .await
            .expect("get_result t2");
        assert_eq!(res.result, CheckResult::FAIL);

        assert_eq!(
            verdict(&ioctx, &oid, "d", &code(t), "t3").await,
            CheckResult::FAIL,
            "replay of the same step"
        );
        assert_eq!(
            verdict(&ioctx, &oid, "d", &code(t + 1), "t4").await,
            CheckResult::SUCCESS,
            "the next step is inside the window and past the index"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_past_step_quirk() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-past-step");
    guarded(&ioctx, &[&oid], async {
        otp::set(&ioctx, &oid, &[device("d")]).await.expect("set");
        let (_, t) = counter(&ioctx, &oid).await;
        let got = [
            verdict(&ioctx, &oid, "d", &code(t - 1), "p1").await,
            verdict(&ioctx, &oid, "d", &code(t), "p2").await,
            verdict(&ioctx, &oid, "d", &code(t + 1), "p3").await,
            verdict(&ioctx, &oid, "d", &code(t + 2), "p4").await,
        ];
        // The t-1 match records last_success = 1 + t.
        assert_eq!(
            got,
            [
                CheckResult::SUCCESS,
                CheckResult::FAIL,
                CheckResult::FAIL,
                CheckResult::SUCCESS,
            ]
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_older_codes_pass_after_the_current_one() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-older-codes");
    guarded(&ioctx, &[&oid], async {
        otp::set(&ioctx, &oid, &[device("d")]).await.expect("set");
        let (_, t) = counter(&ioctx, &oid).await;
        let got = [
            verdict(&ioctx, &oid, "d", &code(t), "o1").await,
            verdict(&ioctx, &oid, "d", &code(t - 1), "o2").await,
            verdict(&ioctx, &oid, "d", &code(t - 2), "o3").await,
        ];
        // Each match records distance + counter: t, then t + 1, then t + 2.
        assert_eq!(
            got,
            [
                CheckResult::SUCCESS,
                CheckResult::SUCCESS,
                CheckResult::SUCCESS,
            ]
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_get_result_unknown() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-unknown");
    guarded(&ioctx, &[&oid], async {
        otp::set(&ioctx, &oid, &[device("d")]).await.expect("set");
        let res = otp::get_result(&ioctx, &oid, "d", "never")
            .await
            .expect("get_result");
        let now = otp::get_current_time(&ioctx, &oid).await.expect("time");
        assert_eq!(res.token, "never");
        assert_eq!(res.result, CheckResult::UNKNOWN);
        assert!(
            res.timestamp.sec.abs_diff(now.sec) <= 5,
            "{res:?} vs {now:?}"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_rate_limit() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-rate");
    guarded(&ioctx, &[&oid], async {
        otp::set(&ioctx, &oid, &[device("d")]).await.expect("set");
        let (before, t) = counter(&ioctx, &oid).await;
        let mut got = Vec::new();
        for token in ["w1", "w2", "w3", "w4", "w5"] {
            got.push(verdict(&ioctx, &oid, "d", &wrong(t), token).await);
        }
        got.push(verdict(&ioctx, &oid, "d", &code(t), "r").await);
        let after = otp::get_current_time(&ioctx, &oid).await.expect("time");
        assert_eq!(
            got,
            [
                CheckResult::FAIL,
                CheckResult::FAIL,
                CheckResult::FAIL,
                CheckResult::FAIL,
                CheckResult::FAIL,
                CheckResult::UNKNOWN,
            ],
            "the sixth check in a step is dropped; clock {before:?} .. {after:?}"
        );
        let res = otp::get_result(&ioctx, &oid, "d", "w1")
            .await
            .expect("get_result w1");
        assert_eq!(res.result, CheckResult::FAIL);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_upsert_keeps_replay_state() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-upsert");
    guarded(&ioctx, &[&oid], async {
        otp::set(&ioctx, &oid, &[device("d")]).await.expect("set");
        let (_, t) = counter(&ioctx, &oid).await;
        assert_eq!(
            verdict(&ioctx, &oid, "d", &code(t), "u1").await,
            CheckResult::SUCCESS
        );
        let narrower = OtpInfo {
            window: 1,
            ..device("d")
        };
        otp::set(&ioctx, &oid, &[narrower]).await.expect("upsert");
        let got = otp::get_one(&ioctx, &oid, "d").await.expect("get_one");
        assert_eq!(got.window, 1);
        assert_eq!(
            verdict(&ioctx, &oid, "d", &code(t), "u2").await,
            CheckResult::FAIL,
            "last_success survived the upsert"
        );
        assert_eq!(
            verdict(&ioctx, &oid, "d", &code(t + 1), "u3").await,
            CheckResult::SUCCESS
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_get_result_decodes_the_check_request() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-get-result-op");
    guarded(&ioctx, &[&oid], async {
        otp::set(&ioctx, &oid, &[device("d")]).await.expect("set");
        // A cls_otp_get_result_op{token "tok"}: ENCODE_START(1, 1), string.
        let get_result_op = Bytes::from(unhex("01010700000003000000746f6b"));
        let err = ioctx
            .exec(&oid, "otp", "otp_get_result", get_result_op)
            .await
            .expect_err("a get-result op body");
        assert!(is_osd_error(&err, EINVAL), "{err:?}");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn otp_result_expires() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-otp-expires");
    guarded(&ioctx, &[&oid], async {
        let two_seconds = OtpInfo {
            step_size: 2,
            ..device("e")
        };
        otp::set(&ioctx, &oid, &[two_seconds]).await.expect("set");
        assert_eq!(
            verdict(&ioctx, &oid, "e", "12345", "x").await,
            CheckResult::FAIL,
            "five digits fail validation"
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
        let res = otp::get_result(&ioctx, &oid, "e", "x")
            .await
            .expect("get_result");
        assert_eq!(res.result, CheckResult::UNKNOWN, "{res:?}");
    })
    .await;
}
