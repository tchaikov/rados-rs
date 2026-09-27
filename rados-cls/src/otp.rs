//! The `otp` class: the TOTP devices radosgw checks S3 MFA against.
//! Mirrors `cls_otp_client.h`.
//!
//! radosgw keeps one object per user, `user:<uid>`, in the zone's otp
//! pool (`<zone>.rgw.otp` by default), and reaches it through
//! `RGWSI_Cls::MFA`. A request whose `x-amz-mfa: <serial> <pin>` names
//! one of the user's devices (`mfa_ids`) runs one `otp_check` and one
//! `otp_get_result`; any other serial is `EACCES` without touching the
//! object. Only a `SUCCESS` marks the request MFA-verified, and a failed
//! check leaves it unverified rather than rejecting it; changing
//! `MfaDelete`, changing versioning on an MFA-enabled bucket, and deleting
//! an object version there (singly or in a multi-object delete) require
//! that mark.
//!
//! Server facts (`src/cls/otp/cls_otp.cc`):
//! - The object's omap holds a `header` key listing the device ids and
//!   one `otp/<id>` key per device; the class keeps nothing in xattrs.
//!   radosgw's `prepare_mfa_write` adds a `cls_version` check and bump and
//!   an `mtime2` to every create, remove and set, and a driver writing
//!   devices alongside radosgw must do the same.
//! - `otp_set` upserts: it parses `seed` into `seed_bin` (base32 when
//!   `seed_type` is `BASE32`, hex otherwise; a seed that does not parse is
//!   `EINVAL`), replaces the device's settings and keeps its recorded
//!   checks and replay index. It validates neither `type` nor
//!   `step_size`. liboath treats a zero step as its 30 s default and a
//!   wrong code returns before the OSD's own division, so the OSD divides
//!   by zero on the first check whose code matches at 30 s; this module
//!   refuses a zero `step_size`.
//! - `otp_remove` skips ids it does not hold and succeeds.
//! - `otp_get` returns whole entries, the seed and `seed_bin` in
//!   cleartext; with `get_all` they come sorted by id; ids it does not
//!   hold are skipped.
//! - `otp_check` returns 0 whether the code is right or wrong (`ENOENT`
//!   for an unknown id). It validates against the primary OSD's clock,
//!   which `get_current_time` reports: TOTP (HMAC-SHA1) over `seed_bin`
//!   at counter `(now - time_ofs) / step_size`, within `window` steps
//!   either side, as many digits as the code has (six and eight work;
//!   five or none fail). It records `{token, now, SUCCESS|FAIL}`.
//! - Rate limit: recorded checks older than `step_size` seconds are
//!   dropped; while five remain, a check records nothing and still returns
//!   0, so its token reads back `UNKNOWN`.
//! - Replay guard: a match records `last_success = distance + counter`,
//!   the distance being the absolute number of steps between the code and
//!   now, and any later match at or below it fails. A code from one step
//!   back therefore records one past the current counter, and the current
//!   and next steps' codes then fail. Conversely a code from a past step
//!   is still accepted after the current one: after `code(t)` records
//!   `t`, `code(t-1)` records `t+1` and `code(t-2)` records `t+2`, so
//!   anyone who has seen an older code in the window can use it once after
//!   the legitimate one.
//! - `otp_get_result` takes a `cls_otp_check_otp_op` (the C++ client's
//!   `cls_otp_get_result_op` never reaches the wire) and returns the
//!   newest recorded check for the token, or `{token, now, UNKNOWN}`;
//!   a recorded check expires after `step_size` seconds.
//! - `otp_get`, `otp_get_result` and `get_current_time` on a missing
//!   object are `ENOENT`.
//!
//! Seeds are secrets and Ceph never redacts them: they travel in
//! cleartext in `otp_set` requests and `otp_get` replies, and
//! [`OtpInfo`]'s JSON prints `seed` as `otp_info_t::dump` does.

use bytes::{Buf, BufMut, Bytes};
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{
    OSDClientError, RadosError, UTime, VersionedDenc, VersionedEncode, encode_with_capacity,
};
use serde::Serialize;

use crate::call;

/// The class name.
pub const CLASS: &str = "otp";

byte_enum! {
    /// `rados::cls::otp::OTPType`. The class never reads it: every device
    /// is validated as TOTP, so `HOTP` is stored but never honoured.
    OtpType { UNKNOWN = 0, HOTP = 1, TOTP = 2 }
}

byte_enum! {
    /// `SeedType`. `otp_set` parses `BASE32` as base32 and anything else,
    /// `UNKNOWN` included, as hex.
    SeedType { UNKNOWN = 0, HEX = 1, BASE32 = 2 }
}

byte_enum! {
    /// `OTPCheckResult`: `UNKNOWN` is also the answer for a token the
    /// class holds no check for (never recorded, rate-limited, expired).
    CheckResult { UNKNOWN = 0, SUCCESS = 1, FAIL = 2 }
}

impl SeedType {
    /// The name `otp_info_t::dump` prints; a value v19 does not know is
    /// `"unknown"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HEX => "hex",
            Self::BASE32 => "base32",
            _ => "unknown",
        }
    }
}

fn seed_type_str<S: serde::Serializer>(
    seed_type: &SeedType,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(seed_type.as_str())
}

/// `otp_info_t`: one device. The JSON follows `otp_info_t::dump`: `type`
/// as a number and `seed` in cleartext; `seed_bin` is not dumped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct OtpInfo {
    #[serde(rename = "type")]
    pub otp_type: OtpType,
    pub id: String,
    pub seed: String,
    #[serde(serialize_with = "seed_type_str")]
    pub seed_type: SeedType,
    /// Filled, and overwritten, by `otp_set` from `seed`; a client sends it
    /// empty.
    #[serde(skip)]
    pub seed_bin: Bytes,
    pub time_ofs: i32,
    pub step_size: u32,
    pub window: u32,
}

/// The C++ defaults: a TOTP device with a 30 second step and a window of
/// two steps either side.
impl Default for OtpInfo {
    fn default() -> Self {
        Self {
            otp_type: OtpType::TOTP,
            id: String::new(),
            seed: String::new(),
            seed_type: SeedType::UNKNOWN,
            seed_bin: Bytes::new(),
            time_ofs: 0,
            step_size: 30,
            window: 2,
        }
    }
}

impl OtpInfo {
    /// A TOTP device with the C++ defaults for everything else, as the
    /// `TOTPConfig` builder makes.
    pub fn totp(id: &str, seed: &str, seed_type: SeedType) -> Self {
        Self {
            id: id.to_owned(),
            seed: seed.to_owned(),
            seed_type,
            ..Self::default()
        }
    }
}

/// `otp_check_t`: one recorded check.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct OtpCheck {
    pub token: String,
    pub timestamp: UTime,
    pub result: CheckResult,
}

/// `cls_otp_set_otp_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct SetOp {
    pub entries: Vec<OtpInfo>,
}

/// `cls_otp_check_otp_op`; also the request of `otp_get_result`, which
/// reads only `id` and `token`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct CheckOp {
    pub id: String,
    pub val: String,
    pub token: String,
}

/// `cls_otp_get_result_reply`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetResultReply {
    pub result: OtpCheck,
}

/// `cls_otp_remove_otp_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct RemoveOp {
    pub ids: Vec<String>,
}

/// `cls_otp_get_otp_op`: `get_all` ignores `ids`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetOp {
    pub get_all: bool,
    pub ids: Vec<String>,
}

/// `cls_otp_get_otp_reply`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetReply {
    pub found_entries: Vec<OtpInfo>,
}

/// `cls_otp_get_current_time_op`: an empty struct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetCurrentTimeOp {}

impl VersionedEncode for GetCurrentTimeOp {
    const MAX_DECODE_VERSION: u8 = 1;

    fn encoding_version(&self, _features: u64) -> u8 {
        1
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        _buf: &mut B,
        _features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        Ok(())
    }

    fn decode_content<B: Buf>(
        _buf: &mut B,
        _features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 1, "GetCurrentTimeOp", "Mimic v13+");
        Ok(Self {})
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(0)
    }
}

rados::impl_denc_for_versioned!(GetCurrentTimeOp);

/// `cls_otp_get_current_time_reply`: the primary OSD's clock.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetCurrentTimeReply {
    pub time: UTime,
}

fn refuse_zero_step(entries: &[OtpInfo]) -> Result<()> {
    match entries.iter().find(|e| e.step_size == 0) {
        Some(e) => Err(OSDClientError::Other(format!(
            "otp device {:?}: step_size must be non-zero",
            e.id
        ))),
        None => Ok(()),
    }
}

fn set_request(entries: &[OtpInfo]) -> Result<SetOp> {
    refuse_zero_step(entries)?;
    Ok(SetOp {
        entries: entries.to_vec(),
    })
}

fn owned(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|&id| id.to_owned()).collect()
}

fn check_request(id: &str, val: &str, token: &str) -> CheckOp {
    CheckOp {
        id: id.to_owned(),
        val: val.to_owned(),
        token: token.to_owned(),
    }
}

fn get_request(ids: &[&str]) -> GetOp {
    GetOp {
        get_all: false,
        ids: owned(ids),
    }
}

/// `otp_set`: upsert `entries`. A zero `step_size` is refused here and
/// nothing is sent; see [`set`].
pub fn set_op(entries: &[OtpInfo]) -> Result<OSDOp> {
    call::op(CLASS, "otp_set", &set_request(entries)?)
}

/// [`set_op`] of one device.
pub fn create_op(info: &OtpInfo) -> Result<OSDOp> {
    set_op(std::slice::from_ref(info))
}

/// `otp_remove`: ids the object does not hold are skipped.
pub fn remove_op(ids: &[&str]) -> Result<OSDOp> {
    call::op(CLASS, "otp_remove", &RemoveOp { ids: owned(ids) })
}

/// `otp_check`: record a check of `val` under `token`; see [`check`].
pub fn check_op(id: &str, val: &str, token: &str) -> Result<OSDOp> {
    call::op(CLASS, "otp_check", &check_request(id, val, token))
}

/// `otp_get_result` for `token`; decode its reply with
/// [`decode_get_result`].
pub fn get_result_op(id: &str, token: &str) -> Result<OSDOp> {
    call::op(CLASS, "otp_get_result", &check_request(id, "", token))
}

/// `otp_get` of `ids`; decode its reply with [`decode_get`].
pub fn get_op(ids: &[&str]) -> Result<OSDOp> {
    call::op(CLASS, "otp_get", &get_request(ids))
}

/// `otp_get` of every device; decode its reply with [`decode_get`].
pub fn get_all_op() -> Result<OSDOp> {
    call::op(
        CLASS,
        "otp_get",
        &GetOp {
            get_all: true,
            ids: Vec::new(),
        },
    )
}

/// `get_current_time`; decode its reply with [`decode_current_time`].
pub fn get_current_time_op() -> Result<OSDOp> {
    call::op(CLASS, "get_current_time", &GetCurrentTimeOp {})
}

/// Decode the reply to [`get_op`] or [`get_all_op`].
pub fn decode_get(reply: &OpReply) -> Result<Vec<OtpInfo>> {
    Ok(call::decode::<GetReply>(reply)?.found_entries)
}

/// Decode the reply to [`get_result_op`].
pub fn decode_get_result(reply: &OpReply) -> Result<OtpCheck> {
    Ok(call::decode::<GetResultReply>(reply)?.result)
}

/// Decode the reply to [`get_current_time_op`].
pub fn decode_current_time(reply: &OpReply) -> Result<UTime> {
    Ok(call::decode::<GetCurrentTimeReply>(reply)?.time)
}

/// Upsert `entries` on `oid`, keeping each existing device's recorded
/// checks and replay index. The class validates neither `step_size` nor
/// `type`. With a zero step liboath uses its 30 s default and a wrong code
/// returns before the OSD's division, so the OSD divides by zero on the
/// device's first check whose code matches at 30 s; a zero `step_size` is
/// therefore refused here and nothing is sent.
pub async fn set(ioctx: &IoCtx, oid: &str, entries: &[OtpInfo]) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "otp_set", &set_request(entries)?)
        .await
        .map(drop)
}

/// [`set`] of one device.
pub async fn create(ioctx: &IoCtx, oid: &str, info: &OtpInfo) -> Result<()> {
    set(ioctx, oid, std::slice::from_ref(info)).await
}

/// Remove `ids` from `oid`; ids it does not hold are skipped.
pub async fn remove(ioctx: &IoCtx, oid: &str, ids: &[&str]) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "otp_remove",
        &RemoveOp { ids: owned(ids) },
    )
    .await
    .map(drop)
}

/// The devices of `ids` that `oid` holds, seeds in cleartext.
pub async fn get(ioctx: &IoCtx, oid: &str, ids: &[&str]) -> Result<Vec<OtpInfo>> {
    let out = call::exec(ioctx, oid, CLASS, "otp_get", &get_request(ids)).await?;
    Ok(call::decode_bytes::<GetReply>(out)?.found_entries)
}

/// The device `id`; `ENOENT` if `oid` does not hold it, as the C++
/// wrapper answers.
pub async fn get_one(ioctx: &IoCtx, oid: &str, id: &str) -> Result<OtpInfo> {
    get(ioctx, oid, &[id])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| OSDClientError::OSDError {
            code: -2,
            message: format!("otp device {id:?} not found"),
        })
}

/// Every device on `oid`, sorted by id, seeds in cleartext.
pub async fn get_all(ioctx: &IoCtx, oid: &str) -> Result<Vec<OtpInfo>> {
    let req = GetOp {
        get_all: true,
        ids: Vec::new(),
    };
    let out = call::exec(ioctx, oid, CLASS, "otp_get", &req).await?;
    Ok(call::decode_bytes::<GetReply>(out)?.found_entries)
}

/// The primary OSD's clock, which `otp_check` validates against.
pub async fn get_current_time(ioctx: &IoCtx, oid: &str) -> Result<UTime> {
    let out = call::exec(ioctx, oid, CLASS, "get_current_time", &GetCurrentTimeOp {}).await?;
    Ok(call::decode_bytes::<GetCurrentTimeReply>(out)?.time)
}

/// Check `val` against device `id` and record the verdict under `token`.
/// Returns `Ok(())` for a right and a wrong code alike (`ENOENT` for an
/// unknown id); the verdict is read with [`get_result`].
///
/// The caller supplies `token`, which should be unique per check, since
/// `otp_get_result` returns the newest recorded check with that token.
/// The C++ client sends 16 characters drawn from `A-Za-z0-9`, `-` and `_`
/// (`gen_rand_alphanumeric`); a driver that mirrors radosgw draws the
/// same.
pub async fn check(ioctx: &IoCtx, oid: &str, id: &str, val: &str, token: &str) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "otp_check",
        &check_request(id, val, token),
    )
    .await
    .map(drop)
}

/// The newest recorded check of device `id` under `token`, or `{token,
/// now, UNKNOWN}` when there is none.
pub async fn get_result(ioctx: &IoCtx, oid: &str, id: &str, token: &str) -> Result<OtpCheck> {
    let req = check_request(id, "", token);
    let out = call::exec(ioctx, oid, CLASS, "otp_get_result", &req).await?;
    Ok(call::decode_bytes::<GetResultReply>(out)?.result)
}

/// [`check`] then [`get_result`] with byte-identical request data, as the
/// C++ client's `OTP::check` sends. `UNKNOWN` normally means the rate
/// limit dropped the check; it also comes back when the recorded check
/// expired before `otp_get_result` arrived, a `step_size` shorter than the
/// round trip.
///
/// The caller supplies `token`, which should be unique per check, since
/// `otp_get_result` returns the newest recorded check with that token.
/// The C++ client sends 16 characters drawn from `A-Za-z0-9`, `-` and `_`
/// (`gen_rand_alphanumeric`); a driver that mirrors radosgw draws the
/// same.
pub async fn check_and_get_result(
    ioctx: &IoCtx,
    oid: &str,
    id: &str,
    val: &str,
    token: &str,
) -> Result<OtpCheck> {
    let indata = encode_with_capacity(&check_request(id, val, token), 0)?;
    call::exec_raw(ioctx, oid, CLASS, "otp_check", indata.clone()).await?;
    let out = call::exec_raw(ioctx, oid, CLASS, "otp_get_result", indata).await?;
    Ok(call::decode_bytes::<GetResultReply>(out)?.result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::Denc;
    use rados::osdclient::types::OpData;

    fn unhex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ascii"), 16).expect("hex")
            })
            .collect()
    }

    fn enc<T: Denc>(v: &T) -> Vec<u8> {
        encode_with_capacity(v, 0).expect("encode").to_vec()
    }

    fn dec<T: Denc>(b: &[u8]) -> T {
        let mut buf = Bytes::copy_from_slice(b);
        let v = T::decode(&mut buf, 0).expect("decode");
        assert!(buf.is_empty(), "trailing bytes");
        v
    }

    fn pin<T: Denc + PartialEq + std::fmt::Debug>(v: &T, hex: &str) {
        assert_eq!(enc(v), unhex(hex));
        assert_eq!(&dec::<T>(&unhex(hex)), v);
    }

    fn a() -> OtpInfo {
        OtpInfo {
            otp_type: OtpType::TOTP,
            id: "dev1".to_owned(),
            seed: "3132".to_owned(),
            seed_type: SeedType::HEX,
            seed_bin: Bytes::new(),
            time_ofs: 0,
            step_size: 30,
            window: 2,
        }
    }

    fn a2() -> OtpInfo {
        OtpInfo {
            seed_bin: Bytes::from_static(b"12"),
            ..a()
        }
    }

    fn b() -> OtpInfo {
        OtpInfo {
            otp_type: OtpType::TOTP,
            id: "dev2".to_owned(),
            seed: "GEZDGNBV".to_owned(),
            seed_type: SeedType::BASE32,
            seed_bin: Bytes::new(),
            time_ofs: -30,
            step_size: 60,
            window: 1,
        }
    }

    fn chk() -> OtpCheck {
        OtpCheck {
            token: "tok".to_owned(),
            timestamp: UTime { sec: 1, nsec: 2 },
            result: CheckResult::SUCCESS,
        }
    }

    // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
    const A_HEX: &str =
        "01012200000002040000006465763104000000333133320100000000000000001e00000002000000";
    // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
    const A2_HEX: &str =
        "010124000000020400000064657631040000003331333201020000003132000000001e00000002000000";
    // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
    const B_HEX: &str =
        "0101260000000204000000646576320800000047455a44474e42560200000000e2ffffff3c00000001000000";
    // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
    const CHK_HEX: &str = "01011000000003000000746f6b010000000200000001";
    // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
    const CHECK_OP_HEX: &str = "01011900000004000000646576310600000031323334353603000000746f6b";
    // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
    const GET_RESULT_REPLY_HEX: &str = "01011600000001011000000003000000746f6b010000000200000001";
    // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
    const GET_REPLY_HEX: &str = "01012e00000001000000010124000000020400000064657631040000003331333201020000003132000000001e00000002000000";
    // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
    const TIME_REPLY_HEX: &str = "01010800000000f1536515cd5b07";

    #[test]
    fn otp_info_encodes_as_derived() {
        assert_eq!(unhex(A_HEX).len(), 40);
        assert_eq!(unhex(A2_HEX).len(), 42);
        assert_eq!(unhex(B_HEX).len(), 44);
        pin(&a(), A_HEX);
        pin(&a2(), A2_HEX);
        pin(&b(), B_HEX);
    }

    #[test]
    fn otp_info_json_matches_dump() {
        let want = serde_json::json!({
            "type": 2, "id": "dev1", "seed": "3132", "seed_type": "hex",
            "time_ofs": 0, "step_size": 30, "window": 2
        });
        assert_eq!(serde_json::to_value(a()).expect("json"), want);
        assert_eq!(serde_json::to_value(a2()).expect("json"), want);
    }

    #[test]
    fn enums_keep_unknown_bytes() {
        let mut wire = unhex(A_HEX);
        assert_eq!(wire[23], 1);
        wire[23] = 7;
        let info: OtpInfo = dec(&wire);
        assert_eq!(info.seed_type, SeedType(7));
        assert_eq!(enc(&info), wire);
        let json = serde_json::to_value(&info).expect("json");
        assert_eq!(json["seed_type"], "unknown");

        let mut wire = unhex(A_HEX);
        assert_eq!(wire[6], 2);
        wire[6] = 9;
        let info: OtpInfo = dec(&wire);
        assert_eq!(info.otp_type, OtpType(9));
        assert_eq!(enc(&info), wire);
        let json = serde_json::to_value(&info).expect("json");
        assert_eq!(json["type"], 9);

        let d = OtpInfo::default();
        assert_eq!(
            (d.otp_type, d.seed_type, d.time_ofs, d.step_size, d.window),
            (OtpType::TOTP, SeedType::UNKNOWN, 0, 30, 2)
        );
    }

    #[test]
    fn otp_check_encodes_as_derived() {
        assert_eq!(unhex(CHK_HEX).len(), 22);
        pin(&chk(), CHK_HEX);
    }

    #[test]
    fn requests_encode_as_derived() {
        // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
        pin(
            &SetOp { entries: vec![a()] },
            "01012c0000000100000001012200000002040000006465763104000000333133320100000000000000001e00000002000000",
        );
        pin(&check_request("dev1", "123456", "tok"), CHECK_OP_HEX);
        // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
        pin(
            &RemoveOp {
                ids: vec!["dev1".to_owned()],
            },
            "01010c000000010000000400000064657631",
        );
        // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
        pin(
            &GetOp {
                get_all: true,
                ids: Vec::new(),
            },
            "0101050000000100000000",
        );
        // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
        pin(
            &get_request(&["dev1"]),
            "01010d00000000010000000400000064657631",
        );
        // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
        pin(&GetCurrentTimeOp {}, "010100000000");
    }

    #[test]
    fn replies_decode_as_derived() {
        pin(&GetResultReply { result: chk() }, GET_RESULT_REPLY_HEX);
        pin(
            &GetReply {
                found_entries: vec![a2()],
            },
            GET_REPLY_HEX,
        );
        // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
        pin(&GetReply::default(), "01010400000000000000");
        pin(
            &GetCurrentTimeReply {
                time: UTime {
                    sec: 1_700_000_000,
                    nsec: 123_456_789,
                },
            },
            TIME_REPLY_HEX,
        );
    }

    #[test]
    fn get_current_time_op_refuses_version_zero() {
        let mut buf = Bytes::from(unhex("000000000000"));
        assert!(GetCurrentTimeOp::decode(&mut buf, 0).is_err());
    }

    fn indata(method: &str, req_hex: &str) -> Vec<u8> {
        let mut want = format!("otp{method}").into_bytes();
        want.extend(unhex(req_hex));
        want
    }

    #[test]
    fn ops_frame_the_class_method_and_request() {
        let op = check_op("dev1", "123456", "tok").expect("op");
        assert_eq!(&op.indata[..], &indata("otp_check", CHECK_OP_HEX)[..]);
        assert!(matches!(
            op.op_data,
            OpData::Call {
                class_len: 3,
                method_len: 9,
                indata_len: 31,
            }
        ));

        // Hand-derived from the ENCODE_* code (report §2); no ceph-dencoder type exists.
        let empty_val = "01011300000004000000646576310000000003000000746f6b";
        assert_eq!(unhex(empty_val).len(), 25);
        let op = get_result_op("dev1", "tok").expect("op");
        assert_eq!(&op.indata[..], &indata("otp_get_result", empty_val)[..]);

        assert!(get_all_op().expect("op").indata.starts_with(b"otpotp_get"));
        assert!(
            remove_op(&["dev1"])
                .expect("op")
                .indata
                .starts_with(b"otpotp_remove")
        );
        let op = get_current_time_op().expect("op");
        assert_eq!(
            &op.indata[..],
            &indata("get_current_time", "010100000000")[..]
        );
    }

    fn reply(hex: &str) -> OpReply {
        OpReply {
            return_code: 0,
            outdata: Bytes::from(unhex(hex)),
        }
    }

    #[test]
    fn decoders_unwrap_the_replies() {
        assert_eq!(decode_get(&reply(GET_REPLY_HEX)).expect("get"), vec![a2()]);
        assert_eq!(
            decode_get_result(&reply(GET_RESULT_REPLY_HEX)).expect("result"),
            chk()
        );
        assert_eq!(
            decode_current_time(&reply(TIME_REPLY_HEX)).expect("time"),
            UTime {
                sec: 1_700_000_000,
                nsec: 123_456_789,
            }
        );
    }

    #[test]
    fn zero_step_size_is_refused_before_sending() {
        let zero_b = OtpInfo {
            step_size: 0,
            ..b()
        };
        match set_op(&[a(), zero_b]) {
            Err(OSDClientError::Other(msg)) => {
                assert!(msg.contains("step_size") && msg.contains("dev2"), "{msg}");
            }
            other => panic!("{other:?}"),
        }
        let zero_a = OtpInfo {
            step_size: 0,
            ..a()
        };
        match create_op(&zero_a) {
            Err(OSDClientError::Other(msg)) => {
                assert!(msg.contains("step_size") && msg.contains("dev1"), "{msg}");
            }
            other => panic!("{other:?}"),
        }
    }
}
