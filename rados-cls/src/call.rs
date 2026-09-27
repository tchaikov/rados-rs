//! The one shape every class call takes: encode the request struct, send
//! a `CALL` op naming the class and method, decode the reply struct.

use bytes::Bytes;
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, encode_with_capacity};

/// A `CALL` op for `class::method` carrying `req`, for a compound operation.
pub(crate) fn op<R: Denc>(class: &str, method: &str, req: &R) -> Result<OSDOp> {
    raw_op(class, method, encode_with_capacity(req, 0)?)
}

/// A `CALL` op for `class::method` carrying `indata` as already encoded.
pub(crate) fn raw_op(class: &str, method: &str, indata: Bytes) -> Result<OSDOp> {
    OSDOp::call(class, method, indata)
}

/// Send one class call on `oid` and return the method's raw reply.
pub(crate) async fn exec<R: Denc>(
    ioctx: &IoCtx,
    oid: &str,
    class: &str,
    method: &str,
    req: &R,
) -> Result<Bytes> {
    exec_raw(ioctx, oid, class, method, encode_with_capacity(req, 0)?).await
}

/// Send one class call on `oid` with `indata` as already encoded and
/// return the method's raw reply.
pub(crate) async fn exec_raw(
    ioctx: &IoCtx,
    oid: &str,
    class: &str,
    method: &str,
    indata: Bytes,
) -> Result<Bytes> {
    ioctx.exec(oid, class, method, indata).await
}

/// Send one class call on `oid` as a write whose reply is wanted:
/// `OpBuilder::returnvec` keeps the method's output data, which the OSD
/// otherwise clears on a successful write, up to
/// `osd_max_write_op_reply_len` (64 bytes by default; more is
/// `EOVERFLOW`). The `user` class's `reset_user_stats2` and the
/// `2pc_queue` class's `2pc_queue_reserve` are such methods.
#[cfg(any(feature = "user", feature = "two_pc_queue"))]
pub(crate) async fn exec_returnvec<R: Denc>(
    ioctx: &IoCtx,
    oid: &str,
    class: &str,
    method: &str,
    req: &R,
) -> Result<Bytes> {
    let op = rados::OpBuilder::new()
        .op(raw_op(class, method, encode_with_capacity(req, 0)?)?)
        .returnvec()
        .build();
    let result = ioctx.execute_op(oid, op).await?;
    Ok(result.first_outdata()?.clone())
}

/// Decode a reply struct from an op's outdata.
pub(crate) fn decode<T: Denc>(reply: &OpReply) -> Result<T> {
    decode_bytes(reply.outdata.clone())
}

/// Decode a reply struct from raw outdata.
pub(crate) fn decode_bytes<T: Denc>(mut out: Bytes) -> Result<T> {
    Ok(T::decode(&mut out, 0)?)
}
