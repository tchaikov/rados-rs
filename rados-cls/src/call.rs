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

/// Decode a reply struct from an op's outdata.
pub(crate) fn decode<T: Denc>(reply: &OpReply) -> Result<T> {
    decode_bytes(reply.outdata.clone())
}

/// Decode a reply struct from raw outdata.
pub(crate) fn decode_bytes<T: Denc>(mut out: Bytes) -> Result<T> {
    Ok(T::decode(&mut out, 0)?)
}
