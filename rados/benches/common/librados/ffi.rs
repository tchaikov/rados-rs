// C names come straight from librados.h; don't rename them to snake_case.
#![allow(non_camel_case_types)]

// Signatures tracked against:
//   https://github.com/ceph/ceph/blob/v20.3.0/src/include/rados/librados.h

use libc::{size_t, time_t};

pub type rados_t = *mut ::std::os::raw::c_void;
pub type rados_ioctx_t = *mut ::std::os::raw::c_void;
pub type rados_completion_t = *mut ::std::os::raw::c_void;
pub type rados_callback_t = ::std::option::Option<
    unsafe extern "C" fn(cb: rados_completion_t, arg: *mut ::std::os::raw::c_void) -> (),
>;

#[cfg(unix)]
#[link(name = "rados", kind = "dylib")]
unsafe extern "C" {
    pub fn rados_create(cluster: *mut rados_t, id: *const ::libc::c_char) -> ::libc::c_int;

    // BUG (librados.h): calling anything that talks to the cluster before
    // `rados_connect` will crash the process, not error out.
    pub fn rados_connect(cluster: rados_t) -> ::libc::c_int;

    // Must `rados_aio_flush` every open ioctx before calling this, otherwise
    // pending writes silently disappear.
    pub fn rados_shutdown(cluster: rados_t);

    pub fn rados_conf_read_file(cluster: rados_t, path: *const ::libc::c_char) -> ::libc::c_int;

    pub fn rados_ioctx_create(
        cluster: rados_t,
        pool_name: *const ::libc::c_char,
        ioctx: *mut rados_ioctx_t,
    ) -> ::libc::c_int;

    // Same AIO-flush caveat as `rados_shutdown`: drain the ioctx first.
    pub fn rados_ioctx_destroy(io: rados_ioctx_t);

    pub fn rados_pool_create(cluster: rados_t, pool_name: *const ::libc::c_char) -> ::libc::c_int;

    pub fn rados_pool_delete(cluster: rados_t, pool_name: *const ::libc::c_char) -> ::libc::c_int;

    // BUG (librados.h): throws a C++ exception across the FFI boundary on
    // ENOMEM instead of returning an error code. Unwinding through Rust is
    // undefined behaviour, so in practice we rely on the allocator not
    // failing for 100-byte allocations.
    pub fn rados_aio_create_completion2(
        cb_arg: *mut ::std::os::raw::c_void,
        cb_complete: rados_callback_t,
        pc: *mut rados_completion_t,
    ) -> ::libc::c_int;

    pub fn rados_aio_is_complete(c: rados_completion_t) -> ::libc::c_int;

    pub fn rados_aio_wait_for_complete_and_cb(c: rados_completion_t) -> ::libc::c_int;

    pub fn rados_aio_release(c: rados_completion_t);

    // Callback-ordering gotcha (from librados.h): the "complete" callback
    // may never fire when a "safe" message arrives first. We use
    // `rados_aio_wait_for_complete_and_cb` above to sidestep this.
    pub fn rados_aio_get_return_value(c: rados_completion_t) -> ::libc::c_int;

    pub fn rados_aio_cancel(io: rados_ioctx_t, completion: rados_completion_t) -> ::libc::c_int;

    pub fn rados_aio_write_full(
        io: rados_ioctx_t,
        oid: *const ::libc::c_char,
        completion: rados_completion_t,
        buf: *const ::libc::c_char,
        len: size_t,
    ) -> ::libc::c_int;

    pub fn rados_aio_remove(
        io: rados_ioctx_t,
        oid: *const ::libc::c_char,
        completion: rados_completion_t,
    ) -> ::libc::c_int;

    pub fn rados_aio_read(
        io: rados_ioctx_t,
        oid: *const ::libc::c_char,
        completion: rados_completion_t,
        buf: *mut ::libc::c_char,
        len: size_t,
        off: u64,
    ) -> ::libc::c_int;

    pub fn rados_aio_stat(
        io: rados_ioctx_t,
        o: *const ::libc::c_char,
        completion: rados_completion_t,
        psize: *mut u64,
        pmtime: *mut time_t,
    ) -> ::libc::c_int;
}
