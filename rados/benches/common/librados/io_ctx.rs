use super::completion::with_completion;
use super::error::{RadosError, RadosResult};
use super::ffi::*;
use super::rados::Rados;
use libc::time_t;
use std::ffi::CString;
use std::ptr;
use std::sync::Arc;

#[derive(Debug)]
pub struct IoCtx {
    pub(crate) ioctx: rados_ioctx_t,
    // Keep the rados client alive; ioctx depends on it.
    #[allow(dead_code)]
    pub(crate) rados: Arc<Rados>,
}

unsafe impl Send for IoCtx {}
unsafe impl Sync for IoCtx {}

impl Drop for IoCtx {
    fn drop(&mut self) {
        unsafe {
            rados_ioctx_destroy(self.ioctx);
        }
    }
}

impl IoCtx {
    pub fn from_rados(rados: Arc<Rados>, pool_name: &str) -> RadosResult<IoCtx> {
        let pool_name_str = CString::new(pool_name)?;
        unsafe {
            let mut ioctx: rados_ioctx_t = ptr::null_mut();
            let ret_code = rados_ioctx_create(rados.rados, pool_name_str.as_ptr(), &mut ioctx);
            RadosError::from_retval(ret_code)?;
            assert!(!ioctx.is_null(), "rados_ioctx_create returned null handle");
            Ok(Self { ioctx, rados })
        }
    }

    pub async fn stat(&self, object_name: &str) -> RadosResult<(u64, time_t)> {
        let obj_name_cstr = CString::new(object_name)?;

        let mut size = Box::new(0u64);
        let mut mtime: Box<time_t> = Box::new(0 as time_t);
        let size_ptr: *mut u64 = &mut *size;
        let mtime_ptr: *mut time_t = &mut *mtime;

        let completion = with_completion(self, move |c| unsafe {
            rados_aio_stat(self.ioctx, obj_name_cstr.as_ptr(), c, size_ptr, mtime_ptr)
        })?;

        completion.await?;

        Ok((*size, *mtime))
    }

    pub async fn remove(&self, object_name: &str) -> RadosResult<()> {
        let obj_name_cstr = CString::new(object_name)?;

        let completion = with_completion(self, move |c| unsafe {
            rados_aio_remove(self.ioctx, obj_name_cstr.as_ptr(), c)
        })?;

        completion.await?;

        Ok(())
    }

    pub async fn write_full(&self, object_name: &str, data: &[u8]) -> RadosResult<()> {
        let obj_name_cstr = CString::new(object_name)?;

        // Soundness: `rados_aio_write_full` synchronously copies `buf` into an
        // internal bufferlist (`bl.append(buf, len)` in librados_c.cc) before
        // queuing the AIO, so the caller's slice only needs to outlive the
        // synchronous call to the closure — not the completion future. This
        // is the opposite of `rados_aio_write`, which requires the caller's
        // buffer to live until the completion fires. Do NOT add a defensive
        // `to_vec()` here: it would charge librados an extra 4 MiB memcpy
        // per op that a production librados caller would never pay, and
        // silently skew the A/B comparison against librados.
        let completion = with_completion(self, move |c| unsafe {
            rados_aio_write_full(
                self.ioctx,
                obj_name_cstr.as_ptr(),
                c,
                data.as_ptr() as *const libc::c_char,
                data.len(),
            )
        })?;

        completion.await?;

        Ok(())
    }

    pub async fn read(&self, object_name: &str, off: u64, buf: &mut [u8]) -> RadosResult<usize> {
        let obj_name_cstr = CString::new(object_name)?;
        let buf_ptr = buf.as_mut_ptr() as *mut libc::c_char;
        let buf_len = buf.len();

        let completion = with_completion(self, move |c| unsafe {
            rados_aio_read(self.ioctx, obj_name_cstr.as_ptr(), c, buf_ptr, buf_len, off)
        })?;

        let bytes_read = completion.await? as usize;
        Ok(bytes_read)
    }
}
