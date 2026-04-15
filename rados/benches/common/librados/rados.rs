use std::ffi::CString;
use std::ptr;

use super::error::{RadosError, RadosResult};
use super::ffi::{
    rados_conf_read_file, rados_connect, rados_create, rados_pool_create, rados_pool_delete,
    rados_shutdown, rados_t,
};

#[derive(Debug)]
pub struct Rados {
    pub(crate) rados: rados_t,
    is_connected: bool,
}

unsafe impl Send for Rados {}
unsafe impl Sync for Rados {}

impl Drop for Rados {
    fn drop(&mut self) {
        if self.is_connected() {
            unsafe {
                rados_shutdown(self.rados);
            }
            self.rados = ptr::null_mut();
            self.is_connected = false;
        }
    }
}

impl Rados {
    pub fn new(user_id: &str) -> RadosResult<Self> {
        let connect_id = CString::new(user_id)?;
        unsafe {
            let mut rados: rados_t = ptr::null_mut();
            let ret_code = rados_create(&mut rados, connect_id.as_ptr());
            RadosError::from_retval(ret_code)?;
            assert!(!rados.is_null(), "rados_create returned null handle");
            Ok(Self {
                rados,
                is_connected: false,
            })
        }
    }

    pub fn read_conf_file(&mut self, config_file: &str) -> RadosResult<()> {
        let conf_file = CString::new(config_file)?;
        unsafe {
            let ret_code = rados_conf_read_file(self.rados, conf_file.as_ptr());
            RadosError::from_retval(ret_code)?;
            Ok(())
        }
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected
    }

    pub async fn connect(mut self) -> RadosResult<Self> {
        assert!(!self.is_connected());

        // rados_connect is a blocking call; hand it to tokio's blocking pool
        // so we don't stall the runtime while librados does its monitor
        // handshake.
        tokio::task::spawn_blocking(move || {
            unsafe {
                let ret_code = rados_connect(self.rados);
                RadosError::from_retval(ret_code)?;
            }
            self.is_connected = true;
            Ok(self)
        })
        .await
        .expect("rados_connect blocking task panicked")
    }

    pub fn create_pool(&self, pool_name: &str) -> RadosResult<()> {
        assert!(self.is_connected());
        let pool_name_str = CString::new(pool_name)?;
        unsafe {
            let ret_code = rados_pool_create(self.rados, pool_name_str.as_ptr());
            RadosError::from_retval(ret_code)?;
            Ok(())
        }
    }

    pub fn delete_pool(&self, pool_name: &str) -> RadosResult<()> {
        assert!(self.is_connected());
        let pool_name_str = CString::new(pool_name)?;
        unsafe {
            let ret_code = rados_pool_delete(self.rados, pool_name_str.as_ptr());
            RadosError::from_retval(ret_code)?;
        }
        Ok(())
    }
}
