use std::ffi;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum RadosError {
    #[error("io error: {0}")]
    IoError(#[from] io::Error),

    #[error("invalid string argument: {0}")]
    NulError(#[from] ffi::NulError),

    #[error("internal error: {msg}")]
    Internal {
        msg: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl RadosError {
    pub fn from_errno(ret: libc::c_int) -> Self {
        Self::IoError(io::Error::from_raw_os_error(ret))
    }
    pub fn from_retval(ret: libc::c_int) -> Result<(), Self> {
        if ret < 0 {
            Err(Self::from_errno(-ret))
        } else {
            Ok(())
        }
    }
}

pub type RadosResult<T> = Result<T, RadosError>;

impl From<i32> for RadosError {
    fn from(err: i32) -> RadosError {
        RadosError::from_errno(-err)
    }
}

impl From<RadosError> for io::Error {
    fn from(err: RadosError) -> Self {
        match err {
            RadosError::IoError(io_err) => io_err,
            RadosError::NulError(nul_err) => io::Error::new(io::ErrorKind::InvalidInput, nul_err),
            RadosError::Internal { msg, .. } => io::Error::other(msg),
        }
    }
}
