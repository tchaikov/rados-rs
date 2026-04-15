// Copyright 2021 John Spray All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::task::AtomicWaker;
use std::ffi::c_void;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::trace;

use super::error::{RadosError, RadosResult};
use super::ffi::{
    rados_aio_cancel, rados_aio_create_completion2, rados_aio_get_return_value,
    rados_aio_is_complete, rados_aio_release, rados_aio_wait_for_complete_and_cb,
    rados_completion_t,
};
use super::io_ctx::IoCtx;

/// A future representing an in-flight asynchronous RADOS operation.
///
/// This struct implements `Future` and will resolve to the result of the operation
/// once it completes. It ensures proper cleanup of RADOS resources even if dropped
/// before completion.
pub(crate) struct Completion<'a> {
    /// The underlying RADOS completion handle
    inner: rados_completion_t,

    /// Waker storage for the async runtime to notify us when the operation completes.
    /// Boxed for stable address, AtomicWaker for lock-free thread-safe access from callback.
    waker: Box<AtomicWaker>,

    /// Reference to the I/O context, needed for cancellation on drop.
    /// This ensures the IoCtx outlives the completion.
    ioctx: &'a IoCtx,
}

// Safety: Completion can be sent between threads because:
// - rados_completion_t is thread-safe (handled by librados)
// - AtomicWaker is Send
// - &IoCtx is Send (IoCtx must be Sync)
unsafe impl Send for Completion<'_> {}

/// Callback invoked by librados when an async operation completes.
///
/// # Safety
/// This function is called from C code and must be `extern "C"`.
/// The `arg` parameter must be a valid pointer to an `AtomicWaker`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn completion_complete(_cb: rados_completion_t, arg: *mut c_void) {
    // Safety: arg is guaranteed to be a valid pointer to our AtomicWaker
    // as long as it comes from a Completion we created
    let waker_ptr = arg as *mut AtomicWaker;
    let atomic_waker = unsafe { &*waker_ptr };

    atomic_waker.wake();
}

impl<'a> Completion<'a> {
    /// Creates a new Completion from a raw RADOS completion handle.
    ///
    /// # Safety
    /// The caller must ensure that `inner` is a valid, non-null rados_completion_t
    /// that has been initialized with our callback.
    unsafe fn from_raw(
        inner: rados_completion_t,
        waker: Box<AtomicWaker>,
        ioctx: &'a IoCtx,
    ) -> Self {
        assert!(!inner.is_null(), "null rados_completion_t");
        Self {
            inner,
            waker,
            ioctx,
        }
    }

    /// Checks if the RADOS operation has completed.
    fn is_complete(&self) -> bool {
        unsafe { rados_aio_is_complete(self.inner) != 0 }
    }

    /// Waits for the operation and its callback to complete.
    ///
    /// # Panics
    /// Panics if the wait operation fails.
    fn wait_for_complete_and_cb(&self) {
        let result = unsafe { rados_aio_wait_for_complete_and_cb(self.inner) };
        assert_eq!(result, 0, "Failed to wait for completion");
    }

    /// Gets the return value of the completed operation.
    fn get_return_value(&self) -> i32 {
        unsafe { rados_aio_get_return_value(self.inner) }
    }

    /// Attempts to cancel the operation if it's still in flight.
    ///
    /// Returns Ok(true) if cancelled, Ok(false) if already complete, or Err on failure.
    fn try_cancel(&self) -> Result<bool, i32> {
        if self.is_complete() {
            return Ok(false);
        }

        let result = unsafe { rados_aio_cancel(self.ioctx.ioctx, self.inner) };
        match result {
            0 => Ok(true),
            e if e == -libc::ENOENT => Ok(false), // Already completed
            e => Err(e),
        }
    }
}

impl Drop for Completion<'_> {
    fn drop(&mut self) {
        // Ensure the operation is cancelled if still in flight
        if let Err(e) = self.try_cancel() {
            // It's unsound to proceed if the operation is still in flight
            panic!("Failed to cancel RADOS operation: {e}");
        }

        // Wait for librados to finish with our callback
        self.wait_for_complete_and_cb();

        // Release the RADOS completion
        unsafe {
            rados_aio_release(self.inner);
        }
    }
}

impl Future for Completion<'_> {
    type Output = RadosResult<u32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.is_complete() {
            // Ensure callback has finished
            self.wait_for_complete_and_cb();

            // Get the result
            let return_value = self.get_return_value();
            trace!("completion {:p} returned {}", self.inner, return_value);
            let result = if return_value < 0 {
                Err(RadosError::from_errno(-return_value))
            } else {
                Ok(return_value as u32)
            };

            Poll::Ready(result)
        } else {
            // Register waker for notification when complete
            self.waker.register(cx.waker());
            Poll::Pending
        }
    }
}

/// Creates a RADOS completion and executes an async operation with it.
///
/// This function ensures that the Completion is only created for successfully
/// initiated operations, preventing resource leaks.
///
/// # Arguments
/// * `ioctx` - The I/O context for the operation
/// * `f` - A closure that initiates the async operation using the completion
///
/// # Returns
/// A `Completion` future that resolves to the operation result
///
/// # Example
/// ```ignore
/// let completion = with_completion(&ioctx, |c| unsafe {
///     rados_aio_read(ioctx.ioctx, "object_name", c, buffer, len, offset)
/// })?;
/// let bytes_read = completion.await?;
/// ```
pub(crate) fn with_completion<F>(ioctx: &IoCtx, f: F) -> RadosResult<Completion<'_>>
where
    F: FnOnce(rados_completion_t) -> libc::c_int,
{
    // Create waker storage with stable address
    let waker = Box::new(AtomicWaker::new());
    let waker_ptr = &*waker as *const AtomicWaker as *mut c_void;

    // Create RADOS completion
    let mut completion: rados_completion_t = std::ptr::null_mut();
    let create_result = unsafe {
        rados_aio_create_completion2(waker_ptr, Some(completion_complete), &mut completion)
    };

    if create_result != 0 {
        panic!("Failed to create RADOS completion: {create_result} (out of memory?)");
    }

    debug_assert!(
        !completion.is_null(),
        "rados_aio_create_completion2 returned null"
    );

    // Execute the async operation
    let operation_result = f(completion);

    if operation_result < 0 {
        // Clean up on error
        unsafe {
            rados_aio_release(completion);
        }
        Err(RadosError::from_errno(-operation_result))
    } else {
        // Return the armed completion
        Ok(unsafe { Completion::from_raw(completion, waker, ioctx) })
    }
}
