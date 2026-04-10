//! Generic helper for wait-with-notification pattern
//!
//! This module provides a reusable pattern for waiting on conditions with
//! tokio::sync::Notify and timeout support.

use std::future::Future;
use std::time::Duration;

/// Wait for a condition to be met with notification and timeout.
///
/// Consolidates the pattern used by `wait_for_auth`, `wait_for_monmap`, and
/// `wait_for_osdmap`.
///
/// # Pattern
///
/// 1. Arm a `Notified` future via `enable()` so any `notify_waiters()` call that
///    fires before the first poll will still wake it.
/// 2. Check if the condition is already met (fast path).
/// 3. Race the armed waiter against the deadline.
/// 4. On wakeup, loop and re-check.
///
/// The `enable()` step is load-bearing. `Notify::notify_waiters()` does not
/// persist a permit, so without arming first there is a TOCTOU window between
/// the check and the await in which a fired notification can be lost — causing
/// the caller to hang for the full timeout even though the condition became
/// true immediately.
///
/// Tokio's own MPMC channel example in the `Notify` docs uses the exact same
/// pinned-`enable()`-then-check idiom; see `tokio/src/sync/notify.rs` around
/// line 169 ("enable the future before checking try_recv") for the canonical
/// formulation.
pub async fn wait_for_condition<T, E, CheckFn, CheckFut>(
    check_fn: CheckFn,
    notify: &tokio::sync::Notify,
    timeout: Duration,
    timeout_error: E,
) -> Result<T, E>
where
    CheckFn: Fn() -> CheckFut,
    CheckFut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // Arm the waiter BEFORE checking state. If a notification fires in the
        // gap between the check returning None and us calling .await below,
        // `enable()` guarantees we still pick it up on the next poll.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if let Some(value) = check_fn().await {
            return Ok(value);
        }

        tokio::select! {
            _ = notified.as_mut() => continue,
            _ = tokio::time::sleep_until(deadline) => return Err(timeout_error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[derive(Debug, PartialEq)]
    enum TestError {
        Timeout,
    }

    #[tokio::test]
    async fn test_condition_already_met() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let state = Arc::new(RwLock::new(true));

        let result = wait_for_condition(
            || {
                let state = Arc::clone(&state);
                async move {
                    let guard = state.read().await;
                    if *guard { Some(42) } else { None }
                }
            },
            &notify,
            Duration::from_secs(1),
            TestError::Timeout,
        )
        .await;

        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_condition_met_after_notification() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let state = Arc::new(RwLock::new(false));

        let state_clone = Arc::clone(&state);
        let notify_clone = Arc::clone(&notify);

        // Spawn task to set state and notify after delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            *state_clone.write().await = true;
            notify_clone.notify_waiters();
        });

        let result = wait_for_condition(
            || {
                let state = Arc::clone(&state);
                async move {
                    let guard = state.read().await;
                    if *guard { Some(42) } else { None }
                }
            },
            &notify,
            Duration::from_secs(1),
            TestError::Timeout,
        )
        .await;

        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_timeout() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let state = Arc::new(RwLock::new(false));

        let result = wait_for_condition(
            || {
                let state = Arc::clone(&state);
                async move {
                    let guard = state.read().await;
                    if *guard { Some(42) } else { None }
                }
            },
            &notify,
            Duration::from_millis(50),
            TestError::Timeout,
        )
        .await;

        assert_eq!(result, Err(TestError::Timeout));
    }

    #[tokio::test]
    async fn test_spurious_wakeup() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let state = Arc::new(RwLock::new(false));

        let state_clone = Arc::clone(&state);
        let notify_clone = Arc::clone(&notify);

        // Spawn task that:
        // 1. Sends spurious notification (state still false)
        // 2. Waits a bit
        // 3. Sets state to true and notifies again
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            // Spurious wakeup - state is still false
            notify_clone.notify_waiters();

            tokio::time::sleep(Duration::from_millis(50)).await;
            // Real wakeup - set state to true
            *state_clone.write().await = true;
            notify_clone.notify_waiters();
        });

        let result = wait_for_condition(
            || {
                let state = Arc::clone(&state);
                async move {
                    let guard = state.read().await;
                    if *guard { Some(42) } else { None }
                }
            },
            &notify,
            Duration::from_millis(200),
            TestError::Timeout,
        )
        .await;

        // Should succeed after spurious wakeup when condition is eventually met
        assert_eq!(result, Ok(42));
    }
}
