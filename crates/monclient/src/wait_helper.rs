//! Generic helper for wait-with-notification pattern
//!
//! This module provides a reusable pattern for waiting on conditions with
//! tokio::sync::Notify and timeout support.

use std::future::Future;
use std::time::Duration;

/// Wait for a condition to be met with notification and timeout
///
/// This is a generic helper that consolidates the common pattern used in
/// wait_for_auth(), wait_for_monmap(), and wait_for_osdmap().
///
/// # Pattern
///
/// 1. Check if condition is already met (fast path)
/// 2. Wait for notification with timeout
/// 3. Double-check condition after notification
/// 4. Return result or timeout error
///
/// # Arguments
///
/// * `check_fn` - Async function that checks if the condition is met and returns Some(T) if true
/// * `notify` - The Notify instance to wait on
/// * `timeout` - Maximum time to wait
/// * `timeout_error` - Error to return on timeout
///
/// # Returns
///
/// Returns Ok(T) if condition is met, or Err(E) on timeout or spurious wakeup
///
/// # Example
///
/// ```rust,ignore
/// wait_for_condition(
///     || async {
///         let state = self.state.read().await;
///         if state.authenticated {
///             Some(())
///         } else {
///             None
///         }
///     },
///     &self.auth_notify,
///     timeout,
///     MonClientError::AuthenticationTimeout,
/// ).await
/// ```
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
    // Fast path: check if condition is already met
    if let Some(value) = check_fn().await {
        return Ok(value);
    }

    // Wait for notification with timeout
    tokio::select! {
        _ = notify.notified() => {
            // Double-check condition after notification
            match check_fn().await {
                Some(value) => Ok(value),
                None => Err(timeout_error), // Spurious wakeup
            }
        }
        _ = tokio::time::sleep(timeout) => {
            Err(timeout_error)
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
                    if *guard {
                        Some(42)
                    } else {
                        None
                    }
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
                    if *guard {
                        Some(42)
                    } else {
                        None
                    }
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
                    if *guard {
                        Some(42)
                    } else {
                        None
                    }
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

        let notify_clone = Arc::clone(&notify);

        // Spawn task to notify without setting state (spurious wakeup)
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            notify_clone.notify_waiters();
        });

        let result = wait_for_condition(
            || {
                let state = Arc::clone(&state);
                async move {
                    let guard = state.read().await;
                    if *guard {
                        Some(42)
                    } else {
                        None
                    }
                }
            },
            &notify,
            Duration::from_millis(100),
            TestError::Timeout,
        )
        .await;

        // Should return timeout error on spurious wakeup
        assert_eq!(result, Err(TestError::Timeout));
    }
}
