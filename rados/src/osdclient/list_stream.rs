//! Paginated RADOS object listing as a [`futures::Stream`].
//!
//! # Notes
//!
//! - The stream fetches objects in pages of `page_size` from the cluster.
//!   Smaller `page_size` values reduce per-page latency but increase the number
//!   of round-trips. A value of 100–1000 is typically a good default.
//! - If the cluster returns an error mid-listing, the stream yields `Err(…)`
//!   and then terminates; no further items are produced.
//! - Object listing is not strongly consistent in RADOS: objects created or
//!   deleted concurrently may be included or excluded unpredictably.

use crate::osdclient::error::OSDClientError;
use crate::osdclient::ioctx::IoCtx;
use futures::stream::Stream;
use std::collections::VecDeque;

/// Create a lazily-paginated stream of object names in a pool.
///
/// Each page fetches at most `page_size` objects from the cluster via
/// `list_objects`. The stream ends automatically when the cluster signals
/// that no further pages exist (returns `None` as the next cursor).
///
/// # Example
///
/// ```no_run
/// use futures::StreamExt;
/// use crate::osdclient::{IoCtx, list_objects_stream};
///
/// # async fn example(ioctx: IoCtx) {
/// let stream = list_objects_stream(ioctx, 100);
/// stream.for_each(|name| async move {
///     match name {
///         Ok(n) => println!("{n}"),
///         Err(e) => eprintln!("listing error: {e}"),
///     }
/// }).await;
/// # }
/// ```
pub fn list_objects_stream(
    ioctx: IoCtx,
    page_size: usize,
) -> impl Stream<Item = Result<String, OSDClientError>> {
    // State: (ioctx, buffered items from current page, cursor for next page, done flag)
    type State = (IoCtx, VecDeque<String>, Option<String>, bool);

    futures::stream::unfold(
        (ioctx, VecDeque::new(), None::<String>, false) as State,
        move |(ioctx, mut buf, cursor, done)| async move {
            // If we hit an error or finished the last page, stop.
            if done && buf.is_empty() {
                return None;
            }

            // Drain the in-memory buffer before fetching the next page.
            if buf.is_empty() {
                match ioctx.list_objects(cursor, page_size).await {
                    Err(e) => {
                        // Yield the error and mark done so no further RPCs are issued.
                        return Some((Err(e), (ioctx, VecDeque::new(), None, true)));
                    }
                    Ok((names, next_cursor)) => {
                        buf.extend(names);
                        let done = next_cursor.is_none();
                        // Empty page with no next cursor — listing is complete.
                        let name = buf.pop_front()?;
                        return Some((Ok(name), (ioctx, buf, next_cursor, done)));
                    }
                }
            }

            // Fast path: still items in the buffer from the previously fetched page.
            let name = buf.pop_front()?;
            Some((Ok(name), (ioctx, buf, cursor, done)))
        },
    )
}
