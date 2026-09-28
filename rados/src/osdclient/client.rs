//! RADOS OSD Client
//!
//! Main entry point for performing object operations against a Ceph cluster.

use crate::monclient::MOSDMap;
use crate::msgr2::{MapReceiver, MapSender};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::denc::{Denc, VersionedEncode};

use crate::osdclient::backoff::BackoffEntry;
use crate::osdclient::error::{ETIMEDOUT, OSDClientError, Result};
use crate::osdclient::messages::MOSDOp;
use crate::osdclient::osdmap::CephRelease;
use crate::osdclient::session::OSDSession;
use crate::osdclient::throttle::Throttle;
use crate::osdclient::tracker::{Tracker, TrackerConfig};
use crate::osdclient::types::{
    ListObjectEntry, ListResult, OSDOp, ObjectId, ObjectLocator, OsdOpFlags, ReadResult,
    RequestRedirect, StatResult, StripedPgId, WatchOp, WriteResult, calc_op_budget,
};
use crate::osdclient::watch::{
    Linger, LingerKind, NotifyResult, PgInterval, WatchEvent, decode_notify_reply, is_no_answer,
    lingers_to_resend, notify_error_code, scan_lingers, watch_error_code, watches_to_ping,
};
use bytes::Bytes;

/// How often a registered watch is pinged (Objecter's `objecter_tick_interval`).
const WATCH_PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// librados's `client_notify_timeout`, sent when the caller passes 0.
const CLIENT_NOTIFY_TIMEOUT_SECS: u32 = 10;

/// Configuration for OSD client
#[derive(Debug, Clone)]
pub struct OSDClientConfig {
    /// Operation timeout configuration
    pub tracker_config: TrackerConfig,
    /// Client incarnation number
    /// This should be unique per client instance to avoid OSD duplicate request detection.
    /// In Ceph C++, this is typically 0, with uniqueness provided by the global_id
    /// in the entity_name instead.
    pub client_inc: u32,
    /// Maximum in-flight operations (default: 1024, matches objecter_inflight_ops)
    pub max_inflight_ops: usize,
    /// Maximum in-flight bytes (default: 100MB, matches objecter_inflight_op_bytes)
    pub max_inflight_bytes: usize,
    /// Compute CRC over the data section in the inner CephMessage footer.
    /// Matches Ceph option `ms_crc_data` (default: true).
    ///
    /// The msgr2 frame layer already provides integrity (epilogue CRC for
    /// plaintext, AES-GCM for encrypted), so the inner data CRC is
    /// redundant.  Setting this to false avoids a full CRC32c pass over
    /// every write payload.
    pub ms_crc_data: bool,
    /// Overrides [`OSDClient::require_osd_release`] for testing: the
    /// release a caller selects request shapes by. The stored map is
    /// never changed.
    pub assume_osd_release: Option<CephRelease>,
}

/// Derive a `client_inc` value suitable for `OSDClientConfig::client_inc`.
///
/// Uses the current seconds-since-epoch so that two clients started a second
/// apart get distinct values, which is enough for the OSD-side duplicate
/// request detector to treat them as separate incarnations. Falls back to `1`
/// on the theoretical path where `SystemTime::now()` precedes `UNIX_EPOCH`
/// (treating `0` as reserved for "not yet set").
pub fn default_client_inc() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |d| d.as_secs() as u32)
}

impl Default for OSDClientConfig {
    fn default() -> Self {
        Self {
            tracker_config: TrackerConfig::default(),
            client_inc: 0,
            max_inflight_ops: crate::osdclient::throttle::DEFAULT_MAX_OPS,
            max_inflight_bytes: crate::osdclient::throttle::DEFAULT_MAX_BYTES,
            ms_crc_data: true,
            assume_osd_release: None,
        }
    }
}

fn effective_release(
    assumed: Option<CephRelease>,
    map: Option<&crate::osdclient::osdmap::OSDMap>,
) -> Option<CephRelease> {
    assumed.or(map.map(|m| CephRelease(m.require_osd_release)))
}

/// Main OSD client for performing object operations
pub struct OSDClient {
    config: OSDClientConfig,
    mon_client: Arc<crate::monclient::MonClient>,
    sessions: Arc<RwLock<HashMap<i32, Arc<OSDSession>>>>,
    pub(crate) tracker: Arc<Tracker>,
    /// Request throttle to prevent resource exhaustion
    throttle: Arc<Throttle>,
    /// Entity name (e.g., "client.admin") from MonClient auth config
    entity_name: Arc<str>,
    /// Global ID from monitor authentication (used in entity_name for request IDs)
    global_id: u64,
    /// Cluster FSID for OSDMap validation
    fsid: crate::UuidD,
    /// Current OSDMap (watch channel for efficient distribution)
    osdmap_tx: watch::Sender<Option<Arc<crate::osdclient::osdmap::OSDMap>>>,
    osdmap_rx: watch::Receiver<Option<Arc<crate::osdclient::osdmap::OSDMap>>>,
    /// Channel for routing MOSDMap messages to sessions
    map_tx: MapSender<MOSDMap>,
    /// Shutdown token for graceful termination
    shutdown_token: CancellationToken,
    /// Weak self-reference for session creation
    self_weak: std::sync::Weak<Self>,
    /// Set to true once we detect our address in the OSDMap blocklist.
    /// After that, all new operations fail immediately with `Blocklisted`.
    blocklisted: AtomicBool,
    /// Minimum OSDMap epoch required before any op is sent.
    ///
    /// A one-way ratchet (only advances forward).  When non-zero, ops are
    /// held in the pause-wait loop until `osdmap.epoch >= epoch_barrier`.
    /// Mirrors `Objecter::epoch_barrier` / `set_epoch_barrier()` in C++.
    epoch_barrier: AtomicU32,
    /// Monotonic transaction ID source, shared across all sessions.
    ///
    /// Tids allocated from here are strictly monotonic for the lifetime of
    /// this `OSDClient`, including across session replacements triggered by
    /// reconnects.  The OSD runs a `debug_op_order` check per
    /// `(client_id, object)` that is *not* reset on TCP reconnect, so a
    /// session-local counter would abort the OSD with
    /// `ceph_abort_msg("out of order op")` the first time a reconnect caused
    /// the new session's tid to fall below the highest tid the OSD had
    /// already seen for the same object.  Mirrors `Objecter::last_tid`.
    next_tid: Arc<AtomicU64>,
    /// Watches and in-flight notifies, keyed by cookie: Objecter's
    /// `linger_ops`. Every `MWatchNotify` is routed through it.
    lingers: dashmap::DashMap<u64, Arc<Linger>>,
    /// Linger cookie source. Starts above 1000, as librados's cookies do
    /// (its tests assert `cookie > 1000`), and never reuses a value, since
    /// the OSD keys a watch by `(cookie, client gid)`.
    next_cookie: AtomicU64,
    /// Cleared by `set_watch_pings_enabled(false)`.
    watch_pings_enabled: AtomicBool,
    /// Stands in for a linger send's submit, which needs a session.
    #[cfg(test)]
    fake_linger_submit: std::sync::Mutex<Option<FakeLingerSubmit>>,
    /// The session every OSD's ops go to, connected or not.
    #[cfg(test)]
    fake_osd: std::sync::Mutex<Option<Arc<OSDSession>>>,
    /// Runs as an op, placed, is about to be handed to its session.
    #[cfg(test)]
    before_submit: std::sync::Mutex<Option<Box<dyn FnMut() + Send>>>,
    /// Runs as an op, pending on its session, is about to have its map
    /// checked.
    #[cfg(test)]
    in_submit_window: std::sync::Mutex<Option<Box<dyn FnMut() + Send>>>,
}

#[cfg(test)]
type FakeLingerSubmit = Box<
    dyn FnMut() -> (
            i32,
            u64,
            Arc<crate::osdclient::osdmap::OSDMap>,
            tokio::sync::oneshot::Receiver<Result<crate::osdclient::types::OpResult>>,
        ) + Send,
>;

/// Where an object lives: the hobject hash its MOSDOp carries, and the
/// pool's PG that contains that hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Placement {
    hash: u32,
    pg: crate::crush::placement::PgId,
}

/// Where an op goes: its hobject hash, its PG's `spg_t`, and that PG's
/// acting OSDs.
type OpRoute = (u32, StripedPgId, Vec<i32>);

/// An `MOSDOp` built by `prepare_op`, carried across `route_and_submit`
/// calls with the map it was last routed against.
struct PreparedOp {
    msg: Arc<MOSDOp>,
    osdmap: Arc<crate::osdclient::osdmap::OSDMap>,
    class: OpClass,
    /// [`crate::osdclient::session::PendingOp::should_resend`].
    should_resend: bool,
}

/// What decides which pause and full flags hold an op back.
#[derive(Clone, Copy)]
struct OpClass {
    is_write: bool,
    is_read: bool,
    respects_full: bool,
    /// A watch ping: held by nothing, not even the epoch barrier, as
    /// Objecter's `_send_linger_ping` skips `_calc_target` (the ping round
    /// itself is skipped under PAUSERD). Registrations, reconnects and
    /// notifies go through the ordinary classification with the linger's
    /// target, so a watch (WRITE) waits out PAUSEWR and a full pool and a
    /// notify (READ) waits out PAUSERD.
    ping: bool,
}

impl OpClass {
    /// Which of `osdmap`'s pause and full flags hold an op on `pool` back:
    /// `(pauserd, pausewr, pool_full)`.
    fn held_by(self, osdmap: &crate::osdclient::osdmap::OSDMap, pool: u64) -> (bool, bool, bool) {
        if self.ping {
            return (false, false, false);
        }
        let pauserd = self.is_read && osdmap.is_pauserd();
        let pausewr = self.is_write && osdmap.is_pausewr();
        let pool_full = self.is_write && self.respects_full && osdmap.is_pool_full(pool);
        (pauserd, pausewr, pool_full)
    }
}

/// How `submit_once_in_map` treats an op.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SubmitKind {
    /// An ordinary op: throttled, and held by every pause flag.
    Op,
    /// A linger send (registration, reconnect, notify): unthrottled, as
    /// Objecter pre-budgets linger sends, and held by the pause flags as
    /// an ordinary op is. Never re-sent: the linger layer sends anew.
    Linger,
    /// A watch ping: unthrottled and held by nothing ([`OpClass::ping`]),
    /// and never re-sent.
    Ping,
}

/// One send by `route_and_submit`.
struct Submitted {
    osd: i32,
    tid: u64,
    /// The map the op was routed in; its epoch is the op's.
    osdmap: Arc<crate::osdclient::osdmap::OSDMap>,
    rx: Result<tokio::sync::oneshot::Receiver<Result<crate::osdclient::types::OpResult>>>,
}

/// The maps one `MOSDMap` advanced this client through, oldest first.
#[derive(Default)]
struct MapBatch {
    maps: Vec<Arc<crate::osdclient::osdmap::OSDMap>>,
    /// Some epochs in between were never seen (a gap, or a full map that
    /// jumps), so an interval change may have gone unnoticed.
    skipped: bool,
    /// The message lacked this epoch, which the mon still holds: ask
    /// for the maps from it.
    request_from: Option<u32>,
}

/// Advance `working`, at `from`, through `mosdmap`'s epochs as
/// `Objecter::handle_osd_map` does (v19.2.2:src/osdc/Objecter.cc:
/// 1202-1241): each epoch from its incremental when that follows the
/// map so far, else from its full map. `incremental` and `full` build
/// the epoch's map, or fail with `None`.
///
/// An epoch neither builds is missing. If the mon still holds it (it is
/// at or above the message's trim lower bound), the walk stops and asks
/// for the maps from the one after the last it has. Otherwise the mon
/// trimmed it: the walk jumps to the lower bound, whose full map must
/// be in the message, and flags the batch skipped.
fn walk_map_epochs(
    mosdmap: &crate::monclient::messages::MOSDMap,
    from: u32,
    mut working: Option<Arc<crate::osdclient::osdmap::OSDMap>>,
    mut incremental: impl FnMut(
        u32,
        &Option<Arc<crate::osdclient::osdmap::OSDMap>>,
    ) -> Option<Arc<crate::osdclient::osdmap::OSDMap>>,
    mut full: impl FnMut(u32) -> Option<Arc<crate::osdclient::osdmap::OSDMap>>,
) -> MapBatch {
    let mut batch = MapBatch::default();
    let lower_bound = mosdmap.cluster_osdmap_trim_lower_bound;
    let mut e = from + 1;
    while e <= mosdmap.get_last() {
        let working_epoch = working.as_ref().map_or(from, |m| m.epoch.as_u32());
        let contiguous = working_epoch == e - 1;
        let next = if contiguous && mosdmap.incremental_maps.contains_key(&e) {
            incremental(e, &working)
        } else if mosdmap.maps.contains_key(&e) {
            // A full map that does not follow the previous epoch jumps
            // over maps nobody here has seen.
            batch.skipped |= !contiguous;
            full(e)
        } else {
            None
        };
        match next {
            Some(map) => {
                working = Some(Arc::clone(&map));
                batch.maps.push(map);
                e += 1;
            }
            None if e >= lower_bound => {
                warn!(
                    "Missing epoch {}; requesting maps from {}",
                    e,
                    working_epoch + 1
                );
                batch.request_from = Some(working_epoch + 1);
                break;
            }
            None => {
                warn!(
                    "Missing epoch {}, trimmed by the mon; jumping to {}",
                    e, lower_bound
                );
                e = lower_bound;
                batch.skipped = true;
            }
        }
    }
    batch
}

/// The OSD's `osd_default_notify_timeout`, which it applies to a wire
/// timeout of 0.
const OSD_DEFAULT_NOTIFY_TIMEOUT_SECS: u32 = 30;

/// How long a notify waits for its completion after each send: the
/// OSD's own timeout for it, plus 30 s. The OSD completes it by then;
/// the bound only covers an OSD that dies with no re-send to follow.
fn notify_outer_bound(timeout_secs: u32) -> std::time::Duration {
    let wire = if timeout_secs == 0 {
        OSD_DEFAULT_NOTIFY_TIMEOUT_SECS
    } else {
        timeout_secs
    };
    std::time::Duration::from_secs(u64::from(wire) + 30)
}

/// Wait for a notify's completion, restarting `bound` each time a send of
/// it reaches a session.
async fn await_notify(
    linger: &Linger,
    mut done_rx: tokio::sync::oneshot::Receiver<(i32, Bytes)>,
    bound: std::time::Duration,
) -> Result<NotifyResult> {
    let done = loop {
        tokio::select! {
            done = &mut done_rx => {
                break done
                    .map_err(|_| OSDClientError::Internal("notify completion dropped".into()))?;
            }
            () = linger.resent.notified() => {}
            () = tokio::time::sleep(bound) => return Err(OSDClientError::Timeout(bound)),
        }
    };
    notify_outcome(done)
}

/// A notify's result from its completion's code and reply.
fn notify_outcome((code, data): (i32, Bytes)) -> Result<NotifyResult> {
    // A completion with no reply map is a failed send, not the OSD's.
    if code < 0 && (code != ETIMEDOUT || data.is_empty()) {
        return Err(OSDClientError::OSDError {
            code,
            message: "notify failed".into(),
        });
    }
    let (acks, missed) = decode_notify_reply(&data)?;
    Ok(NotifyResult {
        acks,
        missed,
        timed_out: code == ETIMEDOUT,
    })
}

/// Removes a linger from the registry when dropped, so a failed or
/// cancelled registration or notify leaves nothing behind.
struct LingerGuard<'a> {
    client: &'a OSDClient,
    cookie: Option<u64>,
}

impl<'a> LingerGuard<'a> {
    fn new(client: &'a OSDClient, cookie: u64) -> Self {
        Self {
            client,
            cookie: Some(cookie),
        }
    }

    /// The registration succeeded: keep the linger.
    fn keep(mut self) {
        self.cookie = None;
    }
}

impl Drop for LingerGuard<'_> {
    fn drop(&mut self) {
        if let Some(cookie) = self.cookie {
            self.client.forget_linger(cookie);
        }
    }
}

/// Await an OSD operation result with a timeout, mapping all error layers to `OSDClientError`.
async fn await_op_result(
    rx: tokio::sync::oneshot::Receiver<Result<crate::osdclient::types::OpResult>>,
    timeout: std::time::Duration,
) -> Result<crate::osdclient::types::OpResult> {
    tokio::time::timeout(timeout, rx)
        .await
        .map_err(|_| OSDClientError::Timeout(timeout))?
        .map_err(|_| OSDClientError::Cancelled)?
}

/// Return the time left until `deadline`, or `Err(Timeout(effective))` if the
/// deadline has already passed. Used by `execute_op`'s retry loop so each
/// pause-wait / reply-await branch can budget against the absolute deadline
/// without duplicating the "elapsed → Timeout" boilerplate.
fn deadline_remaining(
    deadline: std::time::Instant,
    effective: std::time::Duration,
) -> Result<std::time::Duration> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        Err(OSDClientError::Timeout(effective))
    } else {
        Ok(remaining)
    }
}

impl OSDClient {
    /// Create a new OSD client
    pub async fn new(
        config: OSDClientConfig,
        fsid: crate::UuidD,
        mon_client: Arc<crate::monclient::MonClient>,
        osdmap_tx: MapSender<MOSDMap>,
        mut osdmap_rx: MapReceiver<MOSDMap>,
    ) -> Result<Arc<Self>> {
        // Get entity_name and global_id from MonClient
        let entity_name: Arc<str> = mon_client.get_entity_name_string().into();
        let global_id = mon_client.get_global_id().await;

        info!("Creating OSDClient for entity_name={}", entity_name);
        info!("OSDClient using global_id {} from monitor", global_id);

        // Create watch channel for OSDMap distribution
        let (osdmap_tx_watch, osdmap_rx_watch) = watch::channel(None);

        // Create throttle with configured limits
        let throttle = Arc::new(Throttle::new(
            config.max_inflight_ops,
            config.max_inflight_bytes,
        ));
        info!(
            "OSDClient throttle: max_ops={}, max_bytes={}",
            throttle.max_ops(),
            throttle.max_bytes()
        );

        let client = Arc::new_cyclic(|weak: &std::sync::Weak<Self>| {
            // Create timeout callback that cancels operations in sessions
            let weak_for_callback = weak.clone();
            let timeout_callback: crate::osdclient::tracker::TimeoutCallback = Arc::new(
                move |osd_id, tid| {
                    if let Some(client) = weak_for_callback.upgrade() {
                        let sessions = client.sessions.clone();
                        tokio::spawn(async move {
                            let session = {
                                let sessions_guard = sessions.read().await;
                                sessions_guard.get(&osd_id).cloned()
                            };

                            if let Some(session) = session
                                && let Some(pending_op) = session.remove_pending_op(tid)
                            {
                                // Check incarnation - operation might be from a previous connection
                                // that has since reconnected. In that case, silently drop the timeout.
                                let current_incarnation = session.current_incarnation();
                                if pending_op.sent_incarnation != current_incarnation {
                                    debug!(
                                        "Ignoring timeout for stale operation: OSD {} tid={} (sent in incarnation {} but current is {})",
                                        osd_id,
                                        tid,
                                        pending_op.sent_incarnation,
                                        current_incarnation
                                    );
                                    return;
                                }

                                let _ = pending_op.result_tx.send(Err(OSDClientError::Timeout(
                                    client.tracker.operation_timeout(),
                                )));
                                warn!("Cancelled timed-out operation: OSD {} tid={}", osd_id, tid);
                            }
                        });
                    }
                },
            );

            let tracker = Arc::new(Tracker::new(
                config.tracker_config.clone(),
                config.max_inflight_ops,
                timeout_callback,
            ));

            Self {
                config,
                mon_client: Arc::clone(&mon_client),
                sessions: Arc::new(RwLock::new(HashMap::new())),
                tracker,
                throttle,
                entity_name: entity_name.clone(),
                global_id,
                fsid,
                osdmap_tx: osdmap_tx_watch,
                osdmap_rx: osdmap_rx_watch,
                map_tx: osdmap_tx,
                shutdown_token: CancellationToken::new(),
                self_weak: weak.clone(),
                blocklisted: AtomicBool::new(false),
                epoch_barrier: AtomicU32::new(0),
                next_tid: Arc::new(AtomicU64::new(1)),
                lingers: dashmap::DashMap::new(),
                next_cookie: AtomicU64::new(1001),
                watch_pings_enabled: AtomicBool::new(true),
                #[cfg(test)]
                fake_linger_submit: std::sync::Mutex::new(None),
                #[cfg(test)]
                fake_osd: std::sync::Mutex::new(None),
                #[cfg(test)]
                before_submit: std::sync::Mutex::new(None),
                #[cfg(test)]
                in_submit_window: std::sync::Mutex::new(None),
            }
        });

        // Spawn drain task for OSDMap messages. Two independent exit
        // conditions: the shutdown_token being cancelled (explicit
        // OSDClient::shutdown call) or the upstream msgr2 channel closing
        // + weak-ref upgrade failing (natural Arc teardown). Previously
        // only the weak-ref path existed, which meant calling shutdown()
        // while an IoCtx still held a strong Arc would leave this task
        // running until the IoCtx dropped.
        let client_weak = Arc::downgrade(&client);
        let drain_token = client.shutdown_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = drain_token.cancelled() => {
                        info!("OSDClient shutdown_token cancelled, terminating OSDMap drain task");
                        break;
                    }
                    msg = osdmap_rx.recv() => {
                        let Some(msg) = msg else {
                            info!("OSDMap channel closed, terminating OSDMap drain task");
                            break;
                        };
                        if let Some(client_arc) = client_weak.upgrade() {
                            if let Err(e) = client_arc.handle_osdmap(msg).await {
                                error!("Failed to handle OSDMap: {}", e);
                            }
                        } else {
                            info!("OSDClient dropped, terminating OSDMap drain task");
                            break;
                        }
                    }
                }
            }
            info!("OSDClient OSDMap drain task terminated");
        });

        // The watch ping task, stopped like the drain task above.
        let ping_weak = Arc::downgrade(&client);
        let ping_token = client.shutdown_token.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + WATCH_PING_INTERVAL,
                WATCH_PING_INTERVAL,
            );
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ping_token.cancelled() => break,
                    _ = ticker.tick() => {
                        let Some(client) = ping_weak.upgrade() else { break };
                        client.tick_lingers();
                    }
                }
            }
        });

        Ok(client)
    }

    /// Dispatch a session-specific message from an OSD
    ///
    /// This is called directly from the OSD session's io_task with explicit OSD context,
    /// following the Linux kernel pattern of passing `struct ceph_osd *osd` to handlers.
    ///
    /// Reference: ~/dev/linux/net/ceph/osd_client.c handle_backoff(struct ceph_osd *osd, ...)
    pub async fn dispatch_from_osd(
        &self,
        osd_id: i32,
        msg: crate::msgr2::message::Message,
    ) -> Result<()> {
        let msg_type = msg.msg_type();
        debug!("Dispatching message 0x{:04x} from OSD {}", msg_type, osd_id);

        match msg_type {
            crate::osdclient::messages::CEPH_MSG_OSD_OPREPLY => {
                self.handle_osd_op_reply_from_osd(osd_id, msg).await
            }
            crate::osdclient::messages::CEPH_MSG_OSD_BACKOFF => {
                self.handle_backoff_from_osd(osd_id, msg).await
            }
            crate::osdclient::messages::CEPH_MSG_WATCH_NOTIFY => {
                self.handle_watch_notify(osd_id, msg)
            }
            _ => {
                warn!(
                    "Unexpected session-specific message type 0x{:04x} from OSD {}",
                    msg_type, osd_id
                );
                Ok(())
            }
        }
    }

    /// Decode an `MWatchNotify` and route it to its linger by cookie.
    /// Synchronous: it runs inside the session's I/O loop.
    fn handle_watch_notify(&self, osd_id: i32, msg: crate::msgr2::message::Message) -> Result<()> {
        let event =
            crate::osdclient::messages::MWatchNotify::decode(msg.header.version.get(), &msg.front)?;
        debug!(
            "watch-notify from OSD {}: opcode {} cookie {} notify_id {} rc {}",
            osd_id, event.opcode, event.cookie, event.notify_id, event.return_code
        );
        crate::osdclient::watch::route_watch_notify(&self.lingers, event, msg.data);
        Ok(())
    }

    /// The session to `osd_id` ended: its I/O task exited, whether the
    /// connection dropped or the session was closed on purpose. Returns
    /// at once; the lingers on that OSD are re-sent from a spawned task,
    /// since the caller is the dying I/O task and `OSDSession::close()`
    /// awaits it.
    /// Returns whether it spawned the re-send.
    pub(crate) fn on_session_reset(self: &Arc<Self>, osd_id: i32) -> bool {
        // Client shutdown ends every I/O task through the same path.
        if self.shutdown_token.is_cancelled() {
            return false;
        }
        self.spawn_until_shutdown(Arc::clone(self).relinger_after_reset(osd_id));
        true
    }

    async fn relinger_after_reset(self: Arc<Self>, osd_id: i32) {
        let affected: Vec<Arc<Linger>> = self
            .lingers
            .iter()
            .filter(|l| l.lock_state().osd == Some(osd_id))
            .map(|l| Arc::clone(l.value()))
            .collect();
        if !affected.is_empty() {
            info!(
                "session to OSD {} reset; re-sending {} linger(s)",
                osd_id,
                affected.len()
            );
        }
        for linger in affected {
            self.spawn_relinger(linger);
        }
    }

    // ========================================================================
    // Watch/notify: the linger machinery (Objecter's LingerOp)
    // ========================================================================

    /// The client's global id: the `client.<gid>` the OSD names this
    /// client's watches and notifies by (librados's `get_instance_id`).
    pub fn global_id(&self) -> u64 {
        self.global_id
    }

    /// Stop or resume the five-second watch pings, as Objecter's
    /// `objecter_inject_no_watch_ping` does. A test aid: without pings
    /// the OSD times a watch out.
    pub fn set_watch_pings_enabled(&self, enabled: bool) {
        self.watch_pings_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Close the session to `osd` the way a reset ends it, through
    /// `OSDSession::close()` and so through the I/O task's reset hook. A
    /// test aid for the reconnect path.
    pub async fn close_session_for_test(&self, osd: i32) {
        let session = self.sessions.write().await.remove(&osd);
        if let Some(session) = session {
            session.close().await;
        }
    }

    /// The current primary OSD of `object`.
    pub(crate) async fn primary_osd(&self, object: &ObjectId) -> Result<i32> {
        let osdmap = self.get_osdmap().await?;
        let (_, _, osds) = Self::object_to_osds_in_map(&osdmap, object)?;
        Ok(osds[0])
    }

    /// `object`'s PG interval in `osdmap`, placed by the same
    /// `object_pg_in_map` that routes its ops.
    fn linger_interval(
        &self,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        object: &ObjectId,
    ) -> Result<PgInterval> {
        let pool = osdmap
            .pools
            .get(&object.pool)
            .ok_or(OSDClientError::PoolNotFound(object.pool))?;
        let pg = Self::object_pg_in_map(osdmap, object)?.pg;
        let ua = osdmap
            .pg_to_up_acting(&pg)
            .map_err(|e| OSDClientError::Crush(format!("PG->OSD mapping failed: {e}")))?;
        Ok(PgInterval {
            up: ua.up,
            up_primary: ua.up_primary,
            acting: ua.acting,
            acting_primary: ua.acting_primary,
            size: pool.size,
            min_size: pool.min_size,
            pg_num: pool.pg_num,
            pgp_num: pool.pgp_num,
            pg_num_pending: pool.pg_num_pending,
            epoch: osdmap.epoch.as_u32(),
        })
    }

    /// Record where `linger` is going as the send goes out, before any
    /// reply: its PG interval in `osdmap` and the OSD (`osd`, else the
    /// acting primary). A reset of that session or a new interval then
    /// re-sends it even when this send gets no answer.
    fn record_target(
        &self,
        linger: &Linger,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        osd: Option<i32>,
    ) {
        let Ok(interval) = self.linger_interval(osdmap, &linger.object) else {
            return;
        };
        let osd = osd.unwrap_or(interval.acting_primary);
        let mut state = linger.lock_state();
        if osd >= 0 {
            state.osd = Some(osd);
        }
        state.interval = Some(interval);
    }

    /// Send one linger op once, WRITE|READ for a watch as Objecter sends
    /// them, recording its target as it goes out, then await and check the
    /// reply. `seq` is the send's number from [`Linger::begin_send`].
    async fn send_linger_op(
        &self,
        linger: &Linger,
        op: OSDOp,
        name: &str,
        seq: u64,
    ) -> Result<crate::osdclient::types::OpResult> {
        self.cancel_previous_send(linger, seq).await;
        if let Ok(osdmap) = self.get_osdmap().await {
            self.record_target(linger, &osdmap, None);
        }
        let outcome = async {
            let (osd, tid, osdmap, rx, _permit) = self.submit_linger(linger, op).await?;
            let superseded = {
                let mut state = linger.lock_state();
                if state.send_seq == seq {
                    state.in_flight = Some((osd, tid));
                }
                state.send_seq != seq
            };
            if superseded {
                // A later send began while this one was being routed and
                // found nothing to cancel.
                self.cancel_linger_send(osd, tid).await;
                return Err(OSDClientError::Cancelled);
            }
            self.record_target(linger, &osdmap, Some(osd));
            if matches!(linger.kind, LingerKind::Notify { .. }) {
                // Only the latest send, once a session took it, restarts
                // the notify's bound; a re-send that never got out, or
                // one that cancelled itself above, must not extend it.
                linger.resent.notify_one();
            }
            let result = await_op_result(rx, self.tracker.operation_timeout()).await?;
            Self::check_op_result(&result, name)?;
            Ok(result)
        }
        .await;
        {
            let mut state = linger.lock_state();
            if state.send_seq == seq {
                state.sending = false;
                state.resend_pending = outcome.as_ref().is_err_and(is_no_answer);
                state.in_flight = None;
            }
        }
        outcome
    }

    /// Submit one send of `linger` with [`Self::submit_once_in_map`].
    async fn submit_linger(
        &self,
        linger: &Linger,
        op: OSDOp,
    ) -> Result<(
        i32,
        u64,
        Arc<crate::osdclient::osdmap::OSDMap>,
        tokio::sync::oneshot::Receiver<Result<crate::osdclient::types::OpResult>>,
        Option<crate::osdclient::throttle::ThrottlePermit<'_>>,
    )> {
        #[cfg(test)]
        if let Some(submit) = self
            .fake_linger_submit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            let (osd, tid, osdmap, rx) = submit();
            return Ok((osd, tid, osdmap, rx, None));
        }
        self.submit_once_in_map(
            &linger.object,
            vec![op],
            crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
            OsdOpFlags::READ,
            SubmitKind::Linger,
        )
        .await
        .map_err(|e| match e {
            // Held by a pause until the deadline: never sent.
            OSDClientError::Timeout(_) => OSDClientError::Connection(format!("not sent: {e}")),
            e => e,
        })
    }

    /// Cancel the send before `seq` of `linger` if it is still pending, as
    /// `_send_linger` cancels the op at `register_tid` before it sends
    /// anew (v19.2.2:src/osdc/Objecter.cc:586-594): its reply would be
    /// ignored, and a rescan must not send it again.
    async fn cancel_previous_send(&self, linger: &Linger, seq: u64) {
        let previous = {
            let mut state = linger.lock_state();
            if state.send_seq == seq {
                state.in_flight.take()
            } else {
                None
            }
        };
        if let Some((osd, tid)) = previous {
            self.cancel_linger_send(osd, tid).await;
        }
    }

    /// Cancel linger send `tid` on `osd`'s session if it is still pending.
    async fn cancel_linger_send(&self, osd: i32, tid: u64) {
        let session = self.sessions.read().await.get(&osd).cloned();
        if let Some(session) = session
            && let Some(op) = session.remove_pending_op(tid)
        {
            self.cancel_pending_op(osd, op).await;
        }
    }

    /// End `op`, already removed from `osd`'s session, with `Cancelled`:
    /// Objecter's `_cancel_linger_op` (v19.2.2:src/osdc/Objecter.cc:
    /// 3157-3167). It clears the op's callback instead; here its sender
    /// takes `Cancelled` as no answer, and a superseded sender ignores it.
    async fn cancel_pending_op(&self, osd: i32, op: crate::osdclient::session::PendingOp) {
        debug!("cancelling linger op tid={} on OSD {}", op.tid, osd);
        self.tracker.untrack(op.tid, osd).await;
        let _ = op.result_tx.send(Err(OSDClientError::Cancelled));
    }

    /// Register a watch on `object` (`timeout` in seconds, 0 for the OSD's
    /// default). An OSD's rejection is returned (`ENOENT` for a missing
    /// object). A send that gets no answer (a lost session, no reachable
    /// OSD) is not an error: the registration stays pending and is sent
    /// again by the next five-second linger tick, session reset or map
    /// change, as Objecter keeps a linger op queued on its session; if
    /// none succeeds within the Tracker's operation timeout, the watch
    /// fails with `OSDClientError::Timeout`.
    pub(crate) async fn linger_watch(
        self: &Arc<Self>,
        object: ObjectId,
        timeout: u32,
    ) -> Result<(
        Arc<Linger>,
        tokio::sync::mpsc::UnboundedReceiver<WatchEvent>,
    )> {
        let cookie = self.next_cookie.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let linger = Arc::new(Linger::new(
            cookie,
            object,
            LingerKind::Watch { timeout },
            tx,
        ));
        let (reg_tx, reg_rx) = tokio::sync::oneshot::channel();
        linger.lock_state().registration = Some(reg_tx);
        self.lingers.insert(cookie, Arc::clone(&linger));
        let registration = LingerGuard::new(self, cookie);
        self.refuse_after_shutdown()?;

        // The deadline covers the first send too, which a pause can hold
        // for its own full timeout.
        let bound = self.tracker.operation_timeout();
        let deadline = tokio::time::Instant::now() + bound;
        self.send_register(&linger).await;
        tokio::time::timeout_at(deadline, reg_rx)
            .await
            .map_err(|_| OSDClientError::Timeout(bound))?
            .map_err(|_| OSDClientError::Internal("watch registration dropped".into()))??;
        registration.keep();
        debug!("watch {} registered on {:?}", cookie, linger.object);
        Ok((linger, rx))
    }

    /// Send a watch's registration, `WATCH{WATCH}`, once, and apply its
    /// outcome with [`Self::finish_register_send`].
    async fn send_register(&self, linger: &Linger) {
        let LingerKind::Watch { timeout } = linger.kind else {
            return;
        };
        if !self.lingers.contains_key(&linger.cookie) {
            return;
        }
        let sent = std::time::Instant::now();
        let op = OSDOp::watch(linger.cookie, WatchOp::Watch, timeout);
        let seq = linger.begin_send();
        let outcome = self.send_linger_op(linger, op, "watch", seq).await;
        Self::finish_register_send(linger, seq, sent, outcome);
    }

    /// Apply the outcome of registration send `seq` of a watch, sent at
    /// `sent`. Success marks it registered and answers `linger_watch`; an
    /// OSD's rejection answers it with the error; no answer leaves it
    /// pending for a re-send. A send a later one superseded changes
    /// nothing, as [`Self::finish_notify_send`] has it: the later send
    /// cancels it if it is still pending, as `_send_linger` does
    /// (v19.2.2:src/osdc/Objecter.cc:586-594), so `_linger_commit`
    /// never sees its reply, but here the reply may already have come.
    fn finish_register_send(
        linger: &Linger,
        seq: u64,
        sent: std::time::Instant,
        outcome: Result<crate::osdclient::types::OpResult>,
    ) {
        let mut state = linger.lock_state();
        if state.send_seq != seq {
            debug!(
                "watch {} registration send {} superseded, ignoring {:?}",
                linger.cookie,
                seq,
                outcome.map(|_| ())
            );
            return;
        }
        match outcome {
            Ok(_) => {
                state.registered = true;
                state.last_error = None;
                state.watch_valid_thru = Some(sent);
                drop(state);
                linger.finish_registration(Ok(()));
            }
            Err(e) if is_no_answer(&e) => {
                debug!(
                    "watch {} registration got no answer ({}); the next tick re-sends it",
                    linger.cookie, e
                );
            }
            Err(e) => {
                drop(state);
                linger.finish_registration(Err(e));
            }
        }
    }

    /// Re-register a watch with a single `WATCH{RECONNECT}` under a new
    /// generation. An OSD's rejection becomes the watch's one
    /// `Disconnect`; no answer leaves the watch as it is for the next
    /// reset or map change to re-send.
    async fn send_reconnect(self: Arc<Self>, linger: Arc<Linger>) {
        if !self.lingers.contains_key(&linger.cookie) {
            return;
        }
        let generation = {
            let mut state = linger.lock_state();
            state.register_gen += 1;
            state.register_gen
        };
        let op = OSDOp::watch_reconnect(linger.cookie, generation);
        let seq = linger.begin_send();
        let outcome = self
            .send_linger_op(&linger, op, "watch reconnect", seq)
            .await;
        let mut state = linger.lock_state();
        if state.register_gen != generation {
            return;
        }
        match outcome {
            Ok(_) => {
                info!("watch {} reconnected", linger.cookie);
                state.last_error = None;
            }
            Err(e) if is_no_answer(&e) => {
                debug!(
                    "watch {} reconnect got no answer ({}); the next tick re-sends it",
                    linger.cookie, e
                );
            }
            Err(e) => {
                drop(state);
                warn!("watch {} reconnect failed: {}", linger.cookie, e);
                linger.reconnect_failed(watch_error_code(&e));
            }
        }
    }

    /// Send a notify linger's NOTIFY, whole, unless it already completed,
    /// and apply its reply with [`Self::finish_notify_send`].
    async fn send_notify(&self, linger: &Linger) {
        let LingerKind::Notify {
            completion,
            notify_id,
            timeout_secs,
            payload,
        } = &linger.kind
        else {
            return;
        };
        let completed = completion
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none();
        if completed {
            return;
        }
        let seq = {
            let mut state = linger.lock_state();
            // The new primary allocates a new id; zeroed with the send's
            // number taken, so a reply to an older send cannot restore it.
            notify_id.store(0, Ordering::Release);
            Linger::begin_send_locked(&mut state)
        };
        let op = OSDOp::notify(linger.cookie, *timeout_secs, payload.clone());
        let outcome = self.send_linger_op(linger, op, "notify", seq).await;
        Self::finish_notify_send(linger, seq, outcome);
    }

    /// Apply the outcome of send `seq` of a notify linger: record the
    /// notify id its reply carries, or complete the notify with an OSD's
    /// rejection, as Objecter's `_linger_commit` does. No answer leaves
    /// it for the next reset or map change to re-send. A send a later one
    /// superseded, or one that ends after the completion, changes
    /// nothing. The later send cancels it if it is still pending
    /// ([`Self::cancel_previous_send`]), as Objecter's `_send_linger` does
    /// (v19.2.2:src/osdc/Objecter.cc:586-594), but it may already have
    /// ended, and its late reply, its `Cancelled` or its Tracker timeout
    /// must not fail the notify the later send carries.
    fn finish_notify_send(
        linger: &Linger,
        seq: u64,
        outcome: Result<crate::osdclient::types::OpResult>,
    ) {
        let LingerKind::Notify {
            completion,
            notify_id,
            ..
        } = &linger.kind
        else {
            return;
        };
        let state = linger.lock_state();
        let current = state.send_seq == seq
            && completion
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some();
        match outcome {
            Ok(result) => {
                let id = result
                    .ops
                    .first()
                    .and_then(|r| r.outdata.get(..8))
                    .map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes")));
                if current && let Some(id) = id {
                    notify_id.store(id, Ordering::Release);
                }
            }
            Err(e) if is_no_answer(&e) => {
                debug!(
                    "notify {} got no answer ({}); the next tick re-sends it",
                    linger.cookie, e
                );
            }
            Err(e) if !current => {
                debug!(
                    "notify {} send {} superseded, ignoring its error: {}",
                    linger.cookie, seq, e
                );
            }
            Err(e) => {
                drop(state);
                warn!("notify {} failed: {}", linger.cookie, e);
                linger.complete_notify(notify_error_code(&e));
            }
        }
    }

    /// Re-send a notify after a reset or an interval change.
    async fn resend_notify(self: Arc<Self>, linger: Arc<Linger>) {
        if self.lingers.contains_key(&linger.cookie) {
            self.send_notify(&linger).await;
        }
    }

    /// Notify `object`'s watchers with `payload` and wait for them all to
    /// ack or for the OSD's timeout. `timeout_ms` of 0 sends librados's
    /// `client_notify_timeout`.
    pub(crate) async fn notify(
        self: &Arc<Self>,
        object: ObjectId,
        payload: Bytes,
        timeout_ms: u64,
    ) -> Result<NotifyResult> {
        let timeout_secs = if timeout_ms == 0 {
            CLIENT_NOTIFY_TIMEOUT_SECS
        } else {
            u32::try_from(timeout_ms / 1000).unwrap_or(u32::MAX)
        };
        let cookie = self.next_cookie.fetch_add(1, Ordering::Relaxed);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let (events, _) = tokio::sync::mpsc::unbounded_channel();
        let linger = Arc::new(Linger::new(
            cookie,
            object,
            LingerKind::Notify {
                completion: std::sync::Mutex::new(Some(done_tx)),
                notify_id: std::sync::atomic::AtomicU64::new(0),
                timeout_secs,
                payload,
            },
            events,
        ));
        self.lingers.insert(cookie, Arc::clone(&linger));
        let _registration = LingerGuard::new(self, cookie);
        self.refuse_after_shutdown()?;
        // The notify ends with its completion, not with this first send's
        // reply: a re-send on a new interval supersedes it, and the OSD
        // may drop it unanswered, as Objecter never waits on a superseded
        // linger op. The send runs in its own task, as a re-send does, so
        // a caller that stops waiting cannot drop it half way through
        // submit_op and leave an op pending that nothing tracks.
        let first_send = {
            let client = Arc::clone(self);
            let linger = Arc::clone(&linger);
            self.spawn_until_shutdown(async move { client.send_notify(&linger).await })
        };
        let mut done_rx = done_rx;
        tokio::select! {
            done = &mut done_rx => {
                let done = done
                    .map_err(|_| OSDClientError::Internal("notify completion dropped".into()))?;
                return notify_outcome(done);
            }
            _ = first_send => {}
        }
        // As plan 12 has it, the outer bound starts once the first send
        // ends; a later send a session takes restarts it.
        await_notify(&linger, done_rx, notify_outer_bound(timeout_secs)).await
    }

    /// Acknowledge notify `notify_id` for watch `cookie` on `object`.
    pub(crate) async fn notify_ack(
        &self,
        object: &ObjectId,
        notify_id: u64,
        cookie: u64,
        reply: Bytes,
    ) -> Result<()> {
        let (_, _, rx, _permit) = self
            .submit_once(
                object,
                vec![OSDOp::notify_ack(notify_id, cookie, reply)],
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                OsdOpFlags::READ,
            )
            .await?;
        let result = await_op_result(rx, self.tracker.operation_timeout()).await?;
        Self::check_op_result(&result, "notify_ack")
    }

    /// Remove a watch from its object: forgotten here first, so no ping or
    /// reconnect follows, then `WATCH{UNWATCH}` as an ordinary write.
    pub(crate) async fn unwatch(&self, linger: &Linger) -> Result<()> {
        self.forget_linger(linger.cookie);
        let result = self
            .execute_op(
                linger.object.clone(),
                vec![OSDOp::watch(linger.cookie, WatchOp::Unwatch, 0)],
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                OsdOpFlags::empty(),
            )
            .await?;
        Self::check_op_result(&result, "unwatch")
    }

    /// Fail a linger just inserted if shutdown has begun: shutdown cancels
    /// the token before it drains `lingers`, so a linger inserted after
    /// the drain is caught here and one inserted before it is drained.
    fn refuse_after_shutdown(&self) -> Result<()> {
        if self.shutdown_token.is_cancelled() {
            return Err(OSDClientError::Connection("OSDClient shut down".into()));
        }
        Ok(())
    }

    /// Drop a linger from the registry; nothing is sent.
    pub(crate) fn forget_linger(&self, cookie: u64) {
        if let Some((_, linger)) = self.lingers.remove(&cookie) {
            linger.close_events();
        }
    }

    /// Shutdown's end for every linger: a watch gets its `Disconnect` and
    /// its event channel closed, a notify completes with `ECANCELED`.
    fn drain_lingers(&self) {
        let cookies: Vec<u64> = self.lingers.iter().map(|l| *l.key()).collect();
        for cookie in cookies {
            let Some((_, linger)) = self.lingers.remove(&cookie) else {
                continue;
            };
            match linger.kind {
                LingerKind::Watch { .. } => {
                    linger.finish_registration(Err(OSDClientError::Connection(
                        "OSDClient shut down".into(),
                    )));
                    linger.fail(crate::osdclient::error::ENOTCONN);
                }
                LingerKind::Notify { .. } => {
                    linger.complete_notify(crate::osdclient::error::ECANCELED);
                }
            }
            linger.close_events();
        }
    }

    /// One linger tick, every five seconds: re-send the lingers whose
    /// last send got no answer and reconnect watches in error, then ping
    /// every healthy watch with its generation. The pings are skipped
    /// while reads are paused, as Objecter skips them, and while pings are
    /// disabled; the re-sends are not.
    fn tick_lingers(self: &Arc<Self>) {
        for linger in lingers_to_resend(&self.lingers) {
            self.spawn_relinger(linger);
        }
        if !self.watch_pings_enabled.load(Ordering::Relaxed) {
            return;
        }
        if self
            .osdmap_rx
            .borrow()
            .as_ref()
            .is_none_or(|m| m.is_pauserd())
        {
            return;
        }
        for (linger, generation) in watches_to_ping(&self.lingers) {
            self.spawn_until_shutdown(Arc::clone(self).ping_watch(linger, generation));
        }
    }

    async fn ping_watch(self: Arc<Self>, linger: Arc<Linger>, generation: u32) {
        let sent = std::time::Instant::now();
        let outcome = self.send_ping(&linger, generation).await;
        if self.lingers.contains_key(&linger.cookie) {
            linger.ping_finished(generation, sent, outcome);
        }
    }

    async fn send_ping(&self, linger: &Linger, generation: u32) -> Result<()> {
        let (_, _, _, rx, _) = self
            .submit_once_in_map(
                &linger.object,
                vec![OSDOp::watch_ping(linger.cookie, generation)],
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                OsdOpFlags::READ,
                SubmitKind::Ping,
            )
            .await?;
        let result = await_op_result(rx, self.tracker.operation_timeout()).await?;
        Self::check_op_result(&result, "watch ping")
    }

    /// After a batch of maps is published: re-send every linger whose PG
    /// entered a new interval in any of `maps` (each applied epoch, in
    /// order), or every linger when epochs were skipped, and fail those
    /// whose pool is gone. Nothing here is awaited; the re-sends are
    /// spawned.
    fn scan_lingers_on_map_change(
        self: &Arc<Self>,
        maps: &[Arc<crate::osdclient::osdmap::OSDMap>],
        skipped: bool,
    ) {
        let scan = scan_lingers(&self.lingers, maps, skipped, |osdmap, object| {
            self.linger_interval(osdmap, object)
        });
        // A deleted pool unregisters the linger: a watch gets the
        // `ENOTCONN` every lost watch carries, a notify `ENOENT`.
        for linger in scan.pool_gone {
            info!(
                "pool {} of linger {} is gone",
                linger.object.pool, linger.cookie
            );
            match linger.kind {
                LingerKind::Watch { .. } => {
                    // A registration still pending fails with the pool, as
                    // `_check_linger_pool_dne` completes `on_reg_commit`.
                    linger
                        .finish_registration(Err(OSDClientError::PoolNotFound(linger.object.pool)));
                    linger.fail(crate::osdclient::error::ENOTCONN);
                }
                LingerKind::Notify { .. } => {
                    linger.complete_notify(crate::osdclient::error::ENOENT);
                }
            }
            self.forget_linger(linger.cookie);
        }
        for linger in scan.resend {
            self.spawn_relinger(linger);
        }
    }

    /// Re-send `linger` from a spawned task: a watch as `RECONNECT`, a
    /// notify whole.
    fn spawn_relinger(self: &Arc<Self>, linger: Arc<Linger>) {
        let client = Arc::clone(self);
        match linger.kind {
            LingerKind::Watch { .. } => {
                if linger.lock_state().registered {
                    self.spawn_until_shutdown(client.send_reconnect(linger));
                } else {
                    self.spawn_until_shutdown(async move { client.send_register(&linger).await });
                }
            }
            LingerKind::Notify { .. } => {
                self.spawn_until_shutdown(client.resend_notify(linger));
            }
        }
    }

    /// Run `task` in a task of its own that ends when the client shuts
    /// down, as plan 12 has `shutdown_token` cancel every timer and task.
    fn spawn_until_shutdown(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> tokio::task::JoinHandle<()> {
        let shutdown = self.shutdown_token.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                () = task => {}
            }
        })
    }

    /// Get the current OSDMap
    pub async fn get_osdmap(&self) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        self.osdmap_rx
            .borrow()
            .clone()
            .ok_or_else(|| OSDClientError::Connection("OSDMap not available".to_string()))
    }

    /// The cluster's `require_osd_release`, or
    /// [`OSDClientConfig::assume_osd_release`] when that is set.
    ///
    /// The map's value is kept current through incrementals. `None` means
    /// no map has arrived yet; this does not wait for one, whereas
    /// librados's `get_min_compatible_osd`
    /// (`src/librados/RadosClient.cc:436-447@main`) does.
    /// `Some(CephRelease::UNKNOWN)` is a map whose release was never set,
    /// which no v19 mon creates: a fresh v19 mon writes `squid` in
    /// `create_initial` (`src/mon/OSDMonitor.cc:677-687@v19.2.2`).
    pub fn require_osd_release(&self) -> Option<CephRelease> {
        effective_release(
            self.config.assume_osd_release,
            self.osdmap_rx.borrow().as_deref(),
        )
    }

    /// Return the current OSDMap epoch, or 0 if no map has been received yet.
    fn current_epoch_u32(&self) -> u32 {
        self.osdmap_rx
            .borrow()
            .as_ref()
            .map(|m| m.epoch)
            .unwrap_or_default()
            .as_u32()
    }

    /// Wait for OSDMap to be received
    ///
    /// This waits until the OSDClient has received an OSDMap from the monitor cluster.
    /// The OSDMap is required for all object operations as it contains the cluster topology.
    ///
    /// Should be called after subscribing to osdmap to ensure the map is available
    /// before performing object operations.
    pub async fn wait_for_osdmap(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        // epoch=0: any map with epoch ≥ 0 (always true) satisfies the condition.
        self.wait_for_epoch(0, timeout).await
    }

    /// Wait for a specific OSDMap epoch
    pub async fn wait_for_epoch(
        &self,
        epoch: u32,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        let mut rx = self.osdmap_rx.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(map) = rx.borrow_and_update().as_ref()
                    && map.epoch.as_u32() >= epoch
                {
                    return Ok(Arc::clone(map));
                }
                rx.changed()
                    .await
                    .map_err(|_| OSDClientError::Connection("watch channel closed".into()))?;
            }
        })
        .await
        .map_err(|_| OSDClientError::Timeout(timeout))?
    }

    /// Wait for any OSDMap with an epoch strictly greater than `current`'s epoch.
    ///
    /// Used to block an operation that is paused (pool-pause / pool-full) until
    /// the cluster state changes.  Bounded by `timeout` to prevent infinite waits.
    async fn wait_for_newer_osdmap(
        &self,
        current: &Arc<crate::osdclient::osdmap::OSDMap>,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        let target_epoch = current.epoch.as_u32() + 1;
        self.wait_for_epoch(target_epoch, timeout).await
    }

    /// Subscribe to OSDMap updates and wait until the monitor's current epoch arrives.
    pub async fn wait_for_latest_osdmap(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        self.mon_client
            .subscribe(crate::monclient::MonService::OsdMap, 0, 0)
            .await
            .map_err(OSDClientError::MonClient)?;

        let (epoch, _) = self
            .mon_client
            .get_version(crate::monclient::MonService::OsdMap)
            .await
            .map_err(OSDClientError::MonClient)?;

        self.wait_for_epoch(epoch as u32, timeout).await
    }

    /// Get or create a session for an OSD
    async fn get_or_create_session(&self, osd_id: i32) -> Result<Arc<OSDSession>> {
        #[cfg(test)]
        if let Some(session) = self
            .fake_osd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Ok(session);
        }
        // Get OSD address early (before acquiring any locks)
        // This reduces lock contention by doing I/O outside critical section
        let current_addr = self.get_osd_address(osd_id).await?;

        // Check if we already have a session
        // Clone Arc before releasing lock to avoid holding lock across await
        let existing_session = {
            let sessions = self.sessions.read().await;
            sessions.get(&osd_id).map(Arc::clone)
        };

        if let Some(session) = existing_session
            && session.is_connected()
        {
            // Validate that session's address matches current OSDMap
            if let Some(session_addr) = session.get_peer_address() {
                if session_addr.to_socket_addr() == current_addr.to_socket_addr() {
                    return Ok(session);
                } else {
                    info!(
                        "OSD {} address changed in OSDMap (was {:?}, now {:?}), creating new session",
                        osd_id,
                        session_addr.to_socket_addr(),
                        current_addr.to_socket_addr()
                    );
                }
            }
        }

        // Prepare session outside the write lock to minimize critical section
        // Get service auth provider from monitor client
        let auth_provider = self
            .mon_client
            .get_service_auth_provider()
            .await
            .map(|provider| Box::new(provider) as Box<dyn crate::auth::AuthProvider>);

        let mut session = OSDSession::new(
            osd_id,
            auth_provider,
            self.global_id,
            self.map_tx.clone(),
            self.self_weak.clone(),
            Arc::clone(&self.next_tid),
        );

        // Connect to OSD BEFORE acquiring write lock - this is the expensive operation
        // that can take seconds and should not block other session lookups
        info!("Creating new session for OSD {}", osd_id);
        session
            .connect(current_addr, self.shutdown_token.child_token())
            .await?;

        let session = Arc::new(session);

        // Now acquire write lock only to insert the connected session
        // Double-check after acquiring write lock to avoid race condition
        // Clone existing session before releasing lock to check it outside critical section
        let existing_to_check = {
            let sessions = self.sessions.read().await;
            sessions.get(&osd_id).map(Arc::clone)
        };

        if let Some(existing) = existing_to_check
            && existing.is_connected()
        {
            if let Some(session_addr) = existing.get_peer_address()
                && session_addr.to_socket_addr() == current_addr.to_socket_addr()
            {
                // Another task created a session while we were connecting
                // Close our redundant session and return existing one
                session.close().await;
                return Ok(existing);
            }

            // Address changed — fall through to insert the new session.
            // The stale session stays in the map so that insert() returns it
            // and kick_into_session migrates its pending ops.
            info!("OSD {} address changed, will replace session", osd_id);
        }

        let old_session = {
            let mut sessions = self.sessions.write().await;

            // Insert the new session, replacing any disconnected session.
            // insert() returns the old value so we can kick its pending ops.
            session.mark_published();
            sessions.insert(osd_id, Arc::clone(&session))
        };

        // Kick ops from old disconnected session into new session.
        // Matches C++ Objecter::_kick_requests() called after _reopen_session():
        // session is opened first, then ops are resent via the new connection.
        // CRUSH remapping is handled separately by scan_requests_on_map_change().
        if let Some(old) = old_session {
            self.kick_into_session(&old, &session).await;
        }

        Ok(session)
    }

    /// Get OSD address from OSDMap
    async fn get_osd_address(&self, osd_id: i32) -> Result<crate::EntityAddr> {
        let osdmap = self.get_osdmap().await?;

        // Check if OSD exists
        if osd_id < 0 || osd_id as usize >= osdmap.osd_addrs_client.len() {
            return Err(OSDClientError::Connection(format!(
                "OSD {osd_id} not found in OSDMap"
            )));
        }

        // Get the address vector for this OSD
        let addrvec = &osdmap.osd_addrs_client[osd_id as usize];

        // Find a v2 address (msgr2 protocol)
        addrvec
            .addrs
            .iter()
            .find(|addr| matches!(addr.addr_type, crate::EntityAddrType::Msgr2))
            .cloned()
            .ok_or_else(|| {
                OSDClientError::Connection(format!("No msgr2 address found for OSD {osd_id}"))
            })
    }

    /// Where `object` lives in `osdmap`, as Ceph's `Objecter::_calc_target`
    /// computes it (`src/osdc/Objecter.cc:2842-2863`): the raw PG from
    /// `object_locator_to_pg`, reduced by the pool's `raw_pg_to_pg`.
    ///
    /// This is the only way the client places an object: every op, every
    /// re-placement and every linger's interval comes through here.
    fn object_pg_in_map(
        osdmap: &crate::osdclient::osdmap::OSDMap,
        object: &ObjectId,
    ) -> Result<Placement> {
        let pool_info = osdmap
            .pools
            .get(&object.pool)
            .ok_or(OSDClientError::PoolNotFound(object.pool))?;
        let raw = osdmap
            .object_locator_to_pg(&object.oid, &ObjectLocator::from(object))
            .map_err(|e| OSDClientError::Crush(format!("Object->PG mapping failed: {e}")))?;
        Ok(Placement {
            hash: raw.seed,
            pg: pool_info.raw_pg_to_pg(raw),
        })
    }

    /// `object`'s hobject hash, its PG and the PG's acting OSDs.
    fn object_to_osds_in_map(
        osdmap: &crate::osdclient::osdmap::OSDMap,
        object: &ObjectId,
    ) -> Result<OpRoute> {
        let placement = Self::object_pg_in_map(osdmap, object)?;
        let (spg, osds) = Self::pg_to_spg_in_map(osdmap, placement.pg)?;
        debug!(
            "Mapped {}/{}/{} (key {:?}) to PG {spg:?}, OSDs: {osds:?}",
            object.pool, object.namespace, object.oid, object.key
        );
        Ok((placement.hash, spg, osds))
    }

    /// A PG's wire `spg_t` and its acting OSDs.
    fn pg_to_spg_in_map(
        osdmap: &crate::osdclient::osdmap::OSDMap,
        pg: crate::crush::placement::PgId,
    ) -> Result<(StripedPgId, Vec<i32>)> {
        let osds = Self::pg_to_osds_in_map(osdmap, pg)?;
        // For EC pools the wire spg_t carries a per-PG shard index.  For
        // replicated pools `pg_to_spg_shard` returns NO_SHARD (-1), so
        // this is correct in both cases.  Optimized EC pools currently
        // return an error from this call rather than mis-route silently.
        let shard = osdmap
            .pg_to_spg_shard(&pg)
            .map_err(|e| OSDClientError::Crush(format!("EC shard lookup: {e}")))?;
        Ok((StripedPgId::new(pg.pool, pg.seed, shard.0), osds))
    }

    /// Re-place an op already in flight in `osdmap`: its hobject hash,
    /// PG and acting OSDs.
    ///
    /// A PG op (PGNLS) is placed by the listing cursor's hash it carries,
    /// which is kept as is and never re-derived from the op's empty name;
    /// any other op is placed by its object's locator.
    fn replace_pending(osdmap: &crate::osdclient::osdmap::OSDMap, msg: &MOSDOp) -> Result<OpRoute> {
        if !Self::is_pg_op(msg) {
            return Self::object_to_osds_in_map(osdmap, &msg.object);
        }
        let pool = msg.object.pool;
        let pool_info = osdmap
            .pools
            .get(&pool)
            .ok_or(OSDClientError::PoolNotFound(pool))?;
        let pg = Self::cursor_pg(pool_info, pool, msg.object.hash);
        let (spg, osds) = Self::pg_to_spg_in_map(osdmap, pg)?;
        Ok((msg.object.hash, spg, osds))
    }

    /// The PG a PG op whose hobject hash is `hash` goes to in the map
    /// `pool_info` comes from: each map re-reduces the same cursor hash
    /// with `ceph_stable_mod`.
    fn cursor_pg(
        pool_info: &crate::osdclient::PgPool,
        pool: u64,
        hash: u32,
    ) -> crate::crush::placement::PgId {
        pool_info.raw_pg_to_pg(crate::crush::placement::PgId::new(pool, hash))
    }

    fn is_pg_op(msg: &MOSDOp) -> bool {
        OsdOpFlags::from_bits_truncate(msg.flags).contains(OsdOpFlags::PGOP)
    }

    /// The input `object`'s hash is computed from: pool, namespace, and
    /// the locator key, or the name when the key is empty.
    fn placement_key(object: &ObjectId) -> (u64, String, String) {
        let key = if object.key.is_empty() {
            &object.oid
        } else {
            &object.key
        };
        (object.pool, object.namespace.clone(), key.clone())
    }

    fn cached_rescan_osds(
        cache: &mut HashMap<(u64, String, String), OpRoute>,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        msg: &MOSDOp,
    ) -> Result<OpRoute> {
        if Self::is_pg_op(msg) {
            return Self::replace_pending(osdmap, msg);
        }
        let key = Self::placement_key(&msg.object);
        if let Some(entry) = cache.get(&key) {
            return Ok(entry.clone());
        }

        let entry = Self::replace_pending(osdmap, msg)?;
        cache.insert(key, entry.clone());
        Ok(entry)
    }

    /// Whether the PG an op with raw hash `ps` was placed in, when its pool
    /// had `old_pg_num` PGs, splits or merges now the pool has
    /// `new_pg_num`: Objecter::_calc_target's `split_or_merge` (ceph
    /// v19.2.2 src/osdc/Objecter.cc:2925-2931), false when the op recorded
    /// no pg_num.
    fn pg_split_or_merge(pool: u64, ps: u32, old_pg_num: u32, new_pg_num: u32) -> bool {
        use crate::crush::placement::{PgId, ceph_stable_mod, pg_num_mask};
        if old_pg_num == 0 {
            return false;
        }
        let prev = PgId::new(
            pool,
            ceph_stable_mod(ps, old_pg_num, pg_num_mask(old_pg_num)),
        );
        prev.is_split(old_pg_num, new_pg_num, None)
            || prev.is_merge_source(old_pg_num, new_pg_num, None)
            || prev.is_merge_target(old_pg_num, new_pg_num)
    }

    /// Map a PG to its acting OSD set, applying CRUSH placement and all overrides.
    /// Returns `NoOSDs` if the acting set is empty.
    fn pg_to_osds_in_map(
        osdmap: &crate::osdclient::osdmap::OSDMap,
        pg: crate::crush::placement::PgId,
    ) -> Result<Vec<i32>> {
        let osds = osdmap
            .pg_to_acting_osds(&pg)
            .map_err(|e| OSDClientError::Crush(format!("PG->OSD mapping failed: {e}")))?;

        if osds.is_empty() {
            return Err(OSDClientError::NoOSDs);
        }

        Ok(osds)
    }

    /// Apply redirect to an operation
    ///
    /// This modifies the operation's target object and flags based on the redirect
    /// information from an EC pool. Matches the behavior of `combine_with_locator()`
    /// in C++ Objecter (~/dev/ceph/src/osd/osd_types.h).
    pub(crate) fn apply_redirect(op: &mut MOSDOp, redirect: &RequestRedirect) {
        // Update object locator from redirect
        op.object.pool = redirect.redirect_locator.pool_id;
        op.object.key = redirect.redirect_locator.key.clone();
        op.object.namespace = redirect.redirect_locator.namespace.clone();

        // If redirect specifies a different object name, use it
        if !redirect.redirect_object.is_empty() {
            op.object.oid = redirect.redirect_object.clone();
        }

        // Set redirect flags (from ~/dev/ceph/src/osdc/Objecter.cc:3744)
        let redirect_flags =
            OsdOpFlags::REDIRECTED | OsdOpFlags::IGNORE_CACHE | OsdOpFlags::IGNORE_OVERLAY;
        op.flags |= redirect_flags.bits();

        debug!(
            "Applied redirect: pool={}, oid={}, key={}, nspace={}",
            op.object.pool, op.object.oid, op.object.key, op.object.namespace
        );
    }

    /// Execute a built operation created with OpBuilder
    ///
    /// This bridges the OpBuilder API to the internal execute_op implementation.
    /// Provides a convenient way to execute operations built with the fluent OpBuilder API.
    ///
    /// # Arguments
    /// * `pool` - Pool ID
    /// * `oid` - Object name
    /// * `built_op` - Built operation from OpBuilder
    ///
    /// # Returns
    /// Returns the OpResult after handling all redirects
    ///
    /// # Example
    /// ```ignore
    /// use crate::osdclient::OpBuilder;
    ///
    /// let op = OpBuilder::new()
    ///     .read(0, 4096)
    ///     .balance_reads()
    ///     .build();
    ///
    /// let result = client.execute_built_op(pool_id, "my-object", op).await?;
    /// ```
    pub async fn execute_built_op(
        &self,
        pool: u64,
        oid: &str,
        built_op: crate::osdclient::operation::BuiltOp,
    ) -> Result<crate::osdclient::types::OpResult> {
        self.execute_built_op_with_id(ObjectId::new(pool, oid), built_op)
            .await
    }

    /// Execute a pre-built operation with a full [`ObjectId`] (supports namespace and locator key).
    pub async fn execute_built_op_with_id(
        &self,
        object: ObjectId,
        built_op: crate::osdclient::operation::BuiltOp,
    ) -> Result<crate::osdclient::types::OpResult> {
        let timeout = built_op.timeout;
        let priority = if built_op.priority < 0 {
            crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT
        } else {
            built_op.priority
        };
        let extra_flags = built_op.flags;
        self.execute_op(object, built_op.into_ops(), timeout, priority, extra_flags)
            .await
    }

    /// Execute an OSD operation with automatic redirect handling
    ///
    /// This is the common pattern for all OSD operations:
    /// 1. Acquire throttle permit
    /// 2. Get OSDMap epoch
    /// 3. Build MOSDOp message
    /// 4. Redirect retry loop with automatic session management
    /// 5. Return OpResult for caller to process
    ///
    /// # Arguments
    /// * `object` - Full object address (pool, oid, namespace, locator key)
    /// * `ops` - Operations to execute
    ///
    /// # Returns
    /// Returns the OpResult after handling all redirects
    ///
    /// # Errors
    /// On [`OSDClientError::Connection`] or [`OSDClientError::Timeout`] the
    /// OSD may already have applied a write. A caller must re-check the
    /// state the op would change rather than issue it again: an op issued
    /// again is a new op, with a new tid, which the OSD does not
    /// deduplicate against this one.
    async fn execute_op(
        &self,
        object: ObjectId,
        ops: Vec<OSDOp>,
        timeout: Option<std::time::Duration>,
        priority: i32,
        extra_flags: crate::osdclient::types::OsdOpFlags,
    ) -> Result<crate::osdclient::types::OpResult> {
        // Fail immediately if the client has been fenced by the cluster.
        if self.blocklisted.load(Ordering::Relaxed) {
            return Err(OSDClientError::Blocklisted);
        }

        // Calculate operation budget and acquire throttle permit
        let budget = calc_op_budget(&ops);
        let _throttle_permit = self.throttle.acquire(budget).await?;
        debug!(
            "Acquired throttle permit: budget={} bytes, current_ops={}, current_bytes={}",
            budget,
            self.throttle.current_ops(),
            self.throttle.current_bytes()
        );

        // Compute an absolute deadline once so that pause-wait loops don't
        // inadvertently extend the total operation time beyond the timeout.
        let effective_timeout = timeout.unwrap_or_else(|| self.tracker.operation_timeout());
        let deadline = std::time::Instant::now() + effective_timeout;

        let mut op = self.prepare_op(object, ops, extra_flags).await?;

        // Cap connection-error retries so a persistently broken OSD can't spin
        // the caller forever.  Mirrors librados Objecter's reopen-on-reset
        // pattern: when a lossy connection drops mid-op, re-route to a fresh
        // session and resubmit under the op's tid, but only a bounded number
        // of times.
        const MAX_CONNECTION_RETRIES: u32 = 4;
        let mut connection_retries = 0u32;

        // Redirect/pause retry loop
        loop {
            let submitted = self
                .route_and_submit(&mut op, priority, deadline, effective_timeout)
                .await?;
            let primary_osd = submitted.osd;
            let result_rx = match submitted.rx {
                Ok(rx) => rx,
                Err(OSDClientError::Connection(msg_str)) => {
                    // Session died between get_or_create_session and submit_op
                    // (io_loop exited, drain fired).  Reopen on the next
                    // iteration — get_or_create_session will build a fresh
                    // session and we'll re-encode and resubmit under the tid.
                    connection_retries += 1;
                    if connection_retries > MAX_CONNECTION_RETRIES {
                        return Err(OSDClientError::Connection(format!(
                            "OSD {primary_osd} connection repeatedly lost (submit): {msg_str}"
                        )));
                    }
                    warn!(
                        "Op submit failed on OSD {} (attempt {}/{}): {}; retrying on fresh session",
                        primary_osd, connection_retries, MAX_CONNECTION_RETRIES, msg_str
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            // Wait for result using the remaining time towards the deadline.
            let remaining = deadline_remaining(deadline, effective_timeout)?;
            let result = match await_op_result(result_rx, remaining).await {
                Ok(r) => r,
                Err(OSDClientError::Connection(msg_str)) => {
                    // io_loop exited while we were awaiting the reply (drain
                    // fired a Connection error through result_tx).  Reopen on
                    // the next iteration.
                    connection_retries += 1;
                    if connection_retries > MAX_CONNECTION_RETRIES {
                        return Err(OSDClientError::Connection(format!(
                            "OSD {primary_osd} connection repeatedly lost (await): {msg_str}"
                        )));
                    }
                    warn!(
                        "Op lost connection to OSD {} mid-flight (attempt {}/{}): {}; retrying",
                        primary_osd, connection_retries, MAX_CONNECTION_RETRIES, msg_str
                    );
                    // The OSD may have run the op before the connection
                    // dropped. route_and_submit sends it again under its
                    // tid, as Objecter's _kick_requests resends a reset
                    // session's ops (v19.2.6:src/osdc/Objecter.cc:
                    // 2082-2113), so the OSD answers a write it already
                    // ran from its log (v19.2.6:src/osd/PrimaryLogPG.cc:
                    // 2218-2244) rather than running it again.
                    continue;
                }
                Err(e) => return Err(e),
            };

            // ENXIO: OSD rejected the op as misdirected — we sent to a replica instead of the
            // primary (our PG→acting-primary mapping was stale).  In production, the OSD returns
            // ENXIO rather than asserting; we must fetch a newer OSDMap and retry so the next
            // iteration of this loop picks the correct primary.
            //
            // This mirrors Objecter::handle_osd_op_reply()'s ENXIO path in C++.
            if result.result == crate::osdclient::error::ENXIO {
                let remaining = deadline_remaining(deadline, effective_timeout)?;
                warn!(
                    "Op got ENXIO from OSD {} (misdirected op — stale acting-set mapping); \
                     fetching newer OSDMap and retrying",
                    primary_osd
                );
                op.osdmap = self.wait_for_newer_osdmap(&op.osdmap, remaining).await?;
                op.osdmap
                    .prune_snap_context(op.msg.object.pool, &mut Arc::make_mut(&mut op.msg).snaps);
                continue;
            }

            // Check for redirect
            if let Some(redirect) = result.redirect {
                debug!(
                    "Received redirect to pool={}, object={}, retrying",
                    redirect.redirect_locator.pool_id,
                    if redirect.redirect_object.is_empty() {
                        op.msg.object.oid.as_str()
                    } else {
                        redirect.redirect_object.as_str()
                    }
                );
                // pending_op was dropped when the reply arrived, so refcount is 1 here.
                let m = Arc::make_mut(&mut op.msg);
                Self::apply_redirect(m, &redirect);
                continue;
            }

            // No redirect, return result for caller to process
            return Ok(result);
        }
    }

    /// Build the `MOSDOp` for `ops` against the current map, as every send
    /// starts: flags, snap context, and mtime for writes.
    async fn prepare_op(
        &self,
        object: ObjectId,
        ops: Vec<OSDOp>,
        extra_flags: OsdOpFlags,
    ) -> Result<PreparedOp> {
        // Get OSDMap epoch from OSDClient's own osdmap (not from MonClient)
        let osdmap = self.get_osdmap().await?;

        // Build initial message
        // OR in caller-supplied extra flags (e.g. BALANCE_READS, FULL_TRY, FULL_FORCE).
        let flags = MOSDOp::calculate_flags(&ops) | extra_flags.bits();
        let pool = object.pool;
        let mut msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch.as_u32(),
            flags,
            object,
            StripedPgId::from_pg(pool, 0), // Will be set in loop
            ops,
            crate::osdclient::types::RequestId::new(
                &self.entity_name,
                0,
                self.config.client_inc as i32,
            ),
            self.global_id,
        );

        // Initialise snap context from pool (Objecter::_calc_target snapc setup).
        // For pools without snaps this is a no-op (snap_seq=0, snaps=[]).
        (msg.snap_seq, msg.snaps) = osdmap.pool_snap_context(msg.object.pool);
        // Mirror C++ Objecter `_op_submit_with_budget` calling
        // `_prune_snapc` right after building the snapc: catches the
        // narrow window where a snap was just removed by the latest
        // OSDMap delta but is still listed in `pool.snaps`.
        osdmap.prune_snap_context(msg.object.pool, &mut msg.snaps);

        // Pre-compute flag-derived booleans that are constant across iterations.
        let is_write = flags & OsdOpFlags::WRITE.bits() != 0;
        let is_read = !is_write;
        let respects_full = flags & (OsdOpFlags::FULL_TRY | OsdOpFlags::FULL_FORCE).bits() == 0;

        // Set mtime for write ops — mirrors librados setting real_clock::now() when
        // the caller provides no explicit mtime. Reads carry UTime::zero().
        if is_write {
            msg.mtime = crate::UTime::now();
        }
        // retry_attempt 0 = first send; set here so submit_op receives it via Arc.
        msg.retry_attempt = 0;

        // Wrap in Arc so submit_op can take ownership without cloning the payload.
        // Arc::make_mut in the loop is O(1) (refcount == 1 between iterations) and
        // only copies on redirect, which is an EC-pool corner case.
        Ok(PreparedOp {
            msg: Arc::new(msg),
            osdmap,
            class: OpClass {
                is_write,
                is_read,
                respects_full,
                ping: false,
            },
            should_resend: true,
        })
    }

    /// Route `op` against the current map and hand it to its primary's
    /// session once, waiting out pauses, barriers and map changes that
    /// happen while routing. An error in the outer `Result` ends the op;
    /// `Submitted::rx` carries `submit_op`'s own result, whose connection
    /// errors a caller may retry.
    async fn route_and_submit(
        &self,
        op: &mut PreparedOp,
        priority: i32,
        deadline: std::time::Instant,
        effective_timeout: std::time::Duration,
    ) -> Result<Submitted> {
        loop {
            let msg = &mut op.msg;
            // Refresh the epoch in the message to reflect the current OSDMap.
            // C++ _prepare_osd_op reads osdmap->get_epoch() at send time; after a
            // pause-wait that loads a newer map we must do the same.
            Arc::make_mut(msg).osdmap_epoch = op.osdmap.epoch.as_u32();

            // Check pool EIO flag — hard fail, mirrors RECALC_OP_TARGET_POOL_EIO.
            if op.osdmap.is_pool_eio(msg.object.pool) {
                return Err(OSDClientError::OSDError {
                    code: -libc::EIO,
                    message: format!("pool {} has EIO flag set", msg.object.pool),
                });
            }

            // Check epoch barrier — block until osdmap.epoch >= barrier.
            // Mirrors Objecter::_calc_target RECALC_OP_TARGET_BARRIER_NEWER path.
            let barrier = self.epoch_barrier.load(Ordering::Relaxed);
            let behind_barrier =
                !op.class.ping && barrier != 0 && op.osdmap.epoch.as_u32() < barrier;

            // Check pool-pause and pool-full state before sending.
            // Mirrors Objecter::_calc_target() pauserd/pausewr checks.
            let (pauserd, pausewr, pool_full) = op.class.held_by(&op.osdmap, msg.object.pool);
            let paused = behind_barrier || pauserd || pausewr || pool_full;
            if paused {
                let remaining = deadline_remaining(deadline, effective_timeout)?;
                info!(
                    "Op on pool {} is paused (barrier={}, behind_barrier={}, pauserd={}, \
                     pausewr={}, pool_full={}); waiting for OSDMap update",
                    msg.object.pool, barrier, behind_barrier, pauserd, pausewr, pool_full,
                );
                op.osdmap = self.wait_for_newer_osdmap(&op.osdmap, remaining).await?;
                // Prune snap context: remove any snap IDs that were purged in
                // the new OSDMap — mirrors Objecter::_prune_snapc().
                op.osdmap
                    .prune_snap_context(msg.object.pool, &mut Arc::make_mut(msg).snaps);
                continue;
            }

            // Map to OSDs based on current object (using the osdmap we
            // already have), or a PG op by its cursor.
            let (hash, spg, osds) = Self::replace_pending(&op.osdmap, msg)?;
            let primary_osd = osds[0];
            tracing::trace!(
                target: "rados::osdclient::routing",
                "routing epoch={} pool={} oid={} spg={:?} osds={:?} hash=0x{:08x}",
                op.osdmap.epoch.as_u32(),
                msg.object.pool,
                msg.object.oid,
                spg,
                osds,
                hash,
            );

            // Get session
            let session = self.get_or_create_session(primary_osd).await?;

            // Re-check epoch: get_or_create_session can block for TCP connect +
            // auth handshake (hundreds of milliseconds), during which the OSDMap
            // may have advanced.  If so our routing is stale — retry before
            // allocating a TID so the continue costs nothing.
            if self.current_epoch_u32() != op.osdmap.epoch.as_u32() {
                // Bail out early if we've run out of time; we only need the
                // pass/fail result here (the fresh map is fetched below).
                deadline_remaining(deadline, effective_timeout)?;
                op.osdmap = self.get_osdmap().await?;
                op.osdmap
                    .prune_snap_context(msg.object.pool, &mut Arc::make_mut(msg).snaps);
                continue;
            }

            // An op keeps the tid it is first given, as Objecter's
            // _op_submit gives one only to an op without one
            // (v19.2.6:src/osdc/Objecter.cc:2472-2473): the OSD knows a
            // resent write by its reqid and, if it already ran it, answers
            // from its log. Each send under a tid already given carries the
            // next retry_attempt, as _prepare_osd_op numbers every send
            // (3236), so a reply to an earlier send is ignored.
            //
            // This counts the sends of this copy of the op, not a rescan's
            // resends of it (insert_migrated_op numbers the session's
            // copy), so a send after a drain may repeat an attempt number
            // already sent. No reply to that earlier send can be taken for
            // this one's: a drain runs only after the I/O loop has returned
            // with its send channel dropped, and a rescan that moved the op
            // to another OSD closed the session it left before sending it
            // on, so no connection that carried an earlier send can still
            // answer.
            let resend = msg.reqid.tid != 0;
            let tid = if resend {
                msg.reqid.tid
            } else {
                session.next_tid()
            };
            let placed_epoch = op.osdmap.epoch.as_u32();
            {
                let m = Arc::make_mut(msg);
                m.object.hash = hash;
                m.pgid = spg;
                m.osdmap_epoch = placed_epoch;
                m.reqid = crate::osdclient::types::RequestId::new(
                    &self.entity_name,
                    tid,
                    self.config.client_inc as i32,
                );
                if resend {
                    m.retry_attempt += 1;
                }
            }
            let pg_num = op
                .osdmap
                .pools
                .get(&msg.object.pool)
                .map_or(0, |p| p.pg_num);

            #[cfg(test)]
            if let Some(hook) = self
                .before_submit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
            {
                hook();
            }

            // Objecter::op_submit holds rwlock shared from _calc_target
            // through the send, and handle_osd_map holds it unique from
            // publishing a map through its rescan
            // (v19.2.2:src/osdc/Objecter.cc:1164, 2278), so an op is
            // either placed in the new map or pending when the rescan
            // looks. Here a map published after the placement above may
            // have been rescanned before the op became pending, which
            // would miss it: the OSD drops an op of an older interval.
            // submit_op checks, with the op pending and before its frame
            // goes out, that the map is still the one it was placed in;
            // the rescan publishes before it looks, so a map it misses the
            // op in is one this check sees. If the map moved, the op is
            // placed again, unsent.
            let placed = session
                .submit_op(Arc::clone(msg), priority, pg_num, op.should_resend, || {
                    #[cfg(test)]
                    if let Some(hook) = self
                        .in_submit_window
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_mut()
                    {
                        hook();
                    }
                    self.current_epoch_u32() == placed_epoch
                })
                .await;
            let rx = match placed {
                Ok(Some(rx)) => Ok(rx),
                // A linger send or ping taken back ends as no answer, as a
                // map change cancels one already out: the linger machinery
                // sends it again in the new map.
                Ok(None) if !op.should_resend => {
                    debug!(
                        "OSDMap moved past epoch {} before linger tid {} went out; cancelled",
                        placed_epoch, tid
                    );
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let _ = tx.send(Err(OSDClientError::Cancelled));
                    Ok(rx)
                }
                Ok(None) => {
                    debug!(
                        "OSDMap moved past epoch {} before tid {} went out; placing it again",
                        placed_epoch, tid
                    );
                    deadline_remaining(deadline, effective_timeout)?;
                    op.osdmap = self.get_osdmap().await?;
                    op.osdmap
                        .prune_snap_context(msg.object.pool, &mut Arc::make_mut(msg).snaps);
                    continue;
                }
                Err(e) => Err(e),
            };
            return Ok(Submitted {
                osd: primary_osd,
                tid,
                osdmap: Arc::clone(&op.osdmap),
                rx,
            });
        }
    }

    /// Build and send `ops` once, as `execute_op` does before it awaits
    /// the reply, with no retry: the linger ops (register, reconnect,
    /// ping, ack, notify) each decide for themselves what a failure means.
    /// Returns the primary it went to, the map epoch it was sent in, the
    /// reply channel and the throttle permit, which the caller holds until
    /// the reply arrives. The Tracker bounds the reply at its
    /// `operation_timeout`, delivered as `OSDClientError::Timeout`.
    pub(crate) async fn submit_once(
        &self,
        object: &ObjectId,
        ops: Vec<OSDOp>,
        priority: i32,
        extra_flags: OsdOpFlags,
    ) -> Result<(
        i32,
        u32,
        tokio::sync::oneshot::Receiver<Result<crate::osdclient::types::OpResult>>,
        crate::osdclient::throttle::ThrottlePermit<'_>,
    )> {
        let (osd, _, osdmap, rx, permit) = self
            .submit_once_in_map(object, ops, priority, extra_flags, SubmitKind::Op)
            .await?;
        let permit = permit.ok_or_else(|| OSDClientError::Internal("op not budgeted".into()))?;
        Ok((osd, osdmap.epoch.as_u32(), rx, permit))
    }

    /// [`Self::submit_once`], returning the op's tid and the map it was
    /// routed in; a [`SubmitKind::Linger`] or [`SubmitKind::Ping`] op
    /// takes no throttle permit and is never re-sent.
    async fn submit_once_in_map(
        &self,
        object: &ObjectId,
        ops: Vec<OSDOp>,
        priority: i32,
        extra_flags: OsdOpFlags,
        kind: SubmitKind,
    ) -> Result<(
        i32,
        u64,
        Arc<crate::osdclient::osdmap::OSDMap>,
        tokio::sync::oneshot::Receiver<Result<crate::osdclient::types::OpResult>>,
        Option<crate::osdclient::throttle::ThrottlePermit<'_>>,
    )> {
        if self.blocklisted.load(Ordering::Relaxed) {
            return Err(OSDClientError::Blocklisted);
        }
        let permit = self.acquire_budget(&ops, kind).await?;
        let effective_timeout = self.tracker.operation_timeout();
        let deadline = std::time::Instant::now() + effective_timeout;
        let mut op = self.prepare_op(object.clone(), ops, extra_flags).await?;
        op.class.ping = kind == SubmitKind::Ping;
        op.should_resend = kind == SubmitKind::Op;
        let submitted = self
            .route_and_submit(&mut op, priority, deadline, effective_timeout)
            .await?;
        Ok((
            submitted.osd,
            submitted.tid,
            submitted.osdmap,
            submitted.rx?,
            permit,
        ))
    }

    /// The throttle permit `ops` need, or none for a linger op.
    async fn acquire_budget(
        &self,
        ops: &[OSDOp],
        kind: SubmitKind,
    ) -> Result<Option<crate::osdclient::throttle::ThrottlePermit<'_>>> {
        match kind {
            SubmitKind::Op => Ok(Some(self.throttle.acquire(calc_op_budget(ops)).await?)),
            SubmitKind::Linger | SubmitKind::Ping => Ok(None),
        }
    }

    /// Check an OpResult for errors.
    ///
    /// Ceph's result codes are errno-shaped: a negative code is a failure,
    /// zero or positive is success. Some ops report success with a positive
    /// code (`CMPXATTR` returns 1 when the comparison holds), so this tests
    /// `< 0`, not `!= 0`, on the overall result and on the first op's
    /// return code. The per-op `FAILOK` flag is not honoured here: if the
    /// first op carries it and fails, its negative code is reported even
    /// though the OSD let the request succeed.
    pub(crate) fn check_op_result(
        result: &crate::osdclient::types::OpResult,
        op_name: &str,
    ) -> Result<()> {
        if result.result < 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: format!("{op_name} failed"),
            });
        }
        if let Some(op) = result.ops.first()
            && op.return_code < 0
        {
            return Err(OSDClientError::OSDError {
                code: op.return_code,
                message: format!("{op_name} failed"),
            });
        }
        Ok(())
    }

    /// Advance the epoch barrier to `epoch` (no-op if `epoch` ≤ current barrier).
    ///
    /// Ops will be held in the pause-wait loop until the current OSDMap epoch
    /// is at least `epoch`.  This is a one-way ratchet — calling it with a
    /// lower epoch than the current barrier has no effect.
    ///
    /// Mirrors `Objecter::set_epoch_barrier()` in C++.
    pub fn set_epoch_barrier(&self, epoch: u32) {
        self.epoch_barrier.fetch_max(epoch, Ordering::Relaxed);
    }

    /// Read data from an object
    pub async fn read(&self, pool: u64, oid: &str, offset: u64, len: u64) -> Result<ReadResult> {
        debug!(
            "read pool={} oid={} offset={} len={}",
            pool, oid, offset, len
        );

        let ops = vec![OSDOp::read(offset, len)];
        let result = self
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "Read")?;

        Ok(ReadResult {
            data: result.first_outdata()?.clone(),
            version: result.version,
        })
    }

    /// Sparse read data from an object
    ///
    /// Sparse read returns a map of extents indicating which regions of the object
    /// contain data, along with the actual data. This is useful for efficiently
    /// reading sparse objects (e.g., VM disk images with holes).
    pub async fn sparse_read(
        &self,
        pool: u64,
        oid: &str,
        offset: u64,
        len: u64,
    ) -> Result<crate::osdclient::types::SparseReadResult> {
        self.sparse_read_with_id(ObjectId::new(pool, oid), offset, len)
            .await
    }

    /// Sparse read with a full [`ObjectId`] (supports namespace and locator key).
    pub async fn sparse_read_with_id(
        &self,
        object: ObjectId,
        offset: u64,
        len: u64,
    ) -> Result<crate::osdclient::types::SparseReadResult> {
        debug!(
            "sparse_read pool={} oid={} offset={} len={}",
            object.pool, object.oid, offset, len
        );

        let ops = vec![OSDOp::sparse_read(offset, len)];
        let result = self
            .execute_op(
                object,
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "sparse_read")?;

        let sparse = crate::osdclient::types::SparseReadResult::from_op_result(&result)?;

        debug!(
            "Sparse read decoded: {} extents, {} data bytes",
            sparse.extents.len(),
            sparse.data.len()
        );

        Ok(sparse)
    }

    /// Write data to an object
    pub async fn write(
        &self,
        pool: u64,
        oid: &str,
        offset: u64,
        data: bytes::Bytes,
    ) -> Result<WriteResult> {
        debug!(
            "write pool={} oid={} offset={} len={}",
            pool,
            oid,
            offset,
            data.len()
        );

        let ops = vec![OSDOp::write(offset, data)];
        let result = self
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "Write")?;

        Ok(WriteResult {
            version: result.version,
        })
    }

    /// Write full object (overwrite)
    pub async fn write_full(
        &self,
        pool: u64,
        oid: &str,
        data: bytes::Bytes,
    ) -> Result<WriteResult> {
        debug!("write_full pool={} oid={} len={}", pool, oid, data.len());

        let ops = vec![OSDOp::write_full(data)];
        let result = self
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "WriteFull")?;

        Ok(WriteResult {
            version: result.version,
        })
    }

    /// Get object statistics
    pub async fn stat(&self, pool: u64, oid: &str) -> Result<StatResult> {
        debug!("stat pool={} oid={}", pool, oid);
        let ops = vec![OSDOp::stat()];
        let result = self
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "Stat")?;
        StatResult::from_op_result(&result)
    }

    /// Delete an object
    pub async fn delete(&self, pool: u64, oid: &str) -> Result<()> {
        debug!("delete pool={} oid={}", pool, oid);
        let ops = vec![OSDOp::delete()];
        let result = self
            .execute_op(
                ObjectId::new(pool, oid),
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                crate::osdclient::types::OsdOpFlags::empty(),
            )
            .await?;

        Self::check_op_result(&result, "Delete")?;

        Ok(())
    }

    /// Parse a list cursor string into an `HObject` starting position.
    ///
    /// Cursor format: decimal representation of the hobject raw hash.
    /// `None` means "start from the very beginning" (hash=0).
    fn parse_list_cursor(pool: u64, cursor: Option<String>) -> Result<crate::HObject> {
        match cursor {
            None => Ok(crate::HObject::empty_cursor(pool)),
            Some(s) => {
                let hash: u32 = s.parse().map_err(|e| {
                    OSDClientError::InvalidOperation(format!("Invalid list cursor '{s}': {e}"))
                })?;
                Ok(crate::HObject::new(pool, String::new(), hash))
            }
        }
    }

    /// The object a PGNLS request addresses: no name or key, the listed
    /// namespace, and the cursor hash as its hobject hash, as Ceph's
    /// `Objecter::list_nobjects` builds `object_locator_t oloc(pool_id,
    /// list_context->nspace)` (`src/osdc/Objecter.cc:3854`).
    fn pgnls_target(pool: u64, cursor_hash: u32, nspace: &str) -> ObjectId {
        let mut object = ObjectId::with_namespace(pool, "", nspace);
        object.hash = cursor_hash;
        object
    }

    /// Query objects from the PG that contains `hobject_cursor` in the
    /// current map, one page of a listing.
    ///
    /// The op goes out as any other, placed and stamped with the map
    /// current when it is sent and re-placed on a map change, as
    /// `Objecter::list_nobjects` sends each page through `pg_read` and
    /// `op_submit` (v19.2.2:src/osdc/Objecter.cc:3793-3862). The PG
    /// comes from `hobject_cursor.hash` with `ceph_stable_mod` in that map
    /// ([`Self::replace_pending`]), and the cursor hash is the op's
    /// hobject hash, so the OSD's `pgid.contains(head)` check passes.
    ///
    /// Returns the decoded response and result code from the OSD.
    async fn query_pg_objects(
        &self,
        pool: u64,
        nspace: &str,
        hobject_cursor: &crate::HObject,
        max_entries: u64,
        osdmap: &crate::osdclient::osdmap::OSDMap,
    ) -> Result<(crate::osdclient::PgNlsResponse, i32)> {
        let current_pg = osdmap
            .pools
            .get(&pool)
            .map(|p| Self::cursor_pg(p, pool, hobject_cursor.hash).seed)
            .ok_or(OSDClientError::PoolNotFound(pool))?;
        let object = Self::pgnls_target(pool, hobject_cursor.hash, nspace);
        let ops = vec![OSDOp::pgls(
            max_entries,
            hobject_cursor.clone(),
            osdmap.epoch.as_u32(),
        )?];
        let result = self
            .execute_op(
                object,
                ops,
                None,
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                OsdOpFlags::empty(),
            )
            .await?;

        // For PGLS: result = 1 means "reached end of PG" (success)
        //           result = 0 means "more objects available" (success)
        //           result < 0 means error
        if result.result < 0 {
            return Err(OSDClientError::OSDError {
                code: result.result,
                message: format!("List operation failed for PG {current_pg}"),
            });
        }

        // Parse the pg_nls_response from outdata
        if result.ops.is_empty() {
            return Err(OSDClientError::Internal(
                "No operation reply in list response".into(),
            ));
        }

        let outdata = &result.ops[0].outdata;

        // Decode pg_nls_response_t using proper Denc
        let response = crate::osdclient::PgNlsResponse::decode(&mut outdata.as_ref(), 0)?;

        debug!(
            "PG {} returned {} entries, handle.hash={:#x}, result={}",
            current_pg,
            response.entries.len(),
            response.handle.hash,
            result.result
        );

        Ok((response, result.result))
    }

    /// Build a `ListResult` from collected entries and the next cursor handle.
    ///
    /// `next_handle` is the `response.handle` returned by the last PGNLS call.
    /// When `next_handle.max` is true the pool is exhausted; otherwise encode
    /// the handle's hash as the cursor string for the next `list` call.
    fn build_list_result(
        entries: Vec<ListObjectEntry>,
        next_handle: &crate::HObject,
    ) -> ListResult {
        let cursor = if next_handle.max {
            None
        } else {
            Some(next_handle.hash.to_string())
        };
        ListResult { entries, cursor }
    }

    /// List objects in a pool's default namespace
    ///
    /// Lists objects in the specified pool using PGNLS, following the cursor returned
    /// by each OSD reply (bitwise-sorted order).  The cursor is the raw hobject hash
    /// encoded as a decimal string; `None` starts from the beginning of the pool.
    ///
    /// # Arguments
    /// * `pool`        - Pool ID to list objects from
    /// * `cursor`      - Optional pagination cursor (decimal hash string from a prior call)
    /// * `max_entries` - Maximum number of entries to return per call
    pub async fn list(
        &self,
        pool: u64,
        cursor: Option<String>,
        max_entries: u64,
    ) -> Result<ListResult> {
        self.list_in_namespace(pool, "", cursor, max_entries).await
    }

    /// List objects in one namespace of a pool, or in every namespace
    /// when `nspace` is [`ALL_NSPACES`](crate::osdclient::ALL_NSPACES).
    ///
    /// As [`OSDClient::list`], whose cursors it shares; each entry carries
    /// its namespace and locator key.
    pub async fn list_in_namespace(
        &self,
        pool: u64,
        nspace: &str,
        cursor: Option<String>,
        max_entries: u64,
    ) -> Result<ListResult> {
        debug!("list pool={pool} nspace={nspace:?} cursor={cursor:?} max_entries={max_entries}");

        // Fail immediately if the client has been fenced by the cluster.
        if self.blocklisted.load(Ordering::Relaxed) {
            return Err(OSDClientError::Blocklisted);
        }

        // Parse cursor → starting hobject position
        let mut hobject_cursor = Self::parse_list_cursor(pool, cursor)?;

        let osdmap = self.get_osdmap().await?;
        if !osdmap.pools.contains_key(&pool) {
            return Err(OSDClientError::PoolNotFound(pool));
        }

        // Mirrors execute_op pre-flight checks: hard-fail on EIO, pause, and epoch barrier.
        // PGLS is a read, so only pauserd applies (not pausewr/pool_full).
        if osdmap.is_pool_eio(pool) {
            return Err(OSDClientError::OSDError {
                code: -libc::EIO,
                message: format!("pool {pool} has EIO flag set"),
            });
        }
        let barrier = self.epoch_barrier.load(Ordering::Relaxed);
        if barrier != 0 && osdmap.epoch.as_u32() < barrier {
            return Err(OSDClientError::OSDError {
                code: -libc::EAGAIN,
                message: format!(
                    "pool {} epoch {} is behind barrier {}",
                    pool,
                    osdmap.epoch.as_u32(),
                    barrier
                ),
            });
        }
        if osdmap.is_pauserd() {
            return Err(OSDClientError::OSDError {
                code: -libc::EAGAIN,
                message: "cluster reads are paused".into(),
            });
        }

        let mut all_entries: Vec<ListObjectEntry> = Vec::new();

        loop {
            // Pool end reached (cursor returned from a previous call was handle.max)
            if hobject_cursor.max {
                return Ok(ListResult {
                    entries: all_entries,
                    cursor: None,
                });
            }

            // Each page in the map current as it goes out, as
            // list_nobjects reads the map anew for every page.
            let osdmap = self.get_osdmap().await?;
            let pool_info = osdmap
                .pools
                .get(&pool)
                .ok_or(OSDClientError::PoolNotFound(pool))?;
            let current_pg = Self::cursor_pg(pool_info, pool, hobject_cursor.hash).seed;
            debug!(
                "Querying PG {} (hash={:#x}), collected {} entries so far",
                current_pg,
                hobject_cursor.hash,
                all_entries.len()
            );

            let remaining = max_entries.saturating_sub(all_entries.len() as u64);
            let (response, _result_code) = match self
                .query_pg_objects(pool, nspace, &hobject_cursor, remaining, &osdmap)
                .await
            {
                Ok(result) => result,
                Err(OSDClientError::NoOSDs) => {
                    // No OSDs for this PG — advance to the next PG in sorted order
                    // by synthesising the end-of-PG handle that the OSD would have returned.
                    hobject_cursor = Self::pg_hobj_end(pool, current_pg, pool_info.pg_num);
                    continue;
                }
                Err(e) => return Err(e),
            };

            // Collect entries
            for entry in response.entries {
                all_entries.push(ListObjectEntry::new(
                    &entry.nspace,
                    &entry.oid,
                    &entry.locator,
                ));
            }

            // The OSD always returns the correct next cursor — use it directly.
            // This is the "SORTBITWISE" path used by modern Ceph (Quincy+):
            //   list_context->pos = response.handle
            let next_handle = response.handle;

            // Return early once we have enough entries
            if all_entries.len() >= max_entries as usize {
                return Ok(Self::build_list_result(all_entries, &next_handle));
            }

            // Advance cursor for next iteration
            hobject_cursor = next_handle;
        }
    }

    /// How many low hash bits PG `pg_seed` of a pool with `pg_num` PGs
    /// owns, as `pg_t::get_split_bits` (`src/osd/osd_types.cc:830-843`):
    /// with `pg_num` in `[2^(p-1), 2^p)`, a PG that has split owns `p`
    /// bits and one that has not owns `p - 1`.
    fn pg_split_bits(pg_seed: u32, pg_num: u32) -> u32 {
        if pg_num <= 1 {
            return 0;
        }
        let p = 32 - pg_num.leading_zeros();
        let half = 1u32 << (p - 1);
        if pg_seed % half < pg_num % half {
            p
        } else {
            p - 1
        }
    }

    /// Compute the end-of-PG hobject handle for a given PG, matching
    /// `pg_t::get_hobj_end` (`src/osd/osd_types.cc:879-894`).
    ///
    /// This is used to skip over a PG that has no OSDs so we can advance to the
    /// next PG in bitwise-sorted order without issuing an OSD request.
    fn pg_hobj_end(pool: u64, pg_seed: u32, pg_num: u32) -> crate::HObject {
        let bits = Self::pg_split_bits(pg_seed, pg_num);
        let rev_start = pg_seed.reverse_bits();
        let rev_end: u64 = (rev_start as u64) | (0xffff_ffffu64 >> bits);
        let rev_end = rev_end + 1;
        if rev_end >= 0x1_0000_0000 {
            // This PG extends to the very end of the hash space → pool end
            let mut h = crate::HObject::empty_cursor(pool);
            h.max = true;
            h
        } else {
            crate::HObject::new(pool, String::new(), (rev_end as u32).reverse_bits())
        }
    }

    /// List all pools in the cluster
    ///
    /// Returns a list of all pools with their IDs and names.
    pub async fn list_pools(&self) -> Result<Vec<crate::osdclient::types::PoolInfo>> {
        debug!("Listing pools");

        let osdmap = self.get_osdmap().await?;

        // Extract pool information from OSDMap
        let mut pools = Vec::with_capacity(osdmap.pools.len());
        for pool_id in osdmap.pools.keys() {
            if let Some(pool_name) = osdmap.pool_name.get(pool_id) {
                pools.push(crate::osdclient::types::PoolInfo {
                    pool_id: *pool_id,
                    pool_name: pool_name.clone(),
                });
            }
        }

        Ok(pools)
    }

    /// Handle pool operation result: request OSDMap update if needed
    ///
    /// After a successful pool create/delete, the OSDMap epoch advances.
    /// This ensures we receive the updated map.
    async fn handle_pool_op_result(&self, result: &crate::monclient::PoolOpResult) -> Result<()> {
        if !result.is_success() {
            return Err(OSDClientError::Other(format!(
                "Pool operation failed with code {}",
                result.reply_code
            )));
        }

        let target_epoch = result.epoch;
        let current_epoch = self.current_epoch_u32();

        if target_epoch > current_epoch {
            debug!(
                "Requesting OSDMap update from epoch {} to {}",
                current_epoch, target_epoch
            );
            self.mon_client
                .subscribe(
                    crate::monclient::MonService::OsdMap,
                    current_epoch as u64,
                    0,
                )
                .await
                .ok();
        }

        Ok(())
    }

    /// Create a new pool
    ///
    /// This is a simplified interface for pool creation using MPoolOp messages.
    /// For advanced pool creation with custom parameters (pg_num, pgp_num, pool_type, etc.),
    /// use MonClient's invoke() method with a mon_command instead.
    ///
    /// # Arguments
    /// * `pool_name` - Name of the pool to create
    /// * `crush_rule` - Optional CRUSH rule ID (uses cluster default if None)
    ///
    /// # Returns
    /// * `Ok(())` if the pool was created successfully
    /// * `Err(OSDClientError)` if the operation failed
    pub async fn create_pool(&self, pool_name: &str, crush_rule: Option<i16>) -> Result<()> {
        debug!("Creating pool: {}", pool_name);

        // Get current OSDMap epoch
        let version = self.current_epoch_u32() as u64;

        // Create MPoolOp message
        let msg = crate::monclient::MPoolOp::create_pool(
            self.fsid.bytes,
            pool_name.to_string(),
            crush_rule,
            version,
        );

        // Send via MonClient
        let result = self
            .mon_client
            .send_poolop(msg)
            .await
            .map_err(OSDClientError::MonClient)?;

        self.handle_pool_op_result(&result).await?;
        debug!("Pool created successfully: {}", pool_name);
        Ok(())
    }

    /// Delete a pool
    ///
    /// This operation uses the MPoolOp binary message protocol to delete a pool,
    /// matching the official librados implementation.
    ///
    /// # Arguments
    /// * `pool_name` - Name of the pool to delete
    /// * `confirm` - Must be set to true to confirm deletion (safety check)
    ///
    /// # Returns
    /// * `Ok(())` if the pool was deleted successfully
    /// * `Err(OSDClientError)` if the operation failed
    ///
    /// # Safety
    /// This operation is destructive and will delete all data in the pool.
    /// The `confirm` parameter must be explicitly set to `true`.
    pub async fn delete_pool(&self, pool_name: &str, confirm: bool) -> Result<()> {
        if !confirm {
            return Err(OSDClientError::Other(
                "Pool deletion requires explicit confirmation".into(),
            ));
        }

        debug!("Deleting pool: {}", pool_name);

        // Look up pool ID and epoch from OSDMap
        let osdmap = self.get_osdmap().await?;
        let pool_id = osdmap
            .pool_id_by_name(pool_name)
            .ok_or_else(|| OSDClientError::PoolNameNotFound(pool_name.to_owned()))?
            as u32;
        let version = osdmap.epoch.as_u32() as u64;

        debug!(
            "Deleting pool '{}' with ID {} (OSDMap epoch {})",
            pool_name, pool_id, version
        );

        // Create and send MPoolOp delete message
        let msg = crate::monclient::MPoolOp::delete_pool(self.fsid.bytes, pool_id, version);

        let result = self
            .mon_client
            .send_poolop(msg)
            .await
            .map_err(OSDClientError::MonClient)?;

        self.handle_pool_op_result(&result).await?;
        debug!("Pool deleted successfully: {}", pool_name);
        Ok(())
    }

    /// Scan pending ops after OSDMap update, resend if target changed,
    /// or every op when epochs were `skipped`.
    ///
    /// This implements Ceph's `_scan_requests` pattern from Objecter.
    async fn scan_requests_on_map_change(&self, new_epoch: u32, skipped: bool) -> Result<()> {
        let osdmap = self.get_osdmap().await?;

        // Collect session snapshot
        let session_snapshot = self.collect_session_snapshot().await;

        let mut need_resend = Vec::new();
        let mut sessions_to_close: Vec<i32> = Vec::with_capacity(session_snapshot.len());

        // Process each session
        for (osd_id, session) in session_snapshot {
            let should_close = self
                .check_session_health(osd_id, &session, &osdmap, new_epoch)
                .await;

            if should_close {
                sessions_to_close.push(osd_id);
                // Drain all ops; CRUSH errors are silently dropped (session is closing).
                let _ = self
                    .collect_resend_ops(&session, &osdmap, new_epoch, None, false, &mut need_resend)
                    .await;
                continue;
            }

            // Scan ops: only re-target those whose primary OSD changed or whose pool
            // bumped last_force_op_resend since this op was sent.
            let any_migrated = self
                .collect_resend_ops(
                    &session,
                    &osdmap,
                    new_epoch,
                    Some(osd_id),
                    skipped,
                    &mut need_resend,
                )
                .await?;

            if any_migrated {
                // collect_resend_ops already cancelled the io_loop; close the
                // session fully and drain any remaining ops (those whose primary
                // didn't change but whose connection is being torn down).
                sessions_to_close.push(osd_id);
                let _ = self
                    .collect_resend_ops(&session, &osdmap, new_epoch, None, false, &mut need_resend)
                    .await;
            }
        }

        // Close stale sessions
        self.close_stale_sessions(sessions_to_close).await;

        // Resend operations to new targets
        self.resend_migrated_operations(need_resend, new_epoch)
            .await?;

        Ok(())
    }

    /// Collect snapshot of all sessions
    async fn collect_session_snapshot(&self) -> Vec<(i32, Arc<OSDSession>)> {
        let sessions = self.sessions.read().await;
        sessions
            .iter()
            .map(|(id, sess)| (*id, Arc::clone(sess)))
            .collect()
    }

    /// Check if session should be closed due to OSD down or address change
    async fn check_session_health(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        osdmap: &Arc<crate::osdclient::osdmap::OSDMap>,
        new_epoch: u32,
    ) -> bool {
        if osdmap.is_down(osd_id) {
            info!(
                "OSD {} is DOWN in epoch {}, closing session and migrating pending ops",
                osd_id, new_epoch
            );
            return true;
        }

        if session.is_connected() {
            return self
                .session_address_stale(session, osd_id, osdmap, new_epoch)
                .await;
        }

        false
    }

    /// Collect pending ops from a session that need resubmission after an OSDMap change.
    ///
    /// `current_osd`:
    ///   - `Some(id)` — scan path: only collect ops whose primary OSD changed,
    ///     whose pool's `last_force_op_resend` epoch was bumped since the op was
    ///     sent, or whose PG split or merged since it was placed.
    ///   - `None`     — drain path: collect every op unconditionally (session is closing).
    ///
    /// `skipped`, on the scan path, collects every op: epochs were skipped,
    /// so a change of target may have gone unseen, and `_scan_requests`
    /// resends an op its `_calc_target` leaves alone when `skipped_map`
    /// is set (v19.2.2:src/osdc/Objecter.cc:1105-1111). An op collected
    /// only for the skip is not migrated: it goes back out through its
    /// session, which stays up.
    ///
    /// Returns `true` if at least one op was migrated (scan path only; always `false` for
    /// the drain path because all ops stay within the same "session is closing" group).
    ///
    /// CRUSH placement errors are propagated; drain callers should discard with `let _ =`.
    async fn collect_resend_ops(
        &self,
        session: &OSDSession,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        new_epoch: u32,
        current_osd: Option<i32>,
        skipped: bool,
        need_resend: &mut Vec<(i32, crate::osdclient::session::PendingOp)>,
    ) -> Result<bool> {
        let metadata = session.get_pending_ops_metadata();
        let mut placement_cache = HashMap::new();
        let mut any_migrated = false;

        for (tid, msg, op_osdmap_epoch, op_pg_num) in metadata {
            let pool_id = msg.object.pool;
            if Self::fail_if_pool_deleted(session, tid, pool_id, osdmap).await {
                continue;
            }
            let new_pg_num = osdmap.pools.get(&pool_id).map_or(0, |p| p.pg_num);

            let (new_hash, new_spg, new_osds) =
                Self::cached_rescan_osds(&mut placement_cache, osdmap, &msg)?;
            let new_primary = new_osds.first().copied().unwrap_or(-1);

            // Scan path: resend if the op migrated (primary changed, pool forced
            // a resend, PG split or merged), which tears the session down, or
            // if epochs were skipped, which does not.
            // Drain path (current_osd == None): always resend.
            let (needs_resend, migrated) = match current_osd {
                None => (true, false),
                Some(osd_id) => {
                    // Mirrors Objecter::_calc_target step 4: if last_force_op_resend is in
                    // (op_epoch, new_epoch] the op must be resubmitted even if its primary
                    // OSD has not changed.
                    let force_resend = osdmap.pools.get(&pool_id).is_some_and(|p| {
                        let lf = p.canonical_last_force_op_resend().as_u32();
                        lf > op_osdmap_epoch && lf <= new_epoch
                    });
                    // Objecter::_calc_target's split_or_merge
                    // (Objecter.cc:2925-2931, 3009-3013): the OSD drops an
                    // op sent before its PG split or merged, primary or not.
                    let split_or_merge =
                        Self::pg_split_or_merge(pool_id, new_hash, op_pg_num, new_pg_num);
                    let migrated = new_primary != osd_id || force_resend || split_or_merge;
                    (skipped || migrated, migrated)
                }
            };

            if needs_resend && let Some(mut op) = session.remove_pending_op(tid) {
                // A linger send is cancelled, not re-sent, as handle_osd_map
                // does (v19.2.2:src/osdc/Objecter.cc:1343-1350): the linger
                // rescan sends a fresh op for it, and a NOTIFY, a read, is
                // not deduplicated by its reqid.
                if !op.should_resend {
                    self.cancel_pending_op(session.osd_id, op).await;
                    continue;
                }
                // Scan path only: cancel the io_loop so any message already
                // encoded for this tid is dropped before reaching the wire.
                // See `cancel_io_loop`'s doc comment for the full rationale.
                if migrated {
                    session.cancel_io_loop();
                    any_migrated = true;
                }
                op.target.update(new_epoch, new_primary, new_osds.clone());
                op.target.pg_num = new_pg_num;
                // Restamp the MOSDOp pgid from the new map.  A pg_num change
                // (autoscaler split, manual resize) shifts the seed even when
                // the primary OSD is unchanged; without this, the migrated op
                // would be re-encoded with the stale seed and rejected by the
                // target OSD (ENXIO in prod, assertion with debug_misdirected).
                {
                    let msg = Arc::make_mut(&mut op.op);
                    msg.object.hash = new_hash;
                    msg.pgid = new_spg;
                    msg.osdmap_epoch = new_epoch;
                }
                op.state = crate::osdclient::types::OpState::Queued;
                need_resend.push((new_primary, op));
            }
        }

        Ok(any_migrated)
    }

    /// Close sessions that are stale
    async fn close_stale_sessions(&self, sessions_to_close: Vec<i32>) {
        if sessions_to_close.is_empty() {
            return;
        }

        // Remove all stale sessions in a single lock acquisition
        let removed: Vec<_> = {
            let mut sessions = self.sessions.write().await;
            sessions_to_close
                .iter()
                .filter_map(|osd_id| sessions.remove(osd_id).map(|s| (*osd_id, s)))
                .collect()
        };

        for (osd_id, session) in removed {
            info!("Removing stale session for OSD {}", osd_id);
            session.close().await;
        }
    }

    /// Resend migrated operations to their new target OSDs.
    async fn resend_migrated_operations(
        &self,
        need_resend: Vec<(i32, crate::osdclient::session::PendingOp)>,
        new_epoch: u32,
    ) -> Result<()> {
        for (new_osd, pending_op) in need_resend {
            self.resend_single_migrated_op(pending_op, new_osd, new_epoch)
                .await?;
        }
        Ok(())
    }

    /// Resend a single migrated op, re-routing if the OSDMap advanced during
    /// `get_or_create_session` (which can block for TCP connect + auth handshake).
    /// Mirrors the epoch re-check in `execute_op`.
    ///
    /// All failure modes (session creation failed, re-route placement failed,
    /// epoch churning too fast) are reported via `pending_op.result_tx` so the
    /// caller's `for` loop continues with the next op.  Only propagated errors
    /// (from `get_osdmap`) return `Err`.
    async fn resend_single_migrated_op(
        &self,
        mut pending_op: crate::osdclient::session::PendingOp,
        mut target_osd: i32,
        mut epoch_for_op: u32,
    ) -> Result<()> {
        // Cap re-route attempts: if the epoch is churning faster than we can
        // reconnect, give up and fail the op rather than loop forever.
        const MAX_REROUTE_ATTEMPTS: u32 = 8;
        let mut reroute_attempts = 0u32;

        loop {
            let session = match self.get_or_create_session(target_osd).await {
                Ok(s) => s,
                Err(e) => {
                    warn!("Cannot resend op to OSD {}: {}", target_osd, e);
                    let _ = pending_op
                        .result_tx
                        .send(Err(OSDClientError::Connection(format!(
                            "OSD {target_osd} unavailable: {e}"
                        ))));
                    return Ok(());
                }
            };

            if self.current_epoch_u32() == epoch_for_op {
                if let Err(e) = session.insert_migrated_op(pending_op, epoch_for_op).await {
                    warn!("Failed to migrate operation to OSD {}: {}", target_osd, e);
                }
                return Ok(());
            }

            // Epoch advanced during connect/auth — re-route before inserting.
            // Also refresh the op's pgid: a pg_num change bumps the seed even
            // when the primary OSD is unchanged, so keeping the old pgid would
            // send a stale seed to the fresh session.
            let new_osdmap = self.get_osdmap().await?;
            let placed = Self::replace_pending(&new_osdmap, &pending_op.op);
            let (new_hash, new_spg, osds) = match placed {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        "Cannot re-route op after epoch change: pool={} oid={}: {}",
                        pending_op.op.object.pool, pending_op.op.object.oid, e
                    );
                    let _ = pending_op
                        .result_tx
                        .send(Err(OSDClientError::Connection(format!(
                            "Routing failed after epoch change: {e}"
                        ))));
                    return Ok(());
                }
            };

            target_osd = osds[0];
            epoch_for_op = new_osdmap.epoch.as_u32();
            pending_op.target.pg_num = new_osdmap
                .pools
                .get(&pending_op.op.object.pool)
                .map_or(0, |p| p.pg_num);
            {
                let msg = Arc::make_mut(&mut pending_op.op);
                msg.object.hash = new_hash;
                msg.pgid = new_spg;
                msg.osdmap_epoch = epoch_for_op;
            }
            reroute_attempts += 1;
            if reroute_attempts > MAX_REROUTE_ATTEMPTS {
                warn!(
                    "Epoch churning too fast, giving up re-route for pool={} oid={}",
                    pending_op.op.object.pool, pending_op.op.object.oid,
                );
                let _ = pending_op
                    .result_tx
                    .send(Err(OSDClientError::Timeout(std::time::Duration::ZERO)));
                return Ok(());
            }
        }
    }

    /// Check if a session's address no longer matches the OSDMap.
    ///
    /// Returns `true` when the session's peer address does not appear in the
    /// OSDMap's client address vector for the given OSD, indicating the OSD
    /// restarted on a different address.
    async fn session_address_stale(
        &self,
        session: &OSDSession,
        osd_id: i32,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        new_epoch: u32,
    ) -> bool {
        let Some(session_addr) = session.get_peer_address() else {
            return false;
        };
        let Some(map_addrvec) = osdmap.get_osd_addr(osd_id) else {
            return false;
        };
        let session_sockaddr = session_addr.to_socket_addr();
        let map_has_match = map_addrvec.addrs.iter().any(|a| {
            matches!(a.addr_type, crate::EntityAddrType::Msgr2)
                && a.to_socket_addr() == session_sockaddr
        });
        if !map_has_match {
            info!(
                "OSD {} address changed in epoch {} (session: {:?}, map: {:?}), closing session",
                osd_id, new_epoch, session_sockaddr, map_addrvec
            );
        }
        !map_has_match
    }

    /// Fail a pending op whose pool no longer exists, returning true if handled.
    async fn fail_if_pool_deleted(
        session: &OSDSession,
        tid: u64,
        pool_id: u64,
        osdmap: &crate::osdclient::osdmap::OSDMap,
    ) -> bool {
        if osdmap.pools.contains_key(&pool_id) {
            return false;
        }
        if let Some(pending_op) = session.remove_pending_op(tid) {
            let _ = pending_op
                .result_tx
                .send(Err(OSDClientError::PoolNotFound(pool_id)));
        }
        true
    }

    /// Resend pending ops from a disconnected session into a freshly created one.
    ///
    /// Called after the new session for an OSD has been connected, matching the
    /// C++ pattern in `Objecter::_kick_requests()` which runs after
    /// `_reopen_session()`: the session is opened first, then ops are resent via
    /// the new connection.
    ///
    /// CRUSH remapping (ops moving to a *different* OSD) is handled separately by
    /// `scan_requests_on_map_change()` when OSDMap updates arrive.  This function
    /// only resubmits to the same OSD's new session, so there is no mutual
    /// recursion with `get_or_create_session` and no boxing is required.
    async fn kick_into_session(&self, old_session: &OSDSession, new_session: &Arc<OSDSession>) {
        if self.shutdown_token.is_cancelled() {
            return;
        }

        let osdmap = match self.get_osdmap().await {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "kick_into_session: OSD {} has pending ops but no OSDMap: {}",
                    new_session.osd_id, e
                );
                return;
            }
        };

        let metadata = old_session.get_pending_ops_metadata();
        if metadata.is_empty() {
            return;
        }

        info!(
            "kick_into_session: resending {} ops to OSD {}",
            metadata.len(),
            new_session.osd_id
        );

        let new_epoch = osdmap.epoch.as_u32();

        for (tid, msg, ..) in metadata {
            let pool_id = msg.object.pool;
            // One not yet submitted is left to its submitter, which may
            // take it back; it sends on the old session or fails there.
            let Some(mut pending_op) = old_session.remove_submitted_op(tid) else {
                continue;
            };

            // Pool deleted — fail immediately.
            if !osdmap.pools.contains_key(&pool_id) {
                let _ = pending_op
                    .result_tx
                    .send(Err(OSDClientError::PoolNotFound(pool_id)));
                continue;
            }

            // As _kick_requests does (v19.2.2:src/osdc/Objecter.cc:
            // 2087-2092): the reset hook re-sends the linger itself.
            if !pending_op.should_resend {
                self.cancel_pending_op(old_session.osd_id, pending_op).await;
                continue;
            }

            pending_op.state = crate::osdclient::types::OpState::Queued;

            if let Err(e) = new_session.insert_migrated_op(pending_op, new_epoch).await {
                warn!(
                    "kick_into_session: failed to resubmit tid {} to OSD {}: {}",
                    tid, new_session.osd_id, e
                );
            }
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) {
        info!("Shutting down OSDClient");

        // Shutdown tracker first to stop timeout callbacks
        self.tracker.shutdown().await;

        // Cancel all I/O tasks (child tokens are cancelled automatically)
        self.shutdown_token.cancel();

        // Nothing re-sends a linger from here on, so end them all now
        // rather than leave a watcher's `recv` or a `notify` waiting.
        self.drain_lingers();

        // Await all I/O tasks to ensure they have stopped
        // Clone sessions before releasing lock to avoid holding lock across await
        let sessions_to_close = {
            let sessions = self.sessions.read().await;
            sessions.values().map(Arc::clone).collect::<Vec<_>>()
        };

        for session in sessions_to_close {
            session.close().await;
        }

        info!("OSDClient shutdown complete");
    }

    /// Decode and validate MOSDMap message
    ///
    /// Returns the decoded MOSDMap and current epoch if validation passes
    fn decode_and_validate_osdmap(
        &self,
        msg: &crate::msgr2::message::Message,
    ) -> Result<(crate::monclient::messages::MOSDMap, crate::Epoch)> {
        use crate::monclient::messages::MOSDMap;

        info!("Handling OSDMap message ({} bytes)", msg.front.len());

        let mosdmap = MOSDMap::from_message(msg)?;
        info!(
            "Received MOSDMap: epochs [{}..{}], {} full maps, {} incremental maps",
            mosdmap.get_first(),
            mosdmap.get_last(),
            mosdmap.maps.len(),
            mosdmap.incremental_maps.len()
        );

        // Validate FSID
        if mosdmap.fsid != self.fsid.bytes {
            warn!(
                "Ignoring OSDMap with wrong fsid (expected {:?}, got {:?})",
                self.fsid.bytes, mosdmap.fsid
            );
            return Err(OSDClientError::Internal("FSID mismatch".into()));
        }

        // Check if we've already processed these epochs
        let current_epoch = self
            .osdmap_rx
            .borrow()
            .as_ref()
            .map(|m| m.epoch)
            .unwrap_or_default();

        if mosdmap.get_last() <= current_epoch.as_u32() {
            info!(
                "Ignoring OSDMap epochs [{}..{}] <= current epoch {}",
                mosdmap.get_first(),
                mosdmap.get_last(),
                current_epoch
            );
            return Err(OSDClientError::Internal("Stale epoch".into()));
        }

        debug!(
            "Processing OSDMap epochs [{}..{}] > current epoch {}",
            mosdmap.get_first(),
            mosdmap.get_last(),
            current_epoch
        );

        Ok((mosdmap, current_epoch))
    }

    /// Process OSDMap updates (incremental and full maps)
    ///
    /// Returns the updated map if any updates were successfully applied
    fn process_osdmap_updates(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        current_epoch: crate::Epoch,
    ) -> Option<MapBatch> {
        let current_map = self.osdmap_rx.borrow().clone();

        if current_epoch.as_u32() > 0 {
            // We have a current map, apply updates sequentially
            Some(self.apply_sequential_updates(mosdmap, current_epoch, current_map))
        } else {
            // No current map, use latest full map
            self.load_initial_map(mosdmap)
        }
    }

    /// Apply sequential updates to existing map, keeping every applied
    /// epoch's map so the lingers can be checked against each; see
    /// [`walk_map_epochs`].
    fn apply_sequential_updates(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        current_epoch: crate::Epoch,
        current_map: Option<Arc<crate::osdclient::osdmap::OSDMap>>,
    ) -> MapBatch {
        walk_map_epochs(
            mosdmap,
            current_epoch.as_u32(),
            current_map,
            |e, working| self.apply_incremental_map(mosdmap, e, working),
            |e| self.apply_full_map(mosdmap, e),
        )
    }

    /// Apply incremental map update
    fn apply_incremental_map(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        epoch: u32,
        working_map: &Option<Arc<crate::osdclient::osdmap::OSDMap>>,
    ) -> Option<Arc<crate::osdclient::osdmap::OSDMap>> {
        debug!("Applying incremental OSDMap for epoch {}", epoch);
        let inc_bl = mosdmap.incremental_maps.get(&epoch)?;

        match crate::osdclient::osdmap::OSDMapIncremental::decode_versioned(&mut inc_bl.as_ref(), 0)
        {
            Ok(inc_map) => {
                debug!(
                    "Decoded incremental: epoch={}, {} new pools, {} old pools",
                    inc_map.epoch,
                    inc_map.new_pools.len(),
                    inc_map.old_pools.len()
                );

                if let Some(current_map) = working_map {
                    let mut updated_map = (**current_map).clone();
                    if let Err(err) = inc_map.apply_to(&mut updated_map) {
                        warn!("Failed to apply incremental epoch {}: {}", epoch, err);
                        None
                    } else {
                        Some(Arc::new(updated_map))
                    }
                } else {
                    None
                }
            }
            Err(err) => {
                warn!("Failed to decode incremental epoch {}: {}", epoch, err);
                None
            }
        }
    }

    /// Apply full map update
    fn apply_full_map(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        epoch: u32,
    ) -> Option<Arc<crate::osdclient::osdmap::OSDMap>> {
        debug!("Using full OSDMap for epoch {}", epoch);
        let full_bl = mosdmap.maps.get(&epoch)?;

        match crate::osdclient::osdmap::OSDMap::decode_versioned(&mut full_bl.as_ref(), 0) {
            Ok(full_map) => {
                debug!("Decoded full OSDMap: epoch={}", full_map.epoch);
                Some(Arc::new(full_map))
            }
            Err(err) => {
                warn!("Failed to decode full map epoch {}: {}", epoch, err);
                None
            }
        }
    }

    /// Load initial map when no current map exists.
    ///
    /// Picks the latest full map in the message, then applies any incrementals
    /// from the same message that are newer than that full map.  The monitor
    /// bundles a base full map plus trailing incrementals in a single MOSDMap
    /// message; without this second step, pg_temp entries (and other incremental
    /// changes) set during PG peering are silently discarded.
    fn load_initial_map(&self, mosdmap: &crate::monclient::messages::MOSDMap) -> Option<MapBatch> {
        let (&base_epoch, full_bl) = mosdmap.maps.iter().max_by_key(|(e, _)| **e)?;

        debug!("Using latest full OSDMap (epoch {})", base_epoch);
        let full_map =
            match crate::osdclient::osdmap::OSDMap::decode_versioned(&mut full_bl.as_ref(), 0) {
                Ok(m) => {
                    info!("Initial OSDMap loaded: epoch={}", m.epoch);
                    Arc::new(m)
                }
                Err(err) => {
                    warn!("Failed to decode initial full map: {}", err);
                    return None;
                }
            };

        // Apply any incrementals that are newer than the full map and are
        // included in the same MOSDMap message (e.g. pg_temp changes from PG
        // peering that happened after the base-epoch snapshot was taken).
        let full_epoch = crate::Epoch::new(base_epoch);
        if mosdmap.get_last() > base_epoch {
            debug!(
                "Applying {} incremental(s) on top of initial full map (epoch {}..{})",
                mosdmap.get_last() - base_epoch,
                base_epoch,
                mosdmap.get_last(),
            );
            let mut batch =
                self.apply_sequential_updates(mosdmap, full_epoch, Some(full_map.clone()));
            batch.maps.insert(0, full_map);
            Some(batch)
        } else {
            Some(MapBatch {
                maps: vec![full_map],
                ..MapBatch::default()
            })
        }
    }

    /// Update OSDMap state and notify subscribers
    async fn update_osdmap_state(self: &Arc<Self>, batch: MapBatch) -> Result<()> {
        let Some(new_map) = batch.maps.last().cloned() else {
            return Ok(());
        };
        let final_epoch = new_map.epoch;

        // Check blocklist before publishing the new map so that
        // `scan_requests_on_map_change` below can skip normal resend logic when
        // we are fenced (all ops will be failed unconditionally).
        if !self.blocklisted.load(Ordering::Relaxed)
            && let Some(client_addr) = self.mon_client.get_client_addr().await
            && new_map.is_blocklisted(&client_addr)
        {
            error!(
                "OSDClient is blocklisted at epoch {} (addr={}), failing all pending ops",
                final_epoch, client_addr
            );
            self.blocklisted.store(true, Ordering::Relaxed);
            self.fail_all_pending_ops_blocklisted().await;
        }

        self.osdmap_tx.send(Some(Arc::clone(&new_map))).ok();

        if !self.blocklisted.load(Ordering::Relaxed) {
            self.scan_lingers_on_map_change(&batch.maps, batch.skipped);
        }

        info!(
            "OSDMap updated to epoch {}, rescanning pending operations",
            final_epoch
        );

        // Notify MonClient that we received this osdmap epoch
        if let Err(e) = self
            .mon_client
            .notify_map_received(
                crate::monclient::MonService::OsdMap,
                u64::from(u32::from(final_epoch)),
            )
            .await
        {
            warn!(
                "Failed to notify MonClient of osdmap epoch {}: {}",
                final_epoch, e
            );
        }

        // Skip the normal rescan if we just got blocklisted — all ops were
        // already failed above.
        if !self.blocklisted.load(Ordering::Relaxed)
            && let Err(e) = self
                .scan_requests_on_map_change(final_epoch.as_u32(), batch.skipped)
                .await
        {
            warn!("Failed to rescan requests after OSDMap update: {}", e);
        }

        Ok(())
    }

    /// Fail all pending ops across every session with `Blocklisted`.
    ///
    /// Called once when the client detects its address in the OSDMap blocklist
    /// so callers receive an immediate error rather than a timeout.
    async fn fail_all_pending_ops_blocklisted(&self) {
        let session_snapshot = self.collect_session_snapshot().await;
        for (_osd_id, session) in session_snapshot {
            for (tid, ..) in session.get_pending_ops_metadata() {
                if let Some(op) = session.remove_pending_op(tid) {
                    let _ = op.result_tx.send(Err(OSDClientError::Blocklisted));
                }
            }
        }
    }

    /// Handle OSDMap message (moved from MonClient)
    async fn handle_osdmap(self: &Arc<Self>, msg: crate::msgr2::message::Message) -> Result<()> {
        // Decode and validate
        let (mosdmap, current_epoch) = match self.decode_and_validate_osdmap(&msg) {
            Ok(result) => result,
            Err(_) => return Ok(()), // Already logged, skip processing
        };

        // Process map updates
        let new_maps = self.process_osdmap_updates(&mosdmap, current_epoch);

        // Update state if we got a new map
        if let Some(batch) = new_maps {
            // Before the maps are marked got, as Objecter requests them
            // in the loop and calls sub_got after it
            // (v19.2.2:src/osdc/Objecter.cc:1228-1233, 1388).
            if let Some(epoch) = batch.request_from {
                self.request_maps_from(epoch).await;
            }
            self.update_osdmap_state(batch).await?;
        }

        Ok(())
    }

    /// Ask the mon for the OSDMaps from `epoch` on: Objecter's
    /// `_maybe_request_map` (v19.2.2:src/osdc/Objecter.cc:2024-2042),
    /// `sub_want` then `renew_subs`, which [`MonClient::subscribe`] both
    /// does. Objecter asks once (`CEPH_SUBSCRIBE_ONETIME`) unless the map
    /// is full or paused, since it otherwise learns of new maps from the
    /// OSDs; this client keeps one continuous `osdmap` subscription
    /// (`wait_for_latest_osdmap`), and a one-time want would replace it,
    /// on the mon too (v19.2.2:src/mon/Session.h:260-275), and end the
    /// map stream. So the continuous subscription restarts at `epoch`.
    ///
    /// [`MonClient::subscribe`]: crate::monclient::MonClient::subscribe
    async fn request_maps_from(&self, epoch: u32) {
        if let Err(e) = self
            .mon_client
            .subscribe(crate::monclient::MonService::OsdMap, u64::from(epoch), 0)
            .await
        {
            warn!("Failed to request OSDMaps from epoch {}: {}", epoch, e);
        }
    }

    /// Handle OSD operation reply from a specific OSD
    ///
    /// Called with explicit OSD context (Linux kernel pattern)
    async fn handle_osd_op_reply_from_osd(
        &self,
        osd_id: i32,
        msg: crate::msgr2::message::Message,
    ) -> Result<()> {
        use crate::osdclient::messages::MOSDOpReply;

        let tid = msg.tid();

        // Decode the reply. Pass msg.data as Bytes so op outdata can be
        // extracted via Bytes::slice() (zero-copy) instead of memcpy.
        let reply = MOSDOpReply::decode_reply(&msg.front, msg.data)?;

        debug!("OSD {} sent OSDOpReply for tid={}", osd_id, tid);

        let session = self.get_session_for_osd(osd_id).await?;

        // Check if retry is needed (returns Some if EAGAIN on replica read)
        if let Some((pending_op, new_flags)) = session.handle_osd_op_reply(tid, reply).await {
            debug!(
                "OSD {} EAGAIN on replica read tid {}, retrying to primary (flags: 0x{:x} -> 0x{:x})",
                osd_id, tid, pending_op.op.flags, new_flags
            );

            // Resubmit with new flags
            if let Err(e) = session
                .resubmit_with_new_flags(tid, pending_op, new_flags)
                .await
            {
                error!("Failed to resubmit operation tid {}: {}", tid, e);
            }
        }

        Ok(())
    }

    /// Handle OSD backoff from a specific OSD
    ///
    /// Called with explicit OSD context (Linux kernel pattern)
    /// The ACK must be sent back to the same OSD that sent the backoff
    async fn handle_backoff_from_osd(
        &self,
        osd_id: i32,
        msg: crate::msgr2::message::Message,
    ) -> Result<()> {
        use crate::osdclient::messages::{CEPH_OSD_BACKOFF_OP_BLOCK, CEPH_OSD_BACKOFF_OP_UNBLOCK};

        // Decode the backoff message
        let backoff = Self::decode_backoff_message(&msg)?;

        debug!(
            "OSD {} sent backoff op={} for pg={}:{}.{}",
            osd_id, backoff.op, backoff.pgid.pool, backoff.pgid.seed, backoff.pgid.shard
        );

        // Get the session for this OSD
        let session = self.get_session_for_osd(osd_id).await?;

        match backoff.op {
            CEPH_OSD_BACKOFF_OP_BLOCK => self.handle_backoff_block(osd_id, &session, backoff).await,
            CEPH_OSD_BACKOFF_OP_UNBLOCK => {
                self.handle_backoff_unblock(osd_id, &session, backoff).await
            }
            _ => {
                warn!(
                    "Received unknown backoff operation {} from OSD {}",
                    backoff.op, osd_id
                );
                Ok(())
            }
        }
    }

    /// Decode backoff message from raw message
    fn decode_backoff_message(
        msg: &crate::msgr2::message::Message,
    ) -> Result<crate::osdclient::messages::MOSDBackoff> {
        use crate::msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};
        use crate::osdclient::messages::MOSDBackoff;

        let header = CephMsgHeader::new(MOSDBackoff::msg_type(), MOSDBackoff::msg_version(0));
        MOSDBackoff::decode_payload(&header, &msg.front, &[], &[]).map_err(Into::into)
    }

    /// Get session for OSD, returning error if not found
    async fn get_session_for_osd(&self, osd_id: i32) -> Result<Arc<OSDSession>> {
        let sessions = self.sessions.read().await;
        sessions
            .get(&osd_id)
            .cloned()
            .ok_or_else(|| OSDClientError::Connection(format!("No session found for OSD {osd_id}")))
    }

    /// Handle BLOCK backoff operation
    async fn handle_backoff_block(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        backoff: crate::osdclient::messages::MOSDBackoff,
    ) -> Result<()> {
        info!(
            "OSD {} requests backoff: pg={}:{}.{}, id={}, range=[{:?}, {:?})",
            osd_id,
            backoff.pgid.pool,
            backoff.pgid.seed,
            backoff.pgid.shard,
            backoff.id,
            backoff.begin,
            backoff.end
        );

        // Register backoff in session
        self.register_backoff(session, &backoff).await;

        // Send ACK_BLOCK reply
        self.send_backoff_ack(osd_id, session, backoff).await
    }

    /// Register backoff entry in session tracker
    async fn register_backoff(
        &self,
        session: &Arc<OSDSession>,
        backoff: &crate::osdclient::messages::MOSDBackoff,
    ) {
        let tracker = session.backoff_tracker();
        let mut tracker = tracker.write().await;
        let entry = BackoffEntry {
            pgid: backoff.pgid,
            id: backoff.id,
            begin: backoff.begin.clone(),
            end: backoff.end.clone(),
        };
        tracker.register(entry);
        session.set_has_backoffs(true);
    }

    /// Send ACK_BLOCK message to OSD
    async fn send_backoff_ack(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        backoff: crate::osdclient::messages::MOSDBackoff,
    ) -> Result<()> {
        use crate::msgr2::ceph_message::CephMessagePayload;
        use crate::osdclient::messages::{CEPH_OSD_BACKOFF_OP_ACK_BLOCK, MOSDBackoff};

        let ack = MOSDBackoff::new(
            backoff.pgid,
            backoff.map_epoch,
            CEPH_OSD_BACKOFF_OP_ACK_BLOCK,
            backoff.id,
            backoff.begin,
            backoff.end,
        );

        let backoff_id = ack.id;
        let payload = ack
            .encode_payload(0)
            .map_err(|e| OSDClientError::Encoding(format!("Failed to encode ACK_BLOCK: {e}")))?;

        let msg = crate::msgr2::message::Message::new(
            crate::osdclient::messages::CEPH_MSG_OSD_BACKOFF,
            payload,
        )
        .with_version(MOSDBackoff::VERSION);

        let send_tx = session.send_tx();
        if let Err(e) = send_tx.send(msg).await {
            error!("Failed to send ACK_BLOCK to OSD {}: {}", osd_id, e);
        } else {
            debug!(
                "Sent ACK_BLOCK for backoff id={} to OSD {}",
                backoff_id, osd_id
            );
        }
        Ok(())
    }

    /// Handle UNBLOCK backoff operation
    async fn handle_backoff_unblock(
        &self,
        osd_id: i32,
        session: &Arc<OSDSession>,
        backoff: crate::osdclient::messages::MOSDBackoff,
    ) -> Result<()> {
        info!(
            "OSD {} lifts backoff: pg={}:{}.{}, id={}, range=[{:?}, {:?})",
            osd_id,
            backoff.pgid.pool,
            backoff.pgid.seed,
            backoff.pgid.shard,
            backoff.id,
            backoff.begin,
            backoff.end
        );

        // Remove backoff from session
        {
            let tracker = session.backoff_tracker();
            let mut tracker = tracker.write().await;
            tracker.remove_by_id(backoff.id, &backoff.begin, &backoff.end);
            session.set_has_backoffs(!tracker.is_empty());
        }

        // Resend operations that were in the backoff range
        session
            .resend_ops_in_range(&backoff.pgid, &backoff.begin, &backoff.end)
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{MOSDOp, OSDClient, OSDClientConfig, ObjectLocator};
    use crate::osdclient::error::OSDClientError;
    use crate::osdclient::types::ObjectId;
    use crate::osdclient::types::{OpReply, OpResult};
    use bytes::Bytes;
    use std::sync::Arc;

    fn result(overall: i32, first: i32) -> OpResult {
        OpResult {
            result: overall,
            version: 0,
            ops: vec![OpReply {
                return_code: first,
                outdata: Bytes::new(),
            }],
            redirect: None,
        }
    }

    #[test]
    fn check_op_result_treats_positive_codes_as_success() {
        // Ceph is errno-shaped: only a negative code is a failure. A
        // holding CMPXATTR returns 1.
        assert!(OSDClient::check_op_result(&result(0, 0), "t").is_ok());
        assert!(OSDClient::check_op_result(&result(1, 1), "t").is_ok());
        assert!(OSDClient::check_op_result(&result(0, 1), "t").is_ok());
    }

    #[test]
    fn check_op_result_reports_negative_codes() {
        let err = OSDClient::check_op_result(&result(-2, 0), "t").expect_err("overall");
        assert!(matches!(err, OSDClientError::OSDError { code: -2, .. }));
        let err = OSDClient::check_op_result(&result(0, -125), "t").expect_err("per-op");
        assert!(matches!(err, OSDClientError::OSDError { code: -125, .. }));
    }

    #[test]
    fn effective_release_prefers_the_override() {
        use super::effective_release;
        use crate::osdclient::osdmap::{CephRelease, OSDMap};
        let mut map = OSDMap::new();
        map.require_osd_release = 19;
        assert_eq!(effective_release(None, None), None);
        assert_eq!(
            effective_release(None, Some(&map)),
            Some(CephRelease::SQUID)
        );
        assert_eq!(
            effective_release(Some(CephRelease::TENTACLE), Some(&map)),
            Some(CephRelease::TENTACLE)
        );
        assert_eq!(
            effective_release(Some(CephRelease::UMBRELLA), None),
            Some(CephRelease::UMBRELLA)
        );
    }

    fn map_with_pool(id: u64, pg_num: u32) -> crate::osdclient::osdmap::OSDMap {
        let mut map = crate::osdclient::osdmap::OSDMap::new();
        map.pools.insert(
            id,
            crate::osdclient::PgPool {
                object_hash: crate::crush::hash::CEPH_STR_HASH_RJENKINS,
                pg_num,
                pgp_num: pg_num,
                size: 1,
                ..Default::default()
            },
        );
        map
    }

    /// `map_with_pool` plus a one-host CRUSH map over two OSDs, so its
    /// PGs map to OSDs.
    fn map_with_crush(id: u64, pg_num: u32) -> crate::osdclient::osdmap::OSDMap {
        use crate::crush::{
            BucketAlgorithm, BucketData, CrushBucket, CrushMap, CrushRule, CrushRuleStep, RuleOp,
            RuleType,
        };
        let mut crush = CrushMap::new();
        crush.max_devices = 2;
        crush.max_buckets = 1;
        crush.buckets = vec![Some(CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: 0x20000,
            size: 2,
            items: vec![0, 1],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000; 2],
            },
        })];
        crush.rules = vec![Some(CrushRule {
            rule_id: 0,
            rule_type: RuleType::Replicated,
            steps: vec![
                CrushRuleStep {
                    op: RuleOp::Take,
                    arg1: -1,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::ChooseLeafFirstN,
                    arg1: 1,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::Emit,
                    arg1: 0,
                    arg2: 0,
                },
            ],
        })];
        let mut map = map_with_pool(id, pg_num);
        map.max_osd = 2;
        map.osd_weight = vec![0x10000; 2];
        map.crush = Some(crush);
        map
    }

    fn keyed(pool: u64, oid: &str, key: &str, ns: &str) -> ObjectId {
        let mut object = ObjectId::with_namespace(pool, oid, ns);
        object.key = key.to_string();
        object
    }

    fn mosdop(object: ObjectId, ops: Vec<crate::osdclient::types::OSDOp>) -> MOSDOp {
        let flags = MOSDOp::calculate_flags(&ops);
        MOSDOp::new(
            1,
            1,
            flags,
            object,
            crate::osdclient::types::StripedPgId::from_pg(0, 0),
            ops,
            crate::osdclient::types::RequestId::new(&"client.test".into(), 1, 0),
            0,
        )
    }

    #[test]
    fn object_pg_in_map_uses_namespace_and_key() {
        use crate::crush::PgId;
        let map = map_with_pool(2, 32);
        let place = |object: &ObjectId| {
            let p = OSDClient::object_pg_in_map(&map, object).unwrap();
            (p.hash, p.pg)
        };
        assert_eq!(
            place(&ObjectId::with_namespace(2, "foo", "ns1")),
            (0xf4569544, PgId::new(2, 4))
        );
        assert_eq!(
            place(&ObjectId::new(2, "foo")),
            (0x7fc1f406, PgId::new(2, 6))
        );
        assert_eq!(
            place(&keyed(2, "_multipart_obj.2~abc.1", "obj", "")),
            (0xaabc5e21, PgId::new(2, 1))
        );
        assert!(matches!(
            OSDClient::object_pg_in_map(&map, &ObjectId::new(3, "foo")),
            Err(OSDClientError::PoolNotFound(3))
        ));
    }

    #[test]
    fn object_to_osds_in_map_places_by_the_locator() {
        let map = map_with_crush(2, 32);
        let route = |object: &ObjectId| {
            let (hash, spg, osds) = OSDClient::object_to_osds_in_map(&map, object).unwrap();
            (hash, spg.seed, osds)
        };
        // Each pair lands on a different OSD, so a client placing by the
        // name alone would send the op to the wrong one.
        let namespaced = route(&ObjectId::with_namespace(2, "bar", "ns1"));
        let name_only = route(&ObjectId::new(2, "bar"));
        assert_eq!(namespaced, (0x73a75142, 2, vec![1]));
        assert_eq!(name_only, (0xefe6384b, 11, vec![0]));

        let with_key = route(&keyed(2, "_multipart_foo.1", "foo", ""));
        let name_only = route(&ObjectId::new(2, "_multipart_foo.1"));
        assert_eq!(with_key, (0x7fc1f406, 6, vec![1]));
        assert_eq!(name_only, (0xfa7582cc, 12, vec![0]));
        assert_eq!(with_key, route(&ObjectId::new(2, "foo")));
    }

    #[test]
    fn placement_key_separates_namespaces() {
        assert_ne!(
            OSDClient::placement_key(&ObjectId::new(2, "foo")),
            OSDClient::placement_key(&ObjectId::with_namespace(2, "foo", "ns1"))
        );
        assert_eq!(
            OSDClient::placement_key(&keyed(2, "a", "obj", "ns1")),
            OSDClient::placement_key(&keyed(2, "b", "obj", "ns1"))
        );
    }

    /// The end hash of every PG of a `pg_num`-PG pool, or `None` for
    /// the pool's end.
    fn pg_end_hashes(pg_num: u32) -> Vec<Option<u32>> {
        (0..pg_num)
            .map(|seed| {
                let end = OSDClient::pg_hobj_end(2, seed, pg_num);
                (!end.max).then_some(end.hash)
            })
            .collect()
    }

    #[test]
    fn pg_split_bits_for_twelve_pgs() {
        let bits: Vec<u32> = (0..12).map(|s| OSDClient::pg_split_bits(s, 12)).collect();
        assert_eq!(bits, [4, 4, 4, 4, 3, 3, 3, 3, 4, 4, 4, 4]);
        assert_eq!(OSDClient::pg_split_bits(0, 1), 0);
        assert!((0..8).all(|s| OSDClient::pg_split_bits(s, 8) == 3));
    }

    #[test]
    fn pg_hobj_end_twelve_pgs() {
        // PGs 4-7 have not split, so each ends where its 3-bit range does;
        // PG 7's range runs to the end of the hash space.
        assert_eq!(
            pg_end_hashes(12),
            [
                Some(0x8),
                Some(0x9),
                Some(0xa),
                Some(0xb),
                Some(0x2),
                Some(0x3),
                Some(0x1),
                None,
                Some(0x4),
                Some(0x5),
                Some(0x6),
                Some(0x7),
            ]
        );
    }

    #[test]
    fn pg_hobj_end_eight_pgs() {
        assert_eq!(
            pg_end_hashes(8),
            [
                Some(0x4),
                Some(0x5),
                Some(0x6),
                Some(0x7),
                Some(0x2),
                Some(0x3),
                Some(0x1),
                None,
            ]
        );
    }

    #[test]
    fn pg_hobj_end_walks_every_pg_once() {
        let map = map_with_pool(2, 12);
        let pool = &map.pools[&2];
        let mut cursor = crate::HObject::new(2, String::new(), 0);
        let mut visited = Vec::new();
        while !cursor.max {
            let pg = OSDClient::cursor_pg(pool, 2, cursor.hash).seed;
            assert!(visited.len() < 12, "revisited PGs: {visited:?} then {pg}");
            visited.push(pg);
            cursor = OSDClient::pg_hobj_end(2, pg, 12);
        }
        assert_eq!(visited, [0, 8, 4, 2, 10, 6, 1, 9, 5, 3, 11, 7]);
    }

    #[test]
    fn pgls_reduction_is_stable_mod() {
        let map = map_with_pool(2, 12);
        let pool = &map.pools[&2];
        assert_eq!(OSDClient::cursor_pg(pool, 2, 0xef61efce).seed, 6);
    }

    /// `map_with_crush` at `epoch` with its host holding OSD 0 alone, so
    /// every PG's primary is OSD 0 whatever pg_num is.
    fn one_osd_map(pg_num: u32, epoch: u32) -> Arc<crate::osdclient::osdmap::OSDMap> {
        let mut map = map_with_crush(2, pg_num);
        let crush = map.crush.as_mut().expect("crush");
        crush.max_devices = 1;
        let host = crush.buckets[0].as_mut().expect("host");
        host.size = 1;
        host.weight = 0x10000;
        host.items = vec![0];
        host.data = crate::crush::BucketData::Straw2 {
            item_weights: vec![0x10000],
        };
        map.max_osd = 1;
        map.osd_weight = vec![0x10000];
        map.epoch = crate::Epoch::new(epoch);
        Arc::new(map)
    }

    /// An object of pool 2 whose PG with `pg_num` PGs is `seed`.
    fn object_in_pg(pg_num: u32, seed: u32) -> ObjectId {
        let map = map_with_pool(2, pg_num);
        (0..)
            .map(|i| ObjectId::new(2, &format!("obj{i}")))
            .find(|o| OSDClient::object_pg_in_map(&map, o).unwrap().pg.seed == seed)
            .expect("some object lands in every PG")
    }

    /// A session to OSD 0 holding a write to `object`, placed and sent at
    /// epoch 10 when its pool had `pg_num` PGs, with tid 77, re-sent on a
    /// map change or reset if `should_resend`.
    fn session_with_op(
        object: &ObjectId,
        pg_num: u32,
        should_resend: bool,
    ) -> (
        crate::osdclient::session::OSDSession,
        tokio::sync::oneshot::Receiver<super::Result<crate::osdclient::types::OpResult>>,
    ) {
        let (tx, _rx) = crate::msgr2::map_channel(1);
        let session = crate::osdclient::session::OSDSession::new(
            0,
            None,
            0,
            tx,
            std::sync::Weak::new(),
            Arc::new(std::sync::atomic::AtomicU64::new(1)),
        );
        let map = one_osd_map(pg_num, 10);
        let (hash, spg, osds) = OSDClient::object_to_osds_in_map(&map, object).unwrap();
        assert_eq!(osds[0], 0);
        let mut op = mosdop(
            object.clone(),
            vec![crate::osdclient::types::OSDOp::write_full(
                Bytes::from_static(b"x"),
            )],
        );
        op.object.hash = hash;
        op.pgid = spg;
        op.osdmap_epoch = 10;
        op.reqid = crate::osdclient::types::RequestId::new(&"client.test".into(), 77, 0);
        let rx = session.insert_pending_for_test(Arc::new(op), pg_num, should_resend);
        (session, rx)
    }

    /// The ops the scan path of `collect_resend_ops` takes off a session
    /// holding one op to `object` placed with `old_pg_num` PGs, when a map
    /// at epoch 11 has `new_pg_num`.
    async fn rescan(
        object: &ObjectId,
        old_pg_num: u32,
        new_pg_num: u32,
    ) -> Vec<(i32, crate::osdclient::session::PendingOp)> {
        let client = offline_client().await;
        let (session, _rx) = session_with_op(object, old_pg_num, true);
        let map = one_osd_map(new_pg_num, 11);
        let mut need_resend = Vec::new();
        client
            .collect_resend_ops(&session, &map, 11, Some(0), false, &mut need_resend)
            .await
            .expect("placed");
        need_resend
    }

    #[tokio::test]
    async fn an_op_whose_pg_splits_is_resent_to_the_same_primary() {
        let object = object_in_pg(8, 5);
        let resent = rescan(&object, 8, 16).await;
        assert_eq!(resent.len(), 1, "a split resends");
        let (osd, op) = &resent[0];
        assert_eq!(*osd, 0);
        assert_eq!(op.target.pg_num, 16);
        assert_eq!(op.op.osdmap_epoch, 11);
        // The OSD deduplicates a resent write by its reqid.
        assert_eq!(op.op.reqid.tid, 77);
        assert_eq!(op.tid, 77);
        assert_eq!(op.op.reqid.entity_name, "client.test".into());
    }

    #[tokio::test]
    async fn an_op_whose_pg_merges_is_resent_to_the_same_primary() {
        // 8 -> 6: PG 6 merges into PG 2, which is the merge target.
        for seed in [6, 2] {
            let resent = rescan(&object_in_pg(8, seed), 8, 6).await;
            assert_eq!(resent.len(), 1, "PG {seed} merges");
            assert_eq!(resent[0].1.target.pg_num, 6);
        }
        // PG 0 takes no part in the merge, so C++ leaves its op be.
        assert!(rescan(&object_in_pg(8, 0), 8, 6).await.is_empty());
    }

    #[tokio::test]
    async fn skipped_epochs_resend_every_op() {
        let client = offline_client().await;
        let (session, _rx) = session_with_op(&object_in_pg(8, 5), 8, true);
        let mut need_resend = Vec::new();
        client
            .collect_resend_ops(
                &session,
                &one_osd_map(8, 11),
                11,
                Some(0),
                true,
                &mut need_resend,
            )
            .await
            .expect("placed");
        assert_eq!(
            need_resend.len(),
            1,
            "same primary and pg_num, still resent"
        );
        assert_eq!(need_resend[0].0, 0);
        assert_eq!(need_resend[0].1.op.osdmap_epoch, 11);
    }

    #[tokio::test]
    async fn skipped_epochs_leave_the_session_up() {
        let client = offline_client().await;
        let (session, _rx) = session_with_op(&object_in_pg(8, 5), 8, true);
        let mut need_resend = Vec::new();
        let any_migrated = client
            .collect_resend_ops(
                &session,
                &one_osd_map(8, 11),
                11,
                Some(0),
                true,
                &mut need_resend,
            )
            .await
            .expect("placed");
        assert_eq!(need_resend.len(), 1);
        assert!(!any_migrated, "a skip alone closes no session");
        assert!(!session.io_loop_cancelled_for_test());
    }

    fn map_at(epoch: u32) -> Arc<crate::osdclient::osdmap::OSDMap> {
        let mut map = crate::osdclient::osdmap::OSDMap::new();
        map.epoch = crate::Epoch::new(epoch);
        Arc::new(map)
    }

    /// An MOSDMap holding incrementals at `inc` and full maps at `full`,
    /// whose bytes only `walk` below reads, with trim lower bound `lb`.
    fn mosdmap_with(inc: &[u32], full: &[u32], lb: u32) -> crate::monclient::MOSDMap {
        let epochs = |es: &[u32]| es.iter().map(|&e| (e, Bytes::new())).collect();
        crate::monclient::MOSDMap {
            fsid: [0; 16],
            incremental_maps: epochs(inc),
            maps: epochs(full),
            cluster_osdmap_trim_lower_bound: lb,
            newest_map: 100,
        }
    }

    /// Walk `m` from a map at epoch 10, every map building. Returns the
    /// batch and the epochs built, `i` for incremental and `f` for full.
    fn walk(m: &crate::monclient::MOSDMap) -> (super::MapBatch, Vec<String>) {
        let built = std::cell::RefCell::new(Vec::new());
        let batch = super::walk_map_epochs(
            m,
            10,
            Some(map_at(10)),
            |e, working| {
                assert_eq!(working.as_ref().unwrap().epoch.as_u32(), e - 1);
                built.borrow_mut().push(format!("i{e}"));
                Some(map_at(e))
            },
            |e| {
                built.borrow_mut().push(format!("f{e}"));
                Some(map_at(e))
            },
        );
        (batch, built.into_inner())
    }

    fn epochs(batch: &super::MapBatch) -> Vec<u32> {
        batch.maps.iter().map(|m| m.epoch.as_u32()).collect()
    }

    #[test]
    fn a_missing_epoch_the_mon_holds_is_requested() {
        // 12 is missing and above the trim bound: ask for it, and apply
        // nothing after it, not even 13's full map.
        let (batch, built) = walk(&mosdmap_with(&[11, 13, 14], &[13], 5));
        assert_eq!(built, ["i11"]);
        assert_eq!(epochs(&batch), [11]);
        assert_eq!(batch.request_from, Some(12));
        assert!(!batch.skipped);

        // The first epoch missing asks for it.
        let (batch, built) = walk(&mosdmap_with(&[12], &[], 11));
        assert!(built.is_empty() && batch.maps.is_empty());
        assert_eq!(batch.request_from, Some(11));

        // An epoch that fails to build is missing too.
        let m = mosdmap_with(&[11, 12], &[], 0);
        let batch = super::walk_map_epochs(&m, 10, Some(map_at(10)), |_, _| None, |_| None);
        assert_eq!(batch.request_from, Some(11));
    }

    #[test]
    fn a_missing_epoch_the_mon_trimmed_jumps_to_the_trim_bound() {
        // The mon trimmed up to 20: its full map, then incrementals.
        let (batch, built) = walk(&mosdmap_with(&[21, 22], &[20], 20));
        assert_eq!(built, ["f20", "i21", "i22"]);
        assert_eq!(epochs(&batch), [20, 21, 22]);
        assert!(batch.skipped);
        assert_eq!(batch.request_from, None);

        // Without the bound's full map, the next epoch is asked for.
        let (batch, built) = walk(&mosdmap_with(&[21, 22], &[], 20));
        assert!(built.is_empty());
        assert!(batch.skipped);
        assert_eq!(batch.request_from, Some(11));
    }

    #[test]
    fn a_contiguous_message_applies_whole() {
        let (batch, built) = walk(&mosdmap_with(&[11, 12], &[13], 5));
        assert_eq!(built, ["i11", "i12", "f13"]);
        assert!(!batch.skipped);
        assert_eq!(batch.request_from, None);
    }

    #[tokio::test]
    async fn an_op_whose_pg_is_unchanged_is_not_resent() {
        assert!(rescan(&object_in_pg(8, 5), 8, 8).await.is_empty());
        // An op that recorded no pg_num is never judged split.
        assert!(rescan(&object_in_pg(8, 5), 0, 16).await.is_empty());
    }

    /// Rescan one op to `object` sent with 8 PGs, pending on a session
    /// the scan path takes to be `session_osd`'s, against a map with
    /// `new_pg_num` PGs whose primary is OSD 0. Returns what was collected,
    /// whether the op is still pending, and what its sender got.
    async fn rescan_one(
        object: &ObjectId,
        should_resend: bool,
        session_osd: i32,
        new_pg_num: u32,
    ) -> (
        Vec<(i32, crate::osdclient::session::PendingOp)>,
        bool,
        Option<super::Result<crate::osdclient::types::OpResult>>,
    ) {
        let client = offline_client().await;
        let (session, mut rx) = session_with_op(object, 8, should_resend);
        let map = one_osd_map(new_pg_num, 11);
        let mut need_resend = Vec::new();
        client
            .collect_resend_ops(
                &session,
                &map,
                11,
                Some(session_osd),
                false,
                &mut need_resend,
            )
            .await
            .expect("placed");
        let pending = session.get_pending_ops_metadata().len() == 1;
        (need_resend, pending, rx.try_recv().ok())
    }

    #[tokio::test]
    async fn a_linger_send_is_cancelled_not_resent_on_a_split() {
        let (resent, pending, answer) = rescan_one(&object_in_pg(8, 5), false, 0, 16).await;
        assert!(resent.is_empty(), "not collected");
        assert!(!pending, "removed");
        assert!(matches!(answer, Some(Err(OSDClientError::Cancelled))));
    }

    #[tokio::test]
    async fn a_linger_send_is_cancelled_not_resent_on_a_primary_change() {
        // The session is OSD 1's; the map's primary is OSD 0.
        let (resent, pending, answer) = rescan_one(&object_in_pg(8, 5), false, 1, 8).await;
        assert!(resent.is_empty(), "not collected");
        assert!(!pending, "removed");
        assert!(matches!(answer, Some(Err(OSDClientError::Cancelled))));
    }

    #[tokio::test]
    async fn an_ordinary_op_is_still_resent_on_a_primary_change() {
        let (resent, pending, answer) = rescan_one(&object_in_pg(8, 5), true, 1, 8).await;
        assert_eq!(resent.len(), 1);
        assert_eq!(resent[0].0, 0, "to the new primary");
        assert!(!pending && answer.is_none());
        let (resent, _, answer) = rescan_one(&object_in_pg(8, 5), true, 0, 16).await;
        assert_eq!(resent.len(), 1, "a split resends");
        assert!(answer.is_none());
    }

    #[tokio::test]
    async fn a_linger_send_is_cancelled_when_its_session_drains() {
        let client = offline_client().await;
        let object = object_in_pg(8, 5);
        let (session, mut rx) = session_with_op(&object, 8, false);
        let mut need_resend = Vec::new();
        let _ = client
            .collect_resend_ops(
                &session,
                &one_osd_map(8, 11),
                11,
                None,
                false,
                &mut need_resend,
            )
            .await;
        assert!(need_resend.is_empty());
        assert!(session.get_pending_ops_metadata().is_empty());
        assert!(matches!(rx.try_recv(), Ok(Err(OSDClientError::Cancelled))));
    }

    #[tokio::test]
    async fn a_kick_drops_a_linger_send() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 11))).ok();
        let object = object_in_pg(8, 5);
        let (old, mut rx) = session_with_op(&object, 8, false);
        let (new, _) = session_with_op(&object_in_pg(8, 3), 8, true);
        new.remove_pending_op(77).expect("emptied");
        let new = Arc::new(new);
        client.kick_into_session(&old, &new).await;
        assert!(old.get_pending_ops_metadata().is_empty());
        assert!(new.get_pending_ops_metadata().is_empty(), "not moved");
        assert!(matches!(rx.try_recv(), Ok(Err(OSDClientError::Cancelled))));
    }

    #[tokio::test]
    async fn a_linger_send_cancels_the_one_before_it() {
        let client = offline_client().await;
        let object = object_in_pg(8, 5);
        let (session, mut rx) = session_with_op(&object, 8, false);
        client.sessions.write().await.insert(0, Arc::new(session));
        let (watch, _events) = test_watch(&client, 1001, true);

        // The first send went out as tid 77 to OSD 0.
        let first = watch.begin_send();
        watch.lock_state().in_flight = Some((0, 77));
        // A send that is not the latest cancels nothing.
        watch.lock_state().send_seq += 1;
        client.cancel_previous_send(&watch, first).await;
        assert!(rx.try_recv().is_err(), "still pending");
        watch.lock_state().send_seq -= 1;

        // The next send, a reconnect here, cancels it before it goes out.
        Arc::clone(&client).send_reconnect(Arc::clone(&watch)).await;
        assert_eq!(watch.lock_state().send_seq, first + 1, "it was sent");
        let session = Arc::clone(&client.sessions.read().await[&0]);
        assert!(session.get_pending_ops_metadata().is_empty(), "removed");
        assert!(matches!(rx.try_recv(), Ok(Err(OSDClientError::Cancelled))));
        assert_eq!(watch.lock_state().in_flight, None);
    }

    #[test]
    fn split_or_merge_reduces_the_hash_by_the_old_pg_num() {
        // Hash 0x7fc1f406 is PG 6 of 8 and PG 2 of 4 (stable_mod).
        assert!(OSDClient::pg_split_or_merge(2, 0x7fc1f406, 8, 6));
        assert!(OSDClient::pg_split_or_merge(2, 0x7fc1f406, 8, 16));
        assert!(!OSDClient::pg_split_or_merge(2, 0x7fc1f406, 8, 8));
        assert!(!OSDClient::pg_split_or_merge(2, 0x7fc1f406, 0, 16));
        // Hash 0x10 is PG 0 of 8: 8 -> 6 leaves it alone.
        assert!(!OSDClient::pg_split_or_merge(2, 0x10, 8, 6));
    }

    #[test]
    fn replace_pending_keeps_a_pgnls_cursor() {
        let map = map_with_crush(2, 12);
        let mut object = ObjectId::new(2, "");
        object.hash = 0xef61efce;
        let cursor = crate::HObject::new(2, String::new(), 0xef61efce);
        let op = mosdop(
            object,
            vec![crate::osdclient::types::OSDOp::pgls(8, cursor, 1).unwrap()],
        );
        assert!(OSDClient::is_pg_op(&op));
        let (hash, spg, _) = OSDClient::replace_pending(&map, &op).unwrap();
        assert_eq!(hash, 0xef61efce);
        assert_eq!((spg.pool, spg.seed), (2, 6));
    }

    #[test]
    fn replace_pending_uses_the_locator() {
        let map = map_with_crush(2, 32);
        let cases = [
            (ObjectId::with_namespace(2, "foo", "ns1"), 0xf4569544, 4),
            (keyed(2, "_multipart_obj.2~abc.1", "obj", ""), 0xaabc5e21, 1),
        ];
        for (mut object, want_hash, want_seed) in cases {
            object.hash = 0x12345678;
            let op = mosdop(object, vec![crate::osdclient::types::OSDOp::stat()]);
            assert!(!OSDClient::is_pg_op(&op));
            let (hash, spg, _) = OSDClient::replace_pending(&map, &op).unwrap();
            assert_eq!(hash, want_hash);
            assert_eq!((spg.pool, spg.seed), (2, want_seed));
        }
    }

    /// The front of a PGNLS request in `nspace` at cursor hash `hash`.
    fn pgnls_front(nspace: &str, hash: u32) -> bytes::Bytes {
        use crate::msgr2::ceph_message::{CephMessage, CrcFlags};
        let object = OSDClient::pgnls_target(2, hash, nspace);
        let cursor = crate::HObject::new(2, String::new(), hash);
        let op = mosdop(
            object,
            vec![crate::osdclient::types::OSDOp::pgls(8, cursor, 1).unwrap()],
        );
        CephMessage::from_payload(&op, 0, CrcFlags::ALL)
            .unwrap()
            .front
    }

    fn encoded_locator(nspace: &str) -> Vec<u8> {
        use crate::Denc;
        let loc = ObjectLocator {
            pool_id: 2,
            key: String::new(),
            namespace: nspace.to_string(),
            hash: -1,
        };
        let mut buf = bytes::BytesMut::new();
        loc.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn pgnls_target_carries_the_namespace() {
        let target = OSDClient::pgnls_target(2, 0xef61efce, "ns1");
        assert_eq!(target.namespace, "ns1");
        assert_eq!(target.oid, "");
        assert_eq!(target.key, "");
        assert_eq!(target.hash, 0xef61efce);
        assert_eq!(
            ObjectLocator::from(&target),
            ObjectLocator {
                pool_id: 2,
                key: String::new(),
                namespace: "ns1".to_string(),
                hash: -1,
            }
        );
    }

    /// A session to OSD 0, the one every op of `client` goes to, whose
    /// sends come out of the returned channel unanswered.
    async fn fake_osd(
        client: &Arc<OSDClient>,
    ) -> (
        Arc<crate::osdclient::session::OSDSession>,
        tokio::sync::mpsc::Receiver<crate::msgr2::message::Message>,
    ) {
        let (tx, _rx) = crate::msgr2::map_channel(1);
        let mut session = crate::osdclient::session::OSDSession::new(
            0,
            None,
            0,
            tx,
            std::sync::Weak::new(),
            Arc::clone(&client.next_tid),
        );
        let sends = session.sends_for_test();
        let session = Arc::new(session);
        *client.fake_osd.lock().unwrap() = Some(Arc::clone(&session));
        (session, sends)
    }

    /// Take the next op `session` sends, and the pending op it is.
    async fn next_send(
        session: &crate::osdclient::session::OSDSession,
        sends: &mut tokio::sync::mpsc::Receiver<crate::msgr2::message::Message>,
    ) -> crate::osdclient::session::PendingOp {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), sends.recv())
            .await
            .expect("an op went out")
            .expect("sends open");
        session
            .remove_pending_op(frame.tid())
            .expect("the op sent is pending")
    }

    fn pgnls_reply(
        result: i32,
        handle: crate::HObject,
        oid: &str,
    ) -> crate::osdclient::types::OpResult {
        let response = crate::osdclient::PgNlsResponse::with_entries(
            handle,
            vec![crate::osdclient::ListObjectImpl::new("", oid, "")],
        );
        let mut outdata = bytes::BytesMut::new();
        crate::Denc::encode(&response, &mut outdata, 0).expect("encoded");
        crate::osdclient::types::OpResult {
            result,
            version: 0,
            ops: vec![crate::osdclient::types::OpReply {
                return_code: result,
                outdata: outdata.freeze(),
            }],
            redirect: None,
        }
    }

    #[tokio::test]
    async fn each_listing_page_is_placed_in_the_current_map() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (session, mut sends) = fake_osd(&client).await;
        let listing = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.list_in_namespace(2, "", None, 100).await })
        };

        // Page one: PG 0 of 8, at epoch 10. The pool splits to 16 PGs at
        // epoch 11 before its reply.
        let first = next_send(&session, &mut sends).await;
        assert_eq!(first.op.osdmap_epoch, 10);
        assert_eq!(first.op.pgid.seed, 0);
        assert_eq!(first.target.pg_num, 8);
        client.osdmap_tx.send(Some(one_osd_map(16, 11))).ok();
        let cursor = crate::HObject::new(2, String::new(), 0xd);
        let _ = first.result_tx.send(Ok(pgnls_reply(1, cursor, "a")));

        // Page two, at cursor hash 0xd: PG 5 of 8, but PG 13 of the 16
        // now, and stamped with epoch 11.
        let second = next_send(&session, &mut sends).await;
        assert_eq!(second.op.osdmap_epoch, 11);
        assert_eq!(second.op.pgid.seed, 13);
        assert_eq!(second.op.object.hash, 0xd, "still placed by the cursor");
        assert_eq!(second.target.pg_num, 16);
        let mut end = crate::HObject::empty_cursor(2);
        end.max = true;
        let _ = second.result_tx.send(Ok(pgnls_reply(1, end, "b")));

        let listed = listing.await.expect("ran").expect("listed");
        let names: Vec<_> = listed.entries.iter().map(|e| e.oid.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(listed.cursor, None);
    }

    /// A write of `object` sent through `session` at epoch 10 with 8
    /// PGs, as route_and_submit places it; `still_placed` as given.
    async fn submit_write(
        session: &crate::osdclient::session::OSDSession,
        object: &ObjectId,
        still_placed: impl FnOnce() -> bool,
    ) -> super::Result<
        Option<tokio::sync::oneshot::Receiver<super::Result<crate::osdclient::types::OpResult>>>,
    > {
        let map = one_osd_map(8, 10);
        let (hash, spg, _) = OSDClient::object_to_osds_in_map(&map, object).unwrap();
        let mut op = mosdop(
            object.clone(),
            vec![crate::osdclient::types::OSDOp::write_full(
                Bytes::from_static(b"x"),
            )],
        );
        op.object.hash = hash;
        op.pgid = spg;
        op.osdmap_epoch = 10;
        op.reqid = crate::osdclient::types::RequestId::new(&"client.test".into(), 77, 0);
        session
            .submit_op(
                Arc::new(op),
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                8,
                true,
                still_placed,
            )
            .await
    }

    #[tokio::test]
    async fn an_op_whose_map_moved_before_it_went_out_is_taken_back() {
        let client = offline_client().await;
        let (session, mut sends) = fake_osd(&client).await;
        let placed = submit_write(&session, &object_in_pg(8, 5), || false).await;
        assert!(matches!(placed, Ok(None)), "to be placed again");
        assert!(session.get_pending_ops_metadata().is_empty(), "taken back");
        assert!(sends.try_recv().is_err(), "nothing sent");

        // Still placed: it goes out.
        let placed = submit_write(&session, &object_in_pg(8, 5), || true).await;
        assert!(matches!(placed, Ok(Some(_))));
        assert_eq!(session.get_pending_ops_metadata().len(), 1);
        assert_eq!(sends.try_recv().expect("sent").tid(), 77);
    }

    #[tokio::test]
    async fn an_op_a_rescan_took_first_is_left_to_the_rescan() {
        let client = offline_client().await;
        let (session, mut sends) = fake_osd(&client).await;
        // The rescan ran with the op pending, took it and resends it.
        let taken = std::sync::Mutex::new(None);
        let placed = submit_write(&session, &object_in_pg(8, 5), || {
            *taken.lock().unwrap() = session.remove_pending_op(77);
            false
        })
        .await;
        let mut rx = placed.expect("submitted").expect("its reply still comes");
        assert!(sends.try_recv().is_err(), "nothing sent here");
        let taken = taken.into_inner().unwrap().expect("the rescan had it");
        let _ = taken
            .result_tx
            .send(Ok(pgnls_reply(0, crate::HObject::empty_cursor(2), "")));
        assert!(matches!(rx.try_recv(), Ok(Ok(_))), "the resend answers it");
    }

    #[tokio::test]
    async fn a_write_a_rescan_resent_in_the_submit_window_goes_out_once() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (session, mut sends) = fake_osd(&client).await;
        // With the write pending and its map unchecked, the pool splits on
        // the same OSD and the rescan resends the write through the same
        // session, as collect_resend_ops and insert_migrated_op do.
        let publisher = Arc::clone(&client);
        let rescan = Arc::clone(&session);
        let mut fired = false;
        *client.in_submit_window.lock().unwrap() = Some(Box::new(move || {
            if std::mem::replace(&mut fired, true) {
                return;
            }
            publisher.osdmap_tx.send(Some(one_osd_map(16, 11))).ok();
            let (tid, ..) = rescan.get_pending_ops_metadata()[0];
            let op = rescan.remove_pending_op(tid).expect("pending");
            futures::executor::block_on(rescan.insert_migrated_op(op, 11)).expect("resent");
        }));
        let write = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                client
                    .write_full(2, &object_in_pg(16, 13).oid, Bytes::from_static(b"x"))
                    .await
            })
        };

        let first = tokio::time::timeout(std::time::Duration::from_secs(5), sends.recv())
            .await
            .expect("the rescan's send")
            .expect("sends open");
        let again = tokio::time::timeout(std::time::Duration::from_millis(300), sends.recv()).await;
        assert!(
            again.is_err(),
            "tid {} sent again as tid {:?}",
            first.tid(),
            again.ok().flatten().map(|m| m.tid())
        );
        let sent = session
            .remove_pending_op(first.tid())
            .expect("the op sent is pending");
        assert_eq!(sent.op.osdmap_epoch, 11);
        let _ = sent.result_tx.send(Ok(crate::osdclient::types::OpResult {
            result: 0,
            version: 1,
            ops: vec![crate::osdclient::types::OpReply {
                return_code: 0,
                outdata: Bytes::new(),
            }],
            redirect: None,
        }));
        write.await.expect("ran").expect("written");
    }

    /// Lift a backoff on every object of `object`'s PG at epoch 10 with 8
    /// PGs, as an OSD's UNBLOCK does, from inside a synchronous hook.
    fn unblock_pg_of(session: &crate::osdclient::session::OSDSession, object: &ObjectId) {
        let (_, spg, _) = OSDClient::object_to_osds_in_map(&one_osd_map(8, 10), object).unwrap();
        let begin = crate::HObject::empty_cursor(2);
        let mut end = crate::HObject::empty_cursor(2);
        end.max = true;
        futures::executor::block_on(session.resend_ops_in_range(&spg, &begin, &end));
    }

    #[tokio::test]
    async fn an_unblock_does_not_send_an_op_its_submitter_has_not_checked() {
        let client = offline_client().await;
        let (session, mut sends) = fake_osd(&client).await;
        let object = object_in_pg(8, 5);

        // The UNBLOCK lands with the op pending and its map unchecked;
        // the check then takes it back to place it again.
        let sent_in_window = std::cell::Cell::new(None);
        let placed = submit_write(&session, &object, || {
            unblock_pg_of(&session, &object);
            sent_in_window.set(Some(sends.try_recv().is_ok()));
            false
        })
        .await;
        assert_eq!(
            sent_in_window.get(),
            Some(false),
            "nothing before the check"
        );
        assert!(matches!(placed, Ok(None)), "taken back");
        assert!(sends.try_recv().is_err(), "tid 77 never went out");

        // With the map unchanged, only the submitter's frame goes out.
        let placed = submit_write(&session, &object, || {
            unblock_pg_of(&session, &object);
            sent_in_window.set(Some(sends.try_recv().is_ok()));
            true
        })
        .await;
        assert_eq!(sent_in_window.get(), Some(false));
        assert!(matches!(placed, Ok(Some(_))));
        assert_eq!(sends.try_recv().expect("sent").tid(), 77);
        assert!(sends.try_recv().is_err(), "sent once");

        // Once submitted, an UNBLOCK re-sends it.
        unblock_pg_of(&session, &object);
        assert_eq!(sends.try_recv().expect("re-sent").tid(), 77);
    }

    #[tokio::test]
    async fn an_op_a_rescan_resent_on_its_session_is_not_taken_back() {
        let client = offline_client().await;
        let (session, mut sends) = fake_osd(&client).await;
        // The rescan took the op and resent it through the same session.
        let placed = submit_write(&session, &object_in_pg(8, 5), || {
            let op = session.remove_pending_op(77).expect("pending");
            futures::executor::block_on(session.insert_migrated_op(op, 11)).expect("resent");
            false
        })
        .await;
        assert!(matches!(placed, Ok(Some(_))), "its reply still comes");
        let frame = sends.try_recv().expect("the rescan's send");
        assert_eq!(frame.tid(), 77);
        assert!(sends.try_recv().is_err(), "nothing sent here");
        assert_eq!(session.get_pending_ops_metadata().len(), 1, "left pending");
    }

    #[tokio::test]
    async fn an_op_is_placed_again_when_a_map_lands_before_it_is_pending() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (session, mut sends) = fake_osd(&client).await;
        // PG 5 of 8, and PG 13 once it splits to 16.
        let object = object_in_pg(16, 13);
        // The pool splits after the op is placed and before it is
        // pending: the rescan of epoch 11 finds nothing to resend.
        let first_tid = client.next_tid.load(std::sync::atomic::Ordering::Relaxed);
        let publisher = Arc::clone(&client);
        let mut published = false;
        *client.before_submit.lock().unwrap() = Some(Box::new(move || {
            if !std::mem::replace(&mut published, true) {
                publisher.osdmap_tx.send(Some(one_osd_map(16, 11))).ok();
            }
        }));
        let write = {
            let client = Arc::clone(&client);
            let object = object.clone();
            tokio::spawn(async move {
                client
                    .write_full(2, &object.oid, Bytes::from_static(b"x"))
                    .await
            })
        };

        let sent = next_send(&session, &mut sends).await;
        assert_eq!(sent.op.osdmap_epoch, 11, "placed again in the new map");
        assert_eq!(sent.op.pgid.seed, 13);
        assert_eq!(sent.target.pg_num, 16);
        assert_eq!(sent.tid, first_tid, "under the tid of its first placement");
        assert!(
            sends.try_recv().is_err(),
            "the first placement never went out"
        );
        assert!(session.get_pending_ops_metadata().is_empty());
        let _ = sent.result_tx.send(Ok(crate::osdclient::types::OpResult {
            result: 0,
            version: 1,
            ops: vec![crate::osdclient::types::OpReply {
                return_code: 0,
                outdata: Bytes::new(),
            }],
            redirect: None,
        }));
        write.await.expect("ran").expect("written");
    }

    #[tokio::test]
    async fn a_linger_send_taken_back_is_cancelled_not_placed_again() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (session, mut sends) = fake_osd(&client).await;
        let object = object_in_pg(16, 13);
        let publisher = Arc::clone(&client);
        *client.before_submit.lock().unwrap() = Some(Box::new(move || {
            publisher.osdmap_tx.send(Some(one_osd_map(16, 11))).ok();
        }));

        for kind in [super::SubmitKind::Linger, super::SubmitKind::Ping] {
            let (_, _, osdmap, rx, _) = client
                .submit_once_in_map(
                    &object,
                    vec![crate::osdclient::types::OSDOp::watch_ping(1, 1)],
                    crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                    crate::osdclient::types::OsdOpFlags::READ,
                    kind,
                )
                .await
                .expect("routed");
            assert_eq!(osdmap.epoch.as_u32(), 10, "not placed again");
            assert!(
                matches!(rx.await, Ok(Err(OSDClientError::Cancelled))),
                "no answer"
            );
            assert!(sends.try_recv().is_err(), "never sent");
            assert!(session.get_pending_ops_metadata().is_empty());
            client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        }
    }

    /// A successful `MOSDOpReply` at object version `version`, echoing
    /// `retry_attempt`.
    fn op_reply(retry_attempt: i32, version: u64) -> crate::osdclient::messages::MOSDOpReply {
        crate::osdclient::messages::MOSDOpReply {
            _object: ObjectId::new(2, ""),
            _pgid: crate::osdclient::types::StripedPgId::from_pg(2, 0),
            _flags: 0,
            result: 0,
            _epoch: 10,
            _version: version,
            user_version: version,
            retry_attempt,
            redirect: None,
            ops: vec![OpReply {
                return_code: 0,
                outdata: Bytes::new(),
            }],
        }
    }

    /// The retry_attempt of op `tid` if it is pending on `session`.
    fn pending_attempt(session: &crate::osdclient::session::OSDSession, tid: u64) -> Option<i32> {
        session
            .get_pending_ops_metadata()
            .into_iter()
            .find(|(pending, ..)| *pending == tid)
            .map(|(_, op, ..)| op.retry_attempt)
    }

    #[tokio::test]
    async fn a_reply_to_an_earlier_send_leaves_the_op_waiting() {
        let client = offline_client().await;
        let (session, mut sends) = fake_osd(&client).await;
        let mut rx = submit_write(&session, &object_in_pg(8, 5), || true)
            .await
            .expect("submitted")
            .expect("sent");
        // Skipped epochs resend it through the same session, as
        // collect_resend_ops and insert_migrated_op do.
        let op = session.remove_pending_op(77).expect("pending");
        session.insert_migrated_op(op, 11).await.expect("resent");
        assert_eq!(sends.try_recv().expect("sent").tid(), 77);
        assert_eq!(sends.try_recv().expect("resent").tid(), 77);

        // As Objecter ignores it (v19.2.6:src/osdc/Objecter.cc:3448-3459).
        assert!(
            session
                .handle_osd_op_reply(77, op_reply(0, 6))
                .await
                .is_none()
        );
        assert_eq!(pending_attempt(&session, 77), Some(1), "still waiting");
        assert!(rx.try_recv().is_err(), "not answered");

        assert!(
            session
                .handle_osd_op_reply(77, op_reply(1, 7))
                .await
                .is_none()
        );
        let result = rx.try_recv().expect("answered").expect("written");
        assert_eq!(result.version, 7, "by the reply to its latest send");
    }

    /// Take the next frame `session` sends.
    async fn next_frame(
        sends: &mut tokio::sync::mpsc::Receiver<crate::msgr2::message::Message>,
    ) -> crate::msgr2::message::Message {
        tokio::time::timeout(std::time::Duration::from_secs(5), sends.recv())
            .await
            .expect("an op went out")
            .expect("sends open")
    }

    /// Start an append through `execute_op` to a fake OSD 0, and end the
    /// session's connection once the append is out. Returns the session,
    /// its sends, the append's task and the tid the append went out under.
    async fn append_across_a_reset(
        client: &Arc<OSDClient>,
    ) -> (
        Arc<crate::osdclient::session::OSDSession>,
        tokio::sync::mpsc::Receiver<crate::msgr2::message::Message>,
        tokio::task::JoinHandle<super::Result<OpResult>>,
        u64,
    ) {
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (session, mut sends) = fake_osd(client).await;
        let append = {
            let client = Arc::clone(client);
            tokio::spawn(async move {
                client
                    .execute_op(
                        object_in_pg(8, 5),
                        vec![crate::osdclient::types::OSDOp::append(Bytes::from_static(
                            b"x",
                        ))],
                        None,
                        crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                        crate::osdclient::types::OsdOpFlags::empty(),
                    )
                    .await
            })
        };
        let tid = next_frame(&mut sends).await.tid();
        assert_eq!(pending_attempt(&session, tid), Some(0), "a first send");
        assert_eq!(session.lose_connection_for_test().await, 1);
        (session, sends, append, tid)
    }

    #[tokio::test]
    async fn an_op_whose_connection_drops_is_resent_under_its_tid() {
        let client = offline_client().await;
        let (session, mut sends, append, tid) = append_across_a_reset(&client).await;

        assert_eq!(next_frame(&mut sends).await.tid(), tid, "the same reqid");
        assert_eq!(pending_attempt(&session, tid), Some(1), "the next attempt");
        assert_eq!(
            client.next_tid.load(std::sync::atomic::Ordering::Relaxed),
            tid + 1,
            "no tid taken for the resend"
        );
        assert!(sends.try_recv().is_err(), "sent once more");

        assert!(
            session
                .handle_osd_op_reply(tid, op_reply(1, 7))
                .await
                .is_none()
        );
        let result = append.await.expect("ran").expect("appended");
        assert_eq!(result.version, 7);
    }

    #[tokio::test]
    async fn a_reply_to_the_send_before_a_reset_is_ignored() {
        let client = offline_client().await;
        let (session, mut sends, append, tid) = append_across_a_reset(&client).await;
        next_frame(&mut sends).await;

        // As Objecter ignores it (v19.2.6:src/osdc/Objecter.cc:3448-3459),
        // the op waiting for the reply to its own attempt.
        assert!(
            session
                .handle_osd_op_reply(tid, op_reply(0, 6))
                .await
                .is_none()
        );
        assert_eq!(pending_attempt(&session, tid), Some(1), "still waiting");
        tokio::task::yield_now().await;
        assert!(!append.is_finished());

        assert!(
            session
                .handle_osd_op_reply(tid, op_reply(1, 7))
                .await
                .is_none()
        );
        let result = append.await.expect("ran").expect("appended");
        assert_eq!(result.version, 7, "the reply to the latest attempt");
    }

    /// [`fake_osd`], but in the client's map with the client's Tracker, so
    /// its ops are tracked and the Tracker's timeout callback finds them.
    async fn tracked_fake_osd(
        client: &Arc<OSDClient>,
    ) -> (
        Arc<crate::osdclient::session::OSDSession>,
        tokio::sync::mpsc::Receiver<crate::msgr2::message::Message>,
    ) {
        let (tx, _rx) = crate::msgr2::map_channel(1);
        let mut session = crate::osdclient::session::OSDSession::new(
            0,
            None,
            0,
            tx,
            Arc::downgrade(client),
            Arc::clone(&client.next_tid),
        );
        let sends = session.sends_for_test();
        let session = Arc::new(session);
        *client.fake_osd.lock().unwrap() = Some(Arc::clone(&session));
        client
            .sessions
            .write()
            .await
            .insert(0, Arc::clone(&session));
        (session, sends)
    }

    #[tokio::test(start_paused = true)]
    async fn a_resend_to_the_same_osd_outlives_the_deadline_of_the_send_before() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (session, mut sends) = tracked_fake_osd(&client).await;
        let per_send = client.tracker.operation_timeout();
        let append = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                client
                    .execute_op(
                        object_in_pg(8, 5),
                        vec![crate::osdclient::types::OSDOp::append(Bytes::from_static(
                            b"x",
                        ))],
                        Some(per_send * 4),
                        crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                        crate::osdclient::types::OsdOpFlags::empty(),
                    )
                    .await
            })
        };
        let tid = next_frame(&mut sends).await.tid();
        tokio::time::sleep(per_send / 2).await;
        assert_eq!(session.lose_connection_for_test().await, 1);
        next_frame(&mut sends).await;

        // Past the first send's Tracker deadline, short of the resend's.
        tokio::time::sleep(per_send / 2 + std::time::Duration::from_secs(1)).await;
        assert_eq!(pending_attempt(&session, tid), Some(1), "not timed out");
        assert!(
            session
                .handle_osd_op_reply(tid, op_reply(1, 7))
                .await
                .is_none()
        );
        assert_eq!(append.await.expect("ran").expect("appended").version, 7);
    }

    #[tokio::test(start_paused = true)]
    async fn a_resend_outlives_the_deadline_of_a_send_that_never_went_out() {
        let client = offline_client().await;
        let (session, mut sends) = tracked_fake_osd(&client).await;
        let per_send = client.tracker.operation_timeout();
        let object = object_in_pg(8, 5);

        // A session to the same OSD whose I/O task has exited: its send
        // channel is closed.
        let (tx, _rx) = crate::msgr2::map_channel(1);
        let exited = crate::osdclient::session::OSDSession::new(
            0,
            None,
            0,
            tx,
            Arc::downgrade(&client),
            Arc::clone(&client.next_tid),
        );
        assert!(matches!(
            submit_write(&exited, &object, || true).await,
            Err(OSDClientError::Connection(_))
        ));
        assert!(exited.get_pending_ops_metadata().is_empty());

        tokio::time::sleep(per_send / 2).await;
        let _reply = submit_write(&session, &object, || true)
            .await
            .expect("submitted")
            .expect("sent");
        assert_eq!(sends.try_recv().expect("sent").tid(), 77);

        // Past the failed send's Tracker deadline, short of the resend's.
        tokio::time::sleep(per_send / 2 + std::time::Duration::from_secs(1)).await;
        assert!(pending_attempt(&session, 77).is_some(), "not timed out");
    }

    #[tokio::test]
    async fn a_linger_send_is_cancelled_not_resent_when_its_connection_drops() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (session, mut sends) = fake_osd(&client).await;
        for kind in [super::SubmitKind::Linger, super::SubmitKind::Ping] {
            let (_, tid, _, rx, _) = client
                .submit_once_in_map(
                    &object_in_pg(8, 5),
                    vec![crate::osdclient::types::OSDOp::watch_ping(1, 1)],
                    crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                    crate::osdclient::types::OsdOpFlags::READ,
                    kind,
                )
                .await
                .expect("sent");
            assert_eq!(next_frame(&mut sends).await.tid(), tid);
            assert_eq!(session.lose_connection_for_test().await, 1);
            assert!(
                matches!(rx.await, Ok(Err(OSDClientError::Cancelled))),
                "no answer"
            );
            tokio::task::yield_now().await;
            assert!(sends.try_recv().is_err(), "not resent");
            assert!(session.get_pending_ops_metadata().is_empty());
        }
    }

    #[tokio::test]
    async fn a_listing_page_whose_connection_drops_keeps_its_tid_and_cursor() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (session, mut sends) = fake_osd(&client).await;
        let listing = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                client
                    .list_in_namespace(2, "", Some("13".into()), 100)
                    .await
            })
        };

        // The page at cursor hash 0xd goes to PG 5 of 8. The pool then
        // splits to 16 PGs, and the connection drops.
        let tid = next_frame(&mut sends).await.tid();
        client.osdmap_tx.send(Some(one_osd_map(16, 11))).ok();
        assert_eq!(session.lose_connection_for_test().await, 1);

        let resent = next_send(&session, &mut sends).await;
        assert_eq!(resent.tid, tid, "the same reqid");
        assert_eq!(resent.op.retry_attempt, 1);
        assert_eq!(resent.op.object.hash, 0xd, "still placed by the cursor");
        assert_eq!(resent.op.pgid.seed, 13, "PG 13 of the 16 now");
        assert_eq!(resent.op.osdmap_epoch, 11);
        let mut end = crate::HObject::empty_cursor(2);
        end.max = true;
        let _ = resent.result_tx.send(Ok(pgnls_reply(1, end, "a")));

        let listed = listing.await.expect("ran").expect("listed");
        let names: Vec<_> = listed.entries.iter().map(|e| e.oid.as_str()).collect();
        assert_eq!(names, ["a"]);
    }

    #[test]
    fn pgnls_request_encodes_the_namespace() {
        let with_ns = pgnls_front("ns1", 0xef61efce);
        let without = pgnls_front("", 0xef61efce);
        assert!(contains(&with_ns, &encoded_locator("ns1")));
        assert!(contains(&without, &encoded_locator("")));
        assert_eq!(with_ns.len(), without.len() + "ns1".len());
    }

    #[test]
    fn pgnls_request_all_namespaces() {
        let front = pgnls_front(crate::osdclient::ALL_NSPACES, 0xef61efce);
        let locator = encoded_locator(crate::osdclient::ALL_NSPACES);
        // The namespace is the locator's last string before its i64 hash.
        let tail = [&[1u8, 0, 0, 0, 0x01][..], &(-1i64).to_le_bytes()].concat();
        assert!(locator.ends_with(&tail));
        assert!(contains(&front, &locator));
    }

    #[test]
    fn pgnls_target_default_namespace_is_unnamespaced() {
        let mut unnamespaced = ObjectId::new(2, "");
        unnamespaced.hash = 0xef61efce;
        assert_eq!(OSDClient::pgnls_target(2, 0xef61efce, ""), unnamespaced);
    }

    /// An `OSDClient` whose MonClient never connects: enough for the
    /// paths that need no cluster.
    pub(crate) async fn offline_client() -> Arc<OSDClient> {
        offline_client_with(OSDClientConfig::default()).await
    }

    async fn offline_client_with(config: OSDClientConfig) -> Arc<OSDClient> {
        let auth = crate::monclient::auth_config::AuthConfig::no_auth("client.test".to_string());
        let mon_config = crate::monclient::MonClientConfig {
            mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
            auth: Some(auth),
            ..Default::default()
        };
        let mon = crate::monclient::MonClient::new(mon_config, None)
            .await
            .expect("monclient");
        let (tx, rx) = crate::msgr2::map_channel(8);
        OSDClient::new(config, crate::UuidD::default(), mon, tx, rx)
            .await
            .expect("osdclient")
    }

    #[tokio::test]
    async fn watch_notify_messages_are_routed_by_cookie() {
        use crate::osdclient::messages::{CEPH_MSG_WATCH_NOTIFY, CEPH_WATCH_EVENT_NOTIFY};
        use crate::osdclient::watch::{Linger, LingerKind, WatchEvent};

        let client = offline_client().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        client.lingers.insert(
            1001,
            Arc::new(Linger::new(
                1001,
                ObjectId::new(1, "o"),
                LingerKind::Watch { timeout: 0 },
                tx,
            )),
        );

        let mut front = vec![1, CEPH_WATCH_EVENT_NOTIFY];
        front.extend_from_slice(&1001u64.to_le_bytes());
        front.extend_from_slice(&0u64.to_le_bytes());
        front.extend_from_slice(&55u64.to_le_bytes());
        front.extend_from_slice(&[2, 0, 0, 0, b'h', b'i']);
        front.extend_from_slice(&0i32.to_le_bytes());
        front.extend_from_slice(&4242u64.to_le_bytes());
        let mut msg = crate::msgr2::message::Message::new(CEPH_MSG_WATCH_NOTIFY, front.into());
        msg.header.version = crate::denc::zerocopy::little_endian::U16::new(3);

        client.dispatch_from_osd(0, msg).await.expect("dispatch");
        assert_eq!(
            rx.try_recv(),
            Ok(WatchEvent::Notify {
                notify_id: 55,
                notifier_gid: 4242,
                payload: Bytes::from_static(b"hi"),
            })
        );
    }

    #[tokio::test]
    async fn a_vanished_pool_disconnects_its_watch_once() {
        use crate::osdclient::error::ENOTCONN;
        use crate::osdclient::watch::{Linger, LingerKind, PgInterval, WatchEvent};

        let client = offline_client().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let linger = Arc::new(Linger::new(
            1001,
            ObjectId::new(7, "o"),
            LingerKind::Watch { timeout: 0 },
            tx,
        ));
        {
            let mut state = linger.lock_state();
            state.registered = true;
            state.osd = Some(0);
            state.interval = Some(PgInterval {
                up: vec![0],
                up_primary: 0,
                acting: vec![0],
                acting_primary: 0,
                size: 1,
                min_size: 1,
                pg_num: 1,
                pgp_num: 1,
                pg_num_pending: 1,
                epoch: 1,
            });
        }
        client.lingers.insert(1001, linger);

        let empty = Arc::new(crate::osdclient::osdmap::OSDMap::new());
        client.scan_lingers_on_map_change(std::slice::from_ref(&empty), false);
        client.scan_lingers_on_map_change(std::slice::from_ref(&empty), false);
        assert_eq!(rx.try_recv(), Ok(WatchEvent::Disconnect { code: ENOTCONN }));
        assert!(client.lingers.is_empty(), "the linger is unregistered");
        assert_eq!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        );
    }

    #[tokio::test]
    async fn the_reset_hook_does_nothing_after_shutdown() {
        let client = offline_client().await;
        assert!(client.on_session_reset(3));
        client.shutdown().await;
        assert!(!client.on_session_reset(3));
    }

    #[tokio::test]
    async fn only_a_published_session_reports_its_reset() {
        use crate::osdclient::session::report_reset;
        use std::sync::atomic::AtomicBool;

        let client = offline_client().await;
        let weak = Arc::downgrade(&client);
        assert!(!report_reset(&AtomicBool::new(false), &weak, 3));
        assert!(report_reset(&AtomicBool::new(true), &weak, 3));
    }

    #[tokio::test]
    async fn shutdown_ends_every_watch_and_notify() {
        use crate::osdclient::error::{ECANCELED, ENOTCONN};
        use crate::osdclient::watch::{Linger, LingerKind, WatchEvent, Watcher};

        let client = offline_client().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let watch = Arc::new(Linger::new(
            1001,
            ObjectId::new(1, "o"),
            LingerKind::Watch { timeout: 0 },
            tx,
        ));
        client.lingers.insert(1001, Arc::clone(&watch));
        let mut watcher = Watcher::new(Arc::clone(&client), watch, rx);

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        client.lingers.insert(
            1002,
            Arc::new(Linger::new(
                1002,
                ObjectId::new(1, "o"),
                LingerKind::Notify {
                    completion: std::sync::Mutex::new(Some(done_tx)),
                    notify_id: std::sync::atomic::AtomicU64::new(0),
                    timeout_secs: 10,
                    payload: Bytes::new(),
                },
                tx,
            )),
        );

        client.shutdown().await;
        assert!(client.lingers.is_empty());
        assert_eq!(
            watcher.recv().await,
            Some(WatchEvent::Disconnect { code: ENOTCONN })
        );
        assert_eq!(watcher.recv().await, None);
        assert_eq!(done_rx.await, Ok((ECANCELED, Bytes::new())));
    }

    #[tokio::test]
    async fn a_forgotten_watch_ends_its_receiver() {
        use crate::osdclient::watch::{Linger, LingerKind};

        let client = offline_client().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        client.lingers.insert(
            1001,
            Arc::new(Linger::new(
                1001,
                ObjectId::new(1, "o"),
                LingerKind::Watch { timeout: 0 },
                tx,
            )),
        );
        client.forget_linger(1001);
        assert_eq!(rx.recv().await, None);
    }

    fn test_watch(
        client: &Arc<OSDClient>,
        cookie: u64,
        registered: bool,
    ) -> (
        Arc<crate::osdclient::watch::Linger>,
        tokio::sync::mpsc::UnboundedReceiver<crate::osdclient::watch::WatchEvent>,
    ) {
        use crate::osdclient::watch::{Linger, LingerKind};
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let linger = Arc::new(Linger::new(
            cookie,
            ObjectId::new(1, "o"),
            LingerKind::Watch { timeout: 0 },
            tx,
        ));
        {
            let mut state = linger.lock_state();
            state.registered = registered;
            state.osd = Some(0);
        }
        client.lingers.insert(cookie, Arc::clone(&linger));
        (linger, rx)
    }

    #[tokio::test]
    async fn a_linger_send_without_an_answer_fails_nothing() {
        use crate::osdclient::watch::{Linger, LingerKind};

        // No OSDMap: every send fails as a connection error, before an OSD.
        let client = offline_client().await;

        let (watch, mut rx) = test_watch(&client, 1001, true);
        Arc::clone(&client).send_reconnect(Arc::clone(&watch)).await;
        assert_eq!(watch.lock_state().register_gen, 1, "the reconnect was sent");
        assert_eq!(watch.lock_state().last_error, None);
        assert!(rx.try_recv().is_err());

        let (pending, _rx) = test_watch(&client, 1002, false);
        let (reg_tx, mut reg_rx) = tokio::sync::oneshot::channel();
        pending.lock_state().registration = Some(reg_tx);
        client.send_register(&pending).await;
        assert!(reg_rx.try_recv().is_err(), "the registration stays pending");

        let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let notify = Arc::new(Linger::new(
            1003,
            ObjectId::new(1, "o"),
            LingerKind::Notify {
                completion: std::sync::Mutex::new(Some(done_tx)),
                notify_id: std::sync::atomic::AtomicU64::new(0),
                timeout_secs: 10,
                payload: Bytes::new(),
            },
            tx,
        ));
        client.lingers.insert(1003, Arc::clone(&notify));
        client.send_notify(&notify).await;
        assert!(done_rx.try_recv().is_err(), "the notify stays pending");
    }

    #[test]
    fn the_notify_bound_follows_the_osd_timeout() {
        use super::notify_outer_bound;
        use std::time::Duration;
        assert_eq!(notify_outer_bound(0), Duration::from_secs(60));
        assert_eq!(notify_outer_bound(10), Duration::from_secs(40));
    }

    #[tokio::test]
    async fn a_resend_restarts_the_notify_bound() {
        use super::await_notify;
        use crate::osdclient::watch::{Linger, LingerKind};
        use std::time::Duration;

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let linger = Arc::new(Linger::new(
            1001,
            ObjectId::new(1, "o"),
            LingerKind::Notify {
                completion: std::sync::Mutex::new(None),
                notify_id: std::sync::atomic::AtomicU64::new(0),
                timeout_secs: 1,
                payload: Bytes::new(),
            },
            tx,
        ));
        let resender = Arc::clone(&linger);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            resender.resent.notify_one();
            tokio::time::sleep(Duration::from_millis(150)).await;
            let _ = done_tx.send((0, Bytes::from_static(&[0; 8])));
        });
        let result = await_notify(&linger, done_rx, Duration::from_millis(200))
            .await
            .expect("the re-send restarted the bound");
        assert!(result.acks.is_empty() && !result.timed_out);
    }

    #[tokio::test]
    async fn linger_ops_bypass_the_throttle() {
        use super::SubmitKind;
        use crate::osdclient::types::OSDOp;
        use std::time::Duration;

        let client = offline_client_with(OSDClientConfig {
            max_inflight_ops: 1,
            ..OSDClientConfig::default()
        })
        .await;
        let ops = [OSDOp::watch_ping(1001, 0)];
        let held = client
            .acquire_budget(&ops, SubmitKind::Op)
            .await
            .expect("the one slot");
        assert!(held.is_some());
        let blocked = tokio::time::timeout(
            Duration::from_millis(100),
            client.acquire_budget(&ops, SubmitKind::Op),
        )
        .await;
        assert!(blocked.is_err(), "an ordinary op waits for the slot");
        let linger = tokio::time::timeout(
            Duration::from_millis(100),
            client.acquire_budget(&ops, SubmitKind::Linger),
        )
        .await
        .expect("a linger op does not wait")
        .expect("no error");
        assert!(linger.is_none());
    }

    #[test]
    fn a_ping_ignores_every_pause_and_a_registration_does_not() {
        use super::OpClass;

        let mut map = crate::osdclient::osdmap::OSDMap::new();
        map.flags = 1 << 3; // CEPH_OSDMAP_PAUSEWR
        // A registration or reconnect: WATCH is a write.
        let register = OpClass {
            is_write: true,
            is_read: false,
            respects_full: true,
            ping: false,
        };
        assert_eq!(register.held_by(&map, 1), (false, true, false));
        let ping = OpClass {
            ping: true,
            ..register
        };
        assert_eq!(ping.held_by(&map, 1), (false, false, false));

        map.flags = 1 << 2; // CEPH_OSDMAP_PAUSERD
        let notify = OpClass {
            is_write: false,
            is_read: true,
            respects_full: true,
            ping: false,
        };
        assert_eq!(notify.held_by(&map, 1), (true, false, false));
    }

    #[tokio::test]
    async fn a_dropped_op_is_cancelled_not_internal() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        drop(tx);
        let err = super::await_op_result(rx, std::time::Duration::from_secs(1))
            .await
            .expect_err("dropped");
        assert!(matches!(err, OSDClientError::Cancelled), "{err:?}");
    }

    /// Stand in for a linger send's submit on `client`: tid 77 goes to a
    /// session to OSD 0, and before the submit returns, `later` begins
    /// the next send of its linger. Returns the session, and the deadline
    /// at which the Tracker would time tid 77 out.
    async fn fake_linger_submit(
        client: &Arc<OSDClient>,
        later: Option<Arc<crate::osdclient::watch::Linger>>,
    ) -> (
        Arc<crate::osdclient::session::OSDSession>,
        tokio::time::Instant,
    ) {
        let (session, rx) = session_with_op(&ObjectId::new(2, "o"), 8, false);
        let session = Arc::new(session);
        client
            .sessions
            .write()
            .await
            .insert(0, Arc::clone(&session));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(200);
        client.tracker.track(77, 0, deadline).await;
        let mut rx = Some(rx);
        *client.fake_linger_submit.lock().unwrap() = Some(Box::new(move || {
            if let Some(later) = &later {
                later.begin_send();
            }
            let map = Arc::new(crate::osdclient::osdmap::OSDMap::new());
            (0, 77, map, rx.take().expect("one submit"))
        }));
        (session, deadline)
    }

    /// Whether the Tracker still times tid 77 on OSD 0 out at `deadline`:
    /// a stand-in op with that tid, past the deadline, is still pending.
    async fn tid_77_untracked(client: &Arc<OSDClient>, deadline: tokio::time::Instant) -> bool {
        let (probe, mut rx) = session_with_op(&ObjectId::new(2, "o"), 8, false);
        let probe = Arc::new(probe);
        client.sessions.write().await.insert(0, Arc::clone(&probe));
        tokio::time::sleep_until(deadline + std::time::Duration::from_millis(200)).await;
        rx.try_recv().is_err() && probe.remove_pending_op(77).is_some()
    }

    #[tokio::test]
    async fn a_send_a_later_one_superseded_cancels_itself() {
        use crate::osdclient::types::{OSDOp, WatchOp};

        let client = offline_client().await;
        let (linger, _events) = test_watch(&client, 1002, false);
        let (session, deadline) = fake_linger_submit(&client, Some(Arc::clone(&linger))).await;
        let seq = linger.begin_send();
        let outcome = client
            .send_linger_op(&linger, OSDOp::watch(1002, WatchOp::Watch, 0), "watch", seq)
            .await;
        assert!(
            matches!(outcome, Err(OSDClientError::Cancelled)),
            "{outcome:?}"
        );
        assert!(
            session.remove_pending_op(77).is_none(),
            "it cancelled its own tid"
        );
        {
            let state = linger.lock_state();
            assert_eq!(state.send_seq, seq + 1);
            assert!(state.sending, "the later send's state is left alone");
            assert!(!state.resend_pending);
            assert_eq!(state.in_flight, None);
        }
        assert!(tid_77_untracked(&client, deadline).await);
    }

    #[tokio::test]
    async fn a_cancelled_current_send_is_left_for_a_resend() {
        use crate::osdclient::types::{OSDOp, WatchOp};

        let client = offline_client().await;
        let (linger, _events) = test_watch(&client, 1002, false);
        let (session, deadline) = fake_linger_submit(&client, None).await;
        let seq = linger.begin_send();
        let send = {
            let client = Arc::clone(&client);
            let linger = Arc::clone(&linger);
            tokio::spawn(async move {
                let op = OSDOp::watch(1002, WatchOp::Watch, 0);
                client.send_linger_op(&linger, op, "watch", seq).await
            })
        };
        for _ in 0..200 {
            if linger.lock_state().in_flight.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(linger.lock_state().in_flight, Some((0, 77)));

        // As a rescan cancels a pending linger send it would resend.
        client.cancel_linger_send(0, 77).await;
        let outcome = send.await.expect("the send ended");
        assert!(
            matches!(outcome, Err(OSDClientError::Cancelled)),
            "{outcome:?}"
        );
        assert!(session.remove_pending_op(77).is_none());
        {
            let state = linger.lock_state();
            assert!(state.resend_pending && !state.sending);
            assert_eq!(state.in_flight, None);
        }
        assert!(
            tid_77_untracked(&client, deadline).await,
            "the cancel untracked it"
        );
    }

    #[tokio::test]
    async fn an_unanswered_send_is_resent_on_the_next_tick() {
        // No OSDMap: every send fails as a connection error, before an OSD.
        let client = offline_client().await;
        let (pending, _rx) = test_watch(&client, 1002, false);
        let (reg_tx, _reg_rx) = tokio::sync::oneshot::channel();
        pending.lock_state().registration = Some(reg_tx);
        client.send_register(&pending).await;
        {
            let state = pending.lock_state();
            assert_eq!(state.send_seq, 1);
            assert!(state.resend_pending && !state.sending);
        }

        client.tick_lingers();
        for _ in 0..100 {
            if pending.lock_state().send_seq == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let state = pending.lock_state();
        assert_eq!(state.send_seq, 2, "the tick re-sent the registration");
        assert!(state.resend_pending, "still no answer, still pending");
    }

    #[tokio::test]
    async fn a_deleted_pool_fails_a_pending_registration() {
        let client = offline_client().await;
        let (pending, _rx) = test_watch(&client, 1002, false);
        let (reg_tx, reg_rx) = tokio::sync::oneshot::channel();
        {
            let mut state = pending.lock_state();
            state.registration = Some(reg_tx);
            state.interval = Some(crate::osdclient::watch::PgInterval {
                up: vec![0],
                up_primary: 0,
                acting: vec![0],
                acting_primary: 0,
                size: 1,
                min_size: 1,
                pg_num: 1,
                pgp_num: 1,
                pg_num_pending: 1,
                epoch: 1,
            });
        }
        let empty = Arc::new(crate::osdclient::osdmap::OSDMap::new());
        client.scan_lingers_on_map_change(std::slice::from_ref(&empty), false);
        let outcome = reg_rx.await.expect("answered");
        assert!(
            matches!(outcome, Err(OSDClientError::PoolNotFound(1))),
            "{outcome:?}"
        );
        assert!(client.lingers.is_empty());
    }

    #[tokio::test]
    async fn no_linger_starts_after_shutdown() {
        let client = offline_client().await;
        client.shutdown().await;
        let refused = client.linger_watch(ObjectId::new(1, "o"), 0).await;
        assert!(
            matches!(refused, Err(OSDClientError::Connection(_))),
            "{:?}",
            refused.err()
        );
        let err = client
            .notify(ObjectId::new(1, "o"), Bytes::new(), 0)
            .await
            .expect_err("shut down");
        assert!(matches!(err, OSDClientError::Connection(_)), "{err:?}");
        assert!(client.lingers.is_empty());
    }

    fn test_notify(
        cookie: u64,
    ) -> (
        Arc<crate::osdclient::watch::Linger>,
        tokio::sync::oneshot::Receiver<(i32, Bytes)>,
    ) {
        use crate::osdclient::watch::{Linger, LingerKind};
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let linger = Arc::new(Linger::new(
            cookie,
            ObjectId::new(1, "o"),
            LingerKind::Notify {
                completion: std::sync::Mutex::new(Some(done_tx)),
                notify_id: std::sync::atomic::AtomicU64::new(0),
                timeout_secs: 10,
                payload: Bytes::new(),
            },
            tx,
        ));
        (linger, done_rx)
    }

    fn notify_reply(id: u64) -> crate::osdclient::types::OpResult {
        crate::osdclient::types::OpResult {
            result: 0,
            version: 0,
            ops: vec![crate::osdclient::types::OpReply {
                return_code: 0,
                outdata: Bytes::copy_from_slice(&id.to_le_bytes()),
            }],
            redirect: None,
        }
    }

    #[test]
    fn a_superseded_notify_send_changes_nothing() {
        use crate::osdclient::watch::LingerKind;
        let (linger, mut done_rx) = test_notify(1001);
        let first = linger.begin_send();
        let second = linger.begin_send();
        let LingerKind::Notify { notify_id, .. } = &linger.kind else {
            unreachable!()
        };

        // The first send's late reply records no id; its Tracker timeout
        // does not fail the notify the second send carries.
        OSDClient::finish_notify_send(&linger, first, Ok(notify_reply(5)));
        assert_eq!(notify_id.load(std::sync::atomic::Ordering::Acquire), 0);
        OSDClient::finish_notify_send(
            &linger,
            first,
            Err(OSDClientError::Timeout(std::time::Duration::from_secs(30))),
        );
        assert!(done_rx.try_recv().is_err(), "still pending");

        OSDClient::finish_notify_send(&linger, second, Ok(notify_reply(6)));
        assert_eq!(notify_id.load(std::sync::atomic::Ordering::Acquire), 6);
    }

    #[test]
    fn a_superseded_registration_send_changes_nothing() {
        use crate::osdclient::watch::{Linger, LingerKind};
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        let linger = Linger::new(
            1001,
            ObjectId::new(1, "o"),
            LingerKind::Watch { timeout: 0 },
            tx,
        );
        let (reg_tx, mut reg_rx) = tokio::sync::oneshot::channel();
        linger.lock_state().registration = Some(reg_tx);
        let first = linger.begin_send();
        let second = linger.begin_send();
        let sent = std::time::Instant::now();

        // The first send's reply came before the second cancelled it.
        OSDClient::finish_register_send(&linger, first, sent, Ok(notify_reply(0)));
        assert!(!linger.lock_state().registered);
        OSDClient::finish_register_send(
            &linger,
            first,
            sent,
            Err(OSDClientError::OSDError {
                code: crate::osdclient::error::ENOENT,
                message: "watch".into(),
            }),
        );
        assert!(reg_rx.try_recv().is_err(), "still pending");
        {
            let state = linger.lock_state();
            assert!(!state.registered);
            assert_eq!(state.watch_valid_thru, None);
        }

        OSDClient::finish_register_send(&linger, second, sent, Ok(notify_reply(0)));
        assert!(linger.lock_state().registered);
        assert!(matches!(reg_rx.try_recv(), Ok(Ok(()))));
    }

    #[test]
    fn the_latest_registration_send_fails_the_watch() {
        use crate::osdclient::watch::{Linger, LingerKind};
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        let linger = Linger::new(
            1001,
            ObjectId::new(1, "o"),
            LingerKind::Watch { timeout: 0 },
            tx,
        );
        let (reg_tx, mut reg_rx) = tokio::sync::oneshot::channel();
        linger.lock_state().registration = Some(reg_tx);
        let seq = linger.begin_send();
        OSDClient::finish_register_send(
            &linger,
            seq,
            std::time::Instant::now(),
            Err(OSDClientError::OSDError {
                code: crate::osdclient::error::ENOENT,
                message: "watch".into(),
            }),
        );
        assert!(matches!(
            reg_rx.try_recv(),
            Ok(Err(OSDClientError::OSDError { .. }))
        ));
        assert!(!linger.lock_state().registered);
    }

    #[test]
    fn the_latest_notify_send_fails_the_notify() {
        let (linger, mut done_rx) = test_notify(1001);
        let seq = linger.begin_send();
        OSDClient::finish_notify_send(
            &linger,
            seq,
            Err(OSDClientError::OSDError {
                code: crate::osdclient::error::ENOENT,
                message: "notify".into(),
            }),
        );
        assert_eq!(
            done_rx.try_recv(),
            Ok((crate::osdclient::error::ENOENT, Bytes::new()))
        );
    }

    #[tokio::test]
    async fn a_notify_ends_with_its_completion_not_its_first_send() {
        use crate::osdclient::watch::LingerKind;
        use std::time::Duration;

        let client = offline_client().await;
        // Reads are paused: the first send waits out the Tracker's 30 s
        // for a newer map that never comes, as a dropped op waits for its
        // reply.
        let mut map = map_with_pool(1, 8);
        map.flags = 1 << 2; // CEPH_OSDMAP_PAUSERD
        map.epoch = crate::Epoch::new(1);
        client.osdmap_tx.send(Some(Arc::new(map))).ok();

        // A re-send's completion arrives while the first send is held.
        let completer = Arc::clone(&client);
        tokio::spawn(async move {
            loop {
                let linger = completer.lingers.iter().next().map(|l| Arc::clone(&l));
                if let Some(linger) = linger
                    && linger.lock_state().sending
                {
                    let LingerKind::Notify { completion, .. } = &linger.kind else {
                        unreachable!()
                    };
                    let tx = completion.lock().unwrap().take().expect("pending");
                    let _ = tx.send((0, Bytes::from_static(&[0; 8])));
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.notify(ObjectId::new(1, "o"), Bytes::new(), 0),
        )
        .await
        .expect("the notify did not wait on its first send")
        .expect("completed");
        assert!(result.acks.is_empty() && !result.timed_out);
        assert!(client.lingers.is_empty());
    }

    #[tokio::test]
    async fn a_dropped_notify_leaves_its_first_send_running() {
        use std::time::Duration;

        let client = offline_client().await;
        // Reads are paused: the first send is held before any session.
        let mut map = map_with_pool(1, 8);
        map.flags = 1 << 2; // CEPH_OSDMAP_PAUSERD
        map.epoch = crate::Epoch::new(1);
        client.osdmap_tx.send(Some(Arc::new(map))).ok();

        let watcher = Arc::clone(&client);
        let linger = tokio::spawn(async move {
            loop {
                let linger = watcher.lingers.iter().next().map(|l| Arc::clone(&l));
                if let Some(linger) = linger
                    && linger.lock_state().sending
                {
                    return linger;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        // The caller gives up while the send is held, dropping notify().
        let caller = tokio::time::timeout(
            Duration::from_millis(300),
            client.notify(ObjectId::new(1, "o"), Bytes::new(), 0),
        )
        .await;
        assert!(caller.is_err(), "the notify was still waiting");
        let linger = linger.await.expect("the send began");
        assert!(client.lingers.is_empty(), "the caller's guard forgot it");

        // Unpaused, the send runs on to its end, where send_linger_op
        // settles it; dropped with the caller, it would stay mid-send, as
        // it would inside submit_op with an op pending and untracked.
        let mut map = map_with_pool(1, 8);
        map.epoch = crate::Epoch::new(2);
        client.osdmap_tx.send(Some(Arc::new(map))).ok();
        for _ in 0..200 {
            if !linger.lock_state().sending {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let state = linger.lock_state();
        assert!(!state.sending, "the first send ran to its end");
        assert_eq!(state.send_seq, 1);
        assert_eq!(state.in_flight, None);
    }

    #[tokio::test]
    async fn a_notify_send_that_cancels_itself_leaves_the_bound_alone() {
        use crate::osdclient::types::OSDOp;
        use std::time::Duration;

        let client = offline_client().await;
        let (linger, _done_rx) = test_notify(1001);
        fake_linger_submit(&client, Some(Arc::clone(&linger))).await;
        let seq = linger.begin_send();
        let outcome = client
            .send_linger_op(
                &linger,
                OSDOp::notify(1001, 10, Bytes::new()),
                "notify",
                seq,
            )
            .await;
        assert!(
            matches!(outcome, Err(OSDClientError::Cancelled)),
            "{outcome:?}"
        );
        let restarted = tokio::time::timeout(Duration::ZERO, linger.resent.notified()).await;
        assert!(
            restarted.is_err(),
            "the superseded send restarted the bound"
        );
    }

    #[tokio::test]
    async fn a_shutdown_ends_a_held_linger_send() {
        use std::time::Duration;

        let client = offline_client().await;
        // Reads are paused: every send of the notify is held for the
        // Tracker's 30 s before any session.
        let mut map = map_with_pool(1, 8);
        map.flags = 1 << 2; // CEPH_OSDMAP_PAUSERD
        map.epoch = crate::Epoch::new(1);
        client.osdmap_tx.send(Some(Arc::new(map))).ok();

        let caller = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.notify(ObjectId::new(1, "o"), Bytes::new(), 0).await })
        };
        let linger = loop {
            let linger = client.lingers.iter().next().map(|l| Arc::clone(&l));
            if let Some(linger) = linger
                && linger.lock_state().sending
            {
                break linger;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        // A re-send, held as the first send is.
        client.spawn_relinger(Arc::clone(&linger));
        while linger.lock_state().send_seq < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        client.shutdown().await;
        let outcome = tokio::time::timeout(Duration::from_secs(5), caller)
            .await
            .expect("the shutdown ended the notify")
            .expect("the caller ran");
        assert!(outcome.is_err(), "{outcome:?}");
        // Each held send's task holds the linger until it ends.
        for _ in 0..200 {
            if Arc::strong_count(&linger) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            Arc::strong_count(&linger),
            1,
            "a send outlived the shutdown"
        );
    }

    /// Wait until only the caller holds `linger`: every task that took
    /// it has ended.
    async fn only_caller_holds(linger: &Arc<crate::osdclient::watch::Linger>) -> bool {
        for _ in 0..200 {
            if Arc::strong_count(linger) == 1 {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
    }

    /// A registered watch on pool 2's `o`, last sent to OSD 0.
    fn watch_on_osd_0(client: &Arc<OSDClient>) -> Arc<crate::osdclient::watch::Linger> {
        use crate::osdclient::watch::{Linger, LingerKind};
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let linger = Arc::new(Linger::new(
            1001,
            ObjectId::new(2, "o"),
            LingerKind::Watch { timeout: 0 },
            tx,
        ));
        {
            let mut state = linger.lock_state();
            state.registered = true;
            state.osd = Some(0);
        }
        client.lingers.insert(1001, Arc::clone(&linger));
        linger
    }

    #[tokio::test]
    async fn a_shutdown_ends_a_ping_in_flight() {
        let client = offline_client().await;
        client.osdmap_tx.send(Some(one_osd_map(8, 10))).ok();
        let (_session, mut sends) = fake_osd(&client).await;
        let linger = watch_on_osd_0(&client);

        // The ping goes out and its reply never comes: its task waits
        // out the Tracker's 30 s.
        client.tick_lingers();
        tokio::time::timeout(std::time::Duration::from_secs(5), sends.recv())
            .await
            .expect("the ping went out");

        client.shutdown().await;
        assert!(
            only_caller_holds(&linger).await,
            "the ping outlived the shutdown"
        );
    }

    #[tokio::test]
    async fn a_shutdown_ends_a_linger_send_a_reset_began() {
        let client = offline_client().await;
        // Writes are paused: the reconnect a reset sends, a write, is held
        // for the Tracker's 30 s before any session.
        let mut map = (*one_osd_map(8, 10)).clone();
        map.flags = 1 << 3; // CEPH_OSDMAP_PAUSEWR
        client.osdmap_tx.send(Some(Arc::new(map))).ok();
        let linger = watch_on_osd_0(&client);

        assert!(client.on_session_reset(0));
        while linger.lock_state().send_seq == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(linger.lock_state().sending, "the reconnect is held");

        client.shutdown().await;
        assert!(
            only_caller_holds(&linger).await,
            "the re-send outlived the shutdown"
        );
    }

    #[tokio::test]
    async fn an_unreachable_notify_times_out_at_its_bound() {
        use super::await_notify;
        use crate::osdclient::watch::{Linger, LingerKind};
        use std::time::{Duration, Instant};

        // No OSDMap: every send fails before reaching a session.
        let client = offline_client().await;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let linger = Arc::new(Linger::new(
            1001,
            ObjectId::new(1, "o"),
            LingerKind::Notify {
                completion: std::sync::Mutex::new(Some(done_tx)),
                notify_id: std::sync::atomic::AtomicU64::new(0),
                timeout_secs: 1,
                payload: Bytes::new(),
            },
            tx,
        ));
        client.lingers.insert(1001, Arc::clone(&linger));
        // Re-send far faster than the bound, as ticks would.
        let resender = Arc::clone(&client);
        let resent = Arc::clone(&linger);
        let resends = tokio::spawn(async move {
            loop {
                resender.send_notify(&resent).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        let started = Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            await_notify(&linger, done_rx, Duration::from_millis(300)),
        )
        .await
        .expect("the bound expired despite the re-sends");
        resends.abort();
        assert!(
            matches!(outcome, Err(OSDClientError::Timeout(_))),
            "{outcome:?}"
        );
        eprintln!("unreachable notify timed out after {:?}", started.elapsed());
    }
}
