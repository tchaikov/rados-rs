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
        }
    }
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
}

/// An `MOSDOp` built by `prepare_op`, carried across `route_and_submit`
/// calls with the map it was last routed against.
struct PreparedOp {
    msg: Arc<MOSDOp>,
    osdmap: Arc<crate::osdclient::osdmap::OSDMap>,
    class: OpClass,
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
    /// an ordinary op is.
    Linger,
    /// A watch ping: unthrottled and held by nothing ([`OpClass::ping`]).
    Ping,
}

/// One send by `route_and_submit`.
struct Submitted {
    osd: i32,
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
    let (code, data) = loop {
        tokio::select! {
            done = &mut done_rx => {
                break done
                    .map_err(|_| OSDClientError::Internal("notify completion dropped".into()))?;
            }
            () = linger.resent.notified() => {}
            () = tokio::time::sleep(bound) => return Err(OSDClientError::Timeout(bound)),
        }
    };
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
        tokio::spawn(Arc::clone(self).relinger_after_reset(osd_id));
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
        let (_, osds) = self.object_to_osds_in_map(&osdmap, object.pool, &object.oid)?;
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
        let pg = Self::object_pg_in_map(osdmap, object.pool, &object.oid)?;
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
        if let Ok(osdmap) = self.get_osdmap().await {
            self.record_target(linger, &osdmap, None);
        }
        let outcome = async {
            let (osd, osdmap, rx, _permit) = self
                .submit_once_in_map(
                    &linger.object,
                    vec![op],
                    crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
                    OsdOpFlags::READ,
                    SubmitKind::Linger,
                )
                .await
                .map_err(|e| match e {
                    // Held by a pause until the deadline: never sent.
                    OSDClientError::Timeout(_) => {
                        OSDClientError::Connection(format!("not sent: {e}"))
                    }
                    e => e,
                })?;
            self.record_target(linger, &osdmap, Some(osd));
            if matches!(linger.kind, LingerKind::Notify { .. }) {
                // Only a send a session took restarts the notify's bound;
                // a re-send that never got out must not extend it.
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
            }
        }
        outcome
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

    /// Send a watch's registration, `WATCH{WATCH}`, once. Success marks it
    /// registered and answers `linger_watch`; an OSD's rejection answers
    /// it with the error; no answer leaves it pending for a re-send.
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
        match self.send_linger_op(linger, op, "watch", seq).await {
            Ok(_) => {
                {
                    let mut state = linger.lock_state();
                    state.registered = true;
                    state.last_error = None;
                    state.watch_valid_thru = Some(sent);
                }
                linger.finish_registration(Ok(()));
            }
            Err(e) if is_no_answer(&e) => {
                debug!(
                    "watch {} registration got no answer ({}); the next tick re-sends it",
                    linger.cookie, e
                );
            }
            Err(e) => linger.finish_registration(Err(e)),
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
    /// and record the notify id its reply carries unless a later send or
    /// the completion superseded it. An OSD's rejection completes the
    /// notify with the error, as Objecter's `_linger_commit` does; no
    /// answer leaves it for the next reset or map change to re-send.
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
        let completed = || {
            completion
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        };
        if completed() {
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
        match self.send_linger_op(linger, op, "notify", seq).await {
            Ok(result) => {
                let id = result
                    .ops
                    .first()
                    .and_then(|r| r.outdata.get(..8))
                    .map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes")));
                let state = linger.lock_state();
                if state.send_seq == seq
                    && !completed()
                    && let Some(id) = id
                {
                    notify_id.store(id, Ordering::Release);
                }
            }
            Err(e) if is_no_answer(&e) => {
                debug!(
                    "notify {} got no answer ({}); the next tick re-sends it",
                    linger.cookie, e
                );
            }
            Err(e) => {
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
        self.send_notify(&linger).await;
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
            tokio::spawn(Arc::clone(self).ping_watch(linger, generation));
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
        let (_, _, rx, _) = self
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
                    tokio::spawn(client.send_reconnect(linger));
                } else {
                    tokio::spawn(async move { client.send_register(&linger).await });
                }
            }
            LingerKind::Notify { .. } => {
                tokio::spawn(client.resend_notify(linger));
            }
        }
    }

    /// Get the current OSDMap
    pub async fn get_osdmap(&self) -> Result<Arc<crate::osdclient::osdmap::OSDMap>> {
        self.osdmap_rx
            .borrow()
            .clone()
            .ok_or_else(|| OSDClientError::Connection("OSDMap not available".to_string()))
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

    /// The PG an object maps to: every op and every linger's interval is
    /// placed through here.
    fn object_pg_in_map(
        osdmap: &crate::osdclient::osdmap::OSDMap,
        pool: u64,
        oid: &str,
    ) -> Result<crate::crush::placement::PgId> {
        let pool_info = osdmap
            .pools
            .get(&pool)
            .ok_or(OSDClientError::PoolNotFound(pool))?;
        let locator = ObjectLocator::new(pool);
        crate::crush::placement::object_to_pg(oid, &locator, pool_info.pg_num)
            .map_err(|e| OSDClientError::Crush(format!("Object->PG mapping failed: {e}")))
    }

    fn object_to_osds_in_map(
        &self,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        pool: u64,
        oid: &str,
    ) -> Result<(StripedPgId, Vec<i32>)> {
        debug!(
            "OSD weights from map (max_osd={}): {:?}",
            osdmap.max_osd, osdmap.osd_weight
        );

        let pg = Self::object_pg_in_map(osdmap, pool, oid)?;

        let osds = Self::pg_to_osds_in_map(osdmap, pg)?;

        // For EC pools the wire spg_t carries a per-PG shard index.  For
        // replicated pools `pg_to_spg_shard` returns NO_SHARD (-1), so
        // this is correct in both cases.  Optimized EC pools currently
        // return an error from this call rather than mis-route silently.
        let shard = osdmap
            .pg_to_spg_shard(&pg)
            .map_err(|e| OSDClientError::Crush(format!("EC shard lookup: {e}")))?;
        let spg = StripedPgId::new(pg.pool, pg.seed, shard.0);
        debug!(
            "Mapped {}/{} to PG {:?}, OSDs: {:?}, shard: {}",
            pool, oid, pg, osds, shard.0
        );

        Ok((spg, osds))
    }

    fn cached_rescan_osds(
        &self,
        cache: &mut HashMap<(u64, String), (StripedPgId, Vec<i32>)>,
        osdmap: &crate::osdclient::osdmap::OSDMap,
        pool_id: u64,
        object_id: &str,
    ) -> Result<(StripedPgId, Vec<i32>)> {
        let key = (pool_id, object_id.to_owned());
        if let Some(entry) = cache.get(&key) {
            return Ok(entry.clone());
        }

        let entry = self.object_to_osds_in_map(osdmap, pool_id, object_id)?;
        cache.insert(key, entry.clone());
        Ok(entry)
    }

    /// Map a PG to its acting OSD set, applying CRUSH placement and all overrides.
    /// Returns `NoOSDs` if the acting set is empty.
    ///
    /// Shared helper used by both `object_to_osds_in_map` and `query_pg_objects`.
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
    async fn execute_op(
        &self,
        mut object: ObjectId,
        ops: Vec<OSDOp>,
        timeout: Option<std::time::Duration>,
        priority: i32,
        extra_flags: crate::osdclient::types::OsdOpFlags,
    ) -> Result<crate::osdclient::types::OpResult> {
        // Fail immediately if the client has been fenced by the cluster.
        if self.blocklisted.load(Ordering::Relaxed) {
            return Err(OSDClientError::Blocklisted);
        }

        object.calculate_hash();

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
        // session and resubmit, but only a bounded number of times.
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
                    // session and we'll re-encode and resubmit.
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
                    // Don't bump retry_attempt — the OSD never saw this send.
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
                    // Don't bump retry_attempt: the next loop iteration
                    // allocates a fresh tid and submit_op builds a new
                    // PendingOp with attempts=1, so the OSD's echoed
                    // retry_attempt must stay at 0 to satisfy
                    // validate_reply_freshness.
                    Arc::make_mut(&mut op.msg).retry_attempt = 0;
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
                Arc::make_mut(&mut op.msg).retry_attempt += 1;
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
                m.object.calculate_hash();
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

            // Map to OSDs based on current object (using the osdmap we already have)
            let (spg, osds) =
                self.object_to_osds_in_map(&op.osdmap, msg.object.pool, &msg.object.oid)?;
            let primary_osd = osds[0];
            tracing::trace!(
                target: "rados::osdclient::routing",
                "routing epoch={} pool={} oid={} spg={:?} osds={:?} hash=0x{:08x}",
                op.osdmap.epoch.as_u32(),
                msg.object.pool,
                msg.object.oid,
                spg,
                osds,
                msg.object.hash,
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

            // Final pgid re-derivation: between the post-session re-check above
            // and submit_op below, the OSDMap can still advance (e.g. autoscaler
            // growing pg_num), which invalidates the seed we just computed.
            // Re-read the live map once more and recompute spg if the epoch has
            // moved. If the primary also changed, restart the outer loop so we
            // pick up the correct session; otherwise just refresh the pgid
            // before stamping it. Mirrors librados Objecter's rwlock-guarded
            // "compute target + enqueue" ordering without needing a real lock.
            let (final_spg, final_map) = {
                let live = self.get_osdmap().await?;
                if live.epoch != op.osdmap.epoch {
                    // OSDMap moved between the routing decision above
                    // and now — re-prune the snapc against any new
                    // removals before we encode the wire op.
                    live.prune_snap_context(msg.object.pool, &mut Arc::make_mut(msg).snaps);
                    let (new_spg, new_osds) =
                        self.object_to_osds_in_map(&live, msg.object.pool, &msg.object.oid)?;
                    if new_osds.first().copied() != Some(primary_osd) {
                        op.osdmap = live;
                        continue;
                    }
                    (new_spg, live)
                } else {
                    (spg, Arc::clone(&op.osdmap))
                }
            };
            let final_epoch = final_map.epoch.as_u32();

            // Build request ID with fresh TID and stamp pgid
            let tid = session.next_tid();
            {
                let m = Arc::make_mut(msg);
                m.pgid = final_spg;
                m.osdmap_epoch = final_epoch;
                m.reqid = crate::osdclient::types::RequestId::new(
                    &self.entity_name,
                    tid,
                    self.config.client_inc as i32,
                );
            }

            // Submit operation (priority is set in message header).
            // Arc::clone is a cheap refcount bump; submit_op stores the Arc in
            // pending_ops and drops it when the reply arrives, so by the time
            // we reach the next iteration the refcount is back to 1.
            let rx = session.submit_op(Arc::clone(msg), priority).await;
            return Ok(Submitted {
                osd: primary_osd,
                osdmap: final_map,
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
        let (osd, osdmap, rx, permit) = self
            .submit_once_in_map(object, ops, priority, extra_flags, SubmitKind::Op)
            .await?;
        let permit = permit.ok_or_else(|| OSDClientError::Internal("op not budgeted".into()))?;
        Ok((osd, osdmap.epoch.as_u32(), rx, permit))
    }

    /// [`Self::submit_once`], returning the map the op was routed in; a
    /// [`SubmitKind::Linger`] or [`SubmitKind::Ping`] op takes no throttle
    /// permit.
    async fn submit_once_in_map(
        &self,
        object: &ObjectId,
        ops: Vec<OSDOp>,
        priority: i32,
        extra_flags: OsdOpFlags,
        kind: SubmitKind,
    ) -> Result<(
        i32,
        Arc<crate::osdclient::osdmap::OSDMap>,
        tokio::sync::oneshot::Receiver<Result<crate::osdclient::types::OpResult>>,
        Option<crate::osdclient::throttle::ThrottlePermit<'_>>,
    )> {
        if self.blocklisted.load(Ordering::Relaxed) {
            return Err(OSDClientError::Blocklisted);
        }
        let mut object = object.clone();
        object.calculate_hash();
        let permit = self.acquire_budget(&ops, kind).await?;
        let effective_timeout = self.tracker.operation_timeout();
        let deadline = std::time::Instant::now() + effective_timeout;
        let mut op = self.prepare_op(object, ops, extra_flags).await?;
        op.class.ping = kind == SubmitKind::Ping;
        let submitted = self
            .route_and_submit(&mut op, priority, deadline, effective_timeout)
            .await?;
        Ok((submitted.osd, submitted.osdmap, submitted.rx?, permit))
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

    /// Compute the PG id that an object hash belongs to.
    ///
    /// This mirrors Ceph's `ceph_stable_mod(hash, pg_num, pg_num_mask)`:
    /// - If `(hash & mask) < pg_num`  →  `hash & mask`
    /// - Otherwise                    →  `hash & (mask >> 1)`
    ///
    /// `pg_num_mask = (1 << ceil_log2(pg_num)) - 1`; for power-of-2 pg_num it equals
    /// `pg_num - 1`.
    fn hash_to_pg(hash: u32, pg_num: u32) -> u32 {
        if pg_num == 0 {
            return 0;
        }
        // pg_num_mask = (1 << cbits(pg_num - 1)) - 1
        let mask: u32 = if pg_num <= 1 {
            0
        } else {
            (1u32 << (32 - (pg_num - 1).leading_zeros())) - 1
        };
        if (hash & mask) < pg_num {
            hash & mask
        } else {
            hash & (mask >> 1)
        }
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

    /// Query objects from the PG that contains `hobject_cursor`.
    ///
    /// The target PG is derived from `hobject_cursor.hash` using `ceph_stable_mod`, matching
    /// exactly how Ceph's Objecter routes PGNLS requests.  Using the cursor hash as the
    /// object hash in the MOSDOp satisfies the OSD's `pgid.contains(head)` assertion.
    ///
    /// Returns the decoded response and result code from the OSD.
    async fn query_pg_objects(
        &self,
        pool: u64,
        hobject_cursor: &crate::HObject,
        max_entries: u64,
        pool_info: &crate::osdclient::PgPool,
        osdmap: &Arc<crate::osdclient::osdmap::OSDMap>,
    ) -> Result<(crate::osdclient::PgNlsResponse, i32)> {
        // Derive the target PG from the cursor hash — mirrors Objecter::pg_read(current_pg,…)
        let current_pg = Self::hash_to_pg(hobject_cursor.hash, pool_info.pg_num);
        let pg = crate::crush::placement::PgId {
            pool,
            seed: current_pg,
        };

        let osds = Self::pg_to_osds_in_map(osdmap, pg)?;
        let primary_osd = osds[0];

        // For replicated pools `pg_to_spg_shard` returns NO_SHARD; for
        // non-optimised EC it returns the primary's position; for
        // optimised EC it runs `pgtemp_undo_primaryfirst`.  Sending a
        // PGNLS scan with the wrong shard would land on a replica that
        // rejects it as misdirected, so EC pool listings need this just
        // as much as the I/O hot path.
        let shard = osdmap
            .pg_to_spg_shard(&pg)
            .map_err(|e| OSDClientError::Crush(format!("EC shard lookup: {e}")))?;
        let spg = StripedPgId::new(pool, current_pg, shard.0);

        // Get session
        let session = self.get_or_create_session(primary_osd).await?;

        // Create object ID (empty for PGNLS).
        // The hash must match the cursor's hash so the OSD's pgid.contains(head)
        // check passes — hash=0 only belongs to PG 0.
        let mut object = ObjectId::new(pool, "");
        object.hash = hobject_cursor.hash;

        // Create pgls operation
        let ops = vec![OSDOp::pgls(
            max_entries,
            hobject_cursor.clone(),
            osdmap.epoch.as_u32(),
        )?];

        // Acquire throttle permit
        let _throttle_permit = self.throttle.acquire(calc_op_budget(&ops)).await?;

        // Build request ID
        let tid = session.next_tid();
        let reqid = crate::osdclient::types::RequestId::new(
            &self.entity_name,
            tid,
            self.config.client_inc as i32,
        );

        // Build message
        let flags = MOSDOp::calculate_flags(&ops);
        let msg = MOSDOp::new(
            self.config.client_inc,
            osdmap.epoch.as_u32(),
            flags,
            object,
            spg,
            ops,
            reqid,
            self.global_id,
        );

        // Submit operation (use default priority for internal PGLS operations)
        let result_rx = session
            .submit_op(
                Arc::new(msg),
                crate::osdclient::messages::CEPH_MSG_PRIO_DEFAULT,
            )
            .await?;

        // Wait for result with timeout
        let result = await_op_result(result_rx, self.tracker.operation_timeout()).await?;

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

    /// List objects in a pool
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
        debug!(
            "list pool={} cursor={:?} max_entries={}",
            pool, cursor, max_entries
        );

        // Fail immediately if the client has been fenced by the cluster.
        if self.blocklisted.load(Ordering::Relaxed) {
            return Err(OSDClientError::Blocklisted);
        }

        // Parse cursor → starting hobject position
        let mut hobject_cursor = Self::parse_list_cursor(pool, cursor)?;

        // Get OSDMap to look up pool info
        let osdmap = self.get_osdmap().await?;
        let pool_info = osdmap
            .pools
            .get(&pool)
            .ok_or(OSDClientError::PoolNotFound(pool))?;

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

            let current_pg = Self::hash_to_pg(hobject_cursor.hash, pool_info.pg_num);
            debug!(
                "Querying PG {} (hash={:#x}), collected {} entries so far",
                current_pg,
                hobject_cursor.hash,
                all_entries.len()
            );

            let remaining = max_entries.saturating_sub(all_entries.len() as u64);
            let (response, _result_code) = match self
                .query_pg_objects(pool, &hobject_cursor, remaining, pool_info, &osdmap)
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

    /// Compute the end-of-PG hobject handle for a given PG, matching `pg_t::get_hobj_end`.
    ///
    /// This is used to skip over a PG that has no OSDs so we can advance to the
    /// next PG in bitwise-sorted order without issuing an OSD request.
    fn pg_hobj_end(pool: u64, pg_seed: u32, pg_num: u32) -> crate::HObject {
        let bits = if pg_num <= 1 {
            0u32
        } else {
            32 - (pg_num - 1).leading_zeros()
        };
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

    /// Scan pending ops after OSDMap update, resend if target changed
    ///
    /// This implements Ceph's `_scan_requests` pattern from Objecter.
    async fn scan_requests_on_map_change(&self, new_epoch: u32) -> Result<()> {
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
                    .collect_resend_ops(&session, &osdmap, new_epoch, None, &mut need_resend)
                    .await;
                continue;
            }

            // Scan ops: only re-target those whose primary OSD changed or whose pool
            // bumped last_force_op_resend since this op was sent.
            let any_migrated = self
                .collect_resend_ops(&session, &osdmap, new_epoch, Some(osd_id), &mut need_resend)
                .await?;

            if any_migrated {
                // collect_resend_ops already cancelled the io_loop; close the
                // session fully and drain any remaining ops (those whose primary
                // didn't change but whose connection is being torn down).
                sessions_to_close.push(osd_id);
                let _ = self
                    .collect_resend_ops(&session, &osdmap, new_epoch, None, &mut need_resend)
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
    ///   - `Some(id)` — scan path: only collect ops whose primary OSD changed or
    ///     whose pool's `last_force_op_resend` epoch was bumped since the op was sent.
    ///   - `None`     — drain path: collect every op unconditionally (session is closing).
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
        need_resend: &mut Vec<(i32, crate::osdclient::session::PendingOp)>,
    ) -> Result<bool> {
        let metadata = session.get_pending_ops_metadata();
        let mut placement_cache = HashMap::new();
        let mut any_migrated = false;

        for (tid, pool_id, object_id, op_osdmap_epoch) in metadata {
            if Self::fail_if_pool_deleted(session, tid, pool_id, osdmap).await {
                continue;
            }

            let (new_spg, new_osds) =
                self.cached_rescan_osds(&mut placement_cache, osdmap, pool_id, &object_id)?;
            let new_primary = new_osds.first().copied().unwrap_or(-1);

            // Scan path: only resend if primary changed or pool forced a resend.
            // Drain path (current_osd == None): always resend.
            let should_resend = match current_osd {
                None => true,
                Some(osd_id) => {
                    // Mirrors Objecter::_calc_target step 4: if last_force_op_resend is in
                    // (op_epoch, new_epoch] the op must be resubmitted even if its primary
                    // OSD has not changed.
                    let force_resend = osdmap.pools.get(&pool_id).is_some_and(|p| {
                        let lf = p.canonical_last_force_op_resend().as_u32();
                        lf > op_osdmap_epoch && lf <= new_epoch
                    });
                    new_primary != osd_id || force_resend
                }
            };

            if should_resend && let Some(mut op) = session.remove_pending_op(tid) {
                // Scan path only: cancel the io_loop so any message already
                // encoded for this tid is dropped before reaching the wire.
                // See `cancel_io_loop`'s doc comment for the full rationale.
                if current_osd.is_some() {
                    session.cancel_io_loop();
                    any_migrated = true;
                }
                op.target.update(new_epoch, new_primary, new_osds.clone());
                // Restamp the MOSDOp pgid from the new map.  A pg_num change
                // (autoscaler split, manual resize) shifts the seed even when
                // the primary OSD is unchanged; without this, the migrated op
                // would be re-encoded with the stale seed and rejected by the
                // target OSD (ENXIO in prod, assertion with debug_misdirected).
                {
                    let msg = Arc::make_mut(&mut op.op);
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
            let (new_spg, osds) = match self.object_to_osds_in_map(
                &new_osdmap,
                pending_op.op.object.pool,
                &pending_op.op.object.oid,
            ) {
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
            {
                let msg = Arc::make_mut(&mut pending_op.op);
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

        for (tid, pool_id, _, _) in metadata {
            let Some(mut pending_op) = old_session.remove_pending_op(tid) else {
                continue;
            };

            // Pool deleted — fail immediately.
            if !osdmap.pools.contains_key(&pool_id) {
                let _ = pending_op
                    .result_tx
                    .send(Err(OSDClientError::PoolNotFound(pool_id)));
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
        use crate::msgr2::ceph_message::{CephMessagePayload, CephMsgHeader};

        info!("Handling OSDMap message ({} bytes)", msg.front.len());

        // Decode MOSDMap
        let header = CephMsgHeader::new(MOSDMap::msg_type(), MOSDMap::msg_version(0));
        let mosdmap = MOSDMap::decode_payload(&header, &msg.front, &[], &[])?;
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
            self.apply_sequential_updates(mosdmap, current_epoch, current_map)
        } else {
            // No current map, use latest full map
            self.load_initial_map(mosdmap)
        }
    }

    /// Apply sequential updates to existing map, keeping every applied
    /// epoch's map so the lingers can be checked against each.
    fn apply_sequential_updates(
        &self,
        mosdmap: &crate::monclient::messages::MOSDMap,
        current_epoch: crate::Epoch,
        current_map: Option<Arc<crate::osdclient::osdmap::OSDMap>>,
    ) -> Option<MapBatch> {
        let mut working_map = current_map;
        let mut batch = MapBatch::default();

        for e in (current_epoch.as_u32() + 1)..=mosdmap.get_last() {
            let current_map_epoch = working_map.as_ref().map(|m| m.epoch).unwrap_or_default();
            let contiguous = current_map_epoch == crate::Epoch::new(e - 1);

            if contiguous && mosdmap.incremental_maps.contains_key(&e) {
                // Apply incremental
                if let Some(new_map) = self.apply_incremental_map(mosdmap, e, &working_map) {
                    working_map = Some(Arc::clone(&new_map));
                    batch.maps.push(new_map);
                }
            } else if mosdmap.maps.contains_key(&e) {
                // Use full map
                if let Some(new_map) = self.apply_full_map(mosdmap, e) {
                    // A full map that does not follow the previous epoch
                    // jumps over maps nobody here has seen.
                    batch.skipped |= !contiguous;
                    working_map = Some(Arc::clone(&new_map));
                    batch.maps.push(new_map);
                }
            } else {
                warn!("Missing epoch {} (incremental and full)", e);
                batch.skipped = true;
            }
        }

        (!batch.maps.is_empty()).then_some(batch)
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
            let mut batch = self
                .apply_sequential_updates(mosdmap, full_epoch, Some(full_map.clone()))
                .unwrap_or_default();
            batch.maps.insert(0, full_map);
            Some(batch)
        } else {
            Some(MapBatch {
                maps: vec![full_map],
                skipped: false,
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
            && let Err(e) = self.scan_requests_on_map_change(final_epoch.as_u32()).await
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
            for (tid, _, _, _) in session.get_pending_ops_metadata() {
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
            self.update_osdmap_state(batch).await?;
        }

        Ok(())
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
    use super::{OSDClient, OSDClientConfig};
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
