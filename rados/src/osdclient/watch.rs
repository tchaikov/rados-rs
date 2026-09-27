//! Watch/notify: the notify reply and, from the linger machinery, the
//! watches and notifies this client keeps registered with the OSDs.
//!
//! `LIST_WATCHERS` lives in [`crate::osdclient::watchers`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::Denc;
use crate::osdclient::client::OSDClient;
use crate::osdclient::error::{ENOENT, ENOTCONN, ETIMEDOUT, OSDClientError, Result};
use crate::osdclient::messages::{
    CEPH_WATCH_EVENT_DISCONNECT, CEPH_WATCH_EVENT_NOTIFY, CEPH_WATCH_EVENT_NOTIFY_COMPLETE,
    MWatchNotify,
};
use crate::osdclient::types::ObjectId;

/// What a watch receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    /// A notify on the watched object; ack it with the watcher's
    /// `notify_ack` so the notifier does not wait for its timeout.
    Notify {
        notify_id: u64,
        notifier_gid: u64,
        payload: Bytes,
    },
    /// The watch is in error; `code` is a negative errno (`ENOTCONN` when
    /// the OSD dropped it or the object was deleted, `ETIMEDOUT` when the
    /// OSD holds it disconnected). Sent once per error. The client keeps
    /// reconnecting a watch in error other than `ENOTCONN` every five
    /// seconds, and on session resets and map changes; a successful
    /// reconnect clears the error and events resume. After `ENOTCONN` no
    /// events follow until the caller watches again.
    Disconnect { code: i32 },
}

/// A watch or an in-flight notify: Objecter's `LingerOp`.
pub(crate) struct Linger {
    pub(crate) cookie: u64,
    pub(crate) object: ObjectId,
    pub(crate) kind: LingerKind,
    /// Woken each time a send of a notify reaches a session, to restart
    /// its outer bound.
    pub(crate) resent: tokio::sync::Notify,
    /// Taken when the linger is forgotten, so the watcher's `recv` ends.
    events: std::sync::Mutex<Option<mpsc::UnboundedSender<WatchEvent>>>,
    pub(crate) state: std::sync::Mutex<LingerState>,
}

pub(crate) enum LingerKind {
    /// `timeout` is the watch timeout sent with the registration, in
    /// seconds (0 for the OSD's default).
    Watch { timeout: u32 },
    Notify {
        /// Taken by the first NOTIFY_COMPLETE, so a completion racing a
        /// re-send reaches the caller once.
        completion: std::sync::Mutex<Option<oneshot::Sender<(i32, Bytes)>>>,
        /// 0 until the NOTIFY op's reply carries the id.
        notify_id: AtomicU64,
        /// Kept to re-send the notify whole.
        timeout_secs: u32,
        payload: Bytes,
    },
}

#[derive(Debug, Default)]
pub(crate) struct LingerState {
    pub(crate) osd: Option<i32>,
    pub(crate) registered: bool,
    pub(crate) register_gen: u32,
    pub(crate) last_error: Option<i32>,
    pub(crate) watch_valid_thru: Option<Instant>,
    /// The PG interval of the last send, recorded as it goes out.
    pub(crate) interval: Option<PgInterval>,
    /// Counts sends, so a reply to a superseded send is not recorded.
    pub(crate) send_seq: u64,
    /// A send is out and unanswered; no other send or ping follows it.
    pub(crate) sending: bool,
    /// The OSD and tid of the current send once a session took it, until
    /// it ends; the next send cancels it if it is still pending, as
    /// Objecter's `register_tid`.
    pub(crate) in_flight: Option<(i32, u64)>,
    /// The last send got no answer (a lost session, no reachable OSD);
    /// the next linger tick re-sends it, as Objecter re-sends a linger
    /// op when its session reconnects.
    pub(crate) resend_pending: bool,
    /// Answers `linger_watch` once the registration succeeds or an OSD
    /// rejects it.
    pub(crate) registration: Option<oneshot::Sender<Result<()>>>,
}

/// What a PG interval is made of, as far as a client can see it: a
/// change in any field, or a forced resend after `epoch`, means the OSD
/// has re-peered the PG and rebuilt its watches as disconnected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgInterval {
    pub(crate) up: Vec<i32>,
    pub(crate) up_primary: i32,
    pub(crate) acting: Vec<i32>,
    pub(crate) acting_primary: i32,
    pub(crate) size: u8,
    pub(crate) min_size: u8,
    pub(crate) pg_num: u32,
    pub(crate) pgp_num: u32,
    pub(crate) pg_num_pending: u32,
    /// The map epoch this interval was taken from; not part of it.
    pub(crate) epoch: u32,
}

impl PgInterval {
    /// Whether `now`, taken from a later map whose pool has
    /// `last_force_op_resend`, starts a new interval after `self`.
    pub(crate) fn is_new_interval(&self, now: &PgInterval, last_force_op_resend: u32) -> bool {
        self.up != now.up
            || self.up_primary != now.up_primary
            || self.acting != now.acting
            || self.acting_primary != now.acting_primary
            || self.size != now.size
            || self.min_size != now.min_size
            || self.pg_num != now.pg_num
            || self.pgp_num != now.pgp_num
            || self.pg_num_pending != now.pg_num_pending
            || last_force_op_resend > self.epoch
    }
}

/// Whether a linger send failed without an answer from an OSD: the
/// session was lost or never opened, no OSD was reachable, or the PG has
/// the op backed off. Such a send
/// is not a failure of the linger; the next session reset or map change
/// re-sends it, as Objecter's `_linger_ops_resend` does.
pub(crate) fn is_no_answer(err: &OSDClientError) -> bool {
    use crate::msgr2::Msgr2Error;
    matches!(
        err,
        OSDClientError::Connection(_)
            | OSDClientError::NoOSDs
            | OSDClientError::Cancelled
            // A backed-off PG holds the op, as Objecter does, not rejects it.
            | OSDClientError::Backoff(_)
            | OSDClientError::Msgr2(
                Msgr2Error::Io(_) | Msgr2Error::Connection(_) | Msgr2Error::Timeout
            )
    )
}

/// A notify error as the negative errno its completion carries, passed
/// through as Objecter's `_linger_commit` does (`ENOENT` stays `ENOENT`).
pub(crate) fn notify_error_code(err: &OSDClientError) -> i32 {
    match err {
        OSDClientError::OSDError { code, .. } => *code,
        OSDClientError::Timeout(_) => ETIMEDOUT,
        OSDClientError::ObjectNotFound(_) | OSDClientError::PoolNotFound(_) => ENOENT,
        OSDClientError::Blocklisted => -libc::ESHUTDOWN,
        _ => -libc::EIO,
    }
}

/// A watch error as the negative errno a `Disconnect` carries: `ENOENT`
/// becomes `ENOTCONN`, as Objecter's `_normalize_watch_error` does, so a
/// deletion and a reconnect that raced it look the same; a lost
/// connection is `ENOTCONN` and an op timeout `ETIMEDOUT`.
pub(crate) fn watch_error_code(err: &OSDClientError) -> i32 {
    match err {
        OSDClientError::OSDError { code: ENOENT, .. } => ENOTCONN,
        OSDClientError::OSDError { code, .. } => *code,
        OSDClientError::Timeout(_) => ETIMEDOUT,
        _ => ENOTCONN,
    }
}

impl Linger {
    pub(crate) fn new(
        cookie: u64,
        object: ObjectId,
        kind: LingerKind,
        events: mpsc::UnboundedSender<WatchEvent>,
    ) -> Self {
        Self {
            cookie,
            object,
            kind,
            resent: tokio::sync::Notify::new(),
            events: std::sync::Mutex::new(Some(events)),
            state: std::sync::Mutex::new(LingerState::default()),
        }
    }

    pub(crate) fn lock_state(&self) -> std::sync::MutexGuard<'_, LingerState> {
        // The state is plain data, so a panic elsewhere cannot leave it
        // half-updated in a way worse than losing the linger.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record `code` as the watch's error and send one `Disconnect`,
    /// unless an error is already recorded. Returns whether it sent.
    pub(crate) fn fail(&self, code: i32) -> bool {
        {
            let mut state = self.lock_state();
            if state.last_error.is_some() {
                return false;
            }
            state.last_error = Some(code);
        }
        self.send_event(WatchEvent::Disconnect { code });
        true
    }

    /// A reconnect the OSD rejected with `code`: `code` replaces any error
    /// already recorded, as Objecter's `_linger_reconnect` sets
    /// `last_error` unconditionally, so a watch the OSD has since dropped
    /// moves on to `ENOTCONN` and stops being reconnected. The
    /// `Disconnect` goes out only if the watch had no error yet.
    pub(crate) fn reconnect_failed(&self, code: i32) {
        let first = {
            let mut state = self.lock_state();
            let first = state.last_error.is_none();
            state.last_error = Some(code);
            first
        };
        if first {
            self.send_event(WatchEvent::Disconnect { code });
        }
    }

    fn send_event(&self, event: WatchEvent) {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(tx) = events.as_ref() {
            let _ = tx.send(event);
        }
    }

    /// Drop the event sender: the watcher receives what is queued, then
    /// `None`.
    pub(crate) fn close_events(&self) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    /// Apply the outcome of a ping sent at `sent` for `generation`. A
    /// reply for an older generation changes nothing; a success moves
    /// `watch_valid_thru`; the first failure becomes the one `Disconnect`.
    /// A send with no answer (a lost session, no reachable OSD) is not a
    /// failure here: the session's end fails its in-flight ops before the
    /// reset hook runs, and that hook's reconnect settles the watch.
    pub(crate) fn ping_finished(&self, generation: u32, sent: Instant, outcome: Result<()>) {
        let mut state = self.lock_state();
        if state.register_gen != generation {
            debug!(
                "watch {} ping for generation {} is stale, ignoring",
                self.cookie, generation
            );
            return;
        }
        match outcome {
            Ok(()) => state.watch_valid_thru = Some(sent),
            Err(e) if is_no_answer(&e) => {
                debug!("watch {} ping got no answer: {}", self.cookie, e);
            }
            Err(e) => {
                drop(state);
                debug!("watch {} ping failed: {}", self.cookie, e);
                self.fail(watch_error_code(&e));
            }
        }
    }

    /// Start a send: take its number, mark it out, and clear any pending
    /// re-send. Returns the number.
    pub(crate) fn begin_send(&self) -> u64 {
        Self::begin_send_locked(&mut self.lock_state())
    }

    /// [`Self::begin_send`] under a guard the caller already holds.
    pub(crate) fn begin_send_locked(state: &mut LingerState) -> u64 {
        state.send_seq += 1;
        state.sending = true;
        state.resend_pending = false;
        state.send_seq
    }

    /// Answer a pending `linger_watch` with `outcome`, once.
    pub(crate) fn finish_registration(&self, outcome: Result<()>) {
        let tx = self.lock_state().registration.take();
        if let Some(tx) = tx {
            let _ = tx.send(outcome);
        }
    }

    /// Complete a notify with `code` and no reply, unless it completed.
    pub(crate) fn complete_notify(&self, code: i32) {
        if let LingerKind::Notify { completion, .. } = &self.kind {
            let tx = completion
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(tx) = tx {
                let _ = tx.send((code, Bytes::new()));
            }
        }
    }

    fn deliver(&self, msg: MWatchNotify, data: Bytes) {
        match (msg.opcode, &self.kind) {
            (CEPH_WATCH_EVENT_DISCONNECT, _) => {
                self.fail(ENOTCONN);
            }
            (CEPH_WATCH_EVENT_NOTIFY, LingerKind::Watch { .. }) => {
                self.send_event(WatchEvent::Notify {
                    notify_id: msg.notify_id,
                    notifier_gid: msg.notifier_gid,
                    payload: msg.bl,
                });
            }
            (
                CEPH_WATCH_EVENT_NOTIFY_COMPLETE,
                LingerKind::Notify {
                    completion,
                    notify_id,
                    ..
                },
            ) => {
                let known = notify_id.load(Ordering::Acquire);
                if known != 0 && known != msg.notify_id {
                    debug!(
                        "notify cookie {} completion for notify {} != {}, ignoring",
                        self.cookie, msg.notify_id, known
                    );
                    return;
                }
                let tx = completion
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(tx) = tx {
                    let _ = tx.send((msg.return_code, data));
                }
            }
            (opcode, _) => debug!(
                "watch-notify opcode {} does not apply to cookie {}, ignoring",
                opcode, self.cookie
            ),
        }
    }
}

/// A registered watch, from [`crate::IoCtx::watch`]: librados's watch
/// handle. The client keeps it alive with pings and re-registers it
/// across session resets and PG interval changes; events arrive through
/// [`recv`](Self::recv). Dropping it stops the pings and forgets the
/// watch without telling the OSD, which then expires it.
pub struct Watcher {
    client: Arc<OSDClient>,
    linger: Arc<Linger>,
    events: mpsc::UnboundedReceiver<WatchEvent>,
}

impl Watcher {
    pub(crate) fn new(
        client: Arc<OSDClient>,
        linger: Arc<Linger>,
        events: mpsc::UnboundedReceiver<WatchEvent>,
    ) -> Self {
        Self {
            client,
            linger,
            events,
        }
    }

    /// The watch cookie, as the OSD and `list_watchers` report it.
    pub fn cookie(&self) -> u64 {
        self.linger.cookie
    }

    /// The next event; `None` once the client has forgotten the watch
    /// (after `unwatch`, a deleted pool, or the client's shutdown, which
    /// first delivers `Disconnect { code: ENOTCONN }`).
    pub async fn recv(&mut self) -> Option<WatchEvent> {
        self.events.recv().await
    }

    /// Acknowledge a notify with `reply`, which the notifier receives in
    /// its [`NotifyResult::acks`].
    pub async fn notify_ack(&self, notify_id: u64, reply: Bytes) -> Result<()> {
        self.client
            .notify_ack(&self.linger.object, notify_id, self.linger.cookie, reply)
            .await
    }

    /// Remove the watch from the object. `ENOENT` if the object is gone.
    pub async fn unwatch(self) -> Result<()> {
        self.client.unwatch(&self.linger).await
    }

    /// How long ago the watch was last known good, or the error that lost
    /// it: librados's `watch_check`.
    pub fn check(&self) -> Result<Duration> {
        let state = self.linger.lock_state();
        if let Some(code) = state.last_error {
            return Err(OSDClientError::OSDError {
                code,
                message: "watch lost".into(),
            });
        }
        Ok(state
            .watch_valid_thru
            .map_or(Duration::ZERO, |t| t.elapsed()))
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.client.forget_linger(self.linger.cookie);
    }
}

impl std::fmt::Debug for Watcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Watcher");
        d.field("cookie", &self.linger.cookie)
            .field("object", &self.linger.object);
        if let LingerKind::Watch { timeout } = self.linger.kind {
            d.field("timeout", &timeout);
        }
        d.finish()
    }
}

/// The watches a ping round covers, each with the generation to tag its
/// ping with: registered, placed, and with no error recorded, since a
/// failed watch is not pinged until it is registered again.
///
/// A watch with a send out or one awaiting a re-send is not pinged: the
/// OSD would answer `ETIMEDOUT` for a watch it holds disconnected.
/// A reconnect held by PAUSEWR counts as a send out, so the watch goes
/// unpinged for as long as the pause lasts.
pub(crate) fn watches_to_ping(lingers: &DashMap<u64, Arc<Linger>>) -> Vec<(Arc<Linger>, u32)> {
    lingers
        .iter()
        .filter(|l| matches!(l.kind, LingerKind::Watch { .. }))
        .filter_map(|l| {
            let state = l.lock_state();
            (state.registered
                && state.last_error.is_none()
                && state.osd.is_some()
                && !state.sending
                && !state.resend_pending)
                .then(|| (Arc::clone(l.value()), state.register_gen))
        })
        .collect()
}

/// The lingers a tick re-sends: every one whose last send got no answer,
/// and every registered watch in error, except `ENOTCONN` (the OSD has
/// no such watch any more, so a reconnect can only fail the same way).
/// A linger with a send already out is left to it.
pub(crate) fn lingers_to_resend(lingers: &DashMap<u64, Arc<Linger>>) -> Vec<Arc<Linger>> {
    lingers
        .iter()
        .filter(|l| {
            let state = l.lock_state();
            if state.sending {
                return false;
            }
            let in_error = matches!(l.kind, LingerKind::Watch { .. })
                && state.registered
                && state.last_error.is_some_and(|code| code != ENOTCONN);
            state.resend_pending || in_error
        })
        .map(|l| Arc::clone(l.value()))
        .collect()
}

/// What a scan of the lingers against a batch of maps found.
#[derive(Default)]
pub(crate) struct LingerScan {
    /// Lingers to re-send, each once.
    pub(crate) resend: Vec<Arc<Linger>>,
    /// Lingers whose pool no longer exists.
    pub(crate) pool_gone: Vec<Arc<Linger>>,
}

/// Compare every sent linger's recorded interval against each of `maps`
/// in order, as Objecter's `_scan_requests` runs per applied epoch, so an
/// interval that changes and changes back inside one batch still counts.
/// `skipped` (epochs never seen) re-sends every sent linger, as
/// Objecter's `skipped_map` does. `interval_of` places a linger's object
/// in a map. The recorded interval moves to the newest map that changed
/// it, so the next batch compares against that.
pub(crate) fn scan_lingers(
    lingers: &DashMap<u64, Arc<Linger>>,
    maps: &[Arc<crate::osdclient::osdmap::OSDMap>],
    skipped: bool,
    interval_of: impl Fn(&crate::osdclient::osdmap::OSDMap, &ObjectId) -> Result<PgInterval>,
) -> LingerScan {
    let snapshot: Vec<Arc<Linger>> = lingers.iter().map(|l| Arc::clone(l.value())).collect();
    let mut scan = LingerScan::default();
    for linger in snapshot {
        if linger.lock_state().interval.is_none() {
            // Never sent: its first send places it.
            continue;
        }
        let mut changed = skipped;
        let mut gone = false;
        for osdmap in maps {
            let Some(pool) = osdmap.pools.get(&linger.object.pool) else {
                gone = true;
                break;
            };
            let now = match interval_of(osdmap, &linger.object) {
                Ok(now) => now,
                Err(e) => {
                    debug!("linger {}: cannot place it: {}", linger.cookie, e);
                    continue;
                }
            };
            let mut state = linger.lock_state();
            let Some(then) = state.interval.as_ref() else {
                break;
            };
            let force = pool.canonical_last_force_op_resend().as_u32();
            if then.is_new_interval(&now, force) {
                debug!(
                    "linger {} enters a new interval at epoch {}: {:?} -> {:?}",
                    linger.cookie, now.epoch, then, now
                );
                state.interval = Some(now);
                changed = true;
            }
        }
        if gone {
            scan.pool_gone.push(linger);
        } else if changed {
            scan.resend.push(linger);
        }
    }
    scan
}

/// Hand a decoded `MWatchNotify` to the linger its cookie names; `data` is
/// the message's data segment. Every event is routed by cookie, as
/// Objecter does, and nothing here awaits: the caller is the session's
/// I/O loop.
pub(crate) fn route_watch_notify(
    lingers: &DashMap<u64, Arc<Linger>>,
    msg: MWatchNotify,
    data: Bytes,
) {
    let Some(linger) = lingers.get(&msg.cookie).map(|l| Arc::clone(&l)) else {
        debug!(
            "watch-notify opcode {} for unknown cookie {}, dropping",
            msg.opcode, msg.cookie
        );
        return;
    };
    linger.deliver(msg, data);
}

/// One watcher's acknowledgement of a notify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyAck {
    /// The acking watcher's client global id.
    pub gid: u64,
    /// The acking watch's cookie.
    pub cookie: u64,
    /// The payload the watcher acked with.
    pub reply: Bytes,
}

/// A watcher that was still registered but did not ack before the
/// notify timed out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotifyTimeout {
    pub gid: u64,
    pub cookie: u64,
}

/// A completed notify, as librados hands it back: the acks, the watchers
/// that missed it, and whether the OSD completed it by timeout.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NotifyResult {
    pub acks: Vec<NotifyAck>,
    pub missed: Vec<NotifyTimeout>,
    pub timed_out: bool,
}

/// Decode a NOTIFY_COMPLETE's data segment: the OSD's
/// `map<pair<gid, cookie>, bufferlist>` of acks, then its
/// `vector<pair<gid, cookie>>` of watchers that missed the notify.
pub fn decode_notify_reply(data: &[u8]) -> Result<(Vec<NotifyAck>, Vec<NotifyTimeout>)> {
    let mut buf = Bytes::copy_from_slice(data);
    let n = u32::decode(&mut buf, 0)?;
    let mut acks = Vec::new();
    for _ in 0..n {
        let gid = u64::decode(&mut buf, 0)?;
        let cookie = u64::decode(&mut buf, 0)?;
        let reply = Bytes::decode(&mut buf, 0)?;
        acks.push(NotifyAck { gid, cookie, reply });
    }
    let m = u32::decode(&mut buf, 0)?;
    let mut missed = Vec::new();
    for _ in 0..m {
        let gid = u64::decode(&mut buf, 0)?;
        let cookie = u64::decode(&mut buf, 0)?;
        missed.push(NotifyTimeout { gid, cookie });
    }
    Ok((acks, missed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_one_ack_and_one_miss() {
        let mut wire = vec![1, 0, 0, 0];
        wire.extend_from_slice(&11u64.to_le_bytes());
        wire.extend_from_slice(&12u64.to_le_bytes());
        wire.extend_from_slice(&[5, 0, 0, 0]);
        wire.extend_from_slice(b"reply");
        wire.extend_from_slice(&[1, 0, 0, 0]);
        wire.extend_from_slice(&21u64.to_le_bytes());
        wire.extend_from_slice(&22u64.to_le_bytes());

        let (acks, missed) = decode_notify_reply(&wire).expect("decode");
        assert_eq!(
            acks,
            vec![NotifyAck {
                gid: 11,
                cookie: 12,
                reply: Bytes::from_static(b"reply"),
            }]
        );
        assert_eq!(
            missed,
            vec![NotifyTimeout {
                gid: 21,
                cookie: 22
            }]
        );
    }

    #[test]
    fn decodes_empty_lists() {
        let (acks, missed) = decode_notify_reply(&[0; 8]).expect("decode");
        assert!(acks.is_empty());
        assert!(missed.is_empty());
    }

    #[test]
    fn rejects_a_truncated_reply() {
        assert!(decode_notify_reply(&[1, 0, 0, 0, 1]).is_err());
    }

    const WATCH_COOKIE: u64 = 1001;
    const NOTIFY_COOKIE: u64 = 1002;

    fn event(opcode: u8, cookie: u64, notify_id: u64) -> MWatchNotify {
        MWatchNotify {
            opcode,
            cookie,
            ver: 0,
            notify_id,
            bl: Bytes::from_static(b"payload"),
            return_code: 0,
            notifier_gid: 4242,
        }
    }

    struct Registry {
        lingers: DashMap<u64, Arc<Linger>>,
        watch_rx: mpsc::UnboundedReceiver<WatchEvent>,
        done_rx: oneshot::Receiver<(i32, Bytes)>,
    }

    fn registry() -> Registry {
        let lingers = DashMap::new();
        let (tx, watch_rx) = mpsc::unbounded_channel();
        lingers.insert(
            WATCH_COOKIE,
            Arc::new(Linger::new(
                WATCH_COOKIE,
                ObjectId::new(1, "o"),
                LingerKind::Watch { timeout: 0 },
                tx,
            )),
        );
        let (done_tx, done_rx) = oneshot::channel();
        let (tx, _) = mpsc::unbounded_channel();
        lingers.insert(
            NOTIFY_COOKIE,
            Arc::new(Linger::new(
                NOTIFY_COOKIE,
                ObjectId::new(1, "o"),
                LingerKind::Notify {
                    completion: std::sync::Mutex::new(Some(done_tx)),
                    notify_id: AtomicU64::new(0),
                    timeout_secs: 10,
                    payload: Bytes::new(),
                },
                tx,
            )),
        );
        Registry {
            lingers,
            watch_rx,
            done_rx,
        }
    }

    #[test]
    fn a_notify_reaches_the_watch_channel() {
        let mut r = registry();
        route_watch_notify(
            &r.lingers,
            event(CEPH_WATCH_EVENT_NOTIFY, WATCH_COOKIE, 77),
            Bytes::new(),
        );
        assert_eq!(
            r.watch_rx.try_recv(),
            Ok(WatchEvent::Notify {
                notify_id: 77,
                notifier_gid: 4242,
                payload: Bytes::from_static(b"payload"),
            })
        );
        assert!(r.watch_rx.try_recv().is_err());
    }

    #[test]
    fn a_repeated_disconnect_is_reported_once() {
        let mut r = registry();
        for _ in 0..2 {
            route_watch_notify(
                &r.lingers,
                event(CEPH_WATCH_EVENT_DISCONNECT, WATCH_COOKIE, 0),
                Bytes::new(),
            );
        }
        assert_eq!(
            r.watch_rx.try_recv(),
            Ok(WatchEvent::Disconnect { code: ENOTCONN })
        );
        assert!(r.watch_rx.try_recv().is_err());
        let linger = r.lingers.get(&WATCH_COOKIE).unwrap();
        assert_eq!(linger.lock_state().last_error, Some(ENOTCONN));
    }

    #[test]
    fn a_completion_before_the_notify_id_is_known_completes_once() {
        let mut r = registry();
        let data = Bytes::from_static(&[0; 8]);
        route_watch_notify(
            &r.lingers,
            event(CEPH_WATCH_EVENT_NOTIFY_COMPLETE, NOTIFY_COOKIE, 9),
            data.clone(),
        );
        assert_eq!(r.done_rx.try_recv(), Ok((0, data)));
        // A second completion has no sender left to complete.
        route_watch_notify(
            &r.lingers,
            event(CEPH_WATCH_EVENT_NOTIFY_COMPLETE, NOTIFY_COOKIE, 9),
            Bytes::new(),
        );
    }

    #[test]
    fn a_completion_for_another_notify_id_is_ignored() {
        let mut r = registry();
        if let LingerKind::Notify { notify_id, .. } = &r.lingers.get(&NOTIFY_COOKIE).unwrap().kind {
            notify_id.store(10, Ordering::Release);
        }
        route_watch_notify(
            &r.lingers,
            event(CEPH_WATCH_EVENT_NOTIFY_COMPLETE, NOTIFY_COOKIE, 9),
            Bytes::new(),
        );
        assert!(r.done_rx.try_recv().is_err());
        let mut done = event(CEPH_WATCH_EVENT_NOTIFY_COMPLETE, NOTIFY_COOKIE, 10);
        done.return_code = crate::osdclient::error::ETIMEDOUT;
        route_watch_notify(&r.lingers, done, Bytes::new());
        assert_eq!(
            r.done_rx.try_recv(),
            Ok((crate::osdclient::error::ETIMEDOUT, Bytes::new()))
        );
    }

    fn registered(r: &Registry, cookie: u64, generation: u32) -> Arc<Linger> {
        let linger = Arc::clone(r.lingers.get(&cookie).unwrap().value());
        let mut state = linger.lock_state();
        state.registered = true;
        state.osd = Some(0);
        state.register_gen = generation;
        drop(state);
        linger
    }

    #[test]
    fn the_ping_round_skips_failed_and_unregistered_watches() {
        let r = registry();
        assert!(watches_to_ping(&r.lingers).is_empty(), "not registered yet");
        let watch = registered(&r, WATCH_COOKIE, 2);
        // A notify linger is never pinged, registered or not.
        registered(&r, NOTIFY_COOKIE, 0);
        let round = watches_to_ping(&r.lingers);
        assert_eq!(round.len(), 1);
        assert_eq!((round[0].0.cookie, round[0].1), (WATCH_COOKIE, 2));

        watch.fail(ENOTCONN);
        assert!(watches_to_ping(&r.lingers).is_empty());
    }

    #[test]
    fn the_tick_resends_unanswered_and_errored_lingers_only() {
        let r = registry();
        let watch = registered(&r, WATCH_COOKIE, 1);
        assert!(lingers_to_resend(&r.lingers).is_empty());

        watch.lock_state().resend_pending = true;
        assert_eq!(lingers_to_resend(&r.lingers).len(), 1);
        assert!(
            watches_to_ping(&r.lingers).is_empty(),
            "not pinged meanwhile"
        );

        watch.lock_state().sending = true;
        assert!(lingers_to_resend(&r.lingers).is_empty(), "a send is out");

        {
            let mut state = watch.lock_state();
            state.sending = false;
            state.resend_pending = false;
            state.last_error = Some(ETIMEDOUT);
        }
        assert_eq!(lingers_to_resend(&r.lingers).len(), 1, "reconnect in error");
        watch.lock_state().last_error = Some(ENOTCONN);
        assert!(
            lingers_to_resend(&r.lingers).is_empty(),
            "the OSD dropped it"
        );
    }

    #[test]
    fn a_reconnect_answering_enotconn_ends_the_retries() {
        let mut r = registry();
        let watch = registered(&r, WATCH_COOKIE, 1);
        watch.lock_state().last_error = Some(ETIMEDOUT);
        assert_eq!(lingers_to_resend(&r.lingers).len(), 1);

        watch.reconnect_failed(ENOTCONN);
        assert_eq!(watch.lock_state().last_error, Some(ENOTCONN));
        assert!(
            r.watch_rx.try_recv().is_err(),
            "the error was already reported"
        );
        assert!(
            lingers_to_resend(&r.lingers).is_empty(),
            "no more reconnects"
        );

        let (fresh, mut rx) = {
            let (tx, rx) = mpsc::unbounded_channel();
            let linger = Linger::new(
                9,
                ObjectId::new(1, "o"),
                LingerKind::Watch { timeout: 0 },
                tx,
            );
            (linger, rx)
        };
        fresh.reconnect_failed(ETIMEDOUT);
        assert_eq!(
            rx.try_recv(),
            Ok(WatchEvent::Disconnect { code: ETIMEDOUT })
        );
    }

    #[test]
    fn a_stale_generation_ping_reply_changes_nothing() {
        let mut r = registry();
        let watch = registered(&r, WATCH_COOKIE, 3);
        let sent = Instant::now();
        watch.ping_finished(2, sent, Ok(()));
        assert_eq!(watch.lock_state().watch_valid_thru, None);
        watch.ping_finished(
            2,
            sent,
            Err(OSDClientError::OSDError {
                code: ETIMEDOUT,
                message: "ping".into(),
            }),
        );
        assert_eq!(watch.lock_state().last_error, None);
        assert!(r.watch_rx.try_recv().is_err());

        watch.ping_finished(3, sent, Ok(()));
        assert_eq!(watch.lock_state().watch_valid_thru, Some(sent));
    }

    #[test]
    fn failed_pings_send_one_disconnect() {
        let mut r = registry();
        let watch = registered(&r, WATCH_COOKIE, 1);
        let sent = Instant::now();
        // A session loss is left to the reset hook's reconnect.
        watch.ping_finished(1, sent, Err(OSDClientError::Connection("lost".into())));
        assert!(r.watch_rx.try_recv().is_err());
        for code in [ETIMEDOUT, ENOENT] {
            watch.ping_finished(
                1,
                sent,
                Err(OSDClientError::OSDError {
                    code,
                    message: "ping".into(),
                }),
            );
        }
        assert_eq!(
            r.watch_rx.try_recv(),
            Ok(WatchEvent::Disconnect { code: ETIMEDOUT })
        );
        assert!(r.watch_rx.try_recv().is_err());
    }

    #[test]
    fn only_lost_sessions_and_unreachable_osds_are_no_answer() {
        assert!(is_no_answer(&OSDClientError::Connection("lost".into())));
        assert!(is_no_answer(&OSDClientError::NoOSDs));
        assert!(is_no_answer(&OSDClientError::Cancelled));
        assert!(is_no_answer(&OSDClientError::Backoff("blocked".into())));
        assert!(is_no_answer(&OSDClientError::Msgr2(
            crate::msgr2::Msgr2Error::Timeout
        )));
        // Auth, decoding and configuration errors will not heal by waiting.
        for err in [
            crate::msgr2::Msgr2Error::Auth("denied".into()),
            crate::msgr2::Msgr2Error::Deserialization("bad".into()),
            crate::msgr2::Msgr2Error::ConfigError("bad".into()),
        ] {
            assert!(!is_no_answer(&OSDClientError::Msgr2(err)));
        }
        let osd = |code| OSDClientError::OSDError {
            code,
            message: String::new(),
        };
        for err in [
            osd(ENOENT),
            osd(ENOTCONN),
            osd(ETIMEDOUT),
            OSDClientError::Timeout(Duration::from_secs(30)),
        ] {
            assert!(!is_no_answer(&err), "{err:?}");
        }
        assert_eq!(notify_error_code(&osd(ENOENT)), ENOENT);
        assert_eq!(
            notify_error_code(&OSDClientError::Timeout(Duration::from_secs(30))),
            ETIMEDOUT
        );
    }

    #[test]
    fn watch_errors_normalize_enoent_to_enotconn() {
        let err = |code| OSDClientError::OSDError {
            code,
            message: String::new(),
        };
        assert_eq!(watch_error_code(&err(ENOENT)), ENOTCONN);
        assert_eq!(watch_error_code(&err(ETIMEDOUT)), ETIMEDOUT);
        assert_eq!(
            watch_error_code(&OSDClientError::Timeout(Duration::from_secs(30))),
            ETIMEDOUT
        );
    }

    fn interval(up: &[i32], acting: &[i32]) -> PgInterval {
        PgInterval {
            up: up.to_vec(),
            up_primary: up[0],
            acting: acting.to_vec(),
            acting_primary: acting[0],
            size: 3,
            min_size: 2,
            pg_num: 32,
            pgp_num: 32,
            pg_num_pending: 32,
            epoch: 10,
        }
    }

    #[test]
    fn an_interval_changes_with_up_or_acting_alone() {
        let then = interval(&[0, 1, 2], &[0, 1, 2]);
        let mut same = then.clone();
        same.epoch = 11;
        assert!(
            !then.is_new_interval(&same, 0),
            "the epoch is not part of it"
        );

        // pg_temp moves acting and leaves up alone.
        assert!(then.is_new_interval(&interval(&[0, 1, 2], &[3, 1, 2]), 0));
        // pg_temp holds acting while up changes.
        assert!(then.is_new_interval(&interval(&[4, 1, 2], &[0, 1, 2]), 0));

        let mut split = then.clone();
        split.pg_num = 64;
        assert!(then.is_new_interval(&split, 0));
        let mut min_size = then.clone();
        min_size.min_size = 1;
        assert!(then.is_new_interval(&min_size, 0));
    }

    fn map_at(epoch: u32) -> Arc<crate::osdclient::osdmap::OSDMap> {
        let mut map = crate::osdclient::osdmap::OSDMap::new();
        map.epoch = crate::Epoch::new(epoch);
        map.pools
            .insert(1, crate::osdclient::osdmap::PgPool::default());
        Arc::new(map)
    }

    /// Epoch 12 moves the PG to OSD 4; every other epoch has it on 0.
    fn flapping(osdmap: &crate::osdclient::osdmap::OSDMap, _: &ObjectId) -> Result<PgInterval> {
        let mut i = if osdmap.epoch.as_u32() == 12 {
            interval(&[4, 1, 2], &[4, 1, 2])
        } else {
            interval(&[0, 1, 2], &[0, 1, 2])
        };
        i.epoch = osdmap.epoch.as_u32();
        Ok(i)
    }

    #[test]
    fn a_flap_inside_one_batch_resends_once() {
        let r = registry();
        let watch = registered(&r, WATCH_COOKIE, 0);
        watch.lock_state().interval = Some(interval(&[0, 1, 2], &[0, 1, 2]));

        // Only the final map: A -> A, nothing to do.
        let scan = scan_lingers(&r.lingers, &[map_at(13)], false, flapping);
        assert!(scan.resend.is_empty());

        let batch = [map_at(11), map_at(12), map_at(13)];
        let scan = scan_lingers(&r.lingers, &batch, false, flapping);
        let cookies: Vec<u64> = scan.resend.iter().map(|l| l.cookie).collect();
        assert_eq!(cookies, vec![WATCH_COOKIE], "one re-send for A -> B -> A");
        assert_eq!(watch.lock_state().interval.as_ref().unwrap().epoch, 13);
    }

    #[test]
    fn skipped_maps_resend_every_sent_linger() {
        let r = registry();
        // An unregistered watch whose registration is in flight counts too.
        let watch = Arc::clone(r.lingers.get(&WATCH_COOKIE).unwrap().value());
        watch.lock_state().interval = Some(interval(&[0, 1, 2], &[0, 1, 2]));
        let scan = scan_lingers(&r.lingers, &[map_at(20)], true, flapping);
        let cookies: Vec<u64> = scan.resend.iter().map(|l| l.cookie).collect();
        assert_eq!(cookies, vec![WATCH_COOKIE], "the unsent notify is not");
    }

    #[test]
    fn a_vanished_pool_is_reported_not_resent() {
        let r = registry();
        let watch = registered(&r, WATCH_COOKIE, 0);
        watch.lock_state().interval = Some(interval(&[0, 1, 2], &[0, 1, 2]));
        let empty = Arc::new(crate::osdclient::osdmap::OSDMap::new());
        let scan = scan_lingers(&r.lingers, &[empty], true, flapping);
        assert!(scan.resend.is_empty());
        assert_eq!(scan.pool_gone.len(), 1);
    }

    #[test]
    fn a_forced_resend_after_the_send_is_a_new_interval() {
        let then = interval(&[0, 1, 2], &[0, 1, 2]);
        assert!(!then.is_new_interval(&then, 10));
        assert!(then.is_new_interval(&then, 11));
    }

    #[test]
    fn an_unknown_cookie_is_dropped() {
        let mut r = registry();
        route_watch_notify(
            &r.lingers,
            event(CEPH_WATCH_EVENT_NOTIFY, 5, 1),
            Bytes::new(),
        );
        assert!(r.watch_rx.try_recv().is_err());
    }
}
