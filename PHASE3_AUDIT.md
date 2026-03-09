# Phase 3 Audit: Modern Monitor Subscription Behavior

## Executive Summary

The codebase has a **partial but incomplete** implementation of modern monitor subscription behavior. The subscription state machine correctly **assumes stateful mode by default** (`RenewalMode::StatefulOrUnknown`), but lacks:

1. **Capability detection**: No way to query/cache MON_STATEFUL_SUB support from peer monitors
2. **Explicit mode negotiation**: Renewal mode is never explicitly set based on monitor capabilities
3. **Test coverage**: No integration tests verifying behavior with modern vs. legacy monitors
4. **Client-side features**: No mechanism to signal supported capabilities to monitors

The implementation would **work correctly on modern clusters** (Octopus+) but is **not defensive** and provides no feedback about what the monitor actually supports.

---

## 1. Current Subscription Behavior

### Code Location: `crates/monclient/src/subscription.rs`

**Current Architecture:**
- `MonSub` struct manages subscription lifecycle (lines 52-71)
- `RenewalMode` enum defines two modes (lines 44-50):
  - `StatefulOrUnknown`: Modern monitors keep state server-side, no renewals needed
  - `LegacyAcked`: Legacy monitors send ACK with renewal interval

**Default Behavior:**
```rust
// Line 80: subscription.rs
renewal_mode: RenewalMode::StatefulOrUnknown,  // Assumes modern by default
```

**Key Methods:**
- `need_renew()` (lines 90-95): Returns `false` for stateful mode unless explicitly acked
- `acked()` (lines 116-126): Switches to `LegacyAcked` mode when subscribe ack received
- `renewed()` (lines 103-113): Moves subscriptions from `sub_new` to `sub_sent`

**Test Coverage:**
- ✅ Line 345: `test_stateful_subscriptions_do_not_trigger_renewal_from_stale_deadline()` 
  - Verifies stateful mode ignores legacy renewal deadlines
- ✅ Line 358: `test_legacy_ack_enables_renewal_checks()`
  - Verifies ack switches to renewal mode
- ✅ Line 372: `test_clear_resets_legacy_renewal_mode()`
  - Verifies clear resets state

### Code Location: `crates/monclient/src/client.rs`

**Subscription Lifecycle:**

1. **Connect Phase (lines 826-842):**
```rust
// Line 827: Comment explaining MON_STATEFUL_SUB
"MON_STATEFUL_SUB suppresses periodic renewal churn, but a fresh monitor 
session still needs the current subscription set replayed."

let should_send_subscriptions = {
    let mut sub_state = self.subscription_state.write().await;
    sub_state.reload()  // Move sub_sent back to sub_new for new session
};
```

2. **Map Received (lines 886-908):**
```rust
// Lines 890-891: Design comment for modern monitors
"Modern monitors with MON_STATEFUL_SUB keep the subscription state 
server-side and do not require legacy renewal traffic after each map update."

let need_renew = {
    let mut sub_state = self.subscription_state.write().await;
    sub_state.got(what, epoch);
    sub_state.need_renew()  // Only true if renewal_mode == LegacyAcked
};
```

3. **Subscribe ACK (lines 1330-1341):**
```rust
async fn handle_subscribe_ack(&self, msg: msgr2::message::Message) -> Result<()> {
    let ack: MMonSubscribeAck = decode_message(&msg)?;
    info!("Subscription acknowledged by legacy monitor: interval={}", ack.interval);
    
    let mut sub_state = self.subscription_state.write().await;
    sub_state.acked(ack.interval);  // Switches to LegacyAcked mode
    Ok(())
}
```

**Message Types:**

- `MMonSubscribe` (lines 182-207 in messages.rs): Sent by client to request subscriptions
- `MMonSubscribeAck` (lines 213-224 in messages.rs): ACK from legacy monitors with renewal interval

---

## 2. MON_STATEFUL_SUB Handling Analysis

### Where MON_STATEFUL_SUB is Handled:

**Direct References:**
- Line 46 in `subscription.rs`: Comment in `RenewalMode` enum
- Line 827 in `client.rs`: Comment explaining suppressed renewal churn
- Line 890 in `client.rs`: Comment about server-side subscription state

**Implicit Assumptions:**
1. Default `RenewalMode::StatefulOrUnknown` (stateful = assumed true)
2. Only switches to `LegacyAcked` when ACK received
3. No capability negotiation/detection mechanism

### What's MISSING:

❌ **No MON_STATEFUL_SUB flag definition**
- Feature flags not exposed from msgr2 connection
- No constant for MON_STATEFUL_SUB capability bit

❌ **No capability detection**
- Can't query monitor's feature set after connection
- No way to check if monitor has MON_STATEFUL_SUB

❌ **No explicit mode negotiation**
- Mode is switched ONLY on receiving subscribe ack (legacy fallback)
- No proactive capability query

❌ **No client capability announcement**
- Client doesn't advertise MON_STATEFUL_SUB support to monitor
- No way for monitor to know client understands stateful subscriptions

---

## 3. Behavior Mismatch on Modern Clusters

### Expected Octopus+ (Stateful) Behavior:
1. Client sends MMonSubscribe for initial subscription
2. Monitor holds subscription state server-side
3. Client sends NO renewal messages for each map epoch
4. Client only sends new MMonSubscribe if subscription changes
5. On reconnect, client resends current subscriptions

### Actual rados-rs Behavior:
✅ **Correct for modern monitors:**
- Default assumes stateful (correct)
- Doesn't send renewal traffic unless acked (correct)
- Replays subscriptions on reconnect (correct)

⚠️ **Gaps:**
- No way to verify monitor is actually stateful
- No logging/metrics about negotiated subscription mode
- No defensive handling if monitor doesn't support stateful

### Risk Analysis:

**On modern monitors (Octopus+):**
- ✅ Works correctly because default is stateful
- ⚠️ But we don't KNOW it's working—could silently fail

**On legacy monitors (pre-Nautilus):**
- ✅ Falls back correctly when ACK received
- ⚠️ Could experience high renewal traffic before fallback detected
- ⚠️ No timeout for detecting legacy monitors—waits indefinitely for ack

---

## 4. Implementation Batch: Smallest Safe Change

### Batch 1 (PHASE 3.1): Capability Detection & Logging
**Effort**: 2-3 hours
**Risk**: Low (read-only, logging only)

**Files to modify:**
1. `crates/monclient/src/types.rs` - Add feature flag constants
2. `crates/monclient/src/connection.rs` - Expose peer features
3. `crates/monclient/src/client.rs` - Query/log capabilities

**Changes:**

**New file: `crates/monclient/src/types.rs` (add to existing)**
```rust
// Monitor service feature flags (from Ceph source: mon/MonCommands.h)
pub const MON_STATEFUL_SUB: u64 = 1 << 0;

pub struct MonCapabilities {
    pub peer_supported_features: u64,
    pub stateful_sub_supported: bool,
}

impl MonCapabilities {
    pub fn from_features(features: u64) -> Self {
        Self {
            peer_supported_features: features,
            stateful_sub_supported: (features & MON_STATEFUL_SUB) != 0,
        }
    }
}
```

**Modify: `crates/monclient/src/connection.rs` (around line 230)**
```rust
// Add public accessor to MonConnection
pub fn peer_supported_features(&self) -> Option<u64> {
    // Extract from msgr2 connection state machine
    self.connection.state_machine().peer_supported_features
}
```

**Modify: `crates/monclient/src/client.rs` (around line 842, after connect)**
```rust
// After successful connection:
if let Some(features) = active_con.peer_supported_features() {
    let caps = MonCapabilities::from_features(features);
    info!(
        "Connected to monitor rank {}: stateful_sub={}", 
        rank, 
        caps.stateful_sub_supported
    );
}
```

### Batch 2 (PHASE 3.2): Explicit Mode Negotiation
**Effort**: 4-5 hours
**Risk**: Medium (changes subscription state machine)

**Files to modify:**
1. `crates/monclient/src/subscription.rs` - Add capability-aware mode detection
2. `crates/monclient/src/client.rs` - Use capability to set renewal mode
3. `crates/monclient/tests/command_operations.rs` - Add integration tests

**Key Change:**

**Modify: `crates/monclient/src/subscription.rs`**
```rust
// Add method to set renewal mode based on capabilities
pub fn set_renewal_mode_from_features(&mut self, features: u64) {
    if (features & MON_STATEFUL_SUB) != 0 {
        self.renewal_mode = RenewalMode::StatefulOrUnknown;
        debug!("Monitor supports MON_STATEFUL_SUB, stateful mode enabled");
    } else {
        self.renewal_mode = RenewalMode::LegacyAcked;
        debug!("Monitor does NOT support MON_STATEFUL_SUB, enabling renewal");
    }
}

// Modify timeout for waiting for ack (legacy monitor detection)
pub fn set_renewal_timeout(&mut self, timeout: Duration) {
    self.renewal_timeout = Some(Instant::now() + timeout);
}
```

**Modify: `crates/monclient/src/client.rs` (line 842)**
```rust
// After connect, set mode based on capabilities
if let Some(features) = active_con.peer_supported_features() {
    let mut sub_state = self.subscription_state.write().await;
    sub_state.set_renewal_mode_from_features(features);
}
```

### Batch 3 (PHASE 3.3): Defensive Timeout for Legacy Monitor Detection
**Effort**: 2-3 hours
**Risk**: Low (timeout-based, non-breaking)

**Key addition to `tick()`:**
```rust
// In tick(), detect legacy monitors that don't send ACK
if let Some(timeout) = sub_state.get_renewal_timeout() {
    if Instant::now() > timeout && sub_state.renewal_mode == RenewalMode::StatefulOrUnknown {
        warn!("Monitor did not send subscribe ACK within timeout, assuming legacy monitor");
        sub_state.renewal_mode = RenewalMode::LegacyAcked;
        self.send_subscriptions().await?;
    }
}
```

---

## 5. Exact Files & Functions to Edit

### Phase 3.1 Changes:

**File: `crates/monclient/src/types.rs`** (NEW or extended)
- Add: `pub const MON_STATEFUL_SUB: u64` constant
- Add: `pub struct MonCapabilities` with helper method

**File: `crates/monclient/src/connection.rs`**
- **Line 230**: Add method `pub fn peer_supported_features(&self) -> Option<u64>`
- **Line 286**: Update `get_auth_provider()` documentation if needed

**File: `crates/monclient/src/client.rs`**
- **Lines 826-842**: After successful connection, log capability detection
- Add import: `use crate::types::{MON_STATEFUL_SUB, MonCapabilities};`

### Phase 3.2 Changes:

**File: `crates/monclient/src/subscription.rs`**
- **Line 51**: Add field: `renewal_timeout: Option<Instant>`
- **Line 223**: Add method: `set_renewal_mode_from_features(&mut self, features: u64)`
- **Line 223**: Add method: `set_renewal_timeout(&mut self, timeout: Duration)`
- **Tests**: Add `test_capability_based_mode_detection()`

**File: `crates/monclient/src/client.rs`**
- **Line 842** (after `should_send_subscriptions` block): Add capability-based mode setting
- **Line 1059** (in `tick()` method): Add timeout-based legacy detection

### Phase 3.3 Integration Tests:

**File: `crates/monclient/tests/command_operations.rs`**
- Add: `#[tokio::test] async fn test_stateful_subscription_mode()`
- Add: `#[tokio::test] async fn test_legacy_monitor_fallback()`

---

## 6. Test Strategy

### Unit Tests (Phase 3.2):
```rust
#[test]
fn test_capability_based_mode_detection() {
    let mut sub = MonSub::new();
    
    // Test stateful mode detection
    sub.set_renewal_mode_from_features(MON_STATEFUL_SUB);
    assert_eq!(sub.renewal_mode, RenewalMode::StatefulOrUnknown);
    
    // Test legacy mode detection
    sub.set_renewal_mode_from_features(0);  // No MON_STATEFUL_SUB flag
    assert_eq!(sub.renewal_mode, RenewalMode::LegacyAcked);
}

#[test]
fn test_legacy_monitor_timeout_detection() {
    let mut sub = MonSub::new();
    sub.set_renewal_timeout(Duration::from_millis(100));
    
    // Initially StatefulOrUnknown
    assert!(!sub.is_renewal_timeout_exceeded());
    
    // After timeout
    std::thread::sleep(Duration::from_millis(150));
    assert!(sub.is_renewal_timeout_exceeded());
}
```

### Integration Tests (Phase 3.3):

**With modern monitor (Octopus+):**
```bash
./run_test.sh test_stateful_subscription_mode
# Verify: logs show "stateful_sub=true"
# Verify: no renewal traffic after map updates
```

**With legacy monitor (pre-Nautilus):**
```bash
./run_test.sh test_legacy_monitor_fallback
# Verify: logs show "stateful_sub=false" after timeout
# Verify: renewal traffic resumes
```

### Cluster Checks:

```bash
# Run with real Octopus cluster
CEPH_CONF=/etc/ceph/ceph.conf cargo test -p monclient --tests -- --ignored --test-threads=1

# Monitor traffic (should show no renewals for stateful):
tcpdump -i any -A 'dst port 6789' | grep -c "MMonSubscribe"
```

---

## 7. Dependency on Compression Negotiation

### Current State:
✅ **Feature negotiation framework EXISTS:**
- msgr2 `state_machine.rs` (line 829): `peer_supported_features: u64`
- msgr2 `protocol.rs` (line 1202): `set_peer_supported_features()` called after banner
- msgr2 `protocol.rs` (line 1046): Feature set converted to `FeatureSet` enum

### Compression Status:
- **Done**: msgr2 has full compression feature support (FeatureSet::COMPRESSION)
- **Integrated**: Compression negotiation happens in state machine
- **Accessible**: `connection.state_machine().peer_supported_features` available

### NO Dependency:
- ✅ Compression and MON_STATEFUL_SUB use same feature framework
- ✅ Can implement MON_STATEFUL_SUB independently
- ✅ Both are just different bits in same `peer_supported_features` u64

**HOWEVER:** Need to expose `state_machine()` accessor from `Msgr2Connection`:

**File: `crates/msgr2/src/protocol.rs` (around line 2226)**
```rust
pub fn state_machine(&self) -> &StateMachine {
    &self.state_machine
}
```

This is **already done for compression** so should be straightforward.

---

## 8. Risk Assessment

### Phase 3.1 (Capability Detection):
- **Risk**: Low (read-only, logging only)
- **Rollback**: Simple (remove logging)
- **Dependencies**: Needs `state_machine()` accessor exposed (already exists for compression)

### Phase 3.2 (Mode Negotiation):
- **Risk**: Medium (changes subscription state machine logic)
- **Rollback**: Keep `RenewalMode::acked()` as primary switch (always works)
- **Mitigation**: Add feature flag to disable (default enabled)

### Phase 3.3 (Defensive Timeout):
- **Risk**: Low (timeout-based fallback, non-breaking)
- **Rollback**: Simple (remove timeout check)
- **Config**: Make timeout configurable

---

## 9. Summary Table

| Item | Status | Location | Lines | Effort |
|------|--------|----------|-------|--------|
| **Subscription tracking** | ✅ Done | `subscription.rs` | 52-223 | - |
| **Default stateful mode** | ✅ Done | `subscription.rs:80` | - | - |
| **ACK fallback** | ✅ Done | `client.rs:1338` | - | - |
| **Capability detection** | ❌ Missing | `types.rs` | TBD | 2h |
| **Mode negotiation** | ❌ Missing | `subscription.rs`, `client.rs` | TBD | 4h |
| **Defensive timeout** | ❌ Missing | `client.rs:tick()` | TBD | 2h |
| **Integration tests** | ❌ Missing | `tests/` | TBD | 3h |

**Total Phase 3 Work**: ~11 hours (3 batches, can do incrementally)

---

## 10. Implementation Readiness Checklist

### Pre-Implementation:
- [ ] Add `state_machine()` accessor to Msgr2Connection (if not exists)
- [ ] Create feature flag constants in `types.rs`
- [ ] Update MonSub to expose renewal_mode for testing

### Batch 1:
- [ ] Add MonCapabilities struct
- [ ] Expose peer_features from MonConnection
- [ ] Add capability logging on connect
- [ ] Verify logging with local cluster

### Batch 2:
- [ ] Add renewal mode setter from features
- [ ] Update client.rs to use capability
- [ ] Add timeout field to MonSub
- [ ] Add unit tests for mode detection

### Batch 3:
- [ ] Add timeout logic to tick()
- [ ] Add integration tests with real monitors
- [ ] Document negotiated mode in output
- [ ] Add metrics for monitoring

### Verification:
- [ ] `cargo test -p monclient --lib` passes
- [ ] Integration tests with Octopus+ pass
- [ ] Integration tests with legacy pass
- [ ] No subscription traffic regressions
- [ ] Logging shows correct capability detection

