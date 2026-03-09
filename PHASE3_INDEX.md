# Phase 3 Audit: Modern Monitor Subscription Behavior

## Document Index

This folder contains a complete audit and implementation roadmap for Phase 3 work on modern monitor subscription behavior (MON_STATEFUL_SUB support).

### Quick Start

**Start here**: [PHASE3_QUICK_REFERENCE.txt](PHASE3_QUICK_REFERENCE.txt) (5 min read)
- Status summary
- 3 concrete batches with effort/risk
- Critical file:line references
- Next steps

### Comprehensive Analysis

**For detailed understanding**: [PHASE3_AUDIT.md](PHASE3_AUDIT.md) (20 min read)
- Current subscription behavior explanation
- Where MON_STATEFUL_SUB is handled
- Behavior mismatch analysis
- Full implementation batch descriptions
- Risk assessment

### Implementation Guide

**For developers**: [PHASE3_IMPLEMENTATION_ROADMAP.txt](PHASE3_IMPLEMENTATION_ROADMAP.txt) (15 min read)
- Exact file:line changes
- Code snippets ready to copy-paste
- Part-by-part breakdown
- Verification commands
- Timeline estimate

### Executive Context

**For stakeholders**: [PHASE3_EXECUTIVE_SUMMARY.txt](PHASE3_EXECUTIVE_SUMMARY.txt) (10 min read)
- Executive summary with findings
- Current state (what works, what's missing)
- Risk assessment
- Recommended sequence
- Deliverables

---

## Key Findings Summary

### Current State ✅✅❌❌

| Aspect | Status | Details |
|--------|--------|---------|
| **Subscription tracking** | ✅ | MonSub struct fully implemented |
| **Stateful mode default** | ✅ | Correctly assumes RenewalMode::StatefulOrUnknown |
| **ACK fallback** | ✅ | Correctly switches to LegacyAcked when acked |
| **Capability detection** | ❌ | No way to query MON_STATEFUL_SUB from peer |
| **Explicit negotiation** | ❌ | Only implicit fallback via ACK |
| **Defensive timeout** | ❌ | Waits indefinitely if monitor hangs |
| **Mode logging** | ❌ | Silent operation, no visibility |

### What's Missing

1. **No capability detection** - Can't check if monitor supports MON_STATEFUL_SUB
2. **No explicit negotiation** - Mode only switches on receiving ACK (reactive)
3. **No defensive timeout** - No detection of legacy monitors that never send ACK
4. **No monitoring/metrics** - Can't see which mode is active in production

### Implementation Approach

**3 Batches, building in layers:**

| Batch | Effort | Risk | Value |
|-------|--------|------|-------|
| 1: Capability Detection | 2-3h | LOW | Visibility into mode negotiation |
| 2: Explicit Negotiation | 4-5h | MEDIUM | Proactive mode detection from features |
| 3: Defensive Timeout | 2-3h | LOW | Fallback if monitor hangs |
| Integration Tests | 3h | LOW | Confidence with real clusters |

**Total**: ~12-16 hours for complete implementation

---

## Critical Code Locations

### Subscription State Machine

```
subscription.rs:44-50    RenewalMode enum
subscription.rs:80       Default: StatefulOrUnknown
subscription.rs:90       need_renew() returns false for stateful
subscription.rs:116      acked() switches to LegacyAcked
```

### Connection Handling

```
client.rs:826-842        handle_monmap_connect() - reloads on reconnect
client.rs:887-908        notify_map_received() - checks if renewal needed
client.rs:1330-1341      handle_subscribe_ack() - ACK fallback to legacy
```

### Message Types

```
messages.rs:182          MMonSubscribe (client -> monitor)
messages.rs:213          MMonSubscribeAck (monitor -> client, legacy only)
```

---

## Testing

### Unit Tests (Always Work)

```bash
cargo test -p monclient --lib -- subscription::tests
```

Tests verify both modes:
- Stateful mode ignores legacy renewal deadlines (line 345)
- Legacy ACK enables renewal checks (line 358)
- Clear resets legacy renewal mode (line 372)

### Integration Tests (With Real Cluster)

```bash
# With Octopus+ cluster
CEPH_CONF=/etc/ceph/ceph.conf \
cargo test -p monclient --tests -- --ignored --nocapture | grep MON_STATEFUL_SUB
# Expected: "MON_STATEFUL_SUB=true"

# With legacy cluster
# Expected: "MON_STATEFUL_SUB=false"
```

---

## Dependency on Compression

✅ **NO blocking dependency**

Both MON_STATEFUL_SUB and compression use the same feature negotiation framework in msgr2:
- msgr2/src/state_machine.rs - `peer_supported_features: u64`
- msgr2/src/protocol.rs - Feature bits negotiated in banner exchange

Can implement MON_STATEFUL_SUB independently—just a different bit in the same u64.

---

## Recommended Reading Order

1. **First time?** Start with PHASE3_QUICK_REFERENCE.txt (5 min)
2. **Want details?** Read PHASE3_AUDIT.md (20 min)
3. **Ready to implement?** Use PHASE3_IMPLEMENTATION_ROADMAP.txt
4. **Reporting to others?** Share PHASE3_EXECUTIVE_SUMMARY.txt

---

## File Manifest

| File | Size | Lines | Purpose |
|------|------|-------|---------|
| PHASE3_QUICK_REFERENCE.txt | 5.6K | 139 | Quick overview, critical references |
| PHASE3_AUDIT.md | 16K | 464 | Comprehensive technical analysis |
| PHASE3_IMPLEMENTATION_ROADMAP.txt | 12K | 324 | Concrete code changes with line numbers |
| PHASE3_EXECUTIVE_SUMMARY.txt | 16K | 326 | High-level findings and status |
| PHASE3_INDEX.md | This file | - | Document navigation |

**Total**: ~50KB, 1,250 lines of documentation

---

## Next Steps

### For Managers/Stakeholders
→ Read PHASE3_EXECUTIVE_SUMMARY.txt
→ Review risk assessment and timeline
→ Decide which batch(es) to fund

### For Developers
1. Read PHASE3_QUICK_REFERENCE.txt for overview
2. Read PHASE3_IMPLEMENTATION_ROADMAP.txt for concrete changes
3. Start with Batch 1 (lowest risk, immediate feedback)
4. Run tests: `cargo test -p monclient --lib`
5. Deploy Batch 1, then proceed to Batch 2

### For Code Reviewers
→ Read PHASE3_AUDIT.md for full context
→ Check concrete file:line references in ROADMAP
→ Verify test coverage additions
→ Ensure fallback mechanisms preserved

---

## Key Questions Answered

**Q: Does it work on modern clusters?**
A: Yes, correctly (by design assumption). But we don't verify it works.

**Q: What happens on legacy clusters?**
A: Falls back correctly when ACK received. But waits indefinitely if monitor hangs.

**Q: Is this blocking other work?**
A: No. Compression negotiation is independent (same framework, different bit).

**Q: What's the smallest safe first step?**
A: Batch 1 (Capability Detection) - 2-3 hours, LOW risk, immediate visibility.

**Q: How long to full implementation?**
A: 12-16 hours total (can be done in 3 days of focused work).

---

## Contact & Questions

For questions about this audit:
- See PHASE3_AUDIT.md section 10 (Readiness Checklist)
- Check PHASE3_IMPLEMENTATION_ROADMAP.txt for concrete code
- Review test files in crates/monclient/tests/
