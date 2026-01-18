# Denc Replacement Strategy

## Current State
- `denc.rs`: Old Denc trait with `encode() -> Bytes` signature
- `denc_mut.rs`: New DencMut trait with `encode(&mut BufMut)` signature
- Many types have BOTH implementations
- VersionedEncode provides blanket impl for old Denc

## Goal
Replace old Denc entirely with DencMut (renamed to Denc)

## Challenge
The signatures are fundamentally different:
- Old: `fn encode(&self, features: u64) -> Result<Bytes, RadosError>`
- New: `fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError>`

## Solution
Since we want to keep the new (better) signature, we need to:

1. Keep denc_mut.rs as the new denc.rs
2. Remove old denc.rs
3. Update VersionedEncode to work with new signature
4. Update all call sites

## Implementation Plan

### Phase 1: Preparation
- [x] Create branch
- [ ] Document all types using old Denc
- [ ] Document all types using VersionedEncode

### Phase 2: Create New Denc
- [ ] Copy denc_mut.rs content to new file
- [ ] Rename DencMut -> Denc throughout
- [ ] Add VersionedEncode support for new Denc

### Phase 3: Migration
- [ ] Update lib.rs to use new denc module
- [ ] Remove old denc.rs
- [ ] Fix all compilation errors
- [ ] Update tests

### Phase 4: Cleanup
- [ ] Remove denc_mut.rs
- [ ] Remove duplicate implementations
- [ ] Run all tests
- [ ] Commit

## Key Insight
The new Denc trait is BETTER because:
- Zero allocations
- Direct buffer writes
- Caller controls allocation strategy
- Better performance

So we're not just renaming - we're upgrading the entire encoding system.
