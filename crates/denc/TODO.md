# Ceph denc-rs Implementation TODO

## 🎉 Major Achievements

### ✅ Completed Core Infrastructure
1. **Basic Ceph Types** - UTime, String encoding, fundamental types
2. **Generic Container Support** - Vec<T>, BTreeMap<K,V> with proper denc encoding
3. **EntityAddr Complete** - Full entity_addr_t implementation with 100% corpus validation
4. **PgPool Core Implementation** - Successfully decoding pg_pool_t with all major fields
5. **Enhanced VersionedEncode** - ✨ NEW: Improved nested versioned encoding support

### ✅ Key Technical Breakthroughs
1. **Field Alignment Issues Resolved** - Fixed crush_rule/object_hash type mismatches
2. **Complex Nested Encoding** - HitSetParams with multi-level versioned encoding
3. **Version-Dependent Fields** - Proper C++ version logic implementation (v3-v29)
4. **Corpus Validation Framework** - Real Ceph data testing infrastructure
5. **Clean Architecture** - ✨ NEW: Refactored HitSetParams with separate BloomHitSetParams type

## 🔧 Current Status

### pg_pool_t Implementation
- **Decoding**: ✅ SUCCESS - All core fields extracted correctly
- **Encoding**: ⚠️ PARTIAL - Length mismatch (330 vs 229 bytes)
- **Validation**: ✅ Core functionality verified

### Test Results
- **entity_addr_t**: 100% success rate (all corpus files)
- **pg_pool_t**: Decoding successful, encoding needs completion

## 🚧 Immediate Tasks (High Priority)

### 1. Fix pg_pool_t Encoding Completeness
**Problem**: Roundtrip encoding produces 229 bytes vs original 330 bytes
**Solution Needed**:
- [ ] Complete encode_content() implementation for all version-dependent fields
- [ ] Add missing v24+ fields (opts, application_metadata, create_time, v28+ fields)
- [ ] Ensure proper version logic in encoding matches decoding

### 2. Improve VersionedEncode Architecture
**Current Issue**: HitSetParams and other complex types manually implement versioning
**Proposed Solution**:
```rust
// Enhanced VersionedEncode trait
pub trait VersionedEncode {
    fn encoding_version(&self, features: u64) -> u8;
    fn compat_version(&self, features: u64) -> u8;
    fn encode_content(&self, features: u64, version: u8) -> Result<Bytes, RadosError>;
    fn decode_content(bytes: &mut Bytes, version: u8, compat_version: u8) -> Result<Self, RadosError>
    where 
        Self: Sized;
    
    // New: Support for nested versioned encoding
    fn encode_versioned_nested(&self, features: u64) -> Result<Bytes, RadosError> {
        let version = self.encoding_version(features);
        let compat_version = self.compat_version(features);
        
        let mut buf = BytesMut::new();
        buf.put_u8(version);
        buf.put_u8(compat_version);
        
        let content = self.encode_content(features, version)?;
        buf.put_u32_le(content.len() as u32);
        buf.extend_from_slice(&content);
        
        Ok(buf.freeze())
    }
    
    // New: Nested decode support
    fn decode_versioned_nested(bytes: &mut Bytes) -> Result<Self, RadosError>
    where 
        Self: Sized
    {
        if bytes.remaining() < 6 {
            return Err(RadosError::Protocol("Insufficient bytes for versioned header".to_string()));
        }
        
        let version = bytes.get_u8();
        let compat_version = bytes.get_u8();
        let content_size = bytes.get_u32_le() as usize;
        
        if bytes.remaining() < content_size {
            return Err(RadosError::Protocol("Insufficient bytes for versioned content".to_string()));
        }
        
        let mut content_bytes = bytes.split_to(content_size).freeze();
        Self::decode_content(&mut content_bytes, version, compat_version)
    }
}
```

**Benefits**:
- [ ] Unified versioned encoding pattern
- [ ] Reduced code duplication
- [ ] Better maintainability
- [ ] Consistent error handling

### 3. Refactor HitSetParams to Use Enhanced VersionedEncode
**Current**: Manual ENCODE_START/ENCODE_FINISH implementation
**Target**: Clean VersionedEncode implementation
```rust
impl VersionedEncode for HitSetParams {
    fn encoding_version(&self, _features: u64) -> u8 { 1 }
    fn compat_version(&self, _features: u64) -> u8 { 1 }
    
    fn encode_content(&self, features: u64, _version: u8) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        match self {
            HitSetParams::None => buf.put_u8(0),
            HitSetParams::Bloom { fpp_micro, target_size, seed } => {
                buf.put_u8(3);
                let bloom_params = BloomHitSetParams { fpp_micro: *fpp_micro, target_size: *target_size, seed: *seed };
                let bloom_bytes = bloom_params.encode_versioned_nested(features)?;
                buf.extend_from_slice(&bloom_bytes);
            }
            // ... other variants
        }
        Ok(buf.freeze())
    }
    
    // decode_content implementation...
}
```

## 📋 Medium Priority Tasks

### 4. Complete Pool Options (opts) Implementation
- [ ] Investigate pool_opts_t structure in Ceph source
- [ ] Implement proper opts decoding instead of raw bytes
- [ ] Add version-specific opts handling

### 5. Implement Missing Complex Types

#### pg_merge_meta_t
- [ ] Study structure in osd_types.h
- [ ] Implement versioned encoding
- [ ] Add to pg_pool_t

#### BloomHitSetParams Refinement
- [ ] Extract as separate VersionedEncode type
- [ ] Clean up nested encoding logic

### 6. OSD Info Structure (osd_info_t)
- [ ] Analyze osd_types.h for osd_info_t
- [ ] Create corpus test framework
- [ ] Implement with VersionedEncode pattern

## 🎯 Long-term Goals

### 7. CrushWrapper Integration
**Complexity**: High - CRUSH maps are complex tree structures
- [ ] Study CRUSH algorithm implementation
- [ ] Implement crush::CrushWrapper equivalent
- [ ] Add proper weight/hierarchy handling

### 8. Complete OSDMap Implementation
**Dependencies**: osd_info_t, CrushWrapper, pg_pool_t ✅
- [ ] Implement incremental OSDMap support
- [ ] Add epoch handling
- [ ] Full corpus validation

### 9. Performance Optimization
- [ ] Zero-copy decoding where possible
- [ ] Streaming decode for large objects
- [ ] Memory pool for frequent allocations

### 10. Advanced Features
- [ ] Feature flag dependency management
- [ ] Cross-version compatibility testing
- [ ] Error recovery mechanisms

## 📊 Success Metrics

### Immediate (Next 2 weeks)
- [ ] pg_pool_t: 100% roundtrip success rate
- [ ] VersionedEncode: Consistent pattern across all types
- [ ] Documentation: Complete API docs

### Medium-term (Next month)
- [ ] osd_info_t: Complete implementation
- [ ] CrushWrapper: Basic functionality
- [ ] OSDMap: Decode capability

### Long-term (Next 3 months)
- [ ] Full librados compatibility layer
- [ ] Performance benchmarks vs C++ implementation
- [ ] Production-ready error handling

## 🔍 Technical Debt

### Code Quality Issues
1. **Debug Output**: Remove excessive println! statements after stabilization
2. **Error Messages**: More specific error types and messages
3. **Documentation**: Add comprehensive examples and API docs
4. **Testing**: Unit tests for individual components

### Architecture Improvements
1. **Feature Flag System**: Better organization of Ceph feature dependencies
2. **Type Safety**: More compile-time guarantees for version compatibility
3. **Memory Management**: Reduce allocations in hot paths

## 📚 Reference Materials Needed

### Ceph Source Analysis
- [ ] Complete mapping of all pg_pool_t versions (v1-v32)
- [ ] Documentation of pool_opts_t structure
- [ ] CRUSH algorithm documentation
- [ ] OSDMap incremental update mechanism

### Testing Resources
- [ ] Expand corpus with more diverse pool configurations
- [ ] Add negative test cases (malformed data)
- [ ] Cross-version compatibility test matrix

---

**Last Updated**: 2024-08-29
**Current Focus**: pg_pool_t encoding completion + VersionedEncode enhancement
**Next Milestone**: Complete pg_pool_t roundtrip + osd_info_t implementation