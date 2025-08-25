# RADOS-RS Development TODO

## 🔑 Authentication & Security
- [ ] **Complete CephX handshake implementation**
  - [x] Basic CephX structures and types
  - [x] Auth state machine framework
  - [ ] Missing CephXRequest implementations (encode/decode methods)
  - [ ] Missing CephXReply implementations (decode method)
  - [ ] Service ticket handling
  - [ ] Rotating key support
  - [ ] Challenge-response validation
  - [ ] Session key derivation
  
- [ ] **Auth integration with connection**
  - [ ] Auth negotiation in connection handshake
  - [ ] Message signing with session keys
  - [ ] Message verification
  - [ ] Automatic re-authentication on key expiry

## 📡 Messenger Protocol
- [ ] **Fix existing TODOs**
  - [ ] Proper message length calculation in `src/messenger/message.rs:75`
  - [ ] Handle complex auth scenarios in `src/messenger/connection.rs`
  - [ ] Implement keepalive ack handling in `src/messenger/connection.rs`

- [ ] **Enhanced message handling**
  - [ ] Message compression support
  - [ ] Message encryption
  - [ ] Priority queue for messages
  - [ ] Batched message sending

## 🗺️ Cluster Management (Critical Dependency)
- [ ] **Monitor client implementation (MUST COMPLETE FIRST)**
  - [ ] MonMap handling and updates
  - [ ] OSD Map retrieval and parsing
  - [ ] CRUSH map implementation
  - [ ] PG map handling
  - [ ] Cluster status queries
  - [ ] Monitor command support
  - [ ] Automatic map updates
  - [ ] Health check integration

- [ ] **Object location logic (Depends on Mon client)**
  - [ ] PG ID calculation from object ID
  - [ ] CRUSH algorithm implementation
  - [ ] Primary OSD determination
  - [ ] Replica OSD selection
  - [ ] Pool-specific placement rules

## 🎯 RADOS Operations (Depends on cluster maps)
- [ ] **Basic object operations**
  - [ ] Object PUT operation
  - [ ] Object GET operation
  - [ ] Object DELETE operation
  - [ ] Object STAT operation
  - [ ] Object listing (ls)

- [ ] **Advanced object operations**
  - [ ] Extended attributes (xattrs) support
  - [ ] Object classes and methods
  - [ ] Atomic operations
  - [ ] Batch operations
  - [ ] Range operations

- [ ] **Pool management**
  - [ ] Pool creation/deletion
  - [ ] Pool configuration
  - [ ] Namespace support
  - [ ] Pool statistics

## 🏗️ Architecture & Performance
- [ ] **Connection pooling**
  - [ ] Multiple connections per OSD
  - [ ] Connection load balancing
  - [ ] Connection health monitoring

- [ ] **Error handling & resilience**
  - [ ] Retry logic for failed operations
  - [ ] Circuit breaker pattern
  - [ ] Graceful degradation
  - [ ] Connection recovery

- [ ] **Performance optimizations**
  - [ ] Zero-copy message handling where possible
  - [ ] Async I/O optimizations
  - [ ] Memory pool for message buffers
  - [ ] Connection multiplexing

## 🧪 Testing & Quality
- [ ] **Unit tests**
  - [ ] Auth module tests
  - [ ] Messenger protocol tests
  - [ ] Message encoding/decoding tests
  - [ ] Error handling tests

- [ ] **Integration tests**
  - [ ] End-to-end connection tests
  - [ ] Real cluster operation tests
  - [ ] Performance benchmarks
  - [ ] Stress tests

- [ ] **Test infrastructure**
  - [ ] Mock Ceph cluster for testing
  - [ ] Test data generation
  - [ ] Automated test runs
  - [ ] Coverage reporting

## 📈 Advanced Features
- [ ] **High-level APIs**
  - [ ] Async/await object operations
  - [ ] Connection pools
  - [ ] Object streams
  - [ ] Batch operation builder

- [ ] **Monitoring & Observability**  
  - [ ] Metrics collection
  - [ ] Distributed tracing
  - [ ] Health checks
  - [ ] Performance monitoring

- [ ] **Advanced RADOS features**
  - [ ] Snapshots
  - [ ] Watch/Notify
  - [ ] Object locking
  - [ ] Transactions
  - [ ] Compression
  - [ ] Encryption at rest

## 🔧 Infrastructure
- [ ] **Documentation**
  - [ ] API documentation
  - [ ] Usage examples
  - [ ] Architecture guide
  - [ ] Performance tuning guide

- [ ] **CI/CD**
  - [ ] Automated testing
  - [ ] Code formatting checks
  - [ ] Security scanning
  - [ ] Release automation

## 🐛 Known Issues & Technical Debt
- [ ] **Code quality**
  - [ ] Remove unwrap() calls and improve error handling
  - [ ] Add comprehensive logging
  - [ ] Code style consistency
  - [ ] Dead code elimination

- [ ] **Performance issues**
  - [ ] Memory leaks investigation
  - [ ] CPU usage optimization
  - [ ] Network efficiency improvements

## 🎯 Current Priority (Recommended Order)

### Phase 1: Complete Authentication (High Priority)
1. Fix missing CephXRequest/CephXReply implementations
2. Complete CephX handshake flow
3. Integrate auth with connection establishment
4. Test with real Ceph cluster using admin credentials

### Phase 2: Monitor Client (CRITICAL - Must Complete Before OSD Operations)
1. **Implement Monitor connection and communication**
   - Monitor discovery and connection
   - Monitor map (MonMap) handling
   - Basic monitor commands
2. **OSD Map retrieval and processing**
   - Request and receive OSD maps from monitors
   - Parse OSD map structure and metadata
   - Track OSD states (up/down, in/out)
3. **CRUSH map implementation**
   - Parse CRUSH map from monitors
   - Implement CRUSH algorithm for object placement
   - Support for different CRUSH rule types
4. **PG map and placement logic**
   - Calculate PG ID from object name/ID
   - Determine primary and replica OSDs for each PG
   - Handle pool-specific placement rules

### Phase 3: Basic RADOS Operations (High Priority - Depends on Phase 2)  
1. Implement OSD client connections
2. Add basic object PUT/GET/DELETE operations
3. Implement object location resolution using cluster maps
4. Test basic operations against test cluster

### Phase 4: Enhanced Features (Medium Priority)
1. Add comprehensive error handling and retries
2. Implement connection pooling
3. Add extended attributes support
4. Performance optimizations

### Phase 5: Advanced Features (Lower Priority)
1. Snapshots and advanced RADOS features
2. Monitoring and observability
3. High-level convenience APIs

---

**Test Cluster Information:**
- Config: `/home/kefu/dev/rust-app-ceres/docker/ceph-config/ceph.conf`
- Account: `client.admin` 
- Test command: `cargo run --bin rados-client ceph-test`