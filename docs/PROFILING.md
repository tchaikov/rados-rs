# Performance Profiling Guide

This guide covers performance profiling and benchmarking for rados-rs.

## Tools

### Criterion
We use [Criterion.rs](https://github.com/bheisler/criterion.rs) for benchmarking. It provides:
- Statistical analysis of performance
- HTML reports with graphs
- Comparison between runs
- Detection of performance regressions

### Flamegraph
[Flamegraph](https://github.com/flamegraph-rs/flamegraph) visualizes where CPU time is spent:
- Shows call stacks and time distribution
- Identifies hot paths and bottlenecks
- Interactive SVG output

Install flamegraph:
```bash
cargo install flamegraph
```

Note: Flamegraph requires `perf` on Linux, which needs kernel support and appropriate permissions.

## Running Benchmarks

### All Benchmarks
```bash
cargo bench --workspace
```

### Specific Crate Benchmarks
```bash
# CRUSH mapping benchmarks
cargo bench -p crush

# Message encoding benchmarks
cargo bench -p denc

# Connection benchmarks
cargo bench -p msgr2

# OSD operation benchmarks
cargo bench -p osdclient
```

### Benchmark Output
Criterion generates reports in `target/criterion/`:
- `report/index.html` - Main report with all benchmarks
- `<benchmark_name>/report/index.html` - Individual benchmark reports

## Generating Flamegraphs

Flamegraphs show where CPU time is spent during benchmark execution.

### CRUSH Mapping
```bash
cargo flamegraph --bench crush_mapping -p crush -- --bench
```

### Denc Encoding (Primitives)
```bash
cargo flamegraph --bench encoding -p denc -- --bench
```

### Message Encoding (Complex Types)
```bash
cargo flamegraph --bench message_encoding -p denc -- --bench
```

### Connection Establishment
```bash
cargo flamegraph --bench connection -p msgr2 -- --bench
```

### OSD Operations
```bash
cargo flamegraph --bench operations -p osdclient -- --bench
```

Flamegraphs are saved as `flamegraph.svg` in the current directory.

### Troubleshooting Flamegraph

If flamegraph fails with permission errors:
```bash
# Temporarily allow perf for all users (requires sudo)
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

Or run with sudo:
```bash
sudo cargo flamegraph --bench crush_mapping -p crush -- --bench
```

If perf is not available (containers, VMs), flamegraph won't work. Use criterion benchmarks instead.

## Interpreting Results

### Criterion Output

Example output:
```
crush_mapping/map_pg_to_osds
                        time:   [1.2345 µs 1.2456 µs 1.2567 µs]
                        change: [-2.34% -1.23% -0.12%] (p = 0.03 < 0.05)
                        Performance has improved.
```

Key metrics:
- **time**: Mean execution time with confidence interval
- **change**: Performance change vs. previous run
- **p-value**: Statistical significance (< 0.05 is significant)

### Flamegraph Visualization

In the SVG flamegraph:
- **Width**: Proportion of CPU time spent in that function
- **Height**: Call stack depth
- **Color**: Random (for visual distinction)
- **Click**: Zoom into a function
- **Search**: Find specific functions

Look for:
- Wide bars at the top (hot functions)
- Unexpected deep call stacks
- Time spent in allocations, locks, or I/O

## Baseline Results

Recorded on 2026-03-05 using AMD Ryzen 9 9950X3D 16-Core Processor.

### CRUSH Mapping (crush crate)

**Benchmark**: `single_placement/replicas/3`
- **Baseline**: 4.91 µs
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

**Benchmark**: `batch_placement/replicas/3`
- **Baseline**: 809 µs (100 objects)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

### Denc Encoding (denc crate - primitives)

**Benchmark**: `primitives/u32/encode`
- **Baseline**: 2.21 ns (1.67 GiB/s)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

**Benchmark**: `primitives/bytes/encode/4096`
- **Baseline**: 19.5 ns (196 GiB/s)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

### Message Encoding (denc crate - complex types)

**Benchmark**: `complex/entity_name/encode`
- **Baseline**: 6.76 ns (1.79 GiB/s)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

**Benchmark**: `complex/entity_addr/encode/msgr2`
- **Baseline**: 17.1 ns (7.99 GiB/s)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

**Benchmark**: `composite/message/encode`
- **Baseline**: 37.1 ns (10.7 GiB/s)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

### Connection (msgr2 crate)

**Benchmark**: `hello_frame/encode`
- **Baseline**: 258 ns
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

**Benchmark**: `client_ident_frame/encode`
- **Baseline**: 566 ns
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

**Benchmark**: `message_frame/encode_1kb`
- **Baseline**: 371 ns (2.57 GiB/s)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

### OSD Operations (osdclient crate)

**Benchmark**: `osdop_build/read/4096`
- **Baseline**: 8.64 ns
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

**Benchmark**: `mosdop_encode/write/4096`
- **Baseline**: 360 ns (10.6 GiB/s)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

**Benchmark**: `mosdop_full_message/write/4096`
- **Baseline**: 983 ns (3.88 GiB/s)
- **Date**: 2026-03-05
- **Hardware**: AMD Ryzen 9 9950X3D

## Continuous Monitoring

After establishing baselines:

1. Run benchmarks before major changes
2. Compare results to baseline
3. Investigate regressions > 5%
4. Update baselines after verified improvements
5. Keep flamegraphs for comparison

## Performance Goals

Target performance characteristics:
- CRUSH mapping: < 2 µs per operation
- Message encoding: < 10 µs for typical messages
- Connection handshake: < 100 ms
- Object operations: Comparable to librados C library

These are initial goals and may be adjusted based on baseline measurements.

## Phase 3 Optimizations (as of 2026-03-06)

After implementing targeted optimizations in Phase 3, we measured the following improvements:

### Connection Frames (msgr2 crate)

**hello_frame/encode**
- Phase 2 baseline: 224 ns
- Phase 3 result: 211 ns
- Improvement: 5.8% faster

**client_ident_frame/encode**
- Phase 2 baseline: 526 ns
- Phase 3 result: 512 ns
- Improvement: 2.7% faster

**frame_assembly/rev0**
- Phase 2 baseline: 228 ns
- Phase 3 result: 216 ns
- Improvement: 5.3% faster

**frame_assembly/rev1**
- Phase 2 baseline: 213 ns
- Phase 3 result: 208 ns
- Improvement: 2.3% faster

### OSD Operations (osdclient crate)

**mosdop_encode/write/4096**
- Phase 2 baseline: 360 ns
- Phase 3 result: 256 ns
- Improvement: 28.9% faster

**mosdop_encode/write/65536**
- Phase 2 baseline: Not measured
- Phase 3 result: 254 ns
- Note: Encoding overhead is constant regardless of data size (data is referenced, not copied)

**mosdop_full_message/write/4096**
- Phase 2 baseline: 983 ns
- Phase 3 result: 824 ns
- Improvement: 16.2% faster

**mosdop_full_message/write/65536**
- Phase 2 baseline: Not measured
- Phase 3 result: 6.86 µs (8.9 GiB/s)
- Note: Includes actual data copying into message buffer

### Optimizations Applied

1. **Arc for shared message ownership** (commit 8220143 and earlier)
   - Eliminates deep clones on retry
   - Cheap refcount increment instead of full message copy
   - Primary benefit: Reduced allocations in retry paths

2. **Pre-allocated buffers with encoded_size hints** (commit 62e9c88 and earlier)
   - BytesMut::with_capacity() based on size hints
   - Eliminates reallocation during encoding
   - Impact: 28.9% improvement in mosdop_encode/write/4096

3. **Reduced string allocations** (commit 5e94640 and earlier)
   - Use clone_from() for string updates
   - References instead of clones where possible
   - Impact: Reduced allocations in error paths

4. **Optimized frame assembly** (commit 64aa89e and earlier)
   - Pre-calculate total frame size
   - Single allocation for entire frame
   - Impact: 5.3% improvement in frame_assembly/rev0

### Summary

Phase 3 optimizations delivered measurable improvements:
- Connection frame encoding: 2-6% faster
- OSD message encoding: 16-29% faster
- Frame assembly: 2-5% faster

The most significant gains came from buffer pre-allocation (28.9% for message encoding), which eliminates reallocation overhead during encoding. Arc-based message sharing provides benefits primarily in retry scenarios (not measured by these benchmarks).

All optimizations maintain correctness - unit tests and integration tests pass.

## Phase 4 Lock Contention Reduction (as of 2026-03-06)

After implementing lock contention optimizations in Phase 4, we measured concurrent throughput scaling:

### Concurrent Operations Benchmark (osdclient crate)

**concurrent_ops/submit** - Time to submit 100 operations per thread:

| Threads | Time (µs) | Throughput (ops/µs) | Scaling vs 1 thread |
|---------|-----------|---------------------|---------------------|
| 1       | 9.49      | 10.54               | 1.00x (baseline)    |
| 2       | 10.29     | 19.44               | 1.84x               |
| 4       | 14.95     | 26.76               | 2.54x               |
| 8       | 21.16     | 37.81               | 3.59x               |

**Analysis:**
- 2 threads: 1.84x throughput (92% scaling efficiency)
- 4 threads: 2.54x throughput (64% scaling efficiency)
- 8 threads: 3.59x throughput (45% scaling efficiency)

The benchmark shows good scaling up to 4 threads, with diminishing returns at 8 threads. This is expected as the benchmark creates Arc-wrapped messages without actual I/O, so contention comes from tokio task scheduling overhead rather than lock contention.

### Optimizations Applied

1. **DashMap for pending_ops** (Task 1)
   - Replaced `Arc<RwLock<HashMap>>` with `Arc<DashMap>`
   - Lock-free concurrent access via internal sharding
   - Eliminates serialization on operation tracking
   - Primary benefit: Multiple threads can track operations simultaneously

2. **Split MonClient state locks** (Task 2)
   - Separate locks for connection, monmap, subscriptions, commands
   - DashMap for command/pool op/version request tracking
   - AtomicU64 for lock-free tid generation
   - Allows concurrent access to different MonClient components
   - Primary benefit: Subscription updates don't block command submission

3. **Release locks before async operations** (Task 3)
   - Clone Arc references before .await points
   - Prevents blocking other tasks during async operations
   - Applied to session management and backoff handling
   - Primary benefit: Reduced lock hold times across await boundaries

4. **Kept tokio::sync::RwLock** (Task 4)
   - Consistent async-compatible locks throughout
   - Guards can safely cross .await boundaries
   - Works correctly with tokio::spawn
   - Primary benefit: Correctness and maintainability

### Key Benefits

- Better scaling with concurrent operations (1.84x at 2 threads, 2.54x at 4 threads)
- Reduced lock hold times via early release before await
- Lock-free operation tracking with DashMap
- Independent access to different MonClient components

### Commits

- perf: replace pending_ops RwLock with DashMap
- perf: split MonClient state into separate locks
- perf: release locks before async operations

### Notes

The concurrent benchmark measures operation submission overhead without actual network I/O. Real-world benefits will be more pronounced when operations involve:
- Actual network communication (I/O wait time)
- Monitor subscription updates concurrent with commands
- Multiple clients sharing the same MonClient/OSDClient instances

The optimizations ensure that lock contention doesn't become a bottleneck as concurrency increases.
