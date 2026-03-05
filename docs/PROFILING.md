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
