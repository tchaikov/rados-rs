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

### CRUSH Mapping (crush crate)

**Benchmark**: `single_placement/replicas/3`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

**Benchmark**: `batch_placement/replicas/3`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

### Denc Encoding (denc crate - primitives)

**Benchmark**: `primitives/u32/encode`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

**Benchmark**: `primitives/bytes/encode/4096`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

### Message Encoding (denc crate - complex types)

**Benchmark**: `complex/entity_name/encode`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

**Benchmark**: `complex/entity_addr/encode/msgr2`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

**Benchmark**: `composite/message/encode`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

### Connection (msgr2 crate)

**Benchmark**: `hello_frame/encode`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

**Benchmark**: `client_ident_frame/encode`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

**Benchmark**: `message_frame/encode_1kb`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

### OSD Operations (osdclient crate)

**Benchmark**: `osdop_build/read/4096`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

**Benchmark**: `mosdop_encode/write/4096`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

**Benchmark**: `mosdop_full_message/write/4096`
- **Baseline**: TBD
- **Date**: TBD
- **Hardware**: TBD

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
