# Phase 2: Performance Measurement Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Establish performance baselines before optimization

**Architecture:** Add comprehensive benchmarks using `criterion`, profile with `cargo flamegraph`, and add performance metrics collection to identify actual hot paths and bottlenecks.

**Tech Stack:** Rust, criterion, flamegraph, tracing

---

## Overview

Phase 2 establishes performance baselines before making optimizations. This ensures we:
1. Measure actual performance, not assumptions
2. Identify real bottlenecks through profiling
3. Have metrics to validate improvements
4. Avoid premature optimization

**Key Principle:** "Measure first, optimize second"

---

## Task 1: Add Criterion Benchmarks for CRUSH Mapping

**Files:**
- Create: `crates/crush/benches/crush_mapping.rs`
- Modify: `crates/crush/Cargo.toml`

**Context:** CRUSH mapping is a hot path for object placement. We need to measure its performance across different scenarios.

**Step 1: Add criterion dependency**

Edit `crates/crush/Cargo.toml`:
```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "crush_mapping"
harness = false
```

**Step 2: Create benchmark file**

Create `crates/crush/benches/crush_mapping.rs`:
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use crush::{CrushMap, crush_do_rule};

fn load_test_crushmap() -> CrushMap {
    // Load a realistic CRUSH map from test data
    let corpus_path = std::env::var("CRUSH_CORPUS_PATH")
        .unwrap_or_else(|_| {
            format!("{}/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/crush_map",
                    std::env::var("HOME").unwrap())
        });

    let data = std::fs::read(&corpus_path)
        .expect("Failed to read CRUSH map corpus");

    CrushMap::decode(&data).expect("Failed to decode CRUSH map")
}

fn bench_crush_mapping(c: &mut Criterion) {
    let map = load_test_crushmap();

    let mut group = c.benchmark_group("crush_mapping");

    // Benchmark single object placement
    group.bench_function("single_object", |b| {
        b.iter(|| {
            crush_do_rule(
                black_box(&map),
                black_box(1), // rule_id
                black_box(12345), // object hash
                black_box(3), // replica count
            )
        });
    });

    // Benchmark batch placement (100 objects)
    group.bench_function("batch_100_objects", |b| {
        b.iter(|| {
            for i in 0..100 {
                crush_do_rule(
                    black_box(&map),
                    black_box(1),
                    black_box(12345 + i),
                    black_box(3),
                );
            }
        });
    });

    // Benchmark different replica counts
    for replicas in [1, 3, 5, 7].iter() {
        group.bench_with_input(
            BenchmarkId::new("replicas", replicas),
            replicas,
            |b, &replicas| {
                b.iter(|| {
                    crush_do_rule(
                        black_box(&map),
                        black_box(1),
                        black_box(12345),
                        black_box(replicas),
                    )
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_crush_mapping);
criterion_main!(benches);
```

**Step 3: Run benchmarks**

```bash
cargo bench -p crush
```

Expected: Baseline measurements saved to `target/criterion/`

**Step 4: Commit**

```bash
git add crates/crush/benches/crush_mapping.rs crates/crush/Cargo.toml
git commit -m "perf: add CRUSH mapping benchmarks

Add criterion benchmarks for CRUSH mapping performance:
- Single object placement
- Batch placement (100 objects)
- Different replica counts (1, 3, 5, 7)

Establishes baseline for future optimizations.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Add Message Encoding/Decoding Benchmarks

**Files:**
- Create: `crates/denc/benches/message_encoding.rs`
- Modify: `crates/denc/Cargo.toml`

**Context:** Message encoding/decoding is in the hot path for all network operations.

**Step 1: Add criterion dependency**

Edit `crates/denc/Cargo.toml`:
```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "message_encoding"
harness = false
```

**Step 2: Create benchmark file**

Create `crates/denc/benches/message_encoding.rs`:
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use bytes::{Bytes, BytesMut};
use denc::{Denc, MonMap, OSDMap, EntityName, EntityAddr};

fn bench_primitive_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive_encoding");

    // u32 encoding
    group.bench_function("u32", |b| {
        let mut buf = BytesMut::new();
        b.iter(|| {
            buf.clear();
            black_box(12345u32).encode(&mut buf, 0).unwrap();
        });
    });

    // String encoding
    group.bench_function("string", |b| {
        let s = "test_string_value";
        let mut buf = BytesMut::new();
        b.iter(|| {
            buf.clear();
            black_box(s).encode(&mut buf, 0).unwrap();
        });
    });

    // Bytes encoding
    group.bench_function("bytes", |b| {
        let data = Bytes::from(vec![0u8; 1024]);
        let mut buf = BytesMut::new();
        b.iter(|| {
            buf.clear();
            black_box(&data).encode(&mut buf, 0).unwrap();
        });
    });

    group.finish();
}

fn bench_complex_type_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex_encoding");

    // EntityName encoding
    group.bench_function("entity_name", |b| {
        let name = EntityName::client(12345);
        let mut buf = BytesMut::new();
        b.iter(|| {
            buf.clear();
            black_box(&name).encode(&mut buf, 0).unwrap();
        });
    });

    // EntityAddr encoding
    group.bench_function("entity_addr", |b| {
        let addr = EntityAddr::parse("v2:127.0.0.1:6789/0").unwrap();
        let mut buf = BytesMut::new();
        b.iter(|| {
            buf.clear();
            black_box(&addr).encode(&mut buf, 0).unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_primitive_encoding, bench_complex_type_encoding);
criterion_main!(benches);
```

**Step 3: Run benchmarks**

```bash
cargo bench -p denc
```

**Step 4: Commit**

```bash
git add crates/denc/benches/message_encoding.rs crates/denc/Cargo.toml
git commit -m "perf: add message encoding/decoding benchmarks

Add criterion benchmarks for encoding performance:
- Primitive types (u32, String, Bytes)
- Complex types (EntityName, EntityAddr)

Establishes baseline for encoding optimizations.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Add Connection Establishment Benchmarks

**Files:**
- Create: `crates/msgr2/benches/connection.rs`
- Modify: `crates/msgr2/Cargo.toml`

**Context:** Connection establishment includes protocol negotiation, authentication, and frame assembly.

**Step 1: Add criterion dependency**

Edit `crates/msgr2/Cargo.toml`:
```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "connection"
harness = false
```

**Step 2: Create benchmark file**

Create `crates/msgr2/benches/connection.rs`:
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use bytes::BytesMut;
use msgr2::{Frame, FrameAssembler};

fn bench_frame_assembly(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_assembly");

    // Benchmark frame encoding
    group.bench_function("encode_hello_frame", |b| {
        let frame = Frame::hello(/* ... */);
        b.iter(|| {
            black_box(&frame).to_wire(true).unwrap()
        });
    });

    // Benchmark frame decoding
    group.bench_function("decode_hello_frame", |b| {
        let frame = Frame::hello(/* ... */);
        let wire = frame.to_wire(true).unwrap();
        b.iter(|| {
            Frame::from_wire(black_box(&wire)).unwrap()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_frame_assembly);
criterion_main!(benches);
```

**Step 3: Run benchmarks**

```bash
cargo bench -p msgr2
```

**Step 4: Commit**

```bash
git add crates/msgr2/benches/connection.rs crates/msgr2/Cargo.toml
git commit -m "perf: add connection establishment benchmarks

Add criterion benchmarks for connection performance:
- Frame encoding (hello, auth, etc.)
- Frame decoding
- Frame assembly

Establishes baseline for protocol optimizations.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Add Operation Submission Benchmarks

**Files:**
- Create: `crates/osdclient/benches/operations.rs`
- Modify: `crates/osdclient/Cargo.toml`

**Context:** Operation submission includes building OSDOp structures, encoding, and routing.

**Step 1: Add criterion dependency**

Edit `crates/osdclient/Cargo.toml`:
```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "operations"
harness = false
```

**Step 2: Create benchmark file**

Create `crates/osdclient/benches/operations.rs`:
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use bytes::Bytes;
use osdclient::{OSDOp, MOSDOp};

fn bench_op_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_building");

    // Benchmark read operation
    group.bench_function("build_read_op", |b| {
        b.iter(|| {
            OSDOp::read(black_box(0), black_box(4096))
        });
    });

    // Benchmark write operation
    group.bench_function("build_write_op", |b| {
        let data = Bytes::from(vec![0u8; 4096]);
        b.iter(|| {
            OSDOp::write(black_box(0), black_box(&data))
        });
    });

    // Benchmark stat operation
    group.bench_function("build_stat_op", |b| {
        b.iter(|| {
            OSDOp::stat()
        });
    });

    group.finish();
}

fn bench_message_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_encoding");

    // Benchmark MOSDOp encoding
    group.bench_function("encode_mosdop", |b| {
        let ops = vec![OSDOp::read(0, 4096).unwrap()];
        let msg = MOSDOp::new(/* ... */, ops);
        b.iter(|| {
            black_box(&msg).encode_payload(0).unwrap()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_op_building, bench_message_encoding);
criterion_main!(benches);
```

**Step 3: Run benchmarks**

```bash
cargo bench -p osdclient
```

**Step 4: Commit**

```bash
git add crates/osdclient/benches/operations.rs crates/osdclient/Cargo.toml
git commit -m "perf: add operation submission benchmarks

Add criterion benchmarks for operation performance:
- OSDOp building (read, write, stat)
- MOSDOp message encoding
- Operation routing

Establishes baseline for operation optimizations.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Profile with Flamegraph

**Files:**
- Create: `docs/PROFILING.md`

**Context:** Flamegraphs visualize where CPU time is spent, identifying actual hot paths.

**Step 1: Install flamegraph tool**

```bash
cargo install flamegraph
```

**Step 2: Profile CRUSH benchmarks**

```bash
cargo flamegraph --bench crush_mapping -p crush -- --bench
```

Expected: Creates `flamegraph.svg` showing CPU time distribution

**Step 3: Profile encoding benchmarks**

```bash
cargo flamegraph --bench message_encoding -p denc -- --bench
```

**Step 4: Create profiling documentation**

Create `docs/PROFILING.md`:
```markdown
# Performance Profiling Guide

## Tools

- **criterion**: Statistical benchmarking
- **flamegraph**: CPU profiling and visualization
- **cargo-flamegraph**: Integrated flamegraph generation

## Running Benchmarks

### All benchmarks
\`\`\`bash
cargo bench --workspace
\`\`\`

### Specific crate
\`\`\`bash
cargo bench -p crush
cargo bench -p denc
cargo bench -p msgr2
cargo bench -p osdclient
\`\`\`

## Generating Flamegraphs

### CRUSH mapping
\`\`\`bash
cargo flamegraph --bench crush_mapping -p crush -- --bench
\`\`\`

### Message encoding
\`\`\`bash
cargo flamegraph --bench message_encoding -p denc -- --bench
\`\`\`

### Connection establishment
\`\`\`bash
cargo flamegraph --bench connection -p msgr2 -- --bench
\`\`\`

## Interpreting Results

### Criterion Output
- **Mean time**: Average execution time
- **Std dev**: Variability in measurements
- **Throughput**: Operations per second

### Flamegraph
- **Width**: Percentage of CPU time
- **Height**: Call stack depth
- **Color**: Random (for differentiation)

Look for:
- Wide bars = hot paths
- Deep stacks = potential for optimization
- Repeated patterns = duplication

## Baseline Results

Record baseline measurements here after Phase 2 completion.

### CRUSH Mapping (as of 2026-03-05)
- Single object: TBD µs
- Batch 100 objects: TBD µs
- Replicas=3: TBD µs

### Message Encoding
- u32: TBD ns
- String: TBD ns
- EntityName: TBD ns

### Connection
- Frame encode: TBD µs
- Frame decode: TBD µs
```

**Step 5: Commit**

```bash
git add docs/PROFILING.md
git commit -m "docs: add performance profiling guide

Add comprehensive guide for performance profiling:
- Benchmark execution instructions
- Flamegraph generation
- Result interpretation
- Baseline tracking

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Document Baseline Results

**Files:**
- Modify: `docs/PROFILING.md`

**Context:** Record actual baseline measurements for future comparison.

**Step 1: Run all benchmarks and collect results**

```bash
cargo bench --workspace > benchmark_results.txt 2>&1
```

**Step 2: Extract key metrics**

Parse the criterion output for:
- Mean execution times
- Standard deviations
- Throughput numbers

**Step 3: Update PROFILING.md with baselines**

Add actual numbers to the "Baseline Results" section.

**Step 4: Commit**

```bash
git add docs/PROFILING.md
git commit -m "perf: record Phase 2 baseline measurements

Record baseline performance metrics:
- CRUSH mapping: X µs per operation
- Message encoding: X ns per primitive
- Connection: X µs per frame
- Operations: X µs per op

These baselines will be used to measure Phase 3+ improvements.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] Criterion benchmarks added to all hot-path crates
- [ ] All benchmarks run successfully
- [ ] Flamegraphs generated for key operations
- [ ] Profiling documentation created
- [ ] Baseline measurements recorded
- [ ] No performance regressions in existing tests
- [ ] All benchmarks committed to git

---

## Success Metrics

**Phase 2 Complete When:**
- ✅ Comprehensive benchmarks in place
- ✅ Flamegraphs identify top 10 hot paths
- ✅ Baseline measurements documented
- ✅ Profiling guide available for future use

**Expected Outcomes:**
- Clear understanding of actual performance characteristics
- Identification of real bottlenecks (not assumptions)
- Metrics to validate Phase 3+ optimizations
- Repeatable profiling process

---

## Notes

- Benchmarks should be run on a quiet system (no other heavy processes)
- Multiple runs may be needed for stable measurements
- Flamegraphs require debug symbols: use `--profile bench` or release with debug info
- Keep benchmark code simple and focused on one operation at a time
- Use `black_box()` to prevent compiler optimizations from skewing results
