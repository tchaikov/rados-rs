# Denc Testing Guide

This document describes how to run the various tests for the `denc` crate.

## Unit Tests

Run the standard unit tests with:

```bash
cargo test --package denc
```

## Corpus Comparison Test

The corpus comparison test validates that our Rust implementation of dencoding matches the official C++ `ceph-dencoder` tool from the Ceph project.

### Prerequisites

1. **Install ceph-common package** (provides `ceph-dencoder`):

   On Ubuntu/Debian:
   ```bash
   sudo apt-get install ceph-common
   ```

2. **Clone the ceph-object-corpus repository**:

   ```bash
   git clone https://github.com/ceph/ceph-object-corpus.git /tmp/ceph-object-corpus
   ```

   Alternatively, set the `CORPUS_DIR` environment variable to point to your corpus location:
   ```bash
   export CORPUS_DIR=/path/to/ceph-object-corpus
   ```

### Running the Test

Run the corpus comparison test with:

```bash
cd crates/denc
cargo test --test corpus_comparison_test -- --ignored --nocapture
```

The `--ignored` flag is required because the test is marked as ignored (it requires external dependencies).

The `--nocapture` flag allows you to see the detailed output showing which samples passed or failed.

### What the Test Does

For each supported type (e.g., `pg_t`, `entity_addr_t`, `pg_pool_t`, etc.):

1. Finds all corpus samples in `archive/19.2.0-404-g78ddc7f9027/objects/<type_name>/`
2. Decodes each sample with both `ceph-dencoder` and our `dencoder`
3. Exports to JSON and validates both produce valid JSON output
4. Reports success/failure statistics

### Understanding the Results

The test measures the **decode success rate** - the percentage of samples where both decoders successfully decode the corpus.

- ✅ **Pass**: Both decoders successfully decoded the sample
- ❌ **Fail**: One or both decoders failed to decode the sample

Sample output:
```
Testing type: pg_t
    ✓ 016730c304e3a0e626723db78c36a261
    ✓ fdc845711dc13e1b6d32c29cfa9f9fe3
  Result: 10/10 samples passed (both decoded: 10)

Testing type: pg_pool_t
    ✗ rust dencoder failed for efa76e5beacb68688f21e74648f2aa3d: memory allocation error
  Result: 0/10 samples passed (both decoded: 0)

Overall Results
Total: 73/83 samples passed
Both decoders succeeded: 73/83 samples
Decode success rate: 88.0%
```

### Expected Success Rate

The test requires at least a **50% decode success rate** to pass. This acknowledges that:
- Some types may have incomplete implementations
- Some older corpus samples may have compatibility issues

As of this writing, the success rate is approximately **88%**.

### JSON Format Differences

Note that the JSON output format may differ between `ceph-dencoder` and our `dencoder`. This is expected and documented. For example:

- `ceph-dencoder` might output: `"source_pgid": "2.1"`
- `dencoder` might output: `"source_pgid": {"pool": 2, "seed": 1}`

The test validates that both can decode successfully, not that the JSON outputs are identical.

### Troubleshooting

**Test fails with "ceph-dencoder not found":**
- Install the `ceph-common` package as described in Prerequisites

**Test fails with "Corpus not found":**
- Clone the corpus repository as described in Prerequisites
- Or set the `CORPUS_DIR` environment variable

**Low success rate:**
- Some types may have known issues or incomplete implementations
- Check the detailed output to see which specific samples are failing
- File issues for consistently failing samples

## CI Integration

The corpus comparison test runs automatically in CI via the `.github/workflows/ci.yml` workflow. It:
1. Installs `ceph-common`
2. Clones the corpus repository
3. Builds the `dencoder` binary
4. Runs the test

The CI test must maintain at least a 50% success rate to pass.
