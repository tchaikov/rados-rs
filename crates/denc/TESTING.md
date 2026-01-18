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
3. Exports to JSON and performs **strict comparison** of the outputs
4. Reports exact matches, format differences, and decode failures

### Understanding the Results

The test performs **strict JSON comparison** and categorizes results into three groups:

- ✅ **Exact match**: Both decoders produced identical JSON output
- ⚠️  **Format difference**: Both decoded successfully but JSON output differs (needs custom serialization)
- ❌ **Decode failure**: One or both decoders failed to decode the sample (implementation bug)

Sample output:
```
Testing type: pg_t
    ✓ 016730c304e3a0e626723db78c36a261
    ✓ fdc845711dc13e1b6d32c29cfa9f9fe3
  Result: 10/10 exact match, 0 format differences, 0 decode failures

Testing type: eversion_t
    ✓ fd259e78b480855515f5e871a5b571d5
    ✓ fcf6c89e8ebea2811929b48e42f5fd83
  Result: 10/10 exact match, 0 format differences, 0 decode failures

Testing type: pg_pool_t
    ✗ rust dencoder failed for efa76e5beacb68688f21e74648f2aa3d: memory allocation error
  Result: 0/10 exact match, 0 format differences, 10 decode failures

Overall Results
Total samples: 83
  Exact match: 30/83 (36.1%)
  Format differences: 43/83 (51.8%)
  Decode failures: 10/83 (12.0%)

Types with format differences (need custom serialization):
  - utime_t
  - uuid_d
  - entity_addr_t
  - pool_snap_info_t
  - osd_xinfo_t
  - pg_merge_meta_t

Types with decode failures (implementation bugs):
  - pg_pool_t
```

### Expected Success Rate

The test requires at least a **20% exact match rate** to pass. This acknowledges that:
- Some types may have different JSON output formats (requiring custom serialization)
- Some types may have incomplete implementations (causing decode failures)

As of this writing, the exact match rate is approximately **36%**, with most failing samples being format differences that need custom serialization work.

### Format Differences

The test identifies types where JSON output format differs between `ceph-dencoder` and our `dencoder`. These differences are expected and tracked:

**eversion_t**: ✅ **FIXED** - Now uses `Padding<u32>` type to automatically skip `pad` field serialization  
**utime_t**: Rust uses `"sec"`/`"nsec"`; ceph uses `"seconds"`/`"nanoseconds"`  
**uuid_d**: Rust outputs byte array `[1, 2, 3, ...]`; ceph uses UUID string `"01234567-89ab-cdef-..."`  
**entity_addr_t**: Rust outputs sockaddr byte array; ceph uses formatted address string  
**pg_merge_meta_t**: Rust uses nested objects for PgId/EVersion; ceph uses formatted strings like `"2.1"` and `"4'5"` (extra padding fields fixed with `Padding<T>`)

To fix remaining differences, each type needs custom `Serialize` implementation to match ceph-dencoder's output format.

#### Padding Fields

The `Padding<T>` generic wrapper type automatically skips JSON serialization for padding fields that exist in binary formats but should not appear in JSON output. Use it with `#[serde(skip_serializing)]` for fields that match ceph-dencoder behavior.

### Legacy JSON Format Notes

The strict comparison test helps identify and track format differences. Examples of output differences:

- `ceph-dencoder` might output: `"source_pgid": "2.1"`
- `dencoder` might output: `"source_pgid": {"pool": 2, "seed": 1}`

### Troubleshooting

**Test fails with "ceph-dencoder not found":**
- Install the `ceph-common` package as described in Prerequisites

**Test fails with "Corpus not found":**
- Clone the corpus repository as described in Prerequisites
- Or set the `CORPUS_DIR` environment variable

**Low exact match rate:**
- Most differences are expected format differences, not bugs
- Check the test output to see breakdown of exact matches, format differences, and decode failures
- Format differences need custom `Serialize` implementations
- Decode failures indicate actual implementation bugs

## CI Integration

The corpus comparison test runs automatically in CI via the `.github/workflows/ci.yml` workflow. It:
1. Installs `ceph-common`
2. Clones the corpus repository
3. Builds the `dencoder` binary
4. Runs the test

The CI test must maintain at least a 20% exact match rate to pass.
