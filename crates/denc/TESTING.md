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

Testing type: utime_t
    ✓ ff8873641ba784c678ba185f7c7dad4f
    ✓ ff8641c91cd2caf286ae08ce212bbda1
  Result: 10/10 exact match, 0 format differences, 0 decode failures

Testing type: uuid_d
    ✓ 4662bdea8654776d486072ff7074945e
  Result: 1/1 exact match, 0 format differences, 0 decode failures

Testing type: entity_addr_t (Features: 0x40000000000000)
    ✓ ff36be165b5cfd422c5b4c6fb58d0c0e
    ✓ fdc3930aa227b1f48fd4bed04ba79bfe
  Result: 10/10 exact match, 0 format differences, 0 decode failures

Testing type: pg_merge_meta_t
    ✓ 25fffed8c8919b9fc1f82035e31e3e43
    ✓ f76105741a846b08ff2b262929d0a196
  Result: 2/2 exact match, 0 format differences, 0 decode failures

Testing type: pool_snap_info_t
    ⚠ d0d9b07f2a4738ea8d4962db29687585 - format mismatch (timestamp formatting)
  Result: 0/10 exact match, 10 format differences, 0 decode failures

Testing type: pg_pool_t
    ⚠ efa76e5beacb68688f21e74648f2aa3d - format mismatch (UTime formatting)
  Result: 0/10 exact match, 10 format differences, 0 decode failures

Overall Results
Total samples: 83
  Exact match: 53/83 (63.9%)
  Format differences: 30/83 (36.1%)
  Decode failures: 0/83 (0.0%)

Types with format differences (need custom serialization):
  - pool_snap_info_t
  - osd_xinfo_t
  - pg_pool_t

Types with decode failures (implementation bugs):
  - None! All types decode successfully.
```

### Expected Success Rate

The test requires at least a **20% exact match rate** to pass. This acknowledges that:
- Some types may have different JSON output formats (requiring custom serialization)
- Some types may have incomplete implementations (causing decode failures)

As of this writing, the exact match rate is approximately **64%**, with remaining format differences requiring context-dependent UTime serialization. **All types decode successfully** (0% decode failures).

### Format Differences

The test identifies types where JSON output format differs between `ceph-dencoder` and our `dencoder`. Progress on fixing format differences:

**eversion_t**: ✅ **FIXED** - Uses `Padding<u32>` type to skip `pad` field serialization  
**utime_t**: ✅ **FIXED** - Custom serialization with "seconds"/"nanoseconds" field names  
**uuid_d**: ✅ **FIXED** - Custom serialization formats as UUID string `"01234567-89ab-cdef-..."`  
**entity_addr_t**: ✅ **FIXED** - Custom serialization formats sockaddr as IP:port strings, uses v1/v2 type names  
**pg_merge_meta_t**: ✅ **FIXED** - Custom serialization formats PgId as `"pool.seed"` and EVersion as `"epoch'version"`  
**pool_snap_info_t**: ⚠️ **PARTIAL** - Needs timestamp string formatting for UTime fields (context-dependent)  
**osd_xinfo_t**: ⚠️ **PARTIAL** - Needs timestamp string formatting for UTime fields (context-dependent)

Remaining types require context-dependent UTime serialization (timestamp strings vs. object format).

#### Padding Fields

The `Padding<T>` generic wrapper type automatically skips JSON serialization for padding fields that exist in binary formats but should not appear in JSON output. Use it with `#[serde(skip_serializing)]` for fields that match ceph-dencoder behavior.

### Legacy JSON Format Notes

The strict comparison test helps identify and track format differences.

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
- **All types now decode successfully** (0% decode failures)

## CI Integration

The corpus comparison test runs automatically in CI via the `.github/workflows/ci.yml` workflow. It:
1. Installs `ceph-common`
2. Clones the corpus repository
3. Builds the `dencoder` binary
4. Runs the test

The CI test must maintain at least a 20% exact match rate to pass.
