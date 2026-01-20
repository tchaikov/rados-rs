# Corpus Validation Infrastructure

This directory will contain corpus validation tests for verifying that our Rust implementations
correctly encode and decode Ceph data structures.

## Corpus Files

Corpus files are binary-encoded Ceph data structures used for validation. They are located at:
- Local: `~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects`
- GitHub: https://github.com/ceph/ceph-object-corpus

## Dencoder Tool

The `dencoder` binary is a Rust implementation of Ceph's `ceph-dencoder` tool. It can:
- Decode binary corpus files to JSON
- Encode Rust structures to binary format
- Compare encodings with Ceph's reference implementation

### Usage

```bash
# List available types
cargo run --bin dencoder list_types

# Decode a corpus file (once types are implemented)
cargo run --bin dencoder type <typename> import <file> decode dump_json
```

## Testing Strategy

As types are implemented in subsequent commits, they will be validated against corpus files:

1. **Unit Tests**: Each type includes unit tests for basic encoding/decoding
2. **Corpus Tests**: Integration tests validate against real Ceph corpus files
3. **CI Validation**: GitHub Actions runs corpus tests on every commit

## Adding New Types

When adding a new type:
1. Implement the `Denc` trait
2. Add unit tests
3. Register the type in `dencoder.rs`
4. Add corpus validation tests
5. Verify CI passes
