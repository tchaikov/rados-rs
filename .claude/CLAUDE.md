# rados-rs

Native Rust RADOS client for Ceph. Single crate at `rados/` with modules: `denc`, `auth`, `cephconfig`, `crush`, `msgr2`, `monclient`, `osdclient`.

**Minimum Ceph version: Quincy (v17, April 2022).** Pacific and older are unsupported.

## Commands

```bash
# Build & check
cargo check --workspace
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- --no-deps -D warnings

# Unit tests (no cluster needed)
cargo test --workspace --lib

# Integration tests (requires running dev cluster)
CEPH_LIB="/home/kefu/dev/ceph/build/lib" \
ASAN_OPTIONS="detect_odr_violation=0,detect_leaks=0" \
CEPH_CONF="/home/kefu/dev/ceph/build/ceph.conf" \
  cargo test -p rados --tests -- --ignored --nocapture

# Start/stop dev cluster
cd /home/kefu/dev/ceph/build && ../src/vstart.sh -d --without-dashboard
cd /home/kefu/dev/ceph/build && ../src/stop.sh

# Decode corpus file
cd /home/kefu/dev/ceph/build && \
  env ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 \
  CEPH_LIB=/home/kefu/dev/ceph/build/lib \
  bin/ceph-dencoder type <TYPE> import <FILE> decode dump_json
```

Note: Hooks already run `cargo fmt` and `cargo clippy` after every Edit/Write/Bash.

## Workflow

- Read files before modifying them.
- Test after every change. Never accumulate multiple untested changes.
- Commit immediately after tests pass with a descriptive message.
- If a test fails, investigate — don't retry blindly.

## Encoding conventions

### Use Denc everywhere in high-level code

Raw `buf.put_u*_le()` / `buf.get_u*_le()` / `buf.remaining() < N` may only appear inside `impl Denc` or `encode_content`/`decode_content` bodies. High-level code must use:

```rust
self.field.encode(buf, features)?;
let field = Type::decode(buf, features)?;
```

### Version checks

Every `decode_content` must call `check_min_version!` with the Quincy-guaranteed floor:

```rust
crate::denc::check_min_version!(version, 20, "ObjectStatSum", "Quincy v17+");
```

Set the floor to the version Quincy always emits — don't accept older wire formats. Use `decode_if_version!` for fields added after the Quincy baseline.

### ZeroCopyDencode

Use `#[derive(ZeroCopyDencode)]` with `#[repr(C, packed)]` only for fixed-size POD types with no version headers and no variable-length fields.

### Field naming

Prefer simple names (`sec`, `nsec`) over C-style (`tv_sec`, `tv_nsec`). Use custom `Serialize` impls for JSON corpus compatibility.

## Error handling

- Propagate with `?`. No `.expect()` or `.unwrap()` in production code.
- No per-field `.map_err()` in Denc impls — just use `?`.
- Use `|_|` in `map_err` when source error is uninformative (channel closed, semaphore closed, timeout elapsed). Use `|e|` when it carries useful context (parse errors, I/O).

## Code smells to avoid

- Manual `buf.put_*/get_*` outside Denc impls
- Manual `buf.remaining() < N` checks outside Denc impls
- Duplicate constants or type definitions across modules
- `if version >= N` guards for versions below the Quincy floor
- `.expect()` / `.unwrap()` in production paths

## Ceph C++ reference (under ~/dev/ceph)

- `src/include/denc.h` — Denc encoding macros
- `src/osd/OSDMap.{h,cc}` — OSDMap encoding/decoding
- `src/osd/osd_types.{h,cc}` — OSD type definitions (pg_pool_t, pg_stat_t, etc.)
- `doc/dev/msgr2.rst` — msgr2 protocol specification
- Quincy worktree: `~/dev/ceph-worktrees/quincy`
- Corpus data: `ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/`
