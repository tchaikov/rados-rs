# CRUSH compatibility tests

The CRUSH client decoder and mapper are checked against Ceph Quincy v17.2.7
(`b12291d110049b2f35e32e0de30d70e9a4c060d2`) and Tentacle v20.2.4
(`7f793731f1b39eb4f465e960113d2363c311b964`). The offline suite has **101
passing tests** and uses checked-in maps and C-derived vectors, so it needs no
Ceph checkout, container, or cluster.

```sh
cargo test -p rados --test crush --offline
```

It covers compiled CLI maps, exact mappings and retry profiles, device classes
and reclassification, choose arguments, legacy buckets, weight/topology map
effects, and Tentacle MSR rules. OSDMap coverage additionally checks pinned
tool fixtures for raw/up/acting placement, primary affinity, overrides, feature
masks, node flags, and the original 500-OSD workload. The ignored live gate
checks source-order class writes, explicit pool-rule creation, class reweight
maps, monitor-class writes, and generated erasure rules on fresh pinned
clusters.

```sh
bash rados/tests/crush/reference/live-rust-crush-gates.sh quincy all
bash rados/tests/crush/reference/live-rust-crush-gates.sh tentacle all
```

The detailed case ledger, including what is deliberately outside this client
library and when to revisit it, is [reference/DISPOSITIONS.md](reference/DISPOSITIONS.md).
Fixture provenance, pinned image digests, hashes, and regeneration commands
are in [fixtures/README.md](fixtures/README.md),
[reference/README.md](reference/README.md), and
[osdmaptool-fixtures/README.md](../osdmaptool-fixtures/README.md).

The scope is client-visible decoding, queries, placement, and I/O. It does not
claim parity for Ceph's map editor, text/JSON/XML formatting, monitor allocation
or cleanup, balancer scoring, or CLI diagnostics. `straw2_stddev` remains
unported because the selected upstream tests print diagnostics without an
acceptance criterion. The Quincy type-123 tests are a mapping-only adaptation:
the C oracle proves type 123 and Erasure type 3 yield the same 508 ordered
outputs; decoding raw type 123 is not supported.

To add a newer Ceph release, pin its commit and official image digest in the
relevant generator, run the documented `--check` command without that flag to
capture its maps/vectors, inspect every difference, then rerun the offline
suite. Retain a new release-specific fixture only when bytes or client-visible
results differ.
