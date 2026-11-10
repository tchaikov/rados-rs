# Pinned OSDMap tool fixtures

These files are captured from the original OSDMap CLI scenarios using the
official, digest-pinned Ceph images:

| Release | Ceph source commit | Image digest |
| --- | --- | --- |
| Quincy v17.2.7 | `b12291d110049b2f35e32e0de30d70e9a4c060d2` | `a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada` |
| Tentacle v20.2.4 | `7f793731f1b39eb4f465e960113d2363c311b964` | `6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa` |

`ceph.conf.withracks` copies the shared source input from
`src/test/cli/osdmaptool/ceph.conf.withracks` (SHA-256 in `SHA256SUMS`), with
only its final blank line removed for the repository whitespace check.
`create-racks-*.osdmap` captures `create-racks.t`'s
`--create-from-conf ... --with-default-pool` setup. Its source PG `0.0`
result is retained in `create-racks-pg0-*.txt`: exactly `raw ([], p-1) up
([], p-1) acting ([], p-1)`.

`test-map-pgs-*.osdmap` captures `test-map-pgs.t` with 500 OSDs,
`pg_bits=4`, replicated size 3, and the source's `node straw 10 rack straw
10 root straw 0` CRUSH construction. `test-map-pgs-results-*.txt` is the
corresponding pinned-tool `--mark-up-in --test-map-pgs` output: pool 1 has
8,000 PGs and its size-3 count is 8,000. Rust tests replay all 8,000 PGs
against the captured CRUSH map; the tool output remains the independent
source for the workload counts.

`crush-*.osdmap` captures the create/export/import subset of `crush.t`.
`create-print-*.osdmap` captures `create-print.t`'s initial
`--createsimple 3 --with-default-pool` map. It has `max_osd=3` and default
pool 1 with `pg_num=pgp_num=192`. These release encodings are decode-only
fixtures; no Rust OSDMap encoder generates these bytes.

Regenerate captures or verify the committed hashes with:

```sh
python3 rados/tests/osdmaptool-fixtures/generate.py ../ceph
python3 rados/tests/osdmaptool-fixtures/generate.py ../ceph --check
```

The tool creates fresh OSDMaps, so `--check` verifies committed fixture
hashes and byte-compares each fresh release-specific `test-map-pgs` result
with its 500-row count fixture. It does not compare fresh randomized FSIDs
and timestamps byte-for-byte.

The source scenarios' CLI rendering, random-comparator invocation, and map
editor behavior are outside this client library because it exposes none of
those APIs. Loss: the Rust tests do not assert CLI text or implement map
editing. Replacement: decoded pinned maps and client placement assertions.
Risk: a CLI-only regression is not covered. Revisit if this crate exposes an
OSDMap editing or command-line API.

| Upstream case | Rust test | Coverage | Readiness | Verification |
| --- | --- | --- | --- | --- |
| `create-racks.t` `ceph.conf.withracks`, PG `0.0` | `osdclient_osdmap_test.rs::osdmaptool_create_racks_retains_pg0_source_result` | Ported map decode and empty raw/up/acting/primary results | Ready now | Passing offline for both pins |
| `test-map-pgs.t` 500 OSD, `pg_bits=4`, size 3, STRAW node/rack map | `osdclient_osdmap_test.rs::osdmaptool_test_map_pgs_replays_all_source_pgs` | Ported complete 8,000-PG workload; all 500 count/first/primary rows and size count | Ready now | Passing offline for both pins |
| `crush.t` create/export/import; `create-print.t` `--createsimple 3 --with-default-pool` | `osdclient_osdmap_test.rs::osdmaptool_crush_and_create_print_maps_decode` | Ported decode subsets | Ready now | Passing offline for both pins |
| v19.2.x external OSDMap corpus | `v19_*` tests in the three OSDMap integration test files | Partial: supplemental decode/placement coverage, distinct from pinned release gates | Blocked on `RADOS_OSDMAP_CORPUS_DIR` and `RADOS_OSDMAP_INCREMENTAL_CORPUS_DIR` | Blocked; selected tests fail if either input is absent or malformed |

The source links are [Quincy create-racks](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/osdmaptool/create-racks.t), [Quincy test-map-pgs](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/osdmaptool/test-map-pgs.t), [Quincy crush](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/osdmaptool/crush.t), and [Quincy create-print](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/osdmaptool/create-print.t). Tentacle runs the corresponding files at commit `7f793731f1b39eb4f465e960113d2363c311b964`.
