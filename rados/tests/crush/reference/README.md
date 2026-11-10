# Local reference runners and generated data

These helpers are written for rados-rs. They are **not copied Ceph tests**
and do not implement CRUSH. The C runners construct inputs and link the unmodified
`src/crush/mapper.c` and `hash.c` from the pinned Ceph commits. The functional
runner also links `builder.c` and `crush.c` to verify STRAW construction.

The current client-scope decisions, excluded producer behavior, and live-gate
recipe are in [DISPOSITIONS.md](DISPOSITIONS.md). This README contains the
complete reproducible tool, source, command, and artifact provenance.

| Program | Purpose and input origin |
| --- | --- |
| [mapper-regressions.c](mapper-regressions.c) | Generates local regression scenarios, not named upstream tests. Reproduction and output provenance: [generated mapper regressions](#generated-mapper-regressions). |
| [reference-check.c](reference-check.c) | Recreates the four dedicated Tentacle MSR setups, `crush_weights.sh`, both `bad-mappings.t` cases, Quincy type-123 INDEP equivalence, and the three assertion-bearing STRAW/STRAW2 cases. |
| [verify-reference.py](verify-reference.py) | Extracts pinned Ceph sources into a temporary directory, builds the C runner, and compares its results with an instrumented temporary copy of the Rust tests. |
| [prepare-cli.py](prepare-cli.py) | Verifies original inputs against both pinned commits, compiles four text maps with pinned Docker tools, checks all nine upstream test command outputs, and writes eight binary maps here. |
| [choose-args-reference.c](choose-args-reference.c) / [generate-choose-args-reference.py](generate-choose-args-reference.py) | Generates the choose-argument C vectors from pinned `mapper.c` and `hash.c`. |

For the functional reference comparison, run from the repository root:

```sh
cargo test -p rados --test crush --offline
python3 rados/tests/crush/reference/verify-reference.py ../ceph
```

The second command requires git, clang, rustc and a Ceph checkout containing
Quincy `b12291d110049b2f35e32e0de30d70e9a4c060d2` and Tentacle
`7f793731f1b39eb4f465e960113d2363c311b964`. It leaves that checkout unchanged.
No Ceph mapper source is vendored here; only an empty generated `acconfig.h`
is added during the standalone C build.

For `bad-mappings`, the C runner uses the original text map's five devices,
unit weights, STRAW root -1/type 1, hash 0, and rule IDs 0/1 (replicated
FIRSTN and erasure INDEP). It applies the compiler's legacy tunables and calls
the pinned `crush_make_straw_bucket`, asserting all five straws equal 65536.
The audit compares both seed-1/ten-replica results with the actual Rust test,
which asserts the upstream transcript's complete ordered vectors. This checks
placement and builder setup, not the text compiler or binary decoder.

For Quincy INDEP, it constructs the same source hierarchy twice with raw rule
type 123 and Erasure type 3, compares every ordered output before hashing, and
then checks the five resulting digests in both pinned releases. The domains are
100 each for `toosmall`, `basic`, `out_alt`, and `out_contig`, plus 4×27
progressive failures: 508 placement calls per type. Raw type 123 is test-only;
this proves the Rust test-harness adaptation and does not add decoder coverage.

For `straw_zero`, `straw_same`, and `straw2_reweight`, the runner invokes the
unmodified `builder.c` with `straw_calc_version=1`, prints the exact STRAW
weights/lengths, and streams all ordered outputs with the fixed word-FNV-1a
digest documented in the status page. It checks 10,000, 100,000, and 1,000,000
inputs respectively against a temporary instrumented copy of `weights.rs` for
both releases. The runner records the reference platform's unseeded
`rand()%10 == 7` (`changed_weight=45871`); no Rust RNG is substituted.

Cargo tests do not execute these helpers; they embed the generated
[mapper-regressions.txt](mapper-regressions.txt),
[mapper-retries.txt](mapper-retries.txt), and compiled maps stored here. Unmodified
upstream maps and transcripts live in [fixtures](../fixtures); investigation
details needed to reproduce generated data stay in this directory.

## Pinned crushtool containers

For reference compilation on an ARM64 host, use these official Ceph images
by platform-specific digest. Docker (Colima on macOS) is sufficient; no Ceph
cluster is required. Downloading requires network access; the tool runs offline.

| Release | Expected source commit | Linux ARM64 image digest |
| --- | --- | --- |
| Quincy v17.2.7 | `b12291d110049b2f35e32e0de30d70e9a4c060d2` | `sha256:a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada` |
| Tentacle v20.2.4 | `7f793731f1b39eb4f465e960113d2363c311b964` | `sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa` |

```sh
quincy_image=quay.io/ceph/ceph@sha256:a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada
tentacle_image=quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa
docker pull --platform linux/arm64 "$quincy_image"
docker pull --platform linux/arm64 "$tentacle_image"
docker run --rm --network none --entrypoint /bin/sh "$quincy_image" -ec 'ceph --version; rpm -qf /usr/bin/crushtool /usr/bin/ceph'
docker run --rm --network none --entrypoint /bin/sh "$tentacle_image" -ec 'ceph --version; rpm -qf /usr/bin/crushtool /usr/bin/ceph'
```

`crushtool` does not support `--version`; use `ceph --version` for the source
commit and check the owning packages for both tools. Verified on 2026-09-17:
Quincy uses `ceph-base`/`ceph-common` version `17.2.7-0.el8.aarch64`, Tentacle
uses `20.2.4-0.el9.aarch64`, and both source commits match the table.
Both tools compiled the original `bad-mappings.crushmap.txt` and
`show-choose-tries.txt`, reproducing their complete transcript outputs exactly
with networking disabled. Those smoke checks used temporary container files and
did not add generated fixtures.

Keep locally compiled maps and other generated data in `reference/`, with
the command, source inputs, tool version and SHA256;
`fixtures/` is reserved for unmodified files copied from upstream.

## Compiled CLI maps

Run from the repository root, with the above images already downloaded:

```sh
python3 rados/tests/crush/reference/prepare-cli.py ../ceph
```

The script checks every imported text map and transcript against both pinned
Git commits, verifies the container source commit and package versions, then
compiles each map using `crushtool -c /dev/stdin -o /dev/stdout`. Input bytes
come unchanged from `fixtures/`. It replays every original `--test` command,
changing only the input path to `-` to stream the compiled binary into the
container. All weights, tunables, rules, seeds, replica counts and output flags
are retained. Cram's `\t`/`(esc)` notation is decoded for output comparison.
Only after all nine command outputs match for both releases are files written.
No Rust code supplies any generated bytes or expected results.

The four source maps and transcripts are documented with immutable links and
SHA256 in [fixtures/README.md](../fixtures/README.md). `bad-mappings`,
`test-map-firstn-indep`, and `show-choose-tries` each have two test commands;
`set-choose` has three, covering 36,864 vectors. `show-choose-tries` retains
the two complete 50-bin profiles in the original transcript. Generated maps
retain the source fixtures' upstream license notices in `fixtures/COPYING*`.

All four Tentacle binaries equal the respective Quincy bytes followed by
two little-endian u32 MSR defaults (100/100). Both encodings are checked in
and decoded by Cargo tests, which need neither Docker nor a Ceph checkout.

## Retained weight and topology consumers

`generate-weight-topology-reference.py ../ceph --check` imports the identical
`reweight.t`, `reweight_multiple.t`, `multitype.*`, and
`simple.template.multitree*` sources from both pins, compiles their final
states, and captures every ordered mapping used by `weight_topology.rs`.
It also compiles source-shaped maps for `CrushWrapperTest.adjust_item_weight`,
`adjust_subtree_weight`, and `test_crushdiff.sh`: 128 one-replica rows for
each retained state and 1,000 three-replica rows before/after the six-OSD
crushdiff change. The latter has exactly 384 changed rows in both releases.

`TEST_crush_bucket` is captured from the existing self-cleaning pinned
one-MON/three-OSD runner. The generator performs its exact get/decompile,
ID/item/weight extraction, insertion, compilation, and empty-stderr check,
then retains the emitted blob. Both captures contain `host test` at `-3`, a
legacy STRAW item `-2` of raw weight `195`, and straw length `65536`.
The complete function is byte-identical at [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/test_crush_bucket.sh#L25)
and [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/test_crush_bucket.sh#L25),
SHA256 `46099b31dd2735d7b6cdb658e91ef94bee0c7ca32b8f77a77e92ae50e84c2406`.
Compiler stderr remains producer-only.

| Generated file family | Quincy / Tentacle SHA256 |
| --- | --- |
| `weight-topology-adjust-before` | `9a7f8e3bf24df2d2ad8d1e5cbe196d1e7f989144647d57f51a41c2e5204e4391` / `e1451553d1b7fff0419ef52f5f1a1cf3d16619dde51100cd16e8e8b4a526efc6` |
| `weight-topology-adjust-item-after` | `b4b09589d945aa49d772710b1ccd61a358597065a50e19e255ff75a7f88e2097` / `5906bf791b8839354ad762c422fa150297d0e10f498b46e13f09e6607d3e841f` |
| `weight-topology-adjust-subtree-after` | `2d9b7a58e54ddbb6ee6e5be10c70e7e5c73c714fca9cefd6c394c1fb92aa07de` / `84cc345fae1db888f80942823e4e1c5efba6e5e8a3a5d83c3228a104450610c1` |
| `weight-topology-multitype-after` | `88e6256147655b58411f8f5e292474a8b7e6587cdf564c25f79171e7310c7395` / `99de1a60c2d022920d3d1ca5f0fb31e9e303cdac7a17faf6c261dfc567259b67` |
| `weight-topology-multitree-after` | `3f07c6fbc256b08504c68690b3e0c6ac9bdb51f70814d0fcd3530a9010109250` / `4fb9a4266686c25434ca53ef4eaa9a466180a11903e4067dfa357fa42e49d273` |
| `weight-topology-crushdiff-before/after` | `25c27157347f02459b381b14a8cfa76f5cc38f89d67936f3fb87140a599a29b7` / `6739e91eba6efe197f6b58c1f1861ea392e844ccabdb24955f4297c819d41cef`; `3f4b7a8f163cc7c7ffbb2023ed921ce0378a558765528b9733b9aa2177f52834` / `5691e8441abb0cb7df7b6de00955692e988fed479187258444464e85af8da23a` |
| `weight-topology-added-straw` | `2b929333964ca6f6645848b7df6fcf7df208710cbecc588c106865a74ae16c15` / `e071c797bd656d9d9f6331781ee54335119fb332003108877b1c434ac11457aa` |
| `weight-topology-vectors.txt` | `3251593dd8c29ebea34e7b5d12fbb8d461dc5445e9ff55a6036ca6deacec19bd` |

## Legacy Uniform, List, and TREE buckets

`generate-legacy-bucket-reference.py ../ceph --check` verifies and regenerates
two minimal local C-mapper maps (`uniform-local.crush` and `list-local.crush`)
and the original `add-item-in-tree.t` final map. The local maps have no
selected upstream ordered mapping oracle; they use both pinned `crushtool`
releases to capture exact FIRSTN/INDEP rows for x=0..7, three replicas, and
complete, collision, unavailable-device retry, and NONE-hole partial-availability
cases. The TREE fixture replays all eight original additions, compares the decompiled
result with `tree.template.final`, and captures all three original rules for
complete and collision-shortened mappings. The upstream input/test bytes are
identical at both pins: `add-item-in-tree.t` SHA256
`6a66157de9981d5eb40ab291c43363e5a86f57f64411926287308bd895aeeb52`,
`tree.template` `0622bacb6b14b47c766bda9cd3a2e63d5f616c8c48cc4dd533361fcee8731b80`,
and `tree.template.final` `4a22c67c29bc3d9fc640a1fe7425d93ceac96414ca8320c759e66b5ae55bf1c8`.

TREE's `num_nodes` is an on-wire `u8` in both pinned `crush.h` definitions;
the generated map first exposed the prior four-byte Rust decode. Cargo now
checks decoded TREE node counts and 352 exact ordered C vectors across the
two releases. Generation uses no Rust encoder or mapper output.

| Generated file | SHA256 |
| --- | --- |
| `uniform-local.crush` / `list-local.crush` | `4a3fa26db6d2f73f0decfa63218e5199bf5fa27448266def4428cf659b1dfc53` / `0bde3495ffa474f9471a42d0a73cc6cd1dd1d45f6a8dea233d0906539dd5b700` |
| `uniform-local-quincy.crushmap` / `uniform-local-tentacle.crushmap` | `7c84afaf0210fd4823c946c98d8f4ee740cbe5aa1f1021e2a0dd765080ab0e44` / `eb6f3d56f38924d7ad23d7817af316865b0db523049b79d30b10e1b8515631b4` |
| `list-local-quincy.crushmap` / `list-local-tentacle.crushmap` | `a78c587501722ad0cfdfbb2bbdaa4755dbcbd8ee8333c054530bd4aaea049d01` / `7fe4dbf98b95ac20650c0f7f1dcabd26140bb6a4e0f159480f08092effe9e1f4` |
| `tree-add-item-quincy.crushmap` / `tree-add-item-tentacle.crushmap` | `6abad2e583a755308f8e877b71fac84717f1291518e4f4f4f164e53765351eeb` / `156a8e40e146044d78a426f5bcc16b51f7f08693d598d828c5d72d77b8ca024b` |
| `legacy-bucket-vectors.txt` | `7434991dd627b959d6fd95a3e2995025d9d7107c95d5e7c0fa79aeed2823cee1` |

For a newer pinned Ceph release, add its name, version, commit, and image
digest to the generator's `releases` tuple, run it without `--check`, inspect
the new release-specific maps and vectors against these rows, then run
`cargo test -p rados --test crush --offline`. Keep a new fixture only when
bytes or vectors differ.

## Device-class fixture

`device-class.crush` and `device-class.t` are unchanged copies from both pins.
`generate-device-class-reference.py` verifies each source copy, performs the
original compile/decompile/recompile byte comparisons in each network-disabled
container, and writes the two binary encodings plus the C placement grid for
rules 1/2, x=0..19, replicas 3. The grid is a local differential addition;
each result is checked to belong to the requested class. Regenerate or verify
it with:

```sh
python3 rados/tests/crush/reference/generate-device-class-reference.py ../ceph --check
```

| Generated file | SHA256 |
| --- | --- |
| `device-class-quincy.crushmap` | `27f242f550bac5045e996f755316657ddf1f9113053a8c6641aec04d230754ed` |
| `device-class-tentacle.crushmap` | `6ba67dfe56744d233a7b8b8b12671adaff002715c0a3eb8a5c9a206b8d4b6fa8` |
| `device-class-vectors.txt` | `77c06b8a96b7c091e6115633c558b87b39958c8a421226799c5fc8a3066d6e19` |

## Reclassification and monitor-class captures

`generate-reclassify-reference.py ../ceph --check` verifies the unchanged
`fixtures/reclassify.t` and ten original maps at both pins. Its 880 C digests
cover ordered before/after mappings for every rule, replicas 1..10 and x=0..1023.
It preserves `e` 6540/8417, `c` 158/138/0, and `gabe2`/`f` 627/652 movements;
`gabe` remains the transcript's producer-side failure.

`generate-mon-classes-reference.py ../ceph --check` uses adjacent self-cleaning
`live-mon-classes-preflight.sh` to compare fresh pinned-C lifecycle maps and
240 class-rule vectors. The non-root three-OSD memstore topology leaves no
persistent cluster state. It extracts `TEST_mon_classes` from both pins,
checks its identical SHA256 `2a9140cf59fe452de815aa7f61cff56f17c1435a0a3346a2d663eed05be05639`,
extracts its 30 state-changing commands (including idempotent and expected-failure
operations), and runs that source-derived sequence. Sources:
[Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/qa/standalone/crush/crush-classes.sh#L166)
and [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/qa/standalone/crush/crush-classes.sh#L166).

| Generated file | SHA256 |
| --- | --- |
| `mon-classes-removed-quincy.crushmap` | `acd5cef0b526aead6b6766b00fe09a3afda0292b4693cb34a4dc1f07c18f7855` |
| `mon-classes-removed-tentacle.crushmap` | `134c0eb4946f1906d37f5385cb58b1664021e6c1e9db7db4cf07f16c2ebdec98` |
| `mon-classes-asdf-quincy.crushmap` / `mon-classes-asdf-tentacle.crushmap` | `984a961ff5045b53fe63b35e066bec5a4b7c580f3fc4dcba5eaf228f0cfa3deb` / `8ab2044dc534bcdb53f2efa94d96347c341d738cd1c259a280a4abdbfcef5048` |
| `mon-classes-abc-quincy.crushmap` / `mon-classes-abc-tentacle.crushmap` | `a985e3bb855fd009fcb73450084683b57e2dcd2baa804dc67474a02933ad0bf6` / `d7674ea9e91d14a44a9d41a66e7161a8beadc8dc6064faa83a8f20935a7aa4d0` |
| `mon-classes-class2-quincy.crushmap` / `mon-classes-class2-tentacle.crushmap` | `f0be7a5b8245ec8f36c597ab40ce4ddd3f281141db17f19da0e375080b5b0768` / `dc1784f18076c0a15e2a1d977087e11c5dded409b399e6b27a3fc7f3becfe0bc` |
| `mon-classes-vectors.txt` | `43dc7a08206d7ae3229014210afbf0b3f5fe6c0f5185064d89868cfbb041faa3` |

## Choose-argument fixtures

`choose-args-compat-*.crushmap` is produced by each pinned `crushtool` from
the documented `choose_args_compat` setup, then re-encoded by its matching
`ceph-dencoder type CrushWrapper ... set_features ... encode`. The feature
masks are `CRUSH_TUNABLES5|INCARNATION_2` (`432345564227567616`) and that
mask plus `CRUSH_CHOOSE_ARGS` (`432345564229664768`). Quincy uses the
matching `ceph-dencoder` from the pinned official container.
No Rust encoder produces these bytes. The enabled fixtures retain default
index `-1`, one b1 position and weight `666 * 65536`; legacy fixtures contain
no choose set and b1's base weight is `666 * 65536`.

`choose-args-{quincy,tentacle}.crushmap` is the matching enabled encoding of
the unmodified CLI map. It retains the empty index 1 and signed IDs as hash
inputs. Cargo tests consume only these checked-in bytes.

| Generated file | SHA256 |
| --- | --- |
| `choose-args-compat-quincy-legacy.crushmap` / `choose-args-compat-tentacle-legacy.crushmap` | `27d411d512ff7ce0306042903de13495919d9f0ec417c01923fc79c2273afc26` |
| `choose-args-compat-quincy.crushmap` / `choose-args-compat-tentacle.crushmap` | `8dbb3ded6c68c6de7c1d8768c7d02b7cc4d170af175c8eb583811c04b41ab8a3` |
| `choose-args-quincy.crushmap` / `choose-args-tentacle.crushmap` | `d5ee1e866b6c37fcfdd5edacba1e12395c2eed2278c3f0349fb926b6874a6286` |

`choose-args-vectors.txt` has 2,100 ordered C vectors: direct FIRSTN and
INDEP, both recursive CHOOSELEAF operations, chained choices, and Tentacle
MSR INDEP/FIRSTN with an unavailable OSD. Every scenario covers absence,
present-empty, and selected argument sets for seeds 0..99. Quincy and
Tentacle must agree for the first five scenarios; the MSR rows are Tentacle
only. Regenerate or check the checked-in output with:

```sh
python3 rados/tests/crush/reference/generate-choose-args-reference.py ../ceph --check
```

`choose-args-compat.crush`, `choose-args-hosts.txt`, `qa-reweight-final.crush`,
and `qa-move-final.crush` are local reference inputs. The compatibility input
reconstructs `CrushWrapperTest.choose_args_compat`; hosts text is the
source-shaped `check-invalid-map.t` rejection input; the latter two retain the
published final totals from `TEST_reweight` (10/9) and `TEST_move_bucket`
(FOO 6/3, osd.0 3/3, osd.1 3/0). They are explicitly not upstream fixtures.
`qa-update-one-more-quincy.crushmap` and `qa-no-update-one-more-quincy.crushmap`
are pinned-C compiled versions of the unchanged source maps in `fixtures/`.
All QA generated maps use the enabled feature mask above; no Rust encoder
produces them.

`generate-qa-choose-args-states.py` writes the twelve local source-shaped
intermediate QA maps named by `crush-choose-args.sh`; unchanged update/no-update
add states remain fixtures. `qa-intermediate-vectors.txt` retains ten ordered
`crushtool --test --show-mappings` rows for each intermediate state and
`qa-choose-args-vectors.txt` does the same for the four final/add states.
Both pins compile every map and produce identical rows. The update/no-update
maps select index 0 in Rust; the oracle uses an identical default-index spelling
because `crushtool` exposes no choose-index option.

| Generated file | Original text input | SHA256 |
| --- | --- | --- |
| `bad-mappings-quincy.crushmap` | `bad-mappings.crushmap.txt` | `e676cdf4743655ebc5a8efcef7cdb97a0203b3ce6b3bcc0000018adc5ddb6990` |
| `bad-mappings-tentacle.crushmap` | `bad-mappings.crushmap.txt` | `8d8338ee6bf406dbe5f6a7a1697136ce2e449628b25af0a57d342022887ab549` |
| `test-map-firstn-indep-quincy.crushmap` | `test-map-firstn-indep.txt` | `9d262b9c2781313e1d52af38b592358f104285a68c8ce4dff98b35817e5a81fe` |
| `test-map-firstn-indep-tentacle.crushmap` | `test-map-firstn-indep.txt` | `5c0cc6087199c4cf0e2af7e9bd4894ce256a5b5619d11e8f6e1d5c333363b90c` |
| `set-choose-quincy.crushmap` | `set-choose.crushmap.txt` | `39289b7b868a6aa73649749c035887c2e877c8ef1cc39e69f2174b4ad42dc731` |
| `set-choose-tentacle.crushmap` | `set-choose.crushmap.txt` | `8bc67cfd2d1fb08bd192946e271298553daf01d2971d113acc7daf9b89fcb549` |
| `show-choose-tries-quincy.crushmap` | `show-choose-tries.txt` | `e3d9bf356fcaac6c07a23051cf93663497c4aa53e1a624b2d4e6e19148d76c2d` |
| `show-choose-tries-tentacle.crushmap` | `show-choose-tries.txt` | `60facf6aaf3be5faa67507c881f5ffe26926f4b1fe6de3c6fd52c62f4d5f9454` |

## Generated mapper regressions

[`mapper-regressions.c`](mapper-regressions.c) is a local input generator,
linked with unmodified Ceph `src/crush/mapper.c` and `hash.c`. It is not an
upstream test port and contains no Rust-derived expected values. Sources:
[Quincy mapper](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/crush/mapper.c),
[Tentacle mapper](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c).
The rule steps, bucket IDs, weights, tunables and output limits are explicit
in the generator and mirrored in [`regressions.rs`](../regressions.rs).

[`mapper-regressions.txt`](mapper-regressions.txt) contains 11,200 rows:
`scenario out_mask seed result_length [ordered device IDs...]`. Each of
cases 0..6 covers all 16 availability masks of a two-host/two-OSD-per-host
STRAW2 map and seeds 0..99. Empty results and `CRUSH_ITEM_NONE` are preserved.
Cases 0, 1, 2 and 4 were generated independently with both releases and are
byte-identical (6,400 vectors). MSR cases 3, 5 and 6 apply only to Tentacle.
Cases 7..10 check additional malformed MSR blocks; their results are always
empty and are asserted directly instead of storing another 6,400 empty rows.
No negative MSR fanout is passed to the C oracle: that input has no safe C
contract and is covered by Rust validation tests.

[`mapper-retries.txt`](mapper-retries.txt) contains 1,400 rows for local cases
11..24: all-in inputs except cases 13..16 and 23..24, which mark OSD 0 out.
The former exercise recursive leaf retries; the latter exercise direct FIRSTN
fallback. Every case retains the C result length and ordered devices. Cases 11
and 12 verify that zero and negative values leave the prior
positive `choose_tries` or `chooseleaf_tries` override unchanged. Cases 13..16
cover positive and zero local/local-fallback retries with a negative repeat.
Cases 17..19 cover `vary_r` 2 or 0 and `stable` 0 with a negative repeat.
Cases 21..24 add direct FIRSTN coverage for local and local-fallback retries;
the positive and zero variants produce different C vectors under collision or
an out child.
The C mapper computes a recursive seed as `r >> (vary_r - 1)` when `vary_r`
is nonzero; the generated cases use only 0 and 2, avoiding an undefined C
shift count while exercising both branches.

| Case | Behavior |
| --- | --- |
| 0 | Chained INDEP skips unresolved failure domains. |
| 1 | FIRSTN honors explicit chooseleaf retries even with descend_once enabled. |
| 2 | A conventional rule skips CHOOSE_MSR. |
| 3 | MSR rejects a conventional CHOOSE step with an empty result. |
| 4 | EMIT clears its working set. |
| 5 / 6 | MSR FIRSTN / INDEP: fanout 2 x 2, truncated to three results, all availability masks. |
| 7 / 8 | MSR block missing TAKE / EMIT. |
| 9 | An invalid second MSR block discards output from the valid first block. |
| 10 | Device TAKE followed by CHOOSE_MSR is invalid. |
| 11 / 12 | Positive choose/chooseleaf retries survive zero and negative repeats. |
| 13 / 14 | Local retries: positive and zero values, each with a negative repeat. |
| 15 / 16 | Local fallback retries: positive and zero values, each with a negative repeat. |
| 17 / 18 | Recursive `vary_r`: positive and zero values with a negative repeat. |
| 19 | Recursive `stable=0` survives a negative repeat. |
| 20 | Direct FIRSTN ignores zero and negative choose-tries repeats. |
| 21 / 22 | Direct FIRSTN local retries: positive and zero values with a negative repeat. |
| 23 / 24 | Direct FIRSTN local fallback retries: positive and zero values with a negative repeat. |

SHA256:

- Generator: `68e1da540bb52b77a1397a5fd2898ac5a7613b8130ebb5ac273cbf0a0a0de4aa`.
- Existing vectors: `526fbf16663e42265899c0405e0213996c8cd14f391c4aeb52cd8273f0c3127c`.
- Retry vectors: `5fb381c0ddd483e7cf0a75f2731ee728dc4f242e7404158da5b3d5a457a5ba02`.

Generation environment: macOS, Apple clang 21.0.0 (clang-2100.1.1.101),
`-std=gnu99 -O2`. The only build adaptation is an empty generated
`acconfig.h` for these standalone C files. Source and header bytes, including
`include/int_types.h`, are taken directly from the pinned commits.

Reproduce from the Rust repository root with Python 3, clang and a Ceph Git
checkout containing both commits (adjust `ceph` below). This does not change
that checkout. Only regeneration needs these tools; Cargo embeds the result.

```python
from pathlib import Path
import subprocess as sp
import tempfile

ceph = Path('../ceph')
reference = Path('rados/tests/crush/reference')
revisions = {
    'quincy': 'b12291d110049b2f35e32e0de30d70e9a4c060d2',
    'tentacle': '7f793731f1b39eb4f465e960113d2363c311b964',
}
outputs = {}
with tempfile.TemporaryDirectory() as directory:
    for release, revision in revisions.items():
        root = Path(directory) / release
        (root / 'include').mkdir(parents=True)
        for name in ('mapper.c', 'mapper.h', 'hash.c', 'hash.h', 'crush.h',
                     'crush_ln_table.h', 'crush_compat.h'):
            (root / name).write_bytes(sp.check_output(
                ['git', '-C', str(ceph), 'show', f'{revision}:src/crush/{name}']))
        (root / 'include/int_types.h').write_bytes(sp.check_output(
            ['git', '-C', str(ceph), 'show', f'{revision}:src/include/int_types.h']))
        (root / 'acconfig.h').touch()
        command = ['clang', '-std=gnu99', '-O2', f'-I{root}',
                   str(reference / 'mapper-regressions.c'), str(root / 'mapper.c'),
                   str(root / 'hash.c'), '-o', str(root / 'run')]
        if release == 'quincy':
            command.append('-DQUINCY')
        sp.run(command, check=True)
        cases = (0, 1, 2, 4, *range(11, 25)) if release == 'quincy' else range(25)
        outputs[release] = {case: sp.check_output([str(root / 'run'), str(case)])
                            for case in cases}
    for case, data in outputs['quincy'].items():
        assert data == outputs['tentacle'][case]
    for case in (3, 7, 8, 9, 10):
        assert all(row.split()[3:] == [b'0']
                   for row in outputs['tentacle'][case].splitlines())
    (reference / 'mapper-regressions.txt').write_bytes(
        b''.join(outputs['tentacle'][case] for case in range(7)))
    (reference / 'mapper-retries.txt').write_bytes(
        b''.join(outputs['tentacle'][case] for case in range(11, 25)))
```

## Integer boundaries

`bucket-types.c` and `mapper-types.c` include unmodified `mapper.c` to call its
static helpers. The Rust regressions in `bucket.rs` and `tests/mapper_types.rs`
retain their outputs. Both pinned releases agree on the high-bit x/r vectors,
all 65,536 `crush_ln` inputs (word-FNV digest `655ae82589f114e5`), signed STRAW2
division, arithmetic FIRSTN shifts, and wrapped retry/fallback limits.

Reproduce from the repository root with a Ceph checkout at `../ceph`:

```sh
for pin in b12291d110049b2f35e32e0de30d70e9a4c060d2 7f793731f1b39eb4f465e960113d2363c311b964; do
    type_src=$(mktemp -d)
    git -C ../ceph archive "$pin" src/crush src/include/int_types.h | tar -x -C "$type_src"
    touch "$type_src/src/acconfig.h"
    for probe in bucket-types mapper-types; do
        cc -I"$type_src/src" -I"$type_src/src/crush" "rados/tests/crush/reference/$probe.c" "$type_src/src/crush/hash.c" -o "$type_src/$probe"
        "$type_src/$probe"
    done
done
```

The probes construct local boundary scenarios; they are not upstream test ports.
Cargo tests use the frozen outputs and require neither C compiler nor Ceph source.
