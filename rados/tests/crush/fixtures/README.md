# Ceph CRUSH golden fixtures

All maps and `.t` transcripts here are unmodified copies of
`src/test/cli/crushtool/` from both pinned releases:

- v17.2.7: `b12291d110049b2f35e32e0de30d70e9a4c060d2`
- v20.2.4: `7f793731f1b39eb4f465e960113d2363c311b964`

Every imported file was compared byte for byte across these commits. One
copy serves both releases. The twelve original golden transcripts contain
87,040 distinct expected mappings, not 174,080 independent cases.
`set-choose.t` adds 36,864 mappings, `bad-mappings.t` two ordered vectors,
and `test-map-firstn-indep.t` ten bad vectors across twenty mapping calls.
Expected results come from the committed Ceph
transcripts, never from the Rust implementation. No external tools or local
paths are needed to run `cargo test -p rados --test crush --offline`.
Locally generated regression data and their provenance live in
[reference](../reference/README.md#generated-mapper-regressions).

The first line of each `.t` retains the original crushtool command. For the
twelve original golden transcripts, Rust
loads the referenced binary map, applies those tunables and OSD weights,
executes the same rule for every seed and replica count, and checks exact
ordered vectors (including short results and NONE slots) and result-size
histograms. Console formatting and the map-modified advisory are CLI-only
and are not asserted. The helper checks the complete seed sequence and
sample count and internal histogram consistency before invoking the mapper,
so an early mapper failure cannot hide incomplete fixture data.

The original text maps `bad-mappings.crushmap.txt`, `set-choose.crushmap.txt`
`test-map-firstn-indep.txt` and `show-choose-tries.txt` are also compiled by each pinned crushtool.
Those generated binaries and their reproduction instructions live separately
in [reference](../reference/README.md#compiled-cli-maps). Rust decodes both
release variants and checks the original transcripts. The earlier
`functional::bad_mappings` direct-assembly test remains supplementary coverage
of the same two cases, not two additional upstream scenarios.

The following additional unmodified crushtool inputs are retained for the
weight/topology consumer tests. They are byte-identical at the two pins;
generated binaries and exact mapping vectors are documented in
[reference](../reference/README.md#retained-weight-and-topology-consumers).

| File | Source v17.2.7 / v20.2.4 | SHA256 |
| --- | --- | --- |
| `reweight.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/reweight.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/reweight.t) | `480006f39eb3dca311b0cf28f499841ba48394716ff3a191fa6197b1c6c5583f` |
| `reweight_multiple.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/reweight_multiple.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/reweight_multiple.t) | `ffa02cffa4e6a8887ba74b75b2c2fdc14105c37b013e9621677d02ebc542aa53` |
| `multitype.before` / `multitype.after` | [Quincy](https://github.com/ceph/ceph/tree/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool) / [Tentacle](https://github.com/ceph/ceph/tree/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool) | `0cdd4049797af8f536ffbc0088e2be3f06bc6a593dd444a966158720dcd10e20` / `65bb0dab74430497c14d0aec6f7c450aefa587f7d0b2157a03a62d5711b7baf0` |
| `simple.template.multitree` / `.reweighted` | [Quincy](https://github.com/ceph/ceph/tree/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool) / [Tentacle](https://github.com/ceph/ceph/tree/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool) | `e822339b487ae559ba24776fd7eb659325e4f54915cf04f6be2d984aba287ed3` / `fe641197e22d776e46800b9ef04eaad063e0b8f940355f9d45d08bcb0c24d9fe` |

Provenance: paths in the table are relative to `src/test/cli/crushtool/`;
links point to the exact releases above. Adaptations to fixture bytes: none.
`cmd-NN` in test source comments is a locally assigned identifier for the
corresponding unnamed command in each transcript.

Ceph's upstream copyright/license notices are preserved in `COPYING`,
`COPYING-LGPL2.1` and `COPYING-LGPL3`, copied from v17.2.7. These third-party
fixtures retain their upstream licensing; the workspace MIT declaration
does not replace it.

| File | Source v17.2.7 / v20.2.4 | SHA256 |
| --- | --- | --- |
| `device-class.crush` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/device-class.crush) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/device-class.crush) | `80efc22899ac0c09a7d98199f8b4a2b146b72694faa0129c54184c043b794a25` |
| `device-class.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/device-class.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/device-class.t) | `2192a56835468107ed91dbc83b5c7a4f3f74fe97f1e5d6c88399108b4abbf167` |
| `set-choose.crushmap.txt` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/set-choose.crushmap.txt) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/set-choose.crushmap.txt) | `6c29846b7d52c0f575cbdf73d0d98e86b151fe860740a4555e77aacf638b69cc` |
| `set-choose.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/set-choose.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/set-choose.t) | `612b54d0a0b2c0526698aa7b8d275dc6beedc58f1706e605df5cab3ea26ee8ae` |
| `test-map-firstn-indep.txt` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-firstn-indep.txt) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-firstn-indep.txt) | `44f9f719d65e8f70056482e4b3ee2df93fd08207bab2b75e8c2e39bba02d06b6` |
| `test-map-firstn-indep.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-firstn-indep.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-firstn-indep.t) | `15890b6666025c1735adb6add9c8263dfe9a20ccb436f74e7c7abd5dc8b58970` |
| `bad-mappings.crushmap.txt` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/bad-mappings.crushmap.txt) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/bad-mappings.crushmap.txt) | `7769a2304e7a81b864deb9e10eb550b882e12cb3093467f6c612f3a949bcbeeb` |
| `bad-mappings.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/bad-mappings.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/bad-mappings.t) | `1abb4d051849b501d5ab2ade6e869fc1e56105ac1e21a9b4421ec9bae463cfa4` |
| `show-choose-tries.txt` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/show-choose-tries.txt) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/show-choose-tries.txt) | `d84f192d7352fd023a4ff8a8960c38dfbf95b0c260f48a6f4f6c0cd358fe884d` |
| `show-choose-tries.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/show-choose-tries.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/show-choose-tries.t) | `93d3fb809a368f129dba43faa05ee7860ac4ae30949f263c24862d9c093bf51d` |
| `test-map-a.crushmap` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-a.crushmap) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-a.crushmap) | `3eacd04c60fa0143e298c49f6609a5b405fc01fc668f80386da28a8ea40781ca` |
| `test-map-big-1.crushmap` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-big-1.crushmap) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-big-1.crushmap) | `1c53e90b3756abebfad9ba9caa64186ab42af2e11cdb0ce5ef65efc264cb9ad3` |
| `test-map-bobtail-tunables.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-bobtail-tunables.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-bobtail-tunables.t) | `49302596753ab66180af0c2e06761bd5e59763f01df60730a5b5e122bd8fbc86` |
| `test-map-firefly-tunables.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-firefly-tunables.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-firefly-tunables.t) | `cc296abd018b76820566c6ae448108b945c00fb57892ed923b26d006628b3b94` |
| `test-map-hammer-tunables.crushmap` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-hammer-tunables.crushmap) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-hammer-tunables.crushmap) | `c1bf51b90ccdcc7dccef50a5d92384d444c44d75b7c3b4d915b8b30c03170b57` |
| `test-map-hammer-tunables.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-hammer-tunables.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-hammer-tunables.t) | `411c3fe95a514e7f3890879e40f8f1851a987cf57220574c453e3683728d22f0` |
| `test-map-indep.crushmap` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-indep.crushmap) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-indep.crushmap) | `7de3cd6cc1410bd0cf3729ba08c9572f4f6a9c9b696103f938b243e0a6682b9b` |
| `test-map-indep.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-indep.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-indep.t) | `a0a91533a680350a67699b16380280de15108ec6e87e4f1e780cc6f73ad25732` |
| `test-map-jewel-tunables.crushmap` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-jewel-tunables.crushmap) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-jewel-tunables.crushmap) | `4a54a318d1e9f33d1ed405edec87366add5e443433e4fcc40206f130fda46880` |
| `test-map-jewel-tunables.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-jewel-tunables.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-jewel-tunables.t) | `268b70f78c0d6490a31c92a73545832f77f95cb16c44564d0c7a837dc038da0c` |
| `test-map-legacy-tunables.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-legacy-tunables.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-legacy-tunables.t) | `9889916c3eace87448c043d6134b44347724e8207d7429dcb17950081336b3d3` |
| `test-map-tries-vs-retries.crushmap` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-tries-vs-retries.crushmap) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-tries-vs-retries.crushmap) | `df6669db23b0184704bb7b9b99d3d184f5d1ece01ef1607008365ac6df5c3d15` |
| `test-map-tries-vs-retries.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-tries-vs-retries.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-tries-vs-retries.t) | `fe39f33798696c16590154f2dbb7de4331873994fc1c2c41ca31642e103f9f5d` |
| `test-map-vary-r-0.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-0.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-0.t) | `30c80294d774f6a90e078a79eb8c7acffdfd20d3d7ae092d74fe981d9b4790f5` |
| `test-map-vary-r-1.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-1.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-1.t) | `f6fb2fed94167395b4efa80b7efb3b93c5fc14e93af183dcda19114723c2892b` |
| `test-map-vary-r-2.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-2.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-2.t) | `d6602de4e9601beec2dbcb394761a3068b5e9fc22b56482577ca62d98f18ceae` |
| `test-map-vary-r-3.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-3.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-3.t) | `294dbf3fe352e7a8c6ffda508832049ec1512841aad4dcd584d229ffb377e464` |
| `test-map-vary-r-4.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-4.t) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-4.t) | `27d83afe8c9927849c9b7aa655620fea3658a97f392a9f4d8b7306c444095e24` |
| `test-map-vary-r.crushmap` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r.crushmap) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r.crushmap) | `3c8979266fd148244ddd3dd05455f460c2fa411c5d40f4d7d788227716ae2b57` |

## Choose arguments

`choose-args/choose-args.crush` is an unmodified upstream CLI input; its
Quincy and Tentacle transcripts are separate because Tentacle prints MSR
tunables. `qa-update-one-more.crush` and `qa-no-update-one-more.crush` are
unmodified state maps published by `qa/standalone/crush-choose-args.sh` for
`TEST_choose_args_update` and `TEST_no_update_weight_set`, respectively.
Locally assembled compatibility, hosts-text rejection, and remaining QA state
data live in [reference](../reference/README.md#choose-argument-fixtures).

| File | Source | SHA256 |
| --- | --- | --- |
| `choose-args/choose-args.crush` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/choose-args.crush) / [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/choose-args.crush) | `fb2a66da99bbaa4e79a260ef6bb1432a55a0ed6475d1ca00594dcc83fd7dc8e5` |
| `choose-args/choose-args-quincy.t` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/choose-args.t) | `0d99826efd55c7f82546d9cba11ef666b69fb12afb09f6bf1ea9229e79158939` |
| `choose-args/choose-args-tentacle.t` | [Tentacle](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/choose-args.t) | `55673262931785674d1d047088ccf02dfa06ac87023948036e56e75408219cc9` |
| `choose-args/qa-update-one-more.crush` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush-choose-args-expected-one-more-3.txt) | `28bc2c4b4029e2ad4474bd62b689f0e7ecc3e6b8fe2c1660ebca00de3728cc57` |
| `choose-args/qa-no-update-one-more.crush` | [Quincy](https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush-choose-args-expected-one-more-0.txt) | `3460aba58ccbff4023e3126b77aca19c9ff97901b3f38a2a58d26072e775ba4e` |
