# Testing requirements

These requirements apply to agents implementing, porting, or reviewing tests
and Ceph-compatible behavior in this repository. Read them before starting
such work. Keep test documentation, source references, and reports in English.
This file defines the testing process, not a catalogue of component tests.
Keep case lists, fixtures, readiness and execution results beside the tests.

The goal is the same observable behavior as Ceph C/C++, including results,
ordering, errors, and failure handling. A plausible result or a passing smoke
test does not establish compatibility.

## Reference releases

Use both pinned releases when investigating and validating behavior:

| Release | Role | Commit |
| --- | --- | --- |
| v17.2.7 (Quincy) | Required compatibility baseline | `b12291d110049b2f35e32e0de30d70e9a4c060d2` |
| v20.2.4 (Tentacle) | Additional compatibility target | `7f793731f1b39eb4f465e960113d2363c311b964` |

Read the relevant tag or commit, not an unpinned branch. Use `git show` to
inspect another release without switching a shared Ceph checkout. Record
release-specific tests and behavior differences explicitly. Passing a test
against one release does not establish compatibility with the other or with
every intermediate release.

## Start with upstream tests

1. Before changing behavior, find the relevant upstream tests in both
   releases. Search the component's unit tests, librados/neorados where
   applicable, CLI/encoding tests, and relevant shell/QA tests. Record the
   searched paths and commands, including searches that found no counterpart.
2. Read each selected test together with its setup, helpers, fixtures,
   parameter instantiations, and assertions. A test name alone is insufficient.
3. Port the scenario and its assertions before fixing or implementing the
   behavior. Preserve inputs, weights, tunables, seeds, sample counts,
   operation order, boundary cases, failure cases, and expected results.
   Adapt C++ ownership and the test harness to Rust without weakening checks.
4. Run the port against the current implementation. For missing or incorrect
   behavior, record a functional failure, then fix the implementation and
   rerun the same test. A compilation error is not evidence of a behavioral
   mismatch. If the implementation already passes, record that result; do not
   introduce an artificial failure.
5. Run the applicable differential or live-cluster checks and update the
   component's parity inventory in the same change. Keep unresolved checks
   visible; do not report the feature as verified while required gates remain
   blocked.

Expected values must come from the pinned Ceph tests or tools, never from the
Rust implementation being tested. Rust-to-Rust encode/decode round trips and
distribution checks alone do not establish Ceph parity. Do not change an
expected value to accommodate a Rust failure. Reproduce suspected upstream
defects on the corresponding Ceph release and document the evidence first.

## Source references and fixtures

Every ported Rust test must have an adjacent comment identifying the original
test as `release/file::test_name`, where `file` is relative to the Ceph
repository root and `test_name` includes the suite. Include an immutable
commit permalink to the original definition, for example:

```rust
// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.indep_toosmall
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L115
```

List each upstream reference when one Rust test covers multiple originals.
Enumerate all parameterized variants in the test or its inventory entry. For
unnamed CLI/shell scenarios, use a descriptive local case identifier after
`::`, label it as locally assigned, and link to the exact command/assertion
block. Do not present an invented identifier as an upstream test name.

Keep fixture provenance beside the fixtures: original path, release, commit,
source permalink, SHA256, and any adaptations. For generated fixtures, also
record the generation command, inputs, and tool version from the same pinned
Ceph release. Reuse original regression maps and inputs rather than replacing
them with simpler synthetic examples. Preserve applicable copyright and
license notices when copying or adapting upstream code or data.

## Coverage and readiness inventory

Find and read the relevant component inventory beside its tests before
changing coverage, and update it in the same change. For an example of the
format, see the [CRUSH inventory](../rados/tests/crush/test-parity.md); its
case list and component-specific requirements belong there, not in this policy.

Maintain a component-level inventory as tests are investigated and ported.
Every discovered relevant test needs an explicit disposition; missing Rust
functionality is not a reason to omit it. A parameterized family may share a
row only if every variant is listed. Keep both release identities even when
their scenarios are identical, without counting one Rust test multiple times.

Each entry must include the upstream reference and permalink, checked
behavior, variants and fixtures, Rust test path/name (or `None`), adaptations,
and these separate statuses:

| Dimension | Values and meaning |
| --- | --- |
| Coverage | `Not ported`; `Partial` (identify missing assertions/variants); `Ported` (the complete scenario and assertions are executable) |
| Readiness | `Ready now`; `Blocked` (state the prerequisite); `Outside client scope` (give a concrete scope reason) |
| Verification | `Not run`; `Failing`; `Passing`; `Blocked` (record the command, environment, and result or blocker) |

`Ready now` means a faithful executable test can be written with the current
interfaces and available fixtures/oracle. It may expose an implementation
bug and fail. `Ported` describes the test, not successful compatibility.
A missing API, fixture, reference tool, or cluster must be named along with
the concrete action needed to unblock the affected step. For example, test
execution can be blocked even when the test has already been ported.

For each deferred, excluded, or semantically adapted case, record what is
lost, the reason, remaining coverage or replacement, risk, and revisit
condition. Administrative CLI or server-only behavior may be outside a
client library's scope; relevant client behavior exercised by those tests
still needs a disposition. Difficulty, implementation gaps, and passing smoke
tests are not scope exclusions. Record review decisions honestly; use
`Pending` when a disposition has not been reviewed.

## Observable behavior

- Cover all relevant algorithms, modes, configuration and feature variants,
  boundary values, failure cases, retries and state transitions. Do not narrow
  coverage to the easiest supported variant.
- Preserve exact outputs, ordering, lengths, sentinel values, error types and
  side effects. Check intermediate results when a pipeline's final output
  can hide an earlier error; set membership alone cannot prove ordering.
- Reuse upstream regression inputs and expected outputs. Preserve seeds,
  sample counts and acceptance criteria for statistical tests. Golden tests
  complement behavioral and statistical tests rather than replacing them.
  Printing-only diagnostics are not assertion-based compatibility checks.
- Preserve both expectations when releases differ and document which
  version or feature selects the behavior. Unsupported behavior remains a
  documented compatibility gap, not a passing substitute implementation.

## Execution and reporting

- Keep pure algorithm/codec tests runnable without a live cluster. Check in
  the fixtures needed for ordinary offline tests; do not depend on an
  author's home directory or an untracked temporary file.
- Declare external prerequisites for integration tests. An explicitly
  selected parity/integration check must fail clearly when required inputs
  are missing. Do not return successfully and silently skip assertions.
  Report ignored, skipped, and blocked checks separately from passing tests.
- Add focused Rust-specific tests for guarantees without an upstream
  counterpart, such as cancellation, resource limits, or Rust API validation.
  Label them `Rust contract; no upstream analogue found` and record the
  search in the inventory. They supplement Ceph parity tests.
- Start with the narrowest relevant Cargo test command, then run affected
  integration checks and the repository's formatting/lint/test checks
  appropriate to the change. Documentation-only changes need link/reference
  checks and `git diff --check`, not new Rust tests.
- Report exact commands, repository revisions, reference releases, relevant
  seeds/fixture hashes, observed results, and remaining gaps. Distinguish
  an initial pass from a reproduced failure followed by a fix.

Review must check source attribution, preserved assertions and variants,
fixture provenance, execution evidence, and dispositions for missing tests.
A source link without executable assertions is not a port, and an unexecuted
test is not verified compatibility.
