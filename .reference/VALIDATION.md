# Validation and finite reproduction

The reference answers the selected nonprimary-prefix boundary in
[#964 comment 6855786223](https://github.com/nashspence/riverhog/issues/964#issuecomment-6855786223).
Its audited base is `8ce49de94c32ae39562aab5ea4178b1dc8c75e77`.
The [implementation mirror](https://github.com/nashspence/riverhog/commit/e7e65990c2a92572ba00651126992eb2671f7f23)
has exactly the same tree as local tested producer checkpoint
`699d83d7ce78ed209a01f2c737803367d031275a`:
`1cd96b726681bbb32d6c8b85c592f76126ab9c05`.

## Actual checks

| Check | Result | Pytest elapsed | Maintained whole-command elapsed |
| --- | --- | ---: | ---: |
| Initial focused caller/delegate/lifecycle selection | 63 passed | 797.27 s | 836.701 s |
| Final new caller and recovery module | 15 passed | 454.21 s | 502.034 s |
| Full lint on final implementation | Passed: license, format, Ruff, mypy | — | 10.461 s |

The 15 new tests are included in the first 63; they were rerun after two local
variable type annotations and advisory helper timing instrumentation were added.
They are not 78 different tests. The initial checkpoint was
`3f6ca2fbf41b87c5b8b0713e5d8c1b13c1274078`. Its initial lint identified those two
missing annotations; final implementation lint passed after correction. The
proposed helper's executable verification logic did not change between runs.

Both test invocations began and ended with the same clean source checkpoint.
The final handoff changes only reference documentation/evidence relative to the
tested implementation. Product/test blob identity is verified during publication.
Any exact published-tip lint and remote verification performed after that
documentation commit are recorded in the owning-issue publication comment.

These are **focused checks** through maintained `make unit`. Their explicit
`TESTS`/runner overrides mean the Make receipts correctly say
`qualification_eligible: false`; no canonical full-unit or PR qualification is
inferred. The tests themselves passed, including their internal source-owned
growth assertions. Full lint is a normal eligible lint invocation.

The dependency tests include 1,180 root samples for indexed accepted-prefix
selection, nine detached-admission samples, nine currentness samples, and six
full sealed-result-admission samples. Each maintained witness validates its
samples against the owning declaration internally. The prefix witness covers
up to 1,025 journal frames, including historical headers; this is not a
1,000-member publication trial. These existing dependency paths are unchanged
between the initial and final implementation checkpoints.

## Maintained complete-caller witness

The new test uses four distinct members with the existing 28-byte fixture
payload, four genuine primary journals, and one genuine two-entry shared
journal. It selects entry zero, strictly before the physical journal tail.
Member batch size is one. Fanout is independently 1 or 4, and the selected
prefix's documentary text is independently 0 or 4,096 bytes. Each regime has
independently generated identities and real journal admission/validation.

The observed boundary is the **complete provenance-closure owner sequence**:
four member turns, the terminal complete-history/corpus turn, and the final
no-progress turn. Fixture construction, earlier admission, history staging,
and later archive publication stages are outside these counters. This is not
the original comment's 65,536-byte/member encrypted publication workload.

Observers delegate the actual Native advances, validation streams, retained
openings, and indexed anchors. They preserve returned objects and exceptions.
The sum of per-validation Native advances must equal all observed Native
advances across the complete sequence, so replay cannot be hidden in an
unattributed caller.

| Selected prefix bytes | Fanout | Proof openings / anchor lookups | All validation calls | All Native advances | All validation-input bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5,367 | 1 | 1 / 1 | 14 | 3,207 | 146,377 |
| 5,367 | 4 | 4 / 4 | 14 | 3,207 | 146,377 |
| 9,463 | 1 | 1 / 1 | 14 | 3,207 | 154,569 |
| 9,463 | 4 | 4 / 4 | 14 | 3,207 | 154,569 |

In all four regimes, the selected-proof helper performs **zero source payload
reads and zero Native validation advances**. It performs one existing-result
opening and one exact indexed header lookup per attachment.

The surrounding checks remain visible: each primary has three validations
(member semantics, complete-history closure, physical corpus); the shared
journal has one exact selected-prefix closure validation and one physical-tail
corpus validation. There are five distinct validated journals. The shared
prefix/corpus inputs total 11,905 or 20,097 bytes, independently of fanout.
The additional 4,096 documentary bytes therefore add 8,192 bytes across the two
preserved passes, not a factor multiplied by attachment count.

These observations support the targeted distinct shared-prefix work argument.
They do not establish the work bound of every other finalization phase or
selection pattern.

## Timing observations and remaining cost

The final run also measured each actual selected-proof call, including its
original receiving, using monotonic wall and current-thread CPU clocks.
The values below are sums within one regime, not medians or throughput rates.

| Selected prefix bytes | Fanout | Selected-helper wall sum | Selected-helper thread CPU sum |
| ---: | ---: | ---: | ---: |
| 5,367 | 1 | 18.888 ms | 16.681 ms |
| 5,367 | 4 | 84.435 ms | 75.554 ms |
| 9,463 | 1 | 41.702 ms | 39.388 ms |
| 9,463 | 4 | 61.418 ms | 56.608 ms |

Conditions: Linux x86_64, Python 3.12.3, SQLite 3.45.1, SQLAlchemy 2.0.51,
repository-locked dependencies, two pytest workers with load distribution.
Full lint ran concurrently during the early part of each test invocation.
The environment was not dedicated to timing. Observer overhead is included;
no matched uninstrumented or pre-change timing baseline was run. In particular,
these times must not be compared as a speedup against the comment's different
actual-publication fixture.

The helper interval excludes fixture admission, the finalizer's surrounding
transaction acquisition/commit, primary semantics, and the terminal corpus.
The much longer pytest times include genuine preparation/sealing and cleanup.
No time threshold is a correctness or growth gate.

The remaining per-attachment work is material. Existing admission declares
four catalog statements, four lease descriptor opens, three lease durability
calls, one read-only Native database open, and indexed metadata work; the
caller lookup and receiving add their own work. See the source-owned cost table
in [DESIGN.md](DESIGN.md). If a subsequent bounded caller measurement attributes
dominant cost to admission/leases, investigate reuse within the same fenced
transaction with separate currentness and receiving evidence. This reference
does not add that additional cache or infer a 100,000-member runtime.

## Recovery and corruption coverage

The new maintained cases cover source/capture/catalog replacement, missing
retained jobs, wrong exact anchors, genuine busy-workspace admission, service
restart, rollback and committed lost replies, and original Native receiving
interrupted before or after its physical effect.

The raw-corruption case changes a known documentary text value at identical
byte length, asserts every journal metadata field is unchanged, and proves the
changed selected prefix is independently Native-valid but has a different
anchor. The next member can advance provisionally. The unchanged final barrier
must then reject the externally supplied anchor mismatch while closure remains
false and no root receipt exists. Restoring the original bytes permits
completion. This tests the intentional detection-timing boundary directly.

The broader 63-test selection additionally exercises replacement claims,
changed pending cursors, transaction failure, terminal receiving versus forward
work, missing/replaced frozen workspaces and certificates, independent readers,
capacity, indexed historical-prefix corruption/correction, and the invariant
that every prefix is semantically checked even when the final graph would be
valid. The exact selector list is retained in the initial Make receipt.

## First reproduction step

Reconcile the current integration source and the timing boundary in
[DESIGN.md](DESIGN.md), then use the repository's pinned environment:

```bash
make lint
make unit \
  TESTS=tests/unit/test_selected_upload_journal_prefix.py \
  UNIT_PYTEST_ARGS='-n 2 --dist=load --instafail --durations=20 --durations-min=0.25 -p scripts.ci_timing'
```

The initial broader selector list and actual invocation parameters are in
[initial-unit-make.json](evidence/initial-unit-make.json). This environment
selected its already installed pinned tools with
`UV_RUN='mise x python uv -- uv run --locked --all-packages --group dev'` and
kept temporary files/timing output outside the source checkout. No new
developer-facing profiling command is introduced.

Current-source PR CI and tested-merge qualification, PostgreSQL execution,
other platforms, full encrypted publication, installed independent recovery,
whole-path G13 conformance, and the 1,000/10,000/100,000 publication ladder were
not run for this finite reference. Existing archive/recovery paths are unchanged;
that source fact is not a claim of newly executed end-to-end qualification.

## Evidence files

Start with [results.json](evidence/results.json) for the source map, environment,
derived observations and durations. [index.json](evidence/index.json) gives
byte counts and SHA-256 checksums. The final raw pytest record is
[final-unit.json](evidence/final-unit.json). The complete initial raw record,
including all dependency growth samples, is losslessly compressed in
[initial-unit.json.gz](evidence/initial-unit.json.gz). Original Make receipts
and console results are retained beside them. No timing record has been
relabeled to the reconstructed GitHub commit.
