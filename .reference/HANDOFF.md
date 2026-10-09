# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #960
Convention: #903
Audited base: main @ `9d15a170794fddcb66dafa863247959552f861e0`
Audited tree: `bc1bdc117f2f4aa9d845b3f9de0146db831b585d`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration: no additional exposed effort configuration recorded
Requested by: Riverhog maintainer
Published by: OpenAI ChatGPT through the connected GitHub integration

## Purpose and authority

This is an additional implementation reference for the excessively slow 128-file
qualification, not another proposed CI topology. It contains three focused
runtime changes, four regression-test files, and a repeatable native benchmark.
It supplements the earlier #960 design references; it does not supersede issue
decisions, grant itself integration authority, or qualify a release.

Authority remains with #960, subsequent maintainer decisions, and the repository's
normal integration rail. Reconcile against then-current main and the integration
agent's active candidate. Do not apply this branch mechanically. The exact
published commit recorded in #960 is the handoff identity.

The branch does not change fixture cardinality, recipes, compiled recipe identity,
archive format, authorization, source/output semantics, target-worker concurrency,
CI requirements, or qualification deadlines. It does not split the 128-member
collection into unrelated smaller collections. There are no changes to main.

## What is fixed

### 1. Stop re-verifying unchanged compiled policy on every planning read

`RetainedRecipeStore.load()` previously reparsed the recipe and its dependency
closure, reran interface conformance vectors, and verified the compiled body on
every read. Planning contexts create new store instances, so an instance-local
cache would not address repeated work across steps.

The patch memoizes only successful pure validation of the exact pair of stored
JSON documents. The process-local cache has eight entries and admits document
pairs of at most 256 KiB. Larger valid definitions still use ordinary validation:
this is a cache admission limit, not a recipe-size or collection-size limit.

Every load still queries the authoritative database. Missing rows remain missing;
changed bytes are revalidated; the caller's exact recipe identity is checked on
hits as well as misses. Retention/rebinding validation is unchanged. Failed
validation is not cached. Callers receive deep copies because frozen models can
still contain mutable nested JSON. No permission, claim, work state, selection,
credential, or runtime binding is cached by this change.

The native benchmark, using twenty fresh planning contexts per recipe, measured:

| Exact retained recipe | Before warm median | After warm median |
| --- | ---: | ---: |
| audio-archive | 143.53 ms | 6.58 ms |
| conformance-media | 145.42 ms | 7.40 ms |

These are approximately 20x faster retained-definition reads, not a claim of a
20x faster complete qualification. Cold validation remains. Raw samples, cold
measurements, recipe digests, and environment are in the accompanying JSON files.
The benchmarks ran on one shared native host while other tests were active; use
the provided script for controlled comparisons on the integration host.

### 2. Batch existing independent-subject observations

The FFprobe streams and canonical materialization-hint providers advertised a
preferred physical batch of one. Their declared interfaces already permit
independent-subject batching and the current delivery planner already honors it.
Both now advertise a bounded preferred batch of sixteen.

The tests drive the real SQL-backed delivery and accepted-evidence machinery:
128 stream subjects take eight physical deliveries instead of 128; 129 hint
subjects take nine instead of 129. All subjects are accepted exactly once and
the logical question and complete scope are retained. Thus these two stages use
17 rather than 257 physical jobs. This is not a count of every observation job
in the complete recipe.

Whole-scope provenance remains whole-scope, including the 129-member test.
Semantic observer contracts and compiled recipes are unchanged. Provider
**descriptor**, physical request, and accepted-evidence identities legitimately
change with the advertised execution characteristics; do not reuse old accepted
execution evidence or assert byte identity for those records.

### 3. Keep FFprobe scratch bounded while batching

The streams observer now deletes each materialized source after its facts have
been acquired, including error paths, before materializing the next source.
Otherwise increasing the batch could accumulate sixteen large plaintext sources
until job-end cleanup. Only observer-owned workspace copies are removed, never
Riverhog archive objects. Existing whole-workspace cleanup remains.

No extra encoder or observer workers are added. Per-subject heartbeat, normal
access checks, sealed execution deadlines, and result validation stay in place.
A failed member fails the batch; no successful partial result is substituted.

## Changed files

Runtime:
- `some-implementations/stove0/application/server/src/stove0_core/recipe_definitions.py`
- `some-implementations/stove0/observers/ffprobe/src/a_stove0_ffprobe_observer/observer.py`
- `some-implementations/stove0/observers/riverhog-provenance/src/a_stove0_riverhog_provenance_observer/observer.py`

New executable regressions, collected by the existing unit roots:
- `some-implementations/stove0/application/tests/test_retained_recipe_validation_cache.py`
- `some-implementations/stove0/application/tests/test_scale_observation_batches.py`
- `some-implementations/stove0/observers/ffprobe/tests/test_ffprobe_batches.py`
- `some-implementations/stove0/observers/riverhog-provenance/tests/test_hint_batches.py`

`.reference/` contains handoff/evidence only; do not introduce it into main as a
new runtime contract or a second policy inventory.

## Validation actually performed

- Read root AGENTS.md/README, #903, current #960 checkpoints, the relevant
  compiled-planning/observer/retention implementation, and existing focused tests.
- All **17 new regression cases passed** with the repository's unchanged strict
  warning policy in the source-native environment (11.42 seconds).
- Five selected performance regressions were run against untouched base runtime
  source with the new tests: all five failed as expected (11.66 seconds). They
  exposed repeated verification and physical counts of 128/129 instead of 8/9.
- **89 related tests passed, two deselected** (104.82 seconds), covering retained
  definitions, compiled observation/task/media planning, accepted observations,
  retention, recipe configuration, and both affected providers. This broader run
  used the external-environment adjustments below, not altered repository policy.
- `git diff --check` passed. All changed/new Python files parsed successfully.
- The repeatable twenty-read benchmark passed against base and patched source;
  compiled recipe digests matched between them. See the raw JSON evidence.
- Every uploaded source/test blob is checked against its local Git blob hash;
  the final published tree is to be checked against the producer's staged tree.

The tests use actual contract validators, provider implementations and SQL-backed
acceptance, with deterministic test doubles for external reads and FFprobe report
acquisition. They are not a substitute for final-image real media qualification.

## Environment and honest limitations

The available environment has Python 3.13.5, pytest 9.0.2, Pydantic 2.13.5,
SQLAlchemy 2.0.50, ruamel.yaml 0.18.17 and jsonschema 4.26.0. The repository pins
Python 3.12.3. Docker, mise, ruff and mypy are unavailable here. A locked uv setup
was attempted but failed because this environment could not resolve/download the
pinned Python distribution. Workspace source was loaded through PYTHONPATH;
missing rfc8785 was provided outside the repository from exact upstream v0.1.4
source (implementation Git blob `3137d3326b98938affadb1be711ee411eb2ab86e`).
No dependency substitutes or environment workarounds are committed to this branch.

The initial broader strict run reported 87 passes and three failures: SQLite
connection ResourceWarnings surfaced as unraisable warnings under Python 3.13,
and one YAML merge-rejection case failed with the available dependency stack.
A clean-base control reproduced the SQLite warning class and the same YAML case
(32 passes, two failures). The adjusted 89-test run filtered ResourceWarnings
externally and deselected that one YAML case plus an installed-distribution
metadata test whose baseline fails without workspace package metadata. No test
filter or warning relaxation is proposed for the repository. A wider baseline
comparison was stopped after six minutes and is not claimed as a completed run.

**Not performed:** locked full lint/unit/dist-smoke/build, full Linux qualification,
Docker/GitHub CI/CodeQL, the actual 128-file final-image run, or a hosted latency
comparison. No end-to-end runtime target or absence of all regressions is claimed.
The 84-minute-preview report remains historical evidence, not a measurement of
this patch. Long/large-media batches must still be checked against the existing
execution/result budgets before integration; this patch does not waive them.

## Reconciliation and integration checklist

The active integration checkpoint is #960 comment 6087475539. It already describes
retained-recipe caching, observation-dispatch fixes, scratch ownership, fixture
parity and local scheduling. This branch intentionally does not overwrite those
changes. The retained-definition cache is the likely overlap: keep one reconciled
implementation and carry over the mutation/deletion/failure/identity regressions
rather than layering two caches. Provider batching and per-source cleanup can be
reviewed independently.

1. Reconcile the three runtime changes and four test files with the current
   #948/#953/#960 candidate. Preserve its execution/dispatch and scratch fixes.
2. Run the new tests under the locked toolchain without the source-native
   environment exclusions. Retain ordinary repository test/lint policy.
3. Rebuild exact provider images and consume their current descriptors. Preserve
   the single 128-audio-plus-XMP input and the real complete target output; do not
   use cached qualification results or stale image/descriptor identities.
4. Run all four mandatory local gates, full Linux qualification, the real default
   128-file scale qualification, and independent exact-SHA CI/CodeQL according to
   the current integration rail. Also test slow/large inputs, mixed facts, a failed
   late batch member and cancellation with the real runtime budgets and scratch.
5. Record cold/warm planning time, physical observation counts by task, static
   validation/cache cost, provenance/evidence preparation, target execution and
   settlement separately. Confirm the 128-item scale witness actually completes
   with its expected output/lineage, not merely that its preview progresses.
6. Attribute any remaining cost before changing concurrency, fixture size or
   deadlines. These changes remove two measured sources of amplification; they
   are not permission to claim that all provenance/planning cost is solved.

Reproduce the retained-definition microbenchmark from the locked workspace:

```sh
mise x -- uv run --locked --all-packages --group dev \
  python .reference/benchmark_recipe_validation.py --output /tmp/recipe-loads.json
```

## Publication relationship

A comment on #960 must contain the new branch URL, exact published SHA, base,
producer/model and these validation limitations. The available GitHub connector
can publish branches/comments but exposes no native Development-link operation.
Record that limitation in #960 as permitted by #903; the Markdown link does not
pretend to be that native relationship.
