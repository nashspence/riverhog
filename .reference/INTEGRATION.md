# #960 integration job: factor processing proof, keep every invariant

Audited base: `a26e214f77ab806e4230125bf7aad93f8c3328da`  
Owning authority: issue #960, especially maintainer decision
https://github.com/nashspence/riverhog/issues/960#issuecomment-6013807376

This document is deliberately prescriptive about **proof ownership** and
deliberately non-prescriptive about internal refactoring style. The integration
agent should be able to implement the work without re-litigating what may be
split, what must remain real, or what constitutes success.

## Outcome

Replace the current single `processing` Compose lane with independently
required same-SHA proofs so the remote gate can return to roughly 20-30 minutes
under normal hosted conditions.

Do not reduce coverage. Reduce the accidental Cartesian product of independent
dimensions.

The required proof set after integration is:

1. `storage`
2. `ingress-custody`
3. `processing-admission`
4. `processing-primary`
5. `processing-overlap`
6. `review-delivery`
7. `witnesses`

All seven are mandatory leaves of the same `CI gate`.

## Why this split is semantically valid

The current `processing` lane serializes several contracts only because one
shell process owns all their setup:

- exact FTP event/custody and cleanup;
- one 16-audio-plus-sidecar classified FTP collection;
- durable automatic-admission boundaries across API restart;
- two overlapping `conformance-media` routes;
- a second independently produced CLI collection;
- four real Opus target jobs across those two producers;
- full-cardinality output/lineage assertions;
- target/operational metrics;
- completed-work restart/replay;
- an unrelated 2 MiB retrieval-cache overflow assertion.

The only expensive multiplication that matters is executing both overlapping
routes against the full 16-audio collection while also executing both routes
for the second producer. The accepted #960 decision allows each dimension to be
proved where it is strongest instead of requiring all dimensions in one run.

The repository already contains the right authorities:

- `stove0.conformance-media/v1` owns the overlapping-route semantics;
- `stove0.audio-archive/v1` owns the ordinary single audio-archive route using
  the same real Opus target and observer/association machinery.

Do not add a CI-only production recipe or a "disable this route for tests"
escape hatch.

## Lane 1: ingress-custody

### Owns

Extract the current small same-visible-path FTP proof from the beginning of the
processing lane:

- two distinct completed FTP transfers reuse `same-path.bin`;
- both enter listener custody before adapter consumption;
- exact events produce distinct finalized Riverhog collections;
- restart `ftp-spool` and `ftp-listener`;
- claims, completion events, handoffs and handoff intents are reclaimed at the
  durable completion-log tip;
- both retained receipts survive;
- each finalized artifact is retrieved and byte/hash checked.

### Fixture

Exactly the existing two tiny byte strings. No media target execution.

### Does not own

The 16-file workload, Stove0 admission, Opus execution, or output settlement.

### Expected duration

Low single-digit minutes. If this becomes slow, treat that as an ingress
regression rather than hiding it inside processing.

## Lane 2: processing-admission

### Owns

Use the existing **16 WAV + one XMP sidecar** FTP fixture and preserve:

- interrupted first upload and exact REST resume;
- all 16 WAV files plus sidecar enter real FTP custody and finalize in Riverhog;
- `stove0/conformance` description/tag classification;
- Stove0 controller is offline while the producer finalizes so the catalog
  follower must reconcile a missed publication after restart;
- the full observer/classification path runs against the real 17-artifact input;
- drive `intent -> previewed -> work_bound` through the built scheduler/API;
- restart the accepting API between every durable boundary;
- the final accepted preview/work binding is exact and stable.

Strengthen this lane by inspecting the accepted full-cardinality plan at
`work_bound` and asserting:

- both `archive-audio` and `archive-audio-overlap` are selected;
- both target plans bind the intended input collection and exact artifact
  selection;
- the full 16-audio input cardinality (plus the associated sidecar where
  applicable) is represented by the accepted plan/evidence rather than being
  inferred from fixture construction alone.

### Stop point

Do **not** start the expensive target executions after `work_bound`.

The proof here is that the full fixture survives ingest, classification,
observation, planning and durable admission correctly. Target execution is
owned elsewhere.

### Why this matters

This preserves the strongest reason to keep the 16-file fixture while removing
its multiplication by both heavy routes.

## Lane 3: processing-primary

### Owns

This is the one full-cardinality real end-to-end processing proof.

Use:

- real FTP producer;
- 16 WAV + one XMP sidecar;
- Riverhog archive/custody;
- real Stove0 observer stack;
- existing `stove0.audio-archive/v1` single-route recipe;
- real `a-stove0-opus-target`;
- real output publication back to Riverhog.

The work should be created through maintained operator/API surfaces from the
exact FTP receipt. Do not modify the product recipe catalog to make this
possible.

Preserve the material assertions currently made against the
`archive-audio` result:

- completion is successful;
- output collection is real and finalized;
- output artifact inventory/cardinality is correct for the single-route recipe;
- exact source/associated sidecar behavior is preserved according to that
  recipe's actual output policy;
- derivation record is present and exact;
- processing claim input-set identity resolves back to the one intended input
  collection;
- target state/workspace/accepted-status metrics remain internally coherent;
- transfer metrics remain observational;
- restart the completed Stove0/API/worker/observer/target stack;
- querying/waiting again after restart converges without creating new work or
  mutating completed output.

If the single-route recipe's output inventory differs from today's
`conformance-media/archive-audio` child because its intent/output policy is
different, assert the **actual single-route contract** here. Do not copy an
expected filename set that belongs to the overlapping conformance recipe.

### Critical semantic requirement

This lane must remain a genuine:

FTP -> Riverhog -> Stove0 observers/planning -> Opus target -> Riverhog output

lifecycle. No prepared database snapshot or synthetic target result may replace
that path.

### Expected duration

This is expected to be the longest processing shard. The design goal is for one
full-size Opus route plus setup/settlement to fit roughly 20-30 minutes on a
normal hosted runner. Measure before changing fixture cardinality.

## Lane 4: processing-overlap

### Owns

Use `stove0.conformance-media/v1` with a **small real media fixture** and prove
the dimensions that do not require sixteen expensive transforms:

- two independent producers remain real;
  - one FTP-produced classified collection;
  - one CLI-produced classified collection;
- both producers select both overlapping routes:
  - `archive-audio`;
  - `archive-audio-overlap`;
- all four real target jobs execute through the actual Opus target;
- route intents remain distinct (including the existing 128/96 kbps
  conformance distinction);
- all four jobs settle successfully;
- target/result identities are distinct and correctly bound to producer/work;
- completed state survives restart/replay.

Use one WAV per producer. Retain an XMP sidecar on the FTP fixture so the
association/evidence path is exercised by an executing conformance recipe.

### Explicit serialization proof

Today's shell comment says the supplied Opus target intentionally admits one
target job at a time, but the current smoke mostly infers that from eventual
completion.

Make the intended serialization observable in this shard using an existing
maintained target/operator/state surface. Record enough status transitions to
prove that no more than one Opus job is executing simultaneously while all four
jobs make progress to completion.

Do **not** add a new public production API solely for this assertion. If no
maintained external status surface can prove concurrency exactly, prefer a
bounded internal qualification observation already available inside the
deployed target/state container, and keep it test evidence rather than a new
product contract.

### Why small cardinality is valid here

The overlap invariant is:

> two matching routes x two independent producers -> four real jobs, all
> serialized through one real target authority and durably settled.

It is not a sixteen-file scale invariant. Full-cardinality planning and
full-cardinality real transform are separately mandatory in the two preceding
lanes.

## Move cache overflow to storage

The existing processing lane creates a 2 MiB collection solely to prove that a
1 MiB local cache admission budget spills to the elastic cache and that status
accounting reports both stores.

Move that assertion intact to `storage`.

It exercises Riverhog retrieval-cache placement/accounting and does not depend
on Stove0 processing semantics. Keeping it in processing only lengthens and
complicates the wrong proof owner.

## Shared setup/refactoring guidance

The current `scripts/test_compose_smoke.sh` may remain one shared harness if
that keeps setup logic centralized. Do not copy hundreds of lines into seven
nearly-identical scripts.

A good implementation shape is:

- keep generic project/config/secret/bootstrap helpers shared;
- make lane ownership explicit;
- extract fixture-producing shell/Python snippets into functions where multiple
  lanes need them;
- give each lane its own fresh Compose project and durable state;
- never pass mutable Docker volumes/databases between Actions jobs;
- continue verifying exact source/config image labels before qualification;
- retain target-scoped BuildKit GHA caches as acceleration only.

The integration agent may instead split the shell into a small common library
plus executable scenario scripts if that makes state ownership clearer. The
semantic lane boundaries above are the requirement; file layout is not.

## Exact repository changes expected

### `scripts/ci_qualification.py`

Change `COMPOSE_LANES` from:

```python
("storage", "processing", "review-delivery", "witnesses")
```

to the seven-lane set above.

Update `compose_targets()` so each lane builds only the images its real
scenario uses. In particular:

- `ingress-custody`: Riverhog/test + FTP spool, no Stove0 images;
- `processing-admission`: Riverhog/test + FTP spool + Stove0 API/controller
  and observer/preflight images required by `conformance-media`;
- `processing-primary`: Riverhog/test + FTP spool + Stove0 observer stack +
  Opus target;
- `processing-overlap`: Riverhog/test + FTP spool + Stove0 observer stack +
  Opus target;
- review/witness/storage retain their current ownership.

Keep exact label/source verification and target-scoped cache settings.

### `scripts/test_compose_smoke.sh`

Expand the accepted lane selector and refactor the existing processing block
according to the ownership above.

Use the current phase anchors rather than line numbers:

- `ftp-custody`
- `stove0-admission`
- `stove0-processing`

The old `processing` selector must disappear from maintained lane ownership;
policy tests should fail if it survives as a hidden fifth execution path.

The all-local aggregate may still run every lane serially when invoked without
a selector.

### `Makefile`

The existing generic:

```make compose-shard COMPOSE_LANE=<...>
```

is sufficient. Update help text to list the new lane values.

Do not add seven unrelated Make implementations.

### `.github/workflows/ci.yml`

Let all required Compose shards run concurrently. Remove the current
`compose.strategy.max-parallel: 2` throttle (or set it to the full generated
lane count).

The maintainer decision removes the repository courtesy-cap requirement.
Standard public-runner/account concurrency is the capacity governor.

Also remove artificial throttles that prevent already-independent existing
work from running concurrently when doing so helps the 20-30 minute gate.
Do not change `cancel-in-progress: true`.

If total same-workflow leaves materially exceed account concurrency, prefer
bundling only the already-short repository checks rather than reserializing the
new processing proofs. One safe consolidation is a single short repository job
with separate steps for:

- lint;
- compile;
- c2sp-vectors;
- dist-smoke.

Keep PostgreSQL concurrency and filesystem recovery independent because they
have real environment/runtime cost and currently take materially longer.

The stable protected context remains `CI gate`; internal leaf names may change.

### Policy tests

Update/add tests so CI structure is executable policy:

`tests/unit/test_ci_qualification.py`
- exact seven-lane ownership;
- every lane selects a nonempty and appropriate canonical image set;
- local `linux-qualification` invokes every lane exactly once.

`tests/unit/test_makefile.py`
- fake-Docker smoke proves each new selector runs only its owned sections;
- `all` still runs the union;
- the old monolithic `processing` selector is rejected.

`tests/unit/test_github_actions.py`
- generated Compose matrix contains all seven required lanes;
- no Compose max-parallel value reserializes them;
- `CI gate` still requires the Compose job;
- cancellation and exact checkout remain intact.

Add a focused processing-factorization policy test if that is cleaner than
overloading the existing files. It should read the maintained lane/invariant
definition rather than duplicate a prose list in several tests.

## Concurrency model

Do not optimize for minimum runner count.

Current GitHub documentation gives the stated Free-plan account 20 concurrent
standard hosted jobs; public standard-runner use is free/unlimited. CodeQL may
occupy some account slots concurrently, so brief queuing is acceptable.

The important scheduling rule is:

> no heavyweight processing proof should wait behind another heavyweight
> processing proof because of a repository-imposed `max-parallel`.

If the graph needs slot reduction, consolidate sub-five-minute repository/image
bookkeeping before serializing target execution again.

## Measurement and acceptance

Before/after comparison must use hosted exact-SHA runs.

For the integrated candidate:

1. pass the four mandatory local gates;
2. pass complete clean-source `make linux-qualification`;
3. pass independent CI and CodeQL on the exact SHA;
4. retain machine-readable per-lane timings;
5. collect at least three successful comparable hosted runs.

Record at minimum:

- total required-gate critical path;
- each of the seven Compose lane durations;
- `processing-admission` observer/planning/admission time;
- `processing-primary` target execution + settlement time;
- `processing-overlap` four-job execution time and serialization evidence;
- input/output cardinalities and fixture identities.

Success means:

- every invariant in `PROCESSING_COVERAGE.json` is owned by at least one
  required lane and verified by executable assertions;
- no invariant is satisfied only by a unit/mock test when it was previously a
  real Compose proof;
- no production concurrency/recipe semantics were changed merely for CI;
- same-SHA gate remains exhaustive;
- normal hosted critical path is materially reduced, with roughly 20-30
  minutes the engineering target.

If the primary shard alone remains above 30 minutes, investigate the real Opus
execution/settlement cost before reducing the 16-input fixture. The accepted
factorization is intended to expose that cost cleanly.
