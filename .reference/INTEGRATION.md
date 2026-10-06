# #960 integration plan

Authoritative decision:
https://github.com/nashspence/riverhog/issues/960#issuecomment-6014447496

Audited base:
`f4ac676554d5e68cae629f79af618ce35e278a43`

This is the implementation handoff. It supersedes the earlier #903 reference at
`6c5084055bc59905bc8a616fc10e7d152e8f9611` wherever that reference differs.

## Desired result

Required per-SHA Compose qualification becomes:

```text
storage
ingress-custody
processing-admission
processing-e2e
processing-overlap
review-delivery
witnesses
```

All seven are required by the existing aggregate `CI gate`.

The goal is a normal hosted critical path around 20-30 minutes while retaining
the meaningful proof now owned by the monolithic processing lane.

Do not modify production recipe semantics, target concurrency, or scheduler
architecture to achieve this.

## Non-negotiable semantic rule

Every required per-commit processing execution uses the checked
`stove0.conformance-media/v1` recipe.

Do not use `stove0.audio-archive/v1` as substitute evidence for the
`archive-audio` conformance branch. They are different exact recipe/route
definitions.

The single-route recipe may still be used by the independent scale rail if that
is the cleanest way to preserve its historical scale purpose.

---

# 1. Extract `ingress-custody`

Move the existing same-path FTP proof out of processing without changing its
assertions.

Fixture:

- `first exact FTP event`
- `second distinct exact FTP event`
- both uploaded to visible `same-path.bin`.

Keep:

- both events enter listener custody before adapter consumption;
- no visible-path overwrite;
- two distinct finalized Riverhog receipts;
- restart `ftp-spool` and `ftp-listener`;
- exact cleanup of claims/completion-events/handoffs/handoff-intents;
- retained receipts;
- exact Riverhog retrieval and byte/hash comparison.

Do not start Stove0 in this lane.

Expected source owners:

- `scripts/test_compose_smoke.sh`
- `scripts/ci_qualification.py`
- workflow/policy tests.

This should be a very small required job.

---

# 2. Move cache overflow to `storage`

Move the existing 2 MiB overflow fixture and assertions intact:

- local cache admission budget = 1 MiB;
- overflow collection becomes ready in elastic cache;
- ready stores include `local` and `elastic`;
- status accounting reports the exact local budget.

This is Riverhog retrieval-cache placement/accounting, not Stove0 processing.

Do not duplicate it in both lanes.

---

# 3. Build `processing-admission`

This lane preserves the current full-cardinality control-plane proof and does
not execute expensive target jobs.

## Fixture

Exactly the existing representative media collection:

- 16 WAV files;
- one XMP sidecar;
- first WAV is interrupted and resumed at the exact REST offset;
- produced through the real FTP spool into Riverhog;
- classified for `stove0/conformance`.

## Services

Start the real services needed for:

- Riverhog publication/cache;
- Stove0 API/state;
- observer stack;
- Opus target **preflight**.

Do not run an autonomous worker capable of target execution.

Prefer not to start autonomous controller/worker processes at all. Drive the
controller role through the existing admin scheduler request as the current
proof already does.

## Preserve missed-publication behavior

Finalize the FTP collection while autonomous Stove0 admission is offline.

Then prove the catalog/admission path reconciles that publication.

## Preserve durable admission boundaries

Drive:

1. `intent`
2. API restart
3. `previewed`
4. API restart
5. `work_bound`

and assert each state before moving on.

The existing "controller stays offline so no stage can race ahead" behavior is
part of the proof.

## Strengthen the accepted-plan assertion

At `work_bound`, inspect the accepted preview/work definition and prove:

- recipe identity is exactly `stove0.conformance-media/v1` at its checked
  revision/digest;
- selected target branches include exactly the expected audio overlap pair for
  the WAV fixture:
  - `archive-audio`
  - `archive-audio-overlap`;
- each plan binds the intended FTP collection root;
- accepted artifact/evidence selection accounts for all 16 WAV inputs and the
  associated XMP where applicable;
- the two route plans retain their distinct exact intents.

Use the maintained typed models/API rather than parsing logs.

## Preserve manual/admission convergence

Keep the existing explicit exact-receipt workflow preview + work-create call and
assert that it resolves to the same work identity as the automatic admission.

This is an important same-input idempotency/convergence proof.

## Stop here

Do not start the worker and do not wait for target completion.

If target execution begins, the lane has accidentally recoupled control-plane
qualification to the expensive path.

---

# 4. Build `processing-e2e`

This is the required real cross-component execution spine.

## Recipe

Exact `stove0.conformance-media/v1`.

## Producer

Real FTP spool -> Riverhog.

Automatic admission is not required in this lane; `processing-admission`
already owns it. Prefer direct exact-receipt preview/work creation so the lane
creates one intended parent work rather than an automatic duplicate.

## Starting fixture

Start with:

- 4 WAV files;
- one XMP sidecar associated with the first WAV.

Use the same WAV/XMP construction as the current proof.

Four is a measured starting point, not a new contract constant.

It preserves:

- multi-item observation/planning;
- multiple real ffmpeg transforms;
- output ordering/inventory across more than one input;
- one real associated sidecar;
- both overlapping conformance branches.

## Required execution

Both `archive-audio` and `archive-audio-overlap` execute through the real
single Opus target instance.

Do not add a route filter or qualification-only recipe.

## Deep assertions

Preserve the current deep assertions on the 128 kbps `archive-audio` child:

- parent complete;
- both route plans are present;
- child complete with real finalized output;
- for each source WAV:
  - expected `.opus`;
  - expected generated `.opus.xmp`;
- original XMP sidecar retained exactly where the conformance projection
  requires it;
- retained sidecar byte count and SHA-256 equal source;
- exact derivation format/identity;
- processing-claim input-set authority;
- input-set resolution back to the intended source collection.

Do not rewrite expected metadata to match `stove0.audio-archive/v1`; this is
the conformance route and its current metadata projection is the authority.

Also assert the `archive-audio-overlap` child reaches successful terminal
settlement and has a real output collection. It need not repeat the entire
deep inventory assertion already owned by `archive-audio`.

Retain target/operational/transfer metrics as observations.

## Restart/replay

After completion, restart only the components required by this proof:

- Stove0 API/controller/worker;
- processing observers;
- Opus target.

Do not restart review/rclone components in this lane.

Re-query/wait and prove:

- parent/children remain complete;
- settled output identities are unchanged;
- no duplicate target execution/output appears.

---

# 5. Build `processing-overlap`

This lane owns producer multiplicity, overlapping-route multiplicity, and the
real four-job target queue without multiplying them by the 16-file workload.

## Exact recipe

`stove0.conformance-media/v1`.

## Producers

Producer A:

- real FTP collection;
- one WAV;
- one XMP sidecar;
- classified for `stove0/conformance`.

Producer B:

- real CLI collection;
- one WAV;
- independently classified for the same admission recipe.

Both remain real Riverhog finalized collections.

## Work

Each producer must select:

- `archive-audio`
- `archive-audio-overlap`.

That yields four distinct real target jobs.

Assert:

- two distinct parent work identities;
- two target plans per parent;
- four distinct child/target job identities;
- the 128 kbps and 96 kbps route intents remain exact and distinct;
- all four jobs are accepted by the real Opus target;
- all four reach successful terminal target state;
- all four settle durably in Stove0/Riverhog.

## Serialization

Do not modify the production Opus target to make this test faster.

At the audited source, `OpusTargetService` inherits
`PersistentTargetService(maximum_workers=1)` and does not override the worker
count. Four real submitted jobs therefore exercise the actual single-worker
queue.

Do not add a new public production endpoint solely to observe an instantaneous
"running count".

If a stable target-owned internal status observation is already available in the
deployed container, it is reasonable to record queued/running evidence for
diagnostics. It is not necessary to turn that into a new public contract.

## Restart/replay

Restart the processing services/target after all four jobs settle and prove
completed identities remain stable and no duplicate target job/output is
created.

---

# 6. Keep `review-delivery` and `witnesses` independent

Do not pull their setup back into processing.

Current processing bootstrap unnecessarily starts some review/rclone machinery
because the old shared block was written before lane factorization. Correct
that.

- review sampler/review0/rclone descriptor/runtime checks belong with
  `review-delivery`;
- witness runtime belongs with `witnesses`;
- observer/tool parity that is genuinely required for conformance processing
  may stay with `processing-admission` or a focused image proof, but do not
  start unrelated services merely because Compose can.

This service minimization is secondary to target-work factorization but makes
lane ownership clearer and avoids hidden setup coupling.

---

# 7. Refocus `stove0-scale-qualification`

The dedicated scale rail is separate from per-commit conformance semantics.

At the audited source, its default remains 128 media inputs and 2000 frames,
but it invokes the old whole smoke harness.

After factorization, it must not mean "run every unrelated Compose lane with
128 media files".

Give it an explicit non-CI scale selector, for example
`processing-scale`, or an equally focused implementation.

Requirements:

- default remains 128 media inputs and 2000 frames unless a later issue changes
  the scale authority;
- uses final images;
- executes a real media target/publication lifecycle;
- records target/workspace/operational/transfer metrics;
- verifies input/output cardinality and exact derivation;
- does not rerun storage, review-delivery, witnesses, or exact-event FTP tests.

The existing `stove0.audio-archive/v1` single-route recipe is acceptable for
this **scale-only** workload if that best preserves the original #568 intent.
Record the exact recipe in scale evidence.

Do not cite scale-rail success as semantic equivalence evidence for
`stove0.conformance-media/v1`.

`processing-scale` is not a required per-commit `COMPOSE_LANES` member and
must not be added to `CI gate`.

For #960 closure, however, run the default 128-file scale target on the exact
integration SHA.

---

# 8. Source-level implementation map

## `scripts/ci_qualification.py`

Required CI lanes:

```python
COMPOSE_LANES = (
    "storage",
    "ingress-custody",
    "processing-admission",
    "processing-e2e",
    "processing-overlap",
    "review-delivery",
    "witnesses",
)
```

`processing-scale`, if implemented as a selector, is deliberately outside
this tuple.

Update `compose_targets()` from actual service ownership.

Expected direction:

- `ingress-custody`: Riverhog/test + FTP spool; no Stove0 images.
- `processing-admission`: Riverhog/test + FTP spool + Stove0 server,
  conformance observers and Opus target/preflight image.
- `processing-e2e`: same real processing image set needed by exact
  conformance execution; no review/rclone images.
- `processing-overlap`: same minimal processing set.
- `review-delivery`: review/sampler/rclone plus its required Stove0 services.
- `witnesses`: witness images plus Riverhog/test requirements.
- `storage`: current storage images plus moved overflow assertion.

Continue deriving image names from canonical Compose/Bake declarations rather
than maintaining an independent Dockerfile inventory.

Keep exact source/config label verification.

## `scripts/test_compose_smoke.sh`

Keep shared setup/helpers where useful, but make service/lifecycle ownership
explicit.

The selector should accept:

- `all` (union of the seven ordinary Compose lanes);
- seven required lane names;
- optional `processing-scale` for the Make scale rail.

`all` must **not** include `processing-scale`.

Remove the old `processing` selector entirely; do not retain it as an alias.

Avoid compatibility shims: this is pre-v1 and the repository policy prefers
one canonical current name.

Recommended refactoring strategy:

1. extract common Riverhog/bootstrap/config helpers;
2. extract FTP same-path proof;
3. extract media-fixture publication helper parameterized by explicit count,
   sidecar and interrupted-transfer behavior;
4. extract exact Stove0 conformance startup/config helper;
5. implement each lane as a small orchestration function/section over those
   helpers.

Do not copy the entire 1,000+ line script into seven scripts unless extraction
clearly improves state ownership.

## `Makefile`

Keep:

`make compose-shard COMPOSE_LANE=<lane>`

Update help to the new required lane names.

Refocus `stove0-scale-qualification` onto the explicit scale path.

Keep the existing four mandatory pre-commit gates unchanged.

Keep `make linux-qualification` as the exhaustive portable local rail. It
should run all seven ordinary Compose lanes exactly once and not run the 128
scale rail automatically.

## `.github/workflows/ci.yml`

Keep exact checkout, timing artifacts, and stable `CI gate`.

Change Compose matrix concurrency so all seven required Compose leaves may run
at once:

```yaml
max-parallel: 7
```

(or omit the cap if the matrix itself is exactly seven and policy tests express
the same intent).

Change client-platform concurrency from one to all three:

```yaml
max-parallel: 3
```

Leave repository, unit, and image caps unchanged for the first measured
candidate.

That yields approximately 17 simultaneously eligible CI jobs after the plan,
with room for the separate CodeQL workflow under the published 20-standard-job
account ceiling.

If hosted evidence shows heavy processing jobs are queued behind short jobs,
change scheduling again from evidence. Do not reintroduce a courtesy cap.

Keep:

```yaml
cancel-in-progress: true
```

## Tests

### `tests/unit/test_ci_qualification.py`

Assert:

- exact seven required Compose lanes;
- scale selector is not a required CI lane;
- each lane resolves only its needed canonical images;
- `linux_qualification()` invokes every required lane exactly once.

### `tests/unit/test_makefile.py`

Using the existing fake Docker harness, prove:

- each selector executes its owned phase(s);
- `all` is exactly the union of ordinary required lanes;
- old `processing` selector is rejected;
- scale target invokes only the scale path with the requested file/frame
  cardinality;
- processing lanes do not accidentally start review/witness services.

### `tests/unit/test_github_actions.py`

Assert:

- seven-lane generated Compose matrix;
- Compose concurrency does not serialize those seven;
- three client platforms may run concurrently;
- aggregate `CI gate` still depends on Compose and all other mandatory groups;
- exact checkout and cancellation semantics remain.

### Processing proof assertions

Prefer executable assertions in the real harness over a new hand-maintained
coverage manifest on main.

This reference branch's `PROCESSING_COVERAGE.json` is a handoff checklist,
not a new repository source of truth. Do **not** copy it into main as durable
policy.

---

# 9. Implementation sequence

A low-risk integration order:

1. Add new lane names/policy tests while old processing behavior remains.
2. Move cache overflow to storage.
3. Extract ingress-custody and prove parity.
4. Extract processing-admission and prove it cannot execute target jobs.
5. Extract processing-e2e using exact conformance recipe at four WAV + XMP.
6. Extract processing-overlap using two tiny producers/four jobs.
7. Refocus scale target.
8. Remove old processing selector.
9. Raise Compose/client matrix concurrency.
10. Run focused policy/fake-Docker tests.
11. Run four mandatory local gates.
12. Run clean `make linux-qualification`.
13. Run default `make stove0-scale-qualification`.
14. Push exact candidate and require CI + both CodeQL contexts.
15. Obtain at least three comparable green hosted runs before closing #960.

Do not keep both old and new processing executions in CI beyond the short
parity window; that defeats the latency objective.

---

# 10. Timing decision rule

Starting per-commit e2e fixture: 4 WAV + XMP.

After three hosted runs:

- if `processing-e2e` is comfortably below 20 minutes, consider increasing
  to 6 or 8 only if it adds useful confidence;
- if it lands around 20-25 minutes, keep it;
- if it exceeds 30 minutes, profile target execution/publication before reducing
  further;
- do not reduce below a multi-item fixture without an explicit #960 decision.

`processing-admission` must keep the 16-WAV cardinality regardless of e2e
tuning.

The scale rail must keep its explicit 128-file default unless separately
re-authorized.

---

# 11. Closure evidence

A #960 completion comment should contain:

- final exact source SHA;
- exact required lane names;
- fixture cardinality for admission/e2e/overlap;
- exact recipe identities used by each processing/scale proof;
- four mandatory local-gate results;
- `make linux-qualification` result;
- default 128-file `make stove0-scale-qualification` result;
- CI and CodeQL links for the exact SHA;
- three hosted timing tables;
- before/after critical-path comparison;
- explicit statement that no production concurrency/recipe semantics were
  changed for CI;
- any residual bottleneck as a focused follow-up rather than hidden deferred
  qualification.
