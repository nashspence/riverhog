# #960 source audit

Audited source: `main@f4ac676554d5e68cae629f79af618ce35e278a43`  
Tree: `47d614324a59e5e59507663ea1ff5dc323baaab9`

This audit records the source facts behind the corrected #960 integration plan.
It is reference material under #903; issue #960 remains authoritative.

## 1. Current bottleneck is target execution, not CI setup

The three successful post-#954 runs against
`a26e214f77ab806e4230125bf7aad93f8c3328da` had `compose processing`
wall times of approximately:

- 122.8 minutes
- 85.2 minutes
- 117.4 minutes

The corresponding `stove0-processing` phases were approximately:

- 103 minutes
- 71 minutes
- 100 minutes

Image construction, storage bootstrap, Riverhog startup, FTP custody and
admission were comparatively small. The unit critical path was already reduced
to roughly 18-26 minutes.

The current optimization therefore needs to reduce the amount of unrelated real
target work serialized on the one Opus target instance. More image caching,
status polling changes, or scheduler polling changes cannot plausibly deliver
the desired gate reduction.

## 2. What the current processing lane actually multiplies

At the audited source, one processing job combines:

### FTP exact-event/custody proof

Before the media workload, two tiny transfers reuse `same-path.bin`. The
proof establishes:

- both terminal transfers enter listener custody before adapter consumption;
- pathname reuse does not overwrite the first exact event;
- two distinct Riverhog collections are finalized;
- FTP spool/listener restart does not lose receipts;
- cleanup removes claims/completion events/handoffs only at the durable tip;
- Riverhog retrieval reproduces exact bytes/hashes.

This proof has no semantic dependency on media target execution.

### Full media producer

The FTP source is then reconfigured for `stove0/conformance` and produces:

- 16 WAV files by default;
- one XMP sidecar;
- an interrupted first WAV upload resumed at the exact REST offset.

The controller is intentionally offline while this producer finalizes so the
Stove0 catalog follower must discover a missed publication after restart.

### Durable admission proof

The harness advances one automatic admission explicitly through:

- `intent`
- `previewed`
- `work_bound`

and restarts the API between boundaries while the autonomous controller is
offline.

It then performs a manual exact-receipt workflow preview/work-create and asserts
that the resulting work identity equals the automatically admitted work. This
is a meaningful idempotency/convergence assertion and must not be lost.

### Second producer

A separate CLI upload creates a one-WAV classified collection. It is admitted
independently and produces a distinct work identity.

### Cache-overflow proof

A 2 MiB Riverhog upload proves the 1 MiB local retrieval-cache budget spills to
the elastic cache and status/accounting reports both stores. This does not
depend on Stove0 and belongs with storage qualification.

### Target execution

For an audio collection, `stove0.conformance-media/v1` selects two overlapping
routes:

- `archive-audio`: Opus 128 kbps with the conformance metadata projection;
- `archive-audio-overlap`: Opus 96 kbps with a distinct metadata projection.

With the FTP and CLI producers together, the target sees four real jobs.

The Opus target service inherits
`PersistentTargetService(maximum_workers=1)` and does not override the
worker count. Its execution pool is therefore a single worker at the audited
source.

The expensive Cartesian product is consequently:

> full 16-file FTP execution through route A  
> + full 16-file FTP execution through route B  
> + one-file CLI execution through route A  
> + one-file CLI execution through route B

all serialized on one target worker.

## 3. The deepest existing output assertions belong to one conformance child

After all work completes, the current harness selects the FTP parent's
`archive-audio` child and validates its real Riverhog output.

For the default fixture it proves:

- parent and child completion;
- exactly two selected target plans on the parent;
- real finalized output collection;
- one `.opus` and one generated `.opus.xmp` for every WAV;
- retained source `smoke-0000.xmp`;
- retained sidecar bytes/hash equal the source sidecar;
- exact collection derivation;
- exact processing-claim input-set identity;
- input-set resolution back to the intended input collection;
- operational/target metrics;
- empty target workspace after completion;
- completed-state convergence after a broad service restart.

Those assertions are valuable. They do not require the second producer, and
they do not require all sixteen source WAVs in order to prove the conformance
metadata/output semantics on every commit.

## 4. Why the prior single-route substitution was wrong

The earlier #903 handoff proposed using `stove0.audio-archive/v1` for the
full-cardinality execution lane and treating it as equivalent to the
`archive-audio` branch of `stove0.conformance-media/v1`.

They are not exact substitutes.

### `stove0.audio-archive/v1`

Its `archive-audio` route uses:

- operation: `stove0.media.audio-archive/v1`
- target: `opus`
- 128 kbps Opus intent
- `input_retrieval_policy: allow`
- no conformance route metadata projection in the route intent

### `stove0.conformance-media/v1` / `archive-audio`

Its route uses:

- the same operation and target;
- 128 kbps Opus intent;
- a specific metadata projection containing device make/model, GPS, creator,
  tag and capture-time preference;
- `input_retrieval_policy: available-only`.

Recipe identity commits to the complete recipe definition. Route intent and
retrieval policy are therefore material semantics, not incidental test setup.

The corrected per-commit execution proof must continue to invoke
`stove0.conformance-media/v1` exactly.

## 5. Why reducing per-commit execution cardinality does not discard scale proof

The repository has a separate scale authority.

Commit:

`9d7cfdb4dedf24454ac3c1aaccdc753ada114084`

message:

`Qualify Stove0 scale and remove accidental cardinality ceilings (#578)`

closed #568, whose explicit purpose was to qualify/bound Stove0 scale,
workspaces and operational state before the first v1 tag.

That change introduced both:

- a configurable Compose workload whose ordinary default was 16 files; and
- `make stove0-scale-qualification`, defaulting to 128 files and 2000 audio
  frames.

The current Make target still has the explicit 128-file default.

This is strong evidence that "16" is a representative ordinary qualification
workload, while the dedicated 128-file rail is the repository's actual scale
proof.

The corrected #960 split therefore keeps:

- **16 artifacts** in the required per-commit admission/planning proof, so exact
  full-cardinality observation/association/branch planning remains continuously
  exercised;
- a smaller real target-execution workload in the per-commit e2e proof;
- the **128-file scale qualification** on the final #960 integration SHA and
  release-related qualification, where scale belongs.

#960 must refactor the scale target so it no longer reruns unrelated
storage/review/witness proofs simply because the old monolithic shell script
owned everything.

## 6. Correct execution authority

The required per-commit execution lanes should use the exact checked
`stove0.conformance-media/v1` recipe.

### Processing admission

Keep 16 WAV + XMP and stop at durable `work_bound`.

The lane should prove the accepted preview/plan contains both conformance audio
routes and exact full input/evidence selection. It should not start the worker
that can execute target jobs.

The target and observer services still need to be available for real preview
and preflight.

### Processing e2e

Use one real FTP-produced collection and direct exact-receipt invocation of
`stove0.conformance-media/v1`.

Start at 4 WAV + one XMP sidecar.

Both audio routes execute. Deeply inspect the 128 kbps `archive-audio` child
using the current output/lineage assertions, and require the overlap child to
complete/settle too.

Automatic tag admission need not also create this work: the separate admission
lane owns that behavior. Avoiding duplicate automatic work makes the execution
lane about target/publication semantics rather than another admission proof.

### Processing overlap

Use two small independent real producers:

- FTP: one WAV + associated XMP sidecar;
- CLI: one WAV.

Both are processed with exact `stove0.conformance-media/v1`, producing four
real Opus jobs (two routes x two producers). Verify:

- two distinct parent work identities;
- two target plans per parent;
- distinct 128/96 kbps route intents/plan identities;
- four real accepted target jobs;
- four terminal successes and durable settlements;
- completed-state convergence after restart.

No new public status API is needed merely to "prove max concurrency one".
The actual target implementation is single-worker at this source and the
four-job integration exercises its queue. #960 must not change that production
concurrency to make CI faster.

## 7. Scale rail after factorization

The current `stove0-scale-qualification` invokes the old whole smoke harness.
After the lane split it should become a focused scale qualification rather than
"all Compose lanes with 128 media files".

Two requirements matter:

1. It must still exercise real final-image media target publication at the
   declared 128-file scale.
2. It must not be cited as evidence that a different recipe is semantically
   equivalent to `conformance-media`.

The existing `stove0.audio-archive/v1` is suitable as an independent
single-route scale workload if the integration agent chooses that path, because
the scale rail owns target/workspace/cardinality behavior rather than
conformance recipe equivalence. Alternatively a focused exact-conformance scale
path may be used if its cost remains reasonable.

Whichever implementation is selected, record the exact recipe/fixture in the
scale evidence and keep the per-commit conformance execution proofs unchanged.

## 8. Concurrency recommendation

Current matrix caps are:

- repository: 2
- units: 3
- compose: 2
- client platforms: 1
- images: 2

For #960, change only limits that are likely to become critical:

- Compose: run all seven required leaves concurrently.
- Client platforms: run all three platform entries concurrently.

Leave repository/unit/image caps unchanged initially.

That produces an intended maximum around:

- 2 repository
- 3 unit
- 7 Compose
- 3 client platform
- 2 image

= 17 CI jobs, leaving practical headroom for the separately running CodeQL
jobs under the published 20-standard-job Free concurrency limit.

This is not a courtesy limit. It is simply a topology that uses essentially all
useful capacity without allowing short image jobs to crowd out heavyweight
processing proofs. If measurements show a remaining queue-induced critical
path, adjust further.

## 9. Non-authority boundaries

#960 may change qualification topology and test harnesses.

It must not:

- redesign recipe semantics or add a route-disable feature (#953);
- change extension scheduling/target concurrency to make CI faster (#948);
- create a qualification-only public API;
- treat cache state as evidence;
- replace a previous real integration assertion with a mock-only test;
- defer required same-SHA proof to a nightly job.

## 10. Acceptance evidence

The final implementation should report:

- exact final source SHA;
- four mandatory local gates;
- clean `make linux-qualification`;
- issue-specific `make stove0-scale-qualification`;
- exact-SHA CI + both CodeQL contexts;
- at least three successful hosted timing runs;
- per-lane phase/cardinality evidence.

The desired normal hosted critical path is 20-30 minutes. If exact conformance
e2e with four WAVs remains above 30 minutes, profile the real target/publication
path before reducing below a meaningful multi-item fixture.
