# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #960
Convention: #903
Superseding maintainer decision: https://github.com/nashspence/riverhog/issues/960#issuecomment-6014447496
Supersedes reference: `6c5084055bc59905bc8a616fc10e7d152e8f9611` where inconsistent
Audited base: main @ `f4ac676554d5e68cae629f79af618ce35e278a43`
Audited tree: `47d614324a59e5e59507663ea1ff5dc323baaab9`

Produced by: OpenAI ChatGPT
Model: GPT-5.6 Sol
Requested by: Riverhog maintainer
Published by: OpenAI ChatGPT via the connected GitHub integration

Purpose: corrected implementation handoff for #960. Factor the remaining
Stove0 processing gate into independent same-SHA proofs while preserving the
exact `stove0.conformance-media/v1` execution semantics, its real
FTP/Riverhog/Stove0/Opus/Riverhog path, admission durability, overlap behavior,
and the repository's independent scale rail.

This branch is externally produced reference material. It is not an integration
branch and does not itself establish repository authority. Issue #960,
subsequent maintainer decisions, and the normal integration rail remain
authoritative.

## Start here

1. Read `.reference/AUDIT.md` for the source findings that changed the earlier
   handoff.
2. Execute `.reference/INTEGRATION.md` as the implementation plan.
3. Use `.reference/PROCESSING_COVERAGE.json` as the invariant ownership map
   when updating workflow/policy tests.

Do not cherry-pick this branch into main. It intentionally changes no
production/workflow source.

## Important correction from the earlier reference

The earlier handoff proposed using `stove0.audio-archive/v1` as the
full-cardinality execution substitute for the `archive-audio` branch of
`stove0.conformance-media/v1`.

That substitution is not exact:

- the conformance route binds a richer metadata projection;
- the conformance route uses `input_retrieval_policy: available-only`;
- `stove0.audio-archive/v1` uses a different exact recipe/route definition;
- recipe identity commits to the complete recipe definition.

The corrected handoff therefore keeps `stove0.conformance-media/v1` for every
per-commit processing execution proof. The single-route recipe remains useful
only where it is already an independent authority, especially the dedicated
scale rail; it is not evidence for conformance-route equivalence.

## Corrected proof shape

Required per-SHA Compose leaves:

- `storage`
- `ingress-custody`
- `processing-admission`
- `processing-e2e`
- `processing-overlap`
- `review-delivery`
- `witnesses`

Key cardinalities:

- `processing-admission`: existing 16 WAV + one XMP sidecar, through exact
  conformance observation/planning/admission, **without target execution**.
- `processing-e2e`: start at 4 WAV + one XMP sidecar, through exact
  `stove0.conformance-media/v1`; both overlapping routes execute for real.
- `processing-overlap`: one small FTP WAV + XMP and one small CLI WAV; both
  conformance routes for both producers produce four real Opus jobs.
- `make stove0-scale-qualification`: keep the explicit 128-file scale proof as
  a final integration/release-related rail, not a per-commit CI leaf. Refactor
  it away from unrelated Compose scenarios as part of this work.

The 20-30 minute hosted critical-path target is an engineering target, never a
coverage waiver.

## GitHub Actions posture

The maintainer explicitly removed the repository courtesy-cap requirement.
Use standard public GitHub-hosted runner capacity where it shortens the exact
same-SHA gate. Preserve `cancel-in-progress: true`.

The current critical-path recommendation is:

- allow all seven Compose leaves to run together;
- allow all three client-platform entries to run together;
- leave the existing short repository/unit/image caps alone initially unless
  measured scheduling shows they block a heavyweight leaf.

This puts the current CI topology near, but not intentionally beyond, the
published Free-plan standard-runner concurrency ceiling when CodeQL is also
active. Let actual hosted measurements guide any further scheduling changes.

Do not change production target concurrency for CI.

## Validation performed for this handoff

- Re-audited issue #960 and both prior comments.
- Re-audited current main at the exact SHA/tree above.
- Confirmed the only change from the original #954 integration SHA to the
  audited base is later documentation/policy/support work; processing workflow
  source remains the same.
- Re-audited `scripts/test_compose_smoke.sh`,
  `scripts/ci_qualification.py`, `.github/workflows/ci.yml`,
  relevant Make targets and workflow policy tests.
- Compared the exact checked definitions of `stove0.audio-archive/v1` and
  `stove0.conformance-media/v1`.
- Verified the Opus target uses `PersistentTargetService` without overriding
  its default single execution worker.
- Audited the history that introduced the configurable 16-file smoke workload
  and the separate 128-file `stove0-scale-qualification` rail:
  commit `9d7cfdb4dedf24454ac3c1aaccdc753ada114084`, closing #568.
- Rechecked #948 and #953 authority boundaries.
- Rechecked current GitHub documentation for standard public-runner capacity.

## Not validated / limitations

- No Riverhog Docker/Compose qualification was executed by this producer.
- No proposed lane has hosted timing evidence yet.
- Four WAVs for `processing-e2e` is the recommended starting point derived
  from the measured current target-execution critical path; the integration
  agent must retain/tune it from hosted evidence rather than treating four as a
  new semantic constant.
- The exact implementation of the 128-file scale rail may use the existing
  single-route audio-archive authority because scale and conformance are
  separate proofs. It must not be cited as replacement evidence for
  `conformance-media`.
- #948 owns extension execution/scheduling design. #953 owns recipe-language and
  recipe-contract redesign. #960 must not preempt either.
- The publication interface available here cannot create GitHub's native
  Development relationship between this branch and #960. Record that limitation
  in the owning issue; a later capable publisher may add the relationship
  without changing the reference SHA.
