# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #963
Convention: #903
Audited base: refs/heads/main @ 63d4a70bdd48def676f5d754c41eefe53143d8aa
Reference branch: reference/external/963-validation-timing

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer (nashspence), through this conversation
Published through: the connected GitHub tooling; Git commit metadata is publication metadata

This branch is concrete, externally produced integration input, not an alternate
integration rail or completed adoption of the policy. Its creation, tests, publication,
or linkage do not authorize changes to contracts, targets, release requirements, or
protected settings. The owning issue and subsequent maintainer decisions control.
Reconcile against then-current main. Do not merge this reference mechanically.
The exact published SHA recorded in #963 is the durable handoff identity.

## Scope of this reference

The branch carries two apply-ready patches rather than prematurely modifying the
pending onboarding baseline. `.reference/validation-timing.patch` is the tested
implementation/test diff against the audited base. Its expanded source tree is
`0cd76e2a69e7ff37495272609538432558af753a`. The following paths describe changes inside that patch.

- `scripts/ci_qualification.py` adds executable `ValidationWork` definitions derived
  from the existing local/CI work selections. It owns the existing 600-second
  maintained scale target, workload arguments and measurement boundary. Other runtime
  targets remain explicitly unapproved (`None`); the target-completeness function
  rejects them. No baseline or new approval is invented.
- `scripts/ci_timing.py` adds a maintained-work executor that does not accept command
  or target overrides. It retains a running record before launch, warns once when the
  target elapses without terminating the work, and records the exact command outcome.
  Its evidence checker verifies expected workload coverage, source/run identity,
  definitions, completion state, finite measurements, and recomputed comparisons.
  Valid timing misses preserve command success. Functional exit statuses remain
  distinct from evidence completeness. Dirty-source observations stay not compared.
- `scripts/transfer_profile.py` consumes the shared scale definition through the
  executor instead of choosing its own target/workload values. Existing image
  preparation remains separate, the unchanged lifecycle verifies prepared images,
  and lifecycle evidence is checked before reporting. Transfer/recovery scenarios
  retain their existing semantics.
- Focused tests cover unapproved/unknown work, exact target/workload boundaries,
  missing/duplicate/stale/malformed evidence, failed/interrupted runs, report-only
  misses, one-time overrun reporting, and fixed scale-scenario integration.
- `.reference/AGENTS.after-onboarding.patch` contains only the approved post-#961
  document delta. The root AGENTS.md, README, Introduction/Architecture, and their
  existing integrity checks are deliberately untouched here. #961 is unchanged.

## Integrate after #961

1. Finish #961 first. Reconcile the source changes against current main and the
   decisions in #963. Check and apply `.reference/validation-timing.patch` using
   `git apply --check`, then `git apply`. Do not copy `.reference` into the
   authoritative repository.
2. Apply the guide delta only after the approved onboarding guide is present:
   `git apply --check <reference-copy>/.reference/AGENTS.after-onboarding.patch`,
   then apply it. Update the existing document-integrity and focused policy tests
   against the actual integrated document; do not blindly reuse a stale digest.
3. Complete the normal-entrypoint adapters and required evidence consumption below.
   Do not claim that automatic measurement of every gate is already implemented.
4. Obtain explicit approval of the remaining targets/conditions from representative
   measured runs. Target-completeness failures are deliberate until that happens.
5. Run the mandatory locked local gates and independent exact-SHA GitHub checks on
   the integrated source, and review the resulting timing evidence before handoff.

The guide patch was checked against the supplied #961 approved text:
- Input SHA-256: 55f9289fd03100d69c9768790754c87931759cd37b572a100a647f6e4194fcdd
- Output SHA-256: d1bca43831f791a10befbc7c1760cdf05ca4d9315fd8788320180a08fa2d7f12

## Remaining implementation and acceptance (not claimed complete)

This is a focused foundation and one maintained-scenario integration, not the complete
cross-platform rollout. In particular:

- The ordinary Make gates, qualification aggregates, and required CI jobs are not
  yet all automatically wrapped or consuming `check-evidence`. Complete that wiring
  using the shared executable definitions. Measure prerequisites as part of the
  owning gate (for example, distribution construction in `dist-smoke`), and avoid
  recursive dispatch when a Make target starts its own plan command.
- The derived work list covers the existing shared repository/unit/image/Compose
  choices, the local check-in aggregate and the scale scenario. Finish scope discovery
  and adapters for required jobs outside that shared selection (including CodeQL),
  provider/release/installation qualification, matrix execution conditions, and CI
  complete-run wall time. No permission to run restricted qualifications is granted.
- Add executable completeness tests deriving every required entrypoint/job from its
  actual owner, then require targets and reports for that derived set. Passing an
  arbitrary subset to the evidence checker is not proof of global coverage.
- Extend planned target applicability with the approved actual execution conditions,
  including relevant runner/cold/warm distinctions. The retained scale definition
  preserves its existing fixed fixture/prepared-image scope; no new hardware budget
  is asserted. Approvals and measured run history stay in GitHub.
- Complete phase/report retention, summaries/artifact upload, aggregate elapsed time,
  and external queue-delay separation. The reference executor measures its command;
  it does not pretend to measure a whole hosted workflow or verify every phase file.
  An evidence-complete report does not itself mean functional success: consumers
  must continue enforcing the original command/job result.
- Preserve existing failure, restart/replay, source/image checks, fixture semantics,
  safety timeouts and observation bounds. No performance threshold is made blocking.
- Review cancellation/cleanup behavior under the actual platform harnesses. The
  reference retains interrupted evidence as incomplete; it does not replace existing
  process-tree cleanup or claim crash-proof evidence persistence.

## Validation performed

116 focused tests passed; one existing xdist-specific test was deliberately deselected:

```text
python -m pytest -q tests/unit/test_ci_qualification.py \
  tests/unit/test_validation_timing_policy.py \
  tests/unit/test_transfer_profile_script.py tests/unit/test_ci_timing.py \
  -k 'not per_worker'
```

The constrained environment used Python 3.13.5, pytest 9.0.2, PyYAML 6.0.3, repository
source paths on PYTHONPATH, disabled automatic pytest-plugin loading, and the actual
upstream rfc8785 0.1.4 implementation copied into an external temporary dependency
location. No dependency substitution or environment helper is included in this tree.
These are focused behavioral checks, not the repository's locked qualification.

Also performed: Python compilation of changed modules/tests; `git diff --check`;
exact application and result comparison of the post-onboarding guide patch.

## Validation not performed

Locked `make lint`, full canonical unit suite, `make dist-smoke`, `make build`, Linux
qualification, xdist test, mypy/Ruff, real Docker/scale execution, representative timing
runs, provider/release qualification, and independent GitHub checks were not run.
The environment lacks mise, Docker and the complete locked workspace; direct package
installation failed because outbound DNS is unavailable. The `.reference/HANDOFF.md`
file is intentionally permitted only on this #903 reference, not on main's restricted
hand-maintained documentation surface. Full entrypoint tests are not claimed green.

## Publication and linkage

The GitHub publisher reconstructs the supplied reference tree (audited base plus the
three `.reference` files). Verify that published tree against the local reference tree
and record the actual GitHub reference commit SHA in #963. The implementation patch
was separately applied to the audited base and its expanded source-tree identity
recorded above. No producer pre-publication commit SHA is claimed.
The available GitHub actions can publish branches and comments but cannot establish
native Development branch links or issue dependency relationships. Record that
limitation in #963; a capable publisher can add the native relationships without
changing the reference commit. A Markdown link is not claimed to be the native link.
