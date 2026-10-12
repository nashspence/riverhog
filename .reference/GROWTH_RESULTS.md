# Integrate functional, growth and timing evidence separately

Authority: #963 comment 6103991666; the current complete #961 issue body
(two root onboarding documents); #964's owning growth decisions.
This is a required integration specification, not implemented growth reporting.
The accompanying source patches implement timing-policy applicability only; they
do NOT complete growth witness discovery, execution, artifact validation or v1
qualification. Do not mistake source-extracted patch tests for product conformance.

## Apply source and guide deltas in order

The original implementation patch is now `validation-timing.foundation.patch`,
retained byte-for-byte from reference 832fc5ee. Apply it to its audited source
63d4a70b, then apply `timing-applicability.patch`. Both are reference input to
reconcile against current main, not a reason to roll back newer qualification work.
The old `validation-timing.patch` path is intentionally retired on this revision
so the obsolete all-work-needs-a-number rule is not applied accidentally.

The follow-up replaces `require_runtime_targets` with `require_validation_policy`
and separates approved-target, approved-no-target and unresolved. The scale work
retains its approved 600-second target; other real work remains unresolved until
explicit target/no-target disposition. Test fixtures approve only their test work.
The reference cannot supply maintainer approvals by creating a default.

The **current** `AGENTS.after-onboarding.patch` is rebuilt against the exact
approved `AGENTS.md` block in #961's current two-root-document issue body.
First integrate #961's root `README.md` and `AGENTS.md`, removing `docs/`; then
apply the #963 measurement delta without changing #961's other wording.
Current approved-guide SHA-256: `1c427d38b88c9a05fe105dea5bcc862efec52ff516233110fa0ef88b43692f44`;
post-#963 intended SHA-256: `2b951ea6955e15dda3ab30336c0aa623087d8b42e5d819eed0eb1ca2aa5a1df1`.
Earlier old-guide patches/hashes are historical, not the current patch base.
Check applicability against the actual installed guide and update executable
guide integrity, link and policy tests—not just their expected hashes.

## Evidence contract to complete in the integrated implementation

The shared plan identifies required work and references owner declarations and
witnesses. Owners define normative growth scope, dimensions, assumptions and cost
bounds. Neither the scheduler nor a generated report is a second declaration
registry. A newly required operation/witness must not disappear from coverage.

Record exact source, workload, expected declaration/witness identities and revisions,
execution conditions, functional exit, each applicable growth witness result,
measurement completeness, elapsed/phase/aggregate costs and timing comparison.
Bind evidence to the invocation; reject absent, duplicated, stale, mismatched,
truncated, malformed or incomplete required growth artifacts. A filename, hash or
exit 0 alone does not prove the named bound. Consume actual maintained witnesses
with complete-result and counter assertions at their responsible boundaries.

Growth applicability must be derived/reviewed against owner coverage, not decided
by the report's author. An explicit no-applicable-growth result is not a claim of
linear behavior; it cannot hide an in-scope operation lacking a declaration.
Before declaring v1 acceptance, unresolved owner/declaration coverage is a blocker.

## Required outcome matrix (integration tests, not a new public schema)

| Functional | Required growth/evidence | Timing | Qualification outcome |
|---|---|---|---|
| success | complete pass | target met | success |
| success | complete pass | valid target miss | functional/growth success; timing review required |
| success | complete pass | explicitly no numerical target | success; record time and not-compared, never target-pass |
| failure | any | any | failure; never timing-success |
| success | declared bound violated | any | conformance failure |
| success | missing/invalid/incomplete witness | any | evidence failure |
| success | unresolved expected coverage | any | unresolved required validation, not a pass |
| success | complete pass | missing approved target or unresolved applicability | required policy/evidence failure |
| interrupted | incomplete | any | incomplete/failure, never target or growth success |
| success | complete pass | wrong source/workload/conditions | reject required evidence or explicitly not-compare as its owner requires |

Keep evidence completeness and command/conformance success distinct. The original
foundation checker returns evidence completeness even for a truthful failed command;
its consumer MUST continue enforcing the command outcome. Extend consumers so a
truthfully reported growth failure also cannot pass an aggregate merely because
its evidence is complete. Timing approval is scoped only to time, not either failure.

Test altered growth identities, omitted witnesses, no-target claims on targeted
work, false not-applicable claims, cancellation after partial results, malformed
counter values, and a timing miss paired with successful/failed growth results.
Do not let profiler wrappers drop a child growth failure. Functional and growth
work run once; elapsed measurement wraps them rather than rerunning for profiling.

## Reporting and adoption

Maintain visible during-run overrun information, summaries and bounded artifacts.
A target miss never kills work solely because it elapsed. Safety timeouts and
resource/correctness limits remain independent. Internal timing targets remain
reviewed, but are not frozen product speed promises. Explicit no-target means
absence was approved, not that missing evidence is acceptable.

#964 owns algorithmic accounting, counter instrumentation and reviewed work budgets.
The qualification plan schedules/references these, and normal local/CI runs enforce
the expected coverage. Keep the large-workload cadence coverage-driven rather than
making every check run an expensive size ladder. Existing proof stays until its
owner approves an equivalent replacement.

#961's current two-document onboarding precedes the #963 guide delta.
Coordinated #963/#964 preparation is allowed; the shared machinery (including
missing/failed growth-witness propagation) precedes dependent #964 acceptance.
#964 owns real operation-by-operation bound selection, source counters and
executable product witnesses; #963 owns their shared evidence-consumption path. #960's timing closeout
disposition remains limited and its profile/fixture unchanged. Full rollout still
requires entrypoint/matrix/platform/aggregate integration, approved applicability,
locked gates and independent exact-SHA checks; none is claimed by this reference.
