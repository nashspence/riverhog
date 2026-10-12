# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #963; convention: #903.
Original audited implementation base: main@63d4a70bdd48def676f5d754c41eefe53143d8aa
Previous reference / direct parent: 3414857ea7f73011467668c17b6ad51d23e1771b
Historic code-patch reconciliation checkpoint: main@7aa9997bcf05a50c42d7ecd4767730263a25d381
Current source audit: main@152d095038df0c0d0ed642cf265d47b11c65a754
Current audited source tree: 991e994457b487a7dca016b7fc0409485cef25c6
Reference branch: reference/external/963-validation-timing
Decision: https://github.com/nashspence/riverhog/issues/963#issuecomment-6103991666

Produced by: OpenAI ChatGPT
Model: GPT-6 (this editorial correction; prior model provenance remains in branch history)
Configuration/effort: not exposed
Requested by: Riverhog maintainer
Published by: connected GitHub integration; commit account metadata is not authorship.

## Current integration entry — #961 two-document onboarding

**Authoritative current #961 source:** the two complete approved document blocks
in [issue #961](https://github.com/nashspence/riverhog/issues/961), updated
2026-10-12T05:57:18Z. The public introduction is now the root `README.md`; the
developer guide is root `AGENTS.md`. The `docs/` directory and prospective
`docs/introduction.md` are **removed**, not renamed or retained as stubs.
Older #961 comments, reference patches, hashes and three-document diagrams do not
govern the integrated onboarding. Preserve current root-file integrity/link tests.

The current policy authority is [#963 decision 6103991666](https://github.com/nashspence/riverhog/issues/963#issuecomment-6103991666):
`approved-target`, `approved-no-target` or `unresolved`. An approved no-target
still records elapsed evidence and is never a target pass; unresolved applicability
and missing required evidence cannot silently pass. A **valid timing miss is
report-only**; a functional or required growth/evidence failure blocks validation.
Retain #960's original 600-second target and its scoped shortfall disposition.
The older #963 body wording requiring a numerical seconds target for every work
item is superseded to this extent only.

**Current guide delta:** `AGENTS.after-onboarding.patch` has been regenerated
against exactly the current approved #961 `AGENTS.md` block. Its source SHA-256 is
`1c427d38b88c9a05fe105dea5bcc862efec52ff516233110fa0ef88b43692f44`;
expected result SHA-256 is
`2b951ea6955e15dda3ab30336c0aa623087d8b42e5d819eed0eb1ca2aa5a1df1`.
Only the two measurement sections and prescribed handoff sentence change.
The source blocks come from #961's current issue body and #963's later approved
decision, **not** from the earlier three-document #961 snapshot. Check current
input identity and apply with `git apply --check` after #961 is integrated.
If later edits make the exact patch inapplicable, reconcile only the two intended
sections; never revert or edit unrelated onboarding policy to make a patch fit.

**Scope separation:** #963 implements shared work definitions, approved
timing-applicability and automatic validated functional/growth/timing result
collection, propagation and local/CI/qualification coverage. #964 supplies
specific operation-owned growth formulas and runtime counter/witness conformance.
The #963 foundation must prove real absence/failure/invalid-growth handling using
an executable fixture/owner seam; it must not wait for every #964 operation to
be implemented, and a timing-only reference checker is not a complete growth
foundation. Consume #963 before dependent #964 acceptance.

**Integration sequence:** current #961 approved root documents and tests ->
the #963 AGENTS-only measurement delta on that integrated guide ->
source-reconcile the historic foundation + applicability patches to actual
post-#961 `main` -> finish required validation adapters and growth outcome
propagation -> qualify and hand off source SHA/evidence to #964. Historical
source patches were written for main@63d4a70b and **must not** be applied
mechanically to present main or turned into old schema compatibility code.
The source/target hashes above prove this reference's approved-guide
reconciliation only, not finished product integration or locked qualification.

## Start here: revised policy and patch sequence

Read GROWTH_RESULTS.md first. The three results are functional correctness,
applicable growth conformance, and elapsed-time observation/target comparison.
Declared growth failures and missing required evidence are blocking; valid timing
misses are diagnostic. Numerical time targets are not frozen v1 speed promises.
Do not invent a number merely to populate a work definition.

This revision supersedes inconsistent mandatory-numeric-target and guide wording in
the earlier reference. It preserves that reference in history and its exact source
patch as validation-timing.foundation.patch. The old validation-timing.patch path is
retired deliberately. On a disposable checkout of the ORIGINAL audited base:

1. `git apply --check <reference>/.reference/validation-timing.foundation.patch`
2. Apply that foundation.
3. `git apply --check <reference>/.reference/timing-applicability.patch`
4. Apply the follow-up. Reconcile this combined change with then-current main;
   do not roll back newer qualification changes to reproduce a historical tree.

The follow-up implements approved-target / approved-no-target / unresolved timing
policy, rejects inconsistent or silently missing targets, preserves elapsed evidence
for explicitly untargeted work, and updates scale-profile call sites and prospective
unit tests. Only the scale's existing 600-second value is approved in real definitions.
No actual work is silently assigned an untargeted disposition by this reference.

IMPORTANT: These are a timing foundation and applicability patch, NOT completed
functional/growth evidence integration. GROWTH_RESULTS.md specifies the required
coverage, outcomes and adversarial cases still to implement through actual owner
witnesses. The old evidence checker alone is not a growth-conformance gate and an
evidence-complete failed command must still fail its aggregate consumer.

## Guide delta — use the **current** #961 approved root guide

The issue-owned #961 introduction is now root README; root AGENTS is the sole
developer guide. First integrate those two approved files and tests, without
reviving `docs/`. Then apply/check the current
`AGENTS.after-onboarding.patch` described above. It changes the guide's
measurement text according to #963 comment 6103991666, not the underlying
archive, state or onboarding rules. Preserve Alembic/StateSchema and future
forward-only upgrades; discarded pre-v1 layout compatibility stays prohibited.

Earlier #961 hashes (`1fe4e9...` and `171747...`) and earlier copies of this
patch are **historical** and must not be used as current input/output identities.
The exact issue-approved input/output digests are at the top of this handoff.
Recompute them if an authorized later #961 edit is adopted; do not update
expected hashes to conceal unapproved policy changes.

## Actual validation and reproducibility

- The original foundation applies to 63d4a70b and reproduces source tree
  0cd76e2a69e7ff37495272609538432558af753a.
- The applicability overlay applies after it and yields expanded source tree
  06b755ec40ca1fb0783b817abb10298f4116a9c0. This is NOT the reference branch tree.
- Thirteen stdlib checks execute the prospective ValidationWork, comparison and
  required-policy function bodies extracted from that expanded source. They cover
  valid miss/met, explicit no-target, unresolved applicability, invalid policy/value
  combinations, source and command failures, and silent target removal.
- Run: `TIMING_REFERENCE_WORKSPACE=/path/to/expanded-source python
  <reference>/.reference/verify_timing_policy.py -v`.
- The earlier guide's four checks and the 17-test total remain historical to their
  original approved input. The previous #961 guide hashes and patch checks remain historical. The corrected
  Alembic-preserving guide hashes and downstream #963 context were recalculated;
  locked integrated guide qualification remains necessary. The normal locked
  guide tests remain required after integration.

These source-extracted checks do not exercise the full module import graph, process
runner, complete evidence checker, growth artifact consumer, CLI or platform cleanup.
The newly included prospective pytest cases were not run as the complete locked
suite. The prior reference's 116-test report is historical, not validation of this
revision. The environment lacks the locked toolchain, Docker and rfc8785 dependency;
no substitutes were committed or full-gate success claimed.

## Remaining integration

Complete ordinary Make/CI/qualification adapters, required-work discovery, phase and
aggregate evidence, required checks outside the primary workflow, and growth witness
coverage/result propagation. Reference operation-owned growth declarations rather
than inventing a duplicate registry. Tests must reject an omitted witness even if
its timing report is green, and preserve growth failure through profiling wrappers.
Obtain target or explicit no-target decisions for unresolved work; exceptions and
history stay in GitHub. Review actual resource and cold/warm applicability.

#961 remains the guide prerequisite. Coordinated #963/#964 integration is allowed;
the shared evidence foundation precedes dependent growth acceptance. #960's explicit
closeout timing exception remains scoped; keep its profile and functional proof.
Locked local gates, relevant integration qualification and independent exact-SHA
GitHub evidence remain mandatory before claiming adoption. Provider/release authority
is not granted by any of these patches.

## Non-authority and publication

Only .reference material changes here. Do not merge this branch or copy its files to
main as a second policy ledger. Owning issues and current approved contracts control;
reference publication is not production integration or a conformance result.
Preserve the parent reference without force-push. Record the actual GitHub tip SHA
after verifying the published tree against the prepared tree. Native Development and
blocked-by mutation are not exposed by this publisher; retain that limitation in the
issue rather than describing Markdown links as native relationships.
