# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #963; convention: #903.
Original audited implementation base: main@63d4a70bdd48def676f5d754c41eefe53143d8aa
Previous reference / direct parent: 832fc5ee502c11c7c9e9c73fc652c3dd55c202f3
Current reconciliation checkpoint: main@7aa9997bcf05a50c42d7ecd4767730263a25d381
Current tree: 6b0cb7a536a768340911b24e9dd92836033a8d5a
Reference branch: reference/external/963-validation-timing
Decision: https://github.com/nashspence/riverhog/issues/963#issuecomment-6103991666

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer
Published by: connected GitHub integration; commit account metadata is not authorship.

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

## Guide delta

**Pre-v1 migration and backward compatibility are explicitly disallowed.**
#963 consumes #961 comment 6112662156 and preserves its corrected guide; see
#963 comment 6112671450. A migration/old-version upgrade path is NOT a valid
interpretation of the shared timing/failure-evidence machinery.

Apply AGENTS.after-onboarding.patch only after the **current corrected #961 guide**
(#961 comment 6112662156) and the approved growth amendment (6103986307) are
present. The obsolete migration/compatibility wording is not a supported input. It preserves that section, replaces the
measurement sections and adds the updated three-result handoff sentence. Do not
apply the prior guide patch afterward. Actual integrity, semantic and link tests
must be reconciled with the installed guide, not just given copied hash constants.

Current corrected #961 growth-guide input SHA-256:
8f78a6acba90afb1e300577a5a8d145b69a7597c76e758b5bb84d49780ed8349
After #963 measurement delta output SHA-256:
392fc7ad856cede5ff237cbdd06acf7f3ea79ec174fe7d4baea900cb3e3b2fe9

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
  original approved input. The current #961 guide and growth input/output hashes
  were recomputed; the updated #963 patch was applied by exact unique-context
  matching without altering the hard-cut or growth sections. The normal locked
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
