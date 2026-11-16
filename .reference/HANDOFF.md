# NON-AUTHORITATIVE EXTERNAL REFERENCE

- Owning issue: [#996](https://github.com/nashspence/riverhog/issues/996)
- Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
- Controlling design: [#994](https://github.com/nashspence/riverhog/issues/994)
- Reference branch: `reference/external/996-native-execution`

Audited base: `codex/964-session-owner` ([#979](https://github.com/nashspence/riverhog/pull/979)) at **c28eceaa2dfec1b3712135469ba12230e74f5ffa**. Main was observed at `209481c189f7387557a85e1c249184642816e103`. #996/#994 establish the current integration source; this is not a reset instruction.

- Produced by: OpenAI ChatGPT
- Model: GPT-6 Astra Pro
- Configuration: Default mode with delegated source, storage and policy reviews; reasoning/effort setting not exposed.
- Requested by: nashspence, Riverhog maintainer, in the current conversation.
- Published by: OpenAI ChatGPT through connected GitHub tools. Commit metadata identifies publication, not independent producer provenance.

Purpose: provide a focused, concrete Native execution/generation implementation proposal, real-source experiment, incremental SQLite storage prototype and qualification handoff without reopening #994's selected direction.

**Current reading entrypoint: [IMPLEMENTATION.md](IMPLEMENTATION.md).** It links the selected backend, actual evidence and qualification instructions.

**First focused integration step:** on the then-current #979 source, replace journal parent/child recovery representation with the common live conductor and bounded epochs, and replace upload per-yield adoption with selected-generation/final-outcome adoption. Carry the pinned #995 policy/accounting seam and #997 custody through the small real Native → Core/SDK F1 publication and independent-reader slice. Preserve exact semantics and original receiving; prove zero optional-store setup on cheap work before adding promotion to that same executor.

Contents: Native conductor experiment and 22 semantic/failure/lifetime tests; authenticated immutable page-map and WAL capture prototypes; 47 storage/capture/selection tests; exact captured paired observations, commands/hashes and JUnit evidence; concrete owner/consumer, state-cutover and qualification decisions. The source-backed classification/disposition inventory is recorded on #996.

Source relationship: **all product paths match c28eceaa exactly; only `.reference/` is added.** Initial research used `0f8030b0c0cd53eb33456924f070ae5979ed307e`. The complete intervening commit changes LF checkout policy and two qualification regression files, with no Native/Core implementation or dependency changes. Final focused checks and paired observations were rerun on the refreshed base. This is the first published handoff on this branch; no prior #996 reference SHA is superseded. The separate [#995 handoff](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/HANDOFF.md) is an unchanged pinned input, not an installed dependency or transferred authority.

Validation performed: **72 passing tests, zero skips/failures**—69 reference cases plus three unchanged Native baseline cases; pinned Python 3.12.3/locked Native dependencies; Ruff 0.16.0 lint/format; mypy 2.3.0 strict on the two storage modules; real captured Native comparisons; unchanged-product-tree and source-drift review. [VALIDATION.md](VALIDATION.md) specifies the actual scope.

Not validated: repository `make lint` cannot start without `mise`; complete product/merge-checkout CI, F1, integrated Native optional recovery/default-policy eligibility, installed/platform/backend/full-lifecycle growth/receiving and physical-sync/power-loss qualification remain unperformed. Parent SQL removal does not remove the existing child transactions. Storage pin/selector tests are isolated protocol witnesses, not Core/retained-resource implementations.

This branch is external reference input. Requesting, creating, testing, publishing or linking it does not establish or extend an accepted design, contract, release requirement or permission to integrate. Authority remains with #996/#994, subsequent maintainer decisions and Riverhog's normal integration rail. Reconcile and explicitly reproduce/reuse accepted material; do not mechanically apply this branch or copy reference work documents into main.

The exact published reference SHA recorded on #996 is the durable handoff identity; the branch name is navigation. No native Development association, closing PR, issue closure, merge, release or provider action is part of this handoff. After consumption, record a finite adopted/reproduced/partial/superseded/rejected disposition and remaining qualification on #996.
