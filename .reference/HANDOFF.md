# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#991 follow-up 6880916322](https://github.com/nashspence/riverhog/issues/991#issuecomment-6880916322). Convention: [#903](https://github.com/nashspence/riverhog/issues/903).

Branch: `reference/external/991-observer-lifetime`.
Issue-authorized audited base: `6e80898a793532beff7a1718effc0618e2472d58`.
Clean tested implementation: `3e27e21b66da365b3b5385a1cefa2eaf47e3cc86`.
The final handoff appends only `.reference` documentation/evidence to that implementation; all twelve proposed test/support files retain their tested bytes.

Produced by OpenAI ChatGPT; exposed model GPT-6 Astra Pro; Default mode with parallel implementation, test, profile, and independent review work. Requested by the Riverhog integration agent through Nash Spence. Published through the connected GitHub app; commit metadata does not replace this provenance.

## Purpose and first receiving step

Extend the adopted test-owned bounded lifetime through Native observer construction, Native checkpoint/advance helpers, and Core inline receiving. Reuse source metadata while preserving every existing independent root and nested check. The public growth reader, product code, declarations, and custody/concurrency policies remain unchanged.

**First step:** reconcile the final test/support diff from the audited base against current recorder ownership, then run the focused checks in [VERIFY.md](VERIFY.md). Read [DESIGN.md](DESIGN.md) for the four cost boundaries and [RESULTS.md](RESULTS.md) for exact sources, retained observations, checks, and limits.

Core inline receiving requires the explicit outer `SampleBatch((callers.ROOT, units.ROOT), limit=64, include_descendants=True)` used by the retained experiment. Its owner must outlive the caller/unit/Native recorders. Existing root-only callers keep their prior admission. Native `checkpoint_samples` and `advance_samples` use their own four-root scope. These are root-count limits, not byte or RSS guarantees.

The clean-source packet contains 162 passing focused tests, passing pinned Ruff/format/REUSE/mypy checks, one successful three-turn candidate capture, and a matched literal replay retaining all 122 independent roots and 1,078 nested checks. Replay compilation calls fell from 186 to 47; one separate wall-time observation was 24.991175 to 22.320676 seconds. These are observer-helper observations with the limits recorded in RESULTS.md.

This follows the earlier reference `89443ec68dceae61a56990d4181894a3ee368a70`, which the integration agent adopted at the new audited base. The prior branch and pinned handoff remain intact. This packet is the current reference for the remaining lifetime request; it does not undo or requalify that adoption.

## Source and authority

Proposed edits are confined to twelve test/support files. All product/package implementation paths and the public growth-contract reader match the audited base. The `.reference` directory contains finite producer experiments and evidence. The exact published SHA linked from the owning issue is the durable handoff identity; branch names are navigation.

Validation actually performed is recorded in [RESULTS.md](RESULTS.md), with dirty-tree exploratory checks distinguished from clean-source receipts. This reference does not qualify current main, the 95-turn interrupted diagnostic, whole-path growth, publication throughput, full CI, or a release. It adds no timing gate or large trial.

Creation, testing, publication, or the integration agent's request does not establish or extend an accepted design, contract, release requirement, or authority to integrate. Adoption and qualification remain with the owning issue and the repository's normal implementation/integration rail. No native Development association is required.
