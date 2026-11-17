# NON-AUTHORITATIVE EXTERNAL REFERENCE

**Owner:** [#1000](https://github.com/nashspence/riverhog/issues/1000) · **Controlling design:** [#994](https://github.com/nashspence/riverhog/issues/994) · **Convention:** [#903](https://github.com/nashspence/riverhog/issues/903)

- **Branch:** `reference/external/1000-cutover-qualification`
- **Audited base:** `codex/964-session-owner@c28eceaa2dfec1b3712135469ba12230e74f5ffa` ([#979](https://github.com/nashspence/riverhog/pull/979))
- **Produced and published by:** OpenAI ChatGPT, GPT-6 Astra Pro, with delegated source/evidence reviews.
- **Configuration:** Default collaboration mode; effort setting not exposed.
- **Requested by:** Riverhog maintainer in the current conversation.

## Current handoff and first step

This is the first #1000 reference packet. It proposes the concrete qualification implementation, paired measurement contract, coverage reconciliation and finite closure across #995–#999. Those separately pinned references remain implementation input; this packet does not supersede them or transfer their ownership.

**First integration/verification step:** instrument and qualify the existing authenticated/encrypted F1/C1/K2/Q1 publication slice alongside the shared #995/#996/#997 implementation. Preserve a separately identified legacy observation, then prove the redesigned cheap path creates **zero optional checkpoints and no optional resume store**, while retaining mandatory effects, complete original receiving and fresh/warm/evicted archive verification. Use the same executor for the subsequent default adaptive promotion/restart demonstration. Do not wait for a completed whole-repository census.

Start with [IMPLEMENTATION.md](IMPLEMENTATION.md) for the exact owner/file changes and sequence. Consult the focused details only as needed:

| Concern | Specification |
|---|---|
| Three arms, strict matching, clocks, counters and report-only comparisons | [MEASUREMENTS.md](MEASUREMENTS.md) |
| Source-derived coverage, state preservation, contracts and installed gates | [COVERAGE_AND_CUTOVER.md](COVERAGE_AND_CUTOVER.md) |
| Actual process-kill witness, changed fault boundaries and lifecycle growth | [SAFETY_AND_GROWTH.md](SAFETY_AND_GROWTH.md) |
| Exact policy obligations, final head/merge evidence and parent closure | [POLICY_AND_CLOSURE.md](POLICY_AND_CLOSURE.md) |
| What actually ran, original records and remaining qualification | [VALIDATION.md](VALIDATION.md) · [artifact manifest](VALIDATION.json) |

## Source relationship and authority

All additions are inside `.reference/`; product paths exactly match the audited c28 source. The parent explicitly selects the #979 integration rail, explaining the non-main base. `main@209481c189f7387557a85e1c249184642816e103` was inspected for drift, not tested. The clean-source F1 and later reference-authoring executions retain their distinct source records. Reconcile then-current #979 and accepted issue decisions before integration; preserve its WIP.

This branch is externally produced reference material. Its creation, testing, publication, issue reference or maintainer request does not itself establish or extend an accepted design, contract, release requirement or authorization to integrate. Authority remains with the owning issues, subsequent maintainer decisions and the repository's normal integration rail. Reference code is explicit integration input, not a new product command or second registry.

The **actual published commit recorded on #1000** is the durable handoff identity; the branch is navigation. No native Development association is requested. Record finite adoption/reproduction/partial-use/supersession/rejection on the relevant issue. This packet does not claim the redesigned product, full qualification or #994 closure is complete, and it does not resume #991.
