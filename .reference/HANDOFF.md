# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#991 follow-up 6882480917](https://github.com/nashspence/riverhog/issues/991#issuecomment-6882480917). Convention: [#903](https://github.com/nashspence/riverhog/issues/903).

Branch: `reference/external/991-context-lookups`.
Audited base: issue-authorized `6e80898a793532beff7a1718effc0618e2472d58`.
Later inspected integration source: PR #979 at `824383a8a8143aac033f86dd1562a5979f6ec4da`; its complete growth-reader file and locked environment match the audited base.

Produced by OpenAI ChatGPT; exposed model GPT-6 Astra Pro; Default mode with parallel implementation, adversarial test preparation, profiling, and review. Requested by the Riverhog integration agent through Nash Spence. Published through the connected GitHub app; commit metadata does not replace this provenance.

## Purpose and first receiving step

Investigate fixed lookup-map reconstruction during unit derivation. The proposed package edit adds two fresh private lookup maps to each parsed context and changes only three lookup sites. Every expression visit and semantic check remains. Source-bound context construction and all numeric/sample validators retain their existing code.

**First step:** reconcile the focused growth-reader diff against the receiving source, confirm its existing budgets and per-edge checks remain intact, then run the adversarial and retained-input comparisons in [VERIFY.md](VERIFY.md). [DESIGN.md](DESIGN.md) explains the lifetime and memory tradeoff; [RESULTS.md](RESULTS.md) identifies actual source, evidence, and limits.

The previous lifetime reference `5e21cc9c75c86ec08ab4b1c755d98c36bd4e5195` was adopted by the integration agent. This packet is the current entrypoint for the new lookup request; it neither reverses that adoption nor copies the later observer changes into the older audited base. The proposed reader edit is the material delta. Current main was inspected at `209481c189f7387557a85e1c249184642816e103`; its older reader budgets are not the source of this experiment.

## Source and authority

The growth-contract reader implementation contains the proposed edit. Other package/product implementation, declaration formats, public interfaces, and dependency locks retain the audited source. The exact tested implementation and any later evidence-only append are distinguished in RESULTS.md. No result from this reference qualifies current main or the queued integration source.

This externally produced material establishes no new contract, timing gate, release qualification, or authority to integrate. Authority remains with the owning issue, maintainer decisions, and the normal implementation/integration rail. The owning issue's exact published reference SHA is the durable handoff identity; the branch name is navigation. No native Development association is required.
