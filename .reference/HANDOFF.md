# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#998](https://github.com/nashspence/riverhog/issues/998). Convention: [#903](https://github.com/nashspence/riverhog/issues/903). Controlling design: [#994](https://github.com/nashspence/riverhog/issues/994).

Branch: [`reference/external/998-application-replay`](https://github.com/nashspence/riverhog/tree/reference/external/998-application-replay). The exact published reference SHA in the owning issue is the durable handoff identity; the branch is navigation. This is the first #998 reference, not a replacement for the separately owned #995, #996 or #997 handoffs.

Audited base: `codex/964-session-owner@c28eceaa2dfec1b3712135469ba12230e74f5ffa`, the current [#979](https://github.com/nashspence/riverhog/pull/979) rail established by #994. Observed main: `209481c189f7387557a85e1c249184642816e103`. The issue's earlier research anchor is `0f8030b0c0cd53eb33456924f070ae5979ed307e`; this reference audited and exercised c28 directly. Final source recheck found the same rail head and unchanged #998/#994 scope. Product paths equal the audited base. All proposed code, design and evidence are inside `.reference/`.

Produced by: OpenAI ChatGPT. Model: GPT-6 Astra Pro. Configuration: Default collaboration mode with delegated source, integration and qualification reviews; no separate exposed effort setting is asserted. Requested by: the Riverhog maintainer (`nashspence`) in this conversation, for their integration agent. Published through the connected GitHub API by the same producer; commit metadata is publication metadata, not additional producer identity.

## Current entrypoint and first step

Read [IMPLEMENTATION.md](IMPLEMENTATION.md) for the selected changes, owner map and integration order. It routes to bounded details rather than requiring an initial full inventory read.

**First focused integration step:** on the current implementation rail, connect one real Stove0 processing claim/target/SDK completion path to the usable #995/#996/#997 shared contracts, retain a zero-allowance original workflow assignment, and replace target callback persistence with the bounded volatile progress slot. Preserve original acceptance/running/terminal barriers. Verify exact result equivalence, zero callback durability writes, stale/cancelled callback rejection and actual worker receiving. Extend on the same owner path; a completed repository census or six serial issue closures is not a prerequisite.

The package also closes concrete effect/ingress/lifetime windows, chooses Core async seal/readback semantics, specifies late disjoint grants and conservative overlapping useful-work accounting, and supplies the Review0 sampler-specific original acceptance/receiving contract. It preserves accepted evidence, source custody and current authorization, with explicit capacity and incompatible-state refusal.

## Actual validation and limits

The final reference suite passed **65 cases** with no failures/errors/skips in **4.090361s** of recorded pytest time (**6.104744s** including process startup). Ruff lint/format passed all eight Python files; strict mypy passed the two typed helpers. Unchanged owner baselines separately observed **63 Core/SDK**, **18 ingress/Gogurt/CLI**, **15 supplied-app**, and **10 distinct Stove0** cases passing. The Stove0 cohort included one environment-corrected rerun; earlier fixture failures remain recorded. These cohorts are not one uninterrupted production campaign.

See [VALIDATION.md](VALIDATION.md) and [VALIDATION.json](VALIDATION.json) for commands, exact hashes, timing, counter measurements and corrections. The final tests use the same eight reference Python files carried by this tree. They do not implement or qualify the complete proposed application hard cut.

The maintained processing profile was blocked at `mise` preflight; Docker is also absent. No installed Compose/codec/GPU/native-service, PostgreSQL, macOS/Windows, power-loss, live-provider, full processing lifecycle or production CI qualification is claimed. [QUALIFICATION.md](QUALIFICATION.md) gives the concrete integration gates.

This external reference establishes no new repository authority. The owning issues, maintainer decisions and normal integration rail control. Reconcile before applying; preserve current WIP and protected commitments. Publication does not complete #998, resume #991, authorize a merge/release, or require native GitHub Development linkage.
