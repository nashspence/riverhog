# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#999](https://github.com/nashspence/riverhog/issues/999). Convention: [#903](https://github.com/nashspence/riverhog/issues/903). Controlling design: [#994](https://github.com/nashspence/riverhog/issues/994).

Branch: `reference/external/999-storage-recovery`. The exact published commit in the owning #999 comment is the durable handoff identity; the branch name is navigation only. This is the first #999 handoff and supersedes no earlier reference.

Audited base: `codex/964-session-owner` ([#979](https://github.com/nashspence/riverhog/pull/979)) at **c28eceaa2dfec1b3712135469ba12230e74f5ffa**. Main was observed at `209481c189f7387557a85e1c249184642816e103`. #994/#999 establish the integration-source relationship. PR head and owning issue were rechecked without drift before publication.

- Produced by: OpenAI ChatGPT.
- Model: GPT-6 Astra Pro.
- Configuration: Default mode with delegated adapter, discovery, materialization and independent policy/safety reviews. Hidden reasoning/effort configuration is not asserted.
- Requested by: nashspence, Riverhog maintainer, in this conversation.
- Published by: OpenAI ChatGPT through connected GitHub tools. Git author metadata is publication metadata, not independent producer provenance.

**Current reading entrypoint: [IMPLEMENTATION.md](IMPLEMENTATION.md).** It contains the settled owner map and integration order, routing to storage/copy, retrieval/cache, discovery, local materialization/independent recovery, and common capacity/receiving details. [QUALIFICATION.md](QUALIFICATION.md) specifies remaining implementation checks; [VALIDATION.md](VALIDATION.md) records only actual evidence and its limitations.

**First focused integration step:** on then-current #979, implement the original copy executor and volatile advisory progress using existing exact write/result mechanics, retaining source/destination pins, accepted receipts and original receiving. Run the paired actual-copy witness and original-session, sealed-object/lost-acceptance, cancellation and completed-ledger tests. Cheap copy creates zero optional checkpoints without changing archive bytes/backing. Coordinate shared contracts with #995–#998; do not wait for a whole-repository census or all #964.

Contents: concrete implementation/state/public-contract decisions; complete owner/no-change classification; actual Core/adapter/Native/materialization experiments and failure witnesses; exact commands, counters, timings and retained initial failures. The proposal resolves original paid restore identity/renewed consent, quota commit/expiry/rollover, cache/retirement receiving, private discovery generations, no-clobber output/repair and independent recovery. Source relationship: **all product paths match c28eceaa; only `.reference/` is added.** Reference code is executable input, not an integrated production patch. Earlier #995–#998 handoffs remain separately pinned and unchanged.

Validation performed: **48 final reference cases passed**, with no failures/errors/skips; four separate maintained baseline cohorts contain **139 distinct successful cases**, including documented narrow reruns for ten proxy-construction failures and two JUnit setup errors. Pinned reference Ruff lint/format and scoped strict typing passed. Exact code hashes, source reconciliation, product-diff, document-link and artifact checks are recorded with the evidence.

Not validated: full repository `make lint` is blocked by missing `mise`; Docker/official-age/PostgreSQL/native platform/installed-only/full lifecycle qualification, physical power loss, real providers, integrated long-work optional economics, complete public/state activation and exact final integration/merge CI remain unperformed. Test fixtures and Python decrypt substitution do not satisfy those gates. No live provider, operator-destructive, merge, deployment or release action was performed.

This external reference establishes no new repository authority. Owning issues, maintainer decisions and the normal integration rail control; reconcile before use and preserve existing WIP/protected commitments. Publication does not complete #999/#994, resume #991, require native GitHub Development linkage, or authorize a merge/release. Record finite adoption/reproduction/disposition in #999 rather than maintaining an alternate implementation branch.
