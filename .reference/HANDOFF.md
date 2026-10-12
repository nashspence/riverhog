# NON-AUTHORITATIVE EXTERNAL REFERENCE

## Latest handoff: G27/G28 researched integration design

Direct reference parent for this handoff: `6c66476fc88738b51526c2295f95b5720bb60140`.
Audited main/tree: `152d095038df0c0d0ed642cf265d47b11c65a754` /
`991e994457b487a7dca016b7fc0409485cef25c6`. The historical runtime base below is
unchanged; run source diagnostics against the separate audited main checkout.

Start at [g27_g28/DESIGN.md](g27_g28/DESIGN.md), then
[g27_g28/RESEARCH.md](g27_g28/RESEARCH.md),
[g27_g28/README.md](g27_g28/README.md) and
[g27_g28/evidence.json](g27_g28/evidence.json).

This supplies the detailed G27/G28 recommendations: real non-null composite seeks;
separate mutable, exact discovery and sealed-ordinal contracts; exact one-pass
assertion/profile matching; G26 factored query projection and same-member guards;
prepare-once disk-backed complete results with unchanged-view fencing and live
authorization; collection-summary transactional deltas; named cost profiles,
implementation interfaces/owners and a staged acceptance checklist.

New validation: 53 reference tests passed (including 480 seeded native matcher
comparisons), plus 12 unchanged native tests passed / one HTTP test deliberately
deselected. Warnings were errors. Native SQLite/SQLAlchemy work probes retained.
Not run: production summary/fence/query integration, PostgreSQL plans/concurrency,
locked mainline gates, Docker/image lifecycle, exact-SHA CI qualification, or timing
target proof. Model counters are not production conformance. See evidence for
actual commands, dependencies and corrected failed development attempts.

Produced by OpenAI ChatGPT; model GPT-6 Astra Pro; effort/configuration not exposed.
Requested by the Riverhog maintainer; published through the connected GitHub
integration. This handoff supersedes earlier generic G27/G28 proposal guidance,
not accepted issue decisions, G22/G26 material, or the reviewed 64-family inventory
structure. No new owner-bound/profile approval is claimed. Keep the latest #961
onboarding/#963 sequencing and #964 comment 6114967253; do not edit AGENTS.md here.

Only `.reference` files change. No mainline, release, provider or required-check
setting changes. The exact published SHA in #964 is the durable handoff identity.
Native Development/dependency mutation is unavailable to this interface; any
existing relationship is left intact, and no Markdown URL is claimed to establish
one. Normal integration and current issue authority remain binding under #903.

---

The prior G22/G26 handoff is retained byte-for-byte at
[HANDOFF-G22-G26.md](HANDOFF-G22-G26.md). Its producer provenance,
audited base and approved G26/Alembic boundaries remain intact; this new handoff
adds G27/G28, rather than republishing historical tests as new results.
