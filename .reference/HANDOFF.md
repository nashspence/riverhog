# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#991 request, comment 6877686477](https://github.com/nashspence/riverhog/issues/991#issuecomment-6877686477). Convention: [#903](https://github.com/nashspence/riverhog/issues/903).

Branch: `reference/external/991-growth-observer-compilation`.
Audited issue-authorized base: `8c80903818b386f50ddce8cd316cb3d93ec8cc51`.

Produced by: OpenAI ChatGPT. Model: GPT-6 Astra Pro. Configuration: Default mode, with parallel implementation, test, and read-only review work. Requested by: the Riverhog integration agent through Nash Spence. Published through Nash Spence's connected GitHub account; publication metadata does not replace this producer record.

## Purpose and first step

Reduce repeated growth-declaration checking while preserving every actual sample check. The proposed source is a test-owned, explicitly bounded recorder batch using the existing public reader. The entire growth-contract package remains byte-for-byte identical to the audited base.

**First focused receiving step:** reconcile the caller/unit recorder diff with current authoritative source, then run `tests/unit/test_growth_observer_batch.py` using the pinned environment and an external temporary directory as shown in [VERIFY.md](VERIFY.md). The batching helper is an optional recorder integration point; assess it with the owning recorder before adoption.

Read [DESIGN.md](DESIGN.md) for invariants and bounded lifetime, and [RESULTS.md](RESULTS.md) for exact source identities, measurements, receipts, and limits. The exact published handoff SHA in the owning issue is the durable identity; the branch name is navigation.

The selected implementation was profiled and verified at clean published `af68d203f93765bddf9ad89a233a582266f10ce2`. The final handoff appends only `.reference` documentation and evidence to that commit; all implementation/test blobs are identical. Receive the final diff relative to the audited base; intermediate commits retain the research history.

## Source relationship

All product and growth-contract package paths match the audited base. Proposed edits are confined to test support and caller/unit recorder wiring. The `.reference` directory contains producer documentation and finite experiment evidence. Later source `931f01e65d8849835774a97ffbd2954ae6802ce5` was inspected for drift only: its changes from the audited base are the upload-certificate service and its unit tests, without overlapping this proposal. It was not absorbed or qualified.

An initial compilation-memo experiment at `7c78eb33fd310653468c1a2fa9dfac41417c5adf` reduced expression traversals but produced mixed, slower sample-validation timings. Its proposed package changes and associated tests are absent from the current delivered diff; that append-only historical commit and its receipts remain available as research evidence. It is not a recommended optimization or an accepted integration result.

This is a new request-specific reference, not a revision of the earlier #964 primary-member lifecycle handoff. It supersedes no previous published handoff.

## Authority and validation boundary

At the exact selected implementation: **118 focused tests passed**, including the actual three-turn caller fixture and independent serialized-reader checks; repo-wide Ruff, formatting, REUSE, and strict mypy passed using the locked environment. The bounded helper reduced compilation calls from **12 to 5**, preserving all **6 emitted roots and 435 nested sample checks**. One matched replay observed **19.287943 to 12.564396 seconds wall**; it is not an end-to-end product or statistical speedup claim. Original exits, clean start/end identities, and cleanup receipts are retained.

Validation actually performed and omitted is detailed in [RESULTS.md](RESULTS.md). This reference does not qualify current `main`, the full caller census, the previous interrupted run, a distribution, or a release. Integration must independently reconcile and qualify accepted material.

Creation, testing, publication, and the integration agent's request do not establish or extend an accepted design, contract, release requirement, or permission to integrate. Authority remains with the owning issue, subsequent maintainer decisions, and the normal implementation/integration rail. No native Development association is required.
