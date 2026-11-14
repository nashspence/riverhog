# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#991 request, comment 6877686477](https://github.com/nashspence/riverhog/issues/991#issuecomment-6877686477). Convention: [#903](https://github.com/nashspence/riverhog/issues/903).

Branch: `reference/external/991-growth-observer-compilation`.
Audited issue-authorized base: `8c80903818b386f50ddce8cd316cb3d93ec8cc51`.

Produced by: OpenAI ChatGPT. Model: GPT-6 Astra Pro. Configuration: Default mode, with parallel implementation, test, and read-only review work. Requested by: the Riverhog integration agent through Nash Spence. Published through Nash Spence's connected GitHub account; publication metadata does not replace this producer record.

## Purpose and first step

Reduce repeated growth-declaration checking while preserving every actual sample check. The proposed source contains a compilation-local set of checked declaration/metric bodies and a test-owned, explicitly bounded recorder batch using the existing public reader. No public API or declaration schema changes.

**First focused receiving step:** reconcile the small package diff with current authoritative source, then run the two new test modules using the pinned environment and an external temporary directory as shown in [VERIFY.md](VERIFY.md). The batching helper is an optional recorder integration point; assess it with the owning recorder before adoption.

Read [DESIGN.md](DESIGN.md) for invariants and bounded lifetime, and [RESULTS.md](RESULTS.md) for exact source identities, measurements, receipts, and limits. The exact published handoff SHA in the owning issue is the durable identity; the branch name is navigation.

## Source relationship

Product service, journal, and database paths match the audited base. Proposed edits are confined to the growth-contract reader, test support, and caller/unit recorder wiring. The `.reference` directory contains producer documentation and finite experiment evidence. Later source `931f01e65d8849835774a97ffbd2954ae6802ce5` was inspected for drift only: its changes from the audited base are the upload-certificate service and its unit tests, without overlapping this proposal. It was not absorbed or qualified.

This is a new request-specific reference, not a revision of the earlier #964 primary-member lifecycle handoff. It supersedes no previous published handoff.

## Authority and validation boundary

Validation actually performed and omitted is recorded in [RESULTS.md](RESULTS.md). This reference does not qualify current `main`, the full caller census, the previous interrupted run, a distribution, or a release. Integration must independently reconcile and qualify accepted material.

Creation, testing, publication, and the integration agent's request do not establish or extend an accepted design, contract, release requirement, or permission to integrate. Authority remains with the owning issue, subsequent maintainer decisions, and the normal implementation/integration rail. No native Development association is required.
