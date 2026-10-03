# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #951
Convention: #903
Audited base: main @ e854feebf9cb8491ae42e3d743e9514f6780e27b

Produced by: OpenAI ChatGPT
Model: GPT-5.6 Sol
Configuration: reasoning/effort level not exposed
Requested by: Riverhog maintainer
Published by: OpenAI ChatGPT via the connected GitHub integration

Purpose: Provide a researched, executable reference for the recommended v1 retrieval-selection contract in #951: global cache preference across archive-copy source names, strict optional archive-source selection for misses, and separate durable pinning of archive fallback versus cache provenance.

Contents:
- `.reference/retrieval_selection/CONTRACT.md` — researched contract, source anchors, integration boundaries, and required proof.
- `.reference/retrieval_selection/model.py` — dependency-free executable selection model.
- `.reference/retrieval_selection/test_model.py` — focused behavioral witnesses for cache/source ordering, strict overrides, incarnation fencing, partial coverage, and fail-closed copy equivalence.

This branch is externally produced reference material for the owning issue.
Its creation, testing, publication, native issue linkage, or request by an
authorized repository agent does not itself establish or extend an accepted
design, contract, release requirement, or authorization to integrate these
changes.

Authority remains with the owning issue, subsequent maintainer decisions,
and the repository's normal integration rail.

The exact reference commit linked from the owning issue is the handoff
identity; this branch name is navigation only. Reconcile this material
against then-current authoritative repository state and the current owning-
issue decisions; do not apply it mechanically.

Validation performed:
- Audited the exact repository snapshot for `main@e854feebf9cb8491ae42e3d743e9514f6780e27b`.
- Reviewed #763, #869, #908, #909, #492, and #903 against the current implementation boundaries relevant to retrieval/cache/storage identity.
- Ran the standalone reference model tests.
- Ran Python bytecode compilation for the reference model and tests.

Not validated / known limitations:
- This is reference-only design/model material; it intentionally changes no production Riverhog modules or database baseline.
- Full repository `make lint`, `make unit`, `make dist-smoke`, and `make build` are not meaningful evidence for an isolated `.reference` model and were not run as production qualification.
- PostgreSQL migration/baseline changes, HTTP/OpenAPI/client parity, provider qualification, and cache concurrency are integration work.
- The publication interface available to this producer does not expose a native GitHub Development relationship action; if still unavailable at publication time, the owning issue will record that limitation explicitly as required by #903.
