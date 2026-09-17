# riverhog-retrieval-work-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make postgres-concurrency](../../sources/commands.md#q-8d9f4da038), [make compose-smoke](../../sources/commands.md#q-413b0b241b).

- [tests/unit/test\_retrieval\_service.py::test\_retrieval\_plan\_resumes\_across\_more\_than\_two\_internal\_segment\_pages](../../../../../../tests/unit/test_retrieval_service.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/integration/test\_catalog\_schema\_postgres.py::test\_postgres\_retrieval\_plan\_advances\_in\_bounded\_restartable\_steps](../../../../../../tests/integration/test_catalog_schema_postgres.py) — Candidate binding; no reviewed behavior scope is recorded.
