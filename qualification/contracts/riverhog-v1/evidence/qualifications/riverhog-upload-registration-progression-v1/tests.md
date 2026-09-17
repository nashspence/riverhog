# riverhog-upload-registration-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make postgres-concurrency](../../sources/commands.md#q-8d9f4da038), [make compose-smoke](../../sources/commands.md#q-413b0b241b).

- [packages/riverhog-client/tests/test\_transform.py::test\_producer\_streams\_bounded\_batches\_without\_limiting\_collection\_size](../../../../../../packages/riverhog-client/tests/test_transform.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/integration/test\_collection\_upload\_custody\_concurrency.py::test\_distinct\_concurrent\_registrations\_preserve\_both\_members](../../../../../../tests/integration/test_collection_upload_custody_concurrency.py) — Candidate binding; no reviewed behavior scope is recorded.
