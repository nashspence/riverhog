# riverhog-upload-work-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make compose-smoke](../../sources/commands.md#q-413b0b241b).

- [packages/riverhog-protocol/tests/test\_collection\_upload\_transport.py::test\_bounded\_upload\_work\_batch\_binds\_assignment\_and\_checkpoint\_state](../../../../../../packages/riverhog-protocol/tests/test_collection_upload_transport.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_archive\_write\_reaper.py::test\_archive\_maintenance\_drains\_bounded\_progress\_before\_idle\_interval](../../../../../../tests/unit/test_archive_write_reaper.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_incremental\_collection\_producer.py::test\_many\_artifact\_publication\_retains\_only\_the\_unsealed\_pack\_window](../../../../../../tests/unit/test_incremental_collection_producer.py) — Candidate binding; no reviewed behavior scope is recorded.
