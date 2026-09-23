# riverhog-storage-write-segment-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make compose-smoke](../../sources/commands.md#q-413b0b241b), [make provider-qualification](../../sources/commands.md#q-f79927bd2e).

- [packages/riverhog-storage-adapter-support/tests/test\_storage\_adapter\_support.py::test\_http\_write\_traversal\_crosses\_pages\_and\_process\_restart](../../../../../../packages/riverhog-storage-adapter-support/tests/test_storage_adapter_support.py) — Candidate binding; no reviewed behavior scope is recorded.
- [some-implementations/riverhog/storage/filesystem/tests/test\_filesystem\_storage\_adapter.py::test\_segment\_traversal\_is\_bounded\_exact\_and\_restartable](../../../../../../some-implementations/riverhog/storage/filesystem/tests/test_filesystem_storage_adapter.py) — Candidate binding; no reviewed behavior scope is recorded.
- [some-implementations/riverhog/storage/s3-support/tests/test\_s3\_storage\_adapter.py::test\_resumable\_write\_reconciles\_segments\_and\_lost\_completion](../../../../../../some-implementations/riverhog/storage/s3-support/tests/test_s3_storage_adapter.py) — Candidate binding; no reviewed behavior scope is recorded.
