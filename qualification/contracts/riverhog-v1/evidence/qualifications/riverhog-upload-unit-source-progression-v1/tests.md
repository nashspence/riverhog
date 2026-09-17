# riverhog-upload-unit-source-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make compose-smoke](../../sources/commands.md#q-413b0b241b), [make provider-qualification](../../sources/commands.md#q-f79927bd2e).

- [tests/unit/test\_incremental\_collection\_producer.py::test\_many\_artifact\_publication\_retains\_only\_the\_unsealed\_pack\_window](../../../../../../tests/unit/test_incremental_collection_producer.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_pack\_volume.py::test\_pack\_unit\_wire\_payload\_is\_only\_concatenated\_source\_bytes](../../../../../../tests/unit/test_pack_volume.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_pack\_upload.py::test\_checkpoint\_resumes\_across\_riverhog\_and\_adapter\_restart](../../../../../../tests/unit/test_pack_upload.py) — Candidate binding; no reviewed behavior scope is recorded.
