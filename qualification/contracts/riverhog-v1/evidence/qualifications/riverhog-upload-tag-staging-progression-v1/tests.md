# riverhog-upload-tag-staging-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make compose-smoke](../../sources/commands.md#q-413b0b241b), [make provider-qualification](../../sources/commands.md#q-f79927bd2e).

- [tests/unit/test\_incremental\_collection\_producer.py::test\_incremental\_producer\_stages\_unbounded\_logical\_tags\_in\_bounded\_requests](../../../../../../tests/unit/test_incremental_collection_producer.py) — Candidate binding; no reviewed behavior scope is recorded.
- [packages/riverhog-protocol/tests/test\_collection\_tag\_protocol.py::test\_late\_tag\_page\_seeks\_by\_fixed\_identity\_without\_rescanning\_prior\_tags](../../../../../../packages/riverhog-protocol/tests/test_collection_tag_protocol.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_collection\_tags.py::test\_provider\_nodes\_for\_retained\_exact\_revisions\_remain\_recoverable](../../../../../../tests/unit/test_collection_tags.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_collection\_tags.py::test\_tag\_history\_cleanup\_bounds\_all\_subordinate\_rows\_and\_restarts](../../../../../../tests/unit/test_collection_tags.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_collection\_tags.py::test\_tag\_node\_reclamation\_counts\_every\_edge\_and\_node\_row\_at\_work\_one](../../../../../../tests/unit/test_collection_tags.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/integration/test\_catalog\_schema\_postgres.py::test\_postgres\_tag\_history\_cleanup\_serializes\_its\_row\_work\_budget](../../../../../../tests/integration/test_catalog_schema_postgres.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/integration/test\_catalog\_schema\_postgres.py::test\_postgres\_tag\_mutation\_protects\_an\_aba\_root\_before\_its\_first\_commit](../../../../../../tests/integration/test_catalog_schema_postgres.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/integration/test\_collection\_upload\_custody\_concurrency.py::test\_postgres\_upload\_protects\_a\_reused\_tag\_root\_before\_its\_first\_commit](../../../../../../tests/integration/test_collection_upload_custody_concurrency.py) — Candidate binding; no reviewed behavior scope is recorded.
