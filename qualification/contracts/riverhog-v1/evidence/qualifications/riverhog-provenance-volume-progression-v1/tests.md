# riverhog-provenance-volume-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make compose-smoke](../../sources/commands.md#q-413b0b241b), [make provider-qualification](../../sources/commands.md#q-f79927bd2e).

- [packages/riverhog-provenance/tests/test\_segmented\_archive.py::test\_ordered\_segmented\_provenance\_authority\_round\_trips](../../../../../../packages/riverhog-provenance/tests/test_segmented_archive.py) — Candidate binding; no reviewed behavior scope is recorded.
- [packages/riverhog-provenance/tests/test\_segmented\_archive.py::test\_provenance\_segmentation\_limits\_one\_volume\_not\_the\_logical\_total](../../../../../../packages/riverhog-provenance/tests/test_segmented_archive.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_collection\_uploads.py::test\_provenance\_append\_persists\_next\_ordinal\_across\_retry\_and\_restart](../../../../../../tests/unit/test_collection_uploads.py) — Candidate binding; no reviewed behavior scope is recorded.
