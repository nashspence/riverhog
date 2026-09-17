# riverhog-raw-digest-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make compose-smoke](../../sources/commands.md#q-413b0b241b), [make provider-qualification](../../sources/commands.md#q-f79927bd2e).

- [tests/unit/test\_raw\_ingress\_manifest.py::test\_raw\_source\_is\_hashed\_once\_into\_small\_authority\_and\_bounded\_batches](../../../../../../tests/unit/test_raw_ingress_manifest.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/unit/test\_raw\_upload.py::test\_raw\_upload\_resumes\_on\_server\_defined\_age\_part\_boundaries](../../../../../../tests/unit/test_raw_upload.py) — Candidate binding; no reviewed behavior scope is recorded.
