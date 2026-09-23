# riverhog-work-disposition-append/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make postgres-concurrency](../../sources/commands.md#q-8d9f4da038), [make compose-smoke](../../sources/commands.md#q-413b0b241b).

- [some-implementations/stove0/application/tests/test\_riverhog\_adapter.py::test\_post\_root\_settlement\_restarts\_from\_bounded\_portable\_inventory\_progress](../../../../../../some-implementations/stove0/application/tests/test_riverhog_adapter.py) — Candidate binding; no reviewed behavior scope is recorded.
- [tests/integration/test\_collection\_deletion\_concurrency.py::test\_postgres\_concurrent\_first\_disposition\_and\_output\_create\_one\_set](../../../../../../tests/integration/test_collection_deletion_concurrency.py) — Candidate binding; no reviewed behavior scope is recorded.
