# stove0-read-collection-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make database-qualification](../../sources/commands.md#q-27f281b51e), [make compose-smoke](../../sources/commands.md#q-413b0b241b).

- [tests/unit/test\_public\_interface\_parity.py::test\_public\_read\_collection\_selectors\_are\_bounded\_and\_frozen](../../../../../../tests/unit/test_public_interface_parity.py) — Candidate binding; no reviewed behavior scope is recorded.
- [reference/stove0/application/tests/test\_cli.py::test\_stove0\_bounded\_pages\_keep\_rich\_and\_json\_cli\_parity](../../../../../../reference/stove0/application/tests/test_cli.py) — Candidate binding; no reviewed behavior scope is recorded.
- [reference/stove0/application/tests/test\_work\_state.py::test\_sql\_operational\_retention\_scans\_bounded\_pages\_without\_parsing\_all\_work](../../../../../../reference/stove0/application/tests/test_work_state.py) — Candidate binding; no reviewed behavior scope is recorded.
