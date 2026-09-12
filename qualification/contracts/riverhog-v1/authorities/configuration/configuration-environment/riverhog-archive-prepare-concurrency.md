# RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-prepare-concurrency:2f282ebc50 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/15`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY` — `configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f000b8abea6ef3c278a5e7fc557fdaeb93469ea3a513ca1167fdb6d108b197c8 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY"
}
```
