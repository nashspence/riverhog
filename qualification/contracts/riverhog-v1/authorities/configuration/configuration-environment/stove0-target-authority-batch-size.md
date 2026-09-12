# STOVE0_TARGET_AUTHORITY_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-authority-batch-size:101537ae1a -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/112`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE` — `configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE`
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
| `name` | "STOVE0_TARGET_AUTHORITY_BATCH_SIZE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fed65789d3020d9b4b7c7a28e1526dd6ca619912787744890a162ccddb522ef -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGET_AUTHORITY_BATCH_SIZE"
}
```
