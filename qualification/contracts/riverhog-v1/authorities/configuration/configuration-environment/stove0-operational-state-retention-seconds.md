# STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-operational-state-retention-seconds:6d2f2e0943 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/108`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS` — `configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS`
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
| `name` | "STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aae2ee69833a90a419af98af5bd6c1316ed175901beece77bceed74dc03d6522 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS"
}
```
