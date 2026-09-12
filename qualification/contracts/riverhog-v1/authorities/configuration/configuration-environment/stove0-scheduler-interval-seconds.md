# STOVE0_SCHEDULER_INTERVAL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-scheduler-interval-seconds:7bc6463af2 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/110`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS` — `configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS`
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
| `name` | "STOVE0_SCHEDULER_INTERVAL_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0339ff465a59246f9e7bcfee50ca5d24ba85c37ec2e72640e4758a98590f0b7f -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_SCHEDULER_INTERVAL_SECONDS"
}
```
