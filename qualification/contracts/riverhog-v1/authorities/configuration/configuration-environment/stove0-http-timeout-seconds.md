# STOVE0_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-http-timeout-seconds:b4e1c3045c -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/105`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS` — `configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS`
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
| `name` | "STOVE0_HTTP_TIMEOUT_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4cce47c4651b931fb44e14227aacc1fc97bd7af7f5fb6329d9b1cf19ef9031b6 -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "name": "STOVE0_HTTP_TIMEOUT_SECONDS"
}
```
