# STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-browse-token-lifetime-seconds:7901637236 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/82`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS` — `configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS`
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
| `name` | "STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2bcf43717792d7bbf30d204093b3a110db6c9a471b35db259d62f5a8b7af2ded -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"
}
```
