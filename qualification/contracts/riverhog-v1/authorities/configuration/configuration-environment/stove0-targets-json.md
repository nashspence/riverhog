# STOVE0_TARGETS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-targets-json:431130b2bd -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/111`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_TARGETS_JSON` — `configuration-environment:STOVE0_TARGETS_JSON`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_TARGETS_JSON" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7874e6e3ef6287aab088d90e3a049674b4cf961751328e22f1033b4b2b628a90 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGETS_JSON"
}
```
