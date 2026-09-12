# STOVE0_WORKSPACE_ASSURANCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-workspace-assurance:7a5e35e225 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/118`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_WORKSPACE_ASSURANCE` — `configuration-environment:STOVE0_WORKSPACE_ASSURANCE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_WORKSPACE_ASSURANCE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e5ae80183b8904ff23d3a7ad37be1d630e4498cd8f259e6bce8523ff58582669 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_WORKSPACE_ASSURANCE"
}
```
