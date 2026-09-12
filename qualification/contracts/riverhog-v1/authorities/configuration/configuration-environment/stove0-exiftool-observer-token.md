# STOVE0_EXIFTOOL_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-token:6b950c92dc -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/92`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN` — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_EXIFTOOL_OBSERVER_TOKEN" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c02287675c7a36674517be8b08c2675f8a05e8557c4a0f328224959cc4611705 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_TOKEN"
}
```
