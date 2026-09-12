# STOVE0_EXIFTOOL_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-host:93ba793080 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/88`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_HOST` — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_HOST`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_EXIFTOOL_OBSERVER_HOST" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f66d309ba0cb05a29879046d2b6d83145062523048e203034d0e9efbeb3c9c7 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_HOST"
}
```
