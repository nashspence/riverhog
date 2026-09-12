# STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-token-file:7b18cfb96a -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/93`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE` — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e71951650dd52863b47c635acdf2d7e7d6e3d57249e9bca3010be663c2895a7 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE"
}
```
