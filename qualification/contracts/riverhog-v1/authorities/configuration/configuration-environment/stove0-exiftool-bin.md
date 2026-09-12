# STOVE0_EXIFTOOL_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-bin:41bb0dad1c -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/87`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_EXIFTOOL_BIN` — `configuration-environment:STOVE0_EXIFTOOL_BIN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_EXIFTOOL_BIN" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80ab2cd142180343f0f7f4e80a0ddb1b726ae96c440a7a95fdd1673a8d71c9a3 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_BIN"
}
```
