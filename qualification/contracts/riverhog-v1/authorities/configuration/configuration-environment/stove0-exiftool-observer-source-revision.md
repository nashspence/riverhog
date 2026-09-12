# STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-source-revision:bb124ca481 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/91`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION` — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c92e6da496ce8e0a6a293a3a9cc6e480544c6abd981136be3dc3a2420a15443 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION"
}
```
