# STOVE0_ADMISSIONS_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-admissions-path:4cf1a4a644 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/78`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_ADMISSIONS_PATH` — `configuration-environment:STOVE0_ADMISSIONS_PATH`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_ADMISSIONS_PATH" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d647a63f9a08a2ebe093a1962bf59bb0cfba99865c06a718c3015fd02f683b8a -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_ADMISSIONS_PATH"
}
```
