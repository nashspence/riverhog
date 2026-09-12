# STOVE0_OBSERVERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-observers-json:c8c967c775 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/107`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_OBSERVERS_JSON` — `configuration-environment:STOVE0_OBSERVERS_JSON`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_OBSERVERS_JSON" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c87f75bb5b82ad11903b7312905f1c4038bfbd96a1e5325e42cae7926ec1f59a -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_OBSERVERS_JSON"
}
```
