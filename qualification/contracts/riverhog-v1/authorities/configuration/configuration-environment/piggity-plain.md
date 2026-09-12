# PIGGITY_PLAIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-plain:d316d5221e -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/4`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:PIGGITY_PLAIN` — `configuration-environment:PIGGITY_PLAIN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "PIGGITY_PLAIN" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 96c1a14dc58375dfd9b99106670331d30d79c0fa8c8e80a7e5b6a34f33a949c9 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_PLAIN"
}
```
