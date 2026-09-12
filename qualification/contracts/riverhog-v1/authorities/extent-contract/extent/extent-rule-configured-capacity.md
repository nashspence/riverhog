# Extent rule: configured capacity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-configured-capacity:04e3baba41 -->

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `rules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/extents/rules/configured-capacity~1v1`

## Effective policies

- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make contract-freeze`
- Proof: `make operation-qualification`

## Contract

```json
{
  "authority": "the source-linked operator configuration field",
  "capacity_behavior": "explicit-reject-defer-or-throttle",
  "policy": "operational_policy",
  "silent_truncation": "forbidden"
}
```
