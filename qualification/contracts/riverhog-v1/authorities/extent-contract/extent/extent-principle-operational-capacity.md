# Extent principle: operational capacity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-principle-operational-capacity:bf93940c4b -->

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `principles` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/extents/principles/operational_capacity`

## Effective policies

- `extent-principle/operational-capacity/v1`

## Executable sources and proof

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make contract-freeze`
- Proof: `make operation-qualification`

## Contract summary

- Shape: "Capacity policy may explicitly reject, defer, or throttle work, but must not silently truncate it or become an undocumented semantic ceiling."

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ad1c9610f6e7e1b6b5604827016cf80d9082d86ff2cc104d767d7b77a3acd5c -->

```json
"Capacity policy may explicitly reject, defer, or throttle work, but must not silently truncate it or become an undocumented semantic ceiling."
```
