# Extent principle: implementation privacy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-principle-implementation-privacy:33161b9b50 -->

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `principles` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/extents/principles/implementation_privacy`

## Effective policies

- `extent-principle/implementation-privacy/v1`

## Executable sources and proof

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make contract-freeze`
- Proof: `make operation-qualification`

## Contract summary

- Shape: "Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here."

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07993a79f7c7765bcaed9507d56d48ca824daa5f17f94597b36afa302dc99e93 -->

```json
"Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here."
```
