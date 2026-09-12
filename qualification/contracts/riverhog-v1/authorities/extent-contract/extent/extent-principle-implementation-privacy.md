# Extent principle: implementation privacy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-principle-implementation-privacy:33161b9b50 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `principles` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: "Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here."

## Governing policies

- `extent-principle/implementation-privacy/v1`

## Evidence

### Qualification

- `make contract-freeze`
- `make operation-qualification`

### Executable sources

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/principles/implementation_privacy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07993a79f7c7765bcaed9507d56d48ca824daa5f17f94597b36afa302dc99e93 -->

```json
"Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here."
```
