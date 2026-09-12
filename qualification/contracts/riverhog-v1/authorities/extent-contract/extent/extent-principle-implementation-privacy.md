# Extent principle: implementation privacy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-principle-implementation-privacy:33161b9b50 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [extent](index.md) |
| Family | [principles](index.md#f-38214870c8) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ce83c4f0c1"></a>
- Shape: "Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here."

## Governing policies

- <a id="pa-42a1cd0786"></a>[extent-principle/implementation-privacy/v1](../../../policies/index.md#p-5f9f32ebd9)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources.md#src-5ac94d0a12) — `scripts/extent_contract.py::extent_projection`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/principles/implementation_privacy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07993a79f7c7765bcaed9507d56d48ca824daa5f17f94597b36afa302dc99e93 -->

```json
"Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here."
```
