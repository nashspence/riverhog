# Extent principle: logical totals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-principle-logical-totals:5865d74355 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [Extent Contract](index.md) |

## External contract

<a id="p-cfe2e12ee6"></a>
[Where this policy applies](../../../policies/extent-principle-logical-totals-v1/applications.md)


| Field | Value |
|---|---|
| <a id="s-a7b7a6a331"></a>`logical_totals` | `"A finite logical total has no product-level semantic maximum unless its owning contract declares one."` |

## Governing policies

- <a id="pa-e412720086"></a>[extent-principle/logical-totals/v1](#p-cfe2e12ee6)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources/commands.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources/authorities.md#src-5ac94d0a12) — [scripts/extent\_contract.py::extent\_projection](../../../../../../scripts/extent_contract.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/extents/principles/logical_totals`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0afc2a854d4a4bd05de44b981fc84a5b0ee1e83e4feb5c6b9711288513cc8a60 -->

```json
"A finite logical total has no product-level semantic maximum unless its owning contract declares one."
```

</details>
