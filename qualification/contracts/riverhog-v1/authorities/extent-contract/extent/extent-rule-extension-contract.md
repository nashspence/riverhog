# Extent rule: extension contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-extension-contract:432e5bb05c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [extent](index.md) |
| Family | [rules](index.md#f-b9df1e5029) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-880614cb87"></a>
| Field | Shape |
|---|---|
| <a id="s-303840ad30"></a>`authority` | "the independently versioned extension contract" |
| <a id="s-d368ac3e44"></a>`core_semantic_maximum` | null |
| <a id="s-01a1566171"></a>`policy` | "extension_owned" |

## Governing policies

- <a id="pa-2eda1688d1"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources.md#src-5ac94d0a12) — `scripts/extent_contract.py::extent_projection`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/extension-contract~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efca5346e716188b9a70547266ccd8626625735fffbb778123b478aa23865ec2 -->

```json
{
  "authority": "the independently versioned extension contract",
  "core_semantic_maximum": null,
  "policy": "extension_owned"
}
```
