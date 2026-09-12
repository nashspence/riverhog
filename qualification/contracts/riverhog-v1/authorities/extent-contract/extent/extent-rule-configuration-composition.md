# Extent rule: configuration composition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-configuration-composition:64e89d3c00 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [extent](index.md) |
| Family | [rules](index.md#f-b9df1e5029) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-21ec1b261e"></a>
| Field | Shape |
|---|---|
| <a id="s-46a0052dda"></a>`authority` | "the owning validated deployment configuration document" |
| <a id="s-1c5407e795"></a>`declared_operational_maximum` | null |
| <a id="s-2136bd6100"></a>`hidden_maximum` | "forbidden" |
| <a id="s-2cc9c2e451"></a>`policy` | "operational_policy" |
| <a id="s-0b439cb781"></a>`semantic_maximum` | null |
| <a id="s-450d61181b"></a>`silent_truncation` | "forbidden" |

## Governing policies

- <a id="pa-9c6ff8cf4f"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources.md#src-5ac94d0a12) — `scripts/extent_contract.py::extent_projection`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/configuration-composition~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ba1d5cdf00d6737eee66c195f42c64dcb0914af7bd8fd8bc1bbc7102b560420 -->

```json
{
  "authority": "the owning validated deployment configuration document",
  "declared_operational_maximum": null,
  "hidden_maximum": "forbidden",
  "policy": "operational_policy",
  "semantic_maximum": null,
  "silent_truncation": "forbidden"
}
```
