# Extent rule: route progression

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-route-progression:528b079273 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [extent](index.md) |
| Family | [rules](index.md#f-b9df1e5029) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c35859ad2c"></a>
| Field | Shape |
|---|---|
| <a id="s-1aeb3f70a7"></a>`authority` | "the route-owned x-riverhog-read-collection declaration" |
| <a id="s-3f1439835e"></a>`completion` | "the owning progression contract" |
| <a id="s-aa5edca8df"></a>`policy` | "segmented_no_total_max" |

## Governing policies

- <a id="pa-bcac356772"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources.md#src-5ac94d0a12) — `scripts/extent_contract.py::extent_projection`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/route-progression~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 524dea7a4d43815099baefe219d38866fbdfabfb1e615203e0c41da3cc11fca0 -->

```json
{
  "authority": "the route-owned x-riverhog-read-collection declaration",
  "completion": "the owning progression contract",
  "policy": "segmented_no_total_max"
}
```
