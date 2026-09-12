# Extent rule: no semantic maximum

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-no-semantic-maximum:dbf6e2bff5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [extent](index.md) |
| Family | [rules](index.md#f-b9df1e5029) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7ac3159837"></a>
| Field | Shape |
|---|---|
| <a id="s-9ed4171447"></a>`authority` | "the owning schema's deliberate absence of a semantic maximum" |
| <a id="s-90128b405f"></a>`declared_operational_maximum` | null |
| <a id="s-cd86a2fc86"></a>`future_capacity_behavior` | "explicit-configured-reject-defer-or-throttle" |
| <a id="s-4bea6ab41f"></a>`hidden_maximum` | "forbidden" |
| <a id="s-2c27f63017"></a>`policy` | "operational_policy" |
| <a id="s-c7fdc07b8a"></a>`semantic_maximum` | null |
| <a id="s-0f1c9f201f"></a>`silent_truncation` | "forbidden" |

## Governing policies

- <a id="pa-4fd09539fd"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources.md#src-5ac94d0a12) — `scripts/extent_contract.py::extent_projection`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/no-semantic-maximum~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74ed836834648a8834851fe68e382be2989d41657f840c93889a7817b65f8f32 -->

```json
{
  "authority": "the owning schema's deliberate absence of a semantic maximum",
  "declared_operational_maximum": null,
  "future_capacity_behavior": "explicit-configured-reject-defer-or-throttle",
  "hidden_maximum": "forbidden",
  "policy": "operational_policy",
  "semantic_maximum": null,
  "silent_truncation": "forbidden"
}
```
