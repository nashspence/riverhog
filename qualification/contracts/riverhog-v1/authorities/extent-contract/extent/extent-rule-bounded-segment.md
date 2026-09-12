# Extent rule: bounded segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-bounded-segment:6e947b7583 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [extent](index.md) |
| Family | [rules](index.md#f-b9df1e502970) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cc8243a83193"></a>
| Field | Shape |
|---|---|
| <a id="s-1b0d1b06e8b9"></a>`authority` | "the owning schema's x-riverhog-extent declaration" |
| <a id="s-2eb60cfe1e46"></a>`completion` | "the owner-declared progression or repeated-work contract" |
| <a id="s-909f18b8d259"></a>`exceeded` | "bounded-carrier-validation-error" |
| <a id="s-ca689f64faf8"></a>`policy` | "segmented_no_total_max" |
| <a id="s-04349db30a12"></a>`semantic_maximum` | null |

## Governing policies

- <a id="pa-4c868cde0f00"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources.md#q-8c74349e71e4)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [extent:extent-contract](../../../evidence/sources.md#src-5ac94d0a12e8) — `scripts/extent_contract.py::extent_projection`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/bounded-segment~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42485f405b18cf5471144b12f8e6a7bd05c78bba9e4f67d614f9e5725612ec6e -->

```json
{
  "authority": "the owning schema's x-riverhog-extent declaration",
  "completion": "the owner-declared progression or repeated-work contract",
  "exceeded": "bounded-carrier-validation-error",
  "policy": "segmented_no_total_max",
  "semantic_maximum": null
}
```
