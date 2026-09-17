# Extent rule: bounded segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-bounded-segment:6e947b7583 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [Extent Contract](index.md) |

## External contract

<a id="p-2b3f3f1594"></a>
[Where this policy applies](../../../policies/extent-rule-bounded-segment-v1/applications.md)

<a id="s-cc8243a831"></a>

| Field | Value |
|---|---|
| <a id="s-1b0d1b06e8"></a>`authority` | `"the owning schema's x-riverhog-extent declaration"` |
| <a id="s-2eb60cfe1e"></a>`completion` | `"the owner-declared progression or repeated-work contract"` |
| <a id="s-909f18b8d2"></a>`exceeded` | `"bounded-carrier-validation-error"` |
| <a id="s-ca689f64fa"></a>`policy` | `"segmented_no_total_max"` |
| <a id="s-04349db30a"></a>`semantic_maximum` | `null` |

## Governing policies

- <a id="pa-4c868cde0f"></a>[extent-rule/bounded-segment/v1](#p-2b3f3f1594)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources/commands.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources/authorities.md#src-5ac94d0a12) — [scripts/extent\_contract.py::extent\_projection](../../../../../../scripts/extent_contract.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/extents/rules/bounded-segment~1v1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
