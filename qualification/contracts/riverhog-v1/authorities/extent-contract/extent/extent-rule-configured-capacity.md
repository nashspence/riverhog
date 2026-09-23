# Extent rule: configured capacity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-configured-capacity:04e3baba41 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [Extent Contract](index.md) |

## External contract

<a id="p-3ebc61fc99"></a>
[Indexed applications](../../../policies/extent-rule-configured-capacity-v1/applications.md)

<a id="s-173098820a"></a>

| Field | Value |
|---|---|
| <a id="s-715ac4a65c"></a>`authority` | `"the source-linked operator configuration field"` |
| <a id="s-9fbe47966c"></a>`capacity_behavior` | `"explicit-reject-defer-or-throttle"` |
| <a id="s-c550723871"></a>`policy` | `"operational_policy"` |
| <a id="s-c0abbd1353"></a>`silent_truncation` | `"forbidden"` |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.


## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources/commands.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources/authorities.md#src-5ac94d0a12) — [scripts/extent\_contract.py::extent\_projection](../../../../../../scripts/extent_contract.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/extents/rules/configured-capacity~1v1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d22ade33fd6b9425ca71fc8bcf777549db5a17856ba18d64838679ec8d004742 -->

```json
{
  "authority": "the source-linked operator configuration field",
  "capacity_behavior": "explicit-reject-defer-or-throttle",
  "policy": "operational_policy",
  "silent_truncation": "forbidden"
}
```

</details>
