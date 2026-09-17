# Extent principle: configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-principle-configuration:ed868d994d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [Extent Contract](index.md) |

## External contract

<a id="p-aa533611e9"></a>
[Where this policy applies](../../../policies/extent-principle-configuration-v1/applications.md)


| Field | Value |
|---|---|
| <a id="s-f75ba69202"></a>`configuration` | `"Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked."` |

## Governing policies

- <a id="pa-920506e14c"></a>[extent-principle/configuration/v1](#p-aa533611e9)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources/commands.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources/authorities.md#src-5ac94d0a12) — [scripts/extent\_contract.py::extent\_projection](../../../../../../scripts/extent_contract.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/extents/principles/configuration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad9c9db446693f985358d6094a1ca38d63c535fab151fb471f1916d1d9425083 -->

```json
"Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked."
```

</details>
