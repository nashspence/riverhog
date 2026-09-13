# riverhog_protocol.RecipeIdentityDocument.validate_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-recipeidentitydocument-0f00f09506:3d74baee6a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e64d812964"></a>
| Field | Shape |
|---|---|
| <a id="s-dedc6b078f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2d8c87f218"></a>`distribution` | "riverhog-protocol" |
| <a id="s-1e967ac1a6"></a>`module` | "riverhog_protocol" |
| <a id="s-e5e0c65b4b"></a>`name` | "validate_identity" |
| <a id="s-45af1f09ae"></a>`owner` | "riverhog_protocol.RecipeIdentityDocument" |
| <a id="s-40386c1dbd"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.RecipeIdentityDocument](riverhog-protocol-recipeidentitydocument.md)

## Governing policies

- <a id="pa-bcde46daf0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RecipeIdentityDocument.validate_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c13420c4148b36f7e227ec003c70afb895a4d00325fedcf98856fb9544d0ac4d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_identity",
  "owner": "riverhog_protocol.RecipeIdentityDocument",
  "unit": "member"
}
```
