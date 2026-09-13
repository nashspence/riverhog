# riverhog_protocol.RecipeIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-recipeidentitydocument:a37ad0cde8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e558cea1c"></a>
| Field | Shape |
|---|---|
| <a id="s-9eaae69aee"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-11ff291bb0"></a>`distribution` | "riverhog-protocol" |
| <a id="s-c3590830ee"></a>`module` | "riverhog_protocol" |
| <a id="s-c4425c885e"></a>`name` | "RecipeIdentityDocument" |
| <a id="s-0290e175b5"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.RecipeIdentityDocument.validate_identity](riverhog-protocol-recipeidentitydocument-validate-identity.md)

## Governing policies

- <a id="pa-b6d6c2b732"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RecipeIdentityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 252f9c939a0bf48591d084175aa2ea2bc51b536a7c0c5c77de20978ae94d3157 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "81bbe8a24ba76905f82e67de9251e42afc958acd711d9c908bc8802d921fe544",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RecipeIdentityDocument",
  "unit": "export"
}
```
