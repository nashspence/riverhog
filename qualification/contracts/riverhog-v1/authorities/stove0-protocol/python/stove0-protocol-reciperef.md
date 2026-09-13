# stove0_protocol.RecipeRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-reciperef:1fdcf0f8c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff2a8e9869"></a>
| Field | Shape |
|---|---|
| <a id="s-c129fec6ce"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-88ec72f004"></a>`distribution` | "stove0-protocol" |
| <a id="s-af47c72582"></a>`module` | "stove0_protocol" |
| <a id="s-d2319a6f09"></a>`name` | "RecipeRef" |
| <a id="s-0b72ff65f5"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.RecipeRef.from_identity](stove0-protocol-reciperef-from-identity.md)
- [stove0_protocol.RecipeRef.to_identity](stove0-protocol-reciperef-to-identity.md)

## Governing policies

- <a id="pa-c49255b322"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.RecipeRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf5370c550ff48bbdb66e6c9cdba6fbd3c7640a924930d1708d6b1f198247c45 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3790d8f811ebb13fed76cbd0a115a818961abe76707214c58706e9033077b650",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[int, Ge(ge=1)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "RecipeRef",
  "unit": "export"
}
```
