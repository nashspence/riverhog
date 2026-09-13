# stove0_operator_contracts.RecipeCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-recipecatalogview:fd87349113 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b818e8959"></a>
| Field | Shape |
|---|---|
| <a id="s-0a1a96fa87"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6d179e3636"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-997203dcf0"></a>`module` | "stove0_operator_contracts" |
| <a id="s-b83e24cb66"></a>`name` | "RecipeCatalogView" |
| <a id="s-045247b0e7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-40779a14ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.RecipeCatalogView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40443f337f29c478e1f66a7fa6b17924b616fb251e5ba0cb11c7f7efe92a05a0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "08ee97d6df95f95a30d8ad8dd11a1688c3645e24bbed03251383f15e8a5e353c",
    "signature": "\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], recipes: tuple[stove0_operator_contracts.RecipeView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "RecipeCatalogView",
  "unit": "export"
}
```
