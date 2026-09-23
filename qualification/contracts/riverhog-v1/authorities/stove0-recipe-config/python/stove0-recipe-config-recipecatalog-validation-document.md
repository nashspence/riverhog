# stove0_recipe_config.RecipeCatalog.validation_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecatalog-valida-db28abb25a:fde2d46418 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a16fe120fb"></a>
- <a id="s-d5bc704fd8"></a>`distribution`: `stove0-recipe-config`
- <a id="s-d21b649915"></a>`module`: `stove0_recipe_config`
- <a id="s-9f7728a97b"></a>`name`: `validation_document`
- <a id="s-ede7119ce0"></a>`owner`: `stove0_recipe_config.RecipeCatalog`
- <a id="s-3f4c0c15f5"></a>`unit`: `member`

### Declared structure

- <a id="s-4f6c73cad0"></a>`kind`: `"method"`
- <a id="s-30c57f5dbe"></a>`signature`: `"\"(self) -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [RecipeCatalog](stove0-recipe-config-recipecatalog.md)

## Governing policies

- <a id="pa-be875957f2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources/authorities.md#src-9e1422d2d6) — [some-implementations/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCatalog.validation_document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 809ed315b137cdd4a3e007efcce49434fbf5fad34bda2d3bb1b3b9a405001b41 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "validation_document",
  "owner": "stove0_recipe_config.RecipeCatalog",
  "unit": "member"
}
```

</details>
