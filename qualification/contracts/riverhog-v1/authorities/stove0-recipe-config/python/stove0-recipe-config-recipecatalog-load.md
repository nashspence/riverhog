# stove0_recipe_config.RecipeCatalog.load

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecatalog-load:2c1f54ae36 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-25e182f3a6"></a>
- <a id="s-1bc0a90d7a"></a>`distribution`: `stove0-recipe-config`
- <a id="s-14cd8d7f25"></a>`module`: `stove0_recipe_config`
- <a id="s-3a4463eaca"></a>`name`: `load`
- <a id="s-feb4d294ba"></a>`owner`: `stove0_recipe_config.RecipeCatalog`
- <a id="s-f8669e30b2"></a>`unit`: `member`

### Declared structure

- <a id="s-9380d9728e"></a>`kind`: `"classmethod"`
- <a id="s-1545e8a35f"></a>`signature`: `"\"(cls, path: 'Path') -> 'RecipeCatalog'\""`

## Maintained corroboration

### Related interface records

- [RecipeCatalog](stove0-recipe-config-recipecatalog.md)

## Governing policies

- <a id="pa-ca6d2b1863"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — [reference/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCatalog.load`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40d8bd8236c1372195dd779dc388bcacb71054f8c83949155ee3822f5cae71d6 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, path: 'Path') -> 'RecipeCatalog'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "load",
  "owner": "stove0_recipe_config.RecipeCatalog",
  "unit": "member"
}
```

</details>
