# stove0_recipe_config.RecipeCatalog.sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecatalog-sha256:86e49a77b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31bee27fe8"></a>
- <a id="s-b648624797"></a>`distribution`: `stove0-recipe-config`
- <a id="s-8748df0b11"></a>`module`: `stove0_recipe_config`
- <a id="s-841919e6be"></a>`name`: `sha256`
- <a id="s-486eb36b97"></a>`owner`: `stove0_recipe_config.RecipeCatalog`
- <a id="s-8c4a13f63b"></a>`unit`: `member`

### Declared structure

- <a id="s-9a3da91a2c"></a>`kind`: `"property"`
- <a id="s-94d7e480bb"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeCatalog](stove0-recipe-config-recipecatalog.md)

## Governing policies

- <a id="pa-6912888e11"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCatalog.sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad5ef1b5ae69e5cf415e1ab419b7e85ce050ad443df001e26f27f76c2e74a81e -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "sha256",
  "owner": "stove0_recipe_config.RecipeCatalog",
  "unit": "member"
}
```
