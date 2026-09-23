# stove0_recipe_config.FactPredicate.valid_scope

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-factpredicate-valid-scope:2397fccb59 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bc49ea8c5f"></a>
- <a id="s-25ebefab0d"></a>`distribution`: `stove0-recipe-config`
- <a id="s-1a28047002"></a>`module`: `stove0_recipe_config`
- <a id="s-840daccf2d"></a>`name`: `valid_scope`
- <a id="s-173a035e3e"></a>`owner`: `stove0_recipe_config.FactPredicate`
- <a id="s-f5f5d40a04"></a>`unit`: `member`

### Declared structure

- <a id="s-5f04cbdfb5"></a>`kind`: `"method"`
- <a id="s-631dadbcde"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [FactPredicate](stove0-recipe-config-factpredicate.md)

## Governing policies

- <a id="pa-f8db07a919"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources/authorities.md#src-9e1422d2d6) — [some-implementations/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.FactPredicate.valid_scope`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fce82e1f6f8a76be6d194249ddcf065fd886480196ee5fe731f320d3e1b1ef21 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "valid_scope",
  "owner": "stove0_recipe_config.FactPredicate",
  "unit": "member"
}
```

</details>
