# stove0_recipe_config.RecipeRoute.canonical_members

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-reciperoute-canonical-members:133de3e681 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9173f9ad84"></a>
- <a id="s-952d74e537"></a>`distribution`: `stove0-recipe-config`
- <a id="s-6842d68ebf"></a>`module`: `stove0_recipe_config`
- <a id="s-789e01612e"></a>`name`: `canonical_members`
- <a id="s-c2bb262448"></a>`owner`: `stove0_recipe_config.RecipeRoute`
- <a id="s-c9a70086cb"></a>`unit`: `member`

### Declared structure

- <a id="s-341218a09e"></a>`kind`: `"method"`
- <a id="s-b72457e494"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [RecipeRoute](stove0-recipe-config-reciperoute.md)

## Governing policies

- <a id="pa-0211082934"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — [reference/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeRoute.canonical_members`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e1f3ae3e7d671f594dee60a59bea1dd88637a1c4b3afe22e09102f7cdf30620 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "canonical_members",
  "owner": "stove0_recipe_config.RecipeRoute",
  "unit": "member"
}
```

</details>
