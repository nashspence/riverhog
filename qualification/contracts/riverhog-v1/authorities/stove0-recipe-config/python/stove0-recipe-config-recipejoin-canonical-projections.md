# stove0_recipe_config.RecipeJoin.canonical_projections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipejoin-canonical-3e7a91466a:0f6eb665c2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-03e48f47bb"></a>
- <a id="s-f555cac773"></a>`distribution`: `stove0-recipe-config`
- <a id="s-902defd352"></a>`module`: `stove0_recipe_config`
- <a id="s-fe86aa3db5"></a>`name`: `canonical_projections`
- <a id="s-c404ddfb1f"></a>`owner`: `stove0_recipe_config.RecipeJoin`
- <a id="s-64f68ec643"></a>`unit`: `member`

### Declared structure

- <a id="s-444e8fa960"></a>`kind`: `"method"`
- <a id="s-9eb9698ef2"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [RecipeJoin](stove0-recipe-config-recipejoin.md)

## Governing policies

- <a id="pa-f0b661c3f0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources/authorities.md#src-9e1422d2d6) — [reference/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeJoin.canonical_projections`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cce60acdfacf2f6883ad5630d9c118a2cdf91ee27c845e59eb24dcf70e4bbecb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "canonical_projections",
  "owner": "stove0_recipe_config.RecipeJoin",
  "unit": "member"
}
```

</details>
