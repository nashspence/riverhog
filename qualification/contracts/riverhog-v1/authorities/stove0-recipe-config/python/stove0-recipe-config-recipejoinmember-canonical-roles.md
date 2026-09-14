# stove0_recipe_config.RecipeJoinMember.canonical_roles

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipejoinmember-can-350a1722f2:82697e1bbe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2190b21349"></a>
- <a id="s-4d536c2b71"></a>`distribution`: `stove0-recipe-config`
- <a id="s-232833615a"></a>`module`: `stove0_recipe_config`
- <a id="s-86f5e880b7"></a>`name`: `canonical_roles`
- <a id="s-5138b46db1"></a>`owner`: `stove0_recipe_config.RecipeJoinMember`
- <a id="s-d914de2594"></a>`unit`: `member`

### Declared structure

- <a id="s-0634ce3e47"></a>`kind`: `"method"`
- <a id="s-98d446f38c"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [RecipeJoinMember](stove0-recipe-config-recipejoinmember.md)

## Governing policies

- <a id="pa-21ef4c2856"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeJoinMember.canonical_roles`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d03e3f391ec92e56cee201141d211be8d4cf833112d416ebfd388bb585551a2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "canonical_roles",
  "owner": "stove0_recipe_config.RecipeJoinMember",
  "unit": "member"
}
```
