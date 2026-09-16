# stove0_operator_contracts.RecipeView.from_definition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-recipeview-from-definition:7a9935efec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cb7b090a1a"></a>
- <a id="s-d9ea58716f"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-0c375856b2"></a>`module`: `stove0_operator_contracts`
- <a id="s-6e9fa70860"></a>`name`: `from_definition`
- <a id="s-8d1f791426"></a>`owner`: `stove0_operator_contracts.RecipeView`
- <a id="s-94a9538e83"></a>`unit`: `member`

### Declared structure

- <a id="s-01cb1e1e13"></a>`kind`: `"classmethod"`
- <a id="s-aa4e327180"></a>`signature`: `"\"(cls, definition: 'RecipeDefinition') -> 'RecipeView'\""`

## Maintained corroboration

### Related interface records

- [RecipeView](stove0-operator-contracts-recipeview.md)

## Governing policies

- <a id="pa-105f055624"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.RecipeView.from_definition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc7677253d9ac3f0c8d956e52f94d27b5aa70e15240b6929e9f512b62e692fd0 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, definition: 'RecipeDefinition') -> 'RecipeView'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "from_definition",
  "owner": "stove0_operator_contracts.RecipeView",
  "unit": "member"
}
```

</details>
