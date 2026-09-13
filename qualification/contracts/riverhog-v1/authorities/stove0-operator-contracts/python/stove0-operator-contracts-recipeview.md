# stove0_operator_contracts.RecipeView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-recipeview:793dfbe71d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ecc5064913"></a>
| Field | Shape |
|---|---|
| <a id="s-da03e4b9b4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4a8cfd11ec"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-1afd1bbf3c"></a>`module` | "stove0_operator_contracts" |
| <a id="s-5bc2655503"></a>`name` | "RecipeView" |
| <a id="s-069f123be4"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.RecipeView.exact_digest](stove0-operator-contracts-recipeview-exact-digest.md)
- [stove0_operator_contracts.RecipeView.from_definition](stove0-operator-contracts-recipeview-from-definition.md)

## Governing policies

- <a id="pa-97ffd5ad4f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.RecipeView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 96870257d9eb893b91bb878d1c6c801dbe9ec83a6e85940f401a4675d72b0450 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7eee2b5e52261c8b2e7fee8a80d320dcd30aa7ff4511193c970ec212ddd62a4c",
    "signature": "\"(*, definition: stove0_recipe_config.models.RecipeDefinition, sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "RecipeView",
  "unit": "export"
}
```
