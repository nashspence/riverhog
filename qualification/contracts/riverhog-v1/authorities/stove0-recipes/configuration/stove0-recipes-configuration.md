# stove0-recipes configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-recipes:stove0-recipes-configuration:d7b3e8d372 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-recipes` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 39 |

## Machine authority

- `/external_contract/configuration_documents/stove0-recipes`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `configuration:stove0-recipes` — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/models.py::RecipeCatalog`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=67108864, minimum=1, reason=schema-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| value | schema-value | `contract_max` | maximum=86400, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Contract

- `title`: RecipeCatalog
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `format` | no | string |  |
| `operations` | yes | array |  |
| `recipes` | yes | array |  |

### Definitions

| Definition | Shape |
|---|---|
| `ArtifactAssociation` | object |
| `ArtifactFactBinding` | object |
| `ArtifactRule` | object |
| `FactPredicate` | object |
| `InputArtifactContract` | object |
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `ObserverUse` | object |
| `OperationContract` | object |
| `OperationProjection` | object |
| `OutputArtifactContract` | object |
| `RecipeCoordinationRoute` | object |
| `RecipeDefinition` | object |
| `RecipeJoin` | object |
| `RecipeJoinMember` | object |
| `RecipeRef` | object |
| `RecipeRoute` | object |
| `SemanticValidationProfile` | object |
