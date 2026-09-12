# schemas: RecipeCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipecatalogview:293d7d1142 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: RecipeCatalogView
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `recipes` | yes | type="array"; items=(#/components/schemas/RecipeView) |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RecipeView](schemas-recipeview.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeCatalogView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6fcb9e5527342a1d384115f206f2bf6257a2e8a7ed0d10afae020d23a4fa667 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "catalog_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Catalog Sha256",
      "type": "string"
    },
    "recipes": {
      "items": {
        "$ref": "#/components/schemas/RecipeView"
      },
      "title": "Recipes",
      "type": "array"
    }
  },
  "required": [
    "catalog_sha256",
    "recipes"
  ],
  "title": "RecipeCatalogView",
  "type": "object"
}
```
