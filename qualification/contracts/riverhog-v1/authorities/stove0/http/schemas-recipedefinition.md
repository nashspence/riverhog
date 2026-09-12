# schemas: RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipedefinition:31a48bb826 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeDefinition`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactAssociation](schemas-artifactassociation.md)
- [schemas: ObserverUse](schemas-observeruse.md)
- [schemas: RecipeCoordinationRoute](schemas-recipecoordinationroute.md)
- [schemas: RecipeJoin](schemas-recipejoin.md)
- [schemas: RecipeRoute](schemas-reciperoute.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: RecipeDefinition
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allow_derived_inputs` | no | boolean |  |
| `artifact_associations` | no | array |  |
| `event_input_closure` | no | string |  |
| `id` | yes | string |  |
| `join` | no | object (1 fields) |  |
| `observers` | no | array |  |
| `retirement_grace_seconds` | no | integer |  |
| `revision` | yes | integer |  |
| `routes` | yes | array |  |
| `source_retirement_policy` | no | string |  |
| `unmatched_artifact_disposition` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52f1f9e36a3f8419274fd929261ca66a4b3c186ef485463299ad5d704efe1005 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "allow_derived_inputs": {
      "default": false,
      "title": "Allow Derived Inputs",
      "type": "boolean"
    },
    "artifact_associations": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ArtifactAssociation"
      },
      "title": "Artifact Associations",
      "type": "array"
    },
    "event_input_closure": {
      "const": "single-finalized-collection",
      "default": "single-finalized-collection",
      "title": "Event Input Closure",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "join": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RecipeJoin"
        },
        {
          "type": "null"
        }
      ]
    },
    "observers": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ObserverUse"
      },
      "title": "Observers",
      "type": "array"
    },
    "retirement_grace_seconds": {
      "default": 0,
      "minimum": 0,
      "title": "Retirement Grace Seconds",
      "type": "integer"
    },
    "revision": {
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "routes": {
      "items": {
        "discriminator": {
          "mapping": {
            "coordination": "#/components/schemas/RecipeCoordinationRoute",
            "operation": "#/components/schemas/RecipeRoute"
          },
          "propertyName": "kind"
        },
        "oneOf": [
          {
            "$ref": "#/components/schemas/RecipeRoute"
          },
          {
            "$ref": "#/components/schemas/RecipeCoordinationRoute"
          }
        ]
      },
      "minItems": 1,
      "title": "Routes",
      "type": "array"
    },
    "source_retirement_policy": {
      "default": "retain",
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Source Retirement Policy",
      "type": "string"
    },
    "unmatched_artifact_disposition": {
      "enum": [
        "retain-in-source",
        "reject-work"
      ],
      "title": "Unmatched Artifact Disposition",
      "type": "string"
    }
  },
  "required": [
    "id",
    "revision",
    "routes",
    "unmatched_artifact_disposition"
  ],
  "title": "RecipeDefinition",
  "type": "object"
}
```
