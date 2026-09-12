# schemas: RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipedefinition:31a48bb826 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: RecipeDefinition
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allow_derived_inputs` | no | type="boolean" |  |
| `artifact_associations` | no | type="array"; items=(#/components/schemas/ArtifactAssociation) |  |
| `event_input_closure` | no | type="string"; const="single-finalized-collection" |  |
| `id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| `join` | no | anyOf=#/components/schemas/RecipeJoin \| type="null" |  |
| `observers` | no | type="array"; items=(#/components/schemas/ObserverUse) |  |
| `retirement_grace_seconds` | no | type="integer"; minimum=0 |  |
| `revision` | yes | type="integer"; minimum=1 |  |
| `routes` | yes | type="array"; minItems=1; items=(oneOf=#/components/schemas/RecipeRoute \| #/components/schemas/RecipeCoordinationRoute; additional keys=`discriminator`) |  |
| `source_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"] |  |
| `unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactAssociation](schemas-artifactassociation.md)
- [schemas: ObserverUse](schemas-observeruse.md)
- [schemas: RecipeCoordinationRoute](schemas-recipecoordinationroute.md)
- [schemas: RecipeJoin](schemas-recipejoin.md)
- [schemas: RecipeRoute](schemas-reciperoute.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeDefinition`

### Exact owned JSON

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
