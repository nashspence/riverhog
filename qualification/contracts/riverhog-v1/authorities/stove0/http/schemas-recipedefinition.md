# schemas: RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipedefinition:31a48bb826 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-5d4f1f1b9236"></a>
- <a id="s-dab076606723"></a>`title`: RecipeDefinition
- <a id="s-43a28d388ec0"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f105baa7b88e"></a>`allow_derived_inputs` | no | type="boolean" |  |
| <a id="s-d591b26ad651"></a>`artifact_associations` | no | type="array"; items=(#/components/schemas/ArtifactAssociation) |  |
| <a id="s-57bdc94a6383"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection" |  |
| <a id="s-f2fcf3466b80"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2211dbf2b8a3"></a>`join` | no | anyOf=#/components/schemas/RecipeJoin \| type="null" |  |
| <a id="s-095b4c1e00f7"></a>`observers` | no | type="array"; items=(#/components/schemas/ObserverUse) |  |
| <a id="s-ef8d6c456ff4"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0 |  |
| <a id="s-8ae96a90252c"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-823b3b9b9f51"></a>`routes` | yes | type="array"; minItems=1; items=(oneOf=#/components/schemas/RecipeRoute \| #/components/schemas/RecipeCoordinationRoute; additional keys=`discriminator`) |  |
| <a id="s-f280110403d7"></a>`source_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"] |  |
| <a id="s-28b7accac5e7"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_associations](#s-d591b26ad651) | `cardinality · items · operational_policy` | shared above |
| [field observers](#s-095b4c1e00f7) | `cardinality · items · operational_policy` | shared above |
| [field routes](#s-823b3b9b9f51) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactAssociation](schemas-artifactassociation.md)
- [schemas: ObserverUse](schemas-observeruse.md)
- [schemas: RecipeCoordinationRoute](schemas-recipecoordinationroute.md)
- [schemas: RecipeJoin](schemas-recipejoin.md)
- [schemas: RecipeRoute](schemas-reciperoute.md)

## Governing policies

- <a id="pa-3613b270a2d5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-46db70b7970d"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
