# schemas: WorkCreateIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workcreatein:4aad27f9d6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: WorkCreateIn
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `effective_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| `inputs` | yes | type="array"; minItems=1; items=(#/components/schemas/CollectionRootRef) |  |
| `preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `recipe_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| `recipe_revision` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionRootRef](schemas-collectionrootref.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

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

- `/external_contract/http_openapi/stove0/components/schemas/WorkCreateIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 817563604e4e4841d32e612eadcd1d367bfb11d3b8004d487f777820ff9b893e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "effective_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Effective Intent",
      "type": "object"
    },
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/CollectionRootRef"
      },
      "minItems": 1,
      "title": "Inputs",
      "type": "array"
    },
    "preview_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Preview Sha256",
      "type": "string"
    },
    "recipe_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Recipe Id",
      "type": "string"
    },
    "recipe_revision": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Recipe Revision"
    }
  },
  "required": [
    "recipe_id",
    "inputs",
    "preview_sha256"
  ],
  "title": "WorkCreateIn",
  "type": "object"
}
```
