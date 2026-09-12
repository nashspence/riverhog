# schemas: CollectionDescriptionOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondescriptionout:46831cf4ef -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDescriptionOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionDescription](schemas-collectiondescription.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=0, reason=schema-maximum |

## Contract summary

- `title`: CollectionDescriptionOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `description` | yes | object (1 fields) |  |
| `description_identity` | yes | string |  |
| `description_publication` | yes | string |  |
| `description_revision` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a06a3e3817156e13f2bf603f5f26c314164e46195b2ea22f1121fc4bbc1ed24f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    },
    "description_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Description Identity",
      "type": "string"
    },
    "description_publication": {
      "enum": [
        "not_required",
        "current",
        "reconciling"
      ],
      "title": "Description Publication",
      "type": "string"
    },
    "description_revision": {
      "maximum": 9007199254740991,
      "minimum": 0,
      "title": "Description Revision",
      "type": "integer"
    }
  },
  "required": [
    "collection_id",
    "description",
    "description_revision",
    "description_identity",
    "description_publication"
  ],
  "title": "CollectionDescriptionOut",
  "type": "object"
}
```
