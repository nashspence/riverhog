# schemas: ReplaceCollectionDescriptionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-replacecollectiondescriptionrequest:ea324cf540 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ReplaceCollectionDescriptionRequest`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionDescription](schemas-collectiondescription.md)

## Contract summary

- `title`: ReplaceCollectionDescriptionRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `description` | yes | object (1 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4ad12a4df50fa39cb8315778965953000c4534f6e8a07749fb3d3fe84e2ad95 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "description"
  ],
  "title": "ReplaceCollectionDescriptionRequest",
  "type": "object"
}
```
