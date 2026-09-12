# schemas: ReplaceCollectionDescriptionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-replacecollectiondescriptionrequest:ea324cf540 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ReplaceCollectionDescriptionRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `description` | yes | anyOf=#/components/schemas/CollectionDescription \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDescription](schemas-collectiondescription.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ReplaceCollectionDescriptionRequest`

### Exact owned JSON

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
