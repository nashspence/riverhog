# schemas: RetrievalCacheObjectListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcacheobjectlistfiltersout:2fde567a9a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: RetrievalCacheObjectListFiltersOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `cache_store` | yes | anyOf=#/components/schemas/RetrievalCacheStoreName \| type="null" |  |
| `collection_id` | yes | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| `expires_after` | yes | anyOf=type="string" \| type="null" |  |
| `expires_before` | yes | anyOf=type="string" \| type="null" |  |
| `protection` | yes | anyOf=#/components/schemas/RetrievalCacheProtection \| type="null" |  |
| `source_store` | yes | anyOf=#/components/schemas/ArchiveStoreName \| type="null" |  |
| `state` | yes | anyOf=#/components/schemas/RetrievalCacheState \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RetrievalCacheProtection](schemas-retrievalcacheprotection.md)
- [schemas: RetrievalCacheState](schemas-retrievalcachestate.md)
- [schemas: RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheObjectListFiltersOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f3e6df8386f6a8c76b650fb20684a81bc78f1b82279e1d056b7a032345a3ae8 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "cache_store": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RetrievalCacheStoreName"
        },
        {
          "type": "null"
        }
      ]
    },
    "collection_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionId"
        },
        {
          "type": "null"
        }
      ]
    },
    "expires_after": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires After"
    },
    "expires_before": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires Before"
    },
    "protection": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RetrievalCacheProtection"
        },
        {
          "type": "null"
        }
      ]
    },
    "source_store": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveStoreName"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RetrievalCacheState"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "collection_id",
    "source_store",
    "cache_store",
    "state",
    "protection",
    "expires_before",
    "expires_after"
  ],
  "title": "RetrievalCacheObjectListFiltersOut",
  "type": "object"
}
```
