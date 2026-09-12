# schemas: RetrievalCacheObjectListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcacheobjectlistfiltersout:2fde567a9a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8d691b6be52e"></a>
- <a id="s-0c47a50a3764"></a>`title`: RetrievalCacheObjectListFiltersOut
- <a id="s-ad2805047f93"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd585de03206"></a>`cache_store` | yes | anyOf=#/components/schemas/RetrievalCacheStoreName \| type="null" |  |
| <a id="s-ff7436da972c"></a>`collection_id` | yes | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| <a id="s-1f1860910ad9"></a>`expires_after` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-3207f9d87d2e"></a>`expires_before` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-092fcd3fb7d2"></a>`protection` | yes | anyOf=#/components/schemas/RetrievalCacheProtection \| type="null" |  |
| <a id="s-b1c9c2dae1f4"></a>`source_store` | yes | anyOf=#/components/schemas/ArchiveStoreName \| type="null" |  |
| <a id="s-31829d68f2e6"></a>`state` | yes | anyOf=#/components/schemas/RetrievalCacheState \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RetrievalCacheProtection](schemas-retrievalcacheprotection.md)
- [schemas: RetrievalCacheState](schemas-retrievalcachestate.md)
- [schemas: RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

## Governing policies

- <a id="pa-4982ac25d8e0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
