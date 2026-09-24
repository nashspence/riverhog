# schemas: RetrievalCacheObjectListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalcacheobjectlistfiltersout:98f6d09fd8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8d691b6be5"></a>

- <a id="s-ad2805047f"></a>`type`: `"object"`
- <a id="s-f9b9ece19f"></a>`additionalProperties`: `false`
- <a id="s-101127781c"></a>`required`: `["collection_id","source_store","cache_store","state","protection","expires_before","expires_after"]`
- <a id="s-0c47a50a37"></a>`title`: `"RetrievalCacheObjectListFiltersOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd585de032"></a>`cache_store` | yes | anyOf=[([RetrievalCacheStoreName](schemas-retrievalcachestorename.md)); (type="null")] |  |
| <a id="s-ff7436da97"></a>`collection_id` | yes | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-1f1860910a"></a>`expires_after` | yes | anyOf=[(type="string"); (type="null")]; title="Expires After" |  |
| <a id="s-3207f9d87d"></a>`expires_before` | yes | anyOf=[(type="string"); (type="null")]; title="Expires Before" |  |
| <a id="s-092fcd3fb7"></a>`protection` | yes | anyOf=[([RetrievalCacheProtection](schemas-retrievalcacheprotection.md)); (type="null")] |  |
| <a id="s-b1c9c2dae1"></a>`source_store` | yes | anyOf=[([ArchiveStoreName](schemas-archivestorename.md)); (type="null")] |  |
| <a id="s-31829d68f2"></a>`state` | yes | anyOf=[([RetrievalCacheState](schemas-retrievalcachestate.md)); (type="null")] |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)
- [RetrievalCacheProtection](schemas-retrievalcacheprotection.md)
- [RetrievalCacheState](schemas-retrievalcachestate.md)
- [RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

## Governing policies

- <a id="pa-69ed3a94b3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheObjectListFiltersOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
