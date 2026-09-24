# schemas: RetrievalCacheObjectOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalcacheobjectout:bbdaf1ff4c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d8de960a53"></a>

- <a id="s-023b459ad5"></a>`type`: `"object"`
- <a id="s-c0632852e7"></a>`additionalProperties`: `false`
- <a id="s-26250218b5"></a>`required`: `["collection_id","source_store","cache_store","object_id","state","stored_bytes","stored_sha256","cached_at","verified_at","protected_until","new_archive_expires_at","lease_categories","retrieval_job_leases"]`
- <a id="s-71b5428fd5"></a>`title`: `"RetrievalCacheObjectOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06cc3eb427"></a>`cache_store` | yes | [RetrievalCacheStoreName](schemas-retrievalcachestorename.md) |  |
| <a id="s-3b10f667fb"></a>`cached_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Cached At" |  |
| <a id="s-d3807612bf"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-627408e577"></a>`lease_categories` | yes | type="array"; items=(type="string"; enum=["new_archive","retrieval_job"]); title="Lease Categories" |  |
| <a id="s-5e4b6f164b"></a>`new_archive_expires_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="New Archive Expires At" |  |
| <a id="s-51bc3513f0"></a>`object_id` | yes | type="string"; title="Object Id" |  |
| <a id="s-d92ae72187"></a>`protected_until` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Protected Until" |  |
| <a id="s-47dbf3c6e5"></a>`retrieval_job_leases` | yes | type="integer"; title="Retrieval Job Leases" |  |
| <a id="s-ae87dd0d79"></a>`source_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-cbb7f490d3"></a>`state` | yes | [RetrievalCacheState](schemas-retrievalcachestate.md) |  |
| <a id="s-c95d53b4cf"></a>`stored_bytes` | yes | type="integer"; title="Stored Bytes" |  |
| <a id="s-a582abba72"></a>`stored_sha256` | yes | anyOf=[(type="string"); (type="null")]; title="Stored Sha256" |  |
| <a id="s-e2697d058e"></a>`verified_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Verified At" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field lease_categories](#s-627408e577) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field cached_at](#s-3b10f667fb) | `length · characters · fixed` | shared above |
| <a id="s-f70bae8d6e"></a>[field new_archive_expires_at · string value](#s-5e4b6f164b) | `length · characters · fixed` | shared above |
| <a id="s-b13fd5823a"></a>[field protected_until · string value](#s-d92ae72187) | `length · characters · fixed` | shared above |
| [field verified_at](#s-e2697d058e) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)
- [RetrievalCacheState](schemas-retrievalcachestate.md)
- [RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-321017a5d6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-576eaf962c"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-711abdc1bd"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheObjectOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad8287735699fea61c7d6101884711c82c2b9080df4f010cd938bfc7ac021715 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "cache_store": {
      "$ref": "#/components/schemas/RetrievalCacheStoreName"
    },
    "cached_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Cached At",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "lease_categories": {
      "items": {
        "enum": [
          "new_archive",
          "retrieval_job"
        ],
        "type": "string"
      },
      "title": "Lease Categories",
      "type": "array"
    },
    "new_archive_expires_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "New Archive Expires At"
    },
    "object_id": {
      "title": "Object Id",
      "type": "string"
    },
    "protected_until": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Protected Until"
    },
    "retrieval_job_leases": {
      "title": "Retrieval Job Leases",
      "type": "integer"
    },
    "source_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "state": {
      "$ref": "#/components/schemas/RetrievalCacheState"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stored_sha256": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Stored Sha256"
    },
    "verified_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Verified At",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "source_store",
    "cache_store",
    "object_id",
    "state",
    "stored_bytes",
    "stored_sha256",
    "cached_at",
    "verified_at",
    "protected_until",
    "new_archive_expires_at",
    "lease_categories",
    "retrieval_job_leases"
  ],
  "title": "RetrievalCacheObjectOut",
  "type": "object"
}
```

</details>
