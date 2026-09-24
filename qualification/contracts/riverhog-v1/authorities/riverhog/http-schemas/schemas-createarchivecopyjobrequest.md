# schemas: CreateArchiveCopyJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-createarchivecopyjobrequest:8f04344924 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7ae743a845"></a>

- <a id="s-12ac38e78d"></a>`type`: `"object"`
- <a id="s-091ba4bd2f"></a>`additionalProperties`: `false`
- <a id="s-5ef8cdcf94"></a>`required`: `["destination_store","collection_id"]`
- <a id="s-622aa99548"></a>`title`: `"CreateArchiveCopyJobRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-270ecb93fe"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-cdc17a3c6e"></a>`destination_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-0e74bed2bc"></a>`event_context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Event Context" |  |
| <a id="s-2c0e67bd72"></a>`source_store` | no | anyOf=[([ArchiveStoreName](schemas-archivestorename.md)); (type="null")] |  |
| <a id="s-608c284501"></a>`use_cache` | no | anyOf=[(type="boolean"); (type="null")]; title="Use Cache" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b8baef75cb"></a>[field event_context · object value](#s-0e74bed2bc) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field event_context · object value](#s-b8baef75cb) | `encoded-size · bytes · contract_max` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f2776fbb9a"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e105a5b9d0"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-62a8d04432"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateArchiveCopyJobRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 984a2a87873a949b5560b856ea218d5dcf2b55a394ac7f7392c6db2a0445ca24 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "destination_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "event_context": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4096,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-lifecycle-event-context"
          }
        },
        {
          "type": "null"
        }
      ],
      "title": "Event Context"
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
    "use_cache": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "title": "Use Cache"
    }
  },
  "required": [
    "destination_store",
    "collection_id"
  ],
  "title": "CreateArchiveCopyJobRequest",
  "type": "object"
}
```

</details>
