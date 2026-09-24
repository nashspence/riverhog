# schemas: ArchiveCopyJobRequestedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobrequesteddata:545a7e09d3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a2941ac9a5"></a>

- <a id="s-3f778413d8"></a>`type`: `"object"`
- <a id="s-7fd5bd40b8"></a>`additionalProperties`: `false`
- <a id="s-dd448bbdd2"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state"]`
- <a id="s-a595c87f21"></a>`title`: `"ArchiveCopyJobRequestedData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-40d32771b8"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-a427b21273"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Collection Created At" |  |
| <a id="s-2b9acde322"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-2600232578"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-86cf53a6a0"></a>`destination_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-1e9b500aee"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-5f4a574b21"></a>`source_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-8c5e4a443c"></a>`state` | yes | type="string"; const="requested"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-413230b577"></a>[field context · object value](#s-2600232578) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_created_at](#s-a427b21273) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| [field context · object value](#s-413230b577) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c0750f2ca6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-9a2fa6d741"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-f6a4a29593"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobRequestedData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91d9614fb5d19a993c77e87239036c063c4867e1ab8dbb95a708eb363b9ab596 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actor": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "collection_created_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Collection Created At",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "context": {
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
      "title": "Context"
    },
    "destination_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "initiator": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "source_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "state": {
      "const": "requested",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "actor",
    "initiator",
    "collection_id",
    "collection_created_at",
    "source_store",
    "destination_store",
    "state"
  ],
  "title": "ArchiveCopyJobRequestedData",
  "type": "object"
}
```

</details>
