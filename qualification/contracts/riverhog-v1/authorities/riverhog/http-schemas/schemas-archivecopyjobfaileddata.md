# schemas: ArchiveCopyJobFailedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobfaileddata:779163cfde -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ae0c5813b8"></a>

- <a id="s-ef47017d99"></a>`type`: `"object"`
- <a id="s-a38097dd81"></a>`additionalProperties`: `false`
- <a id="s-56b533319d"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state","error"]`
- <a id="s-320f5a34da"></a>`title`: `"ArchiveCopyJobFailedData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f0735553c"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-c01d15c6cd"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Collection Created At" |  |
| <a id="s-51648b5899"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-10a6ce6f03"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-fcd12180cc"></a>`destination_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-2ae141479b"></a>`error` | yes | type="string"; maxLength=16384; minLength=1; title="Error" |  |
| <a id="s-86b38c3ba8"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-9f9be2516d"></a>`source_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-edfbf57773"></a>`state` | yes | type="string"; const="failed"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-025e757b39"></a>[field context · object value](#s-10a6ce6f03) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_created_at](#s-c01d15c6cd) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| [field context · object value](#s-025e757b39) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field error](#s-2ae141479b) | `length · characters · contract_max` | maximum=16384; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f7fe317cf8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-1cadeb9bd1"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-abb67d5027"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobFailedData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4979ad11e30580ba3915070de4067e1e32f0d0c78fcf281cd2b623fe5d0ce36b -->

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
    "error": {
      "maxLength": 16384,
      "minLength": 1,
      "title": "Error",
      "type": "string"
    },
    "initiator": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "source_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "state": {
      "const": "failed",
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
    "state",
    "error"
  ],
  "title": "ArchiveCopyJobFailedData",
  "type": "object"
}
```

</details>
