# schemas: ArchiveCopyJobCompletedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobcompleteddata:f1ff79f381 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e1b34645a6"></a>

- <a id="s-7ccedf6bda"></a>`type`: `"object"`
- <a id="s-9176ef63be"></a>`additionalProperties`: `false`
- <a id="s-2f231de417"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state"]`
- <a id="s-202f39eb75"></a>`title`: `"ArchiveCopyJobCompletedData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1823907230"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-13ccee7dad"></a>`cause` | no | anyOf=[([RiverhogEventCause](schemas-riverhogeventcause.md)); (type="null")] |  |
| <a id="s-81afc89fbd"></a>`collection_created_at` | yes | type="string"; maxLength=64; minLength=1; title="Collection Created At" |  |
| <a id="s-2c4e505af6"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-ba11bf12ce"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-2be8bae336"></a>`destination_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-0cb9e739c1"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-b3670b03ca"></a>`source_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-a77bac5549"></a>`state` | yes | type="string"; const="completed"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-3a4f104d01"></a>[field context · object value](#s-ba11bf12ce) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_created_at](#s-81afc89fbd) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field context · object value](#s-3a4f104d01) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)
- [RiverhogEventCause](schemas-riverhogeventcause.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-09ae03f3f1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-1dd857567b"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-c5410cfb7d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobCompletedData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a30e5abcc86f93fc6fd5b928f4eaa074200c6937a50812ecdc6dec7ba803891 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actor": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "cause": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RiverhogEventCause"
        },
        {
          "type": "null"
        }
      ]
    },
    "collection_created_at": {
      "maxLength": 64,
      "minLength": 1,
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
      "const": "completed",
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
  "title": "ArchiveCopyJobCompletedData",
  "type": "object"
}
```

</details>
