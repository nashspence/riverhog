# schemas: ArchiveCopyIssueData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyissuedata:1d8baeebd5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e00f0b8061"></a>

- <a id="s-02233c626f"></a>`type`: `"object"`
- <a id="s-c4afa683ea"></a>`additionalProperties`: `false`
- <a id="s-0ab3594853"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state","error"]`
- <a id="s-7552c1f7c8"></a>`title`: `"ArchiveCopyIssueData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-80e24aea6c"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-b1d777e884"></a>`cause` | no | anyOf=[([RiverhogEventCause](schemas-riverhogeventcause.md)); (type="null")] |  |
| <a id="s-7338b132ba"></a>`collection_created_at` | yes | type="string"; maxLength=64; minLength=1; title="Collection Created At" |  |
| <a id="s-78259ef48a"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-3c8c6d69f1"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-ccd81c2137"></a>`destination_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-cef168bba0"></a>`error` | yes | type="string"; maxLength=16384; minLength=1; title="Error" |  |
| <a id="s-2528ad92b8"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-a9bafa55f3"></a>`source_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-ea536eae28"></a>`state` | yes | type="string"; const="failed"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-541352f862"></a>[field context · object value](#s-3c8c6d69f1) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_created_at](#s-7338b132ba) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field context · object value](#s-541352f862) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field error](#s-cef168bba0) | `length · characters · contract_max` | maximum=16384; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)
- [RiverhogEventCause](schemas-riverhogeventcause.md)

## Governing policies

- <a id="pa-7f8ade17dd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-0bac2b31a2"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-d6637af304"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyIssueData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3eb788c6be906fcaed65861b06c1464cd4eecf99f7eaeace7f9983cb781e7ead -->

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
  "title": "ArchiveCopyIssueData",
  "type": "object"
}
```

</details>
