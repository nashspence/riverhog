# schemas: RetrievalCompletedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalcompleteddata:37d28ae043 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-056ed2dfde"></a>

- <a id="s-120c6934ea"></a>`type`: `"object"`
- <a id="s-b7d514b026"></a>`additionalProperties`: `false`
- <a id="s-8cda709579"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`
- <a id="s-61ce9d0757"></a>`title`: `"RetrievalCompletedData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-75c41a2961"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-1cdc5b55de"></a>`cause` | no | anyOf=[([RiverhogEventCause](schemas-riverhogeventcause.md)); (type="null")] |  |
| <a id="s-a2fc38c4b7"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; title="Collection Created At" |  |
| <a id="s-c1f47b1801"></a>`collection_id` | no | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-68847b14ff"></a>`collection_ids` | yes | type="array"; items=([CollectionId](schemas-collectionid.md)); minItems=1; title="Collection Ids" |  |
| <a id="s-b7bbd451e9"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-49e730c378"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-faa54f4904"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1; title="Retrieval Id" |  |
| <a id="s-fc7a4df698"></a>`state` | yes | type="string"; const="completed"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_ids](#s-68847b14ff) | `cardinality · items · operational_policy` | shared above |
| <a id="s-890ad10e53"></a>[field context · object value](#s-b7bbd451e9) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-650fc1ef83"></a>[field collection_created_at · string value](#s-a2fc38c4b7) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field context · object value](#s-890ad10e53) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field retrieval_id](#s-faa54f4904) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)
- [RiverhogEventCause](schemas-riverhogeventcause.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-996b6926a6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-0f4ea2e846"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-85c5ed0a92"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCompletedData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83508e13c2e95546866cfb2f88ba2d2bbbf011f5d9b047bdc0f66fdc5c6fb395 -->

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
      "anyOf": [
        {
          "maxLength": 64,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Collection Created At"
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
    "collection_ids": {
      "items": {
        "$ref": "#/components/schemas/CollectionId"
      },
      "minItems": 1,
      "title": "Collection Ids",
      "type": "array"
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
    "initiator": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "retrieval_id": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Retrieval Id",
      "type": "string"
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
    "retrieval_id",
    "collection_ids",
    "state"
  ],
  "title": "RetrievalCompletedData",
  "type": "object"
}
```

</details>
