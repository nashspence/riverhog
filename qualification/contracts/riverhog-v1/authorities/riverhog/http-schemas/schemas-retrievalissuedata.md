# schemas: RetrievalIssueData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalissuedata:409b764339 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-0684cc4629"></a>

- <a id="s-2b50be487f"></a>`type`: `"object"`
- <a id="s-86bacd62da"></a>`additionalProperties`: `false`
- <a id="s-bbba5e5636"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","error"]`
- <a id="s-805afcc7b3"></a>`title`: `"RetrievalIssueData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e41e5e822e"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-cacf608ce3"></a>`cause` | no | anyOf=[([RiverhogEventCause](schemas-riverhogeventcause.md)); (type="null")] |  |
| <a id="s-4c4268ef6a"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; title="Collection Created At" |  |
| <a id="s-dc91964e2c"></a>`collection_id` | no | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-1ea7f25788"></a>`collection_ids` | yes | type="array"; items=([CollectionId](schemas-collectionid.md)); minItems=1; title="Collection Ids" |  |
| <a id="s-dd874d7287"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-99f1d4d657"></a>`error` | yes | type="string"; maxLength=16384; minLength=1; title="Error" |  |
| <a id="s-61cd3658eb"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-642cd7a6f7"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1; title="Retrieval Id" |  |
| <a id="s-f79b5c54a8"></a>`state` | yes | type="string"; const="requested"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_ids](#s-1ea7f25788) | `cardinality · items · operational_policy` | shared above |
| <a id="s-68023b2f77"></a>[field context · object value](#s-dd874d7287) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-7747973112"></a>[field collection_created_at · string value](#s-4c4268ef6a) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field context · object value](#s-68023b2f77) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field error](#s-99f1d4d657) | `length · characters · contract_max` | maximum=16384; minimum=1; reason="schema-maximum" |
| [field retrieval_id](#s-642cd7a6f7) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)
- [RiverhogEventCause](schemas-riverhogeventcause.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8dff421115"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-6b2071dadf"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-c2c873ed4f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalIssueData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e149aec6574de95dcad648d4f68006f0d27b1c3f5728be4e18857378b36852ed -->

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
    "error": {
      "maxLength": 16384,
      "minLength": 1,
      "title": "Error",
      "type": "string"
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
      "const": "requested",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "actor",
    "initiator",
    "retrieval_id",
    "collection_ids",
    "state",
    "error"
  ],
  "title": "RetrievalIssueData",
  "type": "object"
}
```

</details>
