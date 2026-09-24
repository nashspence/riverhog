# schemas: RetrievalCanceledData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalcanceleddata:a937964c7e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d35964f710"></a>

- <a id="s-6203c9109c"></a>`type`: `"object"`
- <a id="s-8121669e0e"></a>`additionalProperties`: `false`
- <a id="s-67ca09de25"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`
- <a id="s-5349fbb4fc"></a>`title`: `"RetrievalCanceledData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1d36db389"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-56195c4795"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Collection Created At" |  |
| <a id="s-ae0d16d36d"></a>`collection_id` | no | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-e28b23990c"></a>`collection_ids` | yes | type="array"; items=([CollectionId](schemas-collectionid.md)); minItems=1; title="Collection Ids" |  |
| <a id="s-4b8b5804e3"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-3a21fe5cb2"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-f3c0e65a18"></a>`reason` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; title="Reason" |  |
| <a id="s-c2baeac4bf"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1; title="Retrieval Id" |  |
| <a id="s-42b4331bfa"></a>`state` | yes | type="string"; const="canceled"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_ids](#s-e28b23990c) | `cardinality · items · operational_policy` | shared above |
| <a id="s-53754bec5a"></a>[field context · object value](#s-4b8b5804e3) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-1f47d38f50"></a>[field collection_created_at · string value](#s-56195c4795) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| [field context · object value](#s-53754bec5a) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| <a id="s-971db8fe81"></a>[field reason · string value](#s-f3c0e65a18) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [field retrieval_id](#s-c2baeac4bf) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-92cb774895"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-8af2a58ded"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-b199ef8429"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCanceledData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77b69a8eaf674d983025997d3fe00494fc544ec8c8ca39d77da08e8711288329 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actor": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "collection_created_at": {
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
    "reason": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Reason"
    },
    "retrieval_id": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Retrieval Id",
      "type": "string"
    },
    "state": {
      "const": "canceled",
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
  "title": "RetrievalCanceledData",
  "type": "object"
}
```

</details>
