# schemas: RetrievalExpiredData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalexpireddata:c0025fa442 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d0c7f0f311"></a>

- <a id="s-3a775b41e7"></a>`type`: `"object"`
- <a id="s-97fd781ed5"></a>`additionalProperties`: `false`
- <a id="s-31a8c93fd5"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`
- <a id="s-bc8955f71d"></a>`title`: `"RetrievalExpiredData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f96d693e0b"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-9acb33c6eb"></a>`cause` | no | anyOf=[([RiverhogEventCause](schemas-riverhogeventcause.md)); (type="null")] |  |
| <a id="s-f05d979290"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; title="Collection Created At" |  |
| <a id="s-96ce8c50d7"></a>`collection_id` | no | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-8293ea4fc4"></a>`collection_ids` | yes | type="array"; items=([CollectionId](schemas-collectionid.md)); minItems=1; title="Collection Ids" |  |
| <a id="s-9f47ac3a10"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-16ccbf7672"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-d49f1927ea"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1; title="Retrieval Id" |  |
| <a id="s-658d75677c"></a>`state` | yes | type="string"; const="expired"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_ids](#s-8293ea4fc4) | `cardinality · items · operational_policy` | shared above |
| <a id="s-cbf4ce90a3"></a>[field context · object value](#s-9f47ac3a10) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-0bf4e263df"></a>[field collection_created_at · string value](#s-f05d979290) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field context · object value](#s-cbf4ce90a3) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field retrieval_id](#s-d49f1927ea) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)
- [RiverhogEventCause](schemas-riverhogeventcause.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-363decddf1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-23ec71df1e"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-de185c9bbc"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalExpiredData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8cd0d37b0fcf859b829d61151d6ae3c46be2c09e0bf959fe8a7a310adfdbff29 -->

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
      "const": "expired",
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
  "title": "RetrievalExpiredData",
  "type": "object"
}
```

</details>
