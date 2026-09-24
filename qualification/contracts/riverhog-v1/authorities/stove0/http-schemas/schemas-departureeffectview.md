# schemas: DepartureEffectView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-departureeffectview:1ac9725290 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-70df5f54bf"></a>

- <a id="s-8afaacffc3"></a>`type`: `"object"`
- <a id="s-1d233d7204"></a>`additionalProperties`: `false`
- <a id="s-2fa0eea5cc"></a>`required`: `["intent","state","attempt_count","created_at","updated_at"]`
- <a id="s-53602e6cbc"></a>`title`: `"DepartureEffectView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1bccd6ad16"></a>`attempt_count` | yes | type="integer"; minimum=0; title="Attempt Count" |  |
| <a id="s-03e1827b39"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Created At" |  |
| <a id="s-dcceff3c41"></a>`failure` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; title="Failure" |  |
| <a id="s-aa911a930d"></a>`intent` | yes | [DepartureEffectIntent](schemas-departureeffectintent.md) |  |
| <a id="s-90b74d5d85"></a>`next_attempt_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Next Attempt At" |  |
| <a id="s-e4df566740"></a>`receipt` | no | anyOf=[([DepartureEffectReceipt](schemas-departureeffectreceipt.md)); (type="null")] |  |
| <a id="s-443f06cb96"></a>`state` | yes | type="string"; enum=["pending","complete"]; title="State" |  |
| <a id="s-5beb275c62"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Updated At" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field created_at](#s-03e1827b39) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| <a id="s-8431bc068e"></a>[field failure · string value](#s-dcceff3c41) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-38cfb7bb57"></a>[field next_attempt_at · string value](#s-90b74d5d85) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| [field updated_at](#s-5beb275c62) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |

## Maintained corroboration

### Referenced contract elements

- [DepartureEffectIntent](schemas-departureeffectintent.md)
- [DepartureEffectReceipt](schemas-departureeffectreceipt.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d47f8871c8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d5ff38bd56"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/DepartureEffectView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 500712028a16624f23a8ee165a6399ae26d5edbee278d4bff2bd13ae038264a7 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "attempt_count": {
      "minimum": 0,
      "title": "Attempt Count",
      "type": "integer"
    },
    "created_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Created At",
      "type": "string"
    },
    "failure": {
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
      "title": "Failure"
    },
    "intent": {
      "$ref": "#/components/schemas/DepartureEffectIntent"
    },
    "next_attempt_at": {
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
      "title": "Next Attempt At"
    },
    "receipt": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/DepartureEffectReceipt"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "enum": [
        "pending",
        "complete"
      ],
      "title": "State",
      "type": "string"
    },
    "updated_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Updated At",
      "type": "string"
    }
  },
  "required": [
    "intent",
    "state",
    "attempt_count",
    "created_at",
    "updated_at"
  ],
  "title": "DepartureEffectView",
  "type": "object"
}
```

</details>
