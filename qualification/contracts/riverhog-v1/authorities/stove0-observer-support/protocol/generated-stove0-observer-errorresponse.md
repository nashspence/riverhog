# generated:stove0-observer: ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-errorresponse:73f4e38e10 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ErrorResponse`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-observer` — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: ErrorResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | #/$defs/ErrorBody |  |

### Definitions

| Definition | Shape |
|---|---|
| `ErrorBody` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d60b687f27e4869742bff79f639b284dc47aeb1f716d9e75178e79b171ac4f9f -->

```json
{
  "$defs": {
    "ErrorBody": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "minLength": 1,
          "title": "Code",
          "type": "string"
        },
        "details": {
          "anyOf": [
            {
              "additionalProperties": true,
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Details"
        },
        "message": {
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "ErrorBody",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "error": {
      "$ref": "#/$defs/ErrorBody"
    }
  },
  "required": [
    "error"
  ],
  "title": "ErrorResponse",
  "type": "object"
}
```
