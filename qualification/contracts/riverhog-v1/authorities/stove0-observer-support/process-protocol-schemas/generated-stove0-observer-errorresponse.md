# generated:stove0-observer: ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-errorresponse:7814a91d96 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-57baa8eb7b"></a>

- <a id="s-c28038b678"></a>`type`: `"object"`
- <a id="s-f78771d079"></a>`additionalProperties`: `false`
- <a id="s-a1f696077a"></a>`required`: `["error"]`
- <a id="s-cf3c53a66f"></a>`title`: `"ErrorResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee6b5deac3"></a>`error` | yes | [ErrorBody](#s-530f84c7d9) |  |

### Definitions

- [ErrorBody](#s-530f84c7d9)

### <a id="s-530f84c7d9"></a>definition `ErrorBody`

- <a id="s-86d2b74c5b"></a>`type`: `"object"`
- <a id="s-c80d53b1bb"></a>`additionalProperties`: `false`
- <a id="s-ee3d9f7631"></a>`required`: `["code","message"]`
- <a id="s-4eabc7c194"></a>`title`: `"ErrorBody"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e18148e63d"></a>`code` | yes | type="string"; minLength=1; title="Code" |  |
| <a id="s-f9c65c535b"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Details" |  |
| <a id="s-5269ade5c8"></a>`message` | yes | type="string"; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-53f088e50b"></a>[definition ErrorBody · field details · object value](#s-f9c65c535b) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-104a8ddb73"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-93bdcc0380"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ErrorResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
