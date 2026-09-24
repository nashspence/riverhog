# generated:stove0-target: ErrorOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-errorout:d98602f47c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-d7a07fe0a7"></a>

- <a id="s-b829673ece"></a>`type`: `"object"`
- <a id="s-3fd1c36f4e"></a>`additionalProperties`: `false`
- <a id="s-55122a6d09"></a>`required`: `["error"]`
- <a id="s-0809d3f8fd"></a>`title`: `"ErrorOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c843986f45"></a>`error` | yes | [ErrorBody](#s-1b3ccdee68) |  |

### Definitions

- [ErrorBody](#s-1b3ccdee68)

### <a id="s-1b3ccdee68"></a>definition `ErrorBody`

- <a id="s-03810a7d46"></a>`type`: `"object"`
- <a id="s-cd6f84baf2"></a>`additionalProperties`: `false`
- <a id="s-ce047f3dd3"></a>`required`: `["code","message"]`
- <a id="s-4bf1ac2b63"></a>`title`: `"ErrorBody"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d28232ace9"></a>`code` | yes | type="string"; minLength=1; title="Code" |  |
| <a id="s-f45a116743"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Details" |  |
| <a id="s-4e3e06e040"></a>`message` | yes | type="string"; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-3f8c710153"></a>[definition ErrorBody · field details · object value](#s-f45a116743) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c134bf2c04"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-f10d8711ef"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/ErrorOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 166dc8c939b1c382be2a92b90c237c8589c2722950cb01db722bf22e02fe4ba8 -->

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
  "title": "ErrorOut",
  "type": "object"
}
```

</details>
