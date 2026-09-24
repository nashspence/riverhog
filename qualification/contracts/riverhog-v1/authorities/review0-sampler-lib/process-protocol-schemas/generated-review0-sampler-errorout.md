# generated:review0-sampler: ErrorOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:review0-sampler-lib:generated-review0-sampler-errorout:a126416ef6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-a555056f5b"></a>

- <a id="s-e5069af2b7"></a>`type`: `"object"`
- <a id="s-04f32d7715"></a>`additionalProperties`: `false`
- <a id="s-a2912ad6a7"></a>`required`: `["error"]`
- <a id="s-a0aa46bbd1"></a>`title`: `"ErrorOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a23ff6f85"></a>`error` | yes | [ErrorBody](#s-3b23122d2d) |  |

### Definitions

- [ErrorBody](#s-3b23122d2d)

### <a id="s-3b23122d2d"></a>definition `ErrorBody`

- <a id="s-aae69fcb07"></a>`type`: `"object"`
- <a id="s-5adfac8b4b"></a>`additionalProperties`: `false`
- <a id="s-1082d7928a"></a>`required`: `["code","message"]`
- <a id="s-d0733ecc10"></a>`title`: `"ErrorBody"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa2c8a5f38"></a>`code` | yes | type="string"; minLength=1; title="Code" |  |
| <a id="s-eb6f8e4af2"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Details" |  |
| <a id="s-b1f243d64c"></a>`message` | yes | type="string"; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"review0-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-5e76da9657"></a>[definition ErrorBody · field details · object value](#s-eb6f8e4af2) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5a6001b3fa"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-607402a776"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/schemas/ErrorOut`

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
