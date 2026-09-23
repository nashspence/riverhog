# generated:stove0-review-sampler: ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-review-sampler-support:generated-stove0-review-sampler-errorresponse:c107224d1d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-76159d0953"></a>

- <a id="s-6004480872"></a>`type`: `"object"`
- <a id="s-0d95bb49ef"></a>`additionalProperties`: `false`
- <a id="s-b1a24e92a2"></a>`required`: `["error"]`
- <a id="s-cd9e354db7"></a>`title`: `"ErrorResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-701af705dd"></a>`error` | yes | [ErrorBody](#s-377a402816) |  |

### Definitions

- [ErrorBody](#s-377a402816)

### <a id="s-377a402816"></a>definition `ErrorBody`

- <a id="s-571e301812"></a>`type`: `"object"`
- <a id="s-6385508fe6"></a>`additionalProperties`: `false`
- <a id="s-e1a984f261"></a>`required`: `["code","message"]`
- <a id="s-131011a406"></a>`title`: `"ErrorBody"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff1f25a08f"></a>`code` | yes | type="string"; minLength=1; title="Code" |  |
| <a id="s-0d9bec331e"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Details" |  |
| <a id="s-27efa2f024"></a>`message` | yes | type="string"; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-review-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b96c4de4f5"></a>[definition ErrorBody · field details · object value](#s-0d9bec331e) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [generated:stove0-review-sampler protocol](../process-protocol/generated-stove0-review-sampler-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-852105bc9d"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-a7f9f031c3"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-review-sampler](../../../evidence/sources/authorities.md#src-b47f3f4d7b) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/schemas.py::sampler\_schema\_bundle](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/ErrorResponse`

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
