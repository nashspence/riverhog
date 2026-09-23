# generated:review0-sampler: ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:review0-sampler-lib:generated-review0-sampler-errorresponse:23706d9550 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-023c0d3183"></a>

- <a id="s-3d5584857c"></a>`type`: `"object"`
- <a id="s-81ce5e09db"></a>`additionalProperties`: `false`
- <a id="s-dbfc166d92"></a>`required`: `["error"]`
- <a id="s-c56c00f152"></a>`title`: `"ErrorResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-865f16765a"></a>`error` | yes | [ErrorBody](#s-b5fe7d4173) |  |

### Definitions

- [ErrorBody](#s-b5fe7d4173)

### <a id="s-b5fe7d4173"></a>definition `ErrorBody`

- <a id="s-9e527536c7"></a>`type`: `"object"`
- <a id="s-34ef4df7c6"></a>`additionalProperties`: `false`
- <a id="s-5816a965a6"></a>`required`: `["code","message"]`
- <a id="s-254d5f48d2"></a>`title`: `"ErrorBody"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-939a30eacd"></a>`code` | yes | type="string"; minLength=1; title="Code" |  |
| <a id="s-083d6c1c07"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Details" |  |
| <a id="s-30f0890adc"></a>`message` | yes | type="string"; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"review0-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-0b05c99a66"></a>[definition ErrorBody · field details · object value](#s-083d6c1c07) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-16fd7356b5"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-530dbfced0"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/schemas/ErrorResponse`

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
