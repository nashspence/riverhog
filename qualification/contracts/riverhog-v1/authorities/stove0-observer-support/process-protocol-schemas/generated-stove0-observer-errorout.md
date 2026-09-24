# generated:stove0-observer: ErrorOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-errorout:960e5c4358 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-1a23fd0f01"></a>

- <a id="s-69845c0276"></a>`type`: `"object"`
- <a id="s-bb7f179d95"></a>`additionalProperties`: `false`
- <a id="s-123194fdcd"></a>`required`: `["error"]`
- <a id="s-742e706c0c"></a>`title`: `"ErrorOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f869f8e929"></a>`error` | yes | [ErrorBody](#s-f2c532fe2c) |  |

### Definitions

- [ErrorBody](#s-f2c532fe2c)

### <a id="s-f2c532fe2c"></a>definition `ErrorBody`

- <a id="s-4a685993e7"></a>`type`: `"object"`
- <a id="s-b064c7371d"></a>`additionalProperties`: `false`
- <a id="s-a9aa02a69e"></a>`required`: `["code","message"]`
- <a id="s-53d4103ad9"></a>`title`: `"ErrorBody"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d9bfdaa754"></a>`code` | yes | type="string"; minLength=1; title="Code" |  |
| <a id="s-3c5f354dc1"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Details" |  |
| <a id="s-ea549879f3"></a>`message` | yes | type="string"; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-221ce1b1db"></a>[definition ErrorBody · field details · object value](#s-3c5f354dc1) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9044fefe31"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-12cc341052"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ErrorOut`

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
