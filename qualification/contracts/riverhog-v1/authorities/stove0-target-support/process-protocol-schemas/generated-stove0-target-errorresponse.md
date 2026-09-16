# generated:stove0-target: ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-errorresponse:48f54b580d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-20be550bc6"></a>

- <a id="s-7170a92276"></a>`type`: `"object"`
- <a id="s-333c8de098"></a>`additionalProperties`: `false`
- <a id="s-130387ad17"></a>`required`: `["error"]`
- <a id="s-c2b1bb33ba"></a>`title`: `"ErrorResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f8b77e343a"></a>`error` | yes | [ErrorBody](#s-adaa4a4dc6) |  |

### Definitions

- [ErrorBody](#s-adaa4a4dc6)

### <a id="s-adaa4a4dc6"></a>definition `ErrorBody`

- <a id="s-6532af578c"></a>`type`: `"object"`
- <a id="s-cab6740549"></a>`additionalProperties`: `false`
- <a id="s-036de947c5"></a>`required`: `["code","message"]`
- <a id="s-a3c73b8821"></a>`title`: `"ErrorBody"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ad82bc8d85"></a>`code` | yes | type="string"; minLength=1 |  |
| <a id="s-d09ecb0659"></a>`details` | no | anyOf=(type="object"; additionalProperties=true) \| (type="null"); default=null |  |
| <a id="s-6907d119c4"></a>`message` | yes | type="string"; minLength=1 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-75f86baa89"></a>[definition ErrorBody · field details · object value](#s-d09ecb0659) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

- <a id="pa-dca134936c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-601f5a477a"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-target](../../../evidence/sources.md#src-2c42f9d39a) — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/ErrorResponse`

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
