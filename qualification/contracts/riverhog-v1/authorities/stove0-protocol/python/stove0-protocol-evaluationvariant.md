# stove0_protocol.EvaluationVariant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationvariant:4a291e1f56 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d01ffa96c6"></a>
- <a id="s-d4f0a6951d"></a>`distribution`: `stove0-protocol`
- <a id="s-a80fc059ed"></a>`module`: `stove0_protocol`
- <a id="s-a49c748fad"></a>`name`: `EvaluationVariant`
- <a id="s-7c85c8f655"></a>`unit`: `export`

### Declared structure

- <a id="s-46f52238b8"></a>`kind`: `"class"`
- <a id="s-741d8a8b1e"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], parameters: dict[str, JsonValue] = <factory>) -> None\""`

#### Validated model schema

<a id="s-2c6445ace7"></a>
- <a id="s-8cd0245eaf"></a>`title`: EvaluationVariant
- <a id="s-d9d597beb4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a14155f81e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-024560daf8"></a>`parameters` | no | type="object"; additional keys=`additionalProperties` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-55aed24ea5"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-2fda50e075"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationVariant`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 731626169128da20eb009f5fc25041e4d01a489891cd1092f59d412b34590427 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "parameters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Parameters",
          "type": "object"
        }
      },
      "required": [
        "id"
      ],
      "title": "EvaluationVariant",
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], parameters: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationVariant",
  "unit": "export"
}
```
