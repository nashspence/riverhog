# stove0_protocol.EvaluationBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationbinding:9927bd19a2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-85742ed1cf"></a>
- <a id="s-9624ade0c2"></a>`distribution`: `stove0-protocol`
- <a id="s-72e34ef8c2"></a>`module`: `stove0_protocol`
- <a id="s-a2b2f59211"></a>`name`: `EvaluationBinding`
- <a id="s-2865ef9298"></a>`unit`: `export`

### Declared structure

- <a id="s-66d0fe08ba"></a>`kind`: `"class"`
- <a id="s-ed645dcc66"></a>`signature`: `"\"(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], matrix_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], variant_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], parameters: dict[str, JsonValue] = <factory>) -> None\""`

#### Validated model schema

<a id="s-58bd82e15d"></a>
- <a id="s-a94adbbb93"></a>`title`: EvaluationBinding
- <a id="s-c4eb81c0b1"></a>`description`: Immutable membership of one work item in a trial/evaluation matrix.
- <a id="s-fcda911618"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0610837ba5"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-84cc98a5db"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-154234d6dc"></a>`parameters` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-669f145fcd"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-4aba2ae1bc"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-96028e2123"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 682781e16dbfdbb06a700b90e7cb0382a80b11aa671171d450bc9210da1c38b9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {}
      },
      "additionalProperties": false,
      "description": "Immutable membership of one work item in a trial/evaluation matrix.",
      "properties": {
        "evaluation_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Evaluation Id",
          "type": "string"
        },
        "matrix_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Matrix Sha256",
          "type": "string"
        },
        "parameters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Parameters",
          "type": "object"
        },
        "variant_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Variant Id",
          "type": "string"
        }
      },
      "required": [
        "evaluation_id",
        "matrix_sha256",
        "variant_id"
      ],
      "title": "EvaluationBinding",
      "type": "object"
    },
    "signature": "\"(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], matrix_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], variant_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], parameters: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationBinding",
  "unit": "export"
}
```
