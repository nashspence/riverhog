# stove0_protocol.EvaluationMatrix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationmatrix:54ed43c4cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2dcd6baa30"></a>
- <a id="s-896760fd5b"></a>`distribution`: `stove0-protocol`
- <a id="s-ae4c33ab8f"></a>`module`: `stove0_protocol`
- <a id="s-064a110e4f"></a>`name`: `EvaluationMatrix`
- <a id="s-2d539792ce"></a>`unit`: `export`

### Declared structure

- <a id="s-217ad60626"></a>`kind`: `"class"`
- <a id="s-e93ec15483"></a>`signature`: `"\"(*, format: Literal['stove0-evaluation-matrix/v1'] = 'stove0-evaluation-matrix/v1', variants: Annotated[tuple[stove0_protocol.models.EvaluationVariant, ...], MinLen(min_length=1)], matrix_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-12db0e5e34"></a>
- <a id="s-33e6ff0eac"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ef0e66d82d"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1" |  |
| <a id="s-faffe2f543"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c45debe1cd"></a>`variants` | yes | type="array"; minItems=1; items=(#/$defs/EvaluationVariant) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-690953775a"></a>`EvaluationVariant` | type="object"; fields=`id`, `parameters`; additional keys=`additionalProperties`, `required` |
| <a id="s-61da80a26e"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationMatrix.seal](stove0-protocol-evaluationmatrix-seal.md)
- [stove0_protocol.EvaluationMatrix.verify_digest](stove0-protocol-evaluationmatrix-verify-digest.md)

## Governing policies

- <a id="pa-7eb7f85086"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationMatrix`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 356f113eee6ca0d6955b97c96b83f8b26cc7b83da7d1752fbedf8b8d52d411d0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "EvaluationVariant": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "parameters": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id"
          ],
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-evaluation-matrix/v1",
          "default": "stove0-evaluation-matrix/v1",
          "type": "string"
        },
        "matrix_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "variants": {
          "items": {
            "$ref": "#/$defs/EvaluationVariant"
          },
          "minItems": 1,
          "type": "array"
        }
      },
      "required": [
        "variants",
        "matrix_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-evaluation-matrix/v1'] = 'stove0-evaluation-matrix/v1', variants: Annotated[tuple[stove0_protocol.models.EvaluationVariant, ...], MinLen(min_length=1)], matrix_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationMatrix",
  "unit": "export"
}
```
