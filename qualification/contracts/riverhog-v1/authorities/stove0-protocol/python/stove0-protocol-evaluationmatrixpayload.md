# stove0_protocol.EvaluationMatrixPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationmatrixpayload:b3db29a0e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-589662a388"></a>
- <a id="s-91516b4bec"></a>`distribution`: `stove0-protocol`
- <a id="s-e5f5987ede"></a>`module`: `stove0_protocol`
- <a id="s-3b1c9676b7"></a>`name`: `EvaluationMatrixPayload`
- <a id="s-3cf1f5b531"></a>`unit`: `export`

### Declared structure

- <a id="s-6643c61491"></a>`kind`: `"class"`
- <a id="s-5ff191501f"></a>`signature`: `"\"(*, format: Literal['stove0-evaluation-matrix/v1'] = 'stove0-evaluation-matrix/v1', variants: Annotated[tuple[stove0_protocol.models.EvaluationVariant, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-6c92a201ba"></a>
- <a id="s-0193dc5767"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d9792edab"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1" |  |
| <a id="s-669002b757"></a>`variants` | yes | type="array"; minItems=1; items=(#/$defs/EvaluationVariant) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-bc8dc94e3b"></a>`EvaluationVariant` | type="object"; fields=`id`, `parameters`; additional keys=`additionalProperties`, `required` |
| <a id="s-09ae6204e9"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationMatrixPayload.canonical_variants](stove0-protocol-evaluationmatrixpayload-canonical-variants.md)

## Governing policies

- <a id="pa-0e2d6fc81f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationMatrixPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 71ae8db72c4fac4497ea043ab081c4b6a86fa16f8b97410c1cc0d9df47eb8bf7 -->

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
        "variants": {
          "items": {
            "$ref": "#/$defs/EvaluationVariant"
          },
          "minItems": 1,
          "type": "array"
        }
      },
      "required": [
        "variants"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-evaluation-matrix/v1'] = 'stove0-evaluation-matrix/v1', variants: Annotated[tuple[stove0_protocol.models.EvaluationVariant, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationMatrixPayload",
  "unit": "export"
}
```
