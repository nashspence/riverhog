# stove0_operator_contracts.EvaluationChildView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationchildview:6b3914916b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-12b69a71fc"></a>
- <a id="s-c6ac2a566b"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-59c72a8c72"></a>`module`: `stove0_operator_contracts`
- <a id="s-1d7f238327"></a>`name`: `EvaluationChildView`
- <a id="s-6f1530b59b"></a>`unit`: `export`

### Declared structure

- <a id="s-eeadf60ad7"></a>`kind`: `"class"`
- <a id="s-f50d26ee62"></a>`signature`: `"\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['pending', 'active', 'complete', 'inapplicable', 'failed', 'canceled'], output: stove0_target_protocol.protocol.OutputCollectionRef \| None = None) -> None\""`

#### Validated model schema

<a id="s-ad894bceda"></a>
- <a id="s-e7f9c347b6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bbb6bdbc91"></a>`output` | no | anyOf=#/$defs/OutputCollectionRef \| type="null" |  |
| <a id="s-99c18a12e1"></a>`state` | yes | type="string"; enum=["pending","active","complete","inapplicable","failed","canceled"] |  |
| <a id="s-f6ec53c14a"></a>`variant_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-e92ac32514"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-89e22d4b9d"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-f5c2ca9269"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationChildView.exact_output](stove0-operator-contracts-evaluationchildview-exact-output.md)

## Governing policies

- <a id="pa-7bf47f15e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationChildView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 855f989d032be19465dbb468094bcd46253be7c05ef1758dd37c8af9a27a1377 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "OutputCollectionRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "output": {
          "anyOf": [
            {
              "$ref": "#/$defs/OutputCollectionRef"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "state": {
          "enum": [
            "pending",
            "active",
            "complete",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "type": "string"
        },
        "variant_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "variant_id",
        "work_id",
        "state"
      ],
      "type": "object"
    },
    "signature": "\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['pending', 'active', 'complete', 'inapplicable', 'failed', 'canceled'], output: stove0_target_protocol.protocol.OutputCollectionRef | None = None) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationChildView",
  "unit": "export"
}
```
