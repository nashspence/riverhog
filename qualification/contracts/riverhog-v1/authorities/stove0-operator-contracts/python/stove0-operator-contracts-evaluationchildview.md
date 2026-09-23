# stove0_operator_contracts.EvaluationChildView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationchildview:6b3914916b -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-e7f9c347b6"></a>`type`: `"object"`
- <a id="s-d7ed91346f"></a>`additionalProperties`: `false`
- <a id="s-3f096b5045"></a>`required`: `["variant_id","work_id","state"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bbb6bdbc91"></a>`output` | no | anyOf=[([OutputCollectionRef](#s-f5c2ca9269)); (type="null")]; default=null |  |
| <a id="s-99c18a12e1"></a>`state` | yes | type="string"; enum=["pending","active","complete","inapplicable","failed","canceled"] |  |
| <a id="s-f6ec53c14a"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-e92ac32514"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-89e22d4b9d)
- [OutputCollectionRef](#s-f5c2ca9269)

##### <a id="s-89e22d4b9d"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-0496f0f180"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-fc564f25fc"></a>2 | not=(const="0") |

##### <a id="s-f5c2ca9269"></a>definition `OutputCollectionRef`

- <a id="s-36193562ad"></a>`type`: `"object"`
- <a id="s-0c787038a9"></a>`additionalProperties`: `false`
- <a id="s-99615327f2"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fc8986c79c"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e4c8308218"></a>`collection_id` | yes | [CollectionId](#s-89e22d4b9d) |  |
| <a id="s-4c000aee11"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fdba2694db"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [exact_output](stove0-operator-contracts-evaluationchildview-exact-output.md)

## Governing policies

- <a id="pa-7bf47f15e8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationChildView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8eec51008ecb91f55d46dcf4c856a174c99d19d3f673717add79720013c3d999 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
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

</details>
