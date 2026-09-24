# stove0_operator_contracts.OperatorWorkflowPreviewRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-operatorworkflo-3091dd2444:75c722726c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-44efad264b"></a>
- <a id="s-1e17bc1c76"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-b0b86a1af8"></a>`module`: `stove0_operator_contracts`
- <a id="s-475939fc7e"></a>`name`: `OperatorWorkflowPreviewRequest`
- <a id="s-943fd4b37d"></a>`unit`: `export`

### Declared structure

- <a id="s-0527160a3e"></a>`kind`: `"class"`
- <a id="s-00b5260b2a"></a>`signature`: `"'(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[NonnegativeDecimal \| None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>) -> None'"`

#### Validated model schema

<a id="s-9b8d033a22"></a>

- <a id="s-851112732e"></a>`type`: `"object"`
- <a id="s-0a3b87a295"></a>`additionalProperties`: `false`
- <a id="s-aa99b34289"></a>`required`: `["recipe_id","inputs"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5832f02aeb"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-8b2b6deb5f)) |  |
| <a id="s-0f4d67ec52"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-ff8622ab45)); minItems=1 |  |
| <a id="s-c323877f28"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-ba04a7f323"></a>`recipe_revision` | no | anyOf=[([NonnegativeDecimal](#s-10e0e11a40); ge=1); (type="null")]; default=null |  |

##### Definitions

- [CollectionId](#s-9021d6f15c)
- [CollectionRootIdentityRef](#s-ff8622ab45)
- [JsonValue](#s-8b2b6deb5f)
- [NonnegativeDecimal](#s-10e0e11a40)

##### <a id="s-9021d6f15c"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-a75a1aaf9e"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-57d6cc3789"></a>2 | not=(const="0") |

##### <a id="s-ff8622ab45"></a>definition `CollectionRootIdentityRef`

- <a id="s-a7c9c61c18"></a>`type`: `"object"`
- <a id="s-46f917d9df"></a>`additionalProperties`: `false`
- <a id="s-eb44a2d445"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3cfa779390"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1d9044a308"></a>`collection_id` | yes | [CollectionId](#s-9021d6f15c) |  |
| <a id="s-31c6ea277f"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8b2b6deb5f"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-10e0e11a40"></a>definition `NonnegativeDecimal`

- <a id="s-b3e0e52816"></a>`type`: `"string"`
- <a id="s-d637d18dbc"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-operator-contracts-operatorworkflowpreviewrequest-canonical-inputs.md)

## Governing policies

- <a id="pa-3575d742f2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.OperatorWorkflowPreviewRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e189ddd2658bb6d0b121ec2d6e90998f39d4284469bbf44d9f85cbb8a6b996ef -->

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
        "CollectionRootIdentityRef": {
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
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/CollectionRootIdentityRef"
          },
          "minItems": 1,
          "type": "array"
        },
        "recipe_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "recipe_revision": {
          "anyOf": [
            {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "recipe_id",
        "inputs"
      ],
      "type": "object"
    },
    "signature": "'(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[NonnegativeDecimal | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "OperatorWorkflowPreviewRequest",
  "unit": "export"
}
```

</details>
