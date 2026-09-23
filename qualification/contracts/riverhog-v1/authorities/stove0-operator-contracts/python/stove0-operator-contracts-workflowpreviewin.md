# stove0_operator_contracts.WorkflowPreviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workflowpreviewin:58e3423444 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09bdb29e6d"></a>
- <a id="s-76c97a5b30"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-9b585e3eaa"></a>`module`: `stove0_operator_contracts`
- <a id="s-8846457f5f"></a>`name`: `WorkflowPreviewIn`
- <a id="s-ad054de7e7"></a>`unit`: `export`

### Declared structure

- <a id="s-fdeee8e80a"></a>`kind`: `"class"`
- <a id="s-25f393fa2c"></a>`signature`: `"'(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int \| None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>) -> None'"`

#### Validated model schema

<a id="s-d04d5dab91"></a>

- <a id="s-3caace8cca"></a>`type`: `"object"`
- <a id="s-bc534b234b"></a>`additionalProperties`: `false`
- <a id="s-6d0f6aa916"></a>`required`: `["recipe_id","inputs"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2d9430f995"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-e10ebafa23)) |  |
| <a id="s-21bef1f98d"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-c90b432181)); minItems=1 |  |
| <a id="s-f25e9c76e6"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-0a6f3ccb15"></a>`recipe_revision` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |

##### Definitions

- [CollectionId](#s-01b0583462)
- [CollectionRootRef](#s-c90b432181)
- [JsonValue](#s-e10ebafa23)

##### <a id="s-01b0583462"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-ddbee53997"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-34f7587dda"></a>2 | not=(const="0") |

##### <a id="s-c90b432181"></a>definition `CollectionRootRef`

- <a id="s-37d5864f74"></a>`type`: `"object"`
- <a id="s-c87665492d"></a>`additionalProperties`: `false`
- <a id="s-e99a5b2fff"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-120dcc48e5"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bc0d1b495a"></a>`collection_id` | yes | [CollectionId](#s-01b0583462) |  |
| <a id="s-dd03cb5e45"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e10ebafa23"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-operator-contracts-workflowpreviewin-canonical-inputs.md)

## Governing policies

- <a id="pa-acc4cce580"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkflowPreviewIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff927c3a87434a68bd08b256b5f9eec2f61f3bd8082b45042610c323a8f2e9ad -->

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
        "CollectionRootRef": {
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
        "JsonValue": {}
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
            "$ref": "#/$defs/CollectionRootRef"
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
              "minimum": 1,
              "type": "integer"
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
    "signature": "'(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkflowPreviewIn",
  "unit": "export"
}
```

</details>
