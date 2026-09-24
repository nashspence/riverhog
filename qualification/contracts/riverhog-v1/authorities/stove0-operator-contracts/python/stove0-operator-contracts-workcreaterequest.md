# stove0_operator_contracts.WorkCreateRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreaterequest:3308bfa9a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2222f02b9a"></a>
- <a id="s-e0e8322f06"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-355116c74d"></a>`module`: `stove0_operator_contracts`
- <a id="s-4b317550e8"></a>`name`: `WorkCreateRequest`
- <a id="s-47df5d7ed3"></a>`unit`: `export`

### Declared structure

- <a id="s-befb126761"></a>`kind`: `"class"`
- <a id="s-9c623b29ed"></a>`signature`: `"\"(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int \| None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-c541982774"></a>

- <a id="s-95cfd53484"></a>`type`: `"object"`
- <a id="s-50f44e3ba3"></a>`additionalProperties`: `false`
- <a id="s-c098f79276"></a>`required`: `["recipe_id","inputs","preview_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bfdc2d00b6"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-a160bed7f2)) |  |
| <a id="s-29eb01bd0b"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-4b4b718d1a)); minItems=1 |  |
| <a id="s-8e2c94cf36"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b201d2bf64"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-a53f2401a3"></a>`recipe_revision` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |

##### Definitions

- [CollectionId](#s-3aeef0d217)
- [CollectionRootIdentityRef](#s-4b4b718d1a)
- [JsonValue](#s-a160bed7f2)

##### <a id="s-3aeef0d217"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-726f8d8d8b"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-8ca66badd8"></a>2 | not=(const="0") |

##### <a id="s-4b4b718d1a"></a>definition `CollectionRootIdentityRef`

- <a id="s-28674c472d"></a>`type`: `"object"`
- <a id="s-8a86b606e7"></a>`additionalProperties`: `false`
- <a id="s-76fd0dc38d"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cfd3876d7a"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6ef9417fbd"></a>`collection_id` | yes | [CollectionId](#s-3aeef0d217) |  |
| <a id="s-6cea428c58"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a160bed7f2"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-operator-contracts-workcreaterequest-canonical-inputs.md)

## Governing policies

- <a id="pa-14ec24428e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreateRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f24a2bc0b08974320e57f919e0cbada2380825088d3e53fd65207fce45ed62b -->

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
            "$ref": "#/$defs/CollectionRootIdentityRef"
          },
          "minItems": 1,
          "type": "array"
        },
        "preview_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
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
        "inputs",
        "preview_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkCreateRequest",
  "unit": "export"
}
```

</details>
