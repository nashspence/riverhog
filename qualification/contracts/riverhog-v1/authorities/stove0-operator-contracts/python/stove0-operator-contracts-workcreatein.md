# stove0_operator_contracts.WorkCreateIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatein:346ce08e5c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-19eca1b23f"></a>
- <a id="s-d020902aca"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-8b38ce800c"></a>`module`: `stove0_operator_contracts`
- <a id="s-2ac58b77aa"></a>`name`: `WorkCreateIn`
- <a id="s-7e7064a029"></a>`unit`: `export`

### Declared structure

- <a id="s-0b1da7d09b"></a>`kind`: `"class"`
- <a id="s-c5b1033def"></a>`signature`: `"\"(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int \| None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-b63bbb84b7"></a>

- <a id="s-6195678c95"></a>`type`: `"object"`
- <a id="s-8d69edcb68"></a>`additionalProperties`: `false`
- <a id="s-47255e4e7e"></a>`required`: `["recipe_id","inputs","preview_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd2307aa0d"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-ed02433ff9)) |  |
| <a id="s-4c95719f45"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-efa4ba92d2)); minItems=1 |  |
| <a id="s-eaf26ba496"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-268a9b87b2"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-5e47a00c0f"></a>`recipe_revision` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |

##### Definitions

- [CollectionId](#s-f6865d067f)
- [CollectionRootRef](#s-efa4ba92d2)
- [JsonValue](#s-ed02433ff9)

##### <a id="s-f6865d067f"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-b66a0a36df"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-526b5eb604"></a>2 | not=(const="0") |

##### <a id="s-efa4ba92d2"></a>definition `CollectionRootRef`

- <a id="s-874e751507"></a>`type`: `"object"`
- <a id="s-4bb31fe09e"></a>`additionalProperties`: `false`
- <a id="s-9f718dde2f"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ac92556c0"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-722779ef5a"></a>`collection_id` | yes | [CollectionId](#s-f6865d067f) |  |
| <a id="s-6c6bcb63d3"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ed02433ff9"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-operator-contracts-workcreatein-canonical-inputs.md)

## Governing policies

- <a id="pa-566c0baa8d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreateIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4595f3f039d0e41179b83e166d359c88236be2fbb4371bf7ca9bb672c35bcf59 -->

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
    "signature": "\"(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkCreateIn",
  "unit": "export"
}
```

</details>
