# riverhog_protocol.RetirementClaimReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retirementclaimreferencedocument:72c7681a98 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a3b7fc9ce8"></a>
- <a id="s-d82f2c90ad"></a>`distribution`: `riverhog-protocol`
- <a id="s-321a1fb187"></a>`module`: `riverhog_protocol`
- <a id="s-0a4afab5ee"></a>`name`: `RetirementClaimReferenceDocument`
- <a id="s-e4146fce05"></a>`unit`: `export`

### Declared structure

- <a id="s-594e347785"></a>`kind`: `"class"`
- <a id="s-4fa14614b2"></a>`signature`: `"\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[NonnegativeDecimal, Ge(ge=1)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_id: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None, output_collection_id: CollectionId \| None = None, outcomes: riverhog_protocol.collection_workflow_transport.ExactSetAuthorityDocument \| None = None) -> None\""`

#### Validated model schema

<a id="s-83a11ebd8e"></a>

- <a id="s-42858d1624"></a>`type`: `"object"`
- <a id="s-cd95ec7059"></a>`additionalProperties`: `false`
- <a id="s-3e370fcd50"></a>`required`: `["claim_id","fence","work_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4040051f30"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ebb211919a"></a>`execution_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-25792bb28a"></a>`fence` | yes | [NonnegativeDecimal](#s-0d8a9e0a98); ge=1 |  |
| <a id="s-8aa807b6bd"></a>`outcomes` | no | anyOf=[([ExactSetAuthorityDocument](#s-6c83c9bf2b)); (type="null")]; default=null |  |
| <a id="s-5b8d5d0a4e"></a>`output_collection_id` | no | anyOf=[([CollectionId](#s-c5d15daa23)); (type="null")]; default=null |  |
| <a id="s-5178cf253f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-bf0ba16d67) |
| 2 | [See `oneOf` alternative 2](#s-e9d70d2c3c) |

##### Definitions

- [CollectionId](#s-c5d15daa23)
- [ExactSetAuthorityDocument](#s-6c83c9bf2b)
- [NonnegativeDecimal](#s-0d8a9e0a98)

##### <a id="s-bf0ba16d67"></a>`oneOf` alternative 1

- <a id="s-1d4b281732"></a>`required`: `["execution_id","output_collection_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-412ece3957"></a>`execution_id` | yes | type="string" |  |
| <a id="s-cc5632f835"></a>`outcomes` | no | type="null" |  |
| <a id="s-dd5a2f007c"></a>`output_collection_id` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |  |

##### <a id="s-e9d70d2c3c"></a>`oneOf` alternative 2

- <a id="s-79b870a8cf"></a>`required`: `["outcomes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2403cc13ee"></a>`execution_id` | no | type="null" |  |
| <a id="s-e1925cf638"></a>`outcomes` | yes | type="object" |  |
| <a id="s-b30f4283a0"></a>`output_collection_id` | no | type="null" |  |

##### <a id="s-c5d15daa23"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-33ed70c73f"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-08ad80a06f"></a>2 | not=(const="0") |

##### <a id="s-6c83c9bf2b"></a>definition `ExactSetAuthorityDocument`

- <a id="s-00a9133b54"></a>`type`: `"object"`
- <a id="s-1d3d20b824"></a>`additionalProperties`: `false`
- <a id="s-908ee70733"></a>`required`: `["count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-28db1d9da4"></a>`count` | yes | [NonnegativeDecimal](#s-0d8a9e0a98); ge=1 |  |
| <a id="s-a8e5ee4523"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0d8a9e0a98"></a>definition `NonnegativeDecimal`

- <a id="s-d90eed6f45"></a>`type`: `"string"`
- <a id="s-1c2eedf67e"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-retirementclaimreferencedocument-getitem.md)
- [validate_settlement_form](riverhog-protocol-retirementclaimreferencedocument-validate-settlement-form.md)
- [get](riverhog-protocol-retirementclaimreferencedocument-get.md)

## Governing policies

- <a id="pa-8a65e8800b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RetirementClaimReferenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6230b0fe7f074ee4d8fd1fbaa98ab4a1368c41e054229c4928494227b73e568 -->

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
        "ExactSetAuthorityDocument": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "count",
            "sha256"
          ],
          "type": "object"
        },
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "oneOf": [
        {
          "properties": {
            "execution_id": {
              "type": "string"
            },
            "outcomes": {
              "type": "null"
            },
            "output_collection_id": {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            }
          },
          "required": [
            "execution_id",
            "output_collection_id"
          ]
        },
        {
          "properties": {
            "execution_id": {
              "type": "null"
            },
            "outcomes": {
              "type": "object"
            },
            "output_collection_id": {
              "type": "null"
            }
          },
          "required": [
            "outcomes"
          ]
        }
      ],
      "properties": {
        "claim_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "execution_id": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "fence": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "outcomes": {
          "anyOf": [
            {
              "$ref": "#/$defs/ExactSetAuthorityDocument"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "output_collection_id": {
          "anyOf": [
            {
              "$ref": "#/$defs/CollectionId"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "claim_id",
        "fence",
        "work_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[NonnegativeDecimal, Ge(ge=1)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_id: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None, output_collection_id: CollectionId | None = None, outcomes: riverhog_protocol.collection_workflow_transport.ExactSetAuthorityDocument | None = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetirementClaimReferenceDocument",
  "unit": "export"
}
```

</details>
