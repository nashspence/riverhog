# riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-sourcecollectionretirem-01fe187845:6c9514f323 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6ed0922f2"></a>
- <a id="s-140230a5dc"></a>`distribution`: `riverhog-protocol`
- <a id="s-9665fe23f7"></a>`module`: `riverhog_protocol`
- <a id="s-eaecfd4b32"></a>`name`: `SourceCollectionRetirementClaimReferenceDocument`
- <a id="s-28b1069042"></a>`unit`: `export`

### Declared structure

- <a id="s-354931abed"></a>`kind`: `"class"`
- <a id="s-5b729b30ad"></a>`signature`: `"\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[NonnegativeDecimal, Ge(ge=1)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_id: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None, output_collection_id: CollectionId \| None = None, outcomes: riverhog_protocol.collection_workflow_transport.ExactSetIdentityDocument \| None = None) -> None\""`

#### Validated model schema

<a id="s-261a9e5bdb"></a>

- <a id="s-143527cc2d"></a>`type`: `"object"`
- <a id="s-c724e8d2f1"></a>`additionalProperties`: `false`
- <a id="s-60f81eb32d"></a>`required`: `["claim_id","fence","work_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e749de66d0"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-94b1d3ca7a"></a>`execution_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-6995a16d2c"></a>`fence` | yes | [NonnegativeDecimal](#s-fabdd0a7e3); ge=1 |  |
| <a id="s-d1cc87b67e"></a>`outcomes` | no | anyOf=[([ExactSetIdentityDocument](#s-8fcf812718)); (type="null")]; default=null |  |
| <a id="s-dfd60b481f"></a>`output_collection_id` | no | anyOf=[([CollectionId](#s-b7035f848b)); (type="null")]; default=null |  |
| <a id="s-f66c58c167"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-ad4e2371a7) |
| 2 | [See `oneOf` alternative 2](#s-1ef14420ca) |

##### Definitions

- [CollectionId](#s-b7035f848b)
- [ExactSetIdentityDocument](#s-8fcf812718)
- [NonnegativeDecimal](#s-fabdd0a7e3)

##### <a id="s-ad4e2371a7"></a>`oneOf` alternative 1

- <a id="s-a00abe1e26"></a>`required`: `["execution_id","output_collection_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c3c900040b"></a>`execution_id` | yes | type="string" |  |
| <a id="s-8523341b6e"></a>`outcomes` | no | type="null" |  |
| <a id="s-a8a1f8947f"></a>`output_collection_id` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |  |

##### <a id="s-1ef14420ca"></a>`oneOf` alternative 2

- <a id="s-62ae107acb"></a>`required`: `["outcomes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5cb807389c"></a>`execution_id` | no | type="null" |  |
| <a id="s-d2169a0203"></a>`outcomes` | yes | type="object" |  |
| <a id="s-4ea9cacdeb"></a>`output_collection_id` | no | type="null" |  |

##### <a id="s-b7035f848b"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e1aff9dbe0"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-eaeac0dbb2"></a>2 | not=(const="0") |

##### <a id="s-8fcf812718"></a>definition `ExactSetIdentityDocument`

- <a id="s-5583a7d90a"></a>`type`: `"object"`
- <a id="s-729994d2a5"></a>`additionalProperties`: `false`
- <a id="s-38eba43e21"></a>`required`: `["count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fe0bf9d948"></a>`count` | yes | [NonnegativeDecimal](#s-fabdd0a7e3); ge=1 |  |
| <a id="s-ab0b69960e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fabdd0a7e3"></a>definition `NonnegativeDecimal`

- <a id="s-9b8df14735"></a>`type`: `"string"`
- <a id="s-85f87f6e8b"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-sourcecollectionretirementclaimreferencedocument-getitem.md)
- [validate_settlement_form](riverhog-protocol-sourcecollectionretirementclaimreferencedocument-validate-s-d2ffc82827.md)
- [get](riverhog-protocol-sourcecollectionretirementclaimreferencedocument-get.md)

## Governing policies

- <a id="pa-a338031a65"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 01d767e847c466fb637c3fcce4088a3f42bdbff0659acc5136f5208a7e335872 -->

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
        "ExactSetIdentityDocument": {
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
              "$ref": "#/$defs/ExactSetIdentityDocument"
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
    "signature": "\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[NonnegativeDecimal, Ge(ge=1)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_id: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None, output_collection_id: CollectionId | None = None, outcomes: riverhog_protocol.collection_workflow_transport.ExactSetIdentityDocument | None = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "SourceCollectionRetirementClaimReferenceDocument",
  "unit": "export"
}
```

</details>
