# riverhog_protocol.RetirementClaimReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retirementclaimreferencedocument:72c7681a98 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-4fa14614b2"></a>`signature`: `"\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_id: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None, output_collection_id: CollectionId \| None = None, outcomes: riverhog_protocol.collection_workflow_transport.ExactSetAuthorityDocument \| None = None) -> None\""`

#### Validated model schema

<a id="s-83a11ebd8e"></a>
- <a id="s-42858d1624"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4040051f30"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ebb211919a"></a>`execution_id` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-25792bb28a"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-8aa807b6bd"></a>`outcomes` | no | anyOf=#/$defs/ExactSetAuthorityDocument \| type="null" |  |
| <a id="s-5b8d5d0a4e"></a>`output_collection_id` | no | anyOf=#/$defs/CollectionId \| type="null" |  |
| <a id="s-5178cf253f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-c5d15daa23"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-6c83c9bf2b"></a>`ExactSetAuthorityDocument` | type="object"; fields=`count`, `sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-retirementclaimreferencedocument-getitem.md)
- [validate_settlement_form](riverhog-protocol-retirementclaimreferencedocument-validate-settlement-form.md)
- [get](riverhog-protocol-retirementclaimreferencedocument-get.md)

## Governing policies

- <a id="pa-8a65e8800b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetirementClaimReferenceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 402d10557aa0805be306f8b741c817a76bc988f589f30f486006a9c5b8400c4d -->

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
        "ExactSetAuthorityDocument": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
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
              "type": "integer"
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
          "minimum": 1,
          "type": "integer"
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
    "signature": "\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_id: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None, output_collection_id: CollectionId | None = None, outcomes: riverhog_protocol.collection_workflow_transport.ExactSetAuthorityDocument | None = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetirementClaimReferenceDocument",
  "unit": "export"
}
```
