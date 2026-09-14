# riverhog_protocol.CollectionDescriptionDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiondescriptiondocument:e51d74dc86 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9fe6ad86cc"></a>
- <a id="s-4a9bb82ae0"></a>`distribution`: `riverhog-protocol`
- <a id="s-d54543eae1"></a>`module`: `riverhog_protocol`
- <a id="s-4b65f4bd06"></a>`name`: `CollectionDescriptionDocument`
- <a id="s-0568af72e8"></a>`unit`: `export`

### Declared structure

- <a id="s-4843afb3fd"></a>`kind`: `"class"`
- <a id="s-d6b753095c"></a>`signature`: `"\"(*, format: Literal['riverhog-collection-description/v1'] = 'riverhog-collection-description/v1', archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], description: CollectionDescription \| None, description_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-de0ced1246"></a>
- <a id="s-0570ee0437"></a>`title`: CollectionDescriptionDocument
- <a id="s-dd99bdb72a"></a>`description`: Canonical independently recoverable description state beside an archive copy.
- <a id="s-75427a3086"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-48822d277f"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-172774a064"></a>`description` | yes | anyOf=#/$defs/CollectionDescription \| type="null" |  |
| <a id="s-239bd713b7"></a>`description_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-45d157bf54"></a>`format` | no | type="string"; const="riverhog-collection-description/v1" |  |
| <a id="s-0e0fc4ee3d"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; additional keys=`x-riverhog-extent` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-fadeb35b68"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionDescriptionDocument.to_json_bytes](riverhog-protocol-collectiondescriptiondocument-to-json-bytes.md)
- [riverhog_protocol.CollectionDescriptionDocument.from_json_bytes](riverhog-protocol-collectiondescriptiondocument-from-json-bytes.md)
- [riverhog_protocol.CollectionDescriptionDocument.validate_identity](riverhog-protocol-collectiondescriptiondocument-validate-identity.md)
- [riverhog_protocol.CollectionDescriptionDocument.seal](riverhog-protocol-collectiondescriptiondocument-seal.md)

## Governing policies

- <a id="pa-640d4e8234"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDescriptionDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a656daa4483a0965d17441629dba55d6a39d49cabb9988050c2818605230eaec -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionDescription": {
          "maxLength": 32768,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 32768,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-catalog-description"
          },
          "x-unicode-normalization": "NFC"
        }
      },
      "additionalProperties": false,
      "description": "Canonical independently recoverable description state beside an archive copy.",
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "description": {
          "anyOf": [
            {
              "$ref": "#/$defs/CollectionDescription"
            },
            {
              "type": "null"
            }
          ]
        },
        "description_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Description Identity",
          "type": "string"
        },
        "format": {
          "const": "riverhog-collection-description/v1",
          "default": "riverhog-collection-description/v1",
          "title": "Format",
          "type": "string"
        },
        "revision": {
          "maximum": 9007199254740991,
          "minimum": 1,
          "title": "Revision",
          "type": "integer",
          "x-riverhog-extent": {
            "policy": "fixed",
            "reason": "exact-json-safe-monotonic-description-revision"
          }
        }
      },
      "required": [
        "archive_root_sha256",
        "revision",
        "description",
        "description_identity"
      ],
      "title": "CollectionDescriptionDocument",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-collection-description/v1'] = 'riverhog-collection-description/v1', archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], description: CollectionDescription | None, description_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDescriptionDocument",
  "unit": "export"
}
```
