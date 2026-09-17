# riverhog_protocol.CollectionUploadFileBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadfilebatchdocument:173ab38ab6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4002f1fbf9"></a>
- <a id="s-137799159a"></a>`distribution`: `riverhog-protocol`
- <a id="s-480ca2826c"></a>`module`: `riverhog_protocol`
- <a id="s-b71e351082"></a>`name`: `CollectionUploadFileBatchDocument`
- <a id="s-ec11c5c66a"></a>`unit`: `export`

### Declared structure

- <a id="s-724e3ea012"></a>`kind`: `"class"`
- <a id="s-ef50f8d273"></a>`signature`: `"'(*, files: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadFileIn], MinLen(min_length=1), MaxLen(max_length=100)]) -> None'"`

#### Validated model schema

<a id="s-0dc1dc3116"></a>

- <a id="s-91c8e59943"></a>`type`: `"object"`
- <a id="s-38f3673641"></a>`additionalProperties`: `false`
- <a id="s-27ff1137ef"></a>`required`: `["files"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e69dcd18dc"></a>`files` | yes | type="array"; items=([CollectionUploadFileIn](#s-624087427b)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"repeated-artifact-registration","reason":"bounded-upload-registration"} |  |

##### Definitions

- [CanonicalRelPath](#s-aedd4af0bf)
- [CapturedFileProvenanceBinding](#s-e2f87c22de)
- [CollectionUploadFileIn](#s-624087427b)
- [CollectionUploadRawPartsIn](#s-979ba8003b)
- [OmittedFileProvenanceBinding](#s-fc35d7c3a7)
- [ProvenanceJournalId](#s-ca7445ece9)
- [ProvenanceStateId](#s-7998e0cad4)

##### <a id="s-aedd4af0bf"></a>definition `CanonicalRelPath`

- <a id="s-57b0e4a4a2"></a>`type`: `"string"`
- <a id="s-1446683e57"></a>`format`: `"riverhog-canonical-relpath-v1"`
- <a id="s-8c023797aa"></a>`maxLength`: `4096`
- <a id="s-8e25322fc2"></a>`minLength`: `1`
- <a id="s-ade24bf622"></a>`pattern`: `"^[^/\\\\]+(?:/[^/\\\\]+)*$"`
- <a id="s-ad367fb4d8"></a>`x-unicode-normalization`: `"NFC"`

###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-b1436a506a"></a>1 | not=(pattern="(?:^\|/)\\.{1,2}(?:/\|$)") |
| <a id="s-2c8822c509"></a>2 | not=(pattern="^\\s\|\\s$") |

##### <a id="s-e2f87c22de"></a>definition `CapturedFileProvenanceBinding`

- <a id="s-3eb6c20ae6"></a>`type`: `"object"`
- <a id="s-829c7bc4dc"></a>`additionalProperties`: `false`
- <a id="s-444de41dd8"></a>`required`: `["journal_id","current_state_id","status"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc1910cf7f"></a>`current_state_id` | yes | [ProvenanceStateId](#s-7998e0cad4) |  |
| <a id="s-193ecf38ac"></a>`journal_id` | yes | [ProvenanceJournalId](#s-ca7445ece9) |  |
| <a id="s-4ac71ad851"></a>`status` | yes | type="string"; const="captured" |  |

##### <a id="s-624087427b"></a>definition `CollectionUploadFileIn`

- <a id="s-3010257398"></a>`type`: `"object"`
- <a id="s-dc80773187"></a>`additionalProperties`: `false`
- <a id="s-a7e14bb919"></a>`required`: `["path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e8bce37c75"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-d43a3b5f31"></a>`path` | yes | [CanonicalRelPath](#s-aedd4af0bf) |  |
| <a id="s-ae7523361a"></a>`provenance` | no | anyOf=[(discriminator={"mapping":{"captured":"#/$defs/CapturedFileProvenanceBinding","omitted":"#/$defs/OmittedFileProvenanceBinding"},"propertyName":"status"}; oneOf=[([CapturedFileProvenanceBinding](#s-e2f87c22de)); ([OmittedFileProvenanceBinding](#s-fc35d7c3a7))]); (type="null")]; default=null |  |
| <a id="s-ec34394539"></a>`raw_parts` | no | anyOf=[([CollectionUploadRawPartsIn](#s-979ba8003b)); (type="null")]; default=null |  |
| <a id="s-7890182389"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-979ba8003b"></a>definition `CollectionUploadRawPartsIn`

- <a id="s-cf04aadf77"></a>`type`: `"object"`
- <a id="s-1c6e69818c"></a>`additionalProperties`: `false`
- <a id="s-8085110b19"></a>`required`: `["part_plaintext_bytes","part_count","ordered_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-917c4b4234"></a>`ordered_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ce52c1a3df"></a>`part_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-560a9fa2aa"></a>`part_plaintext_bytes` | yes | type="integer"; minimum=65536 |  |

##### <a id="s-fc35d7c3a7"></a>definition `OmittedFileProvenanceBinding`

- <a id="s-89b126f7e9"></a>`type`: `"object"`
- <a id="s-064d1981ce"></a>`additionalProperties`: `false`
- <a id="s-87c6fa2e4d"></a>`required`: `["status","omission_reason"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d56c600105"></a>`omission_reason` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| <a id="s-360f7a9bee"></a>`status` | yes | type="string"; const="omitted" |  |

##### <a id="s-ca7445ece9"></a>definition `ProvenanceJournalId`

- <a id="s-ed42ffe768"></a>`type`: `"string"`
- <a id="s-33bc931883"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

##### <a id="s-7998e0cad4"></a>definition `ProvenanceStateId`

- <a id="s-1ba20b25d6"></a>`type`: `"string"`
- <a id="s-bef00b84fc"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

## Maintained corroboration

### Related interface records

- [validate_unique_file_paths](riverhog-protocol-collectionuploadfilebatchdocument-validate-unique-file-paths.md)

## Governing policies

- <a id="pa-efce5a676f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadFileBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04ae8b54bf12ed225355d0ccb742c4728d2c4bbfa00a0a16c3e01d420264770f -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CanonicalRelPath": {
          "allOf": [
            {
              "not": {
                "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
              }
            },
            {
              "not": {
                "pattern": "^\\s|\\s$"
              }
            }
          ],
          "format": "riverhog-canonical-relpath-v1",
          "maxLength": 4096,
          "minLength": 1,
          "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
          "type": "string",
          "x-unicode-normalization": "NFC"
        },
        "CapturedFileProvenanceBinding": {
          "additionalProperties": false,
          "properties": {
            "current_state_id": {
              "$ref": "#/$defs/ProvenanceStateId"
            },
            "journal_id": {
              "$ref": "#/$defs/ProvenanceJournalId"
            },
            "status": {
              "const": "captured",
              "type": "string"
            }
          },
          "required": [
            "journal_id",
            "current_state_id",
            "status"
          ],
          "type": "object"
        },
        "CollectionUploadFileIn": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "path": {
              "$ref": "#/$defs/CanonicalRelPath"
            },
            "provenance": {
              "anyOf": [
                {
                  "discriminator": {
                    "mapping": {
                      "captured": "#/$defs/CapturedFileProvenanceBinding",
                      "omitted": "#/$defs/OmittedFileProvenanceBinding"
                    },
                    "propertyName": "status"
                  },
                  "oneOf": [
                    {
                      "$ref": "#/$defs/CapturedFileProvenanceBinding"
                    },
                    {
                      "$ref": "#/$defs/OmittedFileProvenanceBinding"
                    }
                  ]
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "raw_parts": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionUploadRawPartsIn"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "path",
            "bytes",
            "sha256"
          ],
          "type": "object"
        },
        "CollectionUploadRawPartsIn": {
          "additionalProperties": false,
          "properties": {
            "ordered_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "part_count": {
              "minimum": 1,
              "type": "integer"
            },
            "part_plaintext_bytes": {
              "minimum": 65536,
              "type": "integer"
            }
          },
          "required": [
            "part_plaintext_bytes",
            "part_count",
            "ordered_sha256"
          ],
          "type": "object"
        },
        "OmittedFileProvenanceBinding": {
          "additionalProperties": false,
          "properties": {
            "omission_reason": {
              "minLength": 1,
              "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
              "type": "string"
            },
            "status": {
              "const": "omitted",
              "type": "string"
            }
          },
          "required": [
            "status",
            "omission_reason"
          ],
          "type": "object"
        },
        "ProvenanceJournalId": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        },
        "ProvenanceStateId": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "files": {
          "items": {
            "$ref": "#/$defs/CollectionUploadFileIn"
          },
          "maxItems": 100,
          "minItems": 1,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "repeated-artifact-registration",
            "reason": "bounded-upload-registration"
          }
        }
      },
      "required": [
        "files"
      ],
      "type": "object"
    },
    "signature": "'(*, files: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadFileIn], MinLen(min_length=1), MaxLen(max_length=100)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadFileBatchDocument",
  "unit": "export"
}
```

</details>
