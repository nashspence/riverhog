# riverhog_protocol.CollectionUploadFileBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadfilebatchdocument:173ab38ab6 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-91c8e59943"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e69dcd18dc"></a>`files` | yes | type="array"; minItems=1; maxItems=100; items=(#/$defs/CollectionUploadFileIn); additional keys=`x-riverhog-extent` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-aedd4af0bf"></a>`CanonicalRelPath` | type="string"; format="riverhog-canonical-relpath-v1"; minLength=1; maxLength=4096; pattern="^[^/\\\\]+(?:/[^/\\\\]+)*$"; allOf=additional keys=`not` \| additional keys=`not`; additional keys=`x-unicode-normalization` |
| <a id="s-e2f87c22de"></a>`CapturedFileProvenanceBinding` | type="object"; fields=`current_state_id`, `journal_id`, `status`; additional keys=`additionalProperties`, `required` |
| <a id="s-624087427b"></a>`CollectionUploadFileIn` | type="object"; fields=`bytes`, `path`, `provenance`, `raw_parts`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-979ba8003b"></a>`CollectionUploadRawPartsIn` | type="object"; fields=`ordered_sha256`, `part_count`, `part_plaintext_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-fc35d7c3a7"></a>`OmittedFileProvenanceBinding` | type="object"; fields=`omission_reason`, `status`; additional keys=`additionalProperties`, `required` |
| <a id="s-ca7445ece9"></a>`ProvenanceJournalId` | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |
| <a id="s-7998e0cad4"></a>`ProvenanceStateId` | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadFileBatchDocument.validate_unique_file_paths](riverhog-protocol-collectionuploadfilebatchdocument-validate-unique-file-paths.md)

## Governing policies

- <a id="pa-efce5a676f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadFileBatchDocument`

### Exact owned JSON

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
