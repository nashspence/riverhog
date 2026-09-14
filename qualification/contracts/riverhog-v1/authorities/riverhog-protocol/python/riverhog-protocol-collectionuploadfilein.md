# riverhog_protocol.CollectionUploadFileIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadfilein:f0086a248d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d65abb63f3"></a>
- <a id="s-d05122f27b"></a>`distribution`: `riverhog-protocol`
- <a id="s-267f254c58"></a>`module`: `riverhog_protocol`
- <a id="s-cc2efd5537"></a>`name`: `CollectionUploadFileIn`
- <a id="s-0c59057afa"></a>`unit`: `export`

### Declared structure

- <a id="s-f7ee9ce2f4"></a>`kind`: `"class"`
- <a id="s-650f7668c9"></a>`signature`: `"\"(*, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], raw_parts: riverhog_protocol.collection_upload_transport.CollectionUploadRawPartsIn \| None = None, provenance: Optional[Annotated[riverhog_protocol.collection_upload_transport.CapturedFileProvenanceBinding \| riverhog_protocol.collection_upload_transport.OmittedFileProvenanceBinding, FieldInfo(annotation=NoneType, required=True, discriminator='status')]] = None) -> None\""`

#### Validated model schema

<a id="s-ac3087e8e5"></a>
- <a id="s-7ffb7d72cd"></a>`title`: CollectionUploadFileIn
- <a id="s-431c1cf8c3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ef0950b9b"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-7e7965894e"></a>`path` | yes | #/$defs/CanonicalRelPath |  |
| <a id="s-f6526b2622"></a>`provenance` | no | anyOf=oneOf=#/$defs/CapturedFileProvenanceBinding \| #/$defs/OmittedFileProvenanceBinding; additional keys=`discriminator` \| type="null" |  |
| <a id="s-d708f72f7f"></a>`raw_parts` | no | anyOf=#/$defs/CollectionUploadRawPartsIn \| type="null" |  |
| <a id="s-d662903a5f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-2c127c38b4"></a>`CanonicalRelPath` | type="string"; format="riverhog-canonical-relpath-v1"; minLength=1; maxLength=4096; pattern="^[^/\\\\]+(?:/[^/\\\\]+)*$"; allOf=additional keys=`not` \| additional keys=`not`; additional keys=`x-unicode-normalization` |
| <a id="s-e4b2dfa1c2"></a>`CapturedFileProvenanceBinding` | type="object"; fields=`current_state_id`, `journal_id`, `status`; additional keys=`additionalProperties`, `required` |
| <a id="s-9617869a96"></a>`CollectionUploadRawPartsIn` | type="object"; fields=`ordered_sha256`, `part_count`, `part_plaintext_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-774afcbe22"></a>`OmittedFileProvenanceBinding` | type="object"; fields=`omission_reason`, `status`; additional keys=`additionalProperties`, `required` |
| <a id="s-71d9f30b54"></a>`ProvenanceJournalId` | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |
| <a id="s-31a410e8a3"></a>`ProvenanceStateId` | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |

## Governing policies

- <a id="pa-2e4809c9bd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadFileIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2c8208d009e273563d84f55a5358c5c764d4639bca5d532a53ccab8ddc77ddf -->

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
              "title": "Status",
              "type": "string"
            }
          },
          "required": [
            "journal_id",
            "current_state_id",
            "status"
          ],
          "title": "CapturedFileProvenanceBinding",
          "type": "object"
        },
        "CollectionUploadRawPartsIn": {
          "additionalProperties": false,
          "properties": {
            "ordered_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Ordered Sha256",
              "type": "string"
            },
            "part_count": {
              "minimum": 1,
              "title": "Part Count",
              "type": "integer"
            },
            "part_plaintext_bytes": {
              "minimum": 65536,
              "title": "Part Plaintext Bytes",
              "type": "integer"
            }
          },
          "required": [
            "part_plaintext_bytes",
            "part_count",
            "ordered_sha256"
          ],
          "title": "CollectionUploadRawPartsIn",
          "type": "object"
        },
        "OmittedFileProvenanceBinding": {
          "additionalProperties": false,
          "properties": {
            "omission_reason": {
              "minLength": 1,
              "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
              "title": "Omission Reason",
              "type": "string"
            },
            "status": {
              "const": "omitted",
              "title": "Status",
              "type": "string"
            }
          },
          "required": [
            "status",
            "omission_reason"
          ],
          "title": "OmittedFileProvenanceBinding",
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
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
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
          "default": null,
          "title": "Provenance"
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
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "path",
        "bytes",
        "sha256"
      ],
      "title": "CollectionUploadFileIn",
      "type": "object"
    },
    "signature": "\"(*, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], raw_parts: riverhog_protocol.collection_upload_transport.CollectionUploadRawPartsIn | None = None, provenance: Optional[Annotated[riverhog_protocol.collection_upload_transport.CapturedFileProvenanceBinding | riverhog_protocol.collection_upload_transport.OmittedFileProvenanceBinding, FieldInfo(annotation=NoneType, required=True, discriminator='status')]] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadFileIn",
  "unit": "export"
}
```
