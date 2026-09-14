# riverhog_protocol.CollectionUploadUnitAssignmentDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitass-1298f1453d:0e02024475 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6c34514b88"></a>
- <a id="s-479d0f76ec"></a>`distribution`: `riverhog-protocol`
- <a id="s-979066d2d0"></a>`module`: `riverhog_protocol`
- <a id="s-2b5d1d766e"></a>`name`: `CollectionUploadUnitAssignmentDocument`
- <a id="s-d2930ada45"></a>`unit`: `export`

### Declared structure

- <a id="s-11174e1156"></a>`kind`: `"class"`
- <a id="s-f1ff63be88"></a>`signature`: `"\"(*, volume: riverhog_protocol.collection_upload_transport.CollectionUploadVolumeSummaryDocument, plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], unit: riverhog_protocol.collection_upload_transport.CollectionUploadUnitWorkDocument) -> None\""`

#### Validated model schema

<a id="s-c091031172"></a>
- <a id="s-09667644eb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e622ff639b"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ccdd4bed5e"></a>`unit` | yes | #/$defs/CollectionUploadUnitWorkDocument |  |
| <a id="s-1be20f559c"></a>`volume` | yes | #/$defs/CollectionUploadVolumeSummaryDocument |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d12188e833"></a>`CollectionUploadUnitSourceDocument` | type="object"; fields=`artifact_sha256`, `bytes`, `offset`, `path`; additional keys=`additionalProperties`, `required` |
| <a id="s-e7fc0df57a"></a>`CollectionUploadUnitWorkDocument` | type="object"; fields=`payload_bytes`, `plaintext_bytes`, `sources`, `state`, `unit`; additional keys=`additionalProperties`, `required` |
| <a id="s-a20aef8598"></a>`CollectionUploadVolumeSummaryDocument` | type="object"; fields=`kind`, `sequence`, `volume_id`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-83e124d136"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitAssignmentDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a14e3571948be190f2d7a5448688a15bd37fe871339d537a4aae652ef6dc9f71 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionUploadUnitSourceDocument": {
          "additionalProperties": false,
          "properties": {
            "artifact_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "offset": {
              "minimum": 0,
              "type": "integer"
            },
            "path": {
              "type": "string"
            }
          },
          "required": [
            "path",
            "offset",
            "bytes",
            "artifact_sha256"
          ],
          "type": "object"
        },
        "CollectionUploadUnitWorkDocument": {
          "additionalProperties": false,
          "properties": {
            "payload_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "plaintext_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "sources": {
              "items": {
                "$ref": "#/$defs/CollectionUploadUnitSourceDocument"
              },
              "maxItems": 1000,
              "type": "array",
              "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "progression": "collection-volume-sequence",
                "reason": "bounded-upload-unit-source-map"
              }
            },
            "state": {
              "enum": [
                "pending",
                "committed"
              ],
              "type": "string"
            },
            "unit": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "unit",
            "payload_bytes",
            "plaintext_bytes",
            "sources",
            "state"
          ],
          "type": "object"
        },
        "CollectionUploadVolumeSummaryDocument": {
          "additionalProperties": false,
          "properties": {
            "kind": {
              "enum": [
                "pack",
                "segment"
              ],
              "type": "string"
            },
            "sequence": {
              "minimum": 0,
              "type": "integer"
            },
            "volume_id": {
              "pattern": "^(?:pack|segment)-[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "volume_id",
            "sequence",
            "kind"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "unit": {
          "$ref": "#/$defs/CollectionUploadUnitWorkDocument"
        },
        "volume": {
          "$ref": "#/$defs/CollectionUploadVolumeSummaryDocument"
        }
      },
      "required": [
        "volume",
        "plan_sha256",
        "unit"
      ],
      "type": "object"
    },
    "signature": "\"(*, volume: riverhog_protocol.collection_upload_transport.CollectionUploadVolumeSummaryDocument, plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], unit: riverhog_protocol.collection_upload_transport.CollectionUploadUnitWorkDocument) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitAssignmentDocument",
  "unit": "export"
}
```
