# riverhog_protocol.CollectionUploadWorkBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadworkbatchdocument:2f3ce8bfee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-da21e8fb67"></a>
- <a id="s-043c8f59d5"></a>`distribution`: `riverhog-protocol`
- <a id="s-149aa95b51"></a>`module`: `riverhog_protocol`
- <a id="s-5d03b06b65"></a>`name`: `CollectionUploadWorkBatchDocument`
- <a id="s-aa7fed4df7"></a>`unit`: `export`

### Declared structure

- <a id="s-f8ea43e68d"></a>`kind`: `"class"`
- <a id="s-b68fea4942"></a>`signature`: `"'(*, collection_id: CollectionId, planning_complete: bool, complete: bool, committed_payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], work: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitAssignmentDocument], MaxLen(max_length=64)]) -> None'"`

#### Validated model schema

<a id="s-d72207d86e"></a>
- <a id="s-b423895cbd"></a>`title`: CollectionUploadWorkBatchDocument
- <a id="s-8cda6ecabd"></a>`description`: A bounded acquisition step over currently actionable upload units.
- <a id="s-89745f6d35"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-41a305ac0a"></a>`collection_id` | yes | #/$defs/CollectionId |  |
| <a id="s-ed3a22dcbb"></a>`committed_payload_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-0f5815a1c3"></a>`complete` | yes | type="boolean" |  |
| <a id="s-220f81bee5"></a>`planning_complete` | yes | type="boolean" |  |
| <a id="s-2cba5e289e"></a>`work` | yes | type="array"; maxItems=64; items=(#/$defs/CollectionUploadUnitAssignmentDocument); additional keys=`x-riverhog-extent` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d86841f214"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-00441e0036"></a>`CollectionUploadUnitAssignmentDocument` | type="object"; fields=`plan_sha256`, `unit`, `volume`; additional keys=`additionalProperties`, `required` |
| <a id="s-a34c9f424a"></a>`CollectionUploadUnitSourceDocument` | type="object"; fields=`artifact_sha256`, `bytes`, `offset`, `path`; additional keys=`additionalProperties`, `required` |
| <a id="s-17b7bfe8b3"></a>`CollectionUploadUnitWorkDocument` | type="object"; fields=`payload_bytes`, `plaintext_bytes`, `sources`, `state`, `unit`; additional keys=`additionalProperties`, `required` |
| <a id="s-c2f07785ba"></a>`CollectionUploadVolumeSummaryDocument` | type="object"; fields=`kind`, `sequence`, `volume_id`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadWorkBatchDocument.validate_completion](riverhog-protocol-collectionuploadworkbatchdocument-validate-completion.md)
- [riverhog_protocol.CollectionUploadWorkBatchDocument.canonical_collection_id](riverhog-protocol-collectionuploadworkbatchdocument-canonical-collection-id.md)

## Governing policies

- <a id="pa-cb75ce1ccd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadWorkBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c1fc358885efd1b8835b9b7e705f3ab5d51060544edb741413d5f64fca0a16b -->

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
        "CollectionUploadUnitAssignmentDocument": {
          "additionalProperties": false,
          "description": "One bounded, immutable unit offered by an exact upload session.",
          "properties": {
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Plan Sha256",
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
          "title": "CollectionUploadUnitAssignmentDocument",
          "type": "object"
        },
        "CollectionUploadUnitSourceDocument": {
          "additionalProperties": false,
          "description": "One exact source range supplied in a server-planned upload unit.",
          "properties": {
            "artifact_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Artifact Sha256",
              "type": "string"
            },
            "bytes": {
              "minimum": 0,
              "title": "Bytes",
              "type": "integer"
            },
            "offset": {
              "minimum": 0,
              "title": "Offset",
              "type": "integer"
            },
            "path": {
              "title": "Path",
              "type": "string"
            }
          },
          "required": [
            "path",
            "offset",
            "bytes",
            "artifact_sha256"
          ],
          "title": "CollectionUploadUnitSourceDocument",
          "type": "object"
        },
        "CollectionUploadUnitWorkDocument": {
          "additionalProperties": false,
          "description": "One exact unit and its durable upload checkpoint state.",
          "properties": {
            "payload_bytes": {
              "minimum": 0,
              "title": "Payload Bytes",
              "type": "integer"
            },
            "plaintext_bytes": {
              "minimum": 0,
              "title": "Plaintext Bytes",
              "type": "integer"
            },
            "sources": {
              "items": {
                "$ref": "#/$defs/CollectionUploadUnitSourceDocument"
              },
              "maxItems": 1000,
              "title": "Sources",
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
              "title": "State",
              "type": "string"
            },
            "unit": {
              "minimum": 0,
              "title": "Unit",
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
          "title": "CollectionUploadUnitWorkDocument",
          "type": "object"
        },
        "CollectionUploadVolumeSummaryDocument": {
          "additionalProperties": false,
          "description": "Protocol-owned identity of one immutable collection archive volume.",
          "properties": {
            "kind": {
              "enum": [
                "pack",
                "segment"
              ],
              "title": "Kind",
              "type": "string"
            },
            "sequence": {
              "minimum": 0,
              "title": "Sequence",
              "type": "integer"
            },
            "volume_id": {
              "pattern": "^(?:pack|segment)-[0-9a-f]{64}$",
              "title": "Volume Id",
              "type": "string"
            }
          },
          "required": [
            "volume_id",
            "sequence",
            "kind"
          ],
          "title": "CollectionUploadVolumeSummaryDocument",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "description": "A bounded acquisition step over currently actionable upload units.",
      "properties": {
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "committed_payload_bytes": {
          "minimum": 0,
          "title": "Committed Payload Bytes",
          "type": "integer"
        },
        "complete": {
          "title": "Complete",
          "type": "boolean"
        },
        "planning_complete": {
          "title": "Planning Complete",
          "type": "boolean"
        },
        "work": {
          "items": {
            "$ref": "#/$defs/CollectionUploadUnitAssignmentDocument"
          },
          "maxItems": 64,
          "title": "Work",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "repeated-acquisition-until-complete",
            "reason": "bounded-actionable-work-acquisition"
          }
        }
      },
      "required": [
        "collection_id",
        "planning_complete",
        "complete",
        "committed_payload_bytes",
        "work"
      ],
      "title": "CollectionUploadWorkBatchDocument",
      "type": "object"
    },
    "signature": "'(*, collection_id: CollectionId, planning_complete: bool, complete: bool, committed_payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], work: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitAssignmentDocument], MaxLen(max_length=64)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadWorkBatchDocument",
  "unit": "export"
}
```
