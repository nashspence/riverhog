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

- <a id="s-89745f6d35"></a>`type`: `"object"`
- <a id="s-87102a1ce9"></a>`additionalProperties`: `false`
- <a id="s-f26708c7a2"></a>`required`: `["collection_id","planning_complete","complete","committed_payload_bytes","work"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-41a305ac0a"></a>`collection_id` | yes | [CollectionId](#s-d86841f214) |  |
| <a id="s-ed3a22dcbb"></a>`committed_payload_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-0f5815a1c3"></a>`complete` | yes | type="boolean" |  |
| <a id="s-220f81bee5"></a>`planning_complete` | yes | type="boolean" |  |
| <a id="s-2cba5e289e"></a>`work` | yes | type="array"; items=([CollectionUploadUnitAssignmentDocument](#s-00441e0036)); maxItems=64; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"repeated-acquisition-until-complete","reason":"bounded-actionable-work-acquisition"} |  |

##### Definitions

- [CollectionId](#s-d86841f214)
- [CollectionUploadUnitAssignmentDocument](#s-00441e0036)
- [CollectionUploadUnitSourceDocument](#s-a34c9f424a)
- [CollectionUploadUnitWorkDocument](#s-17b7bfe8b3)
- [CollectionUploadVolumeSummaryDocument](#s-c2f07785ba)

##### <a id="s-d86841f214"></a>definition `CollectionId`

- <a id="s-c564717568"></a>`type`: `"integer"`
- <a id="s-7b034f93a5"></a>`minimum`: `1`

##### <a id="s-00441e0036"></a>definition `CollectionUploadUnitAssignmentDocument`

- <a id="s-4cc2666eb2"></a>`type`: `"object"`
- <a id="s-caacb32e33"></a>`additionalProperties`: `false`
- <a id="s-db95dc7b7a"></a>`required`: `["volume","plan_sha256","unit"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d5e4ce8002"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-de22a52018"></a>`unit` | yes | [CollectionUploadUnitWorkDocument](#s-17b7bfe8b3) |  |
| <a id="s-020836a32a"></a>`volume` | yes | [CollectionUploadVolumeSummaryDocument](#s-c2f07785ba) |  |

##### <a id="s-a34c9f424a"></a>definition `CollectionUploadUnitSourceDocument`

- <a id="s-bb0ffcc5e9"></a>`type`: `"object"`
- <a id="s-0387f76605"></a>`additionalProperties`: `false`
- <a id="s-0a85fd8c4b"></a>`required`: `["path","offset","bytes","artifact_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4be82078e2"></a>`artifact_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bab257a1ac"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-c459d006a6"></a>`offset` | yes | type="integer"; minimum=0 |  |
| <a id="s-7102e1e469"></a>`path` | yes | type="string" |  |

##### <a id="s-17b7bfe8b3"></a>definition `CollectionUploadUnitWorkDocument`

- <a id="s-82331da9e0"></a>`type`: `"object"`
- <a id="s-8c76322a33"></a>`additionalProperties`: `false`
- <a id="s-660f7cce16"></a>`required`: `["unit","payload_bytes","plaintext_bytes","sources","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7da52b9bd3"></a>`payload_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-275a2ecbbe"></a>`plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-a41f520f9e"></a>`sources` | yes | type="array"; items=([CollectionUploadUnitSourceDocument](#s-a34c9f424a)); maxItems=1000; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"collection-volume-sequence","reason":"bounded-upload-unit-source-map"} |  |
| <a id="s-d589f94e2f"></a>`state` | yes | type="string"; enum=["pending","committed"] |  |
| <a id="s-87178b36cc"></a>`unit` | yes | type="integer"; minimum=0 |  |

##### <a id="s-c2f07785ba"></a>definition `CollectionUploadVolumeSummaryDocument`

- <a id="s-9e877ed290"></a>`type`: `"object"`
- <a id="s-931c3ba6cf"></a>`additionalProperties`: `false`
- <a id="s-6196d108ea"></a>`required`: `["volume_id","sequence","kind"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b391c57378"></a>`kind` | yes | type="string"; enum=["pack","segment"] |  |
| <a id="s-f57e9f7cce"></a>`sequence` | yes | type="integer"; minimum=0 |  |
| <a id="s-f53d35c8aa"></a>`volume_id` | yes | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [validate_completion](riverhog-protocol-collectionuploadworkbatchdocument-validate-completion.md)
- [canonical_collection_id](riverhog-protocol-collectionuploadworkbatchdocument-canonical-collection-id.md)

## Governing policies

- <a id="pa-cb75ce1ccd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadWorkBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 240a104d2de5d4a68b3edc4dd4a2f0dcafd0ac1aa50b2fd82ad23f12b0a9d325 -->

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
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "committed_payload_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "complete": {
          "type": "boolean"
        },
        "planning_complete": {
          "type": "boolean"
        },
        "work": {
          "items": {
            "$ref": "#/$defs/CollectionUploadUnitAssignmentDocument"
          },
          "maxItems": 64,
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

</details>
