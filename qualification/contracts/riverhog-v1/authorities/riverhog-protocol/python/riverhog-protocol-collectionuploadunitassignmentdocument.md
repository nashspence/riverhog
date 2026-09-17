# riverhog_protocol.CollectionUploadUnitAssignmentDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitass-1298f1453d:0e02024475 -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-09667644eb"></a>`type`: `"object"`
- <a id="s-1b25300ce9"></a>`additionalProperties`: `false`
- <a id="s-dba794caf8"></a>`required`: `["volume","plan_sha256","unit"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e622ff639b"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ccdd4bed5e"></a>`unit` | yes | [CollectionUploadUnitWorkDocument](#s-e7fc0df57a) |  |
| <a id="s-1be20f559c"></a>`volume` | yes | [CollectionUploadVolumeSummaryDocument](#s-a20aef8598) |  |

##### Definitions

- [CollectionUploadUnitSourceDocument](#s-d12188e833)
- [CollectionUploadUnitWorkDocument](#s-e7fc0df57a)
- [CollectionUploadVolumeSummaryDocument](#s-a20aef8598)

##### <a id="s-d12188e833"></a>definition `CollectionUploadUnitSourceDocument`

- <a id="s-5de5388840"></a>`type`: `"object"`
- <a id="s-654785851a"></a>`additionalProperties`: `false`
- <a id="s-abb8ce52da"></a>`required`: `["path","offset","bytes","artifact_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f55ec22682"></a>`artifact_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8a834688c6"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-bb945ed89d"></a>`offset` | yes | type="integer"; minimum=0 |  |
| <a id="s-1e8d3750a0"></a>`path` | yes | type="string" |  |

##### <a id="s-e7fc0df57a"></a>definition `CollectionUploadUnitWorkDocument`

- <a id="s-1371414108"></a>`type`: `"object"`
- <a id="s-e43b487455"></a>`additionalProperties`: `false`
- <a id="s-072a69acfb"></a>`required`: `["unit","payload_bytes","plaintext_bytes","sources","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-538ec52b7e"></a>`payload_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-2cb1770cb6"></a>`plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-a01a88dc1b"></a>`sources` | yes | type="array"; items=([CollectionUploadUnitSourceDocument](#s-d12188e833)); maxItems=1000; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"collection-volume-sequence","reason":"bounded-upload-unit-source-map"} |  |
| <a id="s-e7b49d9602"></a>`state` | yes | type="string"; enum=["pending","committed"] |  |
| <a id="s-39c0575712"></a>`unit` | yes | type="integer"; minimum=0 |  |

##### <a id="s-a20aef8598"></a>definition `CollectionUploadVolumeSummaryDocument`

- <a id="s-f85e595b78"></a>`type`: `"object"`
- <a id="s-c418ea3ec8"></a>`additionalProperties`: `false`
- <a id="s-e0a616fb49"></a>`required`: `["volume_id","sequence","kind"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-be0976b9b6"></a>`kind` | yes | type="string"; enum=["pack","segment"] |  |
| <a id="s-b5eabe3bab"></a>`sequence` | yes | type="integer"; minimum=0 |  |
| <a id="s-b06857b9eb"></a>`volume_id` | yes | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-83e124d136"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitAssignmentDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
