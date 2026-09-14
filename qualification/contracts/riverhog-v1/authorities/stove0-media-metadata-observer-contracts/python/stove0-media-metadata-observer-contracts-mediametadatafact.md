# stove0_media_metadata_observer_contracts.MediaMetadataFact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-fb0f5ce0ed:3149c27bc6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-99aa99dbbe"></a>
- <a id="s-9c778d8d73"></a>`distribution`: `stove0-media-metadata-observer-contracts`
- <a id="s-4ac12ed9fa"></a>`module`: `stove0_media_metadata_observer_contracts`
- <a id="s-e5a61a906d"></a>`name`: `MediaMetadataFact`
- <a id="s-bdecce9aa4"></a>`unit`: `export`

### Declared structure

- <a id="s-f93b9429ae"></a>`kind`: `"class"`
- <a id="s-7d6ac34844"></a>`signature`: `"\"(*, name: Literal['capture-time', 'container-format', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, evidence: stove0_media_metadata_observer_contracts.contracts.MediaFactEvidence) -> None\""`

#### Validated model schema

<a id="s-1ea8c031f9"></a>
- <a id="s-c50044e3ec"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-85da213ab6"></a>`evidence` | yes | #/$defs/MediaFactEvidence |  |
| <a id="s-aff70022ce"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-a6a962ed5c"></a>`value` | yes | #/$defs/JsonValue |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-62e920a617"></a>`JsonValue` | empty object |
| <a id="s-c2f402a43a"></a>`MediaFactEvidence` | type="object"; fields=`artifact_id`, `field`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-16b59f8075"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaMetadataFact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2056851604846f2fbd9d334a108b9506a96bab0bb1890861db981f53ad3548a8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {},
        "MediaFactEvidence": {
          "additionalProperties": false,
          "properties": {
            "artifact_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "field": {
              "maxLength": 240,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "artifact_id",
            "field"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "evidence": {
          "$ref": "#/$defs/MediaFactEvidence"
        },
        "name": {
          "enum": [
            "capture-time",
            "container-format",
            "creator",
            "device-make",
            "device-model",
            "gps-latitude",
            "gps-longitude"
          ],
          "type": "string"
        },
        "value": {
          "$ref": "#/$defs/JsonValue"
        }
      },
      "required": [
        "name",
        "value",
        "evidence"
      ],
      "type": "object"
    },
    "signature": "\"(*, name: Literal['capture-time', 'container-format', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, evidence: stove0_media_metadata_observer_contracts.contracts.MediaFactEvidence) -> None\""
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MediaMetadataFact",
  "unit": "export"
}
```
