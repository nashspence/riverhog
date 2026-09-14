# stove0_media_archive_target_support.MediaProjectedValue

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-0d069711dc:edeaa692a2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dd46f38ca4"></a>
- <a id="s-32bfb70aa4"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-d5690a54a4"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-d475a78205"></a>`name`: `MediaProjectedValue`
- <a id="s-e27301f78e"></a>`unit`: `export`

### Declared structure

- <a id="s-07f0cff672"></a>`kind`: `"class"`
- <a id="s-6dbaf0f576"></a>`signature`: `"\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, source: Literal['observation', 'recipe'], evidence: tuple[stove0_media_metadata_observer_contracts.contracts.MediaFactEvidence, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-a6afa45f9c"></a>
- <a id="s-dcd28f60ec"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e1aadd981"></a>`evidence` | no | type="array"; items=(#/$defs/MediaFactEvidence) |  |
| <a id="s-07dc9075c1"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-aebfdb916c"></a>`source` | yes | type="string"; enum=["observation","recipe"] |  |
| <a id="s-0202da227e"></a>`value` | yes | #/$defs/JsonValue |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-cd70d44f81"></a>`JsonValue` | empty object |
| <a id="s-d89aacd611"></a>`MediaFactEvidence` | type="object"; fields=`artifact_id`, `field`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaProjectedValue.canonical_evidence](stove0-media-archive-target-support-mediaprojectedvalue-canonical-evidence.md)
- [stove0_media_archive_target_support.MediaProjectedValue.bind_source](stove0-media-archive-target-support-mediaprojectedvalue-bind-source.md)

## Governing policies

- <a id="pa-a2fad9707f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaProjectedValue`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dde6e0aeaded7433550a2bab59c6274c950557aa80cbe5bc8ba7ac86f994154c -->

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
          "default": [],
          "items": {
            "$ref": "#/$defs/MediaFactEvidence"
          },
          "type": "array"
        },
        "name": {
          "enum": [
            "capture-time",
            "creator",
            "device-make",
            "device-model",
            "gps-latitude",
            "gps-longitude"
          ],
          "type": "string"
        },
        "source": {
          "enum": [
            "observation",
            "recipe"
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
        "source"
      ],
      "type": "object"
    },
    "signature": "\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, source: Literal['observation', 'recipe'], evidence: tuple[stove0_media_metadata_observer_contracts.contracts.MediaFactEvidence, ...] = ()) -> None\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "MediaProjectedValue",
  "unit": "export"
}
```
