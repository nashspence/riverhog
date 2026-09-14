# stove0_media_archive_target_support.MediaProjectionItem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-7ca9389034:3cd6bb6a77 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b033958ec2"></a>
- <a id="s-693f47030c"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-150d06f3f1"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-d7e02a93cd"></a>`name`: `MediaProjectionItem`
- <a id="s-62122ca441"></a>`unit`: `export`

### Declared structure

- <a id="s-227449cb3b"></a>`kind`: `"class"`
- <a id="s-b2818943a4"></a>`signature`: `"'(*, input_artifact_id: str, associated_sidecar_artifact_ids: tuple[str, ...] = (), archive_path: str, xmp_path: str, assertions: tuple[stove0_media_metadata_observer_contracts.contracts.MediaMetadataFact, ...] = (), selected: tuple[stove0_media_archive_target_support.projection.MediaProjectedValue, ...] = ()) -> None'"`

#### Validated model schema

<a id="s-06ef93095f"></a>
- <a id="s-7197e20472"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-313a118932"></a>`archive_path` | yes | type="string" |  |
| <a id="s-40432666e3"></a>`assertions` | no | type="array"; items=(#/$defs/MediaMetadataFact) |  |
| <a id="s-2c7c592359"></a>`associated_sidecar_artifact_ids` | no | type="array"; items=(type="string") |  |
| <a id="s-790b4a5584"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-3ac59195a2"></a>`selected` | no | type="array"; items=(#/$defs/MediaProjectedValue) |  |
| <a id="s-3730378d5c"></a>`xmp_path` | yes | type="string" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-08ff9de5de"></a>`JsonValue` | empty object |
| <a id="s-3786098b71"></a>`MediaFactEvidence` | type="object"; fields=`artifact_id`, `field`; additional keys=`additionalProperties`, `required` |
| <a id="s-e33b4f41af"></a>`MediaMetadataFact` | type="object"; fields=`evidence`, `name`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-5797d39559"></a>`MediaProjectedValue` | type="object"; fields=`evidence`, `name`, `source`, `value`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaProjectionItem.canonical_evidence](stove0-media-archive-target-support-mediaprojectionitem-canonical-evidence.md)
- [stove0_media_archive_target_support.MediaProjectionItem.derived_from](stove0-media-archive-target-support-mediaprojectionitem-derived-from.md)
- [stove0_media_archive_target_support.MediaProjectionItem.canonical_path](stove0-media-archive-target-support-mediaprojectionitem-canonical-path.md)
- [stove0_media_archive_target_support.MediaProjectionItem.canonical_sidecars](stove0-media-archive-target-support-mediaprojectionitem-canonical-sidecars.md)

## Governing policies

- <a id="pa-d7cda3ce73"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaProjectionItem`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d720dd59bfe6bdc5839089cee78295b05da3da597b94d51499001d1d0469c020 -->

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
        },
        "MediaMetadataFact": {
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
        "MediaProjectedValue": {
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
        }
      },
      "additionalProperties": false,
      "properties": {
        "archive_path": {
          "type": "string"
        },
        "assertions": {
          "default": [],
          "items": {
            "$ref": "#/$defs/MediaMetadataFact"
          },
          "type": "array"
        },
        "associated_sidecar_artifact_ids": {
          "default": [],
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        "input_artifact_id": {
          "type": "string"
        },
        "selected": {
          "default": [],
          "items": {
            "$ref": "#/$defs/MediaProjectedValue"
          },
          "type": "array"
        },
        "xmp_path": {
          "type": "string"
        }
      },
      "required": [
        "input_artifact_id",
        "archive_path",
        "xmp_path"
      ],
      "type": "object"
    },
    "signature": "'(*, input_artifact_id: str, associated_sidecar_artifact_ids: tuple[str, ...] = (), archive_path: str, xmp_path: str, assertions: tuple[stove0_media_metadata_observer_contracts.contracts.MediaMetadataFact, ...] = (), selected: tuple[stove0_media_archive_target_support.projection.MediaProjectedValue, ...] = ()) -> None'"
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "MediaProjectionItem",
  "unit": "export"
}
```
