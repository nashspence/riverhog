# stove0_media_archive_target_contracts.MediaProjectionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-6d23f20f43:b93167371c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0583469c59"></a>
- <a id="s-e924690e61"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-786c5fda7e"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-08d103b95c"></a>`name`: `MediaProjectionPolicy`
- <a id="s-b5c0bc4f38"></a>`unit`: `export`

### Declared structure

- <a id="s-1b78e378a7"></a>`kind`: `"class"`
- <a id="s-ecf27d7543"></a>`signature`: `"\"(*, format: Literal['stove0-media-projection-policy/v1'] = 'stove0-media-projection-policy/v1', device_make: str \| None = None, device_model: str \| None = None, gps: stove0_media_archive_target_contracts.projection_policy.MediaGps \| None = None, creators: tuple[str, ...] = (), tags: tuple[str, ...] = (), field_preferences: tuple[stove0_media_archive_target_contracts.projection_policy.MediaFieldPreference, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-1ef0b6d911"></a>
- <a id="s-3a66fe7954"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f165b60f52"></a>`creators` | no | type="array"; items=(type="string") |  |
| <a id="s-8a5b4f8985"></a>`device_make` | no | anyOf=type="string" \| type="null" |  |
| <a id="s-5b78b916e3"></a>`device_model` | no | anyOf=type="string" \| type="null" |  |
| <a id="s-bd011a12ad"></a>`field_preferences` | no | type="array"; items=(#/$defs/MediaFieldPreference) |  |
| <a id="s-d84a23de97"></a>`format` | no | type="string"; const="stove0-media-projection-policy/v1" |  |
| <a id="s-767a992ab5"></a>`gps` | no | anyOf=#/$defs/MediaGps \| type="null" |  |
| <a id="s-abb83142af"></a>`tags` | no | type="array"; items=(type="string") |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-9266a8309a"></a>`MediaFieldPreference` | type="object"; fields=`fields`, `name`; additional keys=`additionalProperties`, `required` |
| <a id="s-12cf49e35b"></a>`MediaGps` | type="object"; fields=`latitude`, `longitude`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [canonical_preferences](stove0-media-archive-target-contracts-mediaprojectionpolicy-canonical-preferences.md)
- [canonical_creators](stove0-media-archive-target-contracts-mediaprojectionpolicy-canonical-creators.md)
- [canonical_tags](stove0-media-archive-target-contracts-mediaprojectionpolicy-canonical-tags.md)
- [canonical_optional_text](stove0-media-archive-target-contracts-mediaprojectionpolicy-canonical-optional-text.md)

## Governing policies

- <a id="pa-f31c9b1a62"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaProjectionPolicy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60ce5add8be41e573cfbc0cbdc3dec931648239b003a1d568ed229e0f245c25a -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "MediaFieldPreference": {
          "additionalProperties": false,
          "properties": {
            "fields": {
              "items": {
                "type": "string"
              },
              "minItems": 1,
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
            }
          },
          "required": [
            "name",
            "fields"
          ],
          "type": "object"
        },
        "MediaGps": {
          "additionalProperties": false,
          "properties": {
            "latitude": {
              "type": "number"
            },
            "longitude": {
              "type": "number"
            }
          },
          "required": [
            "latitude",
            "longitude"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "creators": {
          "default": [],
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        "device_make": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "device_model": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "field_preferences": {
          "default": [],
          "items": {
            "$ref": "#/$defs/MediaFieldPreference"
          },
          "type": "array"
        },
        "format": {
          "const": "stove0-media-projection-policy/v1",
          "default": "stove0-media-projection-policy/v1",
          "type": "string"
        },
        "gps": {
          "anyOf": [
            {
              "$ref": "#/$defs/MediaGps"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "tags": {
          "default": [],
          "items": {
            "type": "string"
          },
          "type": "array"
        }
      },
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-media-projection-policy/v1'] = 'stove0-media-projection-policy/v1', device_make: str | None = None, device_model: str | None = None, gps: stove0_media_archive_target_contracts.projection_policy.MediaGps | None = None, creators: tuple[str, ...] = (), tags: tuple[str, ...] = (), field_preferences: tuple[stove0_media_archive_target_contracts.projection_policy.MediaFieldPreference, ...] = ()) -> None\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "MediaProjectionPolicy",
  "unit": "export"
}
```
