# stove0_media_archive_target_contracts.Av1OpusArchiveIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-av1-41e6936986:11eb09eaff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f18e790e7"></a>
- <a id="s-fb119dc711"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-fb1bdaf9bf"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-a2a7fab6c6"></a>`name`: `Av1OpusArchiveIntent`
- <a id="s-42fb7b448c"></a>`unit`: `export`

### Declared structure

- <a id="s-cfc1ffc5e0"></a>`kind`: `"class"`
- <a id="s-ab7445351c"></a>`signature`: `"\"(*, codec: Literal['av1'] = 'av1', container: Literal['mkv'] = 'mkv', quality: Annotated[int, Ge(ge=0), Le(le=63)] = 23, max_height: Annotated[int \| None, Ge(ge=144), Le(le=8640)] = None, audio_bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, salvage: Literal['off', 'safe-remux'] = 'safe-remux', metadata_projection: stove0_media_archive_target_contracts.projection_policy.MediaProjectionPolicy = <factory>) -> None\""`

#### Validated model schema

<a id="s-bfb36d5ba0"></a>
- <a id="s-e538c4d645"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-527bd6fca4"></a>`audio_bitrate_kbps` | no | type="integer"; minimum=16; maximum=512 |  |
| <a id="s-579d91578b"></a>`codec` | no | type="string"; const="av1" |  |
| <a id="s-afc309307b"></a>`container` | no | type="string"; const="mkv" |  |
| <a id="s-2f6ab42ea9"></a>`max_height` | no | anyOf=type="integer"; minimum=144; maximum=8640 \| type="null" |  |
| <a id="s-f4d052b61f"></a>`metadata_projection` | no | #/$defs/MediaProjectionPolicy |  |
| <a id="s-2a49dccf2a"></a>`quality` | no | type="integer"; minimum=0; maximum=63 |  |
| <a id="s-3eed4de269"></a>`salvage` | no | type="string"; enum=["off","safe-remux"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-c28d2d6e04"></a>`MediaFieldPreference` | type="object"; fields=`fields`, `name`; additional keys=`additionalProperties`, `required` |
| <a id="s-09e1a4a6da"></a>`MediaGps` | type="object"; fields=`latitude`, `longitude`; additional keys=`additionalProperties`, `required` |
| <a id="s-0fb0ae795e"></a>`MediaProjectionPolicy` | type="object"; fields=`creators`, `device_make`, `device_model`, `field_preferences`, `format`, `gps`, `tags`; additional keys=`additionalProperties` |

## Governing policies

- <a id="pa-934013709f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.Av1OpusArchiveIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c77dbb5ff502d165d8c42a314df7ffbb080495bf60c01284f03adf24980dd585 -->

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
        },
        "MediaProjectionPolicy": {
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
        }
      },
      "additionalProperties": false,
      "properties": {
        "audio_bitrate_kbps": {
          "default": 128,
          "maximum": 512,
          "minimum": 16,
          "type": "integer"
        },
        "codec": {
          "const": "av1",
          "default": "av1",
          "type": "string"
        },
        "container": {
          "const": "mkv",
          "default": "mkv",
          "type": "string"
        },
        "max_height": {
          "anyOf": [
            {
              "maximum": 8640,
              "minimum": 144,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "metadata_projection": {
          "$ref": "#/$defs/MediaProjectionPolicy"
        },
        "quality": {
          "default": 23,
          "maximum": 63,
          "minimum": 0,
          "type": "integer"
        },
        "salvage": {
          "default": "safe-remux",
          "enum": [
            "off",
            "safe-remux"
          ],
          "type": "string"
        }
      },
      "type": "object"
    },
    "signature": "\"(*, codec: Literal['av1'] = 'av1', container: Literal['mkv'] = 'mkv', quality: Annotated[int, Ge(ge=0), Le(le=63)] = 23, max_height: Annotated[int | None, Ge(ge=144), Le(le=8640)] = None, audio_bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, salvage: Literal['off', 'safe-remux'] = 'safe-remux', metadata_projection: stove0_media_archive_target_contracts.projection_policy.MediaProjectionPolicy = <factory>) -> None\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "Av1OpusArchiveIntent",
  "unit": "export"
}
```
