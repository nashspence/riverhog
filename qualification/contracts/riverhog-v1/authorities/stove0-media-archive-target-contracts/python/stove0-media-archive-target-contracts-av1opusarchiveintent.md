# stove0_media_archive_target_contracts.Av1OpusArchiveIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-av1-41e6936986:11eb09eaff -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-e538c4d645"></a>`type`: `"object"`
- <a id="s-61e9df2e4f"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-527bd6fca4"></a>`audio_bitrate_kbps` | no | type="integer"; minimum=16; maximum=512; default=128 |  |
| <a id="s-579d91578b"></a>`codec` | no | type="string"; const="av1"; default="av1" |  |
| <a id="s-afc309307b"></a>`container` | no | type="string"; const="mkv"; default="mkv" |  |
| <a id="s-2f6ab42ea9"></a>`max_height` | no | anyOf=[(type="integer"; minimum=144; maximum=8640); (type="null")]; default=null |  |
| <a id="s-f4d052b61f"></a>`metadata_projection` | no | [MediaProjectionPolicy](#s-0fb0ae795e) |  |
| <a id="s-2a49dccf2a"></a>`quality` | no | type="integer"; minimum=0; maximum=63; default=23 |  |
| <a id="s-3eed4de269"></a>`salvage` | no | type="string"; enum=["off","safe-remux"]; default="safe-remux" |  |

##### Definitions

- [MediaFieldPreference](#s-c28d2d6e04)
- [MediaGps](#s-09e1a4a6da)
- [MediaProjectionPolicy](#s-0fb0ae795e)

##### <a id="s-c28d2d6e04"></a>definition `MediaFieldPreference`

- <a id="s-2060505c7a"></a>`type`: `"object"`
- <a id="s-99a67274ef"></a>`additionalProperties`: `false`
- <a id="s-607f2570d2"></a>`required`: `["name","fields"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6cc3d7c15d"></a>`fields` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-7f0f935fbd"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |

##### <a id="s-09e1a4a6da"></a>definition `MediaGps`

- <a id="s-977389c181"></a>`type`: `"object"`
- <a id="s-fc24e9920c"></a>`additionalProperties`: `false`
- <a id="s-97fbbbc748"></a>`required`: `["latitude","longitude"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a83f528c8e"></a>`latitude` | yes | type="number" |  |
| <a id="s-0bd8f8931b"></a>`longitude` | yes | type="number" |  |

##### <a id="s-0fb0ae795e"></a>definition `MediaProjectionPolicy`

- <a id="s-c5468b1600"></a>`type`: `"object"`
- <a id="s-622569400c"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-036c2544c0"></a>`creators` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-998a33cc95"></a>`device_make` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-a08c7d222b"></a>`device_model` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-58fe3b9e26"></a>`field_preferences` | no | type="array"; default=[]; items=([MediaFieldPreference](#s-c28d2d6e04)) |  |
| <a id="s-f39e326dd9"></a>`format` | no | type="string"; const="stove0-media-projection-policy/v1"; default="stove0-media-projection-policy/v1" |  |
| <a id="s-03f14af20a"></a>`gps` | no | anyOf=[([MediaGps](#s-09e1a4a6da)); (type="null")]; default=null |  |
| <a id="s-0675c84e95"></a>`tags` | no | type="array"; default=[]; items=(type="string") |  |

## Governing policies

- <a id="pa-934013709f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources/authorities.md#src-dfeb5229f2) — [reference/stove0/targets/media-archive/contracts/src/stove0\_media\_archive\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.Av1OpusArchiveIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
