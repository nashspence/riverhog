# a_stove0_media_archive_contract_lib.Av1OpusArchiveIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-av1op-216dbabc2d:26a2ea2a51 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2f0f6f58c5"></a>
- <a id="s-32c882647c"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-976762fdbf"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-b7a663d41d"></a>`name`: `Av1OpusArchiveIntent`
- <a id="s-92593346bf"></a>`unit`: `export`

### Declared structure

- <a id="s-903af954f4"></a>`kind`: `"class"`
- <a id="s-d4ae08c97a"></a>`signature`: `"\"(*, codec: Literal['av1'] = 'av1', container: Literal['mkv'] = 'mkv', quality: Annotated[int, Ge(ge=0), Le(le=63)] = 23, max_height: Annotated[int \| None, Ge(ge=144), Le(le=8640)] = None, audio_bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, salvage: Literal['off', 'safe-remux'] = 'safe-remux', metadata_projection: a_stove0_media_archive_contract_lib.projection_policy.MediaProjectionPolicy = <factory>) -> None\""`

#### Validated model schema

<a id="s-8c59ff34c5"></a>

- <a id="s-9217ae2a4a"></a>`type`: `"object"`
- <a id="s-545193f4b9"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-419faf5aae"></a>`audio_bitrate_kbps` | no | type="integer"; minimum=16; maximum=512; default=128 |  |
| <a id="s-197eafe7d5"></a>`codec` | no | type="string"; const="av1"; default="av1" |  |
| <a id="s-0721727e58"></a>`container` | no | type="string"; const="mkv"; default="mkv" |  |
| <a id="s-6f00bff191"></a>`max_height` | no | anyOf=[(type="integer"; minimum=144; maximum=8640); (type="null")]; default=null |  |
| <a id="s-4bb4da80d5"></a>`metadata_projection` | no | [MediaProjectionPolicy](#s-41b4f3d2f0) |  |
| <a id="s-03b3dfcc20"></a>`quality` | no | type="integer"; minimum=0; maximum=63; default=23 |  |
| <a id="s-686d9b4add"></a>`salvage` | no | type="string"; enum=["off","safe-remux"]; default="safe-remux" |  |

##### Definitions

- [MediaFieldPreference](#s-1f638b91ce)
- [MediaGps](#s-e4c036f568)
- [MediaProjectionPolicy](#s-41b4f3d2f0)

##### <a id="s-1f638b91ce"></a>definition `MediaFieldPreference`

- <a id="s-cef503c565"></a>`type`: `"object"`
- <a id="s-100bf062dc"></a>`additionalProperties`: `false`
- <a id="s-c3c5824e23"></a>`required`: `["name","fields"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ae218eb64"></a>`fields` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-2a21a0b2d5"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |

##### <a id="s-e4c036f568"></a>definition `MediaGps`

- <a id="s-ce8a5e8769"></a>`type`: `"object"`
- <a id="s-c8ab1be086"></a>`additionalProperties`: `false`
- <a id="s-4dc6475f7c"></a>`required`: `["latitude","longitude"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ffcd1e8a13"></a>`latitude` | yes | type="number" |  |
| <a id="s-933ff4577b"></a>`longitude` | yes | type="number" |  |

##### <a id="s-41b4f3d2f0"></a>definition `MediaProjectionPolicy`

- <a id="s-5fbcf5b131"></a>`type`: `"object"`
- <a id="s-3dd33d8083"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3a5628b53a"></a>`creators` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-fab01d1866"></a>`device_make` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-118e2f15e3"></a>`device_model` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-a41313b646"></a>`field_preferences` | no | type="array"; default=[]; items=([MediaFieldPreference](#s-1f638b91ce)) |  |
| <a id="s-f8164d94ff"></a>`format` | no | type="string"; const="stove0-media-projection-policy/v1"; default="stove0-media-projection-policy/v1" |  |
| <a id="s-bf217c2acf"></a>`gps` | no | anyOf=[([MediaGps](#s-e4c036f568)); (type="null")]; default=null |  |
| <a id="s-9753c9ec5d"></a>`tags` | no | type="array"; default=[]; items=(type="string") |  |

## Governing policies

- <a id="pa-3a05674007"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.Av1OpusArchiveIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87b510b6c7d9fd3bb4d3f7fc57675c0819212ec525376c7154a82838f23fdb09 -->

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
    "signature": "\"(*, codec: Literal['av1'] = 'av1', container: Literal['mkv'] = 'mkv', quality: Annotated[int, Ge(ge=0), Le(le=63)] = 23, max_height: Annotated[int | None, Ge(ge=144), Le(le=8640)] = None, audio_bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, salvage: Literal['off', 'safe-remux'] = 'safe-remux', metadata_projection: a_stove0_media_archive_contract_lib.projection_policy.MediaProjectionPolicy = <factory>) -> None\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "Av1OpusArchiveIntent",
  "unit": "export"
}
```

</details>
