# a_stove0_media_archive_contract_lib.AudioArchiveIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-audio-6a8a73cc2b:d6641b19de -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0a9b52571e"></a>
- <a id="s-ed97be9042"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-1052348e9d"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-40845967be"></a>`name`: `AudioArchiveIntent`
- <a id="s-27893458e1"></a>`unit`: `export`

### Declared structure

- <a id="s-a089f8fd26"></a>`kind`: `"class"`
- <a id="s-a5da6636b4"></a>`signature`: `"\"(*, codec: Literal['opus'] = 'opus', container: Literal['opus'] = 'opus', bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, metadata_projection: a_stove0_media_archive_contract_lib.projection_policy.MediaProjectionPolicy = <factory>) -> None\""`

#### Validated model schema

<a id="s-67fee9d310"></a>

- <a id="s-9c3cb1a3d6"></a>`type`: `"object"`
- <a id="s-564bb2ba21"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66f616bd1a"></a>`bitrate_kbps` | no | type="integer"; minimum=16; maximum=512; default=128 |  |
| <a id="s-536e4d9270"></a>`codec` | no | type="string"; const="opus"; default="opus" |  |
| <a id="s-3c56b97e68"></a>`container` | no | type="string"; const="opus"; default="opus" |  |
| <a id="s-0a3f607404"></a>`metadata_projection` | no | [MediaProjectionPolicy](#s-24070f9172) |  |

##### Definitions

- [MediaFieldPreference](#s-3d249c1297)
- [MediaGps](#s-c19a4657e1)
- [MediaProjectionPolicy](#s-24070f9172)

##### <a id="s-3d249c1297"></a>definition `MediaFieldPreference`

- <a id="s-64bb4fbd53"></a>`type`: `"object"`
- <a id="s-5a404e0f5f"></a>`additionalProperties`: `false`
- <a id="s-3e7bfa0c9a"></a>`required`: `["name","fields"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-65f459cfd4"></a>`fields` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-fd0e05e724"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |

##### <a id="s-c19a4657e1"></a>definition `MediaGps`

- <a id="s-89edc6c14c"></a>`type`: `"object"`
- <a id="s-66d257be42"></a>`additionalProperties`: `false`
- <a id="s-7381043a5b"></a>`required`: `["latitude","longitude"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f134a8a57d"></a>`latitude` | yes | type="number" |  |
| <a id="s-bfc607545d"></a>`longitude` | yes | type="number" |  |

##### <a id="s-24070f9172"></a>definition `MediaProjectionPolicy`

- <a id="s-570b0d57c5"></a>`type`: `"object"`
- <a id="s-fddef00dbf"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c19d71d4cb"></a>`creators` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-b9d84157d1"></a>`device_make` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-8211491673"></a>`device_model` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-b7f9cb8dfe"></a>`field_preferences` | no | type="array"; default=[]; items=([MediaFieldPreference](#s-3d249c1297)) |  |
| <a id="s-f3a7216712"></a>`format` | no | type="string"; const="stove0-media-projection-policy/v1"; default="stove0-media-projection-policy/v1" |  |
| <a id="s-d449de7f07"></a>`gps` | no | anyOf=[([MediaGps](#s-c19a4657e1)); (type="null")]; default=null |  |
| <a id="s-0b7923e60d"></a>`tags` | no | type="array"; default=[]; items=(type="string") |  |

## Governing policies

- <a id="pa-14aaf611aa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.AudioArchiveIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ec924c33ed8dc26544a65f1d293de7a8299f6ecf7031e8ffe335f3649760528 -->

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
        "bitrate_kbps": {
          "default": 128,
          "maximum": 512,
          "minimum": 16,
          "type": "integer"
        },
        "codec": {
          "const": "opus",
          "default": "opus",
          "type": "string"
        },
        "container": {
          "const": "opus",
          "default": "opus",
          "type": "string"
        },
        "metadata_projection": {
          "$ref": "#/$defs/MediaProjectionPolicy"
        }
      },
      "type": "object"
    },
    "signature": "\"(*, codec: Literal['opus'] = 'opus', container: Literal['opus'] = 'opus', bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, metadata_projection: a_stove0_media_archive_contract_lib.projection_policy.MediaProjectionPolicy = <factory>) -> None\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "AudioArchiveIntent",
  "unit": "export"
}
```

</details>
