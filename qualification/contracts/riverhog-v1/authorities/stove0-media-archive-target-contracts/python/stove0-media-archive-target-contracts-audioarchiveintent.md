# stove0_media_archive_target_contracts.AudioArchiveIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-aud-24045fc431:f2041a5c58 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e9beca85e7"></a>
- <a id="s-68944ba22b"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-b998bed64a"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-d7a71a48ad"></a>`name`: `AudioArchiveIntent`
- <a id="s-ad85ad5df2"></a>`unit`: `export`

### Declared structure

- <a id="s-397da9f871"></a>`kind`: `"class"`
- <a id="s-0faca004dc"></a>`signature`: `"\"(*, codec: Literal['opus'] = 'opus', container: Literal['opus'] = 'opus', bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, metadata_projection: stove0_media_archive_target_contracts.projection_policy.MediaProjectionPolicy = <factory>) -> None\""`

#### Validated model schema

<a id="s-cd9f857894"></a>

- <a id="s-2301185882"></a>`type`: `"object"`
- <a id="s-54d7b9e1ea"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4de69062e4"></a>`bitrate_kbps` | no | type="integer"; minimum=16; maximum=512; default=128 |  |
| <a id="s-63c15502f4"></a>`codec` | no | type="string"; const="opus"; default="opus" |  |
| <a id="s-ad463d6aa3"></a>`container` | no | type="string"; const="opus"; default="opus" |  |
| <a id="s-8cc72d3b41"></a>`metadata_projection` | no | [MediaProjectionPolicy](#s-89c84af88a) |  |

##### Definitions

- [MediaFieldPreference](#s-e222d648c9)
- [MediaGps](#s-08ab823d60)
- [MediaProjectionPolicy](#s-89c84af88a)

##### <a id="s-e222d648c9"></a>definition `MediaFieldPreference`

- <a id="s-cdaa260d33"></a>`type`: `"object"`
- <a id="s-582c4781dd"></a>`additionalProperties`: `false`
- <a id="s-f32fcf4780"></a>`required`: `["name","fields"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-87e2b4889c"></a>`fields` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-1f997c5967"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |

##### <a id="s-08ab823d60"></a>definition `MediaGps`

- <a id="s-d2d7467bd0"></a>`type`: `"object"`
- <a id="s-6ab2401432"></a>`additionalProperties`: `false`
- <a id="s-0516664118"></a>`required`: `["latitude","longitude"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a475d11b5"></a>`latitude` | yes | type="number" |  |
| <a id="s-349d10d8ff"></a>`longitude` | yes | type="number" |  |

##### <a id="s-89c84af88a"></a>definition `MediaProjectionPolicy`

- <a id="s-4c77f35f5c"></a>`type`: `"object"`
- <a id="s-434b973cb5"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-68c91641ed"></a>`creators` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-dda1ba525f"></a>`device_make` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-b4dcb42879"></a>`device_model` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-fe8cbe0bf4"></a>`field_preferences` | no | type="array"; default=[]; items=([MediaFieldPreference](#s-e222d648c9)) |  |
| <a id="s-a0c73b3253"></a>`format` | no | type="string"; const="stove0-media-projection-policy/v1"; default="stove0-media-projection-policy/v1" |  |
| <a id="s-ec7a4f028b"></a>`gps` | no | anyOf=[([MediaGps](#s-08ab823d60)); (type="null")]; default=null |  |
| <a id="s-7563bacfb4"></a>`tags` | no | type="array"; default=[]; items=(type="string") |  |

## Governing policies

- <a id="pa-fcbc8d0ea3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources/authorities.md#src-dfeb5229f2) — [reference/stove0/targets/media-archive/contracts/src/stove0\_media\_archive\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.AudioArchiveIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61aed3e8bc983d467ab26de1ee655bfb98746e75e37b1a15a371c2c2a7ab2281 -->

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
    "signature": "\"(*, codec: Literal['opus'] = 'opus', container: Literal['opus'] = 'opus', bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, metadata_projection: stove0_media_archive_target_contracts.projection_policy.MediaProjectionPolicy = <factory>) -> None\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "AudioArchiveIntent",
  "unit": "export"
}
```

</details>
