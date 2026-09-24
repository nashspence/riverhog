# a_stove0_media_archive_contract_lib.MediaProjectionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-media-eaf2b3aeeb:9839334a6b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b164e7a33"></a>
- <a id="s-4f1edd1fca"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-e8321db6c7"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-ec1df0ff0b"></a>`name`: `MediaProjectionPolicy`
- <a id="s-543b9ca8df"></a>`unit`: `export`

### Declared structure

- <a id="s-58f78bc0f4"></a>`kind`: `"class"`
- <a id="s-469569263e"></a>`signature`: `"\"(*, format: Literal['stove0-media-projection-policy/v1'] = 'stove0-media-projection-policy/v1', device_make: str \| None = None, device_model: str \| None = None, gps: a_stove0_media_archive_contract_lib.projection_policy.MediaGps \| None = None, creators: tuple[str, ...] = (), tags: tuple[str, ...] = (), field_preferences: tuple[a_stove0_media_archive_contract_lib.projection_policy.MediaFieldPreference, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-c4a4386234"></a>

- <a id="s-3f7bebe242"></a>`type`: `"object"`
- <a id="s-8b22a9503d"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca4cb0d8c2"></a>`creators` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-fc36571191"></a>`device_make` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-b1fac2c0b3"></a>`device_model` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-44cba3bccb"></a>`field_preferences` | no | type="array"; default=[]; items=([MediaFieldPreference](#s-e6af5f9bd1)) |  |
| <a id="s-9c7c1c3baf"></a>`format` | no | type="string"; const="stove0-media-projection-policy/v1"; default="stove0-media-projection-policy/v1" |  |
| <a id="s-e788d83e7b"></a>`gps` | no | anyOf=[([MediaGps](#s-8131912f0c)); (type="null")]; default=null |  |
| <a id="s-0553b71626"></a>`tags` | no | type="array"; default=[]; items=(type="string") |  |

##### Definitions

- [MediaFieldPreference](#s-e6af5f9bd1)
- [MediaGps](#s-8131912f0c)

##### <a id="s-e6af5f9bd1"></a>definition `MediaFieldPreference`

- <a id="s-e79b4ab096"></a>`type`: `"object"`
- <a id="s-18c0668dd0"></a>`additionalProperties`: `false`
- <a id="s-b8e571e829"></a>`required`: `["name","fields"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac5a64c483"></a>`fields` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-698332ebb3"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |

##### <a id="s-8131912f0c"></a>definition `MediaGps`

- <a id="s-1098ba95d4"></a>`type`: `"object"`
- <a id="s-6999bae8ff"></a>`additionalProperties`: `false`
- <a id="s-35a6aa3518"></a>`required`: `["latitude","longitude"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-73c0ba7a93"></a>`latitude` | yes | type="number" |  |
| <a id="s-75fa591e08"></a>`longitude` | yes | type="number" |  |

## Maintained corroboration

### Related interface records

- [canonical_optional_text](a-stove0-media-archive-contract-lib-mediaprojectionpolicy-canonical-optional-text.md)
- [canonical_tags](a-stove0-media-archive-contract-lib-mediaprojectionpolicy-canonical-tags.md)
- [canonical_preferences](a-stove0-media-archive-contract-lib-mediaprojectionpolicy-canonical-preferences.md)
- [canonical_creators](a-stove0-media-archive-contract-lib-mediaprojectionpolicy-canonical-creators.md)

## Governing policies

- <a id="pa-ec7fcd5042"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.MediaProjectionPolicy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee2f37c3be9075082c151e2cede648732cca0ab0291dd8154f6af13ae1c1c10a -->

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
    "signature": "\"(*, format: Literal['stove0-media-projection-policy/v1'] = 'stove0-media-projection-policy/v1', device_make: str | None = None, device_model: str | None = None, gps: a_stove0_media_archive_contract_lib.projection_policy.MediaGps | None = None, creators: tuple[str, ...] = (), tags: tuple[str, ...] = (), field_preferences: tuple[a_stove0_media_archive_contract_lib.projection_policy.MediaFieldPreference, ...] = ()) -> None\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "MediaProjectionPolicy",
  "unit": "export"
}
```

</details>
