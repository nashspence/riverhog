# a_stove0_media_archive_lib.MediaArchiveProjectionPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaarchivepr-c6c4938f0d:02f3f0a944 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-722999af74"></a>
- <a id="s-5c13ef8a9d"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-ec10857a54"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-980e1df994"></a>`name`: `MediaArchiveProjectionPayload`
- <a id="s-080a22d365"></a>`unit`: `export`

### Declared structure

- <a id="s-02b2a86d3e"></a>`kind`: `"class"`
- <a id="s-5293fba09f"></a>`signature`: `"\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[a_stove0_media_archive_lib.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[a_stove0_media_archive_lib.projection.RetainedXmpSidecar, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-87bf87ff78"></a>

- <a id="s-43b2d1c5bb"></a>`type`: `"object"`
- <a id="s-65b0334112"></a>`additionalProperties`: `false`
- <a id="s-4a86d78b6e"></a>`required`: `["observation_result_sha256s","items"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6fd323ef1d"></a>`format` | no | type="string"; const="stove0-media-archive-projection/v1"; default="stove0-media-archive-projection/v1" |  |
| <a id="s-6fc2977dc0"></a>`items` | yes | type="array"; items=([MediaProjectionItem](#s-b399121093)); minItems=1 |  |
| <a id="s-8c05a5e6c6"></a>`observation_result_sha256s` | yes | type="array"; items=(type="string") |  |
| <a id="s-43bec01f0c"></a>`retained_xmp_sidecars` | no | type="array"; default=[]; items=([RetainedXmpSidecar](#s-5a3d98446f)) |  |

##### Definitions

- [JsonValue](#s-afbd31e61e)
- [MediaFactEvidence](#s-448ffe933f)
- [MediaMetadataFact](#s-c70079a617)
- [MediaProjectedValue](#s-505d156bdd)
- [MediaProjectionItem](#s-b399121093)
- [RetainedXmpSidecar](#s-5a3d98446f)

##### <a id="s-afbd31e61e"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-448ffe933f"></a>definition `MediaFactEvidence`

- <a id="s-9302f3aa58"></a>`type`: `"object"`
- <a id="s-4670b43b94"></a>`additionalProperties`: `false`
- <a id="s-ed0198e5a8"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cf8cf827c0"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-c0b5335fa6"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

##### <a id="s-c70079a617"></a>definition `MediaMetadataFact`

- <a id="s-3e09ec5e82"></a>`type`: `"object"`
- <a id="s-0875cd9325"></a>`additionalProperties`: `false`
- <a id="s-ab8c09923a"></a>`required`: `["name","value","evidence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1cec61a665"></a>`evidence` | yes | [MediaFactEvidence](#s-448ffe933f) |  |
| <a id="s-038fe02d7d"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-89a489fa99"></a>`value` | yes | [JsonValue](#s-afbd31e61e) |  |

##### <a id="s-505d156bdd"></a>definition `MediaProjectedValue`

- <a id="s-401bb8f297"></a>`type`: `"object"`
- <a id="s-6864268929"></a>`additionalProperties`: `false`
- <a id="s-db83078312"></a>`required`: `["name","value","source"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-265b3cc6ce"></a>`evidence` | no | type="array"; default=[]; items=([MediaFactEvidence](#s-448ffe933f)) |  |
| <a id="s-6e4670026e"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-8c1746d086"></a>`source` | yes | type="string"; enum=["observation","recipe"] |  |
| <a id="s-3c76b392d6"></a>`value` | yes | [JsonValue](#s-afbd31e61e) |  |

##### <a id="s-b399121093"></a>definition `MediaProjectionItem`

- <a id="s-e9484b0b33"></a>`type`: `"object"`
- <a id="s-12c5353fc0"></a>`additionalProperties`: `false`
- <a id="s-d2a4b0319f"></a>`required`: `["input_artifact_id","archive_path","xmp_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c9d0cd66c6"></a>`archive_path` | yes | type="string" |  |
| <a id="s-c72b5a6cca"></a>`assertions` | no | type="array"; default=[]; items=([MediaMetadataFact](#s-c70079a617)) |  |
| <a id="s-adae445172"></a>`associated_sidecar_artifact_ids` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-c15b1a4df2"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-62def35f4f"></a>`selected` | no | type="array"; default=[]; items=([MediaProjectedValue](#s-505d156bdd)) |  |
| <a id="s-59919e47de"></a>`xmp_path` | yes | type="string" |  |

##### <a id="s-5a3d98446f"></a>definition `RetainedXmpSidecar`

- <a id="s-07b63eb4ee"></a>`type`: `"object"`
- <a id="s-f24116549c"></a>`additionalProperties`: `false`
- <a id="s-34938b656e"></a>`required`: `["input_artifact_id","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-563d7d9453"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-f743971f1d"></a>`output_path` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [canonical_members](a-stove0-media-archive-lib-mediaarchiveprojectionpayload-canonical-members.md)

## Governing policies

- <a id="pa-eecd133ccf"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaArchiveProjectionPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0753812734e84848a16ef38a9a76084c2ddb52e7308a52a76d82d11828189155 -->

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
        },
        "MediaProjectionItem": {
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
        "RetainedXmpSidecar": {
          "additionalProperties": false,
          "properties": {
            "input_artifact_id": {
              "type": "string"
            },
            "output_path": {
              "type": "string"
            }
          },
          "required": [
            "input_artifact_id",
            "output_path"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-media-archive-projection/v1",
          "default": "stove0-media-archive-projection/v1",
          "type": "string"
        },
        "items": {
          "items": {
            "$ref": "#/$defs/MediaProjectionItem"
          },
          "minItems": 1,
          "type": "array"
        },
        "observation_result_sha256s": {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        "retained_xmp_sidecars": {
          "default": [],
          "items": {
            "$ref": "#/$defs/RetainedXmpSidecar"
          },
          "type": "array"
        }
      },
      "required": [
        "observation_result_sha256s",
        "items"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[a_stove0_media_archive_lib.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[a_stove0_media_archive_lib.projection.RetainedXmpSidecar, ...] = ()) -> None\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "MediaArchiveProjectionPayload",
  "unit": "export"
}
```

</details>
