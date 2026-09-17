# stove0_media_archive_target_support.MediaArchiveProjectionPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-e09af878c8:367bf9ddd7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc6f298259"></a>
- <a id="s-6b8380db07"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-cd5d97b04f"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-d82e5d5fa9"></a>`name`: `MediaArchiveProjectionPayload`
- <a id="s-fccb50dbab"></a>`unit`: `export`

### Declared structure

- <a id="s-1df2912141"></a>`kind`: `"class"`
- <a id="s-7e7322ea5a"></a>`signature`: `"\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[stove0_media_archive_target_support.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[stove0_media_archive_target_support.projection.RetainedXmpSidecar, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-615ba867b7"></a>

- <a id="s-89b6569c4b"></a>`type`: `"object"`
- <a id="s-ac9b5ca7e1"></a>`additionalProperties`: `false`
- <a id="s-1f5b47ced0"></a>`required`: `["observation_result_sha256s","items"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-26780b4a36"></a>`format` | no | type="string"; const="stove0-media-archive-projection/v1"; default="stove0-media-archive-projection/v1" |  |
| <a id="s-d9fdb7bef5"></a>`items` | yes | type="array"; items=([MediaProjectionItem](#s-7e541b1976)); minItems=1 |  |
| <a id="s-3fc7cf0435"></a>`observation_result_sha256s` | yes | type="array"; items=(type="string") |  |
| <a id="s-65e88f9afd"></a>`retained_xmp_sidecars` | no | type="array"; default=[]; items=([RetainedXmpSidecar](#s-551f3d2c14)) |  |

##### Definitions

- [JsonValue](#s-57df564d6d)
- [MediaFactEvidence](#s-2f35d2de95)
- [MediaMetadataFact](#s-3216323aa3)
- [MediaProjectedValue](#s-c29eeee9bc)
- [MediaProjectionItem](#s-7e541b1976)
- [RetainedXmpSidecar](#s-551f3d2c14)

##### <a id="s-57df564d6d"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-2f35d2de95"></a>definition `MediaFactEvidence`

- <a id="s-c85c6c7ab2"></a>`type`: `"object"`
- <a id="s-3c68522ca9"></a>`additionalProperties`: `false`
- <a id="s-872142270b"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d525a8e4a"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-17d88640fe"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

##### <a id="s-3216323aa3"></a>definition `MediaMetadataFact`

- <a id="s-96ddcf57f0"></a>`type`: `"object"`
- <a id="s-af08ff5c94"></a>`additionalProperties`: `false`
- <a id="s-f580e13676"></a>`required`: `["name","value","evidence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba369623ba"></a>`evidence` | yes | [MediaFactEvidence](#s-2f35d2de95) |  |
| <a id="s-54a8288712"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-98149ddf6d"></a>`value` | yes | [JsonValue](#s-57df564d6d) |  |

##### <a id="s-c29eeee9bc"></a>definition `MediaProjectedValue`

- <a id="s-e44d98427e"></a>`type`: `"object"`
- <a id="s-8d47fb508b"></a>`additionalProperties`: `false`
- <a id="s-b63b9b2274"></a>`required`: `["name","value","source"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-911d513572"></a>`evidence` | no | type="array"; default=[]; items=([MediaFactEvidence](#s-2f35d2de95)) |  |
| <a id="s-d274b2fba1"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-e8eab70aaa"></a>`source` | yes | type="string"; enum=["observation","recipe"] |  |
| <a id="s-dc1eda0987"></a>`value` | yes | [JsonValue](#s-57df564d6d) |  |

##### <a id="s-7e541b1976"></a>definition `MediaProjectionItem`

- <a id="s-0b87146286"></a>`type`: `"object"`
- <a id="s-e54b76e8ec"></a>`additionalProperties`: `false`
- <a id="s-240d372ea6"></a>`required`: `["input_artifact_id","archive_path","xmp_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bf9ca2cdd0"></a>`archive_path` | yes | type="string" |  |
| <a id="s-c7d482cfc2"></a>`assertions` | no | type="array"; default=[]; items=([MediaMetadataFact](#s-3216323aa3)) |  |
| <a id="s-ed3bc0dc89"></a>`associated_sidecar_artifact_ids` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-6728c70d95"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-072029ff0c"></a>`selected` | no | type="array"; default=[]; items=([MediaProjectedValue](#s-c29eeee9bc)) |  |
| <a id="s-c5996005b3"></a>`xmp_path` | yes | type="string" |  |

##### <a id="s-551f3d2c14"></a>definition `RetainedXmpSidecar`

- <a id="s-efed119e2c"></a>`type`: `"object"`
- <a id="s-3a2454155c"></a>`additionalProperties`: `false`
- <a id="s-3cf3d71bef"></a>`required`: `["input_artifact_id","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-176ddf895d"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-3a5629e1d3"></a>`output_path` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [canonical_members](stove0-media-archive-target-support-mediaarchiveprojectionpayload-canonical-members.md)

## Governing policies

- <a id="pa-ff35b15178"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources/authorities.md#src-3caa343b1d) — [reference/stove0/targets/media-archive/support/src/stove0\_media\_archive\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjectionPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc888fd2bdf69e3400516b8607d317393a2aa58c8478150919e9a96fef897c27 -->

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
    "signature": "\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[stove0_media_archive_target_support.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[stove0_media_archive_target_support.projection.RetainedXmpSidecar, ...] = ()) -> None\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "MediaArchiveProjectionPayload",
  "unit": "export"
}
```

</details>
