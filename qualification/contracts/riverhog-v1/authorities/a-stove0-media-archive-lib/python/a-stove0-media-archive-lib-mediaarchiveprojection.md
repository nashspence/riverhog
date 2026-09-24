# a_stove0_media_archive_lib.MediaArchiveProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaarchiveprojection:3eb28cafa2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e0be7a5be9"></a>
- <a id="s-e9e2c05e5a"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-3d5031423a"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-16deefc5ed"></a>`name`: `MediaArchiveProjection`
- <a id="s-1dd9bc57a0"></a>`unit`: `export`

### Declared structure

- <a id="s-5e66b818de"></a>`kind`: `"class"`
- <a id="s-deea42637e"></a>`signature`: `"\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[a_stove0_media_archive_lib.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[a_stove0_media_archive_lib.projection.RetainedXmpSidecar, ...] = (), projection_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-e1841f0a45"></a>

- <a id="s-890ade167f"></a>`type`: `"object"`
- <a id="s-bf9d23e705"></a>`additionalProperties`: `false`
- <a id="s-b8edd90593"></a>`required`: `["observation_result_sha256s","items","projection_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f35065647f"></a>`format` | no | type="string"; const="stove0-media-archive-projection/v1"; default="stove0-media-archive-projection/v1" |  |
| <a id="s-7e676f92ff"></a>`items` | yes | type="array"; items=([MediaProjectionItem](#s-dde392a2fd)); minItems=1 |  |
| <a id="s-7e53526b30"></a>`observation_result_sha256s` | yes | type="array"; items=(type="string") |  |
| <a id="s-1a014ca793"></a>`projection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2ac1b968b8"></a>`retained_xmp_sidecars` | no | type="array"; default=[]; items=([RetainedXmpSidecar](#s-ecdb45e70f)) |  |

##### Definitions

- [JsonValue](#s-efc971a9ef)
- [MediaFactEvidence](#s-24ceaeb1e7)
- [MediaMetadataFact](#s-6f7867078f)
- [MediaProjectedValue](#s-fa5e04cf8b)
- [MediaProjectionItem](#s-dde392a2fd)
- [RetainedXmpSidecar](#s-ecdb45e70f)

##### <a id="s-efc971a9ef"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-24ceaeb1e7"></a>definition `MediaFactEvidence`

- <a id="s-459c96b248"></a>`type`: `"object"`
- <a id="s-6792f88e28"></a>`additionalProperties`: `false`
- <a id="s-0ad0d083c6"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3ff45cd13a"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-9bf09ef2cf"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

##### <a id="s-6f7867078f"></a>definition `MediaMetadataFact`

- <a id="s-1c2f4b386b"></a>`type`: `"object"`
- <a id="s-b95335c392"></a>`additionalProperties`: `false`
- <a id="s-f4ecc0253d"></a>`required`: `["name","value","evidence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5d321f4d9d"></a>`evidence` | yes | [MediaFactEvidence](#s-24ceaeb1e7) |  |
| <a id="s-2fa5ea1366"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-ac9b8b37df"></a>`value` | yes | [JsonValue](#s-efc971a9ef) |  |

##### <a id="s-fa5e04cf8b"></a>definition `MediaProjectedValue`

- <a id="s-d2366aa415"></a>`type`: `"object"`
- <a id="s-545e840dd5"></a>`additionalProperties`: `false`
- <a id="s-ef62d978bc"></a>`required`: `["name","value","source"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dbb2a27ab9"></a>`evidence` | no | type="array"; default=[]; items=([MediaFactEvidence](#s-24ceaeb1e7)) |  |
| <a id="s-82d88bb218"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-d81c3a4e05"></a>`source` | yes | type="string"; enum=["observation","recipe"] |  |
| <a id="s-2f738c9f25"></a>`value` | yes | [JsonValue](#s-efc971a9ef) |  |

##### <a id="s-dde392a2fd"></a>definition `MediaProjectionItem`

- <a id="s-3d215aab33"></a>`type`: `"object"`
- <a id="s-3db4f0100a"></a>`additionalProperties`: `false`
- <a id="s-c4e55f25fb"></a>`required`: `["input_artifact_id","archive_path","xmp_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f8db988d90"></a>`archive_path` | yes | type="string" |  |
| <a id="s-49c6078282"></a>`assertions` | no | type="array"; default=[]; items=([MediaMetadataFact](#s-6f7867078f)) |  |
| <a id="s-cc3d12aa06"></a>`associated_sidecar_artifact_ids` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-2f88adaca8"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-b1f9017d65"></a>`selected` | no | type="array"; default=[]; items=([MediaProjectedValue](#s-fa5e04cf8b)) |  |
| <a id="s-34c7fa8b1e"></a>`xmp_path` | yes | type="string" |  |

##### <a id="s-ecdb45e70f"></a>definition `RetainedXmpSidecar`

- <a id="s-965cddcafc"></a>`type`: `"object"`
- <a id="s-095c4b7fdf"></a>`additionalProperties`: `false`
- <a id="s-e65654a16a"></a>`required`: `["input_artifact_id","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-91b3b49bb4"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-99ce7c91b8"></a>`output_path` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [canonical_members](a-stove0-media-archive-lib-mediaarchiveprojection-canonical-members.md)
- [validate_plan_evidence](a-stove0-media-archive-lib-mediaarchiveprojection-validate-plan-evidence.md)
- [seal](a-stove0-media-archive-lib-mediaarchiveprojection-seal.md)
- [verify_digest](a-stove0-media-archive-lib-mediaarchiveprojection-verify-digest.md)
- [item_for](a-stove0-media-archive-lib-mediaarchiveprojection-item-for.md)

## Governing policies

- <a id="pa-343b6d60ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaArchiveProjection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9591d6a94d99faf833ee76df35f4a34227f402ff2b080d699b88487db22f71ff -->

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
        "projection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
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
        "items",
        "projection_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[a_stove0_media_archive_lib.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[a_stove0_media_archive_lib.projection.RetainedXmpSidecar, ...] = (), projection_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "MediaArchiveProjection",
  "unit": "export"
}
```

</details>
