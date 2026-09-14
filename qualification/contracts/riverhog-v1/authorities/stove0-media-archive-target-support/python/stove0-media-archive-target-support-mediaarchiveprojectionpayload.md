# stove0_media_archive_target_support.MediaArchiveProjectionPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-e09af878c8:367bf9ddd7 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-11100408ff"></a>`title`: MediaArchiveProjectionPayload
- <a id="s-89b6569c4b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-26780b4a36"></a>`format` | no | type="string"; const="stove0-media-archive-projection/v1" |  |
| <a id="s-d9fdb7bef5"></a>`items` | yes | type="array"; minItems=1; items=(#/$defs/MediaProjectionItem) |  |
| <a id="s-3fc7cf0435"></a>`observation_result_sha256s` | yes | type="array"; items=(type="string") |  |
| <a id="s-65e88f9afd"></a>`retained_xmp_sidecars` | no | type="array"; items=(#/$defs/RetainedXmpSidecar) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-57df564d6d"></a>`JsonValue` | empty object |
| <a id="s-2f35d2de95"></a>`MediaFactEvidence` | type="object"; fields=`artifact_id`, `field`; additional keys=`additionalProperties`, `required` |
| <a id="s-3216323aa3"></a>`MediaMetadataFact` | type="object"; fields=`evidence`, `name`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-c29eeee9bc"></a>`MediaProjectedValue` | type="object"; fields=`evidence`, `name`, `source`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-7e541b1976"></a>`MediaProjectionItem` | type="object"; fields=`archive_path`, `assertions`, `associated_sidecar_artifact_ids`, `input_artifact_id`, `selected`, `xmp_path`; additional keys=`additionalProperties`, `required` |
| <a id="s-551f3d2c14"></a>`RetainedXmpSidecar` | type="object"; fields=`input_artifact_id`, `output_path`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaArchiveProjectionPayload.canonical_members](stove0-media-archive-target-support-mediaarchiveprojectionpayload-canonical-members.md)

## Governing policies

- <a id="pa-ff35b15178"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjectionPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa9c54c5d62188532a0bcd2c5fa045532c09a19e19ed5b96e9297176fbda8320 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {},
        "MediaFactEvidence": {
          "additionalProperties": false,
          "description": "Exact artifact and ExifTool field from which one value was read.",
          "properties": {
            "artifact_id": {
              "maxLength": 160,
              "minLength": 1,
              "title": "Artifact Id",
              "type": "string"
            },
            "field": {
              "maxLength": 240,
              "minLength": 1,
              "title": "Field",
              "type": "string"
            }
          },
          "required": [
            "artifact_id",
            "field"
          ],
          "title": "MediaFactEvidence",
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
              "title": "Name",
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
          "title": "MediaMetadataFact",
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
              "title": "Evidence",
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
              "title": "Name",
              "type": "string"
            },
            "source": {
              "enum": [
                "observation",
                "recipe"
              ],
              "title": "Source",
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
          "title": "MediaProjectedValue",
          "type": "object"
        },
        "MediaProjectionItem": {
          "additionalProperties": false,
          "properties": {
            "archive_path": {
              "title": "Archive Path",
              "type": "string"
            },
            "assertions": {
              "default": [],
              "items": {
                "$ref": "#/$defs/MediaMetadataFact"
              },
              "title": "Assertions",
              "type": "array"
            },
            "associated_sidecar_artifact_ids": {
              "default": [],
              "items": {
                "type": "string"
              },
              "title": "Associated Sidecar Artifact Ids",
              "type": "array"
            },
            "input_artifact_id": {
              "title": "Input Artifact Id",
              "type": "string"
            },
            "selected": {
              "default": [],
              "items": {
                "$ref": "#/$defs/MediaProjectedValue"
              },
              "title": "Selected",
              "type": "array"
            },
            "xmp_path": {
              "title": "Xmp Path",
              "type": "string"
            }
          },
          "required": [
            "input_artifact_id",
            "archive_path",
            "xmp_path"
          ],
          "title": "MediaProjectionItem",
          "type": "object"
        },
        "RetainedXmpSidecar": {
          "additionalProperties": false,
          "properties": {
            "input_artifact_id": {
              "title": "Input Artifact Id",
              "type": "string"
            },
            "output_path": {
              "title": "Output Path",
              "type": "string"
            }
          },
          "required": [
            "input_artifact_id",
            "output_path"
          ],
          "title": "RetainedXmpSidecar",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-media-archive-projection/v1",
          "default": "stove0-media-archive-projection/v1",
          "title": "Format",
          "type": "string"
        },
        "items": {
          "items": {
            "$ref": "#/$defs/MediaProjectionItem"
          },
          "minItems": 1,
          "title": "Items",
          "type": "array"
        },
        "observation_result_sha256s": {
          "items": {
            "type": "string"
          },
          "title": "Observation Result Sha256S",
          "type": "array"
        },
        "retained_xmp_sidecars": {
          "default": [],
          "items": {
            "$ref": "#/$defs/RetainedXmpSidecar"
          },
          "title": "Retained Xmp Sidecars",
          "type": "array"
        }
      },
      "required": [
        "observation_result_sha256s",
        "items"
      ],
      "title": "MediaArchiveProjectionPayload",
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
