# stove0_media_archive_target_support.MediaArchiveProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-14aa64971e:30c961b4c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b08e612f85"></a>
- <a id="s-eef714bddf"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-b257ccf303"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-493ebd5dd6"></a>`name`: `MediaArchiveProjection`
- <a id="s-7b45bd41b1"></a>`unit`: `export`

### Declared structure

- <a id="s-403a9d3512"></a>`kind`: `"class"`
- <a id="s-eb460d2374"></a>`signature`: `"\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[stove0_media_archive_target_support.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[stove0_media_archive_target_support.projection.RetainedXmpSidecar, ...] = (), projection_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-72887283f3"></a>
- <a id="s-e457100582"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e603cb1db"></a>`format` | no | type="string"; const="stove0-media-archive-projection/v1" |  |
| <a id="s-ad5e552a81"></a>`items` | yes | type="array"; minItems=1; items=(#/$defs/MediaProjectionItem) |  |
| <a id="s-360e960a8d"></a>`observation_result_sha256s` | yes | type="array"; items=(type="string") |  |
| <a id="s-4706233975"></a>`projection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-25c21e858d"></a>`retained_xmp_sidecars` | no | type="array"; items=(#/$defs/RetainedXmpSidecar) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-0831fde7f9"></a>`JsonValue` | empty object |
| <a id="s-c76f05477f"></a>`MediaFactEvidence` | type="object"; fields=`artifact_id`, `field`; additional keys=`additionalProperties`, `required` |
| <a id="s-6057e2e7f2"></a>`MediaMetadataFact` | type="object"; fields=`evidence`, `name`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-055f4b3903"></a>`MediaProjectedValue` | type="object"; fields=`evidence`, `name`, `source`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-5d1e382904"></a>`MediaProjectionItem` | type="object"; fields=`archive_path`, `assertions`, `associated_sidecar_artifact_ids`, `input_artifact_id`, `selected`, `xmp_path`; additional keys=`additionalProperties`, `required` |
| <a id="s-12e210e016"></a>`RetainedXmpSidecar` | type="object"; fields=`input_artifact_id`, `output_path`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [item_for](stove0-media-archive-target-support-mediaarchiveprojection-item-for.md)
- [verify_digest](stove0-media-archive-target-support-mediaarchiveprojection-verify-digest.md)
- [validate_plan_evidence](stove0-media-archive-target-support-mediaarchiveprojection-validate-plan-evidence.md)
- [canonical_members](stove0-media-archive-target-support-mediaarchiveprojection-canonical-members.md)
- [seal](stove0-media-archive-target-support-mediaarchiveprojection-seal.md)

## Governing policies

- <a id="pa-284debfb35"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f02ebdec3db7d3f5da2faaa55e774aa0d6c3a913fc192f8d0b83365304acd048 -->

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
    "signature": "\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[stove0_media_archive_target_support.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[stove0_media_archive_target_support.projection.RetainedXmpSidecar, ...] = (), projection_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "MediaArchiveProjection",
  "unit": "export"
}
```
