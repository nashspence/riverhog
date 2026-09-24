# a_stove0_media_archive_lib.MediaProjectionItem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaprojectionitem:719dc55c5a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7c59bc22a1"></a>
- <a id="s-34243cf231"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-85d248ba68"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-e3cf35fac5"></a>`name`: `MediaProjectionItem`
- <a id="s-3f720b3da5"></a>`unit`: `export`

### Declared structure

- <a id="s-a1a7a9bd49"></a>`kind`: `"class"`
- <a id="s-08d43c79f9"></a>`signature`: `"'(*, input_artifact_id: str, associated_sidecar_artifact_ids: tuple[str, ...] = (), archive_path: str, xmp_path: str, assertions: tuple[a_stove0_media_metadata_contract_lib.contracts.MediaMetadataFact, ...] = (), selected: tuple[a_stove0_media_archive_lib.projection.MediaProjectedValue, ...] = ()) -> None'"`

#### Validated model schema

<a id="s-583b7d73f3"></a>

- <a id="s-1f2857b297"></a>`type`: `"object"`
- <a id="s-a27d92423f"></a>`additionalProperties`: `false`
- <a id="s-72078c9acd"></a>`required`: `["input_artifact_id","archive_path","xmp_path"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-73586a5a16"></a>`archive_path` | yes | type="string" |  |
| <a id="s-ace79ad431"></a>`assertions` | no | type="array"; default=[]; items=([MediaMetadataFact](#s-ae69cbab31)) |  |
| <a id="s-b9aa4cb02e"></a>`associated_sidecar_artifact_ids` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-92be71e470"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-933b5d5ded"></a>`selected` | no | type="array"; default=[]; items=([MediaProjectedValue](#s-b0ac51d47b)) |  |
| <a id="s-f709c4474a"></a>`xmp_path` | yes | type="string" |  |

##### Definitions

- [JsonValue](#s-afd2b76363)
- [MediaFactEvidence](#s-3076d75998)
- [MediaMetadataFact](#s-ae69cbab31)
- [MediaProjectedValue](#s-b0ac51d47b)

##### <a id="s-afd2b76363"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-3076d75998"></a>definition `MediaFactEvidence`

- <a id="s-7e48c57dcd"></a>`type`: `"object"`
- <a id="s-6a1216647e"></a>`additionalProperties`: `false`
- <a id="s-a01b28a8a9"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d9bb9a484"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-2f97a6884b"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

##### <a id="s-ae69cbab31"></a>definition `MediaMetadataFact`

- <a id="s-93f32402b7"></a>`type`: `"object"`
- <a id="s-f4a13272fa"></a>`additionalProperties`: `false`
- <a id="s-488e5f6936"></a>`required`: `["name","value","evidence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5675a4fee5"></a>`evidence` | yes | [MediaFactEvidence](#s-3076d75998) |  |
| <a id="s-ba6fab10e0"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-e1f32ed760"></a>`value` | yes | [JsonValue](#s-afd2b76363) |  |

##### <a id="s-b0ac51d47b"></a>definition `MediaProjectedValue`

- <a id="s-69933a7847"></a>`type`: `"object"`
- <a id="s-7bdc640b11"></a>`additionalProperties`: `false`
- <a id="s-026e59d706"></a>`required`: `["name","value","source"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a16a3a481a"></a>`evidence` | no | type="array"; default=[]; items=([MediaFactEvidence](#s-3076d75998)) |  |
| <a id="s-4ce667167f"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-e91f6910c6"></a>`source` | yes | type="string"; enum=["observation","recipe"] |  |
| <a id="s-c835a5c676"></a>`value` | yes | [JsonValue](#s-afd2b76363) |  |

## Maintained corroboration

### Related interface records

- [canonical_sidecars](a-stove0-media-archive-lib-mediaprojectionitem-canonical-sidecars.md)
- [canonical_path](a-stove0-media-archive-lib-mediaprojectionitem-canonical-path.md)
- [canonical_evidence](a-stove0-media-archive-lib-mediaprojectionitem-canonical-evidence.md)
- [derived_from](a-stove0-media-archive-lib-mediaprojectionitem-derived-from.md)

## Governing policies

- <a id="pa-ba55519cc5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaProjectionItem`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 299686a70a897126d9c9aebf878586a684ad9fdce4514556d2ab416aaefcabb5 -->

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
        }
      },
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
    "signature": "'(*, input_artifact_id: str, associated_sidecar_artifact_ids: tuple[str, ...] = (), archive_path: str, xmp_path: str, assertions: tuple[a_stove0_media_metadata_contract_lib.contracts.MediaMetadataFact, ...] = (), selected: tuple[a_stove0_media_archive_lib.projection.MediaProjectedValue, ...] = ()) -> None'"
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "MediaProjectionItem",
  "unit": "export"
}
```

</details>
