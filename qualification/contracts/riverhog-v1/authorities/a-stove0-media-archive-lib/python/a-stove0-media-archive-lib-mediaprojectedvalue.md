# a_stove0_media_archive_lib.MediaProjectedValue

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaprojectedvalue:7e201416b0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63ac23830a"></a>
- <a id="s-afe8b1c2d6"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-b68ebc9d3d"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-6eb22e19c6"></a>`name`: `MediaProjectedValue`
- <a id="s-15811062f7"></a>`unit`: `export`

### Declared structure

- <a id="s-b42798089b"></a>`kind`: `"class"`
- <a id="s-3b027d19c0"></a>`signature`: `"\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, source: Literal['observation', 'recipe'], evidence: tuple[a_stove0_media_metadata_contract_lib.contracts.MediaFactEvidence, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-96663b291a"></a>

- <a id="s-35f952a7f9"></a>`type`: `"object"`
- <a id="s-235173edde"></a>`additionalProperties`: `false`
- <a id="s-237b09c455"></a>`required`: `["name","value","source"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3dd417c342"></a>`evidence` | no | type="array"; default=[]; items=([MediaFactEvidence](#s-54d11913c0)) |  |
| <a id="s-f61880ac45"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-c1218e7577"></a>`source` | yes | type="string"; enum=["observation","recipe"] |  |
| <a id="s-0e08bd1f10"></a>`value` | yes | [JsonValue](#s-93acff6171) |  |

##### Definitions

- [JsonValue](#s-93acff6171)
- [MediaFactEvidence](#s-54d11913c0)

##### <a id="s-93acff6171"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-54d11913c0"></a>definition `MediaFactEvidence`

- <a id="s-522ec1a728"></a>`type`: `"object"`
- <a id="s-51c9f6a283"></a>`additionalProperties`: `false`
- <a id="s-5277dc0bc1"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-33af953064"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-043bfae5bd"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [bind_source](a-stove0-media-archive-lib-mediaprojectedvalue-bind-source.md)
- [canonical_evidence](a-stove0-media-archive-lib-mediaprojectedvalue-canonical-evidence.md)

## Governing policies

- <a id="pa-4542256a8b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaProjectedValue`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55c0a0cb9588d17102c5a96aebff56d670c55d164696673ea42193fe3ad54939 -->

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
        }
      },
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
    "signature": "\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, source: Literal['observation', 'recipe'], evidence: tuple[a_stove0_media_metadata_contract_lib.contracts.MediaFactEvidence, ...] = ()) -> None\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "MediaProjectedValue",
  "unit": "export"
}
```

</details>
