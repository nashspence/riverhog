# a_stove0_media_metadata_contract_lib.MediaMetadataFact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-d0947d63df:ece283ac6b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2074623198"></a>
- <a id="s-36ba049419"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-baac64e9bb"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-c981c946a0"></a>`name`: `MediaMetadataFact`
- <a id="s-7cab20df32"></a>`unit`: `export`

### Declared structure

- <a id="s-e03378ea3d"></a>`kind`: `"class"`
- <a id="s-608e4e85ef"></a>`signature`: `"\"(*, name: Literal['capture-time', 'container-format', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, evidence: a_stove0_media_metadata_contract_lib.contracts.MediaFactEvidence) -> None\""`

#### Validated model schema

<a id="s-c8058a4669"></a>

- <a id="s-daf7b200d2"></a>`type`: `"object"`
- <a id="s-58901b3dc9"></a>`additionalProperties`: `false`
- <a id="s-04a55b65bd"></a>`required`: `["name","value","evidence"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-deee48bd0d"></a>`evidence` | yes | [MediaFactEvidence](#s-824d6ed15e) |  |
| <a id="s-9851e8ddad"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-a581b896c1"></a>`value` | yes | [JsonValue](#s-ba44752b05) |  |

##### Definitions

- [JsonValue](#s-ba44752b05)
- [MediaFactEvidence](#s-824d6ed15e)

##### <a id="s-ba44752b05"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-824d6ed15e"></a>definition `MediaFactEvidence`

- <a id="s-e9dc60ef22"></a>`type`: `"object"`
- <a id="s-6f699a63ab"></a>`additionalProperties`: `false`
- <a id="s-9fa6b888fe"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4633ec3a99"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-a9c996d3bf"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

## Governing policies

- <a id="pa-90fbc5e912"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MediaMetadataFact`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a3a933d8c02bbc80efee5edebadaac5d3956de82a80210fedd35e02f709f3c4 -->

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
    "signature": "\"(*, name: Literal['capture-time', 'container-format', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, evidence: a_stove0_media_metadata_contract_lib.contracts.MediaFactEvidence) -> None\""
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MediaMetadataFact",
  "unit": "export"
}
```

</details>
