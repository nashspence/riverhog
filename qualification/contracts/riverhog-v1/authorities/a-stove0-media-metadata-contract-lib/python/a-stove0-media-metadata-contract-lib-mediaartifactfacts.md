# a_stove0_media_metadata_contract_lib.MediaArtifactFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-2f4d1153a2:83207977d8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bcd9327e0a"></a>
- <a id="s-27dce7c549"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-4c8b8d714a"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-4987a2b45a"></a>`name`: `MediaArtifactFacts`
- <a id="s-fb7e363cff"></a>`unit`: `export`

### Declared structure

- <a id="s-f8b2fe9f0b"></a>`kind`: `"class"`
- <a id="s-486fb25b8c"></a>`signature`: `"\"(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['observed', 'unsupported'], facts: tuple[a_stove0_media_metadata_contract_lib.contracts.MediaMetadataFact, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-fdc3562bc1"></a>

- <a id="s-0acedc3f54"></a>`type`: `"object"`
- <a id="s-ef85ba742a"></a>`additionalProperties`: `false`
- <a id="s-6324e8d52a"></a>`required`: `["artifact_id","state"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b257c142a"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-3bd0c8e09d"></a>`facts` | no | type="array"; default=[]; items=([MediaMetadataFact](#s-bfea879bd4)) |  |
| <a id="s-d354640447"></a>`state` | yes | type="string"; enum=["observed","unsupported"] |  |

##### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-4fe3e4020e"></a>1 | properties={state: (const="unsupported")}; required=["state"] | properties={facts: (maxItems=0)} | no additional constraint |

##### Definitions

- [JsonValue](#s-a351879f45)
- [MediaFactEvidence](#s-d2df346d47)
- [MediaMetadataFact](#s-bfea879bd4)

##### <a id="s-a351879f45"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-d2df346d47"></a>definition `MediaFactEvidence`

- <a id="s-8f4dd34a16"></a>`type`: `"object"`
- <a id="s-201499e96d"></a>`additionalProperties`: `false`
- <a id="s-c82fe187ca"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8074ceb68a"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-2f8b834fd1"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

##### <a id="s-bfea879bd4"></a>definition `MediaMetadataFact`

- <a id="s-75983747c5"></a>`type`: `"object"`
- <a id="s-3355338ca6"></a>`additionalProperties`: `false`
- <a id="s-1884dbfc89"></a>`required`: `["name","value","evidence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-043eb96b87"></a>`evidence` | yes | [MediaFactEvidence](#s-d2df346d47) |  |
| <a id="s-64138e93b6"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-cfeff07c9c"></a>`value` | yes | [JsonValue](#s-a351879f45) |  |

## Maintained corroboration

### Related interface records

- [valid_state](a-stove0-media-metadata-contract-lib-mediaartifactfacts-valid-state.md)

## Governing policies

- <a id="pa-76d71b84a1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MediaArtifactFacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 624a2c9ea79ff103f5e09c797c5ff9e98e32229971db9844d1e535b002d30225 -->

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
        }
      },
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "state": {
                "const": "unsupported"
              }
            },
            "required": [
              "state"
            ]
          },
          "then": {
            "properties": {
              "facts": {
                "maxItems": 0
              }
            }
          }
        }
      ],
      "properties": {
        "artifact_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "facts": {
          "default": [],
          "items": {
            "$ref": "#/$defs/MediaMetadataFact"
          },
          "type": "array"
        },
        "state": {
          "enum": [
            "observed",
            "unsupported"
          ],
          "type": "string"
        }
      },
      "required": [
        "artifact_id",
        "state"
      ],
      "type": "object"
    },
    "signature": "\"(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['observed', 'unsupported'], facts: tuple[a_stove0_media_metadata_contract_lib.contracts.MediaMetadataFact, ...] = ()) -> None\""
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MediaArtifactFacts",
  "unit": "export"
}
```

</details>
