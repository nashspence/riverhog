# a_stove0_media_metadata_contract_lib.MediaMetadataFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-a34c5effb0:f2d1ead75d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-94caf3b65a"></a>
- <a id="s-a0a9aa0076"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-cf7bd028a1"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-f41ad0c959"></a>`name`: `MediaMetadataFacts`
- <a id="s-9dfaec2c26"></a>`unit`: `export`

### Declared structure

- <a id="s-d6d4cb1c32"></a>`kind`: `"class"`
- <a id="s-5128496019"></a>`signature`: `"'(*, artifacts: Annotated[tuple[a_stove0_media_metadata_contract_lib.contracts.MediaArtifactFacts, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-162310a412"></a>

- <a id="s-61d2194464"></a>`type`: `"object"`
- <a id="s-d1b31bc3c7"></a>`additionalProperties`: `false`
- <a id="s-c4800ea9d8"></a>`required`: `["artifacts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce16455785"></a>`artifacts` | yes | type="array"; items=([MediaArtifactFacts](#s-0e3051380d)); minItems=1 |  |

##### Definitions

- [JsonValue](#s-a1167bbddf)
- [MediaArtifactFacts](#s-0e3051380d)
- [MediaFactEvidence](#s-5b6748571d)
- [MediaMetadataFact](#s-08a5913dc2)

##### <a id="s-a1167bbddf"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-0e3051380d"></a>definition `MediaArtifactFacts`

- <a id="s-7f08e0cee3"></a>`type`: `"object"`
- <a id="s-989c1106b7"></a>`additionalProperties`: `false`
- <a id="s-1355d1e98a"></a>`required`: `["artifact_id","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-302640fe47"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-10561f5060"></a>`facts` | no | type="array"; default=[]; items=([MediaMetadataFact](#s-08a5913dc2)) |  |
| <a id="s-a64f1cd07e"></a>`state` | yes | type="string"; enum=["observed","unsupported"] |  |

###### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-8d0460c42b"></a>1 | properties={state: (const="unsupported")}; required=["state"] | properties={facts: (maxItems=0)} | no additional constraint |

##### <a id="s-5b6748571d"></a>definition `MediaFactEvidence`

- <a id="s-e333f8e42e"></a>`type`: `"object"`
- <a id="s-4bd072ac29"></a>`additionalProperties`: `false`
- <a id="s-c98b80f24b"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-938eb0ca62"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-1dc186243a"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

##### <a id="s-08a5913dc2"></a>definition `MediaMetadataFact`

- <a id="s-e5553a5b22"></a>`type`: `"object"`
- <a id="s-6dd48d671d"></a>`additionalProperties`: `false`
- <a id="s-4d7fe55a65"></a>`required`: `["name","value","evidence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0100d5d854"></a>`evidence` | yes | [MediaFactEvidence](#s-5b6748571d) |  |
| <a id="s-b6a045b3ca"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-87dbf3190f"></a>`value` | yes | [JsonValue](#s-a1167bbddf) |  |

## Maintained corroboration

### Related interface records

- [canonical_artifacts](a-stove0-media-metadata-contract-lib-mediametadatafacts-canonical-artifacts.md)

## Governing policies

- <a id="pa-a3296ff84c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MediaMetadataFacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90c3a5298d447c552ea7e0429b257b947dfa5674433ed0e90cf5a2870946be15 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {},
        "MediaArtifactFacts": {
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
      "properties": {
        "artifacts": {
          "items": {
            "$ref": "#/$defs/MediaArtifactFacts"
          },
          "minItems": 1,
          "type": "array"
        }
      },
      "required": [
        "artifacts"
      ],
      "type": "object"
    },
    "signature": "'(*, artifacts: Annotated[tuple[a_stove0_media_metadata_contract_lib.contracts.MediaArtifactFacts, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MediaMetadataFacts",
  "unit": "export"
}
```

</details>
