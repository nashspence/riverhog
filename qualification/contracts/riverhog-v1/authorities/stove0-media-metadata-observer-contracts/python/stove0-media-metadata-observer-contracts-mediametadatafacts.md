# stove0_media_metadata_observer_contracts.MediaMetadataFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-74ce689e0f:72db9f0f50 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e06e788609"></a>
- <a id="s-1e11dc0855"></a>`distribution`: `stove0-media-metadata-observer-contracts`
- <a id="s-6d880e8fc3"></a>`module`: `stove0_media_metadata_observer_contracts`
- <a id="s-51a11ed78f"></a>`name`: `MediaMetadataFacts`
- <a id="s-3550931bce"></a>`unit`: `export`

### Declared structure

- <a id="s-6b8bce06f6"></a>`kind`: `"class"`
- <a id="s-eb586cc160"></a>`signature`: `"'(*, artifacts: Annotated[tuple[stove0_media_metadata_observer_contracts.contracts.MediaArtifactFacts, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-49f261d70b"></a>

- <a id="s-3cea6a534e"></a>`type`: `"object"`
- <a id="s-db222b5637"></a>`additionalProperties`: `false`
- <a id="s-460c5faa39"></a>`required`: `["artifacts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19d2162c5d"></a>`artifacts` | yes | type="array"; items=([MediaArtifactFacts](#s-29b6511b52)); minItems=1 |  |

##### Definitions

- [JsonValue](#s-38242ad76e)
- [MediaArtifactFacts](#s-29b6511b52)
- [MediaFactEvidence](#s-245fc0465f)
- [MediaMetadataFact](#s-2e44bce7ac)

##### <a id="s-38242ad76e"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-29b6511b52"></a>definition `MediaArtifactFacts`

- <a id="s-a016972818"></a>`type`: `"object"`
- <a id="s-8e974824f5"></a>`additionalProperties`: `false`
- <a id="s-1b89a6d78f"></a>`required`: `["artifact_id","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e1bd95140"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-b5d95b7e85"></a>`facts` | no | type="array"; default=[]; items=([MediaMetadataFact](#s-2e44bce7ac)) |  |
| <a id="s-6a87a64d15"></a>`state` | yes | type="string"; enum=["observed","unsupported"] |  |

###### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-c18ec8d121"></a>1 | properties={state: (const="unsupported")}; required=["state"] | properties={facts: (maxItems=0)} | no additional constraint |

##### <a id="s-245fc0465f"></a>definition `MediaFactEvidence`

- <a id="s-df66f57174"></a>`type`: `"object"`
- <a id="s-242f1453a5"></a>`additionalProperties`: `false`
- <a id="s-f52866def4"></a>`required`: `["artifact_id","field"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d0b9d50ea3"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-eed00bf307"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

##### <a id="s-2e44bce7ac"></a>definition `MediaMetadataFact`

- <a id="s-a5aa723bd4"></a>`type`: `"object"`
- <a id="s-7d4cf1c96d"></a>`additionalProperties`: `false`
- <a id="s-7c98d2f868"></a>`required`: `["name","value","evidence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bf32584c69"></a>`evidence` | yes | [MediaFactEvidence](#s-245fc0465f) |  |
| <a id="s-a8a4ecb4d7"></a>`name` | yes | type="string"; enum=["capture-time","container-format","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |
| <a id="s-d351008648"></a>`value` | yes | [JsonValue](#s-38242ad76e) |  |

## Maintained corroboration

### Related interface records

- [canonical_artifacts](stove0-media-metadata-observer-contracts-mediametadatafacts-canonical-artifacts.md)

## Governing policies

- <a id="pa-fde4d796b2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources/authorities.md#src-8d1649a0f1) — [some-implementations/stove0/observers/contracts/media-metadata/src/stove0\_media\_metadata\_observer\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaMetadataFacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26af0a114b5444e4e8d927f8f655311805e09f3990ed1b6b0f8a1240fa6c4f98 -->

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
    "signature": "'(*, artifacts: Annotated[tuple[stove0_media_metadata_observer_contracts.contracts.MediaArtifactFacts, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MediaMetadataFacts",
  "unit": "export"
}
```

</details>
