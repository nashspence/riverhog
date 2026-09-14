# stove0_media_metadata_observer_contracts.MediaArtifactFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-491157120e:25442dc6f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-21fca26a4b"></a>
- <a id="s-1c307a5c8f"></a>`distribution`: `stove0-media-metadata-observer-contracts`
- <a id="s-e3746e4d8f"></a>`module`: `stove0_media_metadata_observer_contracts`
- <a id="s-34bc7b1003"></a>`name`: `MediaArtifactFacts`
- <a id="s-d6100014d2"></a>`unit`: `export`

### Declared structure

- <a id="s-a329ca9abd"></a>`kind`: `"class"`
- <a id="s-2b23ccdc62"></a>`signature`: `"\"(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['observed', 'unsupported'], facts: tuple[stove0_media_metadata_observer_contracts.contracts.MediaMetadataFact, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-bfe04bf52f"></a>
- <a id="s-69659dd347"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3c6dd9b0ec"></a>`artifact_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-5d0e028eda"></a>`facts` | no | type="array"; items=(#/$defs/MediaMetadataFact) |  |
| <a id="s-64fd95ee2e"></a>`state` | yes | type="string"; enum=["observed","unsupported"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-128351c62f"></a>`JsonValue` | empty object |
| <a id="s-957f003757"></a>`MediaFactEvidence` | type="object"; fields=`artifact_id`, `field`; additional keys=`additionalProperties`, `required` |
| <a id="s-9288049ca6"></a>`MediaMetadataFact` | type="object"; fields=`evidence`, `name`, `value`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_media_metadata_observer_contracts.MediaArtifactFacts.valid_state](stove0-media-metadata-observer-contracts-mediaartifactfacts-valid-state.md)

## Governing policies

- <a id="pa-c4153c653a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaArtifactFacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17faf89cec86223fe34c98c8e979eb53d6de5a01d0cdc43ee9344c36389c753a -->

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
    "signature": "\"(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['observed', 'unsupported'], facts: tuple[stove0_media_metadata_observer_contracts.contracts.MediaMetadataFact, ...] = ()) -> None\""
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MediaArtifactFacts",
  "unit": "export"
}
```
