# stove0_media_metadata_observer_contracts.MediaMetadataFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-74ce689e0f:72db9f0f50 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-3cea6a534e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19d2162c5d"></a>`artifacts` | yes | type="array"; minItems=1; items=(#/$defs/MediaArtifactFacts) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-38242ad76e"></a>`JsonValue` | empty object |
| <a id="s-29b6511b52"></a>`MediaArtifactFacts` | type="object"; fields=`artifact_id`, `facts`, `state`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-245fc0465f"></a>`MediaFactEvidence` | type="object"; fields=`artifact_id`, `field`; additional keys=`additionalProperties`, `required` |
| <a id="s-2e44bce7ac"></a>`MediaMetadataFact` | type="object"; fields=`evidence`, `name`, `value`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_media_metadata_observer_contracts.MediaMetadataFacts.canonical_artifacts](stove0-media-metadata-observer-contracts-mediametadatafacts-canonical-artifacts.md)

## Governing policies

- <a id="pa-fde4d796b2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaMetadataFacts`

### Exact owned JSON

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
