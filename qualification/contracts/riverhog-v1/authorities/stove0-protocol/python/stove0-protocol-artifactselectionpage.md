# stove0_protocol.ArtifactSelectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselectionpage:e313fe515e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cce11045fd"></a>
- <a id="s-56d70c655b"></a>`distribution`: `stove0-protocol`
- <a id="s-03e18b6dc0"></a>`module`: `stove0_protocol`
- <a id="s-61c8bc0d62"></a>`name`: `ArtifactSelectionPage`
- <a id="s-ac3d36c57f"></a>`unit`: `export`

### Declared structure

- <a id="s-fac086ee2c"></a>`kind`: `"class"`
- <a id="s-224510dba9"></a>`signature`: `"\"(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MaxLen(max_length=256)]) -> None\""`

#### Validated model schema

<a id="s-73808f3317"></a>
- <a id="s-cb61981483"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4228ab83c9"></a>`artifacts` | yes | type="array"; maxItems=256; items=(#/$defs/ArtifactSubject); additional keys=`x-riverhog-extent` |  |
| <a id="s-a807dd959e"></a>`authority` | yes | #/$defs/ArtifactSelectionRef |  |
| <a id="s-2460fa0fde"></a>`complete` | yes | type="boolean" |  |
| <a id="s-c6f2138d13"></a>`continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-5169754d6f"></a>`next_continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-4d88ccaa17"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-d3858e980e"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-9da40006db"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-283e8f0087"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSelectionPage.bind_page](stove0-protocol-artifactselectionpage-bind-page.md)

## Governing policies

- <a id="pa-eb1a09b97a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelectionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1a9bffa4bc9f7c27bf097f7be440d5de60f0d0c70d1045ef9c38bc2529e5370 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelectionRef": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
          "type": "object"
        },
        "ArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
              "type": "string"
            },
            "media_type": {
              "anyOf": [
                {
                  "maxLength": 255,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "role",
            "collection",
            "path",
            "bytes",
            "sha256"
          ],
          "type": "object"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "CollectionRootRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "artifacts": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "maxItems": 256,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "selection-bound-start_ordinal",
            "reason": "bounded-artifact-selection-page"
          }
        },
        "authority": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        },
        "complete": {
          "type": "boolean"
        },
        "continuation": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "next_continuation": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "authority",
        "complete",
        "artifacts"
      ],
      "type": "object"
    },
    "signature": "\"(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MaxLen(max_length=256)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ArtifactSelectionPage",
  "unit": "export"
}
```
