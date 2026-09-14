# stove0_target_protocol.TargetInputPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinputpage:b5c9613dd6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-412205faec"></a>
- <a id="s-22e6c8749a"></a>`distribution`: `stove0-target-protocol`
- <a id="s-d46c396912"></a>`module`: `stove0_target_protocol`
- <a id="s-640b511f35"></a>`name`: `TargetInputPage`
- <a id="s-d8bd36b213"></a>`unit`: `export`

### Declared structure

- <a id="s-d39bf980e6"></a>`kind`: `"class"`
- <a id="s-237a789a19"></a>`signature`: `"\"(*, authority: stove0_target_protocol.protocol.TargetInputAuthority, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_target_protocol.protocol.InputArtifact, ...], MaxLen(max_length=256)]) -> None\""`

#### Validated model schema

<a id="s-659cff9b19"></a>
- <a id="s-e81a4de6fc"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e84c58d54"></a>`artifacts` | yes | type="array"; maxItems=256; items=(#/$defs/InputArtifact); additional keys=`x-riverhog-extent` |  |
| <a id="s-5b256c5ba8"></a>`authority` | yes | #/$defs/TargetInputAuthority |  |
| <a id="s-edb25598f6"></a>`complete` | yes | type="boolean" |  |
| <a id="s-538e7ce942"></a>`continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-a0dbd706b2"></a>`next_continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-2a796fd68b"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-aacc57599e"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-15713f5133"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-b712976709"></a>`InputArtifact` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-19d884a805"></a>`TargetInputAuthority` | type="object"; fields=`roles`, `selection`; additional keys=`additionalProperties`, `required` |
| <a id="s-35383094e0"></a>`TargetInputRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetInputPage.bind_page](stove0-target-protocol-targetinputpage-bind-page.md)

## Governing policies

- <a id="pa-bb4fb25791"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bbafc318c502cc739a6da17f1739021ddea63e6f348d731d3197183410c52c2 -->

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
        },
        "InputArtifact": {
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
        "TargetInputAuthority": {
          "additionalProperties": false,
          "properties": {
            "roles": {
              "items": {
                "$ref": "#/$defs/TargetInputRoleCount"
              },
              "minItems": 1,
              "type": "array"
            },
            "selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            }
          },
          "required": [
            "selection",
            "roles"
          ],
          "type": "object"
        },
        "TargetInputRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "artifacts": {
          "items": {
            "$ref": "#/$defs/InputArtifact"
          },
          "maxItems": 256,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "authority-bound-start_ordinal",
            "reason": "bounded-target-input-page"
          }
        },
        "authority": {
          "$ref": "#/$defs/TargetInputAuthority"
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
    "signature": "\"(*, authority: stove0_target_protocol.protocol.TargetInputAuthority, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_target_protocol.protocol.InputArtifact, ...], MaxLen(max_length=256)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetInputPage",
  "unit": "export"
}
```
