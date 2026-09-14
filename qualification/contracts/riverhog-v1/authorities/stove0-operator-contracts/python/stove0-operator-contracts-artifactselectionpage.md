# stove0_operator_contracts.ArtifactSelectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-artifactselectionpage:cfac71f202 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b4744a6bbb"></a>
- <a id="s-6f18d8a8ef"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-681fc212d5"></a>`module`: `stove0_operator_contracts`
- <a id="s-0467b1777a"></a>`name`: `ArtifactSelectionPage`
- <a id="s-78451bb7b2"></a>`unit`: `export`

### Declared structure

- <a id="s-38150fe659"></a>`kind`: `"class"`
- <a id="s-b7ce2efa07"></a>`signature`: `"\"(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MaxLen(max_length=256)]) -> None\""`

#### Validated model schema

<a id="s-49ab52d463"></a>
- <a id="s-71784ef16b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dbd8166b32"></a>`artifacts` | yes | type="array"; maxItems=256; items=(#/$defs/ArtifactSubject); additional keys=`x-riverhog-extent` |  |
| <a id="s-c8519fd604"></a>`authority` | yes | #/$defs/ArtifactSelectionRef |  |
| <a id="s-f3985b237a"></a>`complete` | yes | type="boolean" |  |
| <a id="s-61fc05368d"></a>`continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-30a978c4b5"></a>`next_continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-612021a3f8"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-1ae91cc6c9"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-637edf639e"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-4bb5408e9a"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.ArtifactSelectionPage.bind_page](stove0-operator-contracts-artifactselectionpage-bind-page.md)

## Governing policies

- <a id="pa-8210bec093"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.ArtifactSelectionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bebe5f2fba503eabd3edb6169008cafbcc448ef2922349e24f5ca6d0a6ae9913 -->

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
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "ArtifactSelectionPage",
  "unit": "export"
}
```
