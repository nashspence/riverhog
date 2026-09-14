# stove0_protocol.ArtifactSelection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection:f06645b268 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9819eb3e2f"></a>
- <a id="s-550d2794ad"></a>`distribution`: `stove0-protocol`
- <a id="s-12d29f7299"></a>`module`: `stove0_protocol`
- <a id="s-57b5fb5e53"></a>`name`: `ArtifactSelection`
- <a id="s-a53c8acf0d"></a>`unit`: `export`

### Declared structure

- <a id="s-643e70571f"></a>`kind`: `"class"`
- <a id="s-a6220a5ca2"></a>`signature`: `"\"(*, format: Literal['stove0-artifact-selection/v1'] = 'stove0-artifact-selection/v1', artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-906dd2a2dd"></a>
- <a id="s-adafd05867"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b2cbdf1c91"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-06c68d2d91"></a>`artifacts` | yes | type="array"; minItems=1; items=(#/$defs/ArtifactSubject) |  |
| <a id="s-6578d1a7df"></a>`format` | no | type="string"; const="stove0-artifact-selection/v1" |  |
| <a id="s-19bc8d66c9"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2c1f70cfeb"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-a833d3a7e5"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-b798222c0f"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-c384ddac04"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSelection.canonical_artifacts](stove0-protocol-artifactselection-canonical-artifacts.md)
- [stove0_protocol.ArtifactSelection.canonical_bytes](stove0-protocol-artifactselection-canonical-bytes.md)
- [stove0_protocol.ArtifactSelection.ref](stove0-protocol-artifactselection-ref.md)
- [stove0_protocol.ArtifactSelection.roots](stove0-protocol-artifactselection-roots.md)
- [stove0_protocol.ArtifactSelection.seal](stove0-protocol-artifactselection-seal.md)
- [stove0_protocol.ArtifactSelection.verify_summary_and_digest](stove0-protocol-artifactselection-verify-summary-and-digest.md)

## Governing policies

- <a id="pa-3f4d54c4f3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 70f4d67e2aacc10655c33b8988110d54952c4c4b99058369a867117dc3def602 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
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
        "artifact_count": {
          "minimum": 1,
          "type": "integer"
        },
        "artifacts": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "type": "array"
        },
        "format": {
          "const": "stove0-artifact-selection/v1",
          "default": "stove0-artifact-selection/v1",
          "type": "string"
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
        "artifacts",
        "artifact_count",
        "total_bytes",
        "selection_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-artifact-selection/v1'] = 'stove0-artifact-selection/v1', artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ArtifactSelection",
  "unit": "export"
}
```
