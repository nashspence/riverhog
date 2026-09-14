# stove0_observer_protocol.SemanticFactsConformanceVector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticfactscon-717782a348:9f2fafb6e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b914ba133"></a>
- <a id="s-0880404436"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-d5628b0355"></a>`module`: `stove0_observer_protocol`
- <a id="s-5cb0276e5c"></a>`name`: `SemanticFactsConformanceVector`
- <a id="s-bcad0aaad8"></a>`unit`: `export`

### Declared structure

- <a id="s-e96d6fa11a"></a>`kind`: `"class"`
- <a id="s-c5087eecda"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, facts: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-223115af14"></a>
- <a id="s-ebbb678253"></a>`title`: SemanticFactsConformanceVector
- <a id="s-36ae2d1405"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6587e44586"></a>`accepted` | yes | type="boolean" |  |
| <a id="s-570b8c7765"></a>`facts` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-330b5b2414"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-af3db48fdc"></a>`options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-81ad744300"></a>`subjects` | yes | type="array"; minItems=1; items=(#/$defs/ArtifactSubject) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-fbeb9e9443"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-4534c23b31"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-98f8aff7aa"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-aa856b39f4"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticFactsConformanceVector.canonical_subjects](stove0-observer-protocol-semanticfactsconformancevector-canonical-subjects.md)

## Governing policies

- <a id="pa-962f99ea2a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticFactsConformanceVector`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d558b56e90b959802ad7b3f39d59d6f9c98937e32dd1871399a3cd167476d0d -->

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
              "title": "Bytes",
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
              "title": "Id",
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
              "default": null,
              "title": "Media Type"
            },
            "path": {
              "maxLength": 4096,
              "minLength": 1,
              "title": "Path",
              "type": "string"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Role",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
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
          "title": "ArtifactSubject",
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
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "title": "CollectionRootRef",
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "accepted": {
          "title": "Accepted",
          "type": "boolean"
        },
        "facts": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Facts",
          "type": "object"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        }
      },
      "required": [
        "id",
        "accepted",
        "subjects",
        "facts"
      ],
      "title": "SemanticFactsConformanceVector",
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, facts: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticFactsConformanceVector",
  "unit": "export"
}
```
