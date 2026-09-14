# stove0_observer_protocol.SemanticFactsConformanceVectors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticfactscon-2aaf5975ac:2de32a41d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b0e56a5bc"></a>
- <a id="s-661303850d"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-e7b2c82cbd"></a>`module`: `stove0_observer_protocol`
- <a id="s-d849e7f95c"></a>`name`: `SemanticFactsConformanceVectors`
- <a id="s-67f974ee4e"></a>`unit`: `export`

### Declared structure

- <a id="s-3b647565a7"></a>`kind`: `"class"`
- <a id="s-1671e1ba9c"></a>`signature`: `"\"(*, format: Literal['stove0-semantic-facts-conformance/v1'] = 'stove0-semantic-facts-conformance/v1', profile_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], vectors: Annotated[tuple[stove0_observer_protocol.conformance.SemanticFactsConformanceVector, ...], MinLen(min_length=2)]) -> None\""`

#### Validated model schema

<a id="s-4878de70d1"></a>
- <a id="s-81d0487e81"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d369d8b37"></a>`format` | no | type="string"; const="stove0-semantic-facts-conformance/v1" |  |
| <a id="s-256cc2ad1e"></a>`profile_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-46d10403db"></a>`vectors` | yes | type="array"; minItems=2; items=(#/$defs/SemanticFactsConformanceVector) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-6d23ba4781"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-684234679b"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-a5f794652c"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-b251f751c9"></a>`JsonValue` | empty object |
| <a id="s-ba3d4531de"></a>`SemanticFactsConformanceVector` | type="object"; fields=`accepted`, `facts`, `id`, `options`, `subjects`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticFactsConformanceVectors.canonical_vectors](stove0-observer-protocol-semanticfactsconformancevectors-canonical-vectors.md)
- [stove0_observer_protocol.SemanticFactsConformanceVectors.sha256](stove0-observer-protocol-semanticfactsconformancevectors-sha256.md)
- [stove0_observer_protocol.SemanticFactsConformanceVectors.covers_acceptance_and_rejection](stove0-observer-protocol-semanticfactsconformancevectors-covers-acceptance-and-rejection.md)

## Governing policies

- <a id="pa-d7489868b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticFactsConformanceVectors`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 65b5d5d13916db1cc0b96f60552f4a559c21df90d0ec264645a00238e9e768fb -->

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
        },
        "JsonValue": {},
        "SemanticFactsConformanceVector": {
          "additionalProperties": false,
          "properties": {
            "accepted": {
              "type": "boolean"
            },
            "facts": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "subjects": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "accepted",
            "subjects",
            "facts"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-semantic-facts-conformance/v1",
          "default": "stove0-semantic-facts-conformance/v1",
          "type": "string"
        },
        "profile_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "vectors": {
          "items": {
            "$ref": "#/$defs/SemanticFactsConformanceVector"
          },
          "minItems": 2,
          "type": "array"
        }
      },
      "required": [
        "profile_id",
        "vectors"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-semantic-facts-conformance/v1'] = 'stove0-semantic-facts-conformance/v1', profile_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], vectors: Annotated[tuple[stove0_observer_protocol.conformance.SemanticFactsConformanceVector, ...], MinLen(min_length=2)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticFactsConformanceVectors",
  "unit": "export"
}
```
