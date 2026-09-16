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

- <a id="s-81d0487e81"></a>`type`: `"object"`
- <a id="s-cd108065c6"></a>`additionalProperties`: `false`
- <a id="s-5e460fd14c"></a>`required`: `["profile_id","vectors"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d369d8b37"></a>`format` | no | type="string"; const="stove0-semantic-facts-conformance/v1"; default="stove0-semantic-facts-conformance/v1" |  |
| <a id="s-256cc2ad1e"></a>`profile_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-46d10403db"></a>`vectors` | yes | type="array"; items=([SemanticFactsConformanceVector](#s-ba3d4531de)); minItems=2 |  |

##### Definitions

- [ArtifactSubject](#s-6d23ba4781)
- [CollectionId](#s-684234679b)
- [CollectionRootRef](#s-a5f794652c)
- [JsonValue](#s-b251f751c9)
- [SemanticFactsConformanceVector](#s-ba3d4531de)

##### <a id="s-6d23ba4781"></a>definition `ArtifactSubject`

- <a id="s-34b6b860c4"></a>`type`: `"object"`
- <a id="s-9a794ee27c"></a>`additionalProperties`: `false`
- <a id="s-11d737c73e"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-964c6ed496"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-bfe341b697"></a>`collection` | yes | [CollectionRootRef](#s-a5f794652c) |  |
| <a id="s-d019c064ea"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-aacc7d5609"></a>`media_type` | no | anyOf=(type="string"; maxLength=255; minLength=1) \| (type="null"); default=null |  |
| <a id="s-fa142f8f17"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-637e57d40c"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-33e9174a0f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-684234679b"></a>definition `CollectionId`

- <a id="s-a8f4424f7f"></a>`type`: `"integer"`
- <a id="s-6f77ba6d66"></a>`minimum`: `1`

##### <a id="s-a5f794652c"></a>definition `CollectionRootRef`

- <a id="s-802a6c3a97"></a>`type`: `"object"`
- <a id="s-4026437c40"></a>`additionalProperties`: `false`
- <a id="s-c225541ba8"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f03bf7e5ba"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8a03a9b251"></a>`collection_id` | yes | [CollectionId](#s-684234679b) |  |
| <a id="s-2ec15dc1dc"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b251f751c9"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-ba3d4531de"></a>definition `SemanticFactsConformanceVector`

- <a id="s-1167ec7cde"></a>`type`: `"object"`
- <a id="s-be0ec6706e"></a>`additionalProperties`: `false`
- <a id="s-f9d4b6fa1c"></a>`required`: `["id","accepted","subjects","facts"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed80be656c"></a>`accepted` | yes | type="boolean" |  |
| <a id="s-7209e8b0e5"></a>`facts` | yes | type="object"; additionalProperties=([JsonValue](#s-b251f751c9)) |  |
| <a id="s-51d4047028"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-22bd0b0eeb"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-b251f751c9)) |  |
| <a id="s-0086dd4e16"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6d23ba4781)); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [canonical_vectors](stove0-observer-protocol-semanticfactsconformancevectors-canonical-vectors.md)
- [sha256](stove0-observer-protocol-semanticfactsconformancevectors-sha256.md)
- [covers_acceptance_and_rejection](stove0-observer-protocol-semanticfactsconformancevectors-covers-acceptance-and-rejection.md)

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

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
