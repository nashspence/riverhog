# stove0_observer_protocol.SemanticFactsConformanceVectors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticfactscon-2aaf5975ac:2de32a41d8 -->

Exact externally visible contract owned by this contract element.

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

- [CollectionId](#s-684234679b)
- [CollectionRootIdentityRef](#s-f85308dba1)
- [JsonValue](#s-b251f751c9)
- [SemanticFactsConformanceVector](#s-ba3d4531de)
- [WorkArtifactSubject](#s-2e2e04ae5d)

##### <a id="s-684234679b"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-65ee85af32"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-9db9501a4a"></a>2 | not=(const="0") |

##### <a id="s-f85308dba1"></a>definition `CollectionRootIdentityRef`

- <a id="s-5c2e81bbe6"></a>`type`: `"object"`
- <a id="s-2e0f62be70"></a>`additionalProperties`: `false`
- <a id="s-a68c803f27"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66a7d7dc52"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-59b393e8d0"></a>`collection_id` | yes | [CollectionId](#s-684234679b) |  |
| <a id="s-0e6bcb810b"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

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
| <a id="s-0086dd4e16"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-2e2e04ae5d)); minItems=1 |  |

##### <a id="s-2e2e04ae5d"></a>definition `WorkArtifactSubject`

- <a id="s-2fc7852aab"></a>`type`: `"object"`
- <a id="s-c45f2634cd"></a>`additionalProperties`: `false`
- <a id="s-ad793caa9e"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-13d0ce06d5"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-43f9d4dabc"></a>`collection` | yes | [CollectionRootIdentityRef](#s-f85308dba1) |  |
| <a id="s-7606604c54"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-0e374f2478"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-1ed6d008ac"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-40ecb16ab5"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4e9a2c3c53"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_vectors](stove0-observer-protocol-semanticfactsconformancevectors-canonical-vectors.md)
- [sha256](stove0-observer-protocol-semanticfactsconformancevectors-sha256.md)
- [covers_acceptance_and_rejection](stove0-observer-protocol-semanticfactsconformancevectors-covers-acceptance-and-rejection.md)

## Governing policies

- <a id="pa-d7489868b1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticFactsConformanceVectors`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e092ff70d43911c8eb8081d53038c57b18e32d9c07481de5aac9439ccf8624e5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
        },
        "CollectionRootIdentityRef": {
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
                "$ref": "#/$defs/WorkArtifactSubject"
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
        },
        "WorkArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootIdentityRef"
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
