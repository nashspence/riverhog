# stove0_observer_protocol.SemanticFactsConformanceVector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticfactscon-717782a348:9f2fafb6e4 -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-36ae2d1405"></a>`type`: `"object"`
- <a id="s-14cb32acbf"></a>`additionalProperties`: `false`
- <a id="s-1ebec0b38f"></a>`required`: `["id","accepted","subjects","facts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6587e44586"></a>`accepted` | yes | type="boolean" |  |
| <a id="s-570b8c7765"></a>`facts` | yes | type="object"; additionalProperties=([JsonValue](#s-aa856b39f4)) |  |
| <a id="s-330b5b2414"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-af3db48fdc"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-aa856b39f4)) |  |
| <a id="s-81ad744300"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-fbeb9e9443)); minItems=1 |  |

##### Definitions

- [ArtifactSubject](#s-fbeb9e9443)
- [CollectionId](#s-4534c23b31)
- [CollectionRootRef](#s-98f8aff7aa)
- [JsonValue](#s-aa856b39f4)

##### <a id="s-fbeb9e9443"></a>definition `ArtifactSubject`

- <a id="s-701e1ff8d9"></a>`type`: `"object"`
- <a id="s-63e943cd8d"></a>`additionalProperties`: `false`
- <a id="s-75527610b8"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-62a1e82760"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-11ffac9914"></a>`collection` | yes | [CollectionRootRef](#s-98f8aff7aa) |  |
| <a id="s-489e06121f"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-1a4a8f48fb"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-f09346fa84"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-97975ba9bc"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6ee698a7c2"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4534c23b31"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-340e54b8ec"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-709c0bf6a0"></a>2 | not=(const="0") |

##### <a id="s-98f8aff7aa"></a>definition `CollectionRootRef`

- <a id="s-4fc689cae0"></a>`type`: `"object"`
- <a id="s-24595c5f5a"></a>`additionalProperties`: `false`
- <a id="s-d2eaa4a03c"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa53d4a236"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-334217abdc"></a>`collection_id` | yes | [CollectionId](#s-4534c23b31) |  |
| <a id="s-d8300512c8"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-aa856b39f4"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [canonical_subjects](stove0-observer-protocol-semanticfactsconformancevector-canonical-subjects.md)

## Governing policies

- <a id="pa-962f99ea2a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticFactsConformanceVector`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 198aa50fea58a47e7371c390e5890f3b137ebe8bcf27432a51469cda85bd094f -->

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
        "JsonValue": {}
      },
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
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, facts: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticFactsConformanceVector",
  "unit": "export"
}
```

</details>
