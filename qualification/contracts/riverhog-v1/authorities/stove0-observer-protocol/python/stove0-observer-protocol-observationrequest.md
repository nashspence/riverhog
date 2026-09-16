# stove0_observer_protocol.ObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationrequest:9cc85c2cbf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f3f829083"></a>
- <a id="s-c81d943196"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-0bf9d73c34"></a>`module`: `stove0_observer_protocol`
- <a id="s-9c48fbcc62"></a>`name`: `ObservationRequest`
- <a id="s-98f584771c"></a>`unit`: `export`

### Declared structure

- <a id="s-4a55471211"></a>`kind`: `"class"`
- <a id="s-4c685a412e"></a>`signature`: `"\"(*, format: Literal['stove0-observation-request/v1'] = 'stove0-observation-request/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], observer_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-568e09dd9d"></a>

- <a id="s-562246eac4"></a>`type`: `"object"`
- <a id="s-cfffd27456"></a>`additionalProperties`: `false`
- <a id="s-160a3956dc"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b1bac5e98"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-dab7f42ce2"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-b0c023310c"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9ffc9659a3"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c8131a7271"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1d952552f2"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-ea8263e4f1"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-0722eb34f8)) |  |
| <a id="s-2e1d66fd22"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7cc3e44a9c"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-28ad05e32f"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-5c76f92438)); minItems=1 |  |
| <a id="s-61f0e76a1d"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-7beb75f206"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSubject](#s-5c76f92438)
- [CollectionId](#s-93e49f591e)
- [CollectionRootRef](#s-812d3fa53a)
- [JsonValue](#s-0722eb34f8)

##### <a id="s-5c76f92438"></a>definition `ArtifactSubject`

- <a id="s-49531232dd"></a>`type`: `"object"`
- <a id="s-b61e6f318d"></a>`additionalProperties`: `false`
- <a id="s-713e3b50e8"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-331a9f8113"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-1fbb1ace15"></a>`collection` | yes | [CollectionRootRef](#s-812d3fa53a) |  |
| <a id="s-ff2effa78e"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-b7983576ec"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-1c2dfa7282"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-c722c42028"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0777e4064d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-93e49f591e"></a>definition `CollectionId`

- <a id="s-702a0bce07"></a>`type`: `"integer"`
- <a id="s-4e94b32d98"></a>`minimum`: `1`

##### <a id="s-812d3fa53a"></a>definition `CollectionRootRef`

- <a id="s-26832ae38b"></a>`type`: `"object"`
- <a id="s-84b93f8e1c"></a>`additionalProperties`: `false`
- <a id="s-26d5188160"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e77b024961"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-596f79566b"></a>`collection_id` | yes | [CollectionId](#s-93e49f591e) |  |
| <a id="s-da5d90af93"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0722eb34f8"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [canonical_subjects](stove0-observer-protocol-observationrequest-canonical-subjects.md)
- [verify_digest](stove0-observer-protocol-observationrequest-verify-digest.md)
- [seal](stove0-observer-protocol-observationrequest-seal.md)

## Governing policies

- <a id="pa-a268de4f74"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 529c4544b60992715ae76e85ae1b4519c2a9a89c7199fa2ef973ee78cb37acd1 -->

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
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-observation-request/v1",
          "default": "stove0-observation-request/v1",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "type": "integer"
        },
        "observer_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "observer_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "observer_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "type": "string"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "type": "array"
        },
        "timeout_seconds": {
          "default": 300,
          "maximum": 86400,
          "minimum": 1,
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "observer_registration_id",
        "observer_descriptor_sha256",
        "observer_contract_id",
        "observer_contract_sha256",
        "subjects",
        "request_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-observation-request/v1'] = 'stove0-observation-request/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], observer_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationRequest",
  "unit": "export"
}
```

</details>
