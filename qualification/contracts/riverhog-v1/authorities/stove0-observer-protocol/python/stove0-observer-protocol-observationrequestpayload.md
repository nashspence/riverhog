# stove0_observer_protocol.ObservationRequestPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationrequestpayload:4e73f594e0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cb4ad0899b"></a>
- <a id="s-99095dbcbe"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-f9653f72d7"></a>`module`: `stove0_observer_protocol`
- <a id="s-cb5d9880d2"></a>`name`: `ObservationRequestPayload`
- <a id="s-ed66330f67"></a>`unit`: `export`

### Declared structure

- <a id="s-6fd5de63fc"></a>`kind`: `"class"`
- <a id="s-1049a365cb"></a>`signature`: `"\"(*, format: Literal['stove0-observation-request/v1'] = 'stove0-observation-request/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], observer_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""`

#### Validated model schema

<a id="s-fcd3c8d531"></a>

- <a id="s-f8f1727e6a"></a>`type`: `"object"`
- <a id="s-a678e75edf"></a>`additionalProperties`: `false`
- <a id="s-c4ff3d00e9"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ebf1352eb5"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-47f15e6367"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-b8128f6cc4"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3564c24862"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bf4cbb5394"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6f308432bd"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-9650fd67a3"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-f0b0210f5a)) |  |
| <a id="s-30ee01f17c"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-464c3e04cf"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ec42b20846)); minItems=1 |  |
| <a id="s-017271c335"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-b9c8abee16"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSubject](#s-ec42b20846)
- [CollectionId](#s-59a94e1980)
- [CollectionRootRef](#s-f089104b8c)
- [JsonValue](#s-f0b0210f5a)

##### <a id="s-ec42b20846"></a>definition `ArtifactSubject`

- <a id="s-a09121712f"></a>`type`: `"object"`
- <a id="s-a8e72840ec"></a>`additionalProperties`: `false`
- <a id="s-dd494e4d11"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e66b4219e0"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-89aff51a8f"></a>`collection` | yes | [CollectionRootRef](#s-f089104b8c) |  |
| <a id="s-f36a716a97"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-d9bdb5b3a1"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-e39fc3b897"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-e25fb6c472"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ed8b3f0527"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-59a94e1980"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-6d4cdde696"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-5610c72579"></a>2 | not=(const="0") |

##### <a id="s-f089104b8c"></a>definition `CollectionRootRef`

- <a id="s-1bd623355f"></a>`type`: `"object"`
- <a id="s-c6e2c7d72e"></a>`additionalProperties`: `false`
- <a id="s-f104d113a5"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-45a142eb05"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bd2ff7595c"></a>`collection_id` | yes | [CollectionId](#s-59a94e1980) |  |
| <a id="s-bc4dcbb73b"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f0b0210f5a"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [canonical_subjects](stove0-observer-protocol-observationrequestpayload-canonical-subjects.md)

## Governing policies

- <a id="pa-0c49d93dff"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationRequestPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44c75c56bc6e54c982a2eafd95d27eac76df6997dbebac69f750dd13290c1c8a -->

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
        "subjects"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-observation-request/v1'] = 'stove0-observation-request/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], observer_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationRequestPayload",
  "unit": "export"
}
```

</details>
