# stove0_observer_protocol.ObservationEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationevidence:ea40311ee4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92a28a28e1"></a>
- <a id="s-7352ffa93b"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-fad3bbfd0e"></a>`module`: `stove0_observer_protocol`
- <a id="s-f154ce5c29"></a>`name`: `ObservationEvidence`
- <a id="s-3eb8202b3e"></a>`unit`: `export`

### Declared structure

- <a id="s-44c10aa66c"></a>`kind`: `"class"`
- <a id="s-f7db2fe5d5"></a>`signature`: `"'(*, request: stove0_protocol.models.ObservationRequest, result: stove0_protocol.models.ObservationResult) -> None'"`

#### Validated model schema

<a id="s-be721cf923"></a>

- <a id="s-aeb0dab560"></a>`type`: `"object"`
- <a id="s-fa245623a7"></a>`additionalProperties`: `false`
- <a id="s-b5f2115a9a"></a>`required`: `["request","result"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-85f9eba258"></a>`request` | yes | [ObservationRequest](#s-4f64968632) |  |
| <a id="s-323e57e3d3"></a>`result` | yes | [ObservationResult](#s-05e814edd0) |  |

##### Definitions

- [ArtifactSubject](#s-e68bd8a483)
- [CollectionId](#s-99e3d0ef11)
- [CollectionRootRef](#s-6a61faf8ff)
- [JsonSchemaDocument](#s-d6da873e1b)
- [JsonValue](#s-e7b008bb19)
- [ObservationFailure](#s-e958c0756e)
- [ObservationInapplicable](#s-033d68226a)
- [ObservationRequest](#s-4f64968632)
- [ObservationResult](#s-05e814edd0)
- [ObserverImplementation](#s-5140dfe6b5)

##### <a id="s-e68bd8a483"></a>definition `ArtifactSubject`

- <a id="s-338eec2aff"></a>`type`: `"object"`
- <a id="s-121569ad38"></a>`additionalProperties`: `false`
- <a id="s-1234aa515e"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9abd34eb67"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-3d8d2906d5"></a>`collection` | yes | [CollectionRootRef](#s-6a61faf8ff) |  |
| <a id="s-571656444e"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-5e22bda161"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-56768c51cc"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-bc59dddead"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-449993e83b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-99e3d0ef11"></a>definition `CollectionId`

- <a id="s-75c570fe42"></a>`type`: `"integer"`
- <a id="s-15d197a7bb"></a>`minimum`: `1`

##### <a id="s-6a61faf8ff"></a>definition `CollectionRootRef`

- <a id="s-fc0c031cfb"></a>`type`: `"object"`
- <a id="s-7a228d4b72"></a>`additionalProperties`: `false`
- <a id="s-72c2614eba"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdb90aa8c0"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ea31d30a3c"></a>`collection_id` | yes | [CollectionId](#s-99e3d0ef11) |  |
| <a id="s-d3c2d2b696"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d6da873e1b"></a>definition `JsonSchemaDocument`

- <a id="s-b819935b1a"></a>`type`: `"object"`
- <a id="s-f2993e3f28"></a>`additionalProperties`: `false`
- <a id="s-ec5ce2b138"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e66bddbaa7"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-b2ce96dca7"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-966a9aaf3b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5b17072c7e"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-e7b008bb19)) |  |
| <a id="s-24b4b92435"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e7b008bb19"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-e958c0756e"></a>definition `ObservationFailure`

- <a id="s-fb4c88ea28"></a>`type`: `"object"`
- <a id="s-fe6861ee2c"></a>`additionalProperties`: `false`
- <a id="s-240ce389e0"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ce5c3ec24"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0b4b5a290b"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-3a30d221f5"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-033d68226a"></a>definition `ObservationInapplicable`

- <a id="s-1e70c41431"></a>`type`: `"object"`
- <a id="s-5b3f814a5a"></a>`additionalProperties`: `false`
- <a id="s-eacb0b604c"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d4d3c5e70"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-995f183909"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-4f64968632"></a>definition `ObservationRequest`

- <a id="s-7977502b78"></a>`type`: `"object"`
- <a id="s-aba7ea158e"></a>`additionalProperties`: `false`
- <a id="s-e0501dd046"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a9d30e9017"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-51825c4cda"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-f9d2f4a87c"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a2ec5ce2d4"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a00765c85b"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6f5e0c1538"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-f4bb8aafbb"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-e7b008bb19)) |  |
| <a id="s-e63a659593"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-269ceb10b3"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-2ec3625966"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-e68bd8a483)); minItems=1 |  |
| <a id="s-b029b5bc1a"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-d20cdd787c"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-05e814edd0"></a>definition `ObservationResult`

- <a id="s-a706f88977"></a>`type`: `"object"`
- <a id="s-2eef28e556"></a>`additionalProperties`: `false`
- <a id="s-36077ed96f"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-48169adb07"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-e7b008bb19)) |  |
| <a id="s-b44ad222b3"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-e7b008bb19))); (type="null")]; default=null |  |
| <a id="s-66d7349ec8"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-d6da873e1b)); (type="null")]; default=null |  |
| <a id="s-cf952b3a5e"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-58d9853676"></a>`failure` | no | anyOf=[([ObservationFailure](#s-e958c0756e)); (type="null")]; default=null |  |
| <a id="s-f3f52d5c1d"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-3b0c3d2581"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-033d68226a)); (type="null")]; default=null |  |
| <a id="s-a3f211872c"></a>`observer` | yes | [ObserverImplementation](#s-5140dfe6b5) |  |
| <a id="s-57d7b1def3"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f77c52657a"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-699a625959"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d13d6f0f61"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f700729b8a"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-f2ecc2092f"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-e68bd8a483)); minItems=1 |  |

##### <a id="s-5140dfe6b5"></a>definition `ObserverImplementation`

- <a id="s-03a1f70b20"></a>`type`: `"object"`
- <a id="s-9305fd86b2"></a>`additionalProperties`: `false`
- <a id="s-8425ce6091"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f709218fab"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4d0fa5a1f5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0067fe9c7e"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-21698bef31"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-e9d698bfca"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [bind_result](stove0-observer-protocol-observationevidence-bind-result.md)

## Governing policies

- <a id="pa-4805da7b58"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a27f3a040c5c00d5f9b5e3071720607fdd7e320a4f705ccbdc86efae273c43a -->

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
        "JsonSchemaDocument": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "ObservationFailure": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "retryable": {
              "type": "boolean"
            }
          },
          "required": [
            "code",
            "message",
            "retryable"
          ],
          "type": "object"
        },
        "ObservationInapplicable": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "type": "object"
        },
        "ObservationRequest": {
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
        "ObservationResult": {
          "additionalProperties": false,
          "properties": {
            "execution_evidence": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "facts": {
              "anyOf": [
                {
                  "additionalProperties": {
                    "$ref": "#/$defs/JsonValue"
                  },
                  "type": "object"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "facts_schema": {
              "anyOf": [
                {
                  "$ref": "#/$defs/JsonSchemaDocument"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "facts_sha256": {
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
            "failure": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ObservationFailure"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "format": {
              "const": "stove0-observation-result/v1",
              "default": "stove0-observation-result/v1",
              "type": "string"
            },
            "inapplicable": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ObservationInapplicable"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "observer": {
              "$ref": "#/$defs/ObserverImplementation"
            },
            "observer_contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "observer_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "request_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "result_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "state": {
              "enum": [
                "observed",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
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
            "request_id",
            "state",
            "observer",
            "observer_contract_id",
            "observer_contract_sha256",
            "subjects",
            "result_sha256"
          ],
          "type": "object"
        },
        "ObserverImplementation": {
          "additionalProperties": false,
          "properties": {
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-content-observer/v1",
              "default": "stove0-content-observer/v1",
              "type": "string"
            },
            "source_revision": {
              "maxLength": 200,
              "minLength": 1,
              "type": "string"
            },
            "version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "id",
            "version",
            "source_revision",
            "descriptor_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "request": {
          "$ref": "#/$defs/ObservationRequest"
        },
        "result": {
          "$ref": "#/$defs/ObservationResult"
        }
      },
      "required": [
        "request",
        "result"
      ],
      "type": "object"
    },
    "signature": "'(*, request: stove0_protocol.models.ObservationRequest, result: stove0_protocol.models.ObservationResult) -> None'"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationEvidence",
  "unit": "export"
}
```

</details>
