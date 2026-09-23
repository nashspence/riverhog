# stove0_target_protocol.TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobstatus:dc067334d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31cb463d3b"></a>
- <a id="s-48531cee14"></a>`distribution`: `stove0-target-protocol`
- <a id="s-0ba1fd98bc"></a>`module`: `stove0_target_protocol`
- <a id="s-ed20c46453"></a>`name`: `TargetJobStatus`
- <a id="s-4aa842cf1e"></a>`unit`: `export`

### Declared structure

- <a id="s-3e23c88925"></a>`kind`: `"class"`
- <a id="s-e0720959f8"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['queued', 'running', 'canceling', 'interrupted', 'inapplicable', 'succeeded', 'failed', 'canceled'], attempt: Annotated[int, Ge(ge=1)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], progress: stove0_target_protocol.protocol.TargetProgress, production: stove0_target_protocol.protocol.TargetProductionAuthority \| None = None, output_collection: stove0_target_protocol.protocol.OutputCollectionRef \| None = None, execution_evidence: stove0_target_protocol.protocol.TargetExecutionEvidence \| None = None, derivation: dict[str, typing.Any] \| None = None, effect_receipt: stove0_target_protocol.protocol.ExternalEffectReceipt \| None = None, failure: stove0_target_protocol.protocol.TargetFailure \| None = None, inapplicable: stove0_target_protocol.protocol.TargetInapplicable \| None = None) -> None\""`

#### Validated model schema

<a id="s-66d919c56b"></a>

- <a id="s-e05cbbd276"></a>`type`: `"object"`
- <a id="s-cfff509f1f"></a>`additionalProperties`: `false`
- <a id="s-264e8170c2"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e863ddeeab"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-34af17894f"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null |  |
| <a id="s-0004a645e9"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-fa9e50683e)); (type="null")]; default=null |  |
| <a id="s-e170293c3a"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-f1b5facb9e)); (type="null")]; default=null |  |
| <a id="s-c2358d4323"></a>`failure` | no | anyOf=[([TargetFailure](#s-985a78c617)); (type="null")]; default=null |  |
| <a id="s-16b9c1ba29"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-efacb42ecf)); (type="null")]; default=null |  |
| <a id="s-0fef5e9472"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fdfddb84b6"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-0fc4bfc6da)); (type="null")]; default=null |  |
| <a id="s-b330728783"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb2a827a04"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-faf2b63b18)); (type="null")]; default=null |  |
| <a id="s-768559147f"></a>`progress` | yes | [TargetProgress](#s-ddb01229e7) |  |
| <a id="s-2cdfa080a8"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-83e05f830d"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-78a639312f"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

##### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-4a4e75e80a"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

##### Definitions

- [ArtifactDispositionSetIdentity](#s-eec126ab97)
- [CollectionId](#s-532ff66dff)
- [ExternalEffectReceipt](#s-fa9e50683e)
- [JsonValue](#s-2b1e5f66ee)
- [OutputArtifactRoleCount](#s-1c00d8dab5)
- [OutputArtifactSetIdentity](#s-d29bfb8a33)
- [OutputCollectionRef](#s-0fc4bfc6da)
- [TargetExecutionEvidence](#s-f1b5facb9e)
- [TargetFailure](#s-985a78c617)
- [TargetInapplicable](#s-efacb42ecf)
- [TargetProductionAuthority](#s-faf2b63b18)
- [TargetProgress](#s-ddb01229e7)

##### <a id="s-eec126ab97"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-1ac43a79cb"></a>`type`: `"object"`
- <a id="s-69008b5a37"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5fd260008b"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-978b5d4505"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-921c1b3207"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-41b136828f"></a>`sha256` | yes | type="string" |  |

##### <a id="s-532ff66dff"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-810702888d"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-ddcd8aea07"></a>2 | not=(const="0") |

##### <a id="s-fa9e50683e"></a>definition `ExternalEffectReceipt`

- <a id="s-a780179a47"></a>`type`: `"object"`
- <a id="s-26f1d5c4de"></a>`additionalProperties`: `false`
- <a id="s-d4cdb6f761"></a>`required`: `["job_id","request_sha256","target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-beed6c5834"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6cbd06d232"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1" |  |
| <a id="s-edef0a47fd"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cae47166bf"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8ea3026e8b"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4ee7bd05d5"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1fa2018fce"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6c8f51218f"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-2b1e5f66ee)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-b3c6d8f62c"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2b1e5f66ee"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-1c00d8dab5"></a>definition `OutputArtifactRoleCount`

- <a id="s-e1c501aedf"></a>`type`: `"object"`
- <a id="s-73e7ebb69e"></a>`additionalProperties`: `false`
- <a id="s-723a2e1146"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-84b58cdf4a"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-edafe94714"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-d29bfb8a33"></a>definition `OutputArtifactSetIdentity`

- <a id="s-f2660170f3"></a>`type`: `"object"`
- <a id="s-31c82cbb0e"></a>`additionalProperties`: `false`
- <a id="s-f13ce50361"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4c306d5328"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-f2799bf4b5"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-1c00d8dab5)); minItems=1 |  |
| <a id="s-0f64277733"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cacfe79029"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-0fc4bfc6da"></a>definition `OutputCollectionRef`

- <a id="s-9fd343e94c"></a>`type`: `"object"`
- <a id="s-644adf60f0"></a>`additionalProperties`: `false`
- <a id="s-a9e53abe8a"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab7679fb99"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e697359a19"></a>`collection_id` | yes | [CollectionId](#s-532ff66dff) |  |
| <a id="s-1e6ede547e"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4e999a368d"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f1b5facb9e"></a>definition `TargetExecutionEvidence`

- <a id="s-f6f6048bd6"></a>`type`: `"object"`
- <a id="s-607d737335"></a>`additionalProperties`: `false`
- <a id="s-8f5121726f"></a>`required`: `["target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7152519eb6"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c4aa7505cd"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ec08c3f5e9"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-81658891c9"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-2b1e5f66ee)) |  |
| <a id="s-4ea4ed0938"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-985a78c617"></a>definition `TargetFailure`

- <a id="s-f8af107bd1"></a>`type`: `"object"`
- <a id="s-c62cc669a6"></a>`additionalProperties`: `false`
- <a id="s-cc234670db"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee05d9299c"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7c11ee7806"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-a0d6f212aa"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-efacb42ecf"></a>definition `TargetInapplicable`

- <a id="s-a1832c244c"></a>`type`: `"object"`
- <a id="s-8280f2638a"></a>`additionalProperties`: `false`
- <a id="s-64d33a1d93"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea8d8c9197"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-33e09dc74f"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-faf2b63b18"></a>definition `TargetProductionAuthority`

- <a id="s-b7105faade"></a>`type`: `"object"`
- <a id="s-96fade3548"></a>`additionalProperties`: `false`
- <a id="s-3218f9937e"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54241bff55"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-f92328c04a"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cbb21c75de"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-d555aec02f"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1d57bfbe90"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-d29bfb8a33) |  |
| <a id="s-852703b296"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c6b80a95d4"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b574ba1f07"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-eec126ab97) |  |
| <a id="s-645fff14f1"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-6bf2df9247"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ddb01229e7"></a>definition `TargetProgress`

- <a id="s-11a2c5c73f"></a>`type`: `"object"`
- <a id="s-4c575a760a"></a>`additionalProperties`: `false`
- <a id="s-1d1178f070"></a>`required`: `["phase","completed"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-89e92610bc"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-bd92e0621b"></a>`phase` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-54a5ceb1ec"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null |  |
| <a id="s-98b8165ddd"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null |  |

## Maintained corroboration

### Related interface records

- [canonical_derivation](stove0-target-protocol-targetjobstatus-canonical-derivation.md)
- [validate_terminal_shape](stove0-target-protocol-targetjobstatus-validate-terminal-shape.md)

## Governing policies

- <a id="pa-aa7780be93"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bde9ce45a1e7406c80d99695dfb702b6a556ee8b60f8a8ebde7eeee4b04bf466 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactDispositionSetIdentity": {
          "properties": {
            "disposition_count": {
              "type": "integer"
            },
            "output_artifact_count": {
              "type": "integer"
            },
            "output_edge_count": {
              "type": "integer"
            },
            "sha256": {
              "type": "string"
            }
          },
          "required": [
            "disposition_count",
            "output_edge_count",
            "output_artifact_count",
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
        "ExternalEffectReceipt": {
          "additionalProperties": false,
          "properties": {
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "stove0-external-effect-receipt/v1",
              "default": "stove0-external-effect-receipt/v1",
              "type": "string"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "receipt_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "request_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "result": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object",
              "x-riverhog-encoded-bytes-max": 65536,
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-external-effect-receipt"
              }
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "job_id",
            "request_sha256",
            "target_contract_sha256",
            "operation_contract_sha256",
            "plan_sha256",
            "execution_sha256",
            "result",
            "receipt_sha256"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "OutputArtifactRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "type": "object"
        },
        "OutputArtifactSetIdentity": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "roles": {
              "items": {
                "$ref": "#/$defs/OutputArtifactRoleCount"
              },
              "minItems": 1,
              "type": "array"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "artifact_count",
            "total_bytes",
            "roles",
            "sha256"
          ],
          "type": "object"
        },
        "OutputCollectionRef": {
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
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "type": "object"
        },
        "TargetExecutionEvidence": {
          "additionalProperties": false,
          "properties": {
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "runtime": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "target_contract_sha256",
            "operation_contract_sha256",
            "plan_sha256",
            "execution_sha256"
          ],
          "type": "object"
        },
        "TargetFailure": {
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
        "TargetInapplicable": {
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
        "TargetProductionAuthority": {
          "additionalProperties": false,
          "properties": {
            "disposition_count": {
              "minimum": 1,
              "type": "integer"
            },
            "disposition_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "stove0-target-production/v1",
              "default": "stove0-target-production/v1",
              "type": "string"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "outputs": {
              "$ref": "#/$defs/OutputArtifactSetIdentity"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "production_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "riverhog_disposition_set": {
              "$ref": "#/$defs/ArtifactDispositionSetIdentity"
            },
            "source_edge_count": {
              "minimum": 1,
              "type": "integer"
            },
            "source_edge_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "job_id",
            "plan_sha256",
            "outputs",
            "disposition_count",
            "disposition_sha256",
            "source_edge_count",
            "source_edge_sha256",
            "riverhog_disposition_set",
            "production_sha256"
          ],
          "type": "object"
        },
        "TargetProgress": {
          "additionalProperties": false,
          "properties": {
            "completed": {
              "minimum": 0,
              "type": "integer"
            },
            "phase": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            },
            "total": {
              "anyOf": [
                {
                  "minimum": 0,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "unit": {
              "anyOf": [
                {
                  "maxLength": 40,
                  "minLength": 1,
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
            "phase",
            "completed"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "allOf": [
        {
          "else": {
            "properties": {
              "failure": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "const": "failed"
              }
            }
          },
          "then": {
            "properties": {
              "failure": {
                "type": "object"
              }
            },
            "required": [
              "failure"
            ]
          }
        }
      ],
      "properties": {
        "attempt": {
          "minimum": 1,
          "type": "integer"
        },
        "derivation": {
          "anyOf": [
            {
              "additionalProperties": true,
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "effect_receipt": {
          "anyOf": [
            {
              "$ref": "#/$defs/ExternalEffectReceipt"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "execution_evidence": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetExecutionEvidence"
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
              "$ref": "#/$defs/TargetFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetInapplicable"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "output_collection": {
          "anyOf": [
            {
              "$ref": "#/$defs/OutputCollectionRef"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "production": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetProductionAuthority"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "progress": {
          "$ref": "#/$defs/TargetProgress"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "type": "string"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "state": {
          "enum": [
            "queued",
            "running",
            "canceling",
            "interrupted",
            "inapplicable",
            "succeeded",
            "failed",
            "canceled"
          ],
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "state",
        "attempt",
        "request_sha256",
        "plan_sha256",
        "progress"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['queued', 'running', 'canceling', 'interrupted', 'inapplicable', 'succeeded', 'failed', 'canceled'], attempt: Annotated[int, Ge(ge=1)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], progress: stove0_target_protocol.protocol.TargetProgress, production: stove0_target_protocol.protocol.TargetProductionAuthority | None = None, output_collection: stove0_target_protocol.protocol.OutputCollectionRef | None = None, execution_evidence: stove0_target_protocol.protocol.TargetExecutionEvidence | None = None, derivation: dict[str, typing.Any] | None = None, effect_receipt: stove0_target_protocol.protocol.ExternalEffectReceipt | None = None, failure: stove0_target_protocol.protocol.TargetFailure | None = None, inapplicable: stove0_target_protocol.protocol.TargetInapplicable | None = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetJobStatus",
  "unit": "export"
}
```

</details>
