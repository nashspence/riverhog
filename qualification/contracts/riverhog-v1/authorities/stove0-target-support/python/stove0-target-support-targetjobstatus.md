# stove0_target_support.TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobstatus:435d58f64e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31dee2c892"></a>
- <a id="s-e87c0d32c3"></a>`distribution`: `stove0-target-support`
- <a id="s-33192005b7"></a>`module`: `stove0_target_support`
- <a id="s-f849fc5078"></a>`name`: `TargetJobStatus`
- <a id="s-f2c263a80f"></a>`unit`: `export`

### Declared structure

- <a id="s-c1869de810"></a>`kind`: `"class"`
- <a id="s-b6093c4b29"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['queued', 'running', 'canceling', 'interrupted', 'inapplicable', 'succeeded', 'failed', 'canceled'], attempt: Annotated[int, Ge(ge=1)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], progress: stove0_target_protocol.protocol.TargetProgress, production: stove0_target_protocol.protocol.TargetProductionAuthority \| None = None, output_collection: stove0_target_protocol.protocol.OutputCollectionRef \| None = None, execution_evidence: stove0_target_protocol.protocol.TargetExecutionEvidence \| None = None, derivation: dict[str, typing.Any] \| None = None, effect_receipt: stove0_target_protocol.protocol.ExternalEffectReceipt \| None = None, failure: stove0_target_protocol.protocol.TargetFailure \| None = None, inapplicable: stove0_target_protocol.protocol.TargetInapplicable \| None = None) -> None\""`

#### Validated model schema

<a id="s-089320ef43"></a>

- <a id="s-07503b41ad"></a>`type`: `"object"`
- <a id="s-075de37652"></a>`additionalProperties`: `false`
- <a id="s-b619ea045a"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-be1a7e3f6a"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-9207d0abd0"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null |  |
| <a id="s-5cb268587a"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-8cf5a2952c)); (type="null")]; default=null |  |
| <a id="s-09c3346538"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-0db55b0df7)); (type="null")]; default=null |  |
| <a id="s-40371aa134"></a>`failure` | no | anyOf=[([TargetFailure](#s-230d3835b4)); (type="null")]; default=null |  |
| <a id="s-ec449597d8"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-13a6f507d8)); (type="null")]; default=null |  |
| <a id="s-d15136e338"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ec4ab910f8"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-d4bd7f63aa)); (type="null")]; default=null |  |
| <a id="s-5ee0ffcdcc"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-27c910e141"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-22b4e41004)); (type="null")]; default=null |  |
| <a id="s-7180e43356"></a>`progress` | yes | [TargetProgress](#s-bcecdf072b) |  |
| <a id="s-e0a3c41d92"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-bc529ccad9"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eb75a0e443"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

##### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-46ffc60dfc"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

##### Definitions

- [ArtifactDispositionSetIdentity](#s-5d080fa9ff)
- [CollectionId](#s-2d02310cc8)
- [ExternalEffectReceipt](#s-8cf5a2952c)
- [JsonValue](#s-517d880031)
- [OutputArtifactRoleCount](#s-66ea2baffe)
- [OutputArtifactSetIdentity](#s-710f5ba929)
- [OutputCollectionRef](#s-d4bd7f63aa)
- [TargetExecutionEvidence](#s-0db55b0df7)
- [TargetFailure](#s-230d3835b4)
- [TargetInapplicable](#s-13a6f507d8)
- [TargetProductionAuthority](#s-22b4e41004)
- [TargetProgress](#s-bcecdf072b)

##### <a id="s-5d080fa9ff"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-c00295f6e3"></a>`type`: `"object"`
- <a id="s-2b2eeea679"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-082dc9ace2"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-96666569de"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-c16b1bde97"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-e9579c2f70"></a>`sha256` | yes | type="string" |  |

##### <a id="s-2d02310cc8"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-953c197da0"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-6584e281cf"></a>2 | not=(const="0") |

##### <a id="s-8cf5a2952c"></a>definition `ExternalEffectReceipt`

- <a id="s-d176d1bc26"></a>`type`: `"object"`
- <a id="s-f89e3d04aa"></a>`additionalProperties`: `false`
- <a id="s-d1e09ddea3"></a>`required`: `["job_id","request_sha256","target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-52bdd80240"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7d6f25680e"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1" |  |
| <a id="s-fe3de2e18b"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-935eecb334"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c0811c504a"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f24b43efa4"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7af1808f25"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ac008b1401"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-517d880031)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-1e55eddfd5"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-517d880031"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-66ea2baffe"></a>definition `OutputArtifactRoleCount`

- <a id="s-16bd95f6c3"></a>`type`: `"object"`
- <a id="s-c11eeb4ff2"></a>`additionalProperties`: `false`
- <a id="s-6cea3bacbb"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4412cc688f"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-b05c6420f2"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-710f5ba929"></a>definition `OutputArtifactSetIdentity`

- <a id="s-10583063c3"></a>`type`: `"object"`
- <a id="s-1058f8b9f1"></a>`additionalProperties`: `false`
- <a id="s-816ba1936b"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eb450479c8"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-f7a69b341f"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-66ea2baffe)); minItems=1 |  |
| <a id="s-adee529ec3"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e864f064b8"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-d4bd7f63aa"></a>definition `OutputCollectionRef`

- <a id="s-6d572d1759"></a>`type`: `"object"`
- <a id="s-ca61054a61"></a>`additionalProperties`: `false`
- <a id="s-8a77177d79"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f5fd98d38c"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c106c384c3"></a>`collection_id` | yes | [CollectionId](#s-2d02310cc8) |  |
| <a id="s-0838e9d54b"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e017cb85be"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0db55b0df7"></a>definition `TargetExecutionEvidence`

- <a id="s-be6e1fdec1"></a>`type`: `"object"`
- <a id="s-f592528b91"></a>`additionalProperties`: `false`
- <a id="s-d1afa8930e"></a>`required`: `["target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ffa18be910"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-06cd55c9b1"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-512f0449c4"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f9708036c6"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-517d880031)) |  |
| <a id="s-92ea3e776d"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-230d3835b4"></a>definition `TargetFailure`

- <a id="s-558e876e74"></a>`type`: `"object"`
- <a id="s-d865342d0a"></a>`additionalProperties`: `false`
- <a id="s-ae860a8a68"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a8f08f468a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0bcfa33df2"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-24502e27c3"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-13a6f507d8"></a>definition `TargetInapplicable`

- <a id="s-8a7cff1d87"></a>`type`: `"object"`
- <a id="s-948b18dac4"></a>`additionalProperties`: `false`
- <a id="s-7bddbe446e"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6789fa516"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d709384682"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-22b4e41004"></a>definition `TargetProductionAuthority`

- <a id="s-5f812b6559"></a>`type`: `"object"`
- <a id="s-1cb8ea1de9"></a>`additionalProperties`: `false`
- <a id="s-22c450171b"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8184bd107a"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-e798c64396"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5312366218"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-959c9c0efe"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3855ccd4f3"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-710f5ba929) |  |
| <a id="s-50f350345d"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9b0e5a5fb8"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cf76664153"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-5d080fa9ff) |  |
| <a id="s-5d8d6bafbb"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-e6574719db"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bcecdf072b"></a>definition `TargetProgress`

- <a id="s-e8841dd334"></a>`type`: `"object"`
- <a id="s-a4f42caf48"></a>`additionalProperties`: `false`
- <a id="s-1db20c3729"></a>`required`: `["phase","completed"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-871c9a1e77"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-9a1a2b3eae"></a>`phase` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-037437113d"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null |  |
| <a id="s-13e327f4af"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null |  |

## Maintained corroboration

### Related interface records

- [canonical_derivation](stove0-target-support-targetjobstatus-canonical-derivation.md)
- [validate_terminal_shape](stove0-target-support-targetjobstatus-validate-terminal-shape.md)

## Governing policies

- <a id="pa-394eb7fb67"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8da82bc123f32fdbb977e51be70332b25968f0696d5be1d3a3143c617b7e59e9 -->

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
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "job_id",
            "request_sha256",
            "target_descriptor_sha256",
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
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "target_descriptor_sha256",
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
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetJobStatus",
  "unit": "export"
}
```

</details>
