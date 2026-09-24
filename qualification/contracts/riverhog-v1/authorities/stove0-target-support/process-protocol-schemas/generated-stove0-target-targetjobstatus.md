# generated:stove0-target: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-targetjobstatus:d1ba44ba5f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-b53b2ee7be"></a>

- <a id="s-47bafd9c62"></a>`type`: `"object"`
- <a id="s-a4fb474dfa"></a>`additionalProperties`: `false`
- <a id="s-5f2c593f80"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`
- <a id="s-1e99c2b897"></a>`title`: `"TargetJobStatus"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cbe3d7f342"></a>`attempt` | yes | type="integer"; minimum=1; title="Attempt" |  |
| <a id="s-cb4b465f39"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Derivation" |  |
| <a id="s-8901e58307"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-ae338d6e03)); (type="null")]; default=null |  |
| <a id="s-7d15c4fb5b"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-0dc0b1b879)); (type="null")]; default=null |  |
| <a id="s-777ed1612d"></a>`failure` | no | anyOf=[([TargetFailure](#s-9d3b78ba6a)); (type="null")]; default=null |  |
| <a id="s-7a41c916e6"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-7d4399c5a5)); (type="null")]; default=null |  |
| <a id="s-4c5be04ffe"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-eb57f0600d"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-62d53c6d33)); (type="null")]; default=null |  |
| <a id="s-5fef5c6f54"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-66f4c39c40"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-3ba41cd42a)); (type="null")]; default=null |  |
| <a id="s-2734d6a731"></a>`progress` | yes | [TargetProgress](#s-9bde36ac37) |  |
| <a id="s-ebcda53e0e"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-95fd4f6192"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-9586ac8d93"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"]; title="State" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-ea56dc18ba"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

### Definitions

- [ArtifactDispositionSetIdentity](#s-f5ab2ecb24)
- [CollectionId](#s-007b5483e0)
- [ExternalEffectReceipt](#s-ae338d6e03)
- [JsonValue](#s-5313b1967d)
- [NonnegativeDecimal](#s-361350d16f)
- [OutputArtifactRoleCount](#s-ded4f3d1e2)
- [OutputArtifactSetIdentity](#s-c9db71eb0b)
- [OutputCollectionRef](#s-62d53c6d33)
- [TargetExecutionEvidence](#s-0dc0b1b879)
- [TargetFailure](#s-9d3b78ba6a)
- [TargetInapplicable](#s-7d4399c5a5)
- [TargetProductionAuthority](#s-3ba41cd42a)
- [TargetProgress](#s-9bde36ac37)

### <a id="s-f5ab2ecb24"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-32474235c5"></a>`type`: `"object"`
- <a id="s-f41c60d17c"></a>`description`: `"Small identity for one sealed claim-scoped relational disposition set."`
- <a id="s-e8d3c58377"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`
- <a id="s-67b70444e8"></a>`title`: `"ArtifactDispositionSetIdentity"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a3edc3cf31"></a>`disposition_count` | yes | type="integer"; title="Disposition Count" |  |
| <a id="s-7e1af5137b"></a>`output_artifact_count` | yes | type="integer"; title="Output Artifact Count" |  |
| <a id="s-5682a483bb"></a>`output_edge_count` | yes | type="integer"; title="Output Edge Count" |  |
| <a id="s-ae39ff61ab"></a>`sha256` | yes | type="string"; title="Sha256" |  |

### <a id="s-007b5483e0"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-bf27e9537f"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-3f130b86d1"></a>2 | not=(const="0") |

### <a id="s-ae338d6e03"></a>definition `ExternalEffectReceipt`

- <a id="s-2211e7fb01"></a>`type`: `"object"`
- <a id="s-e4438ed326"></a>`additionalProperties`: `false`
- <a id="s-71fa578779"></a>`required`: `["job_id","request_sha256","target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`
- <a id="s-85b8229c31"></a>`title`: `"ExternalEffectReceipt"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-37d2c456e8"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-2908e9b74e"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1"; title="Format" |  |
| <a id="s-6a32c1c6d7"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-85b2f65f7a"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-5077cabca5"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-47ac8bf81c"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Receipt Sha256" |  |
| <a id="s-2bbcef36ed"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-4f24492115"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-5313b1967d)); title="Result"; x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-457ebbda8a"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |

### <a id="s-5313b1967d"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-361350d16f"></a>definition `NonnegativeDecimal`

- <a id="s-12c3c157ed"></a>`type`: `"string"`
- <a id="s-4fb8ffe476"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-ded4f3d1e2"></a>definition `OutputArtifactRoleCount`

- <a id="s-eeae313281"></a>`type`: `"object"`
- <a id="s-2efb006275"></a>`additionalProperties`: `false`
- <a id="s-8be5aaf8c5"></a>`required`: `["role","count"]`
- <a id="s-908775f5f6"></a>`title`: `"OutputArtifactRoleCount"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0847d25357"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-543cf60440"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-c9db71eb0b"></a>definition `OutputArtifactSetIdentity`

- <a id="s-b22677d877"></a>`type`: `"object"`
- <a id="s-ecb49e4a95"></a>`additionalProperties`: `false`
- <a id="s-b16ba76ea8"></a>`description`: `"Small identity for target outputs already registered with Riverhog."`
- <a id="s-9cc2565c99"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`
- <a id="s-ef1a207cad"></a>`title`: `"OutputArtifactSetIdentity"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-58f900558e"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-2ae1671f95"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-ded4f3d1e2)); minItems=1; title="Roles" |  |
| <a id="s-3e45bae18b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-f58cd05714"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-361350d16f); ge=0 |  |

### <a id="s-62d53c6d33"></a>definition `OutputCollectionRef`

- <a id="s-f2392ba0ac"></a>`type`: `"object"`
- <a id="s-fe6ac9be66"></a>`additionalProperties`: `false`
- <a id="s-ad3715cc24"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`
- <a id="s-874a92399c"></a>`title`: `"OutputCollectionRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce2177192d"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-477301c533"></a>`collection_id` | yes | [CollectionId](#s-007b5483e0) |  |
| <a id="s-15ed87b9a7"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-154d01b9a4"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Derivation Sha256" |  |

### <a id="s-0dc0b1b879"></a>definition `TargetExecutionEvidence`

- <a id="s-1fb6b50278"></a>`type`: `"object"`
- <a id="s-99bb97c6c0"></a>`additionalProperties`: `false`
- <a id="s-fa790f20ca"></a>`required`: `["target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`
- <a id="s-20b7ff4cf7"></a>`title`: `"TargetExecutionEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-84420c7392"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-953cbc2e62"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-1bb5896a42"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-bb1174b3c8"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-5313b1967d)); title="Runtime" |  |
| <a id="s-2873f8b322"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |

### <a id="s-9d3b78ba6a"></a>definition `TargetFailure`

- <a id="s-816f46741f"></a>`type`: `"object"`
- <a id="s-cf02096807"></a>`additionalProperties`: `false`
- <a id="s-3dfca41583"></a>`required`: `["code","message","retryable"]`
- <a id="s-9c1d3982be"></a>`title`: `"TargetFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b11ebe8a7"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-05dec93702"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-4c159531e9"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-7d4399c5a5"></a>definition `TargetInapplicable`

- <a id="s-f677a841c8"></a>`type`: `"object"`
- <a id="s-2cd12e548c"></a>`additionalProperties`: `false`
- <a id="s-e4101142b8"></a>`required`: `["code","message"]`
- <a id="s-b01b08cf69"></a>`title`: `"TargetInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ec2852781"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-e3d01b410d"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-3ba41cd42a"></a>definition `TargetProductionAuthority`

- <a id="s-a2933a7509"></a>`type`: `"object"`
- <a id="s-787256342e"></a>`additionalProperties`: `false`
- <a id="s-8abef48815"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`
- <a id="s-a9e54868cf"></a>`title`: `"TargetProductionAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-09aa237d73"></a>`disposition_count` | yes | type="integer"; minimum=1; title="Disposition Count" |  |
| <a id="s-88040c02c3"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Disposition Sha256" |  |
| <a id="s-49d054fce6"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1"; title="Format" |  |
| <a id="s-634004f0ac"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-21271a7227"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-c9db71eb0b) |  |
| <a id="s-5a92c9dba3"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-689eb43cd6"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Production Sha256" |  |
| <a id="s-0b9cc6f4b0"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-f5ab2ecb24) |  |
| <a id="s-285eeac920"></a>`source_edge_count` | yes | type="integer"; minimum=1; title="Source Edge Count" |  |
| <a id="s-0008bc7ac6"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Source Edge Sha256" |  |

### <a id="s-9bde36ac37"></a>definition `TargetProgress`

- <a id="s-306f9669a5"></a>`type`: `"object"`
- <a id="s-3f17cc0c8a"></a>`additionalProperties`: `false`
- <a id="s-0362ed8454"></a>`required`: `["phase","completed"]`
- <a id="s-47cda35e87"></a>`title`: `"TargetProgress"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0c74ac8a0b"></a>`completed` | yes | type="integer"; minimum=0; title="Completed" |  |
| <a id="s-a214007c28"></a>`phase` | yes | type="string"; maxLength=120; minLength=1; title="Phase" |  |
| <a id="s-ea84c520d2"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null; title="Total" |  |
| <a id="s-85a67606b9"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null; title="Unit" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-7c6c04f30c"></a>[field derivation · object value](#s-cb4b465f39) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field job_id](#s-4c5be04ffe) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-5fef5c6f54) | `length · characters · fixed` | shared above |
| [field request_sha256](#s-95fd4f6192) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-092bf3c556"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-34ff630d27"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-45ca3dc7a9"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetJobStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28f5b6bbe91f110036368d2f00ef7fb8bc949311c8a355afee64fbc8403eb3e3 -->

```json
{
  "$defs": {
    "ArtifactDispositionSetIdentity": {
      "description": "Small identity for one sealed claim-scoped relational disposition set.",
      "properties": {
        "disposition_count": {
          "title": "Disposition Count",
          "type": "integer"
        },
        "output_artifact_count": {
          "title": "Output Artifact Count",
          "type": "integer"
        },
        "output_edge_count": {
          "title": "Output Edge Count",
          "type": "integer"
        },
        "sha256": {
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "disposition_count",
        "output_edge_count",
        "output_artifact_count",
        "sha256"
      ],
      "title": "ArtifactDispositionSetIdentity",
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
          "title": "Execution Sha256",
          "type": "string"
        },
        "format": {
          "const": "stove0-external-effect-receipt/v1",
          "default": "stove0-external-effect-receipt/v1",
          "title": "Format",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "receipt_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Receipt Sha256",
          "type": "string"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        },
        "result": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Result",
          "type": "object",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-external-effect-receipt"
          }
        },
        "target_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Descriptor Sha256",
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
      "title": "ExternalEffectReceipt",
      "type": "object"
    },
    "JsonValue": {},
    "NonnegativeDecimal": {
      "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
      "type": "string"
    },
    "OutputArtifactRoleCount": {
      "additionalProperties": false,
      "properties": {
        "count": {
          "minimum": 1,
          "title": "Count",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role",
        "count"
      ],
      "title": "OutputArtifactRoleCount",
      "type": "object"
    },
    "OutputArtifactSetIdentity": {
      "additionalProperties": false,
      "description": "Small identity for target outputs already registered with Riverhog.",
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "title": "Artifact Count",
          "type": "integer"
        },
        "roles": {
          "items": {
            "$ref": "#/$defs/OutputArtifactRoleCount"
          },
          "minItems": 1,
          "title": "Roles",
          "type": "array"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        },
        "total_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 0
        }
      },
      "required": [
        "artifact_count",
        "total_bytes",
        "roles",
        "sha256"
      ],
      "title": "OutputArtifactSetIdentity",
      "type": "object"
    },
    "OutputCollectionRef": {
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
        },
        "derivation_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Derivation Sha256",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity",
        "derivation_sha256"
      ],
      "title": "OutputCollectionRef",
      "type": "object"
    },
    "TargetExecutionEvidence": {
      "additionalProperties": false,
      "properties": {
        "execution_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Execution Sha256",
          "type": "string"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "runtime": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Runtime",
          "type": "object"
        },
        "target_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Descriptor Sha256",
          "type": "string"
        }
      },
      "required": [
        "target_descriptor_sha256",
        "operation_contract_sha256",
        "plan_sha256",
        "execution_sha256"
      ],
      "title": "TargetExecutionEvidence",
      "type": "object"
    },
    "TargetFailure": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        },
        "retryable": {
          "title": "Retryable",
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "title": "TargetFailure",
      "type": "object"
    },
    "TargetInapplicable": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "TargetInapplicable",
      "type": "object"
    },
    "TargetProductionAuthority": {
      "additionalProperties": false,
      "properties": {
        "disposition_count": {
          "minimum": 1,
          "title": "Disposition Count",
          "type": "integer"
        },
        "disposition_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Disposition Sha256",
          "type": "string"
        },
        "format": {
          "const": "stove0-target-production/v1",
          "default": "stove0-target-production/v1",
          "title": "Format",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "outputs": {
          "$ref": "#/$defs/OutputArtifactSetIdentity"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "production_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Production Sha256",
          "type": "string"
        },
        "riverhog_disposition_set": {
          "$ref": "#/$defs/ArtifactDispositionSetIdentity"
        },
        "source_edge_count": {
          "minimum": 1,
          "title": "Source Edge Count",
          "type": "integer"
        },
        "source_edge_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Source Edge Sha256",
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
      "title": "TargetProductionAuthority",
      "type": "object"
    },
    "TargetProgress": {
      "additionalProperties": false,
      "properties": {
        "completed": {
          "minimum": 0,
          "title": "Completed",
          "type": "integer"
        },
        "phase": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Phase",
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
          "default": null,
          "title": "Total"
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
          "default": null,
          "title": "Unit"
        }
      },
      "required": [
        "phase",
        "completed"
      ],
      "title": "TargetProgress",
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
      "title": "Attempt",
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
      "default": null,
      "title": "Derivation"
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
      "title": "Job Id",
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
      "title": "Plan Sha256",
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
      "title": "Protocol",
      "type": "string"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
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
      "title": "State",
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
  "title": "TargetJobStatus",
  "type": "object"
}
```

</details>
