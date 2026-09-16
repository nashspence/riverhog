# stove0-target-jobs: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-targetjobstatus:e0ff216756 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-jobs](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-bda4c8b11d"></a>

- Document: `TargetJobStatus`

### Document schema

<a id="s-c6d4e24c5d"></a>

- <a id="s-c122f3742c"></a>`type`: `"object"`
- <a id="s-5b74c3c6b4"></a>`additionalProperties`: `false`
- <a id="s-af6e588cc7"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`
- <a id="s-dfc60cdd41"></a>`title`: `"TargetJobStatus"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3df251316d"></a>`attempt` | yes | type="integer"; minimum=1; title="Attempt" |  |
| <a id="s-c6836f380a"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Derivation" |  |
| <a id="s-5e317243bc"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-26a5e8c3b1)); (type="null")]; default=null |  |
| <a id="s-18f9ef6552"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-4cb88d14a9)); (type="null")]; default=null |  |
| <a id="s-ae3c3516eb"></a>`failure` | no | anyOf=[([TargetFailure](#s-9a77996437)); (type="null")]; default=null |  |
| <a id="s-4637ad6e2c"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-9eef827dbc)); (type="null")]; default=null |  |
| <a id="s-251d0845a8"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-82ccbbb62d"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-d7db2264de)); (type="null")]; default=null |  |
| <a id="s-30b6e78e10"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-5126e9e1e1"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-960e8adbc4)); (type="null")]; default=null |  |
| <a id="s-3c2424e430"></a>`progress` | yes | [TargetProgress](#s-80fe077bf8) |  |
| <a id="s-e91fef0a8f"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-6dc483ad49"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-79937baa0d"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"]; title="State" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-31d17d4e31"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

#### Definitions

- [ArtifactDispositionSetIdentity](#s-94e71eb9c2)
- [CollectionId](#s-090ac9d376)
- [ExternalEffectReceipt](#s-26a5e8c3b1)
- [JsonValue](#s-18dd5136d0)
- [OutputArtifactRoleCount](#s-b4c30465ea)
- [OutputArtifactSetIdentity](#s-c8fdc5e154)
- [OutputCollectionRef](#s-d7db2264de)
- [TargetExecutionEvidence](#s-4cb88d14a9)
- [TargetFailure](#s-9a77996437)
- [TargetInapplicable](#s-9eef827dbc)
- [TargetProductionAuthority](#s-960e8adbc4)
- [TargetProgress](#s-80fe077bf8)

#### <a id="s-94e71eb9c2"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-e68bec7a0f"></a>`type`: `"object"`
- <a id="s-c7b28e0ddd"></a>`description`: `"Small identity for one sealed claim-scoped relational disposition set."`
- <a id="s-83514e7595"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`
- <a id="s-ae2ce283b9"></a>`title`: `"ArtifactDispositionSetIdentity"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-635c6a2b10"></a>`disposition_count` | yes | type="integer"; title="Disposition Count" |  |
| <a id="s-90b06c7fc3"></a>`output_artifact_count` | yes | type="integer"; title="Output Artifact Count" |  |
| <a id="s-3c1b9626a3"></a>`output_edge_count` | yes | type="integer"; title="Output Edge Count" |  |
| <a id="s-3f9386eaac"></a>`sha256` | yes | type="string"; title="Sha256" |  |

#### <a id="s-090ac9d376"></a>definition `CollectionId`

- <a id="s-8f0be9ee35"></a>`type`: `"integer"`
- <a id="s-05bed2fdb1"></a>`minimum`: `1`

#### <a id="s-26a5e8c3b1"></a>definition `ExternalEffectReceipt`

- <a id="s-70ea327e16"></a>`type`: `"object"`
- <a id="s-7f93ed9ac5"></a>`additionalProperties`: `false`
- <a id="s-250396edcd"></a>`required`: `["job_id","request_sha256","target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`
- <a id="s-063e652328"></a>`title`: `"ExternalEffectReceipt"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66e5e40c8b"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-c9718428cc"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1"; title="Format" |  |
| <a id="s-a531b71ead"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-23f2cf0420"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-81bfaf3744"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-c2db6755fa"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Receipt Sha256" |  |
| <a id="s-0790af0e1f"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-c3664b2b45"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-18dd5136d0)); title="Result"; x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-a840a4458c"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Contract Sha256" |  |

#### <a id="s-18dd5136d0"></a>definition `JsonValue`

- Accepts: any JSON value.

#### <a id="s-b4c30465ea"></a>definition `OutputArtifactRoleCount`

- <a id="s-6277cce134"></a>`type`: `"object"`
- <a id="s-1926b5f08c"></a>`additionalProperties`: `false`
- <a id="s-28097273b1"></a>`required`: `["role","count"]`
- <a id="s-ec65b666d1"></a>`title`: `"OutputArtifactRoleCount"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a73b6b5b2"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-9d4ffd86a4"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

#### <a id="s-c8fdc5e154"></a>definition `OutputArtifactSetIdentity`

- <a id="s-a182ca978f"></a>`type`: `"object"`
- <a id="s-1097f340d2"></a>`additionalProperties`: `false`
- <a id="s-1b01f5c24a"></a>`description`: `"Small identity for target outputs already registered with Riverhog."`
- <a id="s-e299601079"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`
- <a id="s-8a0dbfda42"></a>`title`: `"OutputArtifactSetIdentity"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-99ce2e4b72"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-36b452d6ec"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-b4c30465ea)); minItems=1; title="Roles" |  |
| <a id="s-377a936112"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-f8cf3bdd48"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

#### <a id="s-d7db2264de"></a>definition `OutputCollectionRef`

- <a id="s-8278105b90"></a>`type`: `"object"`
- <a id="s-dcece5f6e2"></a>`additionalProperties`: `false`
- <a id="s-f5fd0ae51b"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`
- <a id="s-a3cbc45873"></a>`title`: `"OutputCollectionRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-58ab77b442"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-0f5224b449"></a>`collection_id` | yes | [CollectionId](#s-090ac9d376) |  |
| <a id="s-8f846d43bd"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-4fbe73b16e"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Derivation Sha256" |  |

#### <a id="s-4cb88d14a9"></a>definition `TargetExecutionEvidence`

- <a id="s-59f2f97a12"></a>`type`: `"object"`
- <a id="s-002dfc2fd8"></a>`additionalProperties`: `false`
- <a id="s-595d66c52f"></a>`required`: `["target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`
- <a id="s-b4f4caab5a"></a>`title`: `"TargetExecutionEvidence"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61474f96b9"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-0eef83b952"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-c18700d500"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-091a3f50e3"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-18dd5136d0)); title="Runtime" |  |
| <a id="s-dd95b9602c"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Contract Sha256" |  |

#### <a id="s-9a77996437"></a>definition `TargetFailure`

- <a id="s-ef73be6926"></a>`type`: `"object"`
- <a id="s-15c6fd2c7f"></a>`additionalProperties`: `false`
- <a id="s-35d524b8a2"></a>`required`: `["code","message","retryable"]`
- <a id="s-5f46bc6d80"></a>`title`: `"TargetFailure"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f9c643473a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-90af34e7b1"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-fbbc38cd27"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

#### <a id="s-9eef827dbc"></a>definition `TargetInapplicable`

- <a id="s-2d911ea833"></a>`type`: `"object"`
- <a id="s-ea9e6a2700"></a>`additionalProperties`: `false`
- <a id="s-8f7fc1754e"></a>`required`: `["code","message"]`
- <a id="s-ce2175d8c1"></a>`title`: `"TargetInapplicable"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac13c28d94"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-3811ed56e0"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

#### <a id="s-960e8adbc4"></a>definition `TargetProductionAuthority`

- <a id="s-ba045bb97a"></a>`type`: `"object"`
- <a id="s-ddb7083026"></a>`additionalProperties`: `false`
- <a id="s-92007b35c5"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`
- <a id="s-f8ff575923"></a>`title`: `"TargetProductionAuthority"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d1fb71c948"></a>`disposition_count` | yes | type="integer"; minimum=1; title="Disposition Count" |  |
| <a id="s-45a44bb1c6"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Disposition Sha256" |  |
| <a id="s-7972e0c80b"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1"; title="Format" |  |
| <a id="s-46322034bd"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-32d00c74c0"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-c8fdc5e154) |  |
| <a id="s-73ee2930fb"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-10ffde1857"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Production Sha256" |  |
| <a id="s-8845685917"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-94e71eb9c2) |  |
| <a id="s-e633c91a10"></a>`source_edge_count` | yes | type="integer"; minimum=1; title="Source Edge Count" |  |
| <a id="s-79a7c159d0"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Source Edge Sha256" |  |

#### <a id="s-80fe077bf8"></a>definition `TargetProgress`

- <a id="s-eaca2fd175"></a>`type`: `"object"`
- <a id="s-bd7e4e71c3"></a>`additionalProperties`: `false`
- <a id="s-6f43a1a08a"></a>`required`: `["phase","completed"]`
- <a id="s-f9be5850f2"></a>`title`: `"TargetProgress"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c2dbad5d8"></a>`completed` | yes | type="integer"; minimum=0; title="Completed" |  |
| <a id="s-2625063851"></a>`phase` | yes | type="string"; maxLength=120; minLength=1; title="Phase" |  |
| <a id="s-ea22572b4f"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null; title="Total" |  |
| <a id="s-1581b61c21"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null; title="Unit" |  |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-target-jobs-durable-state-identity.md)

## Governing policies

- <a id="pa-ab85be943c"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-target-jobs](../../../evidence/sources.md#src-7b4138829a) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py`

### Machine authority

- `/external_contract/durable_state/owners/5/structure/documents/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88691d8e5fc7229865708347d4ad12582b9eec7c0dbeb258b9d89e0bc8db1f0c -->

```json
{
  "id": "TargetJobStatus",
  "schema": {
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
        "minimum": 1,
        "type": "integer"
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
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
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
        "title": "ExternalEffectReceipt",
        "type": "object"
      },
      "JsonValue": {},
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
            "minimum": 0,
            "title": "Total Bytes",
            "type": "integer"
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
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          }
        },
        "required": [
          "target_contract_sha256",
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
}
```

</details>
