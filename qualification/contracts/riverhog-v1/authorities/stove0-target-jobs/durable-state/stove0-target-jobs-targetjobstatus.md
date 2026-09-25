# stove0-target-jobs: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-targetjobstatus:b16bcfbf8f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-jobs](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-7b308e55b6"></a>

- Document: `TargetJobStatus`

### Document schema

<a id="s-5da2d280f3"></a>

- <a id="s-9be61aa957"></a>`type`: `"object"`
- <a id="s-380002984e"></a>`additionalProperties`: `false`
- <a id="s-25beff784a"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`
- <a id="s-e0a21eaa2a"></a>`title`: `"TargetJobStatus"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de27795472"></a>`attempt` | yes | type="integer"; minimum=1; title="Attempt" |  |
| <a id="s-b016ea6af2"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Derivation" |  |
| <a id="s-6855ffda36"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-8b4a24b0e7)); (type="null")]; default=null |  |
| <a id="s-1433170bd4"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-413264cab5)); (type="null")]; default=null |  |
| <a id="s-b2f7acab2d"></a>`failure` | no | anyOf=[([TargetFailure](#s-caf82a2bb7)); (type="null")]; default=null |  |
| <a id="s-96106d58e1"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-126011d14e)); (type="null")]; default=null |  |
| <a id="s-62ff5af90a"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-d10dc5a215"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-60aceae91c)); (type="null")]; default=null |  |
| <a id="s-b3fe4d5d58"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-a3f8f9e8b4"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-50f71a9fe6)); (type="null")]; default=null |  |
| <a id="s-aee7a1585b"></a>`progress` | yes | [TargetProgress](#s-62dc4927bc) |  |
| <a id="s-b7f3c1237b"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-1ccbe7ea38"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-2bbcd2e963"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"]; title="State" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-bcf99bff0f"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

#### Definitions

- [ArtifactDispositionSetIdentity](#s-2f5c39b47e)
- [CollectionId](#s-bab28859a4)
- [ExternalEffectReceipt](#s-8b4a24b0e7)
- [JsonValue](#s-0c331f06c8)
- [NonnegativeDecimal](#s-57b7598da1)
- [OutputArtifactRoleCount](#s-fac44d866e)
- [OutputArtifactSetIdentity](#s-f27e97cc6d)
- [OutputCollectionRef](#s-60aceae91c)
- [TargetExecutionEvidence](#s-413264cab5)
- [TargetFailure](#s-caf82a2bb7)
- [TargetInapplicable](#s-126011d14e)
- [TargetProductionAuthority](#s-50f71a9fe6)
- [TargetProgress](#s-62dc4927bc)

#### <a id="s-2f5c39b47e"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-46473810c8"></a>`type`: `"object"`
- <a id="s-e5cc5112fe"></a>`description`: `"Small identity for one sealed claim-scoped relational disposition set."`
- <a id="s-710d710ef4"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`
- <a id="s-f57640a4c2"></a>`title`: `"ArtifactDispositionSetIdentity"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa8f656fc2"></a>`disposition_count` | yes | type="integer"; title="Disposition Count" |  |
| <a id="s-3a57761d96"></a>`output_artifact_count` | yes | type="integer"; title="Output Artifact Count" |  |
| <a id="s-b99d8e4e7b"></a>`output_edge_count` | yes | type="integer"; title="Output Edge Count" |  |
| <a id="s-ad2cdc889a"></a>`sha256` | yes | type="string"; title="Sha256" |  |

#### <a id="s-bab28859a4"></a>definition `CollectionId`


##### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-efb1a15bf3"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-1d7cea718d"></a>2 | not=(const="0") |

#### <a id="s-8b4a24b0e7"></a>definition `ExternalEffectReceipt`

- <a id="s-7b5416af66"></a>`type`: `"object"`
- <a id="s-8b1f6081e5"></a>`additionalProperties`: `false`
- <a id="s-f407863d84"></a>`required`: `["job_id","request_sha256","target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`
- <a id="s-c50534f80c"></a>`title`: `"ExternalEffectReceipt"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-17c900e603"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-38ae3f6900"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1"; title="Format" |  |
| <a id="s-677708f0f8"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-4e8040051b"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-52cf4b6f40"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-ae2cf80fb4"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Receipt Sha256" |  |
| <a id="s-ac5775bff1"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-fa745d2441"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-0c331f06c8)); title="Result"; x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-2caf532154"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |

#### <a id="s-0c331f06c8"></a>definition `JsonValue`

- Accepts: any JSON value.

#### <a id="s-57b7598da1"></a>definition `NonnegativeDecimal`

- <a id="s-7520a1b1e5"></a>`type`: `"string"`
- <a id="s-51700ff7f0"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

#### <a id="s-fac44d866e"></a>definition `OutputArtifactRoleCount`

- <a id="s-ef38a5af8a"></a>`type`: `"object"`
- <a id="s-998e38b79b"></a>`additionalProperties`: `false`
- <a id="s-2f61202137"></a>`required`: `["role","count"]`
- <a id="s-773e490c7a"></a>`title`: `"OutputArtifactRoleCount"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa07d4fe99"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-56d080345c"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

#### <a id="s-f27e97cc6d"></a>definition `OutputArtifactSetIdentity`

- <a id="s-e6f3dcb857"></a>`type`: `"object"`
- <a id="s-581bfea580"></a>`additionalProperties`: `false`
- <a id="s-9f563a4e4f"></a>`description`: `"Small identity for target outputs already registered with Riverhog."`
- <a id="s-1681b1efac"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`
- <a id="s-f0d4a50dbd"></a>`title`: `"OutputArtifactSetIdentity"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9e2386c043"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-84184668b6"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-fac44d866e)); minItems=1; title="Roles" |  |
| <a id="s-926db34d54"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-4f51972e03"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-57b7598da1); ge=0 |  |

#### <a id="s-60aceae91c"></a>definition `OutputCollectionRef`

- <a id="s-7eee187521"></a>`type`: `"object"`
- <a id="s-1bb1ac9666"></a>`additionalProperties`: `false`
- <a id="s-e1ca610c9b"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`
- <a id="s-7887032e4e"></a>`title`: `"OutputCollectionRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c99f2e6392"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-15287554e8"></a>`collection_id` | yes | [CollectionId](#s-bab28859a4) |  |
| <a id="s-0e3e738b4d"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-713ee791ec"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Derivation Sha256" |  |

#### <a id="s-413264cab5"></a>definition `TargetExecutionEvidence`

- <a id="s-6de0bc7184"></a>`type`: `"object"`
- <a id="s-f38e0e42da"></a>`additionalProperties`: `false`
- <a id="s-4be196a246"></a>`required`: `["target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`
- <a id="s-98b691e67f"></a>`title`: `"TargetExecutionEvidence"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b40ce4b9d5"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-d11a81b728"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-654bd8746c"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-3d678754f5"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-0c331f06c8)); title="Runtime" |  |
| <a id="s-714e134378"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |

#### <a id="s-caf82a2bb7"></a>definition `TargetFailure`

- <a id="s-6156f92246"></a>`type`: `"object"`
- <a id="s-9571b04a2e"></a>`additionalProperties`: `false`
- <a id="s-a89e21b510"></a>`required`: `["code","message","retryable"]`
- <a id="s-7bf8b8123c"></a>`title`: `"TargetFailure"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e7bbfb87c2"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-ce010009f8"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-f42601805a"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

#### <a id="s-126011d14e"></a>definition `TargetInapplicable`

- <a id="s-68bd2fc352"></a>`type`: `"object"`
- <a id="s-4b69b590ce"></a>`additionalProperties`: `false`
- <a id="s-f03fc342b3"></a>`required`: `["code","message"]`
- <a id="s-9422b6169e"></a>`title`: `"TargetInapplicable"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-36e6c2712e"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-a92b91acbf"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

#### <a id="s-50f71a9fe6"></a>definition `TargetProductionAuthority`

- <a id="s-b210bdd337"></a>`type`: `"object"`
- <a id="s-7a448b4bd4"></a>`additionalProperties`: `false`
- <a id="s-b024d723f2"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`
- <a id="s-3e9c35a032"></a>`title`: `"TargetProductionAuthority"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-958323e96e"></a>`disposition_count` | yes | type="integer"; minimum=1; title="Disposition Count" |  |
| <a id="s-63066746ec"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Disposition Sha256" |  |
| <a id="s-e8fa9aecdf"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1"; title="Format" |  |
| <a id="s-3aa0564b2d"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-a63066f535"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-f27e97cc6d) |  |
| <a id="s-7e90687aec"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-79c217acc0"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Production Sha256" |  |
| <a id="s-0e3d62ff72"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-2f5c39b47e) |  |
| <a id="s-a76592efac"></a>`source_edge_count` | yes | type="integer"; minimum=1; title="Source Edge Count" |  |
| <a id="s-1bbaca524f"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Source Edge Sha256" |  |

#### <a id="s-62dc4927bc"></a>definition `TargetProgress`

- <a id="s-a4de29cbf2"></a>`type`: `"object"`
- <a id="s-2b3969e400"></a>`additionalProperties`: `false`
- <a id="s-a944eaa94b"></a>`required`: `["phase","completed"]`
- <a id="s-1b9f54f9e8"></a>`title`: `"TargetProgress"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9de0286290"></a>`completed` | yes | type="integer"; minimum=0; title="Completed" |  |
| <a id="s-b06ae78987"></a>`phase` | yes | type="string"; maxLength=120; minLength=1; title="Phase" |  |
| <a id="s-107bc1fc82"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null; title="Total" |  |
| <a id="s-02711c2df8"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null; title="Unit" |  |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-target-jobs-durable-state-identity.md)

## Governing policies

- <a id="pa-f6badad064"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-target-jobs](../../../evidence/sources/authorities.md#src-7b4138829a) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/protocol.py::AcceptedTargetJob](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py); [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/protocol.py::TargetJobStatus](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py)

### Machine authority

- `/external_contract/durable_state/owners/7/structure/documents/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5fc70a422fa43d87027238dec07adc71ca17fd735ae5b7a244954d3c9c1c785 -->

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
}
```

</details>
