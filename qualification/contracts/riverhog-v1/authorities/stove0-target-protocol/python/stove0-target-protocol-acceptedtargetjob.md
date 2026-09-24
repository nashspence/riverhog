# stove0_target_protocol.AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-acceptedtargetjob:624de7b1d1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c392c78ff8"></a>
- <a id="s-7607609177"></a>`distribution`: `stove0-target-protocol`
- <a id="s-d6419fb725"></a>`module`: `stove0_target_protocol`
- <a id="s-449903f6ce"></a>`name`: `AcceptedTargetJob`
- <a id="s-9ff0bccf11"></a>`unit`: `export`

### Declared structure

- <a id="s-c6a5e9c4f9"></a>`kind`: `"class"`
- <a id="s-d14127a6d4"></a>`signature`: `"\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-8f14119f71"></a>

- <a id="s-351604421a"></a>`type`: `"object"`
- <a id="s-d4af51f62a"></a>`additionalProperties`: `false`
- <a id="s-0944e2d0af"></a>`required`: `["declaration","request_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f9af5c784f"></a>`declaration` | yes | [TargetJobDeclaration](#s-77b308ad62) |  |
| <a id="s-2239eb7f78"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSelectionRef](#s-8d29998944)
- [BranchWorkBinding](#s-c386399dfd)
- [CollectionId](#s-51c0a8da54)
- [CollectionRootIdentityRef](#s-355d65e0db)
- [ContentObservationEvidence](#s-280acf3f05)
- [ContentObservationFailure](#s-68543b0101)
- [ContentObservationInapplicable](#s-299f3f3858)
- [ContentObservationRequest](#s-ebc6138a13)
- [ContentObservationResult](#s-71aaec4bf3)
- [ControllerEvidence](#s-5da467b0d4)
- [DeclaredWorkspaceProtection](#s-fad13d3179)
- [EffectPlan](#s-745b5b8fa8)
- [EvaluationBinding](#s-770268a072)
- [ExecutionEnvelope](#s-3bde0bf644)
- [JoinWorkBinding](#s-1b5cb48d41)
- [JoinWorkMemberBinding](#s-78f9d97d1f)
- [JsonSchemaValidationProfile](#s-2b849d9051)
- [JsonValue](#s-223b69a7b1)
- [NonnegativeDecimal](#s-117600b7b8)
- [ObserverImplementation](#s-32564876db)
- [OperationIdentityRef](#s-001519c2d8)
- [RecipeIdentityRef](#s-fdefc9a162)
- [TargetInputAuthority](#s-b03acb7057)
- [TargetInputRoleCount](#s-20b519275d)
- [TargetJobDeclaration](#s-77b308ad62)
- [TargetPlanBinding](#s-e66c6034c6)
- [TransformPlan](#s-c0ca1894ee)
- [WorkArtifactSubject](#s-77fdcc38d1)
- [WorkIdentity](#s-77b0ae1511)
- [WorkflowPlan](#s-1e85fec743)

##### <a id="s-8d29998944"></a>definition `ArtifactSelectionRef`

- <a id="s-d5e0366087"></a>`type`: `"object"`
- <a id="s-e68fa6c18a"></a>`additionalProperties`: `false`
- <a id="s-8fe1cc755e"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bc4f4559fa"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-4e2408fc59"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2a4dfcb6a0"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-c386399dfd"></a>definition `BranchWorkBinding`

- <a id="s-db2b174275"></a>`type`: `"object"`
- <a id="s-018398ba73"></a>`additionalProperties`: `false`
- <a id="s-8d051ace96"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9598b0d74e"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa903286c4"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-60a9f76f9f"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb99f83443"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-b35708a9c1"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-51c0a8da54"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-5897e940e6"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-c51c543e0c"></a>2 | not=(const="0") |

##### <a id="s-355d65e0db"></a>definition `CollectionRootIdentityRef`

- <a id="s-855a2c79be"></a>`type`: `"object"`
- <a id="s-6163b0ea09"></a>`additionalProperties`: `false`
- <a id="s-a430997dac"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa120a36ad"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6227f7a1a9"></a>`collection_id` | yes | [CollectionId](#s-51c0a8da54) |  |
| <a id="s-00e47862a7"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-280acf3f05"></a>definition `ContentObservationEvidence`

- <a id="s-6a71960ac3"></a>`type`: `"object"`
- <a id="s-4c8a0828df"></a>`additionalProperties`: `false`
- <a id="s-ea5aec3c32"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-33903abe50"></a>`request` | yes | [ContentObservationRequest](#s-ebc6138a13) |  |
| <a id="s-ec9bb9aae6"></a>`result` | yes | [ContentObservationResult](#s-71aaec4bf3) |  |

##### <a id="s-68543b0101"></a>definition `ContentObservationFailure`

- <a id="s-f13127473c"></a>`type`: `"object"`
- <a id="s-608a38d71b"></a>`additionalProperties`: `false`
- <a id="s-586bd79e46"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ee5665e38"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1893fa1bce"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-31fffb3f76"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-299f3f3858"></a>definition `ContentObservationInapplicable`

- <a id="s-e80930a68d"></a>`type`: `"object"`
- <a id="s-3412dd28e2"></a>`additionalProperties`: `false`
- <a id="s-3cebb342d7"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ee9513eeb"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-19b9892097"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-ebc6138a13"></a>definition `ContentObservationRequest`

- <a id="s-9cc78c1d1b"></a>`type`: `"object"`
- <a id="s-2cda46c1e5"></a>`additionalProperties`: `false`
- <a id="s-5ea5e839ca"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0317fcd5fb"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-173cc4c479"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-112f94afcb"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9c6d46a77b"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6a7b64bfea"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cb8221e2b4"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-ab04691b2b"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-15c8c167f4"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d2eae56c5"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-464a498840"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-77fdcc38d1)); minItems=1 |  |
| <a id="s-6bc9b356ce"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-ccf37bb288"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-71aaec4bf3"></a>definition `ContentObservationResult`

- <a id="s-0171b5e7e8"></a>`type`: `"object"`
- <a id="s-8292228375"></a>`additionalProperties`: `false`
- <a id="s-5f0264c9a1"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aacdfd61a0"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-7202db2570"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-223b69a7b1))); (type="null")]; default=null |  |
| <a id="s-1b3a6c411f"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-2b849d9051)); (type="null")]; default=null |  |
| <a id="s-7cb3a31cb2"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-7826f4e5de"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-68543b0101)); (type="null")]; default=null |  |
| <a id="s-4ab1a46917"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-40d7537c94"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-299f3f3858)); (type="null")]; default=null |  |
| <a id="s-60d94751d0"></a>`observer` | yes | [ObserverImplementation](#s-32564876db) |  |
| <a id="s-ac05b9c893"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5a8f58b6b2"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b12cf12ca4"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d43bd1a3b4"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-68690d2ce5"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-3392ed81b7"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-77fdcc38d1)); minItems=1 |  |

##### <a id="s-5da467b0d4"></a>definition `ControllerEvidence`

- <a id="s-5884801ac2"></a>`type`: `"object"`
- <a id="s-cdfb4e829f"></a>`additionalProperties`: `false`
- <a id="s-060dcb1021"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac04c1d3b2"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a7dbc3b3ee"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-3bde0bf644) |  |
| <a id="s-2ba8e2b33e"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-fad13d3179"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-e3338fd445"></a>`type`: `"string"`
- <a id="s-92152ef8b4"></a>`enum`: `["encrypted-at-rest","memory-backed"]`

##### <a id="s-745b5b8fa8"></a>definition `EffectPlan`

- <a id="s-fa05e34231"></a>`type`: `"object"`
- <a id="s-8c0528e2d3"></a>`additionalProperties`: `false`
- <a id="s-706afbdea6"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a09054604b"></a>`inputs` | yes | [TargetInputAuthority](#s-b03acb7057) |  |
| <a id="s-6a3cdc8dfc"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-4b9c552d88"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-89cba508d8"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-48e5008152"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8623d0c5c3"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f8b038bf91"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-6af8103be3"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4189be10d4"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e2a88ed195"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |

##### <a id="s-770268a072"></a>definition `EvaluationBinding`

- <a id="s-837ddaf253"></a>`type`: `"object"`
- <a id="s-946a4c6a8f"></a>`additionalProperties`: `false`
- <a id="s-5854aa5c67"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19020a20c0"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-86d6e8a491"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b0294fc73c"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-23260c6198"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-3bde0bf644"></a>definition `ExecutionEnvelope`

- <a id="s-8ab934804d"></a>`type`: `"object"`
- <a id="s-7653535c91"></a>`additionalProperties`: `false`
- <a id="s-c8931672ea"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7817a9d491"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-90551e9738"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-475a134152"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-a2af760c99"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-49d28e3072"></a>`target_plan` | yes | [TargetPlanBinding](#s-e66c6034c6) |  |
| <a id="s-b6eb1b8e7b"></a>`workflow_plan` | yes | [WorkflowPlan](#s-1e85fec743) |  |

##### <a id="s-1b5cb48d41"></a>definition `JoinWorkBinding`

- <a id="s-9da78d5a04"></a>`type`: `"object"`
- <a id="s-357719dd22"></a>`additionalProperties`: `false`
- <a id="s-3dbedcce78"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-67630e478c"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d55840bcb1"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-3d1dc2d8e5"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-78f9d97d1f)); minItems=2 |  |
| <a id="s-d1a03f022d"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-78f9d97d1f"></a>definition `JoinWorkMemberBinding`

- <a id="s-f7a4d5e07b"></a>`type`: `"object"`
- <a id="s-c3e6a19f59"></a>`additionalProperties`: `false`
- <a id="s-f5903e7bba"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a1d810d07"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ca1932419d"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-997d138891"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-c6dd98f761"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2b849d9051"></a>definition `JsonSchemaValidationProfile`

- <a id="s-6b8fb0ff7a"></a>`type`: `"object"`
- <a id="s-754004c376"></a>`additionalProperties`: `false`
- <a id="s-dd5df1371c"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9f59d0d539"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-f8a05181c6"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-30b1e5b7a6"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0af8afc796"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-314e892fba"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |

##### <a id="s-223b69a7b1"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-117600b7b8"></a>definition `NonnegativeDecimal`

- <a id="s-86432bbb70"></a>`type`: `"string"`
- <a id="s-1cd7003a44"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-32564876db"></a>definition `ObserverImplementation`

- <a id="s-a3ab850b5e"></a>`type`: `"object"`
- <a id="s-86f6a6b95e"></a>`additionalProperties`: `false`
- <a id="s-fe4fcf4624"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72fa3333e6"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-61c734d552"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7d07ba5cbc"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-3cd199dd97"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-d8c86f2f70"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-001519c2d8"></a>definition `OperationIdentityRef`

- <a id="s-96a391853d"></a>`type`: `"object"`
- <a id="s-2a0820b8f6"></a>`additionalProperties`: `false`
- <a id="s-0aadd67f60"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-825adf6ae7"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4c25107838"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fdefc9a162"></a>definition `RecipeIdentityRef`

- <a id="s-9a0d38e34a"></a>`type`: `"object"`
- <a id="s-8d5441f4e7"></a>`additionalProperties`: `false`
- <a id="s-98aa83aba7"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-819695520f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3873740d4a"></a>`revision` | yes | [NonnegativeDecimal](#s-117600b7b8); ge=1 |  |
| <a id="s-115dc283b8"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b03acb7057"></a>definition `TargetInputAuthority`

- <a id="s-e008409882"></a>`type`: `"object"`
- <a id="s-e4be40bcbd"></a>`additionalProperties`: `false`
- <a id="s-e53b440769"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88c327bfca"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-20b519275d)); minItems=1 |  |
| <a id="s-732e94b20f"></a>`selection` | yes | [ArtifactSelectionRef](#s-8d29998944) |  |

##### <a id="s-20b519275d"></a>definition `TargetInputRoleCount`

- <a id="s-934aea6540"></a>`type`: `"object"`
- <a id="s-d3e61e4f54"></a>`additionalProperties`: `false`
- <a id="s-b629629823"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b8f42cdd15"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-dfd3f075c1"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-77b308ad62"></a>definition `TargetJobDeclaration`

- <a id="s-3bf032b7d9"></a>`type`: `"object"`
- <a id="s-73e0833e83"></a>`additionalProperties`: `false`
- <a id="s-007830b842"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","declared_workspace_protection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-355fc894a4"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-ddd5fe7e0b"></a>`controller_evidence` | yes | [ControllerEvidence](#s-5da467b0d4) |  |
| <a id="s-a30a12878a"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-fad13d3179) |  |
| <a id="s-a53af36200"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-39f3d74caf"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6d0bb5fd0f"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-c0ca1894ee)); ([EffectPlan](#s-745b5b8fa8))] |  |

##### <a id="s-e66c6034c6"></a>definition `TargetPlanBinding`

- <a id="s-f684aadcd2"></a>`type`: `"object"`
- <a id="s-7cf399c48f"></a>`additionalProperties`: `false`
- <a id="s-851ce2fb6b"></a>`required`: `["protocol","target_implementation_id","target_descriptor_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a15cdab855"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-08f97b6fcd"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-15f9d4edff"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cd96b69aed"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-92fe7d10fd"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fb1d51c07c"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-c0ca1894ee"></a>definition `TransformPlan`

- <a id="s-4d3754d18d"></a>`type`: `"object"`
- <a id="s-ffe15f3c32"></a>`additionalProperties`: `false`
- <a id="s-1c00bd1dd5"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3491438be3"></a>`inputs` | yes | [TargetInputAuthority](#s-b03acb7057) |  |
| <a id="s-1ffc9c7f6c"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-27e28cb6c8"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-37d8579139"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b6ccd87c6d"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a7c7133519"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c46a32ca0a"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-e4c1bb5c40"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b24e090075"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d89c84b6a1"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |

##### <a id="s-77fdcc38d1"></a>definition `WorkArtifactSubject`

- <a id="s-60eadda995"></a>`type`: `"object"`
- <a id="s-04e16d88e0"></a>`additionalProperties`: `false`
- <a id="s-a2d48bd645"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-254712a035"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-79873f9ad7"></a>`collection` | yes | [CollectionRootIdentityRef](#s-355d65e0db) |  |
| <a id="s-af51f788f6"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-0df322f2ad"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-6e19b62245"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-ee7ac7a9e6"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d1ee8ff928"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-77b0ae1511"></a>definition `WorkIdentity`

- <a id="s-20d960e99a"></a>`type`: `"object"`
- <a id="s-923240e071"></a>`additionalProperties`: `false`
- <a id="s-8abc34e001"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ab0d2dbc7"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-8b3f0f3fcb"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-770268a072)); (type="null")]; default=null |  |
| <a id="s-30bd2a5d29"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-c386399dfd)); ([JoinWorkBinding](#s-1b5cb48d41))]); (type="null")]; default=null |  |
| <a id="s-49618377c7"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-cffb2318f2"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-355d65e0db)); minItems=1 |  |
| <a id="s-4cde6976a3"></a>`recipe` | yes | [RecipeIdentityRef](#s-fdefc9a162) |  |
| <a id="s-27e49dc38b"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1e85fec743"></a>definition `WorkflowPlan`

- <a id="s-70bfcebe5d"></a>`type`: `"object"`
- <a id="s-742d534491"></a>`additionalProperties`: `false`
- <a id="s-95ea44c2fe"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-84361baefd"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-796692f528"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-d3f9005374"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-280acf3f05)) |  |
| <a id="s-81f55b3817"></a>`operation` | yes | [OperationIdentityRef](#s-001519c2d8) |  |
| <a id="s-071529a4ab"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-6ad4da7c6f"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-d532923e11"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-b6ccc35374"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-31a94588b2"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-47207e62b0"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5f3055b9d5"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-2862a5239f"></a>`work` | yes | [WorkIdentity](#s-77b0ae1511) |  |
| <a id="s-a68c86d208"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [verify_digest](stove0-target-protocol-acceptedtargetjob-verify-digest.md)

## Governing policies

- <a id="pa-43e2eb2826"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.AcceptedTargetJob`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bafb902997baa3fbfd1f2904eeede90acb8be71fcf664c3b4df5418cdbb67f9d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelectionRef": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
          "type": "object"
        },
        "BranchWorkBinding": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "decision_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "kind": {
              "const": "branch",
              "default": "branch",
              "type": "string"
            },
            "parent_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "parent_work_id",
            "branch_id",
            "decision_sha256",
            "artifact_selection_sha256"
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
        "ContentObservationEvidence": {
          "additionalProperties": false,
          "properties": {
            "request": {
              "$ref": "#/$defs/ContentObservationRequest"
            },
            "result": {
              "$ref": "#/$defs/ContentObservationResult"
            }
          },
          "required": [
            "request",
            "result"
          ],
          "type": "object"
        },
        "ContentObservationFailure": {
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
        "ContentObservationInapplicable": {
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
        "ContentObservationRequest": {
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
                "$ref": "#/$defs/WorkArtifactSubject"
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
        "ContentObservationResult": {
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
                  "$ref": "#/$defs/JsonSchemaValidationProfile"
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
                  "$ref": "#/$defs/ContentObservationFailure"
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
                  "$ref": "#/$defs/ContentObservationInapplicable"
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
                "$ref": "#/$defs/WorkArtifactSubject"
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
        "ControllerEvidence": {
          "additionalProperties": false,
          "properties": {
            "controller_evidence_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "execution_envelope": {
              "$ref": "#/$defs/ExecutionEnvelope"
            },
            "format": {
              "const": "stove0-controller-evidence/v1",
              "default": "stove0-controller-evidence/v1",
              "type": "string"
            }
          },
          "required": [
            "execution_envelope",
            "controller_evidence_sha256"
          ],
          "type": "object"
        },
        "DeclaredWorkspaceProtection": {
          "enum": [
            "encrypted-at-rest",
            "memory-backed"
          ],
          "type": "string"
        },
        "EffectPlan": {
          "additionalProperties": false,
          "properties": {
            "inputs": {
              "$ref": "#/$defs/TargetInputAuthority"
            },
            "intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "observation_result_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-effect-target/v1",
              "default": "stove0-effect-target/v1",
              "type": "string"
            },
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "operation_id",
            "operation_contract_sha256",
            "inputs",
            "intent",
            "target_implementation_id",
            "target_descriptor_sha256",
            "plan_sha256"
          ],
          "type": "object"
        },
        "EvaluationBinding": {
          "additionalProperties": false,
          "properties": {
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "matrix_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "parameters": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "variant_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "evaluation_id",
            "matrix_sha256",
            "variant_id"
          ],
          "type": "object"
        },
        "ExecutionEnvelope": {
          "additionalProperties": false,
          "properties": {
            "claim_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "execution_envelope_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "fence": {
              "minimum": 1,
              "type": "integer"
            },
            "format": {
              "const": "stove0-execution-envelope/v1",
              "default": "stove0-execution-envelope/v1",
              "type": "string"
            },
            "target_plan": {
              "$ref": "#/$defs/TargetPlanBinding"
            },
            "workflow_plan": {
              "$ref": "#/$defs/WorkflowPlan"
            }
          },
          "required": [
            "claim_id",
            "fence",
            "workflow_plan",
            "target_plan",
            "execution_envelope_sha256"
          ],
          "type": "object"
        },
        "JoinWorkBinding": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "kind": {
              "const": "join",
              "default": "join",
              "type": "string"
            },
            "members": {
              "items": {
                "$ref": "#/$defs/JoinWorkMemberBinding"
              },
              "minItems": 2,
              "type": "array"
            },
            "parent_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "parent_work_id",
            "branch_set_sha256",
            "members"
          ],
          "type": "object"
        },
        "JoinWorkMemberBinding": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "producer_settlement_sha256": {
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
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "settlement_sha256",
            "artifact_selection_sha256"
          ],
          "type": "object"
        },
        "JsonSchemaValidationProfile": {
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
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "profile_sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
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
        },
        "OperationIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
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
            "sha256"
          ],
          "type": "object"
        },
        "RecipeIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "type": "object"
        },
        "TargetInputAuthority": {
          "additionalProperties": false,
          "properties": {
            "roles": {
              "items": {
                "$ref": "#/$defs/TargetInputRoleCount"
              },
              "minItems": 1,
              "type": "array"
            },
            "selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            }
          },
          "required": [
            "selection",
            "roles"
          ],
          "type": "object"
        },
        "TargetInputRoleCount": {
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
        "TargetJobDeclaration": {
          "additionalProperties": false,
          "properties": {
            "claim_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "controller_evidence": {
              "$ref": "#/$defs/ControllerEvidence"
            },
            "declared_workspace_protection": {
              "$ref": "#/$defs/DeclaredWorkspaceProtection"
            },
            "fence": {
              "minimum": 1,
              "type": "integer"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "plan": {
              "discriminator": {
                "mapping": {
                  "stove0-effect-target/v1": "#/$defs/EffectPlan",
                  "stove0-transform-target/v1": "#/$defs/TransformPlan"
                },
                "propertyName": "protocol"
              },
              "oneOf": [
                {
                  "$ref": "#/$defs/TransformPlan"
                },
                {
                  "$ref": "#/$defs/EffectPlan"
                }
              ]
            }
          },
          "required": [
            "job_id",
            "claim_id",
            "fence",
            "controller_evidence",
            "plan",
            "declared_workspace_protection"
          ],
          "type": "object"
        },
        "TargetPlanBinding": {
          "additionalProperties": false,
          "properties": {
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "plan": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "protocol": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "protocol",
            "target_implementation_id",
            "target_descriptor_sha256",
            "operation_contract_sha256",
            "plan",
            "plan_sha256"
          ],
          "type": "object"
        },
        "TransformPlan": {
          "additionalProperties": false,
          "properties": {
            "inputs": {
              "$ref": "#/$defs/TargetInputAuthority"
            },
            "intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "observation_result_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-transform-target/v1",
              "default": "stove0-transform-target/v1",
              "type": "string"
            },
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "operation_id",
            "operation_contract_sha256",
            "inputs",
            "intent",
            "target_implementation_id",
            "target_descriptor_sha256",
            "plan_sha256"
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
        },
        "WorkIdentity": {
          "additionalProperties": false,
          "properties": {
            "effective_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "evaluation": {
              "anyOf": [
                {
                  "$ref": "#/$defs/EvaluationBinding"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "fork_join": {
              "anyOf": [
                {
                  "discriminator": {
                    "mapping": {
                      "branch": "#/$defs/BranchWorkBinding",
                      "join": "#/$defs/JoinWorkBinding"
                    },
                    "propertyName": "kind"
                  },
                  "oneOf": [
                    {
                      "$ref": "#/$defs/BranchWorkBinding"
                    },
                    {
                      "$ref": "#/$defs/JoinWorkBinding"
                    }
                  ]
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "format": {
              "const": "stove0-work/v1",
              "default": "stove0-work/v1",
              "type": "string"
            },
            "inputs": {
              "items": {
                "$ref": "#/$defs/CollectionRootIdentityRef"
              },
              "minItems": 1,
              "type": "array"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeIdentityRef"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "recipe",
            "inputs",
            "work_id"
          ],
          "type": "object"
        },
        "WorkflowPlan": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-workflow-plan/v1",
              "default": "stove0-workflow-plan/v1",
              "type": "string"
            },
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "observations": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ContentObservationEvidence"
              },
              "type": "array"
            },
            "operation": {
              "$ref": "#/$defs/OperationIdentityRef"
            },
            "output_policy": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "requested_target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "result_kind": {
              "default": "collection",
              "enum": [
                "collection",
                "external-effect"
              ],
              "type": "string"
            },
            "source_collection_retirement_grace_seconds": {
              "default": 0,
              "minimum": 0,
              "type": "integer"
            },
            "source_collection_retirement_policy": {
              "default": "retain",
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "type": "string"
            },
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_registration_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
              "type": "string"
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            },
            "workflow_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work",
            "operation",
            "target_registration_id",
            "target_descriptor_sha256",
            "workflow_plan_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "declaration": {
          "$ref": "#/$defs/TargetJobDeclaration"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "declaration",
        "request_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "AcceptedTargetJob",
  "unit": "export"
}
```

</details>
