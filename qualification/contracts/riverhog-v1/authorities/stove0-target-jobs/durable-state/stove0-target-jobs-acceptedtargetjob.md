# stove0-target-jobs: AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-acceptedtargetjob:9f4d08e69f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-jobs](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-895464670c"></a>

- Document: `AcceptedTargetJob`

### Document schema

<a id="s-e1f1741ba6"></a>

- <a id="s-be8c95f916"></a>`type`: `"object"`
- <a id="s-0a933b8dbe"></a>`additionalProperties`: `false`
- <a id="s-73f0ef927b"></a>`description`: `"Durable, non-secret identity of one accepted target job request."`
- <a id="s-78c2fd66ba"></a>`required`: `["declaration","request_sha256"]`
- <a id="s-6e33c0235a"></a>`title`: `"AcceptedTargetJob"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-405daa5385"></a>`declaration` | yes | [TargetJobDeclaration](#s-91f55fb248) |  |
| <a id="s-61c2c95c95"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### Definitions

- [ArtifactSelectionRef](#s-973e2a3365)
- [ArtifactSubject](#s-6be2fdc787)
- [BranchWorkBinding](#s-548c2def56)
- [CollectionId](#s-cc22d9cbdd)
- [CollectionRootRef](#s-3b4ac5edde)
- [ControllerEvidence](#s-5e0010cbf3)
- [EffectPlan](#s-9469da79f1)
- [EvaluationBinding](#s-f025905d6a)
- [ExecutionEnvelope](#s-033c077b2b)
- [JoinWorkBinding](#s-53a18f36fb)
- [JoinWorkMemberBinding](#s-82dd57ddfa)
- [JsonSchemaDocument](#s-8c4f8b0d2b)
- [JsonValue](#s-326d0b99e9)
- [ObservationEvidence](#s-35224d38b2)
- [ObservationFailure](#s-ee43532728)
- [ObservationInapplicable](#s-063d130a3c)
- [ObservationRequest](#s-df4feebae2)
- [ObservationResult](#s-3073671d72)
- [ObserverImplementation](#s-c48c7036bd)
- [OperationRef](#s-e89fe7446a)
- [RecipeRef](#s-cee329be31)
- [TargetInputAuthority](#s-31aadefae6)
- [TargetInputRoleCount](#s-491d231ef9)
- [TargetJobDeclaration](#s-91f55fb248)
- [TargetPlanBinding](#s-9e23b75bbf)
- [TransformPlan](#s-a54e765c9b)
- [WorkIdentity](#s-7b040f3044)
- [WorkflowPlan](#s-7278f1b514)

#### <a id="s-973e2a3365"></a>definition `ArtifactSelectionRef`

- <a id="s-f757bcc749"></a>`type`: `"object"`
- <a id="s-cb56526d55"></a>`additionalProperties`: `false`
- <a id="s-ffea015db5"></a>`description`: `"Closed reference to a separately retained selection document."`
- <a id="s-2c578cc85c"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`
- <a id="s-ad1b31b5dd"></a>`title`: `"ArtifactSelectionRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4b315d4d06"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-079ddc42e2"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-60c15f2059"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

#### <a id="s-6be2fdc787"></a>definition `ArtifactSubject`

- <a id="s-dbaf3c49fa"></a>`type`: `"object"`
- <a id="s-7d82a6577c"></a>`additionalProperties`: `false`
- <a id="s-2cc9905b13"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-5d76770f9a"></a>`title`: `"ArtifactSubject"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce054e8488"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-c0cf31d463"></a>`collection` | yes | [CollectionRootRef](#s-3b4ac5edde) |  |
| <a id="s-f51d826361"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-91dee40850"></a>`media_type` | no | anyOf=(type="string"; maxLength=255; minLength=1) \| (type="null"); default=null |  |
| <a id="s-ed55fd10a5"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-02a9762ab3"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8d312c3d06"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-548c2def56"></a>definition `BranchWorkBinding`

- <a id="s-c3bd547f4c"></a>`type`: `"object"`
- <a id="s-31d3ce56c4"></a>`additionalProperties`: `false`
- <a id="s-4579edb17d"></a>`description`: `"Stable parent/branch lineage for one ordinary child work identity."`
- <a id="s-50108f11dc"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`
- <a id="s-c80fc910b8"></a>`title`: `"BranchWorkBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-87812a5524"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0001298fc2"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-811cb06e1c"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9626fa2199"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-7c5e005bd9"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-cc22d9cbdd"></a>definition `CollectionId`

- <a id="s-94fbc3278d"></a>`type`: `"integer"`
- <a id="s-15282d711e"></a>`minimum`: `1`

#### <a id="s-3b4ac5edde"></a>definition `CollectionRootRef`

- <a id="s-bcef7a0469"></a>`type`: `"object"`
- <a id="s-64692871ef"></a>`additionalProperties`: `false`
- <a id="s-06e87185c3"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-8d7a8aafa6"></a>`title`: `"CollectionRootRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95d56d20b1"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-802754c36e"></a>`collection_id` | yes | [CollectionId](#s-cc22d9cbdd) |  |
| <a id="s-e600c026b1"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-5e0010cbf3"></a>definition `ControllerEvidence`

- <a id="s-30a0444f98"></a>`type`: `"object"`
- <a id="s-bfa5385832"></a>`additionalProperties`: `false`
- <a id="s-4cd61b1f3c"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`
- <a id="s-d0ec1bd701"></a>`title`: `"ControllerEvidence"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05ddbf913f"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0ce6f1aabf"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-033c077b2b) |  |
| <a id="s-774a81b449"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

#### <a id="s-9469da79f1"></a>definition `EffectPlan`

- <a id="s-f81d3e446d"></a>`type`: `"object"`
- <a id="s-d3190fcca6"></a>`additionalProperties`: `false`
- <a id="s-8a628ee1ab"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`
- <a id="s-61cb5c0d08"></a>`title`: `"EffectPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b217fb6912"></a>`inputs` | yes | [TargetInputAuthority](#s-31aadefae6) |  |
| <a id="s-44a52c8816"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-a164779555"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-87f4e225be"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-850ea99761"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4e7e73d6a7"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7e58ba9b25"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-dabb526351"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4250f8bf28"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5fd579adb8"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |

#### <a id="s-f025905d6a"></a>definition `EvaluationBinding`

- <a id="s-b558d55ed5"></a>`type`: `"object"`
- <a id="s-b91b6668be"></a>`additionalProperties`: `false`
- <a id="s-f7ffa14e06"></a>`description`: `"Immutable membership of one work item in a trial/evaluation matrix."`
- <a id="s-2284f31291"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`
- <a id="s-7249deaf7f"></a>`title`: `"EvaluationBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dfafd92cd5"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6529e05242"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3fa2221975"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-1ba1ae9722"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

#### <a id="s-033c077b2b"></a>definition `ExecutionEnvelope`

- <a id="s-3ba0060bea"></a>`type`: `"object"`
- <a id="s-1f2d406fe4"></a>`additionalProperties`: `false`
- <a id="s-e9fe5ac592"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`
- <a id="s-2755f31872"></a>`title`: `"ExecutionEnvelope"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6db7f617c8"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-00b0c1dd57"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d8bdf98b9e"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-a6adf0fb25"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-0e2227325b"></a>`target_plan` | yes | [TargetPlanBinding](#s-9e23b75bbf) |  |
| <a id="s-451c5081f6"></a>`workflow_plan` | yes | [WorkflowPlan](#s-7278f1b514) |  |

#### <a id="s-53a18f36fb"></a>definition `JoinWorkBinding`

- <a id="s-5d44f8f4d4"></a>`type`: `"object"`
- <a id="s-23f5d1a91b"></a>`additionalProperties`: `false`
- <a id="s-b03ca01888"></a>`description`: `"Stable branch-set lineage for one ordinary join work identity."`
- <a id="s-012e82bc04"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`
- <a id="s-b721b6de65"></a>`title`: `"JoinWorkBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eba5c7f57d"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-293e0b5cb5"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-12a929917c"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-82dd57ddfa)); minItems=2 |  |
| <a id="s-dac8189dfc"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-82dd57ddfa"></a>definition `JoinWorkMemberBinding`

- <a id="s-f21c31e836"></a>`type`: `"object"`
- <a id="s-e80498e632"></a>`additionalProperties`: `false`
- <a id="s-8f1a1e63b3"></a>`description`: `"Exact successful branch result used to derive one join work identity."`
- <a id="s-92e03588f5"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`
- <a id="s-de895a3c99"></a>`title`: `"JoinWorkMemberBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a13c3e6460"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-547f922bf6"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d2351954dd"></a>`producer_settlement_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |
| <a id="s-07e92a2b73"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-8c4f8b0d2b"></a>definition `JsonSchemaDocument`

- <a id="s-50bef47ded"></a>`type`: `"object"`
- <a id="s-8e2e95d057"></a>`additionalProperties`: `false`
- <a id="s-a207d4c673"></a>`required`: `["id","sha256","schema"]`
- <a id="s-31488951a7"></a>`title`: `"JsonSchemaDocument"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6dbf79486f"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-b84f6e9c13"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-5698ad3b1e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-969fe40f76"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-b48e2cb4ac"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-326d0b99e9"></a>definition `JsonValue`

- Accepts: any JSON value.

#### <a id="s-35224d38b2"></a>definition `ObservationEvidence`

- <a id="s-d6d9ede678"></a>`type`: `"object"`
- <a id="s-65877a522c"></a>`additionalProperties`: `false`
- <a id="s-15ffdf89eb"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-e22dfd61b0"></a>`required`: `["request","result"]`
- <a id="s-ab50340c25"></a>`title`: `"ObservationEvidence"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f4b23d0ea"></a>`request` | yes | [ObservationRequest](#s-df4feebae2) |  |
| <a id="s-8b5069ec74"></a>`result` | yes | [ObservationResult](#s-3073671d72) |  |

#### <a id="s-ee43532728"></a>definition `ObservationFailure`

- <a id="s-cf1bd87964"></a>`type`: `"object"`
- <a id="s-2e2f932655"></a>`additionalProperties`: `false`
- <a id="s-ad6596ff95"></a>`required`: `["code","message","retryable"]`
- <a id="s-4e39c7b86e"></a>`title`: `"ObservationFailure"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3648f5aa44"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2888749f14"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-e1801238ab"></a>`retryable` | yes | type="boolean" |  |

#### <a id="s-063d130a3c"></a>definition `ObservationInapplicable`

- <a id="s-303599ff2c"></a>`type`: `"object"`
- <a id="s-7d1992b8d7"></a>`additionalProperties`: `false`
- <a id="s-b2a9bada26"></a>`required`: `["code","message"]`
- <a id="s-4d52c389a0"></a>`title`: `"ObservationInapplicable"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca16a4e2ee"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-05ce5149d6"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

#### <a id="s-df4feebae2"></a>definition `ObservationRequest`

- <a id="s-10e8e0d079"></a>`type`: `"object"`
- <a id="s-08efbade58"></a>`additionalProperties`: `false`
- <a id="s-869a8d97bc"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-faefc37497"></a>`title`: `"ObservationRequest"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c55d52e11e"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-b395c2bf00"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-c218ae48a2"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ce241d0ac2"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ee8fb47f85"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7c68a13b44"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-2a6df5d5f6"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-23d0a7cbf4"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-03b42f25a8"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-1e75a0edb9"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6be2fdc787)); minItems=1 |  |
| <a id="s-03486fda41"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-faa98f7624"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-3073671d72"></a>definition `ObservationResult`

- <a id="s-8e10bf1214"></a>`type`: `"object"`
- <a id="s-ea5bdbca34"></a>`additionalProperties`: `false`
- <a id="s-9c1159eb3b"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-5900e42dcd"></a>`title`: `"ObservationResult"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e6daecbddf"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-30ef3ffd48"></a>`facts` | no | anyOf=(type="object"; additionalProperties=([JsonValue](#s-326d0b99e9))) \| (type="null"); default=null |  |
| <a id="s-ff9ba3ed23"></a>`facts_schema` | no | anyOf=([JsonSchemaDocument](#s-8c4f8b0d2b)) \| (type="null"); default=null |  |
| <a id="s-5957c11276"></a>`facts_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |
| <a id="s-30786e76d4"></a>`failure` | no | anyOf=([ObservationFailure](#s-ee43532728)) \| (type="null"); default=null |  |
| <a id="s-d9341dd0a5"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-c251a8810d"></a>`inapplicable` | no | anyOf=([ObservationInapplicable](#s-063d130a3c)) \| (type="null"); default=null |  |
| <a id="s-0e44867407"></a>`observer` | yes | [ObserverImplementation](#s-c48c7036bd) |  |
| <a id="s-679ad9265d"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e78bafe5be"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c927c1fa47"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3f0df2a4f1"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-68f46ba145"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-6fe3a5f5ea"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6be2fdc787)); minItems=1 |  |

#### <a id="s-c48c7036bd"></a>definition `ObserverImplementation`

- <a id="s-065a701a6d"></a>`type`: `"object"`
- <a id="s-4f9d44be43"></a>`additionalProperties`: `false`
- <a id="s-b546b34fcb"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-d485f73065"></a>`title`: `"ObserverImplementation"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac5ef790b8"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-474c3756e8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-17d9bd7154"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-877c3f9c32"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-8afcb4acd2"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

#### <a id="s-e89fe7446a"></a>definition `OperationRef`

- <a id="s-f432947f67"></a>`type`: `"object"`
- <a id="s-c7481956be"></a>`additionalProperties`: `false`
- <a id="s-f6bee63b03"></a>`required`: `["id","sha256"]`
- <a id="s-844ca9aadf"></a>`title`: `"OperationRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f454a06a05"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-92a1644de9"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-cee329be31"></a>definition `RecipeRef`

- <a id="s-6c0ed8e1d5"></a>`type`: `"object"`
- <a id="s-24527512be"></a>`additionalProperties`: `false`
- <a id="s-b7ebf41392"></a>`required`: `["id","revision","sha256"]`
- <a id="s-1d2d32e636"></a>`title`: `"RecipeRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2217552652"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-59f87aa275"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-106ba04369"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-31aadefae6"></a>definition `TargetInputAuthority`

- <a id="s-5e855f3f97"></a>`type`: `"object"`
- <a id="s-1ee7ba5afd"></a>`additionalProperties`: `false`
- <a id="s-6f4b15e191"></a>`description`: `"Small exact input authority retained by Stove0 and traversed in bounded pages."`
- <a id="s-a64f06c2f9"></a>`required`: `["selection","roles"]`
- <a id="s-e656a1041e"></a>`title`: `"TargetInputAuthority"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1207d9d265"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-491d231ef9)); minItems=1 |  |
| <a id="s-a21a5fff83"></a>`selection` | yes | [ArtifactSelectionRef](#s-973e2a3365) |  |

#### <a id="s-491d231ef9"></a>definition `TargetInputRoleCount`

- <a id="s-4ca72a3d2e"></a>`type`: `"object"`
- <a id="s-9ed2414aeb"></a>`additionalProperties`: `false`
- <a id="s-100c38c4ab"></a>`required`: `["role","count"]`
- <a id="s-cb8a910e75"></a>`title`: `"TargetInputRoleCount"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-16503d3716"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-23d3901aed"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

#### <a id="s-91f55fb248"></a>definition `TargetJobDeclaration`

- <a id="s-1cbb31e22f"></a>`type`: `"object"`
- <a id="s-cf64d0603c"></a>`additionalProperties`: `false`
- <a id="s-b4ba3ddb8e"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`
- <a id="s-387701f355"></a>`title`: `"TargetJobDeclaration"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9cdbb4f748"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-1c3bb8836a"></a>`controller_evidence` | yes | [ControllerEvidence](#s-5e0010cbf3) |  |
| <a id="s-42eeb85923"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-334811f276"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-15fd4a7b19"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"[EffectPlan](#s-9469da79f1)","stove0-transform-target/v1":"[TransformPlan](#s-a54e765c9b)"},"propertyName":"protocol"}; oneOf=([TransformPlan](#s-a54e765c9b)) \| ([EffectPlan](#s-9469da79f1)) |  |
| <a id="s-6e06052b57"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

#### <a id="s-9e23b75bbf"></a>definition `TargetPlanBinding`

- <a id="s-e71f02c218"></a>`type`: `"object"`
- <a id="s-34d3315332"></a>`additionalProperties`: `false`
- <a id="s-540d1c2df2"></a>`description`: `"Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope."`
- <a id="s-6678960845"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`
- <a id="s-a562aacd93"></a>`title`: `"TargetPlanBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d475634be"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c74c7177ff"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-0e566e1dd9"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-39e707e0dc"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c038454d1d"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5331352fef"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

#### <a id="s-a54e765c9b"></a>definition `TransformPlan`

- <a id="s-b2134c7728"></a>`type`: `"object"`
- <a id="s-374327d768"></a>`additionalProperties`: `false`
- <a id="s-0153a5f65b"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`
- <a id="s-5045bcbf72"></a>`title`: `"TransformPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ef53d1dbf0"></a>`inputs` | yes | [TargetInputAuthority](#s-31aadefae6) |  |
| <a id="s-7afc441650"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-33c173195e"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-ce52962d65"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6559bbf52f"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-03cca7c3f1"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ba23a56a22"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-1c3f62c019"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b69bbd54d7"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b706c75894"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |

#### <a id="s-7b040f3044"></a>definition `WorkIdentity`

- <a id="s-a2285802ad"></a>`type`: `"object"`
- <a id="s-c07c0d5e25"></a>`additionalProperties`: `false`
- <a id="s-4b513a3bd9"></a>`required`: `["recipe","inputs","work_id"]`
- <a id="s-8f6d8678d2"></a>`title`: `"WorkIdentity"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c1d5db82e9"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-4e0d6b3ed9"></a>`evaluation` | no | anyOf=([EvaluationBinding](#s-f025905d6a)) \| (type="null"); default=null |  |
| <a id="s-a2aefa59d5"></a>`fork_join` | no | anyOf=(discriminator={"mapping":{"branch":"[BranchWorkBinding](#s-548c2def56)","join":"[JoinWorkBinding](#s-53a18f36fb)"},"propertyName":"kind"}; oneOf=([BranchWorkBinding](#s-548c2def56)) \| ([JoinWorkBinding](#s-53a18f36fb))) \| (type="null"); default=null |  |
| <a id="s-165aecfd8d"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-a1144978a0"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-3b4ac5edde)); minItems=1 |  |
| <a id="s-e21bf10dc2"></a>`recipe` | yes | [RecipeRef](#s-cee329be31) |  |
| <a id="s-fe9e508578"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

#### <a id="s-7278f1b514"></a>definition `WorkflowPlan`

- <a id="s-7280c421ee"></a>`type`: `"object"`
- <a id="s-d4c873a46a"></a>`additionalProperties`: `false`
- <a id="s-5e821c9586"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`
- <a id="s-44e0a173ed"></a>`title`: `"WorkflowPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-17fea187b0"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-66d496dce3"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-d7f3d36782"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-35224d38b2)) |  |
| <a id="s-038e085626"></a>`operation` | yes | [OperationRef](#s-e89fe7446a) |  |
| <a id="s-4c734df6e1"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-fa3c0884bf"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)) |  |
| <a id="s-e389fcca4c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-8d0c35340b"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-ca6d71b3f0"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-bb850cc0f0"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0def0e07f6"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-4f0c6f0bce"></a>`work` | yes | [WorkIdentity](#s-7b040f3044) |  |
| <a id="s-e2a7a4a0c1"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-target-jobs-durable-state-identity.md)

## Governing policies

- <a id="pa-899b241fe4"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-target-jobs](../../../evidence/sources.md#src-7b4138829a) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py`

### Machine authority

- `/external_contract/durable_state/owners/5/structure/documents/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bfd68b20b927724bb6a948754e7626732e555bd7c38d79a2f1524e14a408f0e4 -->

```json
{
  "id": "AcceptedTargetJob",
  "schema": {
    "$defs": {
      "ArtifactSelectionRef": {
        "additionalProperties": false,
        "description": "Closed reference to a separately retained selection document.",
        "properties": {
          "artifact_count": {
            "minimum": 1,
            "title": "Artifact Count",
            "type": "integer"
          },
          "selection_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Selection Sha256",
            "type": "string"
          },
          "total_bytes": {
            "minimum": 0,
            "title": "Total Bytes",
            "type": "integer"
          }
        },
        "required": [
          "selection_sha256",
          "artifact_count",
          "total_bytes"
        ],
        "title": "ArtifactSelectionRef",
        "type": "object"
      },
      "ArtifactSubject": {
        "additionalProperties": false,
        "properties": {
          "bytes": {
            "minimum": 0,
            "title": "Bytes",
            "type": "integer"
          },
          "collection": {
            "$ref": "#/$defs/CollectionRootRef"
          },
          "id": {
            "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
            "title": "Id",
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
            "default": null,
            "title": "Media Type"
          },
          "path": {
            "maxLength": 4096,
            "minLength": 1,
            "title": "Path",
            "type": "string"
          },
          "role": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Role",
            "type": "string"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
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
        "title": "ArtifactSubject",
        "type": "object"
      },
      "BranchWorkBinding": {
        "additionalProperties": false,
        "description": "Stable parent/branch lineage for one ordinary child work identity.",
        "properties": {
          "artifact_selection_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Artifact Selection Sha256",
            "type": "string"
          },
          "branch_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Branch Id",
            "type": "string"
          },
          "decision_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Decision Sha256",
            "type": "string"
          },
          "kind": {
            "const": "branch",
            "default": "branch",
            "title": "Kind",
            "type": "string"
          },
          "parent_work_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Parent Work Id",
            "type": "string"
          }
        },
        "required": [
          "parent_work_id",
          "branch_id",
          "decision_sha256",
          "artifact_selection_sha256"
        ],
        "title": "BranchWorkBinding",
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
          }
        },
        "required": [
          "collection_id",
          "archive_root_sha256",
          "content_identity"
        ],
        "title": "CollectionRootRef",
        "type": "object"
      },
      "ControllerEvidence": {
        "additionalProperties": false,
        "properties": {
          "controller_evidence_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Controller Evidence Sha256",
            "type": "string"
          },
          "execution_envelope": {
            "$ref": "#/$defs/ExecutionEnvelope"
          },
          "format": {
            "const": "stove0-controller-evidence/v1",
            "default": "stove0-controller-evidence/v1",
            "title": "Format",
            "type": "string"
          }
        },
        "required": [
          "execution_envelope",
          "controller_evidence_sha256"
        ],
        "title": "ControllerEvidence",
        "type": "object"
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
            "title": "Intent",
            "type": "object"
          },
          "observation_result_sha256s": {
            "default": [],
            "items": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "title": "Observation Result Sha256S",
            "type": "array"
          },
          "operation_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Operation Contract Sha256",
            "type": "string"
          },
          "operation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Operation Id",
            "type": "string"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "protocol": {
            "const": "stove0-effect-target/v1",
            "default": "stove0-effect-target/v1",
            "title": "Protocol",
            "type": "string"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          },
          "target_implementation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Target Implementation Id",
            "type": "string"
          },
          "target_options": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Target Options",
            "type": "object"
          }
        },
        "required": [
          "operation_id",
          "operation_contract_sha256",
          "inputs",
          "intent",
          "target_implementation_id",
          "target_contract_sha256",
          "plan_sha256"
        ],
        "title": "EffectPlan",
        "type": "object"
      },
      "EvaluationBinding": {
        "additionalProperties": false,
        "description": "Immutable membership of one work item in a trial/evaluation matrix.",
        "properties": {
          "evaluation_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Evaluation Id",
            "type": "string"
          },
          "matrix_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Matrix Sha256",
            "type": "string"
          },
          "parameters": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Parameters",
            "type": "object"
          },
          "variant_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Variant Id",
            "type": "string"
          }
        },
        "required": [
          "evaluation_id",
          "matrix_sha256",
          "variant_id"
        ],
        "title": "EvaluationBinding",
        "type": "object"
      },
      "ExecutionEnvelope": {
        "additionalProperties": false,
        "properties": {
          "claim_id": {
            "maxLength": 160,
            "minLength": 1,
            "title": "Claim Id",
            "type": "string"
          },
          "execution_envelope_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Execution Envelope Sha256",
            "type": "string"
          },
          "fence": {
            "minimum": 1,
            "title": "Fence",
            "type": "integer"
          },
          "format": {
            "const": "stove0-execution-envelope/v1",
            "default": "stove0-execution-envelope/v1",
            "title": "Format",
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
        "title": "ExecutionEnvelope",
        "type": "object"
      },
      "JoinWorkBinding": {
        "additionalProperties": false,
        "description": "Stable branch-set lineage for one ordinary join work identity.",
        "properties": {
          "branch_set_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Branch Set Sha256",
            "type": "string"
          },
          "kind": {
            "const": "join",
            "default": "join",
            "title": "Kind",
            "type": "string"
          },
          "members": {
            "items": {
              "$ref": "#/$defs/JoinWorkMemberBinding"
            },
            "minItems": 2,
            "title": "Members",
            "type": "array"
          },
          "parent_work_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Parent Work Id",
            "type": "string"
          }
        },
        "required": [
          "parent_work_id",
          "branch_set_sha256",
          "members"
        ],
        "title": "JoinWorkBinding",
        "type": "object"
      },
      "JoinWorkMemberBinding": {
        "additionalProperties": false,
        "description": "Exact successful branch result used to derive one join work identity.",
        "properties": {
          "artifact_selection_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Artifact Selection Sha256",
            "type": "string"
          },
          "branch_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Branch Id",
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
            "default": null,
            "title": "Producer Settlement Sha256"
          },
          "settlement_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Settlement Sha256",
            "type": "string"
          }
        },
        "required": [
          "branch_id",
          "settlement_sha256",
          "artifact_selection_sha256"
        ],
        "title": "JoinWorkMemberBinding",
        "type": "object"
      },
      "JsonSchemaDocument": {
        "additionalProperties": false,
        "properties": {
          "dialect": {
            "const": "https://json-schema.org/draft/2020-12/schema",
            "default": "https://json-schema.org/draft/2020-12/schema",
            "title": "Dialect",
            "type": "string"
          },
          "format_policy": {
            "const": "annotation-only",
            "default": "annotation-only",
            "title": "Format Policy",
            "type": "string"
          },
          "id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "schema": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Schema",
            "type": "object"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          }
        },
        "required": [
          "id",
          "sha256",
          "schema"
        ],
        "title": "JsonSchemaDocument",
        "type": "object"
      },
      "JsonValue": {},
      "ObservationEvidence": {
        "additionalProperties": false,
        "description": "Complete routing evidence: immutable request plus accepted result.",
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
        "title": "ObservationEvidence",
        "type": "object"
      },
      "ObservationFailure": {
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
        "title": "ObservationFailure",
        "type": "object"
      },
      "ObservationInapplicable": {
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
        "title": "ObservationInapplicable",
        "type": "object"
      },
      "ObservationRequest": {
        "additionalProperties": false,
        "properties": {
          "format": {
            "const": "stove0-observation-request/v1",
            "default": "stove0-observation-request/v1",
            "title": "Format",
            "type": "string"
          },
          "maximum_result_bytes": {
            "default": 1048576,
            "maximum": 67108864,
            "minimum": 1,
            "title": "Maximum Result Bytes",
            "type": "integer"
          },
          "observer_contract_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Observer Contract Id",
            "type": "string"
          },
          "observer_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Observer Contract Sha256",
            "type": "string"
          },
          "observer_descriptor_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Observer Descriptor Sha256",
            "type": "string"
          },
          "observer_registration_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
            "title": "Observer Registration Id",
            "type": "string"
          },
          "options": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Options",
            "type": "object"
          },
          "request_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Request Id",
            "type": "string"
          },
          "retrieval_policy": {
            "default": "available-only",
            "enum": [
              "available-only",
              "allow"
            ],
            "title": "Retrieval Policy",
            "type": "string"
          },
          "subjects": {
            "items": {
              "$ref": "#/$defs/ArtifactSubject"
            },
            "minItems": 1,
            "title": "Subjects",
            "type": "array"
          },
          "timeout_seconds": {
            "default": 300,
            "maximum": 86400,
            "minimum": 1,
            "title": "Timeout Seconds",
            "type": "integer"
          },
          "work_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Work Id",
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
        "title": "ObservationRequest",
        "type": "object"
      },
      "ObservationResult": {
        "additionalProperties": false,
        "properties": {
          "execution_evidence": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Execution Evidence",
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
            "default": null,
            "title": "Facts"
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
            "default": null,
            "title": "Facts Sha256"
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
            "title": "Format",
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
            "title": "Observer Contract Id",
            "type": "string"
          },
          "observer_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Observer Contract Sha256",
            "type": "string"
          },
          "request_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Request Id",
            "type": "string"
          },
          "result_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Result Sha256",
            "type": "string"
          },
          "state": {
            "enum": [
              "observed",
              "inapplicable",
              "failed",
              "canceled"
            ],
            "title": "State",
            "type": "string"
          },
          "subjects": {
            "items": {
              "$ref": "#/$defs/ArtifactSubject"
            },
            "minItems": 1,
            "title": "Subjects",
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
        "title": "ObservationResult",
        "type": "object"
      },
      "ObserverImplementation": {
        "additionalProperties": false,
        "properties": {
          "descriptor_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Descriptor Sha256",
            "type": "string"
          },
          "id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "protocol": {
            "const": "stove0-content-observer/v1",
            "default": "stove0-content-observer/v1",
            "title": "Protocol",
            "type": "string"
          },
          "source_revision": {
            "maxLength": 200,
            "minLength": 1,
            "title": "Source Revision",
            "type": "string"
          },
          "version": {
            "maxLength": 120,
            "minLength": 1,
            "title": "Version",
            "type": "string"
          }
        },
        "required": [
          "id",
          "version",
          "source_revision",
          "descriptor_sha256"
        ],
        "title": "ObserverImplementation",
        "type": "object"
      },
      "OperationRef": {
        "additionalProperties": false,
        "properties": {
          "id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          }
        },
        "required": [
          "id",
          "sha256"
        ],
        "title": "OperationRef",
        "type": "object"
      },
      "RecipeRef": {
        "additionalProperties": false,
        "properties": {
          "id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "revision": {
            "minimum": 1,
            "title": "Revision",
            "type": "integer"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          }
        },
        "required": [
          "id",
          "revision",
          "sha256"
        ],
        "title": "RecipeRef",
        "type": "object"
      },
      "TargetInputAuthority": {
        "additionalProperties": false,
        "description": "Small exact input authority retained by Stove0 and traversed in bounded pages.",
        "properties": {
          "roles": {
            "items": {
              "$ref": "#/$defs/TargetInputRoleCount"
            },
            "minItems": 1,
            "title": "Roles",
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
        "title": "TargetInputAuthority",
        "type": "object"
      },
      "TargetInputRoleCount": {
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
        "title": "TargetInputRoleCount",
        "type": "object"
      },
      "TargetJobDeclaration": {
        "additionalProperties": false,
        "properties": {
          "claim_id": {
            "maxLength": 160,
            "minLength": 1,
            "title": "Claim Id",
            "type": "string"
          },
          "controller_evidence": {
            "$ref": "#/$defs/ControllerEvidence"
          },
          "fence": {
            "minimum": 1,
            "title": "Fence",
            "type": "integer"
          },
          "job_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Job Id",
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
            ],
            "title": "Plan"
          },
          "workspace_assurance": {
            "enum": [
              "encrypted",
              "ephemeral"
            ],
            "title": "Workspace Assurance",
            "type": "string"
          }
        },
        "required": [
          "job_id",
          "claim_id",
          "fence",
          "controller_evidence",
          "plan",
          "workspace_assurance"
        ],
        "title": "TargetJobDeclaration",
        "type": "object"
      },
      "TargetPlanBinding": {
        "additionalProperties": false,
        "description": "Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope.",
        "properties": {
          "operation_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Operation Contract Sha256",
            "type": "string"
          },
          "plan": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Plan",
            "type": "object"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "protocol": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Protocol",
            "type": "string"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          },
          "target_implementation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Target Implementation Id",
            "type": "string"
          }
        },
        "required": [
          "protocol",
          "target_implementation_id",
          "target_contract_sha256",
          "operation_contract_sha256",
          "plan",
          "plan_sha256"
        ],
        "title": "TargetPlanBinding",
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
            "title": "Intent",
            "type": "object"
          },
          "observation_result_sha256s": {
            "default": [],
            "items": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "title": "Observation Result Sha256S",
            "type": "array"
          },
          "operation_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Operation Contract Sha256",
            "type": "string"
          },
          "operation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Operation Id",
            "type": "string"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "protocol": {
            "const": "stove0-transform-target/v1",
            "default": "stove0-transform-target/v1",
            "title": "Protocol",
            "type": "string"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          },
          "target_implementation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Target Implementation Id",
            "type": "string"
          },
          "target_options": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Target Options",
            "type": "object"
          }
        },
        "required": [
          "operation_id",
          "operation_contract_sha256",
          "inputs",
          "intent",
          "target_implementation_id",
          "target_contract_sha256",
          "plan_sha256"
        ],
        "title": "TransformPlan",
        "type": "object"
      },
      "WorkIdentity": {
        "additionalProperties": false,
        "properties": {
          "effective_intent": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Effective Intent",
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
            "default": null,
            "title": "Fork Join"
          },
          "format": {
            "const": "stove0-work/v1",
            "default": "stove0-work/v1",
            "title": "Format",
            "type": "string"
          },
          "inputs": {
            "items": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "minItems": 1,
            "title": "Inputs",
            "type": "array"
          },
          "recipe": {
            "$ref": "#/$defs/RecipeRef"
          },
          "work_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Work Id",
            "type": "string"
          }
        },
        "required": [
          "recipe",
          "inputs",
          "work_id"
        ],
        "title": "WorkIdentity",
        "type": "object"
      },
      "WorkflowPlan": {
        "additionalProperties": false,
        "properties": {
          "format": {
            "const": "stove0-workflow-plan/v1",
            "default": "stove0-workflow-plan/v1",
            "title": "Format",
            "type": "string"
          },
          "input_retrieval_policy": {
            "default": "available-only",
            "enum": [
              "available-only",
              "allow"
            ],
            "title": "Input Retrieval Policy",
            "type": "string"
          },
          "observations": {
            "default": [],
            "items": {
              "$ref": "#/$defs/ObservationEvidence"
            },
            "title": "Observations",
            "type": "array"
          },
          "operation": {
            "$ref": "#/$defs/OperationRef"
          },
          "output_policy": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Output Policy",
            "type": "object"
          },
          "requested_target_options": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Requested Target Options",
            "type": "object"
          },
          "result_kind": {
            "default": "collection",
            "enum": [
              "collection",
              "external-effect"
            ],
            "title": "Result Kind",
            "type": "string"
          },
          "retirement_grace_seconds": {
            "default": 0,
            "minimum": 0,
            "title": "Retirement Grace Seconds",
            "type": "integer"
          },
          "retirement_policy": {
            "default": "retain",
            "enum": [
              "retain",
              "retire-after-verified-output"
            ],
            "title": "Retirement Policy",
            "type": "string"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          },
          "target_registration_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
            "title": "Target Registration Id",
            "type": "string"
          },
          "work": {
            "$ref": "#/$defs/WorkIdentity"
          },
          "workflow_plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Workflow Plan Sha256",
            "type": "string"
          }
        },
        "required": [
          "work",
          "operation",
          "target_registration_id",
          "target_contract_sha256",
          "workflow_plan_sha256"
        ],
        "title": "WorkflowPlan",
        "type": "object"
      }
    },
    "additionalProperties": false,
    "description": "Durable, non-secret identity of one accepted target job request.",
    "properties": {
      "declaration": {
        "$ref": "#/$defs/TargetJobDeclaration"
      },
      "request_sha256": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Request Sha256",
        "type": "string"
      }
    },
    "required": [
      "declaration",
      "request_sha256"
    ],
    "title": "AcceptedTargetJob",
    "type": "object"
  }
}
```

</details>
