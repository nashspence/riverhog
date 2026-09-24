# stove0-target-jobs: AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-acceptedtargetjob:9f4d08e69f -->

Exact externally visible contract owned by this contract element.

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
| <a id="s-61c2c95c95"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |

#### Definitions

- [ArtifactSelectionRef](#s-973e2a3365)
- [ArtifactSubject](#s-6be2fdc787)
- [BranchWorkBinding](#s-548c2def56)
- [CollectionId](#s-cc22d9cbdd)
- [CollectionRootRef](#s-3b4ac5edde)
- [ContentObservationEvidence](#s-81bc4f2bfe)
- [ContentObservationFailure](#s-7c3aacdf3b)
- [ContentObservationInapplicable](#s-e431d42f7b)
- [ContentObservationRequest](#s-2f34384e0c)
- [ContentObservationResult](#s-131ba80c59)
- [ControllerEvidence](#s-5e0010cbf3)
- [DeclaredWorkspaceProtection](#s-ab8d7953d2)
- [EffectPlan](#s-9469da79f1)
- [EvaluationBinding](#s-f025905d6a)
- [ExecutionEnvelope](#s-033c077b2b)
- [JoinWorkBinding](#s-53a18f36fb)
- [JoinWorkMemberBinding](#s-82dd57ddfa)
- [JsonSchemaValidationProfile](#s-17a6537685)
- [JsonValue](#s-326d0b99e9)
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
| <a id="s-4b315d4d06"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-079ddc42e2"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Selection Sha256" |  |
| <a id="s-60c15f2059"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

#### <a id="s-6be2fdc787"></a>definition `ArtifactSubject`

- <a id="s-dbaf3c49fa"></a>`type`: `"object"`
- <a id="s-7d82a6577c"></a>`additionalProperties`: `false`
- <a id="s-2cc9905b13"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-5d76770f9a"></a>`title`: `"ArtifactSubject"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce054e8488"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-c0cf31d463"></a>`collection` | yes | [CollectionRootRef](#s-3b4ac5edde) |  |
| <a id="s-f51d826361"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-91dee40850"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-ed55fd10a5"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-02a9762ab3"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-8d312c3d06"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

#### <a id="s-548c2def56"></a>definition `BranchWorkBinding`

- <a id="s-c3bd547f4c"></a>`type`: `"object"`
- <a id="s-31d3ce56c4"></a>`additionalProperties`: `false`
- <a id="s-4579edb17d"></a>`description`: `"Stable parent/branch lineage for one ordinary child work identity."`
- <a id="s-50108f11dc"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`
- <a id="s-c80fc910b8"></a>`title`: `"BranchWorkBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-87812a5524"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-0001298fc2"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-811cb06e1c"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Decision Sha256" |  |
| <a id="s-9626fa2199"></a>`kind` | no | type="string"; const="branch"; default="branch"; title="Kind" |  |
| <a id="s-7c5e005bd9"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

#### <a id="s-cc22d9cbdd"></a>definition `CollectionId`


##### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-a95ae396af"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-305a5c8077"></a>2 | not=(const="0") |

#### <a id="s-3b4ac5edde"></a>definition `CollectionRootRef`

- <a id="s-bcef7a0469"></a>`type`: `"object"`
- <a id="s-64692871ef"></a>`additionalProperties`: `false`
- <a id="s-06e87185c3"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-8d7a8aafa6"></a>`title`: `"CollectionRootRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95d56d20b1"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-802754c36e"></a>`collection_id` | yes | [CollectionId](#s-cc22d9cbdd) |  |
| <a id="s-e600c026b1"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

#### <a id="s-81bc4f2bfe"></a>definition `ContentObservationEvidence`

- <a id="s-7081b807f4"></a>`type`: `"object"`
- <a id="s-4d7ccd8359"></a>`additionalProperties`: `false`
- <a id="s-d0a3d50a54"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-78c33e4aee"></a>`required`: `["request","result"]`
- <a id="s-662acdbb3d"></a>`title`: `"ContentObservationEvidence"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-48344647eb"></a>`request` | yes | [ContentObservationRequest](#s-2f34384e0c) |  |
| <a id="s-2eadfb5fe1"></a>`result` | yes | [ContentObservationResult](#s-131ba80c59) |  |

#### <a id="s-7c3aacdf3b"></a>definition `ContentObservationFailure`

- <a id="s-ed78bd41cc"></a>`type`: `"object"`
- <a id="s-cc7a10b708"></a>`additionalProperties`: `false`
- <a id="s-bfd96c6b77"></a>`required`: `["code","message","retryable"]`
- <a id="s-66be6e250f"></a>`title`: `"ContentObservationFailure"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e6fc9f4342"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-32b7b53cc6"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-1411ded81c"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

#### <a id="s-e431d42f7b"></a>definition `ContentObservationInapplicable`

- <a id="s-77762ecc82"></a>`type`: `"object"`
- <a id="s-1bf2c3ff84"></a>`additionalProperties`: `false`
- <a id="s-042937a752"></a>`required`: `["code","message"]`
- <a id="s-93c0b45070"></a>`title`: `"ContentObservationInapplicable"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-23d177cb3e"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-1909756ed3"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

#### <a id="s-2f34384e0c"></a>definition `ContentObservationRequest`

- <a id="s-19b60269b5"></a>`type`: `"object"`
- <a id="s-89ef6eda91"></a>`additionalProperties`: `false`
- <a id="s-1a32dad112"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-050c55dc7f"></a>`title`: `"ContentObservationRequest"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-595b21a9f6"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-c7107abe6e"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-f2b062f31b"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-3cb4f6ad28"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-a7db5346c2"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-925c8d42e4"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-ad4d945d89"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Options" |  |
| <a id="s-7175376468"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-35c43afa81"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-ad8332810f"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6be2fdc787)); minItems=1; title="Subjects" |  |
| <a id="s-b7803a6177"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-fe3e024a0f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

#### <a id="s-131ba80c59"></a>definition `ContentObservationResult`

- <a id="s-fa75ec9d7a"></a>`type`: `"object"`
- <a id="s-58194f18fd"></a>`additionalProperties`: `false`
- <a id="s-2d9a279127"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-d7f4d35294"></a>`title`: `"ContentObservationResult"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0b0e8a6708"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Execution Evidence" |  |
| <a id="s-7f0d121b3c"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-326d0b99e9))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-281acd1fbb"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-17a6537685)); (type="null")]; default=null |  |
| <a id="s-f1cdccd0dc"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-4e2fc8bb59"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-7c3aacdf3b)); (type="null")]; default=null |  |
| <a id="s-2269f476ad"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-c79c4a8de6"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-e431d42f7b)); (type="null")]; default=null |  |
| <a id="s-a53bbd585e"></a>`observer` | yes | [ObserverImplementation](#s-c48c7036bd) |  |
| <a id="s-38f79f88aa"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-8c93eebfff"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-93e7e20515"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-6e37a3bb34"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-799a669a4e"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-57a0cd395f"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6be2fdc787)); minItems=1; title="Subjects" |  |

#### <a id="s-5e0010cbf3"></a>definition `ControllerEvidence`

- <a id="s-30a0444f98"></a>`type`: `"object"`
- <a id="s-bfa5385832"></a>`additionalProperties`: `false`
- <a id="s-4cd61b1f3c"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`
- <a id="s-d0ec1bd701"></a>`title`: `"ControllerEvidence"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05ddbf913f"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Controller Evidence Sha256" |  |
| <a id="s-0ce6f1aabf"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-033c077b2b) |  |
| <a id="s-774a81b449"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1"; title="Format" |  |

#### <a id="s-ab8d7953d2"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-fe6de6dff2"></a>`type`: `"string"`
- <a id="s-fee790ec78"></a>`enum`: `["encrypted-at-rest","memory-backed"]`
- <a id="s-7dcc1b746a"></a>`description`: `"Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy."`

#### <a id="s-9469da79f1"></a>definition `EffectPlan`

- <a id="s-f81d3e446d"></a>`type`: `"object"`
- <a id="s-d3190fcca6"></a>`additionalProperties`: `false`
- <a id="s-8a628ee1ab"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`
- <a id="s-61cb5c0d08"></a>`title`: `"EffectPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b217fb6912"></a>`inputs` | yes | [TargetInputAuthority](#s-31aadefae6) |  |
| <a id="s-44a52c8816"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Intent" |  |
| <a id="s-a164779555"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-87f4e225be"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-850ea99761"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-4e7e73d6a7"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-7e58ba9b25"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1"; title="Protocol" |  |
| <a id="s-a5bc73a13a"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-4250f8bf28"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-5fd579adb8"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Target Options" |  |

#### <a id="s-f025905d6a"></a>definition `EvaluationBinding`

- <a id="s-b558d55ed5"></a>`type`: `"object"`
- <a id="s-b91b6668be"></a>`additionalProperties`: `false`
- <a id="s-f7ffa14e06"></a>`description`: `"Immutable membership of one work item in a trial/evaluation matrix."`
- <a id="s-2284f31291"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`
- <a id="s-7249deaf7f"></a>`title`: `"EvaluationBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dfafd92cd5"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Evaluation Id" |  |
| <a id="s-6529e05242"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Matrix Sha256" |  |
| <a id="s-3fa2221975"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Parameters" |  |
| <a id="s-1ba1ae9722"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Variant Id" |  |

#### <a id="s-033c077b2b"></a>definition `ExecutionEnvelope`

- <a id="s-3ba0060bea"></a>`type`: `"object"`
- <a id="s-1f2d406fe4"></a>`additionalProperties`: `false`
- <a id="s-e9fe5ac592"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`
- <a id="s-2755f31872"></a>`title`: `"ExecutionEnvelope"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6db7f617c8"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-00b0c1dd57"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Envelope Sha256" |  |
| <a id="s-d8bdf98b9e"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-a6adf0fb25"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1"; title="Format" |  |
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
| <a id="s-eba5c7f57d"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Branch Set Sha256" |  |
| <a id="s-293e0b5cb5"></a>`kind` | no | type="string"; const="join"; default="join"; title="Kind" |  |
| <a id="s-12a929917c"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-82dd57ddfa)); minItems=2; title="Members" |  |
| <a id="s-dac8189dfc"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

#### <a id="s-82dd57ddfa"></a>definition `JoinWorkMemberBinding`

- <a id="s-f21c31e836"></a>`type`: `"object"`
- <a id="s-e80498e632"></a>`additionalProperties`: `false`
- <a id="s-8f1a1e63b3"></a>`description`: `"Exact successful branch result used to derive one join work identity."`
- <a id="s-92e03588f5"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`
- <a id="s-de895a3c99"></a>`title`: `"JoinWorkMemberBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a13c3e6460"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-547f922bf6"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-d2351954dd"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Producer Settlement Sha256" |  |
| <a id="s-07e92a2b73"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |

#### <a id="s-17a6537685"></a>definition `JsonSchemaValidationProfile`

- <a id="s-c95680767a"></a>`type`: `"object"`
- <a id="s-ac7f61d0f3"></a>`additionalProperties`: `false`
- <a id="s-25fa7c486d"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-06708c106d"></a>`title`: `"JsonSchemaValidationProfile"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b8d620e6e2"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-f220065e7b"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-fd32cc52d4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-57a3da8f91"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-aa6082e5bb"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Schema" |  |

#### <a id="s-326d0b99e9"></a>definition `JsonValue`

- Accepts: any JSON value.

#### <a id="s-c48c7036bd"></a>definition `ObserverImplementation`

- <a id="s-065a701a6d"></a>`type`: `"object"`
- <a id="s-4f9d44be43"></a>`additionalProperties`: `false`
- <a id="s-b546b34fcb"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-d485f73065"></a>`title`: `"ObserverImplementation"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac5ef790b8"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-474c3756e8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-17d9bd7154"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-877c3f9c32"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-8afcb4acd2"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

#### <a id="s-e89fe7446a"></a>definition `OperationRef`

- <a id="s-f432947f67"></a>`type`: `"object"`
- <a id="s-c7481956be"></a>`additionalProperties`: `false`
- <a id="s-f6bee63b03"></a>`required`: `["id","sha256"]`
- <a id="s-844ca9aadf"></a>`title`: `"OperationRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f454a06a05"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-92a1644de9"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

#### <a id="s-cee329be31"></a>definition `RecipeRef`

- <a id="s-6c0ed8e1d5"></a>`type`: `"object"`
- <a id="s-24527512be"></a>`additionalProperties`: `false`
- <a id="s-b7ebf41392"></a>`required`: `["id","revision","sha256"]`
- <a id="s-1d2d32e636"></a>`title`: `"RecipeRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2217552652"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-59f87aa275"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-106ba04369"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

#### <a id="s-31aadefae6"></a>definition `TargetInputAuthority`

- <a id="s-5e855f3f97"></a>`type`: `"object"`
- <a id="s-1ee7ba5afd"></a>`additionalProperties`: `false`
- <a id="s-6f4b15e191"></a>`description`: `"Small exact input authority retained by Stove0 and traversed in bounded pages."`
- <a id="s-a64f06c2f9"></a>`required`: `["selection","roles"]`
- <a id="s-e656a1041e"></a>`title`: `"TargetInputAuthority"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1207d9d265"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-491d231ef9)); minItems=1; title="Roles" |  |
| <a id="s-a21a5fff83"></a>`selection` | yes | [ArtifactSelectionRef](#s-973e2a3365) |  |

#### <a id="s-491d231ef9"></a>definition `TargetInputRoleCount`

- <a id="s-4ca72a3d2e"></a>`type`: `"object"`
- <a id="s-9ed2414aeb"></a>`additionalProperties`: `false`
- <a id="s-100c38c4ab"></a>`required`: `["role","count"]`
- <a id="s-cb8a910e75"></a>`title`: `"TargetInputRoleCount"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-16503d3716"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-23d3901aed"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

#### <a id="s-91f55fb248"></a>definition `TargetJobDeclaration`

- <a id="s-1cbb31e22f"></a>`type`: `"object"`
- <a id="s-cf64d0603c"></a>`additionalProperties`: `false`
- <a id="s-b4ba3ddb8e"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","declared_workspace_protection"]`
- <a id="s-387701f355"></a>`title`: `"TargetJobDeclaration"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9cdbb4f748"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-1c3bb8836a"></a>`controller_evidence` | yes | [ControllerEvidence](#s-5e0010cbf3) |  |
| <a id="s-a229178eb4"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-ab8d7953d2) |  |
| <a id="s-42eeb85923"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-334811f276"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-15fd4a7b19"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-a54e765c9b)); ([EffectPlan](#s-9469da79f1))]; title="Plan" |  |

#### <a id="s-9e23b75bbf"></a>definition `TargetPlanBinding`

- <a id="s-e71f02c218"></a>`type`: `"object"`
- <a id="s-34d3315332"></a>`additionalProperties`: `false`
- <a id="s-540d1c2df2"></a>`description`: `"Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope."`
- <a id="s-6678960845"></a>`required`: `["protocol","target_implementation_id","target_descriptor_sha256","operation_contract_sha256","plan","plan_sha256"]`
- <a id="s-a562aacd93"></a>`title`: `"TargetPlanBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d475634be"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-c74c7177ff"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Plan" |  |
| <a id="s-0e566e1dd9"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-39e707e0dc"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Protocol" |  |
| <a id="s-ae0e38214a"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-5331352fef"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |

#### <a id="s-a54e765c9b"></a>definition `TransformPlan`

- <a id="s-b2134c7728"></a>`type`: `"object"`
- <a id="s-374327d768"></a>`additionalProperties`: `false`
- <a id="s-0153a5f65b"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`
- <a id="s-5045bcbf72"></a>`title`: `"TransformPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ef53d1dbf0"></a>`inputs` | yes | [TargetInputAuthority](#s-31aadefae6) |  |
| <a id="s-7afc441650"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Intent" |  |
| <a id="s-33c173195e"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-ce52962d65"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-6559bbf52f"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-03cca7c3f1"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-ba23a56a22"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-82c4e734b7"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-b69bbd54d7"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-b706c75894"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Target Options" |  |

#### <a id="s-7b040f3044"></a>definition `WorkIdentity`

- <a id="s-a2285802ad"></a>`type`: `"object"`
- <a id="s-c07c0d5e25"></a>`additionalProperties`: `false`
- <a id="s-4b513a3bd9"></a>`required`: `["recipe","inputs","work_id"]`
- <a id="s-8f6d8678d2"></a>`title`: `"WorkIdentity"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c1d5db82e9"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Effective Intent" |  |
| <a id="s-4e0d6b3ed9"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-f025905d6a)); (type="null")]; default=null |  |
| <a id="s-a2aefa59d5"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-548c2def56)); ([JoinWorkBinding](#s-53a18f36fb))]); (type="null")]; default=null; title="Fork Join" |  |
| <a id="s-165aecfd8d"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1"; title="Format" |  |
| <a id="s-a1144978a0"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-3b4ac5edde)); minItems=1; title="Inputs" |  |
| <a id="s-e21bf10dc2"></a>`recipe` | yes | [RecipeRef](#s-cee329be31) |  |
| <a id="s-fe9e508578"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

#### <a id="s-7278f1b514"></a>definition `WorkflowPlan`

- <a id="s-7280c421ee"></a>`type`: `"object"`
- <a id="s-d4c873a46a"></a>`additionalProperties`: `false`
- <a id="s-5e821c9586"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`
- <a id="s-44e0a173ed"></a>`title`: `"WorkflowPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-17fea187b0"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1"; title="Format" |  |
| <a id="s-66d496dce3"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-d7f3d36782"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-81bc4f2bfe)); title="Observations" |  |
| <a id="s-038e085626"></a>`operation` | yes | [OperationRef](#s-e89fe7446a) |  |
| <a id="s-4c734df6e1"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Output Policy" |  |
| <a id="s-fa3c0884bf"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-326d0b99e9)); title="Requested Target Options" |  |
| <a id="s-e389fcca4c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-27bff322aa"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-45bdd4707f"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-e9adf760b7"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-0def0e07f6"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Target Registration Id" |  |
| <a id="s-4f0c6f0bce"></a>`work` | yes | [WorkIdentity](#s-7b040f3044) |  |
| <a id="s-e2a7a4a0c1"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-target-jobs-durable-state-identity.md)

## Governing policies

- <a id="pa-899b241fe4"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-target-jobs](../../../evidence/sources/authorities.md#src-7b4138829a) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/protocol.py::AcceptedTargetJob](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py); [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/protocol.py::TargetJobStatus](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/documents/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e65abb395ae50d261e58e6c4c897a88b8a9a3cdb25f0282fa8ee68e85d563f0 -->

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
      "ContentObservationEvidence": {
        "additionalProperties": false,
        "description": "Complete routing evidence: immutable request plus accepted result.",
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
        "title": "ContentObservationEvidence",
        "type": "object"
      },
      "ContentObservationFailure": {
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
        "title": "ContentObservationFailure",
        "type": "object"
      },
      "ContentObservationInapplicable": {
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
        "title": "ContentObservationInapplicable",
        "type": "object"
      },
      "ContentObservationRequest": {
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
        "title": "ContentObservationRequest",
        "type": "object"
      },
      "ContentObservationResult": {
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
            "default": null,
            "title": "Facts Sha256"
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
            "title": "Format",
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
        "title": "ContentObservationResult",
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
      "DeclaredWorkspaceProtection": {
        "description": "Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy.",
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
          "target_descriptor_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Descriptor Sha256",
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
          "target_descriptor_sha256",
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
      "JsonSchemaValidationProfile": {
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
          "profile_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Profile Sha256",
            "type": "string"
          },
          "schema": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Schema",
            "type": "object"
          }
        },
        "required": [
          "id",
          "profile_sha256",
          "schema"
        ],
        "title": "JsonSchemaValidationProfile",
        "type": "object"
      },
      "JsonValue": {},
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
          "declared_workspace_protection": {
            "$ref": "#/$defs/DeclaredWorkspaceProtection"
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
          "target_descriptor_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Descriptor Sha256",
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
          "target_descriptor_sha256",
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
          "target_descriptor_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Descriptor Sha256",
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
          "target_descriptor_sha256",
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
              "$ref": "#/$defs/ContentObservationEvidence"
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
          "source_collection_retirement_grace_seconds": {
            "default": 0,
            "minimum": 0,
            "title": "Source Collection Retirement Grace Seconds",
            "type": "integer"
          },
          "source_collection_retirement_policy": {
            "default": "retain",
            "description": "Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks.",
            "enum": [
              "retain",
              "retire-after-verified-output"
            ],
            "title": "Source Collection Retirement Policy",
            "type": "string"
          },
          "target_descriptor_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Descriptor Sha256",
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
          "target_descriptor_sha256",
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
