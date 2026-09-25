# stove0-target-jobs: AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-acceptedtargetjob:c0d2aaedab -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-jobs](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6e65022349"></a>

- Document: `AcceptedTargetJob`

### Document schema

<a id="s-e7aba1bf92"></a>

- <a id="s-65424e9acd"></a>`type`: `"object"`
- <a id="s-832e1eb2fc"></a>`additionalProperties`: `false`
- <a id="s-50d8964bcd"></a>`description`: `"Durable, non-secret identity of one accepted target job request."`
- <a id="s-fbabb87cf7"></a>`required`: `["declaration","request_sha256"]`
- <a id="s-465973b99e"></a>`title`: `"AcceptedTargetJob"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7189fc1fde"></a>`declaration` | yes | [TargetJobDeclaration](#s-7c3c2fdf1b) |  |
| <a id="s-66c5685358"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |

#### Definitions

- [ArtifactSelectionRef](#s-d1ec2b4ec6)
- [BranchWorkBinding](#s-f0dd35b26e)
- [CollectionId](#s-ab8f7c159a)
- [CollectionRootIdentityRef](#s-ca429d17a4)
- [ContentObservationEvidence](#s-bb085ac2ff)
- [ContentObservationFailure](#s-7a60eaa4ea)
- [ContentObservationInapplicable](#s-21dcdadf33)
- [ContentObservationRequest](#s-3cb93280f2)
- [ContentObservationResult](#s-1ce58357f0)
- [ControllerEvidence](#s-4e58d9c816)
- [DeclaredWorkspaceProtection](#s-fa2e0cb807)
- [EffectPlan](#s-7dbe757072)
- [EvaluationBinding](#s-11f391ef68)
- [ExecutionEnvelope](#s-27098e3753)
- [JoinWorkBinding](#s-90e8f87b36)
- [JoinWorkMemberBinding](#s-e025dd85f8)
- [JsonSchemaValidationProfile](#s-c9a12465b3)
- [JsonValue](#s-6ffc48ee1c)
- [NonnegativeDecimal](#s-18ab1fe48a)
- [ObserverImplementation](#s-3dec6c64dd)
- [OperationIdentityRef](#s-47d47e23a9)
- [RecipeIdentityRef](#s-6f4f70855f)
- [TargetInputAuthority](#s-9acef27d86)
- [TargetInputRoleCount](#s-a7f7611324)
- [TargetJobDeclaration](#s-7c3c2fdf1b)
- [TargetPlanBinding](#s-a2d24a74bd)
- [TransformPlan](#s-877870ac70)
- [WorkArtifactSubject](#s-008addac06)
- [WorkIdentity](#s-2694c1c670)
- [WorkflowPlan](#s-15f125a094)

#### <a id="s-d1ec2b4ec6"></a>definition `ArtifactSelectionRef`

- <a id="s-e46c4b12dc"></a>`type`: `"object"`
- <a id="s-0713c4dbb8"></a>`additionalProperties`: `false`
- <a id="s-e8c4f8a2b9"></a>`description`: `"Closed reference to a separately retained selection document."`
- <a id="s-d8ef2ad778"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`
- <a id="s-5d8c86c301"></a>`title`: `"ArtifactSelectionRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9a0a5691b7"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-b42ae761ac"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Selection Sha256" |  |
| <a id="s-31f50da81e"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-18ab1fe48a); ge=0 |  |

#### <a id="s-f0dd35b26e"></a>definition `BranchWorkBinding`

- <a id="s-3cd8995e84"></a>`type`: `"object"`
- <a id="s-81926f6690"></a>`additionalProperties`: `false`
- <a id="s-16215dbe6e"></a>`description`: `"Stable parent/branch lineage for one ordinary child work identity."`
- <a id="s-ddfde2aca3"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`
- <a id="s-e340dfc3aa"></a>`title`: `"BranchWorkBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5dc4d29e97"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-5e87b9b78f"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-6a5eb6d412"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Decision Sha256" |  |
| <a id="s-1da925dfb0"></a>`kind` | no | type="string"; const="branch"; default="branch"; title="Kind" |  |
| <a id="s-a85bd088e2"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

#### <a id="s-ab8f7c159a"></a>definition `CollectionId`


##### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-1b60cacad3"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-b1bc1f3650"></a>2 | not=(const="0") |

#### <a id="s-ca429d17a4"></a>definition `CollectionRootIdentityRef`

- <a id="s-ca3e61ef31"></a>`type`: `"object"`
- <a id="s-16a98d716d"></a>`additionalProperties`: `false`
- <a id="s-29d611b01e"></a>`description`: `"Embedded Stove0 reference to the Riverhog collection-root identity."`
- <a id="s-9900405729"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-0a055add47"></a>`title`: `"CollectionRootIdentityRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8127a4816f"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-f2f0dddc24"></a>`collection_id` | yes | [CollectionId](#s-ab8f7c159a) |  |
| <a id="s-36aec6601b"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

#### <a id="s-bb085ac2ff"></a>definition `ContentObservationEvidence`

- <a id="s-71ded1e482"></a>`type`: `"object"`
- <a id="s-fbafb23e93"></a>`additionalProperties`: `false`
- <a id="s-3f419c62a5"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-d6fd8062d7"></a>`required`: `["request","result"]`
- <a id="s-9b19f9e795"></a>`title`: `"ContentObservationEvidence"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ebb4dfa092"></a>`request` | yes | [ContentObservationRequest](#s-3cb93280f2) |  |
| <a id="s-cfa6b0b0e8"></a>`result` | yes | [ContentObservationResult](#s-1ce58357f0) |  |

#### <a id="s-7a60eaa4ea"></a>definition `ContentObservationFailure`

- <a id="s-93938c9ad0"></a>`type`: `"object"`
- <a id="s-ae004872a4"></a>`additionalProperties`: `false`
- <a id="s-833b1308b0"></a>`required`: `["code","message","retryable"]`
- <a id="s-e59e0a689f"></a>`title`: `"ContentObservationFailure"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6ef4663b0f"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-dfe1a1ea0f"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-573b4c4152"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

#### <a id="s-21dcdadf33"></a>definition `ContentObservationInapplicable`

- <a id="s-f621bb769d"></a>`type`: `"object"`
- <a id="s-7f292f1631"></a>`additionalProperties`: `false`
- <a id="s-aa9b5f3150"></a>`required`: `["code","message"]`
- <a id="s-4a04ce6541"></a>`title`: `"ContentObservationInapplicable"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a20399d646"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-e12ab15ee0"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

#### <a id="s-3cb93280f2"></a>definition `ContentObservationRequest`

- <a id="s-89ed945633"></a>`type`: `"object"`
- <a id="s-811bf6ac76"></a>`additionalProperties`: `false`
- <a id="s-bee943ce1b"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-5de0c1d8c6"></a>`title`: `"ContentObservationRequest"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-39d9fd3cc5"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-acd3dff490"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-a25786df44"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-9e151e0ac4"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-f03b6bc95f"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-783d3e4a8c"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-c2c471c30d"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Options" |  |
| <a id="s-bc448e7eee"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-e19772d1d5"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-a6049716c7"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-008addac06)); minItems=1; title="Subjects" |  |
| <a id="s-2d7dc351c9"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-bbcc98ef0f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

#### <a id="s-1ce58357f0"></a>definition `ContentObservationResult`

- <a id="s-3cc0560363"></a>`type`: `"object"`
- <a id="s-165df03d13"></a>`additionalProperties`: `false`
- <a id="s-a0f28e5e90"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-1fd744b368"></a>`title`: `"ContentObservationResult"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3ddbde6a48"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Execution Evidence" |  |
| <a id="s-109489863b"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-0461c574d3"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-c9a12465b3)); (type="null")]; default=null |  |
| <a id="s-6d370db66e"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-560355171f"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-7a60eaa4ea)); (type="null")]; default=null |  |
| <a id="s-a36bdada69"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-934d3fafdc"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-21dcdadf33)); (type="null")]; default=null |  |
| <a id="s-04e36aea7b"></a>`observer` | yes | [ObserverImplementation](#s-3dec6c64dd) |  |
| <a id="s-7710c2a6ef"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-70c0c3b3f4"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-b0587f435a"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-4ef4674e7e"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-876539dec6"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-86e2cc3c93"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-008addac06)); minItems=1; title="Subjects" |  |

#### <a id="s-4e58d9c816"></a>definition `ControllerEvidence`

- <a id="s-7012a6f0ba"></a>`type`: `"object"`
- <a id="s-1381245c09"></a>`additionalProperties`: `false`
- <a id="s-f6d653200e"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`
- <a id="s-9351f6e48f"></a>`title`: `"ControllerEvidence"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-62a14208c8"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Controller Evidence Sha256" |  |
| <a id="s-4d2558c47b"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-27098e3753) |  |
| <a id="s-dd408da56c"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1"; title="Format" |  |

#### <a id="s-fa2e0cb807"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-b150010a0f"></a>`type`: `"string"`
- <a id="s-41de30067f"></a>`enum`: `["encrypted-at-rest","memory-backed"]`
- <a id="s-f97254137a"></a>`description`: `"Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy."`

#### <a id="s-7dbe757072"></a>definition `EffectPlan`

- <a id="s-f645f3a8f1"></a>`type`: `"object"`
- <a id="s-15d78f13ae"></a>`additionalProperties`: `false`
- <a id="s-50650c7a91"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`
- <a id="s-0b14fd7dc0"></a>`title`: `"EffectPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1ef0df18c3"></a>`inputs` | yes | [TargetInputAuthority](#s-9acef27d86) |  |
| <a id="s-f2e0e2dc7a"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Intent" |  |
| <a id="s-6ff1087984"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-900c14d93e"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-e169bddb34"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-619ecc96a1"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-ac50e7b697"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1"; title="Protocol" |  |
| <a id="s-c828ae545f"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-64268e1ac3"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-acc0fa015f"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Target Options" |  |

#### <a id="s-11f391ef68"></a>definition `EvaluationBinding`

- <a id="s-959497a549"></a>`type`: `"object"`
- <a id="s-87c9f0b3df"></a>`additionalProperties`: `false`
- <a id="s-fb5a7a0176"></a>`description`: `"Immutable membership of one work item in a trial/evaluation matrix."`
- <a id="s-cca81e8bb6"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`
- <a id="s-cc69381b15"></a>`title`: `"EvaluationBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f7e79694b7"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Evaluation Id" |  |
| <a id="s-af7d86740a"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Matrix Sha256" |  |
| <a id="s-dc6bc4dfcb"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Parameters" |  |
| <a id="s-bed538d867"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Variant Id" |  |

#### <a id="s-27098e3753"></a>definition `ExecutionEnvelope`

- <a id="s-74dfef823d"></a>`type`: `"object"`
- <a id="s-b323bd6653"></a>`additionalProperties`: `false`
- <a id="s-d400a27a10"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`
- <a id="s-cc8d25bfd5"></a>`title`: `"ExecutionEnvelope"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9282f468d7"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-12b57602dc"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Envelope Sha256" |  |
| <a id="s-f100b389d5"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-479559acca"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1"; title="Format" |  |
| <a id="s-f617423fe4"></a>`target_plan` | yes | [TargetPlanBinding](#s-a2d24a74bd) |  |
| <a id="s-9db8af632d"></a>`workflow_plan` | yes | [WorkflowPlan](#s-15f125a094) |  |

#### <a id="s-90e8f87b36"></a>definition `JoinWorkBinding`

- <a id="s-1156bd2770"></a>`type`: `"object"`
- <a id="s-532932e592"></a>`additionalProperties`: `false`
- <a id="s-190e4accc3"></a>`description`: `"Stable branch-set lineage for one ordinary join work identity."`
- <a id="s-e126ac9e74"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`
- <a id="s-242f3fbae6"></a>`title`: `"JoinWorkBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8c25969a37"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Branch Set Sha256" |  |
| <a id="s-b45de927d3"></a>`kind` | no | type="string"; const="join"; default="join"; title="Kind" |  |
| <a id="s-34901a1f0c"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-e025dd85f8)); minItems=2; title="Members" |  |
| <a id="s-146b3a24ff"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

#### <a id="s-e025dd85f8"></a>definition `JoinWorkMemberBinding`

- <a id="s-f69b76ce0d"></a>`type`: `"object"`
- <a id="s-33bc1520ea"></a>`additionalProperties`: `false`
- <a id="s-7ab881c764"></a>`description`: `"Exact successful branch result used to derive one join work identity."`
- <a id="s-d8623fb4be"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`
- <a id="s-52020741b8"></a>`title`: `"JoinWorkMemberBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b46b6b4ce"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-1b8608e13c"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-8f55a96092"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Producer Settlement Sha256" |  |
| <a id="s-defd5cdb1a"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |

#### <a id="s-c9a12465b3"></a>definition `JsonSchemaValidationProfile`

- <a id="s-8fb93c83bb"></a>`type`: `"object"`
- <a id="s-84aa73ebd4"></a>`additionalProperties`: `false`
- <a id="s-879f702fa8"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-d4a4df9912"></a>`title`: `"JsonSchemaValidationProfile"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06c19470f5"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-0acb771444"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-cdbaf140eb"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-338bbafd73"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-24db254b29"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Schema" |  |

#### <a id="s-6ffc48ee1c"></a>definition `JsonValue`

- Accepts: any JSON value.

#### <a id="s-18ab1fe48a"></a>definition `NonnegativeDecimal`

- <a id="s-bb01c9b368"></a>`type`: `"string"`
- <a id="s-f9f867b2f8"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

#### <a id="s-3dec6c64dd"></a>definition `ObserverImplementation`

- <a id="s-bccbb2c39d"></a>`type`: `"object"`
- <a id="s-c78d95fc1c"></a>`additionalProperties`: `false`
- <a id="s-acd5aeae42"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-8b0c4f709d"></a>`title`: `"ObserverImplementation"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5743db276d"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-70825f4090"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-efb26e1bfe"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-461dd78358"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-4b32175e2e"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

#### <a id="s-47d47e23a9"></a>definition `OperationIdentityRef`

- <a id="s-f0132c2eb5"></a>`type`: `"object"`
- <a id="s-e1efcab18e"></a>`additionalProperties`: `false`
- <a id="s-da36e93e42"></a>`description`: `"Embedded Stove0 reference to the Riverhog operation identity."`
- <a id="s-1ed420d05c"></a>`required`: `["id","sha256"]`
- <a id="s-85211d5718"></a>`title`: `"OperationIdentityRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8c51dffad4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-5e8db45f1b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

#### <a id="s-6f4f70855f"></a>definition `RecipeIdentityRef`

- <a id="s-df368a9936"></a>`type`: `"object"`
- <a id="s-fbeb68c856"></a>`additionalProperties`: `false`
- <a id="s-855dc6d0af"></a>`description`: `"Embedded Stove0 reference to the Riverhog recipe identity."`
- <a id="s-d6a7eeb07b"></a>`required`: `["id","revision","sha256"]`
- <a id="s-ac95d6b415"></a>`title`: `"RecipeIdentityRef"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d0f03f45a6"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-3b48db89b8"></a>`revision` | yes | [NonnegativeDecimal](#s-18ab1fe48a); ge=1 |  |
| <a id="s-7fe4f9adf3"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

#### <a id="s-9acef27d86"></a>definition `TargetInputAuthority`

- <a id="s-d29406ad0a"></a>`type`: `"object"`
- <a id="s-986e574cd8"></a>`additionalProperties`: `false`
- <a id="s-178292817d"></a>`description`: `"Small exact input authority retained by Stove0 and traversed in bounded pages."`
- <a id="s-1829bcbf5d"></a>`required`: `["selection","roles"]`
- <a id="s-597f4f39ed"></a>`title`: `"TargetInputAuthority"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-252bc0f711"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-a7f7611324)); minItems=1; title="Roles" |  |
| <a id="s-05a342fd06"></a>`selection` | yes | [ArtifactSelectionRef](#s-d1ec2b4ec6) |  |

#### <a id="s-a7f7611324"></a>definition `TargetInputRoleCount`

- <a id="s-accfd2e70c"></a>`type`: `"object"`
- <a id="s-c148835949"></a>`additionalProperties`: `false`
- <a id="s-39f46f96e4"></a>`required`: `["role","count"]`
- <a id="s-7fb9d6669f"></a>`title`: `"TargetInputRoleCount"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7229ab332b"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-ddbb71f3d5"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

#### <a id="s-7c3c2fdf1b"></a>definition `TargetJobDeclaration`

- <a id="s-fbb2e21431"></a>`type`: `"object"`
- <a id="s-d576fd0394"></a>`additionalProperties`: `false`
- <a id="s-8319e92874"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","declared_workspace_protection"]`
- <a id="s-e7c04adc33"></a>`title`: `"TargetJobDeclaration"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-351d0a634c"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-5d085b28ea"></a>`controller_evidence` | yes | [ControllerEvidence](#s-4e58d9c816) |  |
| <a id="s-79d76722af"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-fa2e0cb807) |  |
| <a id="s-11b5525cbe"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-2f4cb2fa9b"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-b35786e5e9"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-877870ac70)); ([EffectPlan](#s-7dbe757072))]; title="Plan" |  |

#### <a id="s-a2d24a74bd"></a>definition `TargetPlanBinding`

- <a id="s-1c0fe184a8"></a>`type`: `"object"`
- <a id="s-cf24dcdbc9"></a>`additionalProperties`: `false`
- <a id="s-6c242fec0f"></a>`description`: `"Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope."`
- <a id="s-0bb20b93da"></a>`required`: `["protocol","target_implementation_id","target_descriptor_sha256","operation_contract_sha256","plan","plan_sha256"]`
- <a id="s-84018c5179"></a>`title`: `"TargetPlanBinding"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-64e588694d"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-880698b230"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Plan" |  |
| <a id="s-a1c47d145f"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-472d41a7ce"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Protocol" |  |
| <a id="s-bbf17be4ce"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-eeac255d4c"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |

#### <a id="s-877870ac70"></a>definition `TransformPlan`

- <a id="s-b093485c46"></a>`type`: `"object"`
- <a id="s-9459dd5ce7"></a>`additionalProperties`: `false`
- <a id="s-73f9bb82c3"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`
- <a id="s-cec7d28baf"></a>`title`: `"TransformPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-02f03ab4db"></a>`inputs` | yes | [TargetInputAuthority](#s-9acef27d86) |  |
| <a id="s-1843c4ae94"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Intent" |  |
| <a id="s-aa851aaa4e"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-8c847474e8"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-340411c973"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-b2bfb6c2db"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-cf416be620"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-05306776ad"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-27b0922a41"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-38d4bd6a28"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Target Options" |  |

#### <a id="s-008addac06"></a>definition `WorkArtifactSubject`

- <a id="s-5b35232c67"></a>`type`: `"object"`
- <a id="s-9539078950"></a>`additionalProperties`: `false`
- <a id="s-4e7670633e"></a>`description`: `"A collection logical file assigned an ID and role within one Stove0 work."`
- <a id="s-c42a2227d5"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-56184f9afc"></a>`title`: `"WorkArtifactSubject"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f0df87fc2f"></a>`bytes` | yes | [NonnegativeDecimal](#s-18ab1fe48a); ge=0 |  |
| <a id="s-687413c4c0"></a>`collection` | yes | [CollectionRootIdentityRef](#s-ca429d17a4) |  |
| <a id="s-cfc9965010"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-a9a29920e7"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-cf7e2a049e"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-86b1924930"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-861eab698b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

#### <a id="s-2694c1c670"></a>definition `WorkIdentity`

- <a id="s-46c21324bc"></a>`type`: `"object"`
- <a id="s-cccb71bde8"></a>`additionalProperties`: `false`
- <a id="s-2618852af9"></a>`required`: `["recipe","inputs","work_id"]`
- <a id="s-e186545d17"></a>`title`: `"WorkIdentity"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11678610e4"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Effective Intent" |  |
| <a id="s-65eb79baec"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-11f391ef68)); (type="null")]; default=null |  |
| <a id="s-91ddf6b5fb"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-f0dd35b26e)); ([JoinWorkBinding](#s-90e8f87b36))]); (type="null")]; default=null; title="Fork Join" |  |
| <a id="s-2da5972531"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1"; title="Format" |  |
| <a id="s-85209fffb5"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-ca429d17a4)); minItems=1; title="Inputs" |  |
| <a id="s-0fe65040ed"></a>`recipe` | yes | [RecipeIdentityRef](#s-6f4f70855f) |  |
| <a id="s-639402aee9"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

#### <a id="s-15f125a094"></a>definition `WorkflowPlan`

- <a id="s-771eb60d7b"></a>`type`: `"object"`
- <a id="s-543073c20f"></a>`additionalProperties`: `false`
- <a id="s-471729a484"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`
- <a id="s-61c368463d"></a>`title`: `"WorkflowPlan"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac85e44040"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1"; title="Format" |  |
| <a id="s-41e008b328"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-44bb80a840"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-bb085ac2ff)); title="Observations" |  |
| <a id="s-699a3c0936"></a>`operation` | yes | [OperationIdentityRef](#s-47d47e23a9) |  |
| <a id="s-afc46127d7"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Output Policy" |  |
| <a id="s-011554aafb"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-6ffc48ee1c)); title="Requested Target Options" |  |
| <a id="s-579c8b6a90"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-01d382a339"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-63842c3057"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-fec2e92ef8"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-a0d000ceae"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Target Registration Id" |  |
| <a id="s-77360f10c9"></a>`work` | yes | [WorkIdentity](#s-2694c1c670) |  |
| <a id="s-ba393d5df2"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-target-jobs-durable-state-identity.md)

## Governing policies

- <a id="pa-2eaa218fbf"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-target-jobs](../../../evidence/sources/authorities.md#src-7b4138829a) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/protocol.py::AcceptedTargetJob](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py); [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/protocol.py::TargetJobStatus](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py)

### Machine authority

- `/external_contract/durable_state/owners/7/structure/documents/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3cead066b3bc08362012e59c42336423580f624c09a568d0039c6b687f09b61a -->

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
            "$ref": "#/$defs/NonnegativeDecimal",
            "ge": 0
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
      "CollectionRootIdentityRef": {
        "additionalProperties": false,
        "description": "Embedded Stove0 reference to the Riverhog collection-root identity.",
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
        "title": "CollectionRootIdentityRef",
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
              "$ref": "#/$defs/WorkArtifactSubject"
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
              "$ref": "#/$defs/WorkArtifactSubject"
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
      "NonnegativeDecimal": {
        "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
        "type": "string"
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
      "OperationIdentityRef": {
        "additionalProperties": false,
        "description": "Embedded Stove0 reference to the Riverhog operation identity.",
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
        "title": "OperationIdentityRef",
        "type": "object"
      },
      "RecipeIdentityRef": {
        "additionalProperties": false,
        "description": "Embedded Stove0 reference to the Riverhog recipe identity.",
        "properties": {
          "id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "revision": {
            "$ref": "#/$defs/NonnegativeDecimal",
            "ge": 1
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
        "title": "RecipeIdentityRef",
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
      "WorkArtifactSubject": {
        "additionalProperties": false,
        "description": "A collection logical file assigned an ID and role within one Stove0 work.",
        "properties": {
          "bytes": {
            "$ref": "#/$defs/NonnegativeDecimal",
            "ge": 0
          },
          "collection": {
            "$ref": "#/$defs/CollectionRootIdentityRef"
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
        "title": "WorkArtifactSubject",
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
              "$ref": "#/$defs/CollectionRootIdentityRef"
            },
            "minItems": 1,
            "title": "Inputs",
            "type": "array"
          },
          "recipe": {
            "$ref": "#/$defs/RecipeIdentityRef"
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
            "$ref": "#/$defs/OperationIdentityRef"
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
