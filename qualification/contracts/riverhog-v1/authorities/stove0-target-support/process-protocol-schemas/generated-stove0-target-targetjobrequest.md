# generated:stove0-target: TargetJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-targetjobrequest:b693910851 -->

Secret-bearing target invocation; never store this document durably.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-b7e2cff5c0"></a>

- <a id="s-410c8a40ce"></a>`type`: `"object"`
- <a id="s-7005dc6cc9"></a>`additionalProperties`: `false`
- <a id="s-aa1f317f99"></a>`description`: `"Secret-bearing target invocation; never store this document durably."`
- <a id="s-f8ec2b0017"></a>`required`: `["declaration","runtime","callback_access","request_sha256"]`
- <a id="s-ba65af0f16"></a>`title`: `"TargetJobRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa20dcc90c"></a>`callback_access` | yes | [TargetCallbackAccess](#s-4a6151aff8) |  |
| <a id="s-c5d8e65407"></a>`declaration` | yes | [TargetJobDeclaration](#s-2833c36a58) |  |
| <a id="s-8724456f30"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-5614a253d3"></a>`runtime` | yes | [TargetRuntimeAuthority](#s-0dadecd6af) |  |

### Definitions

- [ArtifactSelectionRef](#s-657a10e518)
- [BranchWorkBinding](#s-21ae556270)
- [CollectionId](#s-78cd023b68)
- [CollectionRootIdentityRef](#s-43fbbd3c35)
- [ContentObservationEvidence](#s-f50bdec28b)
- [ContentObservationFailure](#s-90be301ea5)
- [ContentObservationInapplicable](#s-fb053cfa40)
- [ContentObservationRequest](#s-c7a2925267)
- [ContentObservationResult](#s-073b812e4e)
- [ControllerEvidence](#s-ba0cab1e94)
- [DeclaredWorkspaceProtection](#s-05ee0ceec3)
- [EffectPlan](#s-81cfff6704)
- [EvaluationBinding](#s-fcab002bb2)
- [ExecutionEnvelope](#s-d6d1b42663)
- [JoinWorkBinding](#s-465ef625e0)
- [JoinWorkMemberBinding](#s-b02b34545d)
- [JsonSchemaValidationProfile](#s-61a9572469)
- [JsonValue](#s-35500bba5f)
- [NonnegativeDecimal](#s-7c2dafd839)
- [ObserverImplementation](#s-53f5ddfdf6)
- [OperationIdentityRef](#s-f7aecc543b)
- [RecipeIdentityRef](#s-7bdf7a73a3)
- [TargetCallbackAccess](#s-4a6151aff8)
- [TargetInputAuthority](#s-32ba1dc94b)
- [TargetInputRoleCount](#s-44780440f4)
- [TargetJobDeclaration](#s-2833c36a58)
- [TargetPlanBinding](#s-30e6867f94)
- [TargetRuntimeAuthority](#s-0dadecd6af)
- [TransformPlan](#s-d9d22e348c)
- [WorkArtifactSubject](#s-48c029b0e4)
- [WorkIdentity](#s-fa0e1b5a44)
- [WorkflowPlan](#s-06c3ebb676)

### <a id="s-657a10e518"></a>definition `ArtifactSelectionRef`

- <a id="s-7c9020b9c2"></a>`type`: `"object"`
- <a id="s-4a8d582a17"></a>`additionalProperties`: `false`
- <a id="s-17b33b8ddd"></a>`description`: `"Closed reference to a separately retained selection document."`
- <a id="s-22dbcd34bf"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`
- <a id="s-e2ae9fedeb"></a>`title`: `"ArtifactSelectionRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e8351d3651"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-b2a96b6462"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Selection Sha256" |  |
| <a id="s-69d91fe2bd"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

### <a id="s-21ae556270"></a>definition `BranchWorkBinding`

- <a id="s-e02658294f"></a>`type`: `"object"`
- <a id="s-1f49b0bffa"></a>`additionalProperties`: `false`
- <a id="s-85e17ae92d"></a>`description`: `"Stable parent/branch lineage for one ordinary child work identity."`
- <a id="s-ba4382b598"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`
- <a id="s-4fb16a2105"></a>`title`: `"BranchWorkBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bfb0551fcc"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-c663991ba3"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-cff0bdbaa4"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Decision Sha256" |  |
| <a id="s-677f89560e"></a>`kind` | no | type="string"; const="branch"; default="branch"; title="Kind" |  |
| <a id="s-e120adb554"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

### <a id="s-78cd023b68"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-9a6e8c499b"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-f38b54a213"></a>2 | not=(const="0") |

### <a id="s-43fbbd3c35"></a>definition `CollectionRootIdentityRef`

- <a id="s-0e4ac19838"></a>`type`: `"object"`
- <a id="s-4bb5331a5a"></a>`additionalProperties`: `false`
- <a id="s-33971bff23"></a>`description`: `"Embedded Stove0 reference to the Riverhog collection-root identity."`
- <a id="s-bd1f050211"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-6a0d09ea95"></a>`title`: `"CollectionRootIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f18cedd028"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-c8dee49209"></a>`collection_id` | yes | [CollectionId](#s-78cd023b68) |  |
| <a id="s-e027292deb"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-f50bdec28b"></a>definition `ContentObservationEvidence`

- <a id="s-94c25f063b"></a>`type`: `"object"`
- <a id="s-d5a633e8ec"></a>`additionalProperties`: `false`
- <a id="s-b9ae41ffab"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-1adfdac1a8"></a>`required`: `["request","result"]`
- <a id="s-00c6dea1de"></a>`title`: `"ContentObservationEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3317dd6107"></a>`request` | yes | [ContentObservationRequest](#s-c7a2925267) |  |
| <a id="s-ba7a5426c2"></a>`result` | yes | [ContentObservationResult](#s-073b812e4e) |  |

### <a id="s-90be301ea5"></a>definition `ContentObservationFailure`

- <a id="s-766ab2c01b"></a>`type`: `"object"`
- <a id="s-1940ff51d2"></a>`additionalProperties`: `false`
- <a id="s-d36f208cf1"></a>`required`: `["code","message","retryable"]`
- <a id="s-277fa316ce"></a>`title`: `"ContentObservationFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-832e8805aa"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-32cc14a9e1"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-a7df401010"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-fb053cfa40"></a>definition `ContentObservationInapplicable`

- <a id="s-89627b2bc0"></a>`type`: `"object"`
- <a id="s-b5be1c058c"></a>`additionalProperties`: `false`
- <a id="s-1c8c198a81"></a>`required`: `["code","message"]`
- <a id="s-e9b7628e12"></a>`title`: `"ContentObservationInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19a23fcfac"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-6ed5a144b2"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-c7a2925267"></a>definition `ContentObservationRequest`

- <a id="s-9b1aaa863d"></a>`type`: `"object"`
- <a id="s-77047b10cd"></a>`additionalProperties`: `false`
- <a id="s-0aa0d2519a"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-395c21ee4f"></a>`title`: `"ContentObservationRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca00ff4f91"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-cfef832c8c"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-0d35498296"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-5e6d8ada44"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-76ea2deb34"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-ab8b165346"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-8faa283d89"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Options" |  |
| <a id="s-1a75f9d635"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-d84dce8570"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-b5c7481b8a"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-48c029b0e4)); minItems=1; title="Subjects" |  |
| <a id="s-f8fa691991"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-9fb7474c6c"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-073b812e4e"></a>definition `ContentObservationResult`

- <a id="s-4cbd7566c6"></a>`type`: `"object"`
- <a id="s-94e8aadced"></a>`additionalProperties`: `false`
- <a id="s-1ae70ea4c1"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-af9d4f4a5a"></a>`title`: `"ContentObservationResult"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fe247654e7"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Execution Evidence" |  |
| <a id="s-b1b9bdc7f0"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-35500bba5f))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-187883e89a"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-61a9572469)); (type="null")]; default=null |  |
| <a id="s-e0bda94812"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-686885d6b8"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-90be301ea5)); (type="null")]; default=null |  |
| <a id="s-5a611285e7"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-c6dfd0f28b"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-fb053cfa40)); (type="null")]; default=null |  |
| <a id="s-08dffe89b8"></a>`observer` | yes | [ObserverImplementation](#s-53f5ddfdf6) |  |
| <a id="s-c682ac2a82"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-c95e8d0fa3"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-f2ea418e3f"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-818d62163b"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-67a774aed3"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-d8c7f8838f"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-48c029b0e4)); minItems=1; title="Subjects" |  |

### <a id="s-ba0cab1e94"></a>definition `ControllerEvidence`

- <a id="s-326f625358"></a>`type`: `"object"`
- <a id="s-157e950eef"></a>`additionalProperties`: `false`
- <a id="s-ab4875364b"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`
- <a id="s-24704f5470"></a>`title`: `"ControllerEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-628ec383a4"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Controller Evidence Sha256" |  |
| <a id="s-f319b11538"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-d6d1b42663) |  |
| <a id="s-861e02cca2"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1"; title="Format" |  |

### <a id="s-05ee0ceec3"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-f4f23d7a7c"></a>`type`: `"string"`
- <a id="s-d1f9d437be"></a>`enum`: `["encrypted-at-rest","memory-backed"]`
- <a id="s-fa6a4640ac"></a>`description`: `"Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy."`

### <a id="s-81cfff6704"></a>definition `EffectPlan`

- <a id="s-474382a340"></a>`type`: `"object"`
- <a id="s-10e84ffb97"></a>`additionalProperties`: `false`
- <a id="s-9c168a8f05"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`
- <a id="s-cf5d9ec858"></a>`title`: `"EffectPlan"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-724dab03ff"></a>`inputs` | yes | [TargetInputAuthority](#s-32ba1dc94b) |  |
| <a id="s-4aa85e938a"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Intent" |  |
| <a id="s-d1778e78cf"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-d81dbbc3fc"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-e515567e79"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-33128cdcb7"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-1caabd4a60"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1"; title="Protocol" |  |
| <a id="s-77ae67ab1f"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-adaac4167a"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-373b742306"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Target Options" |  |

### <a id="s-fcab002bb2"></a>definition `EvaluationBinding`

- <a id="s-53399bef60"></a>`type`: `"object"`
- <a id="s-b868738b36"></a>`additionalProperties`: `false`
- <a id="s-0ab99410aa"></a>`description`: `"Immutable membership of one work item in a trial/evaluation matrix."`
- <a id="s-b3bf042b68"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`
- <a id="s-353ee7a340"></a>`title`: `"EvaluationBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-baf572436d"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Evaluation Id" |  |
| <a id="s-45ab1bf357"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Matrix Sha256" |  |
| <a id="s-2d22f039dd"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Parameters" |  |
| <a id="s-fd0643edc7"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Variant Id" |  |

### <a id="s-d6d1b42663"></a>definition `ExecutionEnvelope`

- <a id="s-11e917844a"></a>`type`: `"object"`
- <a id="s-c8c7ed59f1"></a>`additionalProperties`: `false`
- <a id="s-f655a45ebd"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`
- <a id="s-1e8d729ddd"></a>`title`: `"ExecutionEnvelope"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88e89c334d"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-b8aeb904c6"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Envelope Sha256" |  |
| <a id="s-c5718b94c5"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-9aadcfc131"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1"; title="Format" |  |
| <a id="s-f1d9131b16"></a>`target_plan` | yes | [TargetPlanBinding](#s-30e6867f94) |  |
| <a id="s-e8273584ef"></a>`workflow_plan` | yes | [WorkflowPlan](#s-06c3ebb676) |  |

### <a id="s-465ef625e0"></a>definition `JoinWorkBinding`

- <a id="s-49ae66c5ea"></a>`type`: `"object"`
- <a id="s-a9a9d98dfc"></a>`additionalProperties`: `false`
- <a id="s-9748372bac"></a>`description`: `"Stable branch-set lineage for one ordinary join work identity."`
- <a id="s-8382426382"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`
- <a id="s-7dd36243ee"></a>`title`: `"JoinWorkBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-045970648f"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Branch Set Sha256" |  |
| <a id="s-fca67acbaf"></a>`kind` | no | type="string"; const="join"; default="join"; title="Kind" |  |
| <a id="s-ff0bceec35"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-b02b34545d)); minItems=2; title="Members" |  |
| <a id="s-c2fe2f98e7"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

### <a id="s-b02b34545d"></a>definition `JoinWorkMemberBinding`

- <a id="s-1f9798e3fc"></a>`type`: `"object"`
- <a id="s-abd7853f3a"></a>`additionalProperties`: `false`
- <a id="s-6c02f490a7"></a>`description`: `"Exact successful branch result used to derive one join work identity."`
- <a id="s-edaa8487a5"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`
- <a id="s-bcf49cc207"></a>`title`: `"JoinWorkMemberBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b3961383ae"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-d2a1fd0755"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-457b19c11d"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Producer Settlement Sha256" |  |
| <a id="s-a472a2f78b"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |

### <a id="s-61a9572469"></a>definition `JsonSchemaValidationProfile`

- <a id="s-f81af88855"></a>`type`: `"object"`
- <a id="s-ae76df2b7f"></a>`additionalProperties`: `false`
- <a id="s-13762ad71e"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-49a9023e16"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9f9df25911"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-665fd84406"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-80bf9d2fd7"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-a7576d1128"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-d3083e27c3"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Schema" |  |

### <a id="s-35500bba5f"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-7c2dafd839"></a>definition `NonnegativeDecimal`

- <a id="s-e2e5e7b4e2"></a>`type`: `"string"`
- <a id="s-5fc64ef232"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-53f5ddfdf6"></a>definition `ObserverImplementation`

- <a id="s-1acd37a21f"></a>`type`: `"object"`
- <a id="s-2327b1b029"></a>`additionalProperties`: `false`
- <a id="s-2ee163bd57"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-0fe150010c"></a>`title`: `"ObserverImplementation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e34527d72"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-fc76d2d665"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-cbad6d6407"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-09f73b2244"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-d49324c029"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

### <a id="s-f7aecc543b"></a>definition `OperationIdentityRef`

- <a id="s-5a307772c6"></a>`type`: `"object"`
- <a id="s-c576017504"></a>`additionalProperties`: `false`
- <a id="s-5e2f27e482"></a>`description`: `"Embedded Stove0 reference to the Riverhog operation identity."`
- <a id="s-d24991e7d0"></a>`required`: `["id","sha256"]`
- <a id="s-af455a6a80"></a>`title`: `"OperationIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b0ac5a63b2"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-6abfb7ed0f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-7bdf7a73a3"></a>definition `RecipeIdentityRef`

- <a id="s-640c9d009f"></a>`type`: `"object"`
- <a id="s-9239c4302e"></a>`additionalProperties`: `false`
- <a id="s-9ff492064e"></a>`description`: `"Embedded Stove0 reference to the Riverhog recipe identity."`
- <a id="s-0b4dc34a52"></a>`required`: `["id","revision","sha256"]`
- <a id="s-23c4034805"></a>`title`: `"RecipeIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab1c4d0bd9"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-e47dfe854f"></a>`revision` | yes | [NonnegativeDecimal](#s-7c2dafd839); ge=1 |  |
| <a id="s-cb0a25e0ff"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-4a6151aff8"></a>definition `TargetCallbackAccess`

- <a id="s-4355582dd8"></a>`type`: `"object"`
- <a id="s-2a899aef83"></a>`additionalProperties`: `false`
- <a id="s-c42f4b3fce"></a>`description`: `"Secret-bearing execution callback authority excluded from plan identity."`
- <a id="s-e00cc73a9b"></a>`required`: `["stove0_base_url","token"]`
- <a id="s-bc4d700855"></a>`title`: `"TargetCallbackAccess"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4dcffe6116"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-1e9177a27f"></a>`stove0_base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Stove0 Base Url" |  |
| <a id="s-ee37b0e41e"></a>`token` | yes | type="string"; maxLength=4096; minLength=1; title="Token" |  |

### <a id="s-32ba1dc94b"></a>definition `TargetInputAuthority`

- <a id="s-2008d035d9"></a>`type`: `"object"`
- <a id="s-cc2f98a3df"></a>`additionalProperties`: `false`
- <a id="s-08e5af2ab3"></a>`description`: `"Small exact input authority retained by Stove0 and traversed in bounded pages."`
- <a id="s-8c4c1523c5"></a>`required`: `["selection","roles"]`
- <a id="s-594592d83e"></a>`title`: `"TargetInputAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a81e694238"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-44780440f4)); minItems=1; title="Roles" |  |
| <a id="s-44edff7f46"></a>`selection` | yes | [ArtifactSelectionRef](#s-657a10e518) |  |

### <a id="s-44780440f4"></a>definition `TargetInputRoleCount`

- <a id="s-20d1510703"></a>`type`: `"object"`
- <a id="s-f0d6a13ec6"></a>`additionalProperties`: `false`
- <a id="s-9d79676d03"></a>`required`: `["role","count"]`
- <a id="s-4c2f53a545"></a>`title`: `"TargetInputRoleCount"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5fd1be1e9a"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-aa0c676731"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-2833c36a58"></a>definition `TargetJobDeclaration`

- <a id="s-ea6f3b0485"></a>`type`: `"object"`
- <a id="s-ca1f9b9847"></a>`additionalProperties`: `false`
- <a id="s-c5754cefa9"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","declared_workspace_protection"]`
- <a id="s-fafe4a3027"></a>`title`: `"TargetJobDeclaration"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9f034ab875"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-699d5593d4"></a>`controller_evidence` | yes | [ControllerEvidence](#s-ba0cab1e94) |  |
| <a id="s-c0fca41396"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-05ee0ceec3) |  |
| <a id="s-abf6b01141"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-534f02f71e"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-4775dd90c6"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-d9d22e348c)); ([EffectPlan](#s-81cfff6704))]; title="Plan" |  |

### <a id="s-30e6867f94"></a>definition `TargetPlanBinding`

- <a id="s-244b1edb5a"></a>`type`: `"object"`
- <a id="s-33504ef8e8"></a>`additionalProperties`: `false`
- <a id="s-a85b133b9c"></a>`description`: `"Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope."`
- <a id="s-059188d13f"></a>`required`: `["protocol","target_implementation_id","target_descriptor_sha256","operation_contract_sha256","plan","plan_sha256"]`
- <a id="s-b29a1c4471"></a>`title`: `"TargetPlanBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-579f6f10cf"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-bcae9aec73"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Plan" |  |
| <a id="s-31b7123d16"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-5a1b9de4c7"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Protocol" |  |
| <a id="s-ff709aae6b"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-1714f8fa05"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |

### <a id="s-0dadecd6af"></a>definition `TargetRuntimeAuthority`

- <a id="s-4f43c0be6d"></a>`type`: `"object"`
- <a id="s-24ae8a842b"></a>`additionalProperties`: `false`
- <a id="s-4ab4f0ae19"></a>`required`: `["riverhog_base_url","capability_token"]`
- <a id="s-300c1282d0"></a>`title`: `"TargetRuntimeAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9820237ec8"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-77d9ec5ed5"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1; title="Capability Token" |  |
| <a id="s-a98bb1d70d"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-d4abf742d6"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1"; title="Transport" |  |

### <a id="s-d9d22e348c"></a>definition `TransformPlan`

- <a id="s-775537c9db"></a>`type`: `"object"`
- <a id="s-ab8b38f6d0"></a>`additionalProperties`: `false`
- <a id="s-83369f69dd"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`
- <a id="s-929bceb568"></a>`title`: `"TransformPlan"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b34338c783"></a>`inputs` | yes | [TargetInputAuthority](#s-32ba1dc94b) |  |
| <a id="s-f0985c09ce"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Intent" |  |
| <a id="s-f9eda71177"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-54caf54d55"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-b77f44fc2d"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-7e9999279c"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-ef1e823e33"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-216c18991c"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-33c44ece06"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-ef78b87819"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Target Options" |  |

### <a id="s-48c029b0e4"></a>definition `WorkArtifactSubject`

- <a id="s-20fddf9462"></a>`type`: `"object"`
- <a id="s-381f0abe3e"></a>`additionalProperties`: `false`
- <a id="s-b5b1e9ac84"></a>`description`: `"A collection logical file assigned an ID and role within one Stove0 work."`
- <a id="s-ff53746ef8"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-9ae02e2ca4"></a>`title`: `"WorkArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4cf8c98834"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-e1610d46e4"></a>`collection` | yes | [CollectionRootIdentityRef](#s-43fbbd3c35) |  |
| <a id="s-60c9ecd4ed"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-51d3491e3d"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-70b28d302a"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-3a923c6f5d"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-2eb8985aab"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-fa0e1b5a44"></a>definition `WorkIdentity`

- <a id="s-cc546d9a0f"></a>`type`: `"object"`
- <a id="s-4175de3bd9"></a>`additionalProperties`: `false`
- <a id="s-052a9ab085"></a>`required`: `["recipe","inputs","work_id"]`
- <a id="s-833ad5a956"></a>`title`: `"WorkIdentity"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c7c4f32480"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Effective Intent" |  |
| <a id="s-e7b558aac7"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-fcab002bb2)); (type="null")]; default=null |  |
| <a id="s-79017bc1a8"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-21ae556270)); ([JoinWorkBinding](#s-465ef625e0))]); (type="null")]; default=null; title="Fork Join" |  |
| <a id="s-9ffa159b9f"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1"; title="Format" |  |
| <a id="s-e9199280e9"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-43fbbd3c35)); minItems=1; title="Inputs" |  |
| <a id="s-a73313c59e"></a>`recipe` | yes | [RecipeIdentityRef](#s-7bdf7a73a3) |  |
| <a id="s-1426db3034"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-06c3ebb676"></a>definition `WorkflowPlan`

- <a id="s-8ef45c706c"></a>`type`: `"object"`
- <a id="s-1b743d6135"></a>`additionalProperties`: `false`
- <a id="s-f20a9e70af"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`
- <a id="s-ad2d8e17cd"></a>`title`: `"WorkflowPlan"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c7d58110d7"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1"; title="Format" |  |
| <a id="s-1a5c29c53c"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-1f133d2b1c"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-f50bdec28b)); title="Observations" |  |
| <a id="s-fe524ef0d5"></a>`operation` | yes | [OperationIdentityRef](#s-f7aecc543b) |  |
| <a id="s-fb9d96e479"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Output Policy" |  |
| <a id="s-31642f7d92"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-35500bba5f)); title="Requested Target Options" |  |
| <a id="s-c66b12982f"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-67723e141f"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-7f045fe06f"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-1c3548a5fe"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-c66d80d7a4"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Target Registration Id" |  |
| <a id="s-c0877071c7"></a>`work` | yes | [WorkIdentity](#s-fa0e1b5a44) |  |
| <a id="s-e4a514655d"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field request_sha256](#s-8724456f30) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetCallbackAccess · field stove0_base_url](#s-1e9177a27f) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [definition TargetCallbackAccess · field token](#s-ee37b0e41e) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition TargetRuntimeAuthority · field capability_token](#s-77d9ec5ed5) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition TargetRuntimeAuthority · field riverhog_base_url](#s-a98bb1d70d) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-08ac4fe1f5"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-090b7c06a8"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetJobRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21b19a6c33b4c1a8a0c01d8dc6ba9e7192313589f6e83c0d02189679418d72d9 -->

```json
{
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
    "TargetCallbackAccess": {
      "additionalProperties": false,
      "description": "Secret-bearing execution callback authority excluded from plan identity.",
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "stove0_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Stove0 Base Url",
          "type": "string"
        },
        "token": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Token",
          "type": "string"
        }
      },
      "required": [
        "stove0_base_url",
        "token"
      ],
      "title": "TargetCallbackAccess",
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
    "TargetRuntimeAuthority": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "capability_token": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Capability Token",
          "type": "string"
        },
        "riverhog_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Riverhog Base Url",
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "title": "Transport",
          "type": "string"
        }
      },
      "required": [
        "riverhog_base_url",
        "capability_token"
      ],
      "title": "TargetRuntimeAuthority",
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
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
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
  "description": "Secret-bearing target invocation; never store this document durably.",
  "properties": {
    "callback_access": {
      "$ref": "#/$defs/TargetCallbackAccess"
    },
    "declaration": {
      "$ref": "#/$defs/TargetJobDeclaration"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
      "type": "string"
    },
    "runtime": {
      "$ref": "#/$defs/TargetRuntimeAuthority"
    }
  },
  "required": [
    "declaration",
    "runtime",
    "callback_access",
    "request_sha256"
  ],
  "title": "TargetJobRequest",
  "type": "object"
}
```

</details>
