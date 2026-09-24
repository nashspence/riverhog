# stove0_protocol.WorkflowPreviewPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload:84aca37c7e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2461dd6b8e"></a>
- <a id="s-5ea870c95d"></a>`distribution`: `stove0-protocol`
- <a id="s-d6d5456dcd"></a>`module`: `stove0_protocol`
- <a id="s-ff0d754d7e"></a>`name`: `WorkflowPreviewPayload`
- <a id="s-38713e24cf"></a>`unit`: `export`

### Declared structure

- <a id="s-96661c935c"></a>`kind`: `"class"`
- <a id="s-a5e07e8608"></a>`signature`: `"\"(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ContentObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan \| None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome \| None = None, warnings: tuple[str, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-1e78bdb07c"></a>

- <a id="s-feb6ba32f5"></a>`type`: `"object"`
- <a id="s-ae80e5fbc3"></a>`additionalProperties`: `false`
- <a id="s-02b697d640"></a>`required`: `["preview_id","state","work"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b30dcb3d0a"></a>`branch_set_plan` | no | anyOf=[([BranchSetPlan](#s-ac347c0cd8)); (type="null")]; default=null |  |
| <a id="s-c379edf48f"></a>`branch_sets` | no | type="array"; default=[]; items=([BranchSetPlan](#s-ac347c0cd8)) |  |
| <a id="s-31b4b211e0"></a>`format` | no | type="string"; const="stove0-workflow-preview/v1"; default="stove0-workflow-preview/v1" |  |
| <a id="s-1adcd4dab7"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-5aa3687009)) |  |
| <a id="s-fe36f7ef89"></a>`outcome` | no | anyOf=[([PreviewOutcome](#s-646bc7313c)); (type="null")]; default=null |  |
| <a id="s-938257af25"></a>`preview_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-04b2ffaddb"></a>`selections` | no | type="array"; default=[]; items=([ArtifactSelection](#s-f60b1a5e54)) |  |
| <a id="s-9203fc6d7a"></a>`state` | yes | type="string"; enum=["ready","inapplicable","failed","canceled"] |  |
| <a id="s-5b1a659ab4"></a>`target_plans` | no | type="array"; default=[]; items=([BranchTargetPreview](#s-c63d6c3a12)) |  |
| <a id="s-edfd6642ed"></a>`warnings` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-1d1f04c8df"></a>`work` | yes | [WorkIdentity](#s-2dee6e501d) |  |

##### Definitions

- [ArtifactSelection](#s-f60b1a5e54)
- [ArtifactSelectionRef](#s-25c3352ca0)
- [BranchPlan](#s-e80af9ea09)
- [BranchSetPlan](#s-ac347c0cd8)
- [BranchTargetPreview](#s-c63d6c3a12)
- [BranchWorkBinding](#s-98a9d5f252)
- [CollectionId](#s-698d38abc2)
- [CollectionRootIdentityRef](#s-4691e14417)
- [ContentObservationEvidence](#s-5aa3687009)
- [ContentObservationFailure](#s-23599ba1be)
- [ContentObservationInapplicable](#s-e8b8a9f0a7)
- [ContentObservationRequest](#s-50dcddc221)
- [ContentObservationResult](#s-d1543bb528)
- [CoordinationBranchPlan](#s-b3f4effed9)
- [EvaluationBinding](#s-728a09528c)
- [JoinDeclaration](#s-c2ab9188ec)
- [JoinMemberDeclaration](#s-e8bf375b4c)
- [JoinWorkBinding](#s-f1638f3bf8)
- [JoinWorkMemberBinding](#s-4672a5a5ce)
- [JsonSchemaValidationProfile](#s-d025436fab)
- [JsonValue](#s-b9e54fccce)
- [NonnegativeDecimal](#s-3e1519eb20)
- [ObserverImplementation](#s-e2e80dc9e7)
- [OperationIdentityRef](#s-75f088b2ca)
- [PreviewOutcome](#s-646bc7313c)
- [RecipeIdentityRef](#s-146be65276)
- [TargetPlanBinding](#s-309ba1a6a2)
- [WorkArtifactSubject](#s-6e5bbbaa33)
- [WorkIdentity](#s-2dee6e501d)
- [WorkflowPlan](#s-fc2771c64b)
- [WorkflowPlanIntent](#s-d91b346052)

##### <a id="s-f60b1a5e54"></a>definition `ArtifactSelection`

- <a id="s-2f5610dec0"></a>`type`: `"object"`
- <a id="s-69d933cc58"></a>`additionalProperties`: `false`
- <a id="s-a7b2fedd1f"></a>`required`: `["artifacts","artifact_count","total_bytes","selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa8051f5ea"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-156cb2afc4"></a>`artifacts` | yes | type="array"; items=([WorkArtifactSubject](#s-6e5bbbaa33)); minItems=1 |  |
| <a id="s-632339253f"></a>`format` | no | type="string"; const="stove0-artifact-selection/v1"; default="stove0-artifact-selection/v1" |  |
| <a id="s-d573a9f0fc"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0927dc42b5"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-3e1519eb20); ge=0 |  |

##### <a id="s-25c3352ca0"></a>definition `ArtifactSelectionRef`

- <a id="s-0805432c10"></a>`type`: `"object"`
- <a id="s-285b35c525"></a>`additionalProperties`: `false`
- <a id="s-3d2076b00b"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-412659fba1"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-200dff9845"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-30a4b9c965"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-3e1519eb20); ge=0 |  |

##### <a id="s-e80af9ea09"></a>definition `BranchPlan`

- <a id="s-18a5cd5198"></a>`type`: `"object"`
- <a id="s-09b4c09d4e"></a>`additionalProperties`: `false`
- <a id="s-4fb0fcd7f3"></a>`required`: `["branch_id","artifact_selection","workflow_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-45377d48d5"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-25c3352ca0) |  |
| <a id="s-9799d56c9a"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d656602cac"></a>`kind` | no | type="string"; const="leaf"; default="leaf" |  |
| <a id="s-115b2ae419"></a>`workflow_plan` | yes | [WorkflowPlan](#s-fc2771c64b) |  |

##### <a id="s-ac347c0cd8"></a>definition `BranchSetPlan`

- <a id="s-6c44c2bb64"></a>`type`: `"object"`
- <a id="s-5a47bde8fb"></a>`additionalProperties`: `false`
- <a id="s-6371532c9c"></a>`required`: `["parent_work","decision_sha256","branches","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f37732e752"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0f8338fe4e"></a>`branches` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/CoordinationBranchPlan","leaf":"#/$defs/BranchPlan"},"propertyName":"kind"}; oneOf=[([BranchPlan](#s-e80af9ea09)); ([CoordinationBranchPlan](#s-b3f4effed9))]); minItems=1 |  |
| <a id="s-d0640752d6"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-19670075ab"></a>`evidence_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-cff47816ca"></a>`format` | no | type="string"; const="stove0-branch-set/v1"; default="stove0-branch-set/v1" |  |
| <a id="s-b7bf168cb4"></a>`join` | no | anyOf=[([JoinDeclaration](#s-c2ab9188ec)); (type="null")]; default=null |  |
| <a id="s-34a5107c1e"></a>`parent_work` | yes | [WorkIdentity](#s-2dee6e501d) |  |
| <a id="s-44dd962fe3"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-1947896a76"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### <a id="s-c63d6c3a12"></a>definition `BranchTargetPreview`

- <a id="s-647ef94248"></a>`type`: `"object"`
- <a id="s-7d2964ca4a"></a>`additionalProperties`: `false`
- <a id="s-9000b41694"></a>`required`: `["branch_id","work_id","workflow_plan_sha256","target_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-987440e37e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b1f7da2bbd"></a>`target_plan` | yes | [TargetPlanBinding](#s-309ba1a6a2) |  |
| <a id="s-36ac5216b9"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2183a94484"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-98a9d5f252"></a>definition `BranchWorkBinding`

- <a id="s-19032a2fe9"></a>`type`: `"object"`
- <a id="s-e09e590ea7"></a>`additionalProperties`: `false`
- <a id="s-90e5249627"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-68f235ff3e"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-836b659e80"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d4f959bcaa"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2046c4a6db"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-73243fd922"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-698d38abc2"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-93b9e2805a"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-7c5e721ab0"></a>2 | not=(const="0") |

##### <a id="s-4691e14417"></a>definition `CollectionRootIdentityRef`

- <a id="s-4a94ba67b0"></a>`type`: `"object"`
- <a id="s-7ea86c1e2b"></a>`additionalProperties`: `false`
- <a id="s-d94fa1980f"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dc96fcfe6c"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a257234ecd"></a>`collection_id` | yes | [CollectionId](#s-698d38abc2) |  |
| <a id="s-95f5b36251"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5aa3687009"></a>definition `ContentObservationEvidence`

- <a id="s-b4ea4d2f13"></a>`type`: `"object"`
- <a id="s-a71272547b"></a>`additionalProperties`: `false`
- <a id="s-4795316fe4"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7c57a91344"></a>`request` | yes | [ContentObservationRequest](#s-50dcddc221) |  |
| <a id="s-d262a56f65"></a>`result` | yes | [ContentObservationResult](#s-d1543bb528) |  |

##### <a id="s-23599ba1be"></a>definition `ContentObservationFailure`

- <a id="s-242ca0c04d"></a>`type`: `"object"`
- <a id="s-25b0a7b6c5"></a>`additionalProperties`: `false`
- <a id="s-8306f2bd11"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a11c93507"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a6fa6386dd"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-a08e2a78b9"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-e8b8a9f0a7"></a>definition `ContentObservationInapplicable`

- <a id="s-68a008c968"></a>`type`: `"object"`
- <a id="s-fe5b107453"></a>`additionalProperties`: `false`
- <a id="s-53cdc9f25e"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95ea6635f0"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-363a03b624"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-50dcddc221"></a>definition `ContentObservationRequest`

- <a id="s-152af6dd7e"></a>`type`: `"object"`
- <a id="s-7f8c800e41"></a>`additionalProperties`: `false`
- <a id="s-232b33a814"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b40afef7d2"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-0423ed842b"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-241b34b350"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4d97686120"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e43330147c"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d1775bab1"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-248a0b5d23"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-73b99175dd"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c536c8c5e2"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-aa499b0cec"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-6e5bbbaa33)); minItems=1 |  |
| <a id="s-6d9390eb25"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-f16f725582"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d1543bb528"></a>definition `ContentObservationResult`

- <a id="s-93a6b95f28"></a>`type`: `"object"`
- <a id="s-1a1e2e422c"></a>`additionalProperties`: `false`
- <a id="s-35e6277c82"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a99cdb7e2"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-edfff6c62c"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-b9e54fccce))); (type="null")]; default=null |  |
| <a id="s-eecaee54f9"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-d025436fab)); (type="null")]; default=null |  |
| <a id="s-2ac2757e5f"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-33a09a90bc"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-23599ba1be)); (type="null")]; default=null |  |
| <a id="s-89edbe7f13"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-1ef1402905"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-e8b8a9f0a7)); (type="null")]; default=null |  |
| <a id="s-c4a720a5fc"></a>`observer` | yes | [ObserverImplementation](#s-e2e80dc9e7) |  |
| <a id="s-22d6a38c9e"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1c509888d2"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5e0a59f218"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f787928973"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-655b98a78a"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-a833f43c68"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-6e5bbbaa33)); minItems=1 |  |

##### <a id="s-b3f4effed9"></a>definition `CoordinationBranchPlan`

- <a id="s-8c9205d7e3"></a>`type`: `"object"`
- <a id="s-848e26b584"></a>`additionalProperties`: `false`
- <a id="s-ebe5115a0a"></a>`required`: `["branch_id","artifact_selection","work","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d64f075b3b"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-25c3352ca0) |  |
| <a id="s-79739dce80"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b894fc1348"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-81f6314782"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-bd2607ad7e"></a>`work` | yes | [WorkIdentity](#s-2dee6e501d) |  |

##### <a id="s-728a09528c"></a>definition `EvaluationBinding`

- <a id="s-6b30077ef4"></a>`type`: `"object"`
- <a id="s-b2e1bd2350"></a>`additionalProperties`: `false`
- <a id="s-b5d2a73fbb"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4dde6a79e2"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c700df7d9b"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-778027f779"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-8665299be2"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-c2ab9188ec"></a>definition `JoinDeclaration`

- <a id="s-c0b0006878"></a>`type`: `"object"`
- <a id="s-83a7e7ba35"></a>`additionalProperties`: `false`
- <a id="s-75810bb6d6"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5496a92ce2"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-446871ed26"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-bfd2986a94"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e72e468be0"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-e8bf375b4c)); minItems=2 |  |
| <a id="s-769d54910a"></a>`recipe` | yes | [RecipeIdentityRef](#s-146be65276) |  |
| <a id="s-de227ea2cb"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-d91b346052) |  |

##### <a id="s-e8bf375b4c"></a>definition `JoinMemberDeclaration`

- <a id="s-c50b64869b"></a>`type`: `"object"`
- <a id="s-38d833e633"></a>`additionalProperties`: `false`
- <a id="s-f2a09198c9"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f72f71d252"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8373029cd2"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-f1638f3bf8"></a>definition `JoinWorkBinding`

- <a id="s-05c80fa2d3"></a>`type`: `"object"`
- <a id="s-27bc336d22"></a>`additionalProperties`: `false`
- <a id="s-3627ab6382"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61f9f1045d"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f6ab173870"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-4a49302541"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-4672a5a5ce)); minItems=2 |  |
| <a id="s-b6f2c19b3f"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4672a5a5ce"></a>definition `JoinWorkMemberBinding`

- <a id="s-fe0f756f4b"></a>`type`: `"object"`
- <a id="s-9f11953e2d"></a>`additionalProperties`: `false`
- <a id="s-9ed6e5dcd8"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdc5cd9b36"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8d7731a20a"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0cfc6f7c94"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-61f82bfc80"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d025436fab"></a>definition `JsonSchemaValidationProfile`

- <a id="s-88125417db"></a>`type`: `"object"`
- <a id="s-724300a750"></a>`additionalProperties`: `false`
- <a id="s-140f006fd9"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-49aa7716e8"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-95d3cbbb29"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-071a429220"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-64952c27a0"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d726cd3ae7"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |

##### <a id="s-b9e54fccce"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-3e1519eb20"></a>definition `NonnegativeDecimal`

- <a id="s-240cfedd0d"></a>`type`: `"string"`
- <a id="s-71cccf0c54"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-e2e80dc9e7"></a>definition `ObserverImplementation`

- <a id="s-ac6a9e515d"></a>`type`: `"object"`
- <a id="s-513b00d53f"></a>`additionalProperties`: `false`
- <a id="s-9525099828"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3da710bc54"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-259f118208"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-58d0bd1cc8"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-4d18f5ac37"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-535112c4b8"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-75f088b2ca"></a>definition `OperationIdentityRef`

- <a id="s-2e1d766b68"></a>`type`: `"object"`
- <a id="s-6ff87efe1f"></a>`additionalProperties`: `false`
- <a id="s-9557917231"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1335d25149"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-29c39fc59a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-646bc7313c"></a>definition `PreviewOutcome`

- <a id="s-c32fdc5311"></a>`type`: `"object"`
- <a id="s-2c329555db"></a>`additionalProperties`: `false`
- <a id="s-14bd9b314d"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c082355983"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e5b6f7ce65"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-4dd4a4495c"></a>`retryable` | no | anyOf=[(type="boolean"); (type="null")]; default=null |  |

##### <a id="s-146be65276"></a>definition `RecipeIdentityRef`

- <a id="s-224bf9712c"></a>`type`: `"object"`
- <a id="s-b43eca6d7d"></a>`additionalProperties`: `false`
- <a id="s-d1e4c49e3e"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d578aad87e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-15c9825d37"></a>`revision` | yes | [NonnegativeDecimal](#s-3e1519eb20); ge=1 |  |
| <a id="s-af09a82118"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-309ba1a6a2"></a>definition `TargetPlanBinding`

- <a id="s-b9fe4a2b37"></a>`type`: `"object"`
- <a id="s-1f6e7c7253"></a>`additionalProperties`: `false`
- <a id="s-8bd368d4c9"></a>`required`: `["protocol","target_implementation_id","target_descriptor_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c515fc3dd"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e35df6ae44"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-24d8c1cd98"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-94abd444d9"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5e6fed0b3a"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0c6d4193ca"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-6e5bbbaa33"></a>definition `WorkArtifactSubject`

- <a id="s-6b98fb8ee6"></a>`type`: `"object"`
- <a id="s-49157ae307"></a>`additionalProperties`: `false`
- <a id="s-3a9d511186"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c16ca5a07f"></a>`bytes` | yes | [NonnegativeDecimal](#s-3e1519eb20); ge=0 |  |
| <a id="s-2fd003fbae"></a>`collection` | yes | [CollectionRootIdentityRef](#s-4691e14417) |  |
| <a id="s-917092ebf8"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-1b9e9a1648"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-95f6943e9c"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-47853a90f2"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-07ab22c5d3"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2dee6e501d"></a>definition `WorkIdentity`

- <a id="s-e39a8d12f1"></a>`type`: `"object"`
- <a id="s-e2bb16c626"></a>`additionalProperties`: `false`
- <a id="s-9a382510d5"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fbbb28701f"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-4b8285efa9"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-728a09528c)); (type="null")]; default=null |  |
| <a id="s-cfd8863aac"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-98a9d5f252)); ([JoinWorkBinding](#s-f1638f3bf8))]); (type="null")]; default=null |  |
| <a id="s-58c8f33d75"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-761fc61acc"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-4691e14417)); minItems=1 |  |
| <a id="s-efd732d228"></a>`recipe` | yes | [RecipeIdentityRef](#s-146be65276) |  |
| <a id="s-9f20ce4900"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fc2771c64b"></a>definition `WorkflowPlan`

- <a id="s-e61b052b20"></a>`type`: `"object"`
- <a id="s-78a502fa54"></a>`additionalProperties`: `false`
- <a id="s-4b7e16109d"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-10d93a6828"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-b103ba45ab"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-a8650e06d9"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-5aa3687009)) |  |
| <a id="s-ebf5058853"></a>`operation` | yes | [OperationIdentityRef](#s-75f088b2ca) |  |
| <a id="s-197c42c93d"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-3d69eb8ea5"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-abd9c4730c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-73e5fd1e59"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-1b3c4798a9"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-815139e1f7"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-baa8fa4030"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-dd171af85a"></a>`work` | yes | [WorkIdentity](#s-2dee6e501d) |  |
| <a id="s-13cdf27c31"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d91b346052"></a>definition `WorkflowPlanIntent`

- <a id="s-5085634746"></a>`type`: `"object"`
- <a id="s-a71ea4cdb7"></a>`additionalProperties`: `false`
- <a id="s-4b91d2f39c"></a>`required`: `["operation","target_registration_id","target_descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f04cf9a48a"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-f843d10730"></a>`operation` | yes | [OperationIdentityRef](#s-75f088b2ca) |  |
| <a id="s-43dd393160"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-a557b541fd"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b9e54fccce)) |  |
| <a id="s-b27bc722e7"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-7c66ff9f27"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-43045a4333"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-e9e5eb33e3"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c951de7bc9"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_warnings](stove0-protocol-workflowpreviewpayload-canonical-warnings.md)
- [canonical_target_plans](stove0-protocol-workflowpreviewpayload-canonical-target-plans.md)
- [canonical_observations](stove0-protocol-workflowpreviewpayload-canonical-observations.md)
- [canonical_selections](stove0-protocol-workflowpreviewpayload-canonical-selections.md)
- [canonical_child_branch_sets](stove0-protocol-workflowpreviewpayload-canonical-child-branch-sets.md)
- [validate_state](stove0-protocol-workflowpreviewpayload-validate-state.md)

## Governing policies

- <a id="pa-3d08a5cfc9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99c2d88374dee57a90bdbb7b84c7457651539ec6e0d1761792b7328ec57c10a3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelection": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "artifacts": {
              "items": {
                "$ref": "#/$defs/WorkArtifactSubject"
              },
              "minItems": 1,
              "type": "array"
            },
            "format": {
              "const": "stove0-artifact-selection/v1",
              "default": "stove0-artifact-selection/v1",
              "type": "string"
            },
            "selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            }
          },
          "required": [
            "artifacts",
            "artifact_count",
            "total_bytes",
            "selection_sha256"
          ],
          "type": "object"
        },
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
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
          "type": "object"
        },
        "BranchPlan": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "kind": {
              "const": "leaf",
              "default": "leaf",
              "type": "string"
            },
            "workflow_plan": {
              "$ref": "#/$defs/WorkflowPlan"
            }
          },
          "required": [
            "branch_id",
            "artifact_selection",
            "workflow_plan"
          ],
          "type": "object"
        },
        "BranchSetPlan": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "branches": {
              "items": {
                "discriminator": {
                  "mapping": {
                    "coordination": "#/$defs/CoordinationBranchPlan",
                    "leaf": "#/$defs/BranchPlan"
                  },
                  "propertyName": "kind"
                },
                "oneOf": [
                  {
                    "$ref": "#/$defs/BranchPlan"
                  },
                  {
                    "$ref": "#/$defs/CoordinationBranchPlan"
                  }
                ]
              },
              "minItems": 1,
              "type": "array"
            },
            "decision_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "evidence_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "format": {
              "const": "stove0-branch-set/v1",
              "default": "stove0-branch-set/v1",
              "type": "string"
            },
            "join": {
              "anyOf": [
                {
                  "$ref": "#/$defs/JoinDeclaration"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "parent_work": {
              "$ref": "#/$defs/WorkIdentity"
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
            }
          },
          "required": [
            "parent_work",
            "decision_sha256",
            "branches",
            "branch_set_sha256"
          ],
          "type": "object"
        },
        "BranchTargetPreview": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_plan": {
              "$ref": "#/$defs/TargetPlanBinding"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "workflow_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "work_id",
            "workflow_plan_sha256",
            "target_plan"
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
        "CoordinationBranchPlan": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "kind": {
              "const": "coordination",
              "default": "coordination",
              "type": "string"
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            }
          },
          "required": [
            "branch_id",
            "artifact_selection",
            "work",
            "branch_set_sha256"
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
        "JoinDeclaration": {
          "additionalProperties": false,
          "properties": {
            "effective_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "format": {
              "const": "stove0-join-declaration/v1",
              "default": "stove0-join-declaration/v1",
              "type": "string"
            },
            "join_declaration_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "members": {
              "items": {
                "$ref": "#/$defs/JoinMemberDeclaration"
              },
              "minItems": 2,
              "type": "array"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeIdentityRef"
            },
            "workflow_intent": {
              "$ref": "#/$defs/WorkflowPlanIntent"
            }
          },
          "required": [
            "members",
            "recipe",
            "workflow_intent",
            "join_declaration_sha256"
          ],
          "type": "object"
        },
        "JoinMemberDeclaration": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "output_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "branch_id",
            "output_roles"
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
        "PreviewOutcome": {
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
              "anyOf": [
                {
                  "type": "boolean"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "code",
            "message"
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
        "WorkArtifactSubject": {
          "additionalProperties": false,
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
        },
        "WorkflowPlanIntent": {
          "additionalProperties": false,
          "properties": {
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
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
            }
          },
          "required": [
            "operation",
            "target_registration_id",
            "target_descriptor_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "branch_set_plan": {
          "anyOf": [
            {
              "$ref": "#/$defs/BranchSetPlan"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "branch_sets": {
          "default": [],
          "items": {
            "$ref": "#/$defs/BranchSetPlan"
          },
          "type": "array"
        },
        "format": {
          "const": "stove0-workflow-preview/v1",
          "default": "stove0-workflow-preview/v1",
          "type": "string"
        },
        "observations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ContentObservationEvidence"
          },
          "type": "array"
        },
        "outcome": {
          "anyOf": [
            {
              "$ref": "#/$defs/PreviewOutcome"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "preview_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "selections": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ArtifactSelection"
          },
          "type": "array"
        },
        "state": {
          "enum": [
            "ready",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "type": "string"
        },
        "target_plans": {
          "default": [],
          "items": {
            "$ref": "#/$defs/BranchTargetPreview"
          },
          "type": "array"
        },
        "warnings": {
          "default": [],
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        }
      },
      "required": [
        "preview_id",
        "state",
        "work"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ContentObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome | None = None, warnings: tuple[str, ...] = ()) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreviewPayload",
  "unit": "export"
}
```

</details>
