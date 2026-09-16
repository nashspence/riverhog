# stove0_operator_contracts.WorkPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workpage:33311faae3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2dd2b8da0b"></a>
- <a id="s-8b7fbfa500"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-6fd93a611c"></a>`module`: `stove0_operator_contracts`
- <a id="s-b3e67ec67c"></a>`name`: `WorkPage`
- <a id="s-447dd88c07"></a>`unit`: `export`

### Declared structure

- <a id="s-edae9587c9"></a>`kind`: `"class"`
- <a id="s-c26cfdc24b"></a>`signature`: `"\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken \| None, sort: Literal['updated_at', 'phase', 'work_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], work: tuple[stove0_operator_contracts.WorkView, ...]) -> None\""`

#### Validated model schema

<a id="s-b08a8d0f8d"></a>

- <a id="s-0792c9deff"></a>`type`: `"object"`
- <a id="s-1073440351"></a>`additionalProperties`: `false`
- <a id="s-b4e6dc45e2"></a>`required`: `["page_size","next_page_token","sort","order","filters","work"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-09c4782719"></a>`filters` | yes | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-164b603fbb"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](#s-c49e02645f)); (type="null")] |  |
| <a id="s-1debc18ecd"></a>`order` | yes | type="string"; enum=["asc","desc"] |  |
| <a id="s-35306e4609"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-1000225221"></a>`sort` | yes | type="string"; enum=["updated_at","phase","work_id"] |  |
| <a id="s-c8e577bfb7"></a>`work` | yes | type="array"; items=([WorkView](#s-4aa66e2d9c)) |  |

##### Definitions

- [AcceptedTargetJob](#s-208b06065a)
- [ArtifactDispositionSetIdentity](#s-c5c7727779)
- [ArtifactSelectionRef](#s-150a1801da)
- [ArtifactSubject](#s-de978a3ece)
- [BranchPlan](#s-f96334dc32)
- [BranchSetPlan](#s-49b578e783)
- [BranchWorkBinding](#s-8cbf22827d)
- [BrowsePageToken](#s-c49e02645f)
- [CollectionId](#s-1d96dcbbf7)
- [CollectionRootRef](#s-b17390db18)
- [ControllerEvidence](#s-44bea4f2a3)
- [CoordinationBranchPlan](#s-e7c9c504b1)
- [CoordinationChildSettlementRef](#s-37212f95c0)
- [CoordinationCollectionResult](#s-8d60ad63ea)
- [CoordinationSettlement](#s-3cd778c362)
- [EffectPlan](#s-40282e1d49)
- [EvaluationBinding](#s-bbce34584e)
- [ExecutionEnvelope](#s-43438037ba)
- [ExternalEffectReceipt](#s-a7307cbf35)
- [JoinDeclaration](#s-6119604037)
- [JoinInputPlan](#s-633723fedd)
- [JoinMemberDeclaration](#s-c91b761cb4)
- [JoinPlan](#s-7131fac719)
- [JoinWorkBinding](#s-4c5de6ce14)
- [JoinWorkMemberBinding](#s-5c808e6b0d)
- [JsonSchemaDocument](#s-b1b174c154)
- [JsonValue](#s-e69ffb8f42)
- [ObservationEvidence](#s-974a0b49dd)
- [ObservationFailure](#s-2b4f70265d)
- [ObservationInapplicable](#s-6ba44c630f)
- [ObservationRequest](#s-344e016956)
- [ObservationResult](#s-653fedbaa6)
- [ObserverImplementation](#s-4ea959a8e4)
- [OperationRef](#s-b94524bd3b)
- [OutputArtifactRoleCount](#s-d272ee95e3)
- [OutputArtifactSetIdentity](#s-e582b1dee5)
- [OutputCollectionRef](#s-b5248b631f)
- [PreviewAcceptanceView](#s-44ff8f56f5)
- [PreviewTargetExpectationView](#s-e8ea0d4175)
- [RecipeRef](#s-30ef719226)
- [TargetExecutionEvidence](#s-999ad9a563)
- [TargetFailure](#s-f978fe6e1d)
- [TargetInapplicable](#s-ebe3418092)
- [TargetInputAuthority](#s-1a92c22544)
- [TargetInputRoleCount](#s-59fa45567d)
- [TargetJobDeclaration](#s-2b32e80fe5)
- [TargetJobStatus](#s-5967cc9caf)
- [TargetOutputBindingSetIdentity](#s-bb7e7c05be)
- [TargetPlanBinding](#s-86dbed90c7)
- [TargetProductionAuthority](#s-147c06beda)
- [TargetProgress](#s-8600e54be5)
- [TargetSettlementAuthority](#s-03994a2ce6)
- [TransformPlan](#s-aebe8702e3)
- [WorkClaimView](#s-637d0dd0ad)
- [WorkFailureView](#s-28e671ad54)
- [WorkIdentity](#s-81716c1153)
- [WorkInapplicableView](#s-009ae10476)
- [WorkView](#s-4aa66e2d9c)
- [WorkflowPlan](#s-53509a0132)
- [WorkflowPlanIntent](#s-38187fd5bb)

##### <a id="s-208b06065a"></a>definition `AcceptedTargetJob`

- <a id="s-496580deec"></a>`type`: `"object"`
- <a id="s-896b8b2ffc"></a>`additionalProperties`: `false`
- <a id="s-3a9baf0f45"></a>`required`: `["declaration","request_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db5abf855f"></a>`declaration` | yes | [TargetJobDeclaration](#s-2b32e80fe5) |  |
| <a id="s-7e4ccafe00"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c5c7727779"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-252d2caa0b"></a>`type`: `"object"`
- <a id="s-3f31a3f37f"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d1744638a0"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-82c267ccdd"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-76b7e084a2"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-66df554da5"></a>`sha256` | yes | type="string" |  |

##### <a id="s-150a1801da"></a>definition `ArtifactSelectionRef`

- <a id="s-258b684159"></a>`type`: `"object"`
- <a id="s-be16d1f9f9"></a>`additionalProperties`: `false`
- <a id="s-6962712a10"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-076d5157e3"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-8c0d2110aa"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bf2317f217"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-de978a3ece"></a>definition `ArtifactSubject`

- <a id="s-b8d979fa75"></a>`type`: `"object"`
- <a id="s-d9c290ebfa"></a>`additionalProperties`: `false`
- <a id="s-dbc9361bd8"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b8eb98708e"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-6dc9b1b191"></a>`collection` | yes | [CollectionRootRef](#s-b17390db18) |  |
| <a id="s-68d83dfa9f"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-a92b55b8ce"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-74a38ed9ee"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-597709f0fd"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-62691c04b7"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f96334dc32"></a>definition `BranchPlan`

- <a id="s-2ff8848f7b"></a>`type`: `"object"`
- <a id="s-b9ef6de0e5"></a>`additionalProperties`: `false`
- <a id="s-20920649f2"></a>`required`: `["branch_id","artifact_selection","workflow_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ff5ae085c"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-150a1801da) |  |
| <a id="s-58286aef6e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e03f322b75"></a>`kind` | no | type="string"; const="leaf"; default="leaf" |  |
| <a id="s-93f71660ea"></a>`workflow_plan` | yes | [WorkflowPlan](#s-53509a0132) |  |

##### <a id="s-49b578e783"></a>definition `BranchSetPlan`

- <a id="s-acdfb51c71"></a>`type`: `"object"`
- <a id="s-f27fb476e1"></a>`additionalProperties`: `false`
- <a id="s-6b1653969c"></a>`required`: `["parent_work","decision_sha256","branches","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8239367b6b"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ebcaa1c84b"></a>`branches` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/CoordinationBranchPlan","leaf":"#/$defs/BranchPlan"},"propertyName":"kind"}; oneOf=[([BranchPlan](#s-f96334dc32)); ([CoordinationBranchPlan](#s-e7c9c504b1))]); minItems=1 |  |
| <a id="s-0650647b77"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7a6691ceb2"></a>`evidence_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-db3189b2b6"></a>`format` | no | type="string"; const="stove0-branch-set/v1"; default="stove0-branch-set/v1" |  |
| <a id="s-f4969f1e67"></a>`join` | no | anyOf=[([JoinDeclaration](#s-6119604037)); (type="null")]; default=null |  |
| <a id="s-5605fa6991"></a>`parent_work` | yes | [WorkIdentity](#s-81716c1153) |  |
| <a id="s-15a0c97bdd"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-6f3ccf7191"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### <a id="s-8cbf22827d"></a>definition `BranchWorkBinding`

- <a id="s-0f4910ffc3"></a>`type`: `"object"`
- <a id="s-c285fa3e8b"></a>`additionalProperties`: `false`
- <a id="s-0570aaa6a7"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8a9fac7589"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f75262b404"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b332bdf053"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fc94c623c9"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-efbeaa2cb9"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c49e02645f"></a>definition `BrowsePageToken`

- <a id="s-a49a3d7d31"></a>`type`: `"string"`
- <a id="s-01a458973e"></a>`maxLength`: `8192`
- <a id="s-7fed8d998f"></a>`minLength`: `1`

##### <a id="s-1d96dcbbf7"></a>definition `CollectionId`

- <a id="s-f10dfe8f7d"></a>`type`: `"integer"`
- <a id="s-d40443953a"></a>`minimum`: `1`

##### <a id="s-b17390db18"></a>definition `CollectionRootRef`

- <a id="s-708bf67ff0"></a>`type`: `"object"`
- <a id="s-ff8feb491b"></a>`additionalProperties`: `false`
- <a id="s-9b850c5ef8"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a3b7d39b64"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1b7b8c73c2"></a>`collection_id` | yes | [CollectionId](#s-1d96dcbbf7) |  |
| <a id="s-9a6e20f489"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-44bea4f2a3"></a>definition `ControllerEvidence`

- <a id="s-00876257f3"></a>`type`: `"object"`
- <a id="s-9934c41129"></a>`additionalProperties`: `false`
- <a id="s-f6fcb77ed6"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2523c8233e"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e4c84be55f"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-43438037ba) |  |
| <a id="s-3a7d1df33d"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-e7c9c504b1"></a>definition `CoordinationBranchPlan`

- <a id="s-3cc5dc6656"></a>`type`: `"object"`
- <a id="s-3107d1a64d"></a>`additionalProperties`: `false`
- <a id="s-d0013d9a9d"></a>`required`: `["branch_id","artifact_selection","work","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8505f89645"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-150a1801da) |  |
| <a id="s-c9197d5c98"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8fad96ab68"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0d0a5cada6"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-fd5c35659b"></a>`work` | yes | [WorkIdentity](#s-81716c1153) |  |

##### <a id="s-37212f95c0"></a>definition `CoordinationChildSettlementRef`

- <a id="s-920820f146"></a>`type`: `"object"`
- <a id="s-561897111c"></a>`additionalProperties`: `false`
- <a id="s-fe55ee677c"></a>`required`: `["branch_id","kind","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5c281a0633"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a6df0fc10f"></a>`kind` | yes | type="string"; enum=["collection","external-effect","coordination"] |  |
| <a id="s-196cadaf05"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8d60ad63ea"></a>definition `CoordinationCollectionResult`

- <a id="s-3b618455fc"></a>`type`: `"object"`
- <a id="s-48c612d7ae"></a>`additionalProperties`: `false`
- <a id="s-55529fe4b7"></a>`required`: `["producer_work_id","join_settlement_sha256","derivation_sha256","output_collection","output_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a5d394c7d"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ef4cf99093"></a>`join_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7f1eb4e474"></a>`output_collection` | yes | [CollectionRootRef](#s-b17390db18) |  |
| <a id="s-bee3314ce9"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-150a1801da) |  |
| <a id="s-14e62e7f1e"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3cd778c362"></a>definition `CoordinationSettlement`

- <a id="s-55f9aec69d"></a>`type`: `"object"`
- <a id="s-ced6681ea0"></a>`additionalProperties`: `false`
- <a id="s-f0e06f4847"></a>`required`: `["work","branch_set_sha256","children","contains_external_effects","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4d4dc36552"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1a1b3248d9"></a>`children` | yes | type="array"; items=([CoordinationChildSettlementRef](#s-37212f95c0)) |  |
| <a id="s-dfa20c87ce"></a>`collection_result` | no | anyOf=[([CoordinationCollectionResult](#s-8d60ad63ea)); (type="null")]; default=null |  |
| <a id="s-27f15036c0"></a>`contains_external_effects` | yes | type="boolean" |  |
| <a id="s-fd188445fa"></a>`final_join_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-1cb363d74e"></a>`format` | no | type="string"; const="stove0-coordination-settlement/v1"; default="stove0-coordination-settlement/v1" |  |
| <a id="s-6c701f1c17"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ca07f9b716"></a>`work` | yes | [WorkIdentity](#s-81716c1153) |  |

##### <a id="s-40282e1d49"></a>definition `EffectPlan`

- <a id="s-219845a237"></a>`type`: `"object"`
- <a id="s-f7e2954897"></a>`additionalProperties`: `false`
- <a id="s-b71da421ff"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc8cb6ff64"></a>`inputs` | yes | [TargetInputAuthority](#s-1a92c22544) |  |
| <a id="s-b0b802e07a"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-e93b0bdf56"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-7382100bb6"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ca1dd15921"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-626376c16e"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6867c370fe"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-2d573d35db"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f6976a246b"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-af89abf51e"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |

##### <a id="s-bbce34584e"></a>definition `EvaluationBinding`

- <a id="s-330458b977"></a>`type`: `"object"`
- <a id="s-455e50b291"></a>`additionalProperties`: `false`
- <a id="s-06f1c7f1f1"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-170a4af2c5"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d1e7f33807"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fe39af3016"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-aaed4912ca"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-43438037ba"></a>definition `ExecutionEnvelope`

- <a id="s-47f39680e5"></a>`type`: `"object"`
- <a id="s-7c1222ba60"></a>`additionalProperties`: `false`
- <a id="s-f13ce5e1c2"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2eb76ae911"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-01f469e10a"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6ca142301c"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-32de6eae7b"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-26afa6b3f3"></a>`target_plan` | yes | [TargetPlanBinding](#s-86dbed90c7) |  |
| <a id="s-48c513011d"></a>`workflow_plan` | yes | [WorkflowPlan](#s-53509a0132) |  |

##### <a id="s-a7307cbf35"></a>definition `ExternalEffectReceipt`

- <a id="s-a33c67b9fa"></a>`type`: `"object"`
- <a id="s-6dbbb6dc32"></a>`additionalProperties`: `false`
- <a id="s-8f65c64444"></a>`required`: `["job_id","request_sha256","target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5f60417f89"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8657fc2a2b"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1" |  |
| <a id="s-d1c314e036"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d33f3d2fec"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2ce2b001c2"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4425844ab3"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-25ea44ed91"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0a16db51e2"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-96dc539d8e"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6119604037"></a>definition `JoinDeclaration`

- <a id="s-b6543dfb4e"></a>`type`: `"object"`
- <a id="s-d45aa110be"></a>`additionalProperties`: `false`
- <a id="s-e4b64c41cc"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1d2a5f76a9"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-0be33adac5"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-e639d0316a"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-71efbe4396"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-c91b761cb4)); minItems=2 |  |
| <a id="s-f8c5c82747"></a>`recipe` | yes | [RecipeRef](#s-30ef719226) |  |
| <a id="s-47cb0a9aff"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-38187fd5bb) |  |

##### <a id="s-633723fedd"></a>definition `JoinInputPlan`

- <a id="s-661ba89728"></a>`type`: `"object"`
- <a id="s-fc15dad44f"></a>`additionalProperties`: `false`
- <a id="s-fcc372a498"></a>`required`: `["branch_id","settlement_sha256","derivation_sha256","output_collection","artifact_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a072a7f30d"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-150a1801da) |  |
| <a id="s-3cac7cc7ef"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2c92365269"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-395114ca37"></a>`output_collection` | yes | [CollectionRootRef](#s-b17390db18) |  |
| <a id="s-806f202b90"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-04fe909211"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c91b761cb4"></a>definition `JoinMemberDeclaration`

- <a id="s-39f7414531"></a>`type`: `"object"`
- <a id="s-a9088ca590"></a>`additionalProperties`: `false`
- <a id="s-a84d6b17e5"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdafd13f8b"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1a2da1b783"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-7131fac719"></a>definition `JoinPlan`

- <a id="s-f9690c08d8"></a>`type`: `"object"`
- <a id="s-2c06679224"></a>`additionalProperties`: `false`
- <a id="s-449a98c1f0"></a>`required`: `["parent_work_id","branch_set_sha256","declaration","inputs","work","workflow_plan","join_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ccc1fce432"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0e322a3084"></a>`declaration` | yes | [JoinDeclaration](#s-6119604037) |  |
| <a id="s-2b822b278b"></a>`format` | no | type="string"; const="stove0-join-plan/v1"; default="stove0-join-plan/v1" |  |
| <a id="s-3b26c32c3d"></a>`inputs` | yes | type="array"; items=([JoinInputPlan](#s-633723fedd)); minItems=2 |  |
| <a id="s-4c83debcf2"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-81a913589b"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-be7e6c49e1"></a>`work` | yes | [WorkIdentity](#s-81716c1153) |  |
| <a id="s-e738f37916"></a>`workflow_plan` | yes | [WorkflowPlan](#s-53509a0132) |  |

##### <a id="s-4c5de6ce14"></a>definition `JoinWorkBinding`

- <a id="s-69968ac427"></a>`type`: `"object"`
- <a id="s-73a7d5b7da"></a>`additionalProperties`: `false`
- <a id="s-a5387b6bb1"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8301d1cef1"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-49a6657cc6"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-ebb8f3de2c"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-5c808e6b0d)); minItems=2 |  |
| <a id="s-f20e992cbd"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5c808e6b0d"></a>definition `JoinWorkMemberBinding`

- <a id="s-333945f57c"></a>`type`: `"object"`
- <a id="s-01f868878d"></a>`additionalProperties`: `false`
- <a id="s-a93a91a742"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c413b70af9"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4bdb5e9e56"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a4ef02c738"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-369295a840"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b1b174c154"></a>definition `JsonSchemaDocument`

- <a id="s-66780cd29f"></a>`type`: `"object"`
- <a id="s-d13b2fb185"></a>`additionalProperties`: `false`
- <a id="s-1248800ccc"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b19e074ad3"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-7b5ec6ace1"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-4262b46f86"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7551507fe6"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-95c7150e11"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e69ffb8f42"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-974a0b49dd"></a>definition `ObservationEvidence`

- <a id="s-df3b9a9562"></a>`type`: `"object"`
- <a id="s-f1f945a3f1"></a>`additionalProperties`: `false`
- <a id="s-663193a719"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4aba43045a"></a>`request` | yes | [ObservationRequest](#s-344e016956) |  |
| <a id="s-e5d475f5bd"></a>`result` | yes | [ObservationResult](#s-653fedbaa6) |  |

##### <a id="s-2b4f70265d"></a>definition `ObservationFailure`

- <a id="s-cc8426ee55"></a>`type`: `"object"`
- <a id="s-438f5a5d63"></a>`additionalProperties`: `false`
- <a id="s-698fa83e3c"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f2990da9e2"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9e685fd3f9"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-8f5aafbaca"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-6ba44c630f"></a>definition `ObservationInapplicable`

- <a id="s-01f5930c59"></a>`type`: `"object"`
- <a id="s-f4981666d3"></a>`additionalProperties`: `false`
- <a id="s-85aed75145"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-408dade4f0"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-136c63e3ac"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-344e016956"></a>definition `ObservationRequest`

- <a id="s-7f4c5b5a85"></a>`type`: `"object"`
- <a id="s-096adbad61"></a>`additionalProperties`: `false`
- <a id="s-3c8f343ef5"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9a9abebf4b"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-7fc3bfecde"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-5fee287a95"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3e6bca3d96"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ef48bece48"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b0051a3ae6"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-4c1c666de1"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-d90085c20e"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0a61179625"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-ced814a343"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-de978a3ece)); minItems=1 |  |
| <a id="s-b5a2ac3286"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-0da60f4559"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-653fedbaa6"></a>definition `ObservationResult`

- <a id="s-10d6e51b61"></a>`type`: `"object"`
- <a id="s-51df5fb104"></a>`additionalProperties`: `false`
- <a id="s-5b6931a1c7"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-718c341de4"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-461de349b1"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42))); (type="null")]; default=null |  |
| <a id="s-a7dad57b0b"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-b1b174c154)); (type="null")]; default=null |  |
| <a id="s-e7337b0853"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-e4c7234ba8"></a>`failure` | no | anyOf=[([ObservationFailure](#s-2b4f70265d)); (type="null")]; default=null |  |
| <a id="s-356943688c"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-73ef200608"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-6ba44c630f)); (type="null")]; default=null |  |
| <a id="s-446e3e6e58"></a>`observer` | yes | [ObserverImplementation](#s-4ea959a8e4) |  |
| <a id="s-94c332d2b9"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f80577cd34"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ed1a71ad54"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-49dac9661d"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0d58b236d5"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-4dab494a38"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-de978a3ece)); minItems=1 |  |

##### <a id="s-4ea959a8e4"></a>definition `ObserverImplementation`

- <a id="s-ab481d2b86"></a>`type`: `"object"`
- <a id="s-59215d58af"></a>`additionalProperties`: `false`
- <a id="s-bf3a18e46b"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c9e30ac631"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-89ad78da1a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-97688092cf"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-c1e2305fff"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-0e9c4598d3"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-b94524bd3b"></a>definition `OperationRef`

- <a id="s-d7ec4667c3"></a>`type`: `"object"`
- <a id="s-bc98099865"></a>`additionalProperties`: `false`
- <a id="s-3fa5b1dc1b"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-55d434d7fa"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d1d9d352ee"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d272ee95e3"></a>definition `OutputArtifactRoleCount`

- <a id="s-402225c1ee"></a>`type`: `"object"`
- <a id="s-70631c673f"></a>`additionalProperties`: `false`
- <a id="s-701998879e"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a0eeb31c6"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-3fe464f238"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-e582b1dee5"></a>definition `OutputArtifactSetIdentity`

- <a id="s-d23d003105"></a>`type`: `"object"`
- <a id="s-9ff8839996"></a>`additionalProperties`: `false`
- <a id="s-1003bcc839"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-387cc6eb15"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-88bc18b63c"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-d272ee95e3)); minItems=1 |  |
| <a id="s-ea748cbbd0"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d235ba771e"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-b5248b631f"></a>definition `OutputCollectionRef`

- <a id="s-c23a6e46e8"></a>`type`: `"object"`
- <a id="s-a2f4b94100"></a>`additionalProperties`: `false`
- <a id="s-87feba3849"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cca2df8393"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b10b92058b"></a>`collection_id` | yes | [CollectionId](#s-1d96dcbbf7) |  |
| <a id="s-05cb824b99"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-76b27c811b"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-44ff8f56f5"></a>definition `PreviewAcceptanceView`

- <a id="s-0ea66cc6df"></a>`type`: `"object"`
- <a id="s-acdd8e5311"></a>`additionalProperties`: `false`
- <a id="s-ca1f20a669"></a>`required`: `["preview_sha256","branch_set_sha256","target_plans"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1842aca51d"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-60e83d588a"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-702796a3e0"></a>`target_plans` | yes | type="array"; items=([PreviewTargetExpectationView](#s-e8ea0d4175)) |  |

##### <a id="s-e8ea0d4175"></a>definition `PreviewTargetExpectationView`

- <a id="s-0eaad9992f"></a>`type`: `"object"`
- <a id="s-c3cebd42eb"></a>`additionalProperties`: `false`
- <a id="s-9791c201e9"></a>`required`: `["branch_id","work_id","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a4165eb201"></a>`branch_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-d9f4a0f4af"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-923e3ec7d8"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-30ef719226"></a>definition `RecipeRef`

- <a id="s-8d461694cf"></a>`type`: `"object"`
- <a id="s-86624adb90"></a>`additionalProperties`: `false`
- <a id="s-737a969c51"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0fec8d225c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5149220a21"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-5b7031ae4c"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-999ad9a563"></a>definition `TargetExecutionEvidence`

- <a id="s-9bc6abfaa8"></a>`type`: `"object"`
- <a id="s-dcc030001d"></a>`additionalProperties`: `false`
- <a id="s-18667bff80"></a>`required`: `["target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06830b8831"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-230a0cd0ec"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-66f1f1f160"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f5d7569c5d"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-536754fa68"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f978fe6e1d"></a>definition `TargetFailure`

- <a id="s-a2dd1f3ad8"></a>`type`: `"object"`
- <a id="s-6835547068"></a>`additionalProperties`: `false`
- <a id="s-8a3a51831e"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a834381bdb"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a919b4e2cf"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-a7ad7aed71"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-ebe3418092"></a>definition `TargetInapplicable`

- <a id="s-d7b220966f"></a>`type`: `"object"`
- <a id="s-4c7f24f278"></a>`additionalProperties`: `false`
- <a id="s-2e887f0123"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d5abf8269a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bbf749978d"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-1a92c22544"></a>definition `TargetInputAuthority`

- <a id="s-29b943f61a"></a>`type`: `"object"`
- <a id="s-10b0bd28a2"></a>`additionalProperties`: `false`
- <a id="s-e8088dc42a"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aed5af0fe9"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-59fa45567d)); minItems=1 |  |
| <a id="s-e2a33ddc94"></a>`selection` | yes | [ArtifactSelectionRef](#s-150a1801da) |  |

##### <a id="s-59fa45567d"></a>definition `TargetInputRoleCount`

- <a id="s-6f97a7b7db"></a>`type`: `"object"`
- <a id="s-2954a375cc"></a>`additionalProperties`: `false`
- <a id="s-15f65916d6"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dbc0de0f50"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-3a858dd664"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-2b32e80fe5"></a>definition `TargetJobDeclaration`

- <a id="s-2f2a912c43"></a>`type`: `"object"`
- <a id="s-fb19d943ae"></a>`additionalProperties`: `false`
- <a id="s-ddc48eb58f"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6764330893"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-eee1e7692c"></a>`controller_evidence` | yes | [ControllerEvidence](#s-44bea4f2a3) |  |
| <a id="s-9b40116757"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-16a9e7f98c"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a338f17ed6"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-aebe8702e3)); ([EffectPlan](#s-40282e1d49))] |  |
| <a id="s-b7d79cbb7c"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

##### <a id="s-5967cc9caf"></a>definition `TargetJobStatus`

- <a id="s-08f5b347c2"></a>`type`: `"object"`
- <a id="s-842128df33"></a>`additionalProperties`: `false`
- <a id="s-883cfe6190"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cac44d12d6"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-ab1e1b53de"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null |  |
| <a id="s-99bbf72f1a"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-a7307cbf35)); (type="null")]; default=null |  |
| <a id="s-3b8baea3f6"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-999ad9a563)); (type="null")]; default=null |  |
| <a id="s-d2943c56ea"></a>`failure` | no | anyOf=[([TargetFailure](#s-f978fe6e1d)); (type="null")]; default=null |  |
| <a id="s-d927e29dbb"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-ebe3418092)); (type="null")]; default=null |  |
| <a id="s-91553359d5"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bd536499f5"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-b5248b631f)); (type="null")]; default=null |  |
| <a id="s-103e7ec35d"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ff8f79b0eb"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-147c06beda)); (type="null")]; default=null |  |
| <a id="s-03146523d3"></a>`progress` | yes | [TargetProgress](#s-8600e54be5) |  |
| <a id="s-c803151656"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-c9a5865999"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-72be82fab4"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

###### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-16269c4748"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

##### <a id="s-bb7e7c05be"></a>definition `TargetOutputBindingSetIdentity`

- <a id="s-91dcb37b06"></a>`type`: `"object"`
- <a id="s-f524930317"></a>`additionalProperties`: `false`
- <a id="s-c983a57366"></a>`required`: `["artifact_count","total_bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f2709a8c6"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-3f2a32fecf"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5cf4fc0d40"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-86dbed90c7"></a>definition `TargetPlanBinding`

- <a id="s-d672156181"></a>`type`: `"object"`
- <a id="s-656e089c22"></a>`additionalProperties`: `false`
- <a id="s-7cf3500f2b"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-68edd64960"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-202a6d4408"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-5ac3b82060"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f7a6c3a481"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-68766d7bd4"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4473bbbc99"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-147c06beda"></a>definition `TargetProductionAuthority`

- <a id="s-55c44817a0"></a>`type`: `"object"`
- <a id="s-2e4b9f95d0"></a>`additionalProperties`: `false`
- <a id="s-2416ab8a4e"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b3f8e07098"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-6bc773d495"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fd8a4ae962"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-e8220c9a16"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c61b168f8f"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-e582b1dee5) |  |
| <a id="s-27a4c9c8cf"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bc856303ef"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f2518a08d3"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-c5c7727779) |  |
| <a id="s-f926180529"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-a29223560d"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8600e54be5"></a>definition `TargetProgress`

- <a id="s-98a2abcc40"></a>`type`: `"object"`
- <a id="s-c4e2cd1d64"></a>`additionalProperties`: `false`
- <a id="s-adf3dc5aed"></a>`required`: `["phase","completed"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-193095738d"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-c48692ddba"></a>`phase` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-cdf7284be8"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null |  |
| <a id="s-3989931737"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null |  |

##### <a id="s-03994a2ce6"></a>definition `TargetSettlementAuthority`

- <a id="s-87df072f1d"></a>`type`: `"object"`
- <a id="s-3b43e3ef1d"></a>`additionalProperties`: `false`
- <a id="s-cdb0a280be"></a>`required`: `["job_id","production_sha256","output_collection","output_bindings","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-216a4aeb8f"></a>`format` | no | type="string"; const="stove0-target-settlement/v1"; default="stove0-target-settlement/v1" |  |
| <a id="s-764b269815"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f287c79eb9"></a>`output_bindings` | yes | [TargetOutputBindingSetIdentity](#s-bb7e7c05be) |  |
| <a id="s-697533a146"></a>`output_collection` | yes | [OutputCollectionRef](#s-b5248b631f) |  |
| <a id="s-f33689eb8f"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d102b1b973"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-aebe8702e3"></a>definition `TransformPlan`

- <a id="s-b37af51319"></a>`type`: `"object"`
- <a id="s-fcb38326ac"></a>`additionalProperties`: `false`
- <a id="s-b63506d21c"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fee6958f35"></a>`inputs` | yes | [TargetInputAuthority](#s-1a92c22544) |  |
| <a id="s-c932d5fee5"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-deb4ea69fd"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-f21ec45ea1"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ab58eb323c"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-edbf64a1e3"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-44a44ee075"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-8e0fd35064"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8823b2b3d6"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9393826117"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |

##### <a id="s-637d0dd0ad"></a>definition `WorkClaimView`

- <a id="s-2fe3367647"></a>`type`: `"object"`
- <a id="s-bc09dab7d4"></a>`additionalProperties`: `false`
- <a id="s-61776308d1"></a>`required`: `["claim_id","fence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9a24a7bf91"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-18e3640425"></a>`fence` | yes | type="integer"; minimum=1 |  |

##### <a id="s-28e671ad54"></a>definition `WorkFailureView`

- <a id="s-c625804d40"></a>`type`: `"object"`
- <a id="s-6a2dd2b37e"></a>`additionalProperties`: `false`
- <a id="s-4feb3fe442"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88a7aa92de"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-0cbf30e0f9"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-dc3bc40230"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-81716c1153"></a>definition `WorkIdentity`

- <a id="s-38efbdf674"></a>`type`: `"object"`
- <a id="s-e7391f1f8f"></a>`additionalProperties`: `false`
- <a id="s-5d3383275d"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-baa1c6ef4b"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-4986eeacf1"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-bbce34584e)); (type="null")]; default=null |  |
| <a id="s-4fd1f45d64"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-8cbf22827d)); ([JoinWorkBinding](#s-4c5de6ce14))]); (type="null")]; default=null |  |
| <a id="s-3efe762718"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-ae142c547a"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-b17390db18)); minItems=1 |  |
| <a id="s-9aa8342902"></a>`recipe` | yes | [RecipeRef](#s-30ef719226) |  |
| <a id="s-7a0eb331d6"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-009ae10476"></a>definition `WorkInapplicableView`

- <a id="s-6cb304b9c3"></a>`type`: `"object"`
- <a id="s-11e68d36a5"></a>`additionalProperties`: `false`
- <a id="s-adc91aaff7"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-adef1e94ab"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-c5b35ce9f0"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-4aa66e2d9c"></a>definition `WorkView`

- <a id="s-800ad0c03b"></a>`type`: `"object"`
- <a id="s-b5964ed008"></a>`additionalProperties`: `false`
- <a id="s-918cbb140c"></a>`required`: `["work_id","work","phase","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59a399ea93"></a>`abandon_outcome` | no | anyOf=[(type="string"; enum=["inapplicable","failed","canceled"]); (type="null")]; default=null |  |
| <a id="s-adf38483f8"></a>`branch_set_plan` | no | anyOf=[([BranchSetPlan](#s-49b578e783)); (type="null")]; default=null |  |
| <a id="s-fdfcfdb5a3"></a>`claim` | no | anyOf=[([WorkClaimView](#s-637d0dd0ad)); (type="null")]; default=null |  |
| <a id="s-2b896be0bb"></a>`controller_evidence` | no | anyOf=[([ControllerEvidence](#s-44bea4f2a3)); (type="null")]; default=null |  |
| <a id="s-bccf00b236"></a>`coordination_cancel_requested` | no | type="boolean"; default=false |  |
| <a id="s-eb28c24f8a"></a>`coordination_settlement` | no | anyOf=[([CoordinationSettlement](#s-3cd778c362)); (type="null")]; default=null |  |
| <a id="s-7d0f4a5424"></a>`expected_target_plan_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-b7f2fab784"></a>`failure` | no | anyOf=[([WorkFailureView](#s-28e671ad54)); (type="null")]; default=null |  |
| <a id="s-623cd07fa4"></a>`format` | no | type="string"; const="stove0-work-view/v1"; default="stove0-work-view/v1" |  |
| <a id="s-6ae49c14dc"></a>`inapplicable` | no | anyOf=[([WorkInapplicableView](#s-009ae10476)); (type="null")]; default=null |  |
| <a id="s-f41bc3b93c"></a>`join_plan` | no | anyOf=[([JoinPlan](#s-7131fac719)); (type="null")]; default=null |  |
| <a id="s-7f980af1d8"></a>`observation_requests` | no | type="array"; default=[]; items=([ObservationRequest](#s-344e016956)) |  |
| <a id="s-45475212e2"></a>`observation_results` | no | type="array"; default=[]; items=([ObservationResult](#s-653fedbaa6)) |  |
| <a id="s-cd1f16e43c"></a>`output` | no | anyOf=[([OutputCollectionRef](#s-b5248b631f)); (type="null")]; default=null |  |
| <a id="s-1906fb3340"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-032d02f56f"></a>`preview_acceptance` | no | anyOf=[([PreviewAcceptanceView](#s-44ff8f56f5)); (type="null")]; default=null |  |
| <a id="s-4f95abce27"></a>`retirement_remaining` | no | type="array"; default=[]; items=(type="integer") |  |
| <a id="s-63e60f44a5"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-b2f012722b"></a>`target_plan` | no | anyOf=[(discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-aebe8702e3)); ([EffectPlan](#s-40282e1d49))]); (type="null")]; default=null |  |
| <a id="s-f2ea240adc"></a>`target_request` | no | anyOf=[([AcceptedTargetJob](#s-208b06065a)); (type="null")]; default=null |  |
| <a id="s-4ba382ef4a"></a>`target_settlement` | no | anyOf=[([TargetSettlementAuthority](#s-03994a2ce6)); (type="null")]; default=null |  |
| <a id="s-c77a7c9023"></a>`target_status` | no | anyOf=[([TargetJobStatus](#s-5967cc9caf)); (type="null")]; default=null |  |
| <a id="s-0d30b15ffc"></a>`work` | yes | [WorkIdentity](#s-81716c1153) |  |
| <a id="s-628f6b1ed9"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ccd5817c84"></a>`workflow_plan` | no | anyOf=[([WorkflowPlan](#s-53509a0132)); (type="null")]; default=null |  |

##### <a id="s-53509a0132"></a>definition `WorkflowPlan`

- <a id="s-195d99e581"></a>`type`: `"object"`
- <a id="s-7f233c9768"></a>`additionalProperties`: `false`
- <a id="s-61259067d4"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e9415a5484"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-76b49787b3"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-4b96392b74"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-974a0b49dd)) |  |
| <a id="s-8ada58129f"></a>`operation` | yes | [OperationRef](#s-b94524bd3b) |  |
| <a id="s-1dbdb782e8"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-f06b07ae83"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-485abec9f9"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-f04aabc495"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-9572a5cd42"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-70042d4982"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ea6222b836"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-86bed0d05c"></a>`work` | yes | [WorkIdentity](#s-81716c1153) |  |
| <a id="s-6b354a130d"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-38187fd5bb"></a>definition `WorkflowPlanIntent`

- <a id="s-ddf4997dc7"></a>`type`: `"object"`
- <a id="s-bc5f352465"></a>`additionalProperties`: `false`
- <a id="s-fcabfbcf61"></a>`required`: `["operation","target_registration_id","target_contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ffdab31989"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-1559d130af"></a>`operation` | yes | [OperationRef](#s-b94524bd3b) |  |
| <a id="s-fccfabf54f"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-716cb1593f"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e69ffb8f42)) |  |
| <a id="s-e210fd3955"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-5b65ab0390"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-63ccff9dc5"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-52d388a179"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-219fb1eaa8"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [from_page](stove0-operator-contracts-workpage-from-page.md)

## Governing policies

- <a id="pa-3c28a21596"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c0492a1b9346214b051fcf956d3b04331f218c99895764fe6c6ed7cdbd50afe -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AcceptedTargetJob": {
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
        "ArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootRef"
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
            "retirement_grace_seconds": {
              "default": 0,
              "minimum": 0,
              "type": "integer"
            },
            "retirement_policy": {
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
        "BrowsePageToken": {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
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
        "CoordinationChildSettlementRef": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "kind": {
              "enum": [
                "collection",
                "external-effect",
                "coordination"
              ],
              "type": "string"
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "kind",
            "settlement_sha256"
          ],
          "type": "object"
        },
        "CoordinationCollectionResult": {
          "additionalProperties": false,
          "properties": {
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "join_settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "output_collection": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "output_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "producer_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "producer_work_id",
            "join_settlement_sha256",
            "derivation_sha256",
            "output_collection",
            "output_selection"
          ],
          "type": "object"
        },
        "CoordinationSettlement": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "children": {
              "items": {
                "$ref": "#/$defs/CoordinationChildSettlementRef"
              },
              "type": "array"
            },
            "collection_result": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CoordinationCollectionResult"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "contains_external_effects": {
              "type": "boolean"
            },
            "final_join_settlement_sha256": {
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
            "format": {
              "const": "stove0-coordination-settlement/v1",
              "default": "stove0-coordination-settlement/v1",
              "type": "string"
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            }
          },
          "required": [
            "work",
            "branch_set_sha256",
            "children",
            "contains_external_effects",
            "settlement_sha256"
          ],
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
            "target_contract_sha256": {
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
            "target_contract_sha256",
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
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "$ref": "#/$defs/RecipeRef"
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
        "JoinInputPlan": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "output_collection": {
              "$ref": "#/$defs/CollectionRootRef"
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
            "derivation_sha256",
            "output_collection",
            "artifact_selection"
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
        "JoinPlan": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "declaration": {
              "$ref": "#/$defs/JoinDeclaration"
            },
            "format": {
              "const": "stove0-join-plan/v1",
              "default": "stove0-join-plan/v1",
              "type": "string"
            },
            "inputs": {
              "items": {
                "$ref": "#/$defs/JoinInputPlan"
              },
              "minItems": 2,
              "type": "array"
            },
            "join_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "parent_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            },
            "workflow_plan": {
              "$ref": "#/$defs/WorkflowPlan"
            }
          },
          "required": [
            "parent_work_id",
            "branch_set_sha256",
            "declaration",
            "inputs",
            "work",
            "workflow_plan",
            "join_plan_sha256"
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
        "JsonSchemaDocument": {
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
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "ObservationEvidence": {
          "additionalProperties": false,
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
          "type": "object"
        },
        "ObservationFailure": {
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
        "ObservationInapplicable": {
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
        "ObservationRequest": {
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
                "$ref": "#/$defs/ArtifactSubject"
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
        "ObservationResult": {
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
              "default": null
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
                "$ref": "#/$defs/ArtifactSubject"
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
        "OperationRef": {
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
        "PreviewAcceptanceView": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "preview_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_plans": {
              "items": {
                "$ref": "#/$defs/PreviewTargetExpectationView"
              },
              "type": "array"
            }
          },
          "required": [
            "preview_sha256",
            "branch_set_sha256",
            "target_plans"
          ],
          "type": "object"
        },
        "PreviewTargetExpectationView": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "work_id",
            "plan_sha256"
          ],
          "type": "object"
        },
        "RecipeRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "minimum": 1,
              "type": "integer"
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
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "target_contract_sha256",
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
            },
            "workspace_assurance": {
              "enum": [
                "encrypted",
                "ephemeral"
              ],
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
          "type": "object"
        },
        "TargetJobStatus": {
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
        "TargetOutputBindingSetIdentity": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
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
            "target_contract_sha256": {
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
            "target_contract_sha256",
            "operation_contract_sha256",
            "plan",
            "plan_sha256"
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
        },
        "TargetSettlementAuthority": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-target-settlement/v1",
              "default": "stove0-target-settlement/v1",
              "type": "string"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "output_bindings": {
              "$ref": "#/$defs/TargetOutputBindingSetIdentity"
            },
            "output_collection": {
              "$ref": "#/$defs/OutputCollectionRef"
            },
            "production_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "job_id",
            "production_sha256",
            "output_collection",
            "output_bindings",
            "settlement_sha256"
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
            "target_contract_sha256": {
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
            "target_contract_sha256",
            "plan_sha256"
          ],
          "type": "object"
        },
        "WorkClaimView": {
          "additionalProperties": false,
          "properties": {
            "claim_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "fence": {
              "minimum": 1,
              "type": "integer"
            }
          },
          "required": [
            "claim_id",
            "fence"
          ],
          "type": "object"
        },
        "WorkFailureView": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "maxLength": 160,
              "minLength": 1,
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
                "$ref": "#/$defs/CollectionRootRef"
              },
              "minItems": 1,
              "type": "array"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeRef"
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
        "WorkInapplicableView": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "maxLength": 160,
              "minLength": 1,
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
        "WorkView": {
          "additionalProperties": false,
          "properties": {
            "abandon_outcome": {
              "anyOf": [
                {
                  "enum": [
                    "inapplicable",
                    "failed",
                    "canceled"
                  ],
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
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
            "claim": {
              "anyOf": [
                {
                  "$ref": "#/$defs/WorkClaimView"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "controller_evidence": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ControllerEvidence"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "coordination_cancel_requested": {
              "default": false,
              "type": "boolean"
            },
            "coordination_settlement": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CoordinationSettlement"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "expected_target_plan_sha256": {
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
                  "$ref": "#/$defs/WorkFailureView"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "format": {
              "const": "stove0-work-view/v1",
              "default": "stove0-work-view/v1",
              "type": "string"
            },
            "inapplicable": {
              "anyOf": [
                {
                  "$ref": "#/$defs/WorkInapplicableView"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "join_plan": {
              "anyOf": [
                {
                  "$ref": "#/$defs/JoinPlan"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "observation_requests": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ObservationRequest"
              },
              "type": "array"
            },
            "observation_results": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ObservationResult"
              },
              "type": "array"
            },
            "output": {
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
            "phase": {
              "enum": [
                "eligible",
                "claimed",
                "observing",
                "planning",
                "target_preflight",
                "queued",
                "executing",
                "output_finalizing",
                "verifying",
                "settled",
                "retirement_pending",
                "coordinating",
                "abandon_pending",
                "complete",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "preview_acceptance": {
              "anyOf": [
                {
                  "$ref": "#/$defs/PreviewAcceptanceView"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "retirement_remaining": {
              "default": [],
              "items": {
                "type": "integer"
              },
              "type": "array"
            },
            "revision": {
              "minimum": 1,
              "type": "integer"
            },
            "target_plan": {
              "anyOf": [
                {
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
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "target_request": {
              "anyOf": [
                {
                  "$ref": "#/$defs/AcceptedTargetJob"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "target_settlement": {
              "anyOf": [
                {
                  "$ref": "#/$defs/TargetSettlementAuthority"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "target_status": {
              "anyOf": [
                {
                  "$ref": "#/$defs/TargetJobStatus"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "workflow_plan": {
              "anyOf": [
                {
                  "$ref": "#/$defs/WorkflowPlan"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "work_id",
            "work",
            "phase",
            "revision"
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
                "$ref": "#/$defs/ObservationEvidence"
              },
              "type": "array"
            },
            "operation": {
              "$ref": "#/$defs/OperationRef"
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
            "retirement_grace_seconds": {
              "default": 0,
              "minimum": 0,
              "type": "integer"
            },
            "retirement_policy": {
              "default": "retain",
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "type": "string"
            },
            "target_contract_sha256": {
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
            "target_contract_sha256",
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
              "$ref": "#/$defs/OperationRef"
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
            "retirement_grace_seconds": {
              "default": 0,
              "minimum": 0,
              "type": "integer"
            },
            "retirement_policy": {
              "default": "retain",
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "type": "string"
            },
            "target_contract_sha256": {
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
            "target_contract_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "filters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "next_page_token": {
          "anyOf": [
            {
              "$ref": "#/$defs/BrowsePageToken"
            },
            {
              "type": "null"
            }
          ]
        },
        "order": {
          "enum": [
            "asc",
            "desc"
          ],
          "type": "string"
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        },
        "sort": {
          "enum": [
            "updated_at",
            "phase",
            "work_id"
          ],
          "type": "string"
        },
        "work": {
          "items": {
            "$ref": "#/$defs/WorkView"
          },
          "type": "array"
        }
      },
      "required": [
        "page_size",
        "next_page_token",
        "sort",
        "order",
        "filters",
        "work"
      ],
      "type": "object"
    },
    "signature": "\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['updated_at', 'phase', 'work_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], work: tuple[stove0_operator_contracts.WorkView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkPage",
  "unit": "export"
}
```

</details>
