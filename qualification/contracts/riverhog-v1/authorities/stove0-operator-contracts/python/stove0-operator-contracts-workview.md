# stove0_operator_contracts.WorkView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workview:fa1e81cc66 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5543e41a2d"></a>
- <a id="s-0f9e34c87d"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-0d3f0d932c"></a>`module`: `stove0_operator_contracts`
- <a id="s-10c7e78d48"></a>`name`: `WorkView`
- <a id="s-c30d3c568c"></a>`unit`: `export`

### Declared structure

- <a id="s-8f472cb504"></a>`kind`: `"class"`
- <a id="s-50a2295057"></a>`signature`: `"\"(*, format: Literal['stove0-work-view/v1'] = 'stove0-work-view/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], work: stove0_protocol.models.WorkIdentity, phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'source_collection_retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], claim: stove0_operator_contracts.WorkClaimView \| None = None, preview_acceptance: stove0_operator_contracts.PreviewAcceptanceView \| None = None, expected_target_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observation_requests: tuple[stove0_protocol.models.ContentObservationRequest, ...] = (), observation_results: tuple[stove0_protocol.models.ContentObservationResult, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan \| None = None, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement \| None = None, join_plan: stove0_protocol.fork_join.JoinPlan \| None = None, coordination_cancel_requested: bool = False, workflow_plan: stove0_protocol.models.WorkflowPlan \| None = None, target_plan: Optional[Annotated[stove0_target_protocol.protocol.TransformPlan \| stove0_target_protocol.protocol.EffectPlan, FieldInfo(annotation=NoneType, required=True, discriminator='protocol')]] = None, controller_evidence: stove0_protocol.models.ControllerEvidence \| None = None, target_request: stove0_target_protocol.protocol.AcceptedTargetJob \| None = None, target_status: stove0_target_protocol.protocol.TargetJobStatus \| None = None, output: stove0_target_protocol.protocol.OutputCollectionRef \| None = None, target_settlement: stove0_target_protocol.protocol.TargetSettlementAuthority \| None = None, source_collection_retirement_remaining: tuple[int, ...] = (), failure: stove0_operator_contracts.WorkFailureView \| None = None, inapplicable: stove0_operator_contracts.WorkInapplicableView \| None = None, abandon_outcome: Optional[Literal['inapplicable', 'failed', 'canceled']] = None) -> None\""`

#### Validated model schema

<a id="s-a40389dd8c"></a>

- <a id="s-d9fcb94add"></a>`type`: `"object"`
- <a id="s-020ce278f6"></a>`additionalProperties`: `false`
- <a id="s-61be1eea86"></a>`required`: `["work_id","work","phase","revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab960e0d3d"></a>`abandon_outcome` | no | anyOf=[(type="string"; enum=["inapplicable","failed","canceled"]); (type="null")]; default=null |  |
| <a id="s-f958ec485a"></a>`branch_set_plan` | no | anyOf=[([BranchSetPlan](#s-dd5e33c681)); (type="null")]; default=null |  |
| <a id="s-bd573492a7"></a>`claim` | no | anyOf=[([WorkClaimView](#s-427778f2b9)); (type="null")]; default=null |  |
| <a id="s-b1e2a94d1f"></a>`controller_evidence` | no | anyOf=[([ControllerEvidence](#s-8b01483f06)); (type="null")]; default=null |  |
| <a id="s-38800f8969"></a>`coordination_cancel_requested` | no | type="boolean"; default=false |  |
| <a id="s-24e82d8b48"></a>`coordination_settlement` | no | anyOf=[([CoordinationSettlement](#s-1d102c88d4)); (type="null")]; default=null |  |
| <a id="s-0a25ef733a"></a>`expected_target_plan_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-755a0aa975"></a>`failure` | no | anyOf=[([WorkFailureView](#s-3d393d60f5)); (type="null")]; default=null |  |
| <a id="s-6e80be1bc8"></a>`format` | no | type="string"; const="stove0-work-view/v1"; default="stove0-work-view/v1" |  |
| <a id="s-1c1aa9aed3"></a>`inapplicable` | no | anyOf=[([WorkInapplicableView](#s-b96615054f)); (type="null")]; default=null |  |
| <a id="s-9ecde628f8"></a>`join_plan` | no | anyOf=[([JoinPlan](#s-580727b15b)); (type="null")]; default=null |  |
| <a id="s-2eac372f78"></a>`observation_requests` | no | type="array"; default=[]; items=([ContentObservationRequest](#s-29ce1a6015)) |  |
| <a id="s-d1adc32ca5"></a>`observation_results` | no | type="array"; default=[]; items=([ContentObservationResult](#s-227c883339)) |  |
| <a id="s-496eb2f34b"></a>`output` | no | anyOf=[([OutputCollectionRef](#s-875ef95b02)); (type="null")]; default=null |  |
| <a id="s-0f3b7878ab"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","source_collection_retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-c6e7d1e9da"></a>`preview_acceptance` | no | anyOf=[([PreviewAcceptanceView](#s-3f37573c13)); (type="null")]; default=null |  |
| <a id="s-31c116f346"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-673b8efa57"></a>`source_collection_retirement_remaining` | no | type="array"; default=[]; items=(type="integer") |  |
| <a id="s-dc1b03eaf2"></a>`target_plan` | no | anyOf=[(discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-e6d9103d68)); ([EffectPlan](#s-014776d0e1))]); (type="null")]; default=null |  |
| <a id="s-21ea49ddbd"></a>`target_request` | no | anyOf=[([AcceptedTargetJob](#s-c2ad8615d4)); (type="null")]; default=null |  |
| <a id="s-06b370748a"></a>`target_settlement` | no | anyOf=[([TargetSettlementAuthority](#s-0123d3e011)); (type="null")]; default=null |  |
| <a id="s-862849fdb3"></a>`target_status` | no | anyOf=[([TargetJobStatus](#s-a1c5d06fa9)); (type="null")]; default=null |  |
| <a id="s-4e27e5925b"></a>`work` | yes | [WorkIdentity](#s-9c239c9b47) |  |
| <a id="s-2f68bca083"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d3a1fc440"></a>`workflow_plan` | no | anyOf=[([WorkflowPlan](#s-eb01ec776f)); (type="null")]; default=null |  |

##### Definitions

- [AcceptedTargetJob](#s-c2ad8615d4)
- [ArtifactDispositionSetIdentity](#s-ce60455507)
- [ArtifactSelectionRef](#s-02ae186c7f)
- [BranchPlan](#s-1e5a1e87dc)
- [BranchSetPlan](#s-dd5e33c681)
- [BranchWorkBinding](#s-b2406b87c9)
- [CollectionId](#s-4dbecc0dae)
- [CollectionRootIdentityRef](#s-580d415889)
- [ContentObservationEvidence](#s-4f3cc3755e)
- [ContentObservationFailure](#s-43ac14522d)
- [ContentObservationInapplicable](#s-55c1595faf)
- [ContentObservationRequest](#s-29ce1a6015)
- [ContentObservationResult](#s-227c883339)
- [ControllerEvidence](#s-8b01483f06)
- [CoordinationBranchPlan](#s-1519e82c69)
- [CoordinationChildSettlementRef](#s-a2dcd47a50)
- [CoordinationCollectionResult](#s-d545cb9313)
- [CoordinationSettlement](#s-1d102c88d4)
- [DeclaredWorkspaceProtection](#s-2cfe70e1d2)
- [EffectPlan](#s-014776d0e1)
- [EvaluationBinding](#s-80d3264a22)
- [ExecutionEnvelope](#s-fce11dc92f)
- [ExternalEffectReceipt](#s-233c85f6dd)
- [JoinDeclaration](#s-c1baf21d38)
- [JoinInputPlan](#s-91107c71f9)
- [JoinMemberDeclaration](#s-ffbd632b88)
- [JoinPlan](#s-580727b15b)
- [JoinWorkBinding](#s-1382221b54)
- [JoinWorkMemberBinding](#s-4a00613a76)
- [JsonSchemaValidationProfile](#s-c226f5a8a0)
- [JsonValue](#s-51b717ae82)
- [NonnegativeDecimal](#s-d3736abf2d)
- [ObserverImplementation](#s-4f624728c8)
- [OperationIdentityRef](#s-a55916c7f8)
- [OutputArtifactRoleCount](#s-2794126534)
- [OutputArtifactSetIdentity](#s-27cd42b093)
- [OutputCollectionRef](#s-875ef95b02)
- [PreviewAcceptanceView](#s-3f37573c13)
- [PreviewTargetExpectationView](#s-436a7cc3a8)
- [RecipeIdentityRef](#s-8b84428d84)
- [TargetExecutionEvidence](#s-280bb22c73)
- [TargetFailure](#s-43f40e974c)
- [TargetInapplicable](#s-a6ab979dc2)
- [TargetInputAuthority](#s-afa6c620cf)
- [TargetInputRoleCount](#s-c94d2a1e38)
- [TargetJobDeclaration](#s-f46f29e737)
- [TargetJobStatus](#s-a1c5d06fa9)
- [TargetOutputBindingSetIdentity](#s-44ba4eaf6e)
- [TargetPlanBinding](#s-ca9c274c51)
- [TargetProductionAuthority](#s-0793231fae)
- [TargetProgress](#s-509bfc0c82)
- [TargetSettlementAuthority](#s-0123d3e011)
- [TransformPlan](#s-e6d9103d68)
- [WorkArtifactSubject](#s-c6274c9652)
- [WorkClaimView](#s-427778f2b9)
- [WorkFailureView](#s-3d393d60f5)
- [WorkIdentity](#s-9c239c9b47)
- [WorkInapplicableView](#s-b96615054f)
- [WorkflowPlan](#s-eb01ec776f)
- [WorkflowPlanIntent](#s-ef75a8f0d2)

##### <a id="s-c2ad8615d4"></a>definition `AcceptedTargetJob`

- <a id="s-ef4cfe5df5"></a>`type`: `"object"`
- <a id="s-15959b1872"></a>`additionalProperties`: `false`
- <a id="s-2d6e5be533"></a>`required`: `["declaration","request_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb3bdef16a"></a>`declaration` | yes | [TargetJobDeclaration](#s-f46f29e737) |  |
| <a id="s-5ade17cc37"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ce60455507"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-2a0269dfcd"></a>`type`: `"object"`
- <a id="s-874c0dfbef"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59b7f1d981"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-fd73125c81"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-e6b900dcb6"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-25541d3a25"></a>`sha256` | yes | type="string" |  |

##### <a id="s-02ae186c7f"></a>definition `ArtifactSelectionRef`

- <a id="s-9c4fb81452"></a>`type`: `"object"`
- <a id="s-ee596a1033"></a>`additionalProperties`: `false`
- <a id="s-96ad0a7a71"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-50bcddf04f"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-40db8c03a5"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b458aa1b4f"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-d3736abf2d); ge=0 |  |

##### <a id="s-1e5a1e87dc"></a>definition `BranchPlan`

- <a id="s-709a29af70"></a>`type`: `"object"`
- <a id="s-de2bf72fc1"></a>`additionalProperties`: `false`
- <a id="s-1610ec2598"></a>`required`: `["branch_id","artifact_selection","workflow_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d4833b36a3"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-02ae186c7f) |  |
| <a id="s-6694454137"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2c98ba3964"></a>`kind` | no | type="string"; const="leaf"; default="leaf" |  |
| <a id="s-5f360e3082"></a>`workflow_plan` | yes | [WorkflowPlan](#s-eb01ec776f) |  |

##### <a id="s-dd5e33c681"></a>definition `BranchSetPlan`

- <a id="s-c1eab3dfed"></a>`type`: `"object"`
- <a id="s-87075b84d9"></a>`additionalProperties`: `false`
- <a id="s-4777f38636"></a>`required`: `["parent_work","decision_sha256","branches","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b3abee8a30"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3fcbb52569"></a>`branches` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/CoordinationBranchPlan","leaf":"#/$defs/BranchPlan"},"propertyName":"kind"}; oneOf=[([BranchPlan](#s-1e5a1e87dc)); ([CoordinationBranchPlan](#s-1519e82c69))]); minItems=1 |  |
| <a id="s-8d0af96e65"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6e23d0088e"></a>`evidence_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-552d504f76"></a>`format` | no | type="string"; const="stove0-branch-set/v1"; default="stove0-branch-set/v1" |  |
| <a id="s-4309f16143"></a>`join` | no | anyOf=[([JoinDeclaration](#s-c1baf21d38)); (type="null")]; default=null |  |
| <a id="s-a49334563f"></a>`parent_work` | yes | [WorkIdentity](#s-9c239c9b47) |  |
| <a id="s-0b9b6c4de5"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-2215e24007"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### <a id="s-b2406b87c9"></a>definition `BranchWorkBinding`

- <a id="s-3c7e6d0ff2"></a>`type`: `"object"`
- <a id="s-ea332d56ac"></a>`additionalProperties`: `false`
- <a id="s-469851727b"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-703f4a44bc"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f2557f28e8"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-aeb79c8e5e"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5833058b87"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-667ae7bde3"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4dbecc0dae"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-45ebf9ab84"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-6d53baf943"></a>2 | not=(const="0") |

##### <a id="s-580d415889"></a>definition `CollectionRootIdentityRef`

- <a id="s-a4b287b41e"></a>`type`: `"object"`
- <a id="s-a6a475bf63"></a>`additionalProperties`: `false`
- <a id="s-029baf19f5"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88829fc2a3"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9e2213ccc9"></a>`collection_id` | yes | [CollectionId](#s-4dbecc0dae) |  |
| <a id="s-f5c01a499e"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4f3cc3755e"></a>definition `ContentObservationEvidence`

- <a id="s-32b8b9b66a"></a>`type`: `"object"`
- <a id="s-5e0f7f1caa"></a>`additionalProperties`: `false`
- <a id="s-271fbbe7ed"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-412e6a1691"></a>`request` | yes | [ContentObservationRequest](#s-29ce1a6015) |  |
| <a id="s-20392b1db2"></a>`result` | yes | [ContentObservationResult](#s-227c883339) |  |

##### <a id="s-43ac14522d"></a>definition `ContentObservationFailure`

- <a id="s-e6019a161f"></a>`type`: `"object"`
- <a id="s-25c233a1c3"></a>`additionalProperties`: `false`
- <a id="s-ebce813ff8"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-513f3ad186"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8494b6e4e7"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-1914137297"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-55c1595faf"></a>definition `ContentObservationInapplicable`

- <a id="s-4102d84c95"></a>`type`: `"object"`
- <a id="s-f6bf6e38da"></a>`additionalProperties`: `false`
- <a id="s-1e1c956ee0"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eb853005c4"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a4bd832870"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-29ce1a6015"></a>definition `ContentObservationRequest`

- <a id="s-832239dd5b"></a>`type`: `"object"`
- <a id="s-0eb302ba6b"></a>`additionalProperties`: `false`
- <a id="s-79aed94625"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d40b40a186"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-2dd2c64bc5"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-5a15c05751"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cf633b4759"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4de262641a"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3bdf930f83"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-5c7df1e342"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-740ddbc7f9"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0ddd97c76a"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-5e59a4c70c"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-c6274c9652)); minItems=1 |  |
| <a id="s-1e9ce13135"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-e918026a5e"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-227c883339"></a>definition `ContentObservationResult`

- <a id="s-0432d26a1a"></a>`type`: `"object"`
- <a id="s-a6b2fa40a5"></a>`additionalProperties`: `false`
- <a id="s-fca2cc5800"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-701f7f49a4"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-e1d59428db"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-51b717ae82))); (type="null")]; default=null |  |
| <a id="s-72f1b5fdf2"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-c226f5a8a0)); (type="null")]; default=null |  |
| <a id="s-e31fa84b72"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-22cacf446c"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-43ac14522d)); (type="null")]; default=null |  |
| <a id="s-807c1417d0"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-5a3beabb7b"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-55c1595faf)); (type="null")]; default=null |  |
| <a id="s-d716908cc0"></a>`observer` | yes | [ObserverImplementation](#s-4f624728c8) |  |
| <a id="s-c9c50e65f2"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5b2bd24e37"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8fda4c8b48"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-19db5de285"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ff29b536ce"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-e202aab34c"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-c6274c9652)); minItems=1 |  |

##### <a id="s-8b01483f06"></a>definition `ControllerEvidence`

- <a id="s-6205980d42"></a>`type`: `"object"`
- <a id="s-be65a85089"></a>`additionalProperties`: `false`
- <a id="s-52d3dd484d"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-01e55d984a"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-83c0bd5988"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-fce11dc92f) |  |
| <a id="s-4d5408a00a"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-1519e82c69"></a>definition `CoordinationBranchPlan`

- <a id="s-1a97ee31c3"></a>`type`: `"object"`
- <a id="s-8ace32451e"></a>`additionalProperties`: `false`
- <a id="s-eea7728219"></a>`required`: `["branch_id","artifact_selection","work","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6c8899a85"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-02ae186c7f) |  |
| <a id="s-23408be125"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8e79be1cc1"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0c2cf74251"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-dba8f489e1"></a>`work` | yes | [WorkIdentity](#s-9c239c9b47) |  |

##### <a id="s-a2dcd47a50"></a>definition `CoordinationChildSettlementRef`

- <a id="s-af396c959c"></a>`type`: `"object"`
- <a id="s-c96b0c1a70"></a>`additionalProperties`: `false`
- <a id="s-9194594ddf"></a>`required`: `["branch_id","kind","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0302ed771f"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-de9b53c3ef"></a>`kind` | yes | type="string"; enum=["collection","external-effect","coordination"] |  |
| <a id="s-15c64add61"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d545cb9313"></a>definition `CoordinationCollectionResult`

- <a id="s-e94303e960"></a>`type`: `"object"`
- <a id="s-2206f2597f"></a>`additionalProperties`: `false`
- <a id="s-c2df027eff"></a>`required`: `["producer_work_id","join_settlement_sha256","derivation_sha256","output_collection","output_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95d2fe61e7"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2397f1650b"></a>`join_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8bff6c4936"></a>`output_collection` | yes | [CollectionRootIdentityRef](#s-580d415889) |  |
| <a id="s-14a9e89862"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-02ae186c7f) |  |
| <a id="s-f1a4f679de"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1d102c88d4"></a>definition `CoordinationSettlement`

- <a id="s-38c92ee05f"></a>`type`: `"object"`
- <a id="s-92e79b52c5"></a>`additionalProperties`: `false`
- <a id="s-e7ff7494c9"></a>`required`: `["work","branch_set_sha256","children","contains_external_effects","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f0e0f22ae"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2799e7c723"></a>`children` | yes | type="array"; items=([CoordinationChildSettlementRef](#s-a2dcd47a50)) |  |
| <a id="s-f71f6e03a5"></a>`collection_result` | no | anyOf=[([CoordinationCollectionResult](#s-d545cb9313)); (type="null")]; default=null |  |
| <a id="s-8f7fc5f516"></a>`contains_external_effects` | yes | type="boolean" |  |
| <a id="s-3db394c721"></a>`final_join_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-e9968643db"></a>`format` | no | type="string"; const="stove0-coordination-settlement/v1"; default="stove0-coordination-settlement/v1" |  |
| <a id="s-2d7acce4e1"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2000c99227"></a>`work` | yes | [WorkIdentity](#s-9c239c9b47) |  |

##### <a id="s-2cfe70e1d2"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-9ef3bd5ecf"></a>`type`: `"string"`
- <a id="s-142fc254b6"></a>`enum`: `["encrypted-at-rest","memory-backed"]`

##### <a id="s-014776d0e1"></a>definition `EffectPlan`

- <a id="s-ee134b4799"></a>`type`: `"object"`
- <a id="s-718e1de2b1"></a>`additionalProperties`: `false`
- <a id="s-b02f5e431d"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-84831691e3"></a>`inputs` | yes | [TargetInputAuthority](#s-afa6c620cf) |  |
| <a id="s-63638e6d1c"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-103dbdb19c"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-135a6ffa34"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ac003b7096"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-08eb524e4d"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dd9d19659c"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-6e8e65f085"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a9b6fbe415"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3dee8c7567"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |

##### <a id="s-80d3264a22"></a>definition `EvaluationBinding`

- <a id="s-41db7ee56b"></a>`type`: `"object"`
- <a id="s-762c2e335f"></a>`additionalProperties`: `false`
- <a id="s-f6f33d0b16"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8b135942d6"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-17a271d308"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b94e5bce12"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-0e616f97e7"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-fce11dc92f"></a>definition `ExecutionEnvelope`

- <a id="s-eec2927e69"></a>`type`: `"object"`
- <a id="s-e8f7e736f5"></a>`additionalProperties`: `false`
- <a id="s-988bfdf061"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cdc20f3775"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-36f5d50da2"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-07e3c16502"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-62f1ebaa8b"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-b6e895aa37"></a>`target_plan` | yes | [TargetPlanBinding](#s-ca9c274c51) |  |
| <a id="s-b8bcc36af3"></a>`workflow_plan` | yes | [WorkflowPlan](#s-eb01ec776f) |  |

##### <a id="s-233c85f6dd"></a>definition `ExternalEffectReceipt`

- <a id="s-9b7cfafd94"></a>`type`: `"object"`
- <a id="s-76953ba4c8"></a>`additionalProperties`: `false`
- <a id="s-6ff396da9a"></a>`required`: `["job_id","request_sha256","target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea42f689fa"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-24528514be"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1" |  |
| <a id="s-2ae0176508"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0941d003a8"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c245a1dbbb"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f48f5ef200"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-206c187821"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dcfc6ecab9"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-9fa354ea48"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c1baf21d38"></a>definition `JoinDeclaration`

- <a id="s-3fc92998c8"></a>`type`: `"object"`
- <a id="s-2b99d53811"></a>`additionalProperties`: `false`
- <a id="s-c73eac568f"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e8f0fba70"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-b9693fc381"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-c384c4190c"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-663d3da993"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-ffbd632b88)); minItems=2 |  |
| <a id="s-f172045bbf"></a>`recipe` | yes | [RecipeIdentityRef](#s-8b84428d84) |  |
| <a id="s-3c958231fa"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-ef75a8f0d2) |  |

##### <a id="s-91107c71f9"></a>definition `JoinInputPlan`

- <a id="s-4612533890"></a>`type`: `"object"`
- <a id="s-b2e0df97e4"></a>`additionalProperties`: `false`
- <a id="s-433d3848d7"></a>`required`: `["branch_id","settlement_sha256","derivation_sha256","output_collection","artifact_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-373c8613a7"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-02ae186c7f) |  |
| <a id="s-c9a84ef776"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5598adb625"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-63f113903f"></a>`output_collection` | yes | [CollectionRootIdentityRef](#s-580d415889) |  |
| <a id="s-1d25973c64"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-f06b09199a"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ffbd632b88"></a>definition `JoinMemberDeclaration`

- <a id="s-aec3db680e"></a>`type`: `"object"`
- <a id="s-f60c362aea"></a>`additionalProperties`: `false`
- <a id="s-23f97d4a07"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-62ef04a0d5"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b77a0710ac"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-580727b15b"></a>definition `JoinPlan`

- <a id="s-986ab4333c"></a>`type`: `"object"`
- <a id="s-f0b1dd8914"></a>`additionalProperties`: `false`
- <a id="s-0920c6b252"></a>`required`: `["parent_work_id","branch_set_sha256","declaration","inputs","work","workflow_plan","join_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa3121563e"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b2517f05fd"></a>`declaration` | yes | [JoinDeclaration](#s-c1baf21d38) |  |
| <a id="s-3772659ed2"></a>`format` | no | type="string"; const="stove0-join-plan/v1"; default="stove0-join-plan/v1" |  |
| <a id="s-ae792d5980"></a>`inputs` | yes | type="array"; items=([JoinInputPlan](#s-91107c71f9)); minItems=2 |  |
| <a id="s-db46d97831"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c95e141ddb"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-967f7aee42"></a>`work` | yes | [WorkIdentity](#s-9c239c9b47) |  |
| <a id="s-615556d272"></a>`workflow_plan` | yes | [WorkflowPlan](#s-eb01ec776f) |  |

##### <a id="s-1382221b54"></a>definition `JoinWorkBinding`

- <a id="s-b10bfd0d82"></a>`type`: `"object"`
- <a id="s-439a7c43c3"></a>`additionalProperties`: `false`
- <a id="s-0174cf0916"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88f0589449"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-59a0e07194"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-4745f4fbb9"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-4a00613a76)); minItems=2 |  |
| <a id="s-6c0305119e"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4a00613a76"></a>definition `JoinWorkMemberBinding`

- <a id="s-4355001ffb"></a>`type`: `"object"`
- <a id="s-bd316409fb"></a>`additionalProperties`: `false`
- <a id="s-806362c319"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f20cde2028"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a3d17fe041"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d6555874d0"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-68f221a586"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c226f5a8a0"></a>definition `JsonSchemaValidationProfile`

- <a id="s-4e496f6f39"></a>`type`: `"object"`
- <a id="s-b1289e56d6"></a>`additionalProperties`: `false`
- <a id="s-def74d3210"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d40c58b6e0"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-a6147cdcff"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-b978536048"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d2fba6257c"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-df72cee0b5"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |

##### <a id="s-51b717ae82"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-d3736abf2d"></a>definition `NonnegativeDecimal`

- <a id="s-991c633c88"></a>`type`: `"string"`
- <a id="s-899ddd0512"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-4f624728c8"></a>definition `ObserverImplementation`

- <a id="s-ee3821d74d"></a>`type`: `"object"`
- <a id="s-d3a647c791"></a>`additionalProperties`: `false`
- <a id="s-3152478efd"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3e1b92cd31"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ac8e5f10e4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9b5a7ccf54"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-9f64b4bed1"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-6502874256"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-a55916c7f8"></a>definition `OperationIdentityRef`

- <a id="s-bc79cc39a3"></a>`type`: `"object"`
- <a id="s-0379fe3f06"></a>`additionalProperties`: `false`
- <a id="s-2e26124400"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0bf3d4a32f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6d36b9fd05"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2794126534"></a>definition `OutputArtifactRoleCount`

- <a id="s-445eacb405"></a>`type`: `"object"`
- <a id="s-e205722427"></a>`additionalProperties`: `false`
- <a id="s-24f4146bdc"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83f69c48c7"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-2748414093"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-27cd42b093"></a>definition `OutputArtifactSetIdentity`

- <a id="s-b192a8dea9"></a>`type`: `"object"`
- <a id="s-a0eefbd31c"></a>`additionalProperties`: `false`
- <a id="s-23dfd7e287"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7a253111a5"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-7593c18d8d"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-2794126534)); minItems=1 |  |
| <a id="s-60638eebe3"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2275c9ccf3"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-d3736abf2d); ge=0 |  |

##### <a id="s-875ef95b02"></a>definition `OutputCollectionRef`

- <a id="s-1802535693"></a>`type`: `"object"`
- <a id="s-86174a1273"></a>`additionalProperties`: `false`
- <a id="s-728393b32d"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d193249bbf"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-883af209f0"></a>`collection_id` | yes | [CollectionId](#s-4dbecc0dae) |  |
| <a id="s-9263290fd8"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-836af3fe58"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3f37573c13"></a>definition `PreviewAcceptanceView`

- <a id="s-9d5ba36c24"></a>`type`: `"object"`
- <a id="s-2fb6b762d3"></a>`additionalProperties`: `false`
- <a id="s-2156a3f8c0"></a>`required`: `["preview_sha256","branch_set_sha256","target_plans"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8a23298ce9"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9dbbd2182a"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-59563296f9"></a>`target_plans` | yes | type="array"; items=([PreviewTargetExpectationView](#s-436a7cc3a8)) |  |

##### <a id="s-436a7cc3a8"></a>definition `PreviewTargetExpectationView`

- <a id="s-bb3958ef67"></a>`type`: `"object"`
- <a id="s-4d7f90aacd"></a>`additionalProperties`: `false`
- <a id="s-052b3331b9"></a>`required`: `["branch_id","work_id","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3aaf397226"></a>`branch_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-db05eda3be"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1137bc0900"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8b84428d84"></a>definition `RecipeIdentityRef`

- <a id="s-4c3504c2eb"></a>`type`: `"object"`
- <a id="s-68c765857e"></a>`additionalProperties`: `false`
- <a id="s-dca101567b"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72ba7a6f45"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1d7f64d718"></a>`revision` | yes | [NonnegativeDecimal](#s-d3736abf2d); ge=1 |  |
| <a id="s-473ccd80b5"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-280bb22c73"></a>definition `TargetExecutionEvidence`

- <a id="s-f496886ab3"></a>`type`: `"object"`
- <a id="s-d2d2d6bc86"></a>`additionalProperties`: `false`
- <a id="s-3cb6432bfa"></a>`required`: `["target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-68d0f40b3e"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-04ec7256da"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4055f5dba0"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7f0e4212ba"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-1b691f8102"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-43f40e974c"></a>definition `TargetFailure`

- <a id="s-129eeee89f"></a>`type`: `"object"`
- <a id="s-0a2d178acb"></a>`additionalProperties`: `false`
- <a id="s-d58fbd3fd4"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-177e0e4d37"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bbc94551b4"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-0421ac6254"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-a6ab979dc2"></a>definition `TargetInapplicable`

- <a id="s-5b335de9db"></a>`type`: `"object"`
- <a id="s-dcba1d9f9c"></a>`additionalProperties`: `false`
- <a id="s-0ab3d7b5c1"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7557be594c"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7cda2f7081"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-afa6c620cf"></a>definition `TargetInputAuthority`

- <a id="s-a619f05a94"></a>`type`: `"object"`
- <a id="s-531db1cecd"></a>`additionalProperties`: `false`
- <a id="s-d5846cfa02"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb11aedaee"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-c94d2a1e38)); minItems=1 |  |
| <a id="s-7704a3a851"></a>`selection` | yes | [ArtifactSelectionRef](#s-02ae186c7f) |  |

##### <a id="s-c94d2a1e38"></a>definition `TargetInputRoleCount`

- <a id="s-e74eca20fe"></a>`type`: `"object"`
- <a id="s-9188d5fbf3"></a>`additionalProperties`: `false`
- <a id="s-a6ab4f114e"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-23ef5f4d0e"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-1b26ffedd0"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-f46f29e737"></a>definition `TargetJobDeclaration`

- <a id="s-63ce409e42"></a>`type`: `"object"`
- <a id="s-eb6193b66d"></a>`additionalProperties`: `false`
- <a id="s-6af507fca1"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","declared_workspace_protection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7113ff8e93"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-ebc598f038"></a>`controller_evidence` | yes | [ControllerEvidence](#s-8b01483f06) |  |
| <a id="s-92c34b1833"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-2cfe70e1d2) |  |
| <a id="s-ae29e9b9b5"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-e9141da4d3"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-431fa94f0b"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-e6d9103d68)); ([EffectPlan](#s-014776d0e1))] |  |

##### <a id="s-a1c5d06fa9"></a>definition `TargetJobStatus`

- <a id="s-2ab3d45b3e"></a>`type`: `"object"`
- <a id="s-eb20a6ff5c"></a>`additionalProperties`: `false`
- <a id="s-df3c7617f0"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8b4f6128e9"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-d396417626"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null |  |
| <a id="s-60d89d4aa2"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-233c85f6dd)); (type="null")]; default=null |  |
| <a id="s-1ea93e8c0c"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-280bb22c73)); (type="null")]; default=null |  |
| <a id="s-2dea9d3137"></a>`failure` | no | anyOf=[([TargetFailure](#s-43f40e974c)); (type="null")]; default=null |  |
| <a id="s-586946552e"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-a6ab979dc2)); (type="null")]; default=null |  |
| <a id="s-8b3ffd5479"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ae50c8248b"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-875ef95b02)); (type="null")]; default=null |  |
| <a id="s-1578835364"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c839cc27ca"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-0793231fae)); (type="null")]; default=null |  |
| <a id="s-9cd6e92b59"></a>`progress` | yes | [TargetProgress](#s-509bfc0c82) |  |
| <a id="s-17eb23c5ee"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-9ae6e4de6c"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b954866701"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

###### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-0d70aff632"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

##### <a id="s-44ba4eaf6e"></a>definition `TargetOutputBindingSetIdentity`

- <a id="s-2162af41be"></a>`type`: `"object"`
- <a id="s-f937f3e622"></a>`additionalProperties`: `false`
- <a id="s-64309be23f"></a>`required`: `["artifact_count","total_bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0fbad77715"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-e94470be05"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-00c6f35590"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-d3736abf2d); ge=0 |  |

##### <a id="s-ca9c274c51"></a>definition `TargetPlanBinding`

- <a id="s-2d1b92ec0d"></a>`type`: `"object"`
- <a id="s-9da10028f3"></a>`additionalProperties`: `false`
- <a id="s-aba1b09b87"></a>`required`: `["protocol","target_implementation_id","target_descriptor_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ddcc095ad5"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-44bebf4a91"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-d4bcada89e"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c65732c500"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e3f5c1c624"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eb4dce3869"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-0793231fae"></a>definition `TargetProductionAuthority`

- <a id="s-319653a745"></a>`type`: `"object"`
- <a id="s-464c29b519"></a>`additionalProperties`: `false`
- <a id="s-05674a22bc"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c78fd1c64"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-4bc2782bf1"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d3aad54019"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-ae116bd3ad"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f34224e953"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-27cd42b093) |  |
| <a id="s-99483927e4"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b344169587"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-005ad94cb9"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-ce60455507) |  |
| <a id="s-3f3fbb2851"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-d08b7512a9"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-509bfc0c82"></a>definition `TargetProgress`

- <a id="s-252d04a937"></a>`type`: `"object"`
- <a id="s-6d781dcf05"></a>`additionalProperties`: `false`
- <a id="s-b3929bc205"></a>`required`: `["phase","completed"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7c450bcc99"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-eb3b7f0c57"></a>`phase` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-4f60514794"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null |  |
| <a id="s-d09b9ee956"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null |  |

##### <a id="s-0123d3e011"></a>definition `TargetSettlementAuthority`

- <a id="s-be2f882803"></a>`type`: `"object"`
- <a id="s-183fe2f37f"></a>`additionalProperties`: `false`
- <a id="s-10c13abc1c"></a>`required`: `["job_id","production_sha256","output_collection","output_bindings","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bc34a31008"></a>`format` | no | type="string"; const="stove0-target-settlement/v1"; default="stove0-target-settlement/v1" |  |
| <a id="s-d254190479"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7466836827"></a>`output_bindings` | yes | [TargetOutputBindingSetIdentity](#s-44ba4eaf6e) |  |
| <a id="s-5ef6d67468"></a>`output_collection` | yes | [OutputCollectionRef](#s-875ef95b02) |  |
| <a id="s-0f70378ca4"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3bc1159cf8"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e6d9103d68"></a>definition `TransformPlan`

- <a id="s-9f4e8799c9"></a>`type`: `"object"`
- <a id="s-7e08a8f85b"></a>`additionalProperties`: `false`
- <a id="s-ce72b5cf65"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d4a53ebe3c"></a>`inputs` | yes | [TargetInputAuthority](#s-afa6c620cf) |  |
| <a id="s-d20c22398e"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-ea5ac40576"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-089f85bb3e"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d412c08907"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c831800b57"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c04a9a75b9"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-7418aa55d0"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-47be0d30b3"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-383f2dd9f8"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |

##### <a id="s-c6274c9652"></a>definition `WorkArtifactSubject`

- <a id="s-b6e61b7580"></a>`type`: `"object"`
- <a id="s-eaa4ee7972"></a>`additionalProperties`: `false`
- <a id="s-7a5d404399"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eb6f246761"></a>`bytes` | yes | [NonnegativeDecimal](#s-d3736abf2d); ge=0 |  |
| <a id="s-f81ed55f32"></a>`collection` | yes | [CollectionRootIdentityRef](#s-580d415889) |  |
| <a id="s-561de937b6"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-56203a2277"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-216822dcef"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-31c887bcce"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ec03552c14"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-427778f2b9"></a>definition `WorkClaimView`

- <a id="s-a4af5499dd"></a>`type`: `"object"`
- <a id="s-6dca7365cc"></a>`additionalProperties`: `false`
- <a id="s-78a0d434df"></a>`required`: `["claim_id","fence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce6dc06b6b"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-becf367e67"></a>`fence` | yes | type="integer"; minimum=1 |  |

##### <a id="s-3d393d60f5"></a>definition `WorkFailureView`

- <a id="s-5253454c5b"></a>`type`: `"object"`
- <a id="s-84e1547204"></a>`additionalProperties`: `false`
- <a id="s-0e8e6da2d3"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f2743ee2a4"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-19bcc0d976"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-135d377198"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-9c239c9b47"></a>definition `WorkIdentity`

- <a id="s-17ba947b3a"></a>`type`: `"object"`
- <a id="s-0808a47cb5"></a>`additionalProperties`: `false`
- <a id="s-a03fb6b98a"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b3c8ff41cd"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-32c462d0f5"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-80d3264a22)); (type="null")]; default=null |  |
| <a id="s-071846b040"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-b2406b87c9)); ([JoinWorkBinding](#s-1382221b54))]); (type="null")]; default=null |  |
| <a id="s-048ddd21eb"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-7def94aa7c"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-580d415889)); minItems=1 |  |
| <a id="s-95d8658e71"></a>`recipe` | yes | [RecipeIdentityRef](#s-8b84428d84) |  |
| <a id="s-6fc62b1d5e"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b96615054f"></a>definition `WorkInapplicableView`

- <a id="s-1b9282677f"></a>`type`: `"object"`
- <a id="s-71509b28fb"></a>`additionalProperties`: `false`
- <a id="s-d7811996d5"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e6ab9a6322"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-522e1e15b8"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-eb01ec776f"></a>definition `WorkflowPlan`

- <a id="s-f4ab44d29e"></a>`type`: `"object"`
- <a id="s-7803659d75"></a>`additionalProperties`: `false`
- <a id="s-d646b2c9bd"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1a73a6edb9"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-a07730c7f5"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-f627f64c49"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-4f3cc3755e)) |  |
| <a id="s-fd4e5ef98c"></a>`operation` | yes | [OperationIdentityRef](#s-a55916c7f8) |  |
| <a id="s-4ba3d3fe78"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-debc92cb04"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-52de873acd"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-5e589a163f"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-00837eb8c0"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-79c8a572bb"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-560f70e10c"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-583039e459"></a>`work` | yes | [WorkIdentity](#s-9c239c9b47) |  |
| <a id="s-2524d7bb47"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ef75a8f0d2"></a>definition `WorkflowPlanIntent`

- <a id="s-1801760575"></a>`type`: `"object"`
- <a id="s-b23bd610a5"></a>`additionalProperties`: `false`
- <a id="s-a4571d1947"></a>`required`: `["operation","target_registration_id","target_descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-15814f14b9"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-9c9e49fe96"></a>`operation` | yes | [OperationIdentityRef](#s-a55916c7f8) |  |
| <a id="s-413d840dc7"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-87246782bf"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-51b717ae82)) |  |
| <a id="s-f5defad7d1"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-16178165c6"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-f8152689e7"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-e6ed84a351"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f7f1f9a166"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [exact_identity](stove0-operator-contracts-workview-exact-identity.md)
- [from_record](stove0-operator-contracts-workview-from-record.md)

## Governing policies

- <a id="pa-198c4952db"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eca56d55d940e29c90c23f18128fc19f26559e5ca6f1967b8222149e861bf139 -->

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
              "$ref": "#/$defs/CollectionRootIdentityRef"
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
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "$ref": "#/$defs/CollectionRootIdentityRef"
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
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "target_descriptor_sha256",
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
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
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
            "$ref": "#/$defs/ContentObservationRequest"
          },
          "type": "array"
        },
        "observation_results": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ContentObservationResult"
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
            "source_collection_retirement_pending",
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
        "revision": {
          "minimum": 1,
          "type": "integer"
        },
        "source_collection_retirement_remaining": {
          "default": [],
          "items": {
            "type": "integer"
          },
          "type": "array"
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
    "signature": "\"(*, format: Literal['stove0-work-view/v1'] = 'stove0-work-view/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], work: stove0_protocol.models.WorkIdentity, phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'source_collection_retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], claim: stove0_operator_contracts.WorkClaimView | None = None, preview_acceptance: stove0_operator_contracts.PreviewAcceptanceView | None = None, expected_target_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observation_requests: tuple[stove0_protocol.models.ContentObservationRequest, ...] = (), observation_results: tuple[stove0_protocol.models.ContentObservationResult, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None = None, join_plan: stove0_protocol.fork_join.JoinPlan | None = None, coordination_cancel_requested: bool = False, workflow_plan: stove0_protocol.models.WorkflowPlan | None = None, target_plan: Optional[Annotated[stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, FieldInfo(annotation=NoneType, required=True, discriminator='protocol')]] = None, controller_evidence: stove0_protocol.models.ControllerEvidence | None = None, target_request: stove0_target_protocol.protocol.AcceptedTargetJob | None = None, target_status: stove0_target_protocol.protocol.TargetJobStatus | None = None, output: stove0_target_protocol.protocol.OutputCollectionRef | None = None, target_settlement: stove0_target_protocol.protocol.TargetSettlementAuthority | None = None, source_collection_retirement_remaining: tuple[int, ...] = (), failure: stove0_operator_contracts.WorkFailureView | None = None, inapplicable: stove0_operator_contracts.WorkInapplicableView | None = None, abandon_outcome: Optional[Literal['inapplicable', 'failed', 'canceled']] = None) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkView",
  "unit": "export"
}
```

</details>
