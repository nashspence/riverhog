# stove0_core.WorkRecord

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workrecord:6c77e989e0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf838cb901"></a>
- <a id="s-5904376c13"></a>`distribution`: `stove0-server`
- <a id="s-fce8b221a1"></a>`module`: `stove0_core`
- <a id="s-2910eb6032"></a>`name`: `WorkRecord`
- <a id="s-e048a799a3"></a>`unit`: `export`

### Declared structure

- <a id="s-1d335543f0"></a>`kind`: `"class"`
- <a id="s-c7a88eb901"></a>`signature`: `"\"(*, format: Literal['stove0-work-record/v1'] = 'stove0-work-record/v1', work: stove0_protocol.models.WorkIdentity, phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'] = 'eligible', revision: Annotated[int, Ge(ge=1)] = 1, claim: stove0_core.work_state.ClaimBinding \| None = None, preview_acceptance: stove0_core.work_state.PreviewAcceptance \| None = None, expected_target_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observation_requests: tuple[stove0_protocol.models.ObservationRequest, ...] = (), observation_results: tuple[stove0_protocol.models.ObservationResult, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan \| None = None, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement \| None = None, join_plan: stove0_protocol.fork_join.JoinPlan \| None = None, coordination_cancel_requested: bool = False, workflow_plan: stove0_protocol.models.WorkflowPlan \| None = None, target_plan: Optional[Annotated[stove0_target_protocol.protocol.TransformPlan \| stove0_target_protocol.protocol.EffectPlan, FieldInfo(annotation=NoneType, required=True, discriminator='protocol')]] = None, controller_evidence: stove0_protocol.models.ControllerEvidence \| None = None, target_request: stove0_target_protocol.protocol.AcceptedTargetJob \| None = None, target_status: stove0_target_protocol.protocol.TargetJobStatus \| None = None, output: stove0_target_protocol.protocol.OutputCollectionRef \| None = None, target_settlement: stove0_target_protocol.protocol.TargetSettlementAuthority \| None = None, retirement_remaining: tuple[int, ...] = (), failure: stove0_core.work_state.WorkFailure \| None = None, inapplicable: stove0_core.work_state.WorkInapplicable \| None = None, abandon_outcome: Optional[Literal['inapplicable', 'failed', 'canceled']] = None) -> None\""`

#### Validated model schema

<a id="s-d77634b583"></a>

- <a id="s-6f2c845e27"></a>`type`: `"object"`
- <a id="s-1e1c76e6dd"></a>`additionalProperties`: `false`
- <a id="s-7e24fdae8c"></a>`required`: `["work"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7be2e0e961"></a>`abandon_outcome` | no | anyOf=[(type="string"; enum=["inapplicable","failed","canceled"]); (type="null")]; default=null |  |
| <a id="s-6a30dff341"></a>`branch_set_plan` | no | anyOf=[([BranchSetPlan](#s-267b351e3e)); (type="null")]; default=null |  |
| <a id="s-67fdcde291"></a>`claim` | no | anyOf=[([ClaimBinding](#s-449a0108a5)); (type="null")]; default=null |  |
| <a id="s-7ce1cb8447"></a>`controller_evidence` | no | anyOf=[([ControllerEvidence](#s-792112d1dc)); (type="null")]; default=null |  |
| <a id="s-cf4f0a753a"></a>`coordination_cancel_requested` | no | type="boolean"; default=false |  |
| <a id="s-ff706747eb"></a>`coordination_settlement` | no | anyOf=[([CoordinationSettlement](#s-e5b78b9013)); (type="null")]; default=null |  |
| <a id="s-04afd03d1a"></a>`expected_target_plan_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-c8c0dd7d18"></a>`failure` | no | anyOf=[([WorkFailure](#s-83b371afec)); (type="null")]; default=null |  |
| <a id="s-3349a7c7ab"></a>`format` | no | type="string"; const="stove0-work-record/v1"; default="stove0-work-record/v1" |  |
| <a id="s-29dc0ad13e"></a>`inapplicable` | no | anyOf=[([WorkInapplicable](#s-db6729968b)); (type="null")]; default=null |  |
| <a id="s-4a102fd383"></a>`join_plan` | no | anyOf=[([JoinPlan](#s-5fcc715ea0)); (type="null")]; default=null |  |
| <a id="s-90932f158b"></a>`observation_requests` | no | type="array"; default=[]; items=([ObservationRequest](#s-ba8270c7f9)) |  |
| <a id="s-445652acfe"></a>`observation_results` | no | type="array"; default=[]; items=([ObservationResult](#s-9982b86647)) |  |
| <a id="s-8639f8c9cd"></a>`output` | no | anyOf=[([OutputCollectionRef](#s-9ec3e43bb5)); (type="null")]; default=null |  |
| <a id="s-cfa8645f93"></a>`phase` | no | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"]; default="eligible" |  |
| <a id="s-4377c4cd4d"></a>`preview_acceptance` | no | anyOf=[([PreviewAcceptance](#s-576b85d135)); (type="null")]; default=null |  |
| <a id="s-48aa2a7a0b"></a>`retirement_remaining` | no | type="array"; default=[]; items=(type="integer") |  |
| <a id="s-b3083f768f"></a>`revision` | no | type="integer"; minimum=1; default=1 |  |
| <a id="s-62d5d8a7ec"></a>`target_plan` | no | anyOf=[(discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-a231d93eaa)); ([EffectPlan](#s-4d12c94e21))]); (type="null")]; default=null |  |
| <a id="s-f54c929438"></a>`target_request` | no | anyOf=[([AcceptedTargetJob](#s-d21b80fda0)); (type="null")]; default=null |  |
| <a id="s-b0dd6a3b07"></a>`target_settlement` | no | anyOf=[([TargetSettlementAuthority](#s-ecc0928f0c)); (type="null")]; default=null |  |
| <a id="s-192fce5c22"></a>`target_status` | no | anyOf=[([TargetJobStatus](#s-5dd520fa29)); (type="null")]; default=null |  |
| <a id="s-c3a6c23ccd"></a>`work` | yes | [WorkIdentity](#s-0d1c76ace2) |  |
| <a id="s-a339f5e794"></a>`workflow_plan` | no | anyOf=[([WorkflowPlan](#s-c4c4682715)); (type="null")]; default=null |  |

##### Definitions

- [AcceptedTargetJob](#s-d21b80fda0)
- [ArtifactDispositionSetIdentity](#s-61c024bd2c)
- [ArtifactSelectionRef](#s-65aa7e1286)
- [ArtifactSubject](#s-ce72dd420c)
- [BranchPlan](#s-c67174ab76)
- [BranchSetPlan](#s-267b351e3e)
- [BranchWorkBinding](#s-c4ede85f6b)
- [ClaimBinding](#s-449a0108a5)
- [CollectionId](#s-786195a7a0)
- [CollectionRootRef](#s-a2325beae0)
- [ControllerEvidence](#s-792112d1dc)
- [CoordinationBranchPlan](#s-bcd3578233)
- [CoordinationChildSettlementRef](#s-99f5278470)
- [CoordinationCollectionResult](#s-6d987183ff)
- [CoordinationSettlement](#s-e5b78b9013)
- [EffectPlan](#s-4d12c94e21)
- [EvaluationBinding](#s-83ae919e3f)
- [ExecutionEnvelope](#s-6014c68881)
- [ExternalEffectReceipt](#s-657d3f6caa)
- [JoinDeclaration](#s-3e6ca93dfb)
- [JoinInputPlan](#s-a30048f57f)
- [JoinMemberDeclaration](#s-dcd109e50c)
- [JoinPlan](#s-5fcc715ea0)
- [JoinWorkBinding](#s-e406e2f73c)
- [JoinWorkMemberBinding](#s-62e41c1192)
- [JsonSchemaValidationProfile](#s-d285b708ae)
- [JsonValue](#s-c77b395b7f)
- [ObservationEvidence](#s-447a5c5c04)
- [ObservationFailure](#s-98486ba445)
- [ObservationInapplicable](#s-e53628e23d)
- [ObservationRequest](#s-ba8270c7f9)
- [ObservationResult](#s-9982b86647)
- [ObserverImplementation](#s-82d5a45306)
- [OperationRef](#s-363e2a2f20)
- [OutputArtifactRoleCount](#s-ddd8716850)
- [OutputArtifactSetIdentity](#s-894cf9eb24)
- [OutputCollectionRef](#s-9ec3e43bb5)
- [PreviewAcceptance](#s-576b85d135)
- [PreviewTargetExpectation](#s-7f5f648622)
- [RecipeRef](#s-5aef8239d4)
- [TargetExecutionEvidence](#s-2e0bca4b0b)
- [TargetFailure](#s-d364a912a3)
- [TargetInapplicable](#s-1e1a45f35d)
- [TargetInputAuthority](#s-261fcd04e6)
- [TargetInputRoleCount](#s-689a44c66b)
- [TargetJobDeclaration](#s-a2846d6598)
- [TargetJobStatus](#s-5dd520fa29)
- [TargetOutputBindingSetIdentity](#s-c2669656b4)
- [TargetPlanBinding](#s-4065e53771)
- [TargetProductionAuthority](#s-a38d869f0a)
- [TargetProgress](#s-42cd6e50b8)
- [TargetSettlementAuthority](#s-ecc0928f0c)
- [TransformPlan](#s-a231d93eaa)
- [WorkFailure](#s-83b371afec)
- [WorkIdentity](#s-0d1c76ace2)
- [WorkInapplicable](#s-db6729968b)
- [WorkflowPlan](#s-c4c4682715)
- [WorkflowPlanIntent](#s-0a412d9082)

##### <a id="s-d21b80fda0"></a>definition `AcceptedTargetJob`

- <a id="s-2ada92d142"></a>`type`: `"object"`
- <a id="s-1dc629c0d1"></a>`additionalProperties`: `false`
- <a id="s-e01ec54b02"></a>`required`: `["declaration","request_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0aa13c7940"></a>`declaration` | yes | [TargetJobDeclaration](#s-a2846d6598) |  |
| <a id="s-2925f0dc50"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-61c024bd2c"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-0d2cb9f8fa"></a>`type`: `"object"`
- <a id="s-2c6c8b6f46"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a75c8e8076"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-b64876d323"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-1f26f35c7d"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-6a655d4857"></a>`sha256` | yes | type="string" |  |

##### <a id="s-65aa7e1286"></a>definition `ArtifactSelectionRef`

- <a id="s-eccec3c883"></a>`type`: `"object"`
- <a id="s-b75150dc9b"></a>`additionalProperties`: `false`
- <a id="s-29ecfa49a9"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2bf8f00d55"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-872ad71742"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-419a1f1780"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-ce72dd420c"></a>definition `ArtifactSubject`

- <a id="s-4d4988e729"></a>`type`: `"object"`
- <a id="s-be802b1586"></a>`additionalProperties`: `false`
- <a id="s-d1e41608ef"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-049f1d47df"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-ebc5ed9de7"></a>`collection` | yes | [CollectionRootRef](#s-a2325beae0) |  |
| <a id="s-b2a887c88a"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-d8e9133a35"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-565bd2ebf4"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-746fbd5523"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-24a10d633e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c67174ab76"></a>definition `BranchPlan`

- <a id="s-038f2d914a"></a>`type`: `"object"`
- <a id="s-11e8088aaf"></a>`additionalProperties`: `false`
- <a id="s-968132c23b"></a>`required`: `["branch_id","artifact_selection","workflow_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f5adac586"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-65aa7e1286) |  |
| <a id="s-ee7b62c510"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-403eec5660"></a>`kind` | no | type="string"; const="leaf"; default="leaf" |  |
| <a id="s-705e0b60eb"></a>`workflow_plan` | yes | [WorkflowPlan](#s-c4c4682715) |  |

##### <a id="s-267b351e3e"></a>definition `BranchSetPlan`

- <a id="s-e03158fe98"></a>`type`: `"object"`
- <a id="s-bdffc102a6"></a>`additionalProperties`: `false`
- <a id="s-a8ca34fa87"></a>`required`: `["parent_work","decision_sha256","branches","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bc872f6a70"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-81478dfc96"></a>`branches` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/CoordinationBranchPlan","leaf":"#/$defs/BranchPlan"},"propertyName":"kind"}; oneOf=[([BranchPlan](#s-c67174ab76)); ([CoordinationBranchPlan](#s-bcd3578233))]); minItems=1 |  |
| <a id="s-6c9e5d178a"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2d19876120"></a>`evidence_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-189970a44f"></a>`format` | no | type="string"; const="stove0-branch-set/v1"; default="stove0-branch-set/v1" |  |
| <a id="s-00631a5d74"></a>`join` | no | anyOf=[([JoinDeclaration](#s-3e6ca93dfb)); (type="null")]; default=null |  |
| <a id="s-31dc095429"></a>`parent_work` | yes | [WorkIdentity](#s-0d1c76ace2) |  |
| <a id="s-309cc5972d"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-05142934f6"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### <a id="s-c4ede85f6b"></a>definition `BranchWorkBinding`

- <a id="s-ad0bb2aecc"></a>`type`: `"object"`
- <a id="s-2c7359e9c0"></a>`additionalProperties`: `false`
- <a id="s-945c82d794"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-468adb9c26"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e54b6539e7"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6c345ec8bc"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-943e7721e2"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-bad3e6ada9"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-449a0108a5"></a>definition `ClaimBinding`

- <a id="s-a917041edd"></a>`type`: `"object"`
- <a id="s-09d75875e6"></a>`additionalProperties`: `false`
- <a id="s-3e3cd92d59"></a>`required`: `["claim_id","fence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-13c0be93e6"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-05784dd796"></a>`fence` | yes | type="integer"; minimum=1 |  |

##### <a id="s-786195a7a0"></a>definition `CollectionId`

- <a id="s-a3f66314b0"></a>`type`: `"integer"`
- <a id="s-20d0f5cd18"></a>`minimum`: `1`

##### <a id="s-a2325beae0"></a>definition `CollectionRootRef`

- <a id="s-56dd1abfda"></a>`type`: `"object"`
- <a id="s-c41dfb2801"></a>`additionalProperties`: `false`
- <a id="s-18274674d2"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b8486419b"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-65cdf93afb"></a>`collection_id` | yes | [CollectionId](#s-786195a7a0) |  |
| <a id="s-916ec8d51c"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-792112d1dc"></a>definition `ControllerEvidence`

- <a id="s-51db11d73f"></a>`type`: `"object"`
- <a id="s-c1320ec522"></a>`additionalProperties`: `false`
- <a id="s-f3fc2c4cc4"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb8047cf12"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2ff986db07"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-6014c68881) |  |
| <a id="s-5624b0d154"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-bcd3578233"></a>definition `CoordinationBranchPlan`

- <a id="s-b12f33d191"></a>`type`: `"object"`
- <a id="s-252e1e299f"></a>`additionalProperties`: `false`
- <a id="s-61c9be76a1"></a>`required`: `["branch_id","artifact_selection","work","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-89b1fb7bf7"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-65aa7e1286) |  |
| <a id="s-23f728eec6"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7d8f1754d2"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-478d02a01a"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-30cde0a71b"></a>`work` | yes | [WorkIdentity](#s-0d1c76ace2) |  |

##### <a id="s-99f5278470"></a>definition `CoordinationChildSettlementRef`

- <a id="s-86c45e43ab"></a>`type`: `"object"`
- <a id="s-121df5b69a"></a>`additionalProperties`: `false`
- <a id="s-7560d40299"></a>`required`: `["branch_id","kind","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4733f662ea"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-52d6b979e3"></a>`kind` | yes | type="string"; enum=["collection","external-effect","coordination"] |  |
| <a id="s-0a6501855a"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6d987183ff"></a>definition `CoordinationCollectionResult`

- <a id="s-2e70e9f13c"></a>`type`: `"object"`
- <a id="s-234da31d0c"></a>`additionalProperties`: `false`
- <a id="s-b7963c9465"></a>`required`: `["producer_work_id","join_settlement_sha256","derivation_sha256","output_collection","output_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a7a8257860"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3665a64155"></a>`join_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0b542b192a"></a>`output_collection` | yes | [CollectionRootRef](#s-a2325beae0) |  |
| <a id="s-0e158e88b7"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-65aa7e1286) |  |
| <a id="s-f903d9bc09"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e5b78b9013"></a>definition `CoordinationSettlement`

- <a id="s-7a03306212"></a>`type`: `"object"`
- <a id="s-e2c9bc3001"></a>`additionalProperties`: `false`
- <a id="s-461628328c"></a>`required`: `["work","branch_set_sha256","children","contains_external_effects","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3df02a8e3f"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-519b465913"></a>`children` | yes | type="array"; items=([CoordinationChildSettlementRef](#s-99f5278470)) |  |
| <a id="s-c62000f858"></a>`collection_result` | no | anyOf=[([CoordinationCollectionResult](#s-6d987183ff)); (type="null")]; default=null |  |
| <a id="s-774f2d6fa5"></a>`contains_external_effects` | yes | type="boolean" |  |
| <a id="s-75561bf0c1"></a>`final_join_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-566e55b9f2"></a>`format` | no | type="string"; const="stove0-coordination-settlement/v1"; default="stove0-coordination-settlement/v1" |  |
| <a id="s-a6229ac29d"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b55a6afc51"></a>`work` | yes | [WorkIdentity](#s-0d1c76ace2) |  |

##### <a id="s-4d12c94e21"></a>definition `EffectPlan`

- <a id="s-91b94e3cbd"></a>`type`: `"object"`
- <a id="s-efa420e272"></a>`additionalProperties`: `false`
- <a id="s-8905da7323"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8a45663eba"></a>`inputs` | yes | [TargetInputAuthority](#s-261fcd04e6) |  |
| <a id="s-ba4e1fac1e"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-aa24e20ac7"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-9a6093c102"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3da3ab3a9e"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-842a421a00"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-acb7307d90"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-0db4c94306"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a02376d72c"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-98c90479e7"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |

##### <a id="s-83ae919e3f"></a>definition `EvaluationBinding`

- <a id="s-fb5947168c"></a>`type`: `"object"`
- <a id="s-d6a0c3e158"></a>`additionalProperties`: `false`
- <a id="s-155dc6ed0c"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9fa31d12d6"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b3641c53f"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c192e7bb9b"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-485ff0d35f"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-6014c68881"></a>definition `ExecutionEnvelope`

- <a id="s-a65eab8843"></a>`type`: `"object"`
- <a id="s-fb3b8c436b"></a>`additionalProperties`: `false`
- <a id="s-dbb4d0a904"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-53d83e95fb"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-60bf9866c0"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c2a85c5d21"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-50f536571b"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-73b37806d3"></a>`target_plan` | yes | [TargetPlanBinding](#s-4065e53771) |  |
| <a id="s-eebcb517aa"></a>`workflow_plan` | yes | [WorkflowPlan](#s-c4c4682715) |  |

##### <a id="s-657d3f6caa"></a>definition `ExternalEffectReceipt`

- <a id="s-a1d849935a"></a>`type`: `"object"`
- <a id="s-6638f01690"></a>`additionalProperties`: `false`
- <a id="s-29ef386386"></a>`required`: `["job_id","request_sha256","target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0dee708427"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0f0a4f71c8"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1" |  |
| <a id="s-4967df4bb1"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1849ad9916"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-be7a885643"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ad6ac95273"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a583eaa093"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-42fb133e35"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-36a30d4761"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3e6ca93dfb"></a>definition `JoinDeclaration`

- <a id="s-7f9a94f94b"></a>`type`: `"object"`
- <a id="s-3d5aa544c0"></a>`additionalProperties`: `false`
- <a id="s-f0b4e6b727"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce43a08203"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-6dee7352af"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-0b01355d8e"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8fdc2daff2"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-dcd109e50c)); minItems=2 |  |
| <a id="s-7e874efbc7"></a>`recipe` | yes | [RecipeRef](#s-5aef8239d4) |  |
| <a id="s-ed248f3251"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-0a412d9082) |  |

##### <a id="s-a30048f57f"></a>definition `JoinInputPlan`

- <a id="s-3c6c9740bd"></a>`type`: `"object"`
- <a id="s-49bf803b7a"></a>`additionalProperties`: `false`
- <a id="s-17eb3bf498"></a>`required`: `["branch_id","settlement_sha256","derivation_sha256","output_collection","artifact_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cbde268b64"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-65aa7e1286) |  |
| <a id="s-2356ee2b0e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-96635616d2"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b93e25d7cc"></a>`output_collection` | yes | [CollectionRootRef](#s-a2325beae0) |  |
| <a id="s-417cd54f78"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-b5d96cba29"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-dcd109e50c"></a>definition `JoinMemberDeclaration`

- <a id="s-af48dc7385"></a>`type`: `"object"`
- <a id="s-bc6c9e59e9"></a>`additionalProperties`: `false`
- <a id="s-22d347811f"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed147c8c6c"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f4a95f8433"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-5fcc715ea0"></a>definition `JoinPlan`

- <a id="s-1c4a525958"></a>`type`: `"object"`
- <a id="s-348603b2e9"></a>`additionalProperties`: `false`
- <a id="s-c8a2540200"></a>`required`: `["parent_work_id","branch_set_sha256","declaration","inputs","work","workflow_plan","join_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c8dae9c7c3"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-155b78cff0"></a>`declaration` | yes | [JoinDeclaration](#s-3e6ca93dfb) |  |
| <a id="s-c4594f2b1a"></a>`format` | no | type="string"; const="stove0-join-plan/v1"; default="stove0-join-plan/v1" |  |
| <a id="s-da7cd2c15d"></a>`inputs` | yes | type="array"; items=([JoinInputPlan](#s-a30048f57f)); minItems=2 |  |
| <a id="s-5364761e65"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-16a398c8b0"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d625247c14"></a>`work` | yes | [WorkIdentity](#s-0d1c76ace2) |  |
| <a id="s-d396575946"></a>`workflow_plan` | yes | [WorkflowPlan](#s-c4c4682715) |  |

##### <a id="s-e406e2f73c"></a>definition `JoinWorkBinding`

- <a id="s-696a462858"></a>`type`: `"object"`
- <a id="s-f4b3a8e95b"></a>`additionalProperties`: `false`
- <a id="s-f6fdf1b1a3"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8eec65bc81"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3afe96b236"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-ac3e7b165f"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-62e41c1192)); minItems=2 |  |
| <a id="s-4fe35adf28"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-62e41c1192"></a>definition `JoinWorkMemberBinding`

- <a id="s-124a710878"></a>`type`: `"object"`
- <a id="s-6c5b35c17c"></a>`additionalProperties`: `false`
- <a id="s-19d4209766"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-319db24465"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-102176f5e5"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7715fda9ca"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-98a155b385"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d285b708ae"></a>definition `JsonSchemaValidationProfile`

- <a id="s-5b16321272"></a>`type`: `"object"`
- <a id="s-0758d1c35a"></a>`additionalProperties`: `false`
- <a id="s-bcf2c4250d"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f1887a0fb5"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-20085774f6"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-c59139596f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3602ab8c0a"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b5e988b484"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |

##### <a id="s-c77b395b7f"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-447a5c5c04"></a>definition `ObservationEvidence`

- <a id="s-a74342047f"></a>`type`: `"object"`
- <a id="s-bb4a77e7c9"></a>`additionalProperties`: `false`
- <a id="s-a6891b2094"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-628ad62e60"></a>`request` | yes | [ObservationRequest](#s-ba8270c7f9) |  |
| <a id="s-a876974e8c"></a>`result` | yes | [ObservationResult](#s-9982b86647) |  |

##### <a id="s-98486ba445"></a>definition `ObservationFailure`

- <a id="s-e8e515c6f2"></a>`type`: `"object"`
- <a id="s-87a841e408"></a>`additionalProperties`: `false`
- <a id="s-059e52de1f"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-027d769d6c"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d0c5f31791"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-a39afe3e81"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-e53628e23d"></a>definition `ObservationInapplicable`

- <a id="s-9a11d16aa5"></a>`type`: `"object"`
- <a id="s-38666e15a9"></a>`additionalProperties`: `false`
- <a id="s-e8d192deb3"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1f30dfdc83"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3321699f1a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-ba8270c7f9"></a>definition `ObservationRequest`

- <a id="s-55f1aff22a"></a>`type`: `"object"`
- <a id="s-ab4e91518d"></a>`additionalProperties`: `false`
- <a id="s-03f2ff40d6"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3f985e101e"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-50393058c0"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-f36d33ccef"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bf58ace172"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-890248b5e9"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-82c4dc48ff"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-9846fca96a"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-f9eab45a86"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-18e2f54814"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-077f307657"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ce72dd420c)); minItems=1 |  |
| <a id="s-dc33224b49"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-a9d4b45711"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9982b86647"></a>definition `ObservationResult`

- <a id="s-f836f2b925"></a>`type`: `"object"`
- <a id="s-bf8d7309e5"></a>`additionalProperties`: `false`
- <a id="s-73638a4bb2"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-857ff8c860"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-98595bd1d8"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-c77b395b7f))); (type="null")]; default=null |  |
| <a id="s-1c7f4edf97"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-d285b708ae)); (type="null")]; default=null |  |
| <a id="s-c6c3ea3f59"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-1cc7ba071a"></a>`failure` | no | anyOf=[([ObservationFailure](#s-98486ba445)); (type="null")]; default=null |  |
| <a id="s-1c322d0156"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-f707f3db26"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-e53628e23d)); (type="null")]; default=null |  |
| <a id="s-8454437317"></a>`observer` | yes | [ObserverImplementation](#s-82d5a45306) |  |
| <a id="s-3af51d6210"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-55c6a6b718"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-77af31283e"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b52673e614"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b11329316"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-b576961cd2"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ce72dd420c)); minItems=1 |  |

##### <a id="s-82d5a45306"></a>definition `ObserverImplementation`

- <a id="s-525ad101ce"></a>`type`: `"object"`
- <a id="s-92718005d7"></a>`additionalProperties`: `false`
- <a id="s-7f5fcfe2be"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c8859b64b1"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dc471e560f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fb954e2027"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-13c145a258"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-e78be98b1a"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-363e2a2f20"></a>definition `OperationRef`

- <a id="s-df3adb288c"></a>`type`: `"object"`
- <a id="s-9c665441ce"></a>`additionalProperties`: `false`
- <a id="s-359defbf4d"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e86df1f92c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1c2a527d8d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ddd8716850"></a>definition `OutputArtifactRoleCount`

- <a id="s-9898de0e17"></a>`type`: `"object"`
- <a id="s-e2d6521180"></a>`additionalProperties`: `false`
- <a id="s-37357eec50"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a2a0f1e823"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-1dab162c93"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-894cf9eb24"></a>definition `OutputArtifactSetIdentity`

- <a id="s-b8afc558ba"></a>`type`: `"object"`
- <a id="s-379600e436"></a>`additionalProperties`: `false`
- <a id="s-de82925a55"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c0251f037f"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-b729fbe43b"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-ddd8716850)); minItems=1 |  |
| <a id="s-19870b8a13"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1733e59f36"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-9ec3e43bb5"></a>definition `OutputCollectionRef`

- <a id="s-92efb5d400"></a>`type`: `"object"`
- <a id="s-4239dc725a"></a>`additionalProperties`: `false`
- <a id="s-690db20182"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e36a424d5c"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-15fb9fb62a"></a>`collection_id` | yes | [CollectionId](#s-786195a7a0) |  |
| <a id="s-76453cdf6c"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1a4a35282e"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-576b85d135"></a>definition `PreviewAcceptance`

- <a id="s-60673ebf55"></a>`type`: `"object"`
- <a id="s-316951ee2d"></a>`additionalProperties`: `false`
- <a id="s-fe1e84bfc6"></a>`required`: `["preview_sha256","branch_set_sha256","target_plans"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-73a4549824"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6a00b48f06"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4da005001a"></a>`target_plans` | yes | type="array"; items=([PreviewTargetExpectation](#s-7f5f648622)) |  |

##### <a id="s-7f5f648622"></a>definition `PreviewTargetExpectation`

- <a id="s-7408fca500"></a>`type`: `"object"`
- <a id="s-38fc4bb94f"></a>`additionalProperties`: `false`
- <a id="s-43ac5f8e7a"></a>`required`: `["branch_id","work_id","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72b1db2309"></a>`branch_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-963618b09c"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cd983fc434"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5aef8239d4"></a>definition `RecipeRef`

- <a id="s-8cddcb6818"></a>`type`: `"object"`
- <a id="s-3b84a18692"></a>`additionalProperties`: `false`
- <a id="s-44064021fe"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-64cbfcbb60"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-125c21fba6"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-fda8973ab8"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2e0bca4b0b"></a>definition `TargetExecutionEvidence`

- <a id="s-cf322565c8"></a>`type`: `"object"`
- <a id="s-9d29e35fa0"></a>`additionalProperties`: `false`
- <a id="s-b7be575179"></a>`required`: `["target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-31874781c3"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0b1f3d1a27"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-50d9b24b48"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0b231df174"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-033b59ed46"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d364a912a3"></a>definition `TargetFailure`

- <a id="s-ba7703dcdf"></a>`type`: `"object"`
- <a id="s-51ecb2332c"></a>`additionalProperties`: `false`
- <a id="s-56b2e4e79c"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cd3d0c0c67"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-47843c9a93"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-808ddae618"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-1e1a45f35d"></a>definition `TargetInapplicable`

- <a id="s-eac8fd15a8"></a>`type`: `"object"`
- <a id="s-a073bc60c4"></a>`additionalProperties`: `false`
- <a id="s-f079aafa82"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8d5e1edbf0"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4f2b0837b7"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-261fcd04e6"></a>definition `TargetInputAuthority`

- <a id="s-e946e1bc80"></a>`type`: `"object"`
- <a id="s-b360a87cda"></a>`additionalProperties`: `false`
- <a id="s-1202d46e14"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-be8734167a"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-689a44c66b)); minItems=1 |  |
| <a id="s-5aae056f65"></a>`selection` | yes | [ArtifactSelectionRef](#s-65aa7e1286) |  |

##### <a id="s-689a44c66b"></a>definition `TargetInputRoleCount`

- <a id="s-d3944ac619"></a>`type`: `"object"`
- <a id="s-9408e86f5f"></a>`additionalProperties`: `false`
- <a id="s-9b90aa4e36"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b90d5fdd50"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-7ddd777d61"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-a2846d6598"></a>definition `TargetJobDeclaration`

- <a id="s-0cc6023011"></a>`type`: `"object"`
- <a id="s-82f71c2cc6"></a>`additionalProperties`: `false`
- <a id="s-d4c84fcf2b"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d7142c6c5e"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-21fabdaeaa"></a>`controller_evidence` | yes | [ControllerEvidence](#s-792112d1dc) |  |
| <a id="s-37ef7d6a3d"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-eb4f86414f"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f67ca1f5f2"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-a231d93eaa)); ([EffectPlan](#s-4d12c94e21))] |  |
| <a id="s-d1c262037c"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

##### <a id="s-5dd520fa29"></a>definition `TargetJobStatus`

- <a id="s-0e36a479fb"></a>`type`: `"object"`
- <a id="s-13436f4a0e"></a>`additionalProperties`: `false`
- <a id="s-2461ece6a4"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b10f3d4742"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-e20ee59486"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null |  |
| <a id="s-7eb12820f4"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-657d3f6caa)); (type="null")]; default=null |  |
| <a id="s-e2249b4fd7"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-2e0bca4b0b)); (type="null")]; default=null |  |
| <a id="s-dec417d137"></a>`failure` | no | anyOf=[([TargetFailure](#s-d364a912a3)); (type="null")]; default=null |  |
| <a id="s-73b74b8fc9"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-1e1a45f35d)); (type="null")]; default=null |  |
| <a id="s-fa3d5ba723"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8106202aea"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-9ec3e43bb5)); (type="null")]; default=null |  |
| <a id="s-577646a742"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e607496631"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-a38d869f0a)); (type="null")]; default=null |  |
| <a id="s-afd95eba80"></a>`progress` | yes | [TargetProgress](#s-42cd6e50b8) |  |
| <a id="s-b937da7180"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-64cec25dfc"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-776160b365"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

###### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-092ca8153b"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

##### <a id="s-c2669656b4"></a>definition `TargetOutputBindingSetIdentity`

- <a id="s-4095bfe8f1"></a>`type`: `"object"`
- <a id="s-e671c1ca26"></a>`additionalProperties`: `false`
- <a id="s-4fde8f0780"></a>`required`: `["artifact_count","total_bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e965b2232"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-db44c04b55"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-650160f77c"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-4065e53771"></a>definition `TargetPlanBinding`

- <a id="s-695776791a"></a>`type`: `"object"`
- <a id="s-65e21dfb36"></a>`additionalProperties`: `false`
- <a id="s-3fbda92fd3"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a19d7ee0ee"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9000a1d381"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-048c7a078d"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0d5ce2e8d3"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-598af4193c"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa1ba9fed0"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-a38d869f0a"></a>definition `TargetProductionAuthority`

- <a id="s-1c2fb84efa"></a>`type`: `"object"`
- <a id="s-cbb04acdc5"></a>`additionalProperties`: `false`
- <a id="s-df9897fc28"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-991d2c5f1a"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-c5285c8a52"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8b1ca7d8ef"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-8cb241a0bb"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3a0803914c"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-894cf9eb24) |  |
| <a id="s-fcc92c5669"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bf0bf0c3ad"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-52d93deb4a"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-61c024bd2c) |  |
| <a id="s-3882d5e341"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-5e21ee47ed"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-42cd6e50b8"></a>definition `TargetProgress`

- <a id="s-70f16fadd5"></a>`type`: `"object"`
- <a id="s-ede38f8848"></a>`additionalProperties`: `false`
- <a id="s-cf139caf9f"></a>`required`: `["phase","completed"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-15bb8fdb1a"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-d58da738d5"></a>`phase` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-9ab9861064"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null |  |
| <a id="s-50e8a3254b"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null |  |

##### <a id="s-ecc0928f0c"></a>definition `TargetSettlementAuthority`

- <a id="s-ad9eb73d6b"></a>`type`: `"object"`
- <a id="s-16322c3601"></a>`additionalProperties`: `false`
- <a id="s-3670a197ba"></a>`required`: `["job_id","production_sha256","output_collection","output_bindings","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-30ad3c1046"></a>`format` | no | type="string"; const="stove0-target-settlement/v1"; default="stove0-target-settlement/v1" |  |
| <a id="s-d220a3492b"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4f3276f520"></a>`output_bindings` | yes | [TargetOutputBindingSetIdentity](#s-c2669656b4) |  |
| <a id="s-15fc6958ca"></a>`output_collection` | yes | [OutputCollectionRef](#s-9ec3e43bb5) |  |
| <a id="s-6562170401"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ebf2d4a012"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a231d93eaa"></a>definition `TransformPlan`

- <a id="s-083e18770c"></a>`type`: `"object"`
- <a id="s-96c69d9e59"></a>`additionalProperties`: `false`
- <a id="s-1e0cc14910"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea9dfa29e7"></a>`inputs` | yes | [TargetInputAuthority](#s-261fcd04e6) |  |
| <a id="s-ec419401f0"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-898ada2c32"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-6d8609a7bd"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6b430c2559"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b130c7025a"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1e06d94127"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-08e90f5cb4"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-feb00dfcb2"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7dbf5de8f6"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |

##### <a id="s-83b371afec"></a>definition `WorkFailure`

- <a id="s-7cf0751ec5"></a>`type`: `"object"`
- <a id="s-ab1ea279ca"></a>`additionalProperties`: `false`
- <a id="s-7378215d89"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d14c5a741c"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-1d2024c721"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-3eefdcaa71"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-0d1c76ace2"></a>definition `WorkIdentity`

- <a id="s-bfa6d4d64d"></a>`type`: `"object"`
- <a id="s-422e4892b8"></a>`additionalProperties`: `false`
- <a id="s-ad869808ce"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0721f3e897"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-11345f42fe"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-83ae919e3f)); (type="null")]; default=null |  |
| <a id="s-50294b10c8"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-c4ede85f6b)); ([JoinWorkBinding](#s-e406e2f73c))]); (type="null")]; default=null |  |
| <a id="s-5366ce2a6c"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-96658276a3"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-a2325beae0)); minItems=1 |  |
| <a id="s-2e9bd567ca"></a>`recipe` | yes | [RecipeRef](#s-5aef8239d4) |  |
| <a id="s-9ff450ebc6"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-db6729968b"></a>definition `WorkInapplicable`

- <a id="s-89fb12c1f7"></a>`type`: `"object"`
- <a id="s-3c27a31300"></a>`additionalProperties`: `false`
- <a id="s-88e2285179"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9284ae8cc3"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-24b9af3533"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-c4c4682715"></a>definition `WorkflowPlan`

- <a id="s-dc228d295f"></a>`type`: `"object"`
- <a id="s-43bf5de202"></a>`additionalProperties`: `false`
- <a id="s-beffd58c8e"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e19d21bff"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-8a14231cfb"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-90a5e398b8"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-447a5c5c04)) |  |
| <a id="s-b55354eb3d"></a>`operation` | yes | [OperationRef](#s-363e2a2f20) |  |
| <a id="s-e05d8d0bdf"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-6426c60f1b"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-19ad7038fa"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-361ae161ca"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-9193f918e5"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-d991d624bf"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b60edd93b"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-d495f1b94b"></a>`work` | yes | [WorkIdentity](#s-0d1c76ace2) |  |
| <a id="s-6ac17ca065"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0a412d9082"></a>definition `WorkflowPlanIntent`

- <a id="s-1cca569f8e"></a>`type`: `"object"`
- <a id="s-c4ed9f6fdf"></a>`additionalProperties`: `false`
- <a id="s-1423f2ece7"></a>`required`: `["operation","target_registration_id","target_contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38779692d5"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-3c1fd47096"></a>`operation` | yes | [OperationRef](#s-363e2a2f20) |  |
| <a id="s-4e934b0d25"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-93bf4c41da"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-c77b395b7f)) |  |
| <a id="s-f3026e539a"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-b21b2bdd28"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-fed79d71fa"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-00c6e481d4"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-96fc0b81be"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [validate_shape](stove0-core-workrecord-validate-shape.md)
- [work_id](stove0-core-workrecord-work-id.md)

## Governing policies

- <a id="pa-92a9aa10e7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkRecord`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2116dd970fb24e5c57b8d5cd7354d2c7ff7da7ce17298dcd624d82b28946dc6 -->

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
        "ClaimBinding": {
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
        "PreviewAcceptance": {
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
                "$ref": "#/$defs/PreviewTargetExpectation"
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
        "PreviewTargetExpectation": {
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
        "WorkFailure": {
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
        "WorkInapplicable": {
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
              "$ref": "#/$defs/ClaimBinding"
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
              "$ref": "#/$defs/WorkFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "format": {
          "const": "stove0-work-record/v1",
          "default": "stove0-work-record/v1",
          "type": "string"
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/WorkInapplicable"
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
          "default": "eligible",
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
              "$ref": "#/$defs/PreviewAcceptance"
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
          "default": 1,
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
        "work"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-work-record/v1'] = 'stove0-work-record/v1', work: stove0_protocol.models.WorkIdentity, phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'] = 'eligible', revision: Annotated[int, Ge(ge=1)] = 1, claim: stove0_core.work_state.ClaimBinding | None = None, preview_acceptance: stove0_core.work_state.PreviewAcceptance | None = None, expected_target_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observation_requests: tuple[stove0_protocol.models.ObservationRequest, ...] = (), observation_results: tuple[stove0_protocol.models.ObservationResult, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None = None, join_plan: stove0_protocol.fork_join.JoinPlan | None = None, coordination_cancel_requested: bool = False, workflow_plan: stove0_protocol.models.WorkflowPlan | None = None, target_plan: Optional[Annotated[stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, FieldInfo(annotation=NoneType, required=True, discriminator='protocol')]] = None, controller_evidence: stove0_protocol.models.ControllerEvidence | None = None, target_request: stove0_target_protocol.protocol.AcceptedTargetJob | None = None, target_status: stove0_target_protocol.protocol.TargetJobStatus | None = None, output: stove0_target_protocol.protocol.OutputCollectionRef | None = None, target_settlement: stove0_target_protocol.protocol.TargetSettlementAuthority | None = None, retirement_remaining: tuple[int, ...] = (), failure: stove0_core.work_state.WorkFailure | None = None, inapplicable: stove0_core.work_state.WorkInapplicable | None = None, abandon_outcome: Optional[Literal['inapplicable', 'failed', 'canceled']] = None) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkRecord",
  "unit": "export"
}
```

</details>
