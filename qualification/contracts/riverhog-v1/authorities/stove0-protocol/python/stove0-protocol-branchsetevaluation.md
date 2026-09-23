# stove0_protocol.BranchSetEvaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetevaluation:9176523a64 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17d7bf5ef7"></a>
- <a id="s-2a9345ab29"></a>`distribution`: `stove0-protocol`
- <a id="s-027d20e8f4"></a>`module`: `stove0_protocol`
- <a id="s-5abec8d0f8"></a>`name`: `BranchSetEvaluation`
- <a id="s-cb703fb53f"></a>`unit`: `export`

### Declared structure

- <a id="s-bdb0eee52a"></a>`kind`: `"class"`
- <a id="s-00e0b1edda"></a>`signature`: `"\"(*, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], succeeded_branches: tuple[stove0_protocol.fork_join.BranchSettlement, ...], succeeded_effects: tuple[stove0_protocol.fork_join.BranchEffectSettlement, ...], succeeded_coordinations: tuple[stove0_protocol.fork_join.CoordinationSettlement, ...], unsettled_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], failed_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], inapplicable_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], interrupted_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], canceled_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], join_ready: bool, resolved_join_plan: stove0_protocol.fork_join.JoinPlan \| None, join_state: Literal['not-declared', 'waiting', 'ready', 'succeeded', 'failed', 'inapplicable', 'interrupted', 'canceled'], join_settlement: stove0_protocol.fork_join.JoinSettlement \| None, unsettled_work_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...], branch_set_succeeded: bool, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement \| None, retirement_requested: bool, coordination_complete_for_retirement: bool) -> None\""`

#### Validated model schema

<a id="s-6bd4ef694c"></a>

- <a id="s-df50c1af49"></a>`type`: `"object"`
- <a id="s-d0eb67a26b"></a>`additionalProperties`: `false`
- <a id="s-e582dedf48"></a>`required`: `["branch_set_sha256","succeeded_branches","succeeded_effects","succeeded_coordinations","unsettled_branch_ids","failed_branch_ids","inapplicable_branch_ids","interrupted_branch_ids","canceled_branch_ids","join_ready","resolved_join_plan","join_state","join_settlement","unsettled_work_ids","branch_set_succeeded","coordination_settlement","retirement_requested","coordination_complete_for_retirement"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3ea261abe1"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f676a82373"></a>`branch_set_succeeded` | yes | type="boolean" |  |
| <a id="s-72f69c56a7"></a>`canceled_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-5c44be2bfb"></a>`coordination_complete_for_retirement` | yes | type="boolean" |  |
| <a id="s-5753eb2db8"></a>`coordination_settlement` | yes | anyOf=[([CoordinationSettlement](#s-30838c8f33)); (type="null")] |  |
| <a id="s-cff9fc8545"></a>`failed_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-3cfa86af97"></a>`inapplicable_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-ef75abd979"></a>`interrupted_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-6d1761ec48"></a>`join_ready` | yes | type="boolean" |  |
| <a id="s-ebd8179689"></a>`join_settlement` | yes | anyOf=[([JoinSettlement](#s-957979c02a)); (type="null")] |  |
| <a id="s-d2f7df5ed4"></a>`join_state` | yes | type="string"; enum=["not-declared","waiting","ready","succeeded","failed","inapplicable","interrupted","canceled"] |  |
| <a id="s-84fbc90442"></a>`resolved_join_plan` | yes | anyOf=[([JoinPlan](#s-4e1ac6fed6)); (type="null")] |  |
| <a id="s-8d0e6bcc38"></a>`retirement_requested` | yes | type="boolean" |  |
| <a id="s-889f8fa59f"></a>`succeeded_branches` | yes | type="array"; items=([BranchSettlement](#s-d733b6629a)) |  |
| <a id="s-8f3cb2744c"></a>`succeeded_coordinations` | yes | type="array"; items=([CoordinationSettlement](#s-30838c8f33)) |  |
| <a id="s-b499ad396e"></a>`succeeded_effects` | yes | type="array"; items=([BranchEffectSettlement](#s-4a95cdec01)) |  |
| <a id="s-37531a3243"></a>`unsettled_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-ca93d556cc"></a>`unsettled_work_ids` | yes | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |

##### Definitions

- [ArtifactSelectionRef](#s-035224a7a0)
- [ArtifactSubject](#s-1ac801a307)
- [BranchEffectSettlement](#s-4a95cdec01)
- [BranchSettlement](#s-d733b6629a)
- [BranchWorkBinding](#s-183805a91c)
- [CollectionId](#s-bf37d1b48d)
- [CollectionRootRef](#s-6aec22395d)
- [CoordinationChildSettlementRef](#s-7e09803f5f)
- [CoordinationCollectionResult](#s-2dea16537d)
- [CoordinationSettlement](#s-30838c8f33)
- [EvaluationBinding](#s-968822e600)
- [JoinDeclaration](#s-5433b5d3c5)
- [JoinInputPlan](#s-49428c6f7d)
- [JoinMemberDeclaration](#s-87da931b4b)
- [JoinPlan](#s-4e1ac6fed6)
- [JoinSettlement](#s-957979c02a)
- [JoinWorkBinding](#s-d7b0ac8dd7)
- [JoinWorkMemberBinding](#s-c7f82d34ca)
- [JsonSchemaValidationProfile](#s-6de184bd07)
- [JsonValue](#s-66063e338d)
- [ObservationEvidence](#s-79dfbcc12f)
- [ObservationFailure](#s-934204c078)
- [ObservationInapplicable](#s-36d81105fe)
- [ObservationRequest](#s-4385eb9ee8)
- [ObservationResult](#s-85dc2a4d3a)
- [ObserverImplementation](#s-cbea6e2fa4)
- [OperationRef](#s-d76d5602aa)
- [RecipeRef](#s-5b91ffc491)
- [WorkIdentity](#s-d107b6bb2a)
- [WorkflowPlan](#s-272f50f9e6)
- [WorkflowPlanIntent](#s-c5cdaabb80)

##### <a id="s-035224a7a0"></a>definition `ArtifactSelectionRef`

- <a id="s-c1850526b0"></a>`type`: `"object"`
- <a id="s-6e9d2b1cba"></a>`additionalProperties`: `false`
- <a id="s-a3ec67c2cc"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-27d79ddc0b"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-c92dc92b41"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-054979c69d"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-1ac801a307"></a>definition `ArtifactSubject`

- <a id="s-87a1ddd10d"></a>`type`: `"object"`
- <a id="s-acfb2ec276"></a>`additionalProperties`: `false`
- <a id="s-b178516083"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-68d88975cf"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-08ce00472d"></a>`collection` | yes | [CollectionRootRef](#s-6aec22395d) |  |
| <a id="s-f307cf3945"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-f291304c9a"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-b77c7a5a7c"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-0331ada6e3"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c2e271d7f5"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4a95cdec01"></a>definition `BranchEffectSettlement`

- <a id="s-5a0a08b209"></a>`type`: `"object"`
- <a id="s-203703ace0"></a>`additionalProperties`: `false`
- <a id="s-348bd2b864"></a>`required`: `["branch_id","work_id","workflow_plan_sha256","effect_receipt_sha256","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e08eb4b75c"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-072f70c65a"></a>`effect_receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6f1f6978d1"></a>`format` | no | type="string"; const="stove0-branch-effect-settlement/v1"; default="stove0-branch-effect-settlement/v1" |  |
| <a id="s-e5d7405f3d"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b4d984bda5"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a96f9a4d7"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d733b6629a"></a>definition `BranchSettlement`

- <a id="s-be562882d0"></a>`type`: `"object"`
- <a id="s-aab7a1c856"></a>`additionalProperties`: `false`
- <a id="s-24690ab7cb"></a>`required`: `["branch_id","work_id","workflow_plan_sha256","derivation_sha256","producer_settlement_sha256","output_collection","output_selection","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f6d9d8848"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2447e67c6a"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-262db05bab"></a>`format` | no | type="string"; const="stove0-branch-settlement/v1"; default="stove0-branch-settlement/v1" |  |
| <a id="s-5bff6e5b03"></a>`output_collection` | yes | [CollectionRootRef](#s-6aec22395d) |  |
| <a id="s-e988db6c34"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-035224a7a0) |  |
| <a id="s-ada4209d18"></a>`producer_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4edd57f3b7"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a2cd4c8618"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f58d6b9c45"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-183805a91c"></a>definition `BranchWorkBinding`

- <a id="s-fc272d1303"></a>`type`: `"object"`
- <a id="s-c6a52cb92e"></a>`additionalProperties`: `false`
- <a id="s-e05355cc21"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a56014b4e5"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6996dd4e4d"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c6f8d8d507"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fe41fcda77"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-bed3755dbc"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bf37d1b48d"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-d463fff913"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-221e4e02b8"></a>2 | not=(const="0") |

##### <a id="s-6aec22395d"></a>definition `CollectionRootRef`

- <a id="s-73eae17ae2"></a>`type`: `"object"`
- <a id="s-7a041bf336"></a>`additionalProperties`: `false`
- <a id="s-158606cb24"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6a978fcb12"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-68901a6bae"></a>`collection_id` | yes | [CollectionId](#s-bf37d1b48d) |  |
| <a id="s-107521f1cb"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-7e09803f5f"></a>definition `CoordinationChildSettlementRef`

- <a id="s-8261b152c0"></a>`type`: `"object"`
- <a id="s-fbe502b337"></a>`additionalProperties`: `false`
- <a id="s-4c35a3a684"></a>`required`: `["branch_id","kind","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f467d06999"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b6ffc9d937"></a>`kind` | yes | type="string"; enum=["collection","external-effect","coordination"] |  |
| <a id="s-dab0f0fd0d"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2dea16537d"></a>definition `CoordinationCollectionResult`

- <a id="s-e1425270fa"></a>`type`: `"object"`
- <a id="s-37b797c28e"></a>`additionalProperties`: `false`
- <a id="s-c99456b5c7"></a>`required`: `["producer_work_id","join_settlement_sha256","derivation_sha256","output_collection","output_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-abb79bf29e"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3596bcfdb2"></a>`join_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d8f7add1ee"></a>`output_collection` | yes | [CollectionRootRef](#s-6aec22395d) |  |
| <a id="s-c9c81ac6c6"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-035224a7a0) |  |
| <a id="s-78b3548142"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-30838c8f33"></a>definition `CoordinationSettlement`

- <a id="s-63f9bd43c8"></a>`type`: `"object"`
- <a id="s-a73b285d01"></a>`additionalProperties`: `false`
- <a id="s-fb1cd4fbf9"></a>`required`: `["work","branch_set_sha256","children","contains_external_effects","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ea9d20f51"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1939923a0c"></a>`children` | yes | type="array"; items=([CoordinationChildSettlementRef](#s-7e09803f5f)) |  |
| <a id="s-5bee9d0cfe"></a>`collection_result` | no | anyOf=[([CoordinationCollectionResult](#s-2dea16537d)); (type="null")]; default=null |  |
| <a id="s-cacbe76919"></a>`contains_external_effects` | yes | type="boolean" |  |
| <a id="s-cf71b4ce35"></a>`final_join_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-3a83e8da6e"></a>`format` | no | type="string"; const="stove0-coordination-settlement/v1"; default="stove0-coordination-settlement/v1" |  |
| <a id="s-1afbb6b657"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f74da0563f"></a>`work` | yes | [WorkIdentity](#s-d107b6bb2a) |  |

##### <a id="s-968822e600"></a>definition `EvaluationBinding`

- <a id="s-9cbb6cde36"></a>`type`: `"object"`
- <a id="s-793a13745b"></a>`additionalProperties`: `false`
- <a id="s-212977dd87"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c0ac61247"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f655ca59d5"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-69538707b9"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-1dcf2be909"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-5433b5d3c5"></a>definition `JoinDeclaration`

- <a id="s-05b447885d"></a>`type`: `"object"`
- <a id="s-c796f837f7"></a>`additionalProperties`: `false`
- <a id="s-681e9f14af"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac9f3e376c"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-f0c3f78849"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-645c82249b"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4c8efb7747"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-87da931b4b)); minItems=2 |  |
| <a id="s-443fc9d3bd"></a>`recipe` | yes | [RecipeRef](#s-5b91ffc491) |  |
| <a id="s-34c74a8f55"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-c5cdaabb80) |  |

##### <a id="s-49428c6f7d"></a>definition `JoinInputPlan`

- <a id="s-02371746ea"></a>`type`: `"object"`
- <a id="s-53b4833d9f"></a>`additionalProperties`: `false`
- <a id="s-3128a7e466"></a>`required`: `["branch_id","settlement_sha256","derivation_sha256","output_collection","artifact_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-562417a72c"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-035224a7a0) |  |
| <a id="s-bd500c242e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-66a9a1e8d7"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8a55dbbbce"></a>`output_collection` | yes | [CollectionRootRef](#s-6aec22395d) |  |
| <a id="s-3b1a4cadf9"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-f1b034f4f6"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-87da931b4b"></a>definition `JoinMemberDeclaration`

- <a id="s-4d22075673"></a>`type`: `"object"`
- <a id="s-cdbea05d62"></a>`additionalProperties`: `false`
- <a id="s-270edfb152"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ca60d3e16"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d3d5571da9"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-4e1ac6fed6"></a>definition `JoinPlan`

- <a id="s-05a640dd9e"></a>`type`: `"object"`
- <a id="s-dc963ee054"></a>`additionalProperties`: `false`
- <a id="s-131ea4cfea"></a>`required`: `["parent_work_id","branch_set_sha256","declaration","inputs","work","workflow_plan","join_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-da48c5c792"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5991b904b9"></a>`declaration` | yes | [JoinDeclaration](#s-5433b5d3c5) |  |
| <a id="s-93f40839da"></a>`format` | no | type="string"; const="stove0-join-plan/v1"; default="stove0-join-plan/v1" |  |
| <a id="s-4e7abc4229"></a>`inputs` | yes | type="array"; items=([JoinInputPlan](#s-49428c6f7d)); minItems=2 |  |
| <a id="s-ae3e4f552d"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ad0531092f"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-975a132582"></a>`work` | yes | [WorkIdentity](#s-d107b6bb2a) |  |
| <a id="s-1d7109b5c0"></a>`workflow_plan` | yes | [WorkflowPlan](#s-272f50f9e6) |  |

##### <a id="s-957979c02a"></a>definition `JoinSettlement`

- <a id="s-c1dc2d2450"></a>`type`: `"object"`
- <a id="s-867d47289a"></a>`additionalProperties`: `false`
- <a id="s-68b529b206"></a>`required`: `["work_id","workflow_plan_sha256","join_plan_sha256","derivation_sha256","producer_settlement_sha256","output_collection","output_selection","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e2a013613a"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9c038d9b0b"></a>`format` | no | type="string"; const="stove0-join-settlement/v1"; default="stove0-join-settlement/v1" |  |
| <a id="s-d2460b1121"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c51ad98ada"></a>`output_collection` | yes | [CollectionRootRef](#s-6aec22395d) |  |
| <a id="s-96d1ffd65f"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-035224a7a0) |  |
| <a id="s-73b5b4360a"></a>`producer_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e34b00265d"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9df07c2d83"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bf671b4b74"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d7b0ac8dd7"></a>definition `JoinWorkBinding`

- <a id="s-5c14832246"></a>`type`: `"object"`
- <a id="s-75fc0f5a23"></a>`additionalProperties`: `false`
- <a id="s-7777ef5540"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a32570d2d3"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a7438669b5"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-fc5259bfe3"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-c7f82d34ca)); minItems=2 |  |
| <a id="s-2310a5423e"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c7f82d34ca"></a>definition `JoinWorkMemberBinding`

- <a id="s-81c686d5e1"></a>`type`: `"object"`
- <a id="s-b6c869b23d"></a>`additionalProperties`: `false`
- <a id="s-662b44eb0a"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1884065a96"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e58b3c6a30"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-37c607efa7"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-c1da32b7fe"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6de184bd07"></a>definition `JsonSchemaValidationProfile`

- <a id="s-c3d3fbc3b2"></a>`type`: `"object"`
- <a id="s-988e808880"></a>`additionalProperties`: `false`
- <a id="s-c70b87645a"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a6de79702"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-71facbf2b7"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-381afe8050"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d26892b4f7"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-929bda51df"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |

##### <a id="s-66063e338d"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-79dfbcc12f"></a>definition `ObservationEvidence`

- <a id="s-bb3b873b8c"></a>`type`: `"object"`
- <a id="s-0c3b00ae39"></a>`additionalProperties`: `false`
- <a id="s-4098a53abb"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-201423862b"></a>`request` | yes | [ObservationRequest](#s-4385eb9ee8) |  |
| <a id="s-761994adc6"></a>`result` | yes | [ObservationResult](#s-85dc2a4d3a) |  |

##### <a id="s-934204c078"></a>definition `ObservationFailure`

- <a id="s-716465a7a9"></a>`type`: `"object"`
- <a id="s-1d1eb311bf"></a>`additionalProperties`: `false`
- <a id="s-24339ca0fe"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c9646e926"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0986fe3906"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-7817b3a147"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-36d81105fe"></a>definition `ObservationInapplicable`

- <a id="s-6d672b4dae"></a>`type`: `"object"`
- <a id="s-693896f2fa"></a>`additionalProperties`: `false`
- <a id="s-c6cde284fd"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-436f3bfe23"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9a7f43b8be"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-4385eb9ee8"></a>definition `ObservationRequest`

- <a id="s-e812ab48df"></a>`type`: `"object"`
- <a id="s-8282ecb30b"></a>`additionalProperties`: `false`
- <a id="s-5a97e09d6b"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-873f00f6e3"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-04c86fa22f"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-74fcd9bdbc"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-35aff6fc89"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-43d045a8ab"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b47284868a"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-d764f39be4"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-537a90958a"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6236ec21be"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-5efb2ae10c"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-1ac801a307)); minItems=1 |  |
| <a id="s-525ab5a022"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-85ab73287d"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-85dc2a4d3a"></a>definition `ObservationResult`

- <a id="s-03cde5e7e5"></a>`type`: `"object"`
- <a id="s-2280a31bc7"></a>`additionalProperties`: `false`
- <a id="s-737f2d028e"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f73be934fa"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-9935aa5880"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-66063e338d))); (type="null")]; default=null |  |
| <a id="s-05cb5e01a7"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-6de184bd07)); (type="null")]; default=null |  |
| <a id="s-684a901cdd"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-428121ed7f"></a>`failure` | no | anyOf=[([ObservationFailure](#s-934204c078)); (type="null")]; default=null |  |
| <a id="s-1d185721a9"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-7fbc40093e"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-36d81105fe)); (type="null")]; default=null |  |
| <a id="s-47edfb3e1e"></a>`observer` | yes | [ObserverImplementation](#s-cbea6e2fa4) |  |
| <a id="s-614d78bac9"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-98eb7caa76"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-36ce29a604"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-61f43d58cc"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a9d4201da1"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-9d602c449e"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-1ac801a307)); minItems=1 |  |

##### <a id="s-cbea6e2fa4"></a>definition `ObserverImplementation`

- <a id="s-76396ad2d7"></a>`type`: `"object"`
- <a id="s-fee8b001c5"></a>`additionalProperties`: `false`
- <a id="s-a59f69d5cd"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-496a02c33b"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3c9065c5fd"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b22bdc7aa5"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-dea7000613"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-1be97970f8"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-d76d5602aa"></a>definition `OperationRef`

- <a id="s-e787303fca"></a>`type`: `"object"`
- <a id="s-be2bd2e8ff"></a>`additionalProperties`: `false`
- <a id="s-9079cb00de"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3f6e7959a5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e33c0f0f06"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5b91ffc491"></a>definition `RecipeRef`

- <a id="s-acf71daff1"></a>`type`: `"object"`
- <a id="s-eaec9df95d"></a>`additionalProperties`: `false`
- <a id="s-ce25292501"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f3bed7a5f8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ffa174d786"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-9307c3aac1"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d107b6bb2a"></a>definition `WorkIdentity`

- <a id="s-c65bf7b4e4"></a>`type`: `"object"`
- <a id="s-425ef67bf1"></a>`additionalProperties`: `false`
- <a id="s-23121db8f4"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d3248c8810"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-f19a707530"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-968822e600)); (type="null")]; default=null |  |
| <a id="s-77c4657870"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-183805a91c)); ([JoinWorkBinding](#s-d7b0ac8dd7))]); (type="null")]; default=null |  |
| <a id="s-0817940880"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-3713fd610f"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-6aec22395d)); minItems=1 |  |
| <a id="s-30f8a7033e"></a>`recipe` | yes | [RecipeRef](#s-5b91ffc491) |  |
| <a id="s-fd1b2d8c09"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-272f50f9e6"></a>definition `WorkflowPlan`

- <a id="s-7114c010fd"></a>`type`: `"object"`
- <a id="s-6845dcc8b3"></a>`additionalProperties`: `false`
- <a id="s-161066d7b9"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-baedcad4c1"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-62d3980b55"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-9bfd4278b7"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-79dfbcc12f)) |  |
| <a id="s-697866bf0c"></a>`operation` | yes | [OperationRef](#s-d76d5602aa) |  |
| <a id="s-42cd5d5dd2"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-e2346fc0bf"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-0b2c2543da"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-f99ae86a24"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-b8e9e9c7ce"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-81e3bbb5d8"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-223134a78d"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-1b504a5a88"></a>`work` | yes | [WorkIdentity](#s-d107b6bb2a) |  |
| <a id="s-321e3275db"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c5cdaabb80"></a>definition `WorkflowPlanIntent`

- <a id="s-b835eccb60"></a>`type`: `"object"`
- <a id="s-3a1f4d0c54"></a>`additionalProperties`: `false`
- <a id="s-810670574a"></a>`required`: `["operation","target_registration_id","target_contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-253750107d"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-f70e5809ef"></a>`operation` | yes | [OperationRef](#s-d76d5602aa) |  |
| <a id="s-0f91ab51c9"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-89617f62bc"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-66063e338d)) |  |
| <a id="s-c673334c77"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-6eab56e83e"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-da970684e5"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-87c0ce91ea"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-53fe4a28ca"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Governing policies

- <a id="pa-893a4051d5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetEvaluation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c1f8b88c7c54d47f5760bc961249236d2885e9ea69e78bfd4d051b819c2051f -->

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
        "BranchEffectSettlement": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "effect_receipt_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "stove0-branch-effect-settlement/v1",
              "default": "stove0-branch-effect-settlement/v1",
              "type": "string"
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
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
            "effect_receipt_sha256",
            "settlement_sha256"
          ],
          "type": "object"
        },
        "BranchSettlement": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "stove0-branch-settlement/v1",
              "default": "stove0-branch-settlement/v1",
              "type": "string"
            },
            "output_collection": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "output_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "producer_settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
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
            "derivation_sha256",
            "producer_settlement_sha256",
            "output_collection",
            "output_selection",
            "settlement_sha256"
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
        "JoinSettlement": {
          "additionalProperties": false,
          "properties": {
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "stove0-join-settlement/v1",
              "default": "stove0-join-settlement/v1",
              "type": "string"
            },
            "join_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "output_collection": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "output_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "producer_settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
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
            "work_id",
            "workflow_plan_sha256",
            "join_plan_sha256",
            "derivation_sha256",
            "producer_settlement_sha256",
            "output_collection",
            "output_selection",
            "settlement_sha256"
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
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "branch_set_succeeded": {
          "type": "boolean"
        },
        "canceled_branch_ids": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "type": "array"
        },
        "coordination_complete_for_retirement": {
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
          ]
        },
        "failed_branch_ids": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "type": "array"
        },
        "inapplicable_branch_ids": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "type": "array"
        },
        "interrupted_branch_ids": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "type": "array"
        },
        "join_ready": {
          "type": "boolean"
        },
        "join_settlement": {
          "anyOf": [
            {
              "$ref": "#/$defs/JoinSettlement"
            },
            {
              "type": "null"
            }
          ]
        },
        "join_state": {
          "enum": [
            "not-declared",
            "waiting",
            "ready",
            "succeeded",
            "failed",
            "inapplicable",
            "interrupted",
            "canceled"
          ],
          "type": "string"
        },
        "resolved_join_plan": {
          "anyOf": [
            {
              "$ref": "#/$defs/JoinPlan"
            },
            {
              "type": "null"
            }
          ]
        },
        "retirement_requested": {
          "type": "boolean"
        },
        "succeeded_branches": {
          "items": {
            "$ref": "#/$defs/BranchSettlement"
          },
          "type": "array"
        },
        "succeeded_coordinations": {
          "items": {
            "$ref": "#/$defs/CoordinationSettlement"
          },
          "type": "array"
        },
        "succeeded_effects": {
          "items": {
            "$ref": "#/$defs/BranchEffectSettlement"
          },
          "type": "array"
        },
        "unsettled_branch_ids": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "type": "array"
        },
        "unsettled_work_ids": {
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "type": "array"
        }
      },
      "required": [
        "branch_set_sha256",
        "succeeded_branches",
        "succeeded_effects",
        "succeeded_coordinations",
        "unsettled_branch_ids",
        "failed_branch_ids",
        "inapplicable_branch_ids",
        "interrupted_branch_ids",
        "canceled_branch_ids",
        "join_ready",
        "resolved_join_plan",
        "join_state",
        "join_settlement",
        "unsettled_work_ids",
        "branch_set_succeeded",
        "coordination_settlement",
        "retirement_requested",
        "coordination_complete_for_retirement"
      ],
      "type": "object"
    },
    "signature": "\"(*, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], succeeded_branches: tuple[stove0_protocol.fork_join.BranchSettlement, ...], succeeded_effects: tuple[stove0_protocol.fork_join.BranchEffectSettlement, ...], succeeded_coordinations: tuple[stove0_protocol.fork_join.CoordinationSettlement, ...], unsettled_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], failed_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], inapplicable_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], interrupted_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], canceled_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], join_ready: bool, resolved_join_plan: stove0_protocol.fork_join.JoinPlan | None, join_state: Literal['not-declared', 'waiting', 'ready', 'succeeded', 'failed', 'inapplicable', 'interrupted', 'canceled'], join_settlement: stove0_protocol.fork_join.JoinSettlement | None, unsettled_work_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...], branch_set_succeeded: bool, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None, retirement_requested: bool, coordination_complete_for_retirement: bool) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchSetEvaluation",
  "unit": "export"
}
```

</details>
