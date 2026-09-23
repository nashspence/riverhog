# stove0_protocol.BranchSetDecision

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetdecision:fecb4aab4c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-000575956d"></a>
- <a id="s-08ef765399"></a>`distribution`: `stove0-protocol`
- <a id="s-6bf012a543"></a>`module`: `stove0_protocol`
- <a id="s-7b883408c3"></a>`name`: `BranchSetDecision`
- <a id="s-95e760f5fd"></a>`unit`: `export`

### Declared structure

- <a id="s-8f9c043a80"></a>`kind`: `"class"`
- <a id="s-310283ee79"></a>`signature`: `"'(*, plan: stove0_protocol.fork_join.BranchSetPlan, selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...], branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = ()) -> None'"`

#### Validated model schema

<a id="s-78870634c9"></a>

- <a id="s-552a881ef0"></a>`type`: `"object"`
- <a id="s-ca398fb1b4"></a>`additionalProperties`: `false`
- <a id="s-5ce38cca4e"></a>`required`: `["plan","selections"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-842cbaef4a"></a>`branch_sets` | no | type="array"; default=[]; items=([BranchSetPlan](#s-e4176c3948)) |  |
| <a id="s-2199788d98"></a>`plan` | yes | [BranchSetPlan](#s-e4176c3948) |  |
| <a id="s-c8c52262e9"></a>`selections` | yes | type="array"; items=([ArtifactSelection](#s-c48c4e501f)) |  |

##### Definitions

- [ArtifactSelection](#s-c48c4e501f)
- [ArtifactSelectionRef](#s-c2afda83a9)
- [ArtifactSubject](#s-60594ab5fe)
- [BranchPlan](#s-321c3e0167)
- [BranchSetPlan](#s-e4176c3948)
- [BranchWorkBinding](#s-7f0e2606ca)
- [CollectionId](#s-c79ec92285)
- [CollectionRootRef](#s-8e3a0f73b8)
- [CoordinationBranchPlan](#s-3ba08b84f3)
- [EvaluationBinding](#s-0bd3ea262a)
- [JoinDeclaration](#s-f2d7b7c3a4)
- [JoinMemberDeclaration](#s-4aebf2ceea)
- [JoinWorkBinding](#s-71ce17a935)
- [JoinWorkMemberBinding](#s-aecd90451b)
- [JsonSchemaValidationProfile](#s-3cae332589)
- [JsonValue](#s-056ad80d93)
- [ObservationEvidence](#s-29cb963d0a)
- [ObservationFailure](#s-52cc96a2a6)
- [ObservationInapplicable](#s-40b3c88c80)
- [ObservationRequest](#s-a979d313c7)
- [ObservationResult](#s-3879b20b2e)
- [ObserverImplementation](#s-0e52288b72)
- [OperationRef](#s-99eb3814e2)
- [RecipeRef](#s-0ea8c3b906)
- [WorkIdentity](#s-71955789c7)
- [WorkflowPlan](#s-53ad6d3d21)
- [WorkflowPlanIntent](#s-a19463651e)

##### <a id="s-c48c4e501f"></a>definition `ArtifactSelection`

- <a id="s-aa28c9cef1"></a>`type`: `"object"`
- <a id="s-07f0179ac1"></a>`additionalProperties`: `false`
- <a id="s-5b44d84024"></a>`required`: `["artifacts","artifact_count","total_bytes","selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0e34a5452f"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-8783984bd7"></a>`artifacts` | yes | type="array"; items=([ArtifactSubject](#s-60594ab5fe)); minItems=1 |  |
| <a id="s-5898b50f7d"></a>`format` | no | type="string"; const="stove0-artifact-selection/v1"; default="stove0-artifact-selection/v1" |  |
| <a id="s-a1a82185ec"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-42fd05d948"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-c2afda83a9"></a>definition `ArtifactSelectionRef`

- <a id="s-fd55a3e56b"></a>`type`: `"object"`
- <a id="s-08d56fac13"></a>`additionalProperties`: `false`
- <a id="s-1f7d65a7c8"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e06365add"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-119217fcfe"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b086cb76a"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-60594ab5fe"></a>definition `ArtifactSubject`

- <a id="s-aa243fb86b"></a>`type`: `"object"`
- <a id="s-cedc837b6e"></a>`additionalProperties`: `false`
- <a id="s-026d0a646f"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8d5272b9a8"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-efb814f523"></a>`collection` | yes | [CollectionRootRef](#s-8e3a0f73b8) |  |
| <a id="s-c940931140"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-6bb7dff046"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-7709bd4ac5"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-9b2f1d741f"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-11c79aa3da"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-321c3e0167"></a>definition `BranchPlan`

- <a id="s-ed1ff76112"></a>`type`: `"object"`
- <a id="s-ac9611e5c0"></a>`additionalProperties`: `false`
- <a id="s-ad1f6c1855"></a>`required`: `["branch_id","artifact_selection","workflow_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d43c813df1"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-c2afda83a9) |  |
| <a id="s-843518aa86"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bdfbfba482"></a>`kind` | no | type="string"; const="leaf"; default="leaf" |  |
| <a id="s-e62fbd8e7f"></a>`workflow_plan` | yes | [WorkflowPlan](#s-53ad6d3d21) |  |

##### <a id="s-e4176c3948"></a>definition `BranchSetPlan`

- <a id="s-e3b41d5e1a"></a>`type`: `"object"`
- <a id="s-5a8341cdb1"></a>`additionalProperties`: `false`
- <a id="s-2b6ae0045a"></a>`required`: `["parent_work","decision_sha256","branches","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3a008d0df5"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a237ff4370"></a>`branches` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/CoordinationBranchPlan","leaf":"#/$defs/BranchPlan"},"propertyName":"kind"}; oneOf=[([BranchPlan](#s-321c3e0167)); ([CoordinationBranchPlan](#s-3ba08b84f3))]); minItems=1 |  |
| <a id="s-395bc5c422"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-38a25633f3"></a>`evidence_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-19271b8433"></a>`format` | no | type="string"; const="stove0-branch-set/v1"; default="stove0-branch-set/v1" |  |
| <a id="s-a908fa53bc"></a>`join` | no | anyOf=[([JoinDeclaration](#s-f2d7b7c3a4)); (type="null")]; default=null |  |
| <a id="s-c0bcdc1dc3"></a>`parent_work` | yes | [WorkIdentity](#s-71955789c7) |  |
| <a id="s-92bc4b4fd4"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-4faa5362d2"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### <a id="s-7f0e2606ca"></a>definition `BranchWorkBinding`

- <a id="s-5bc9030ae9"></a>`type`: `"object"`
- <a id="s-d74c1822b1"></a>`additionalProperties`: `false`
- <a id="s-d79aea800a"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f8806870ca"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6e08fb591c"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e3d77c173b"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb78108ba4"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-ba21b955a0"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c79ec92285"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-ee326f281c"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-1594700d4d"></a>2 | not=(const="0") |

##### <a id="s-8e3a0f73b8"></a>definition `CollectionRootRef`

- <a id="s-991d7d1267"></a>`type`: `"object"`
- <a id="s-2ab838ce4b"></a>`additionalProperties`: `false`
- <a id="s-41a6637762"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c09f6635d4"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e6c56b9a36"></a>`collection_id` | yes | [CollectionId](#s-c79ec92285) |  |
| <a id="s-31cf13def3"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3ba08b84f3"></a>definition `CoordinationBranchPlan`

- <a id="s-9637ff8c43"></a>`type`: `"object"`
- <a id="s-fd1869ba7a"></a>`additionalProperties`: `false`
- <a id="s-ad7f416730"></a>`required`: `["branch_id","artifact_selection","work","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a00d8d24f3"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-c2afda83a9) |  |
| <a id="s-5f36c6119f"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cddc2ea0ff"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e1276ca09f"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-3057eaf095"></a>`work` | yes | [WorkIdentity](#s-71955789c7) |  |

##### <a id="s-0bd3ea262a"></a>definition `EvaluationBinding`

- <a id="s-765f0e7233"></a>`type`: `"object"`
- <a id="s-b739e8f948"></a>`additionalProperties`: `false`
- <a id="s-6056d79551"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-243d78d6b4"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ebcbdc8989"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e514513a1e"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-7e21f935c0"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-f2d7b7c3a4"></a>definition `JoinDeclaration`

- <a id="s-b2b3d5e732"></a>`type`: `"object"`
- <a id="s-a580aa6ff0"></a>`additionalProperties`: `false`
- <a id="s-528ef4be89"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac45f4f56c"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-1135acc835"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-e5c03680b8"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b0e4955098"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-4aebf2ceea)); minItems=2 |  |
| <a id="s-7ff42251c9"></a>`recipe` | yes | [RecipeRef](#s-0ea8c3b906) |  |
| <a id="s-068d5de458"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-a19463651e) |  |

##### <a id="s-4aebf2ceea"></a>definition `JoinMemberDeclaration`

- <a id="s-b607f75396"></a>`type`: `"object"`
- <a id="s-a3febc4015"></a>`additionalProperties`: `false`
- <a id="s-d239b2a865"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca72cb2ba9"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5347bfa25e"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-71ce17a935"></a>definition `JoinWorkBinding`

- <a id="s-24c42832ba"></a>`type`: `"object"`
- <a id="s-38fc174e24"></a>`additionalProperties`: `false`
- <a id="s-8324c62aec"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d422a5316"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0a2adeb20c"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-496660f12a"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-aecd90451b)); minItems=2 |  |
| <a id="s-2cc61d50d6"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-aecd90451b"></a>definition `JoinWorkMemberBinding`

- <a id="s-faef9754fa"></a>`type`: `"object"`
- <a id="s-b91bdbd362"></a>`additionalProperties`: `false`
- <a id="s-aa0a93339b"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8907ef7420"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-471efe4d06"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e9e712f7be"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-6aab8213a1"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3cae332589"></a>definition `JsonSchemaValidationProfile`

- <a id="s-83f7661a07"></a>`type`: `"object"`
- <a id="s-958459fb12"></a>`additionalProperties`: `false`
- <a id="s-251929564e"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-679f1c14a2"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-d9d738311a"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-f647317197"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c34ec41993"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-43a5915cd5"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |

##### <a id="s-056ad80d93"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-29cb963d0a"></a>definition `ObservationEvidence`

- <a id="s-f255f830f9"></a>`type`: `"object"`
- <a id="s-8df0fc81f2"></a>`additionalProperties`: `false`
- <a id="s-a549bcec04"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-39e1b57b30"></a>`request` | yes | [ObservationRequest](#s-a979d313c7) |  |
| <a id="s-1d25c87a7d"></a>`result` | yes | [ObservationResult](#s-3879b20b2e) |  |

##### <a id="s-52cc96a2a6"></a>definition `ObservationFailure`

- <a id="s-5bf5476670"></a>`type`: `"object"`
- <a id="s-0fd8925031"></a>`additionalProperties`: `false`
- <a id="s-cc46b69523"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c936e91ae"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-50da608868"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-3f6a0d25a4"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-40b3c88c80"></a>definition `ObservationInapplicable`

- <a id="s-d538382b75"></a>`type`: `"object"`
- <a id="s-71bff27499"></a>`additionalProperties`: `false`
- <a id="s-321295cac3"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5faaa85dda"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-90b9e4d25a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-a979d313c7"></a>definition `ObservationRequest`

- <a id="s-53da214158"></a>`type`: `"object"`
- <a id="s-2c0208adab"></a>`additionalProperties`: `false`
- <a id="s-c859b72a3d"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-03e29b8489"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-26365550db"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-7d2f928cdf"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d5ebdd1372"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2efc1531a1"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb1ee42a6e"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-0c4cb20233"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-39243afaa7"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-45e03f95e7"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-745a80f585"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-60594ab5fe)); minItems=1 |  |
| <a id="s-e5a0f17508"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-5a9bbe0654"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3879b20b2e"></a>definition `ObservationResult`

- <a id="s-be23075e51"></a>`type`: `"object"`
- <a id="s-4f9e8d2f9b"></a>`additionalProperties`: `false`
- <a id="s-c9486180cc"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b310f994ed"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-527e96c980"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-056ad80d93))); (type="null")]; default=null |  |
| <a id="s-683f17c102"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-3cae332589)); (type="null")]; default=null |  |
| <a id="s-0a6aaab2a7"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-22e69fbb11"></a>`failure` | no | anyOf=[([ObservationFailure](#s-52cc96a2a6)); (type="null")]; default=null |  |
| <a id="s-bf6d6f4e0e"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-fe291a84d6"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-40b3c88c80)); (type="null")]; default=null |  |
| <a id="s-c61676da7b"></a>`observer` | yes | [ObserverImplementation](#s-0e52288b72) |  |
| <a id="s-f0d23850e4"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a056427758"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-54b38528d7"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0e174e8570"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5bbe1d3da0"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-b7fbbdad29"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-60594ab5fe)); minItems=1 |  |

##### <a id="s-0e52288b72"></a>definition `ObserverImplementation`

- <a id="s-1437e52210"></a>`type`: `"object"`
- <a id="s-7ee89a5a74"></a>`additionalProperties`: `false`
- <a id="s-b90f2b7833"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-03afadb1e3"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8a9483080b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8967ea6326"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-1e417a29e3"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-9dffd7e2ec"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-99eb3814e2"></a>definition `OperationRef`

- <a id="s-599ed8205e"></a>`type`: `"object"`
- <a id="s-bb6c6a2f36"></a>`additionalProperties`: `false`
- <a id="s-64ee19ca0a"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ce5980846"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-19ce920924"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0ea8c3b906"></a>definition `RecipeRef`

- <a id="s-a13c404ffd"></a>`type`: `"object"`
- <a id="s-96b1d4d954"></a>`additionalProperties`: `false`
- <a id="s-f300687c70"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-281be1f804"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0a75cbdffa"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-cd4ed7afcd"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-71955789c7"></a>definition `WorkIdentity`

- <a id="s-04784c2df5"></a>`type`: `"object"`
- <a id="s-b514a02888"></a>`additionalProperties`: `false`
- <a id="s-4748b493d1"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d460910d56"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-846b1d959d"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-0bd3ea262a)); (type="null")]; default=null |  |
| <a id="s-0c8993400f"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-7f0e2606ca)); ([JoinWorkBinding](#s-71ce17a935))]); (type="null")]; default=null |  |
| <a id="s-e781738441"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-ab0653673e"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-8e3a0f73b8)); minItems=1 |  |
| <a id="s-7781f451ee"></a>`recipe` | yes | [RecipeRef](#s-0ea8c3b906) |  |
| <a id="s-25bf8a06c9"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-53ad6d3d21"></a>definition `WorkflowPlan`

- <a id="s-9d60db1421"></a>`type`: `"object"`
- <a id="s-2959fe277a"></a>`additionalProperties`: `false`
- <a id="s-c66ebb944f"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-179970fa88"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-64e3ec07f3"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-9a20f366c5"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-29cb963d0a)) |  |
| <a id="s-561104fd4c"></a>`operation` | yes | [OperationRef](#s-99eb3814e2) |  |
| <a id="s-d58282fe62"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-9a0a0e2c6f"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-4fc2f6388c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-d7cee45ad4"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-e90d760a09"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-b25f5e9311"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ed174f4fd2"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-4a1baf322d"></a>`work` | yes | [WorkIdentity](#s-71955789c7) |  |
| <a id="s-a09cbaa21a"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a19463651e"></a>definition `WorkflowPlanIntent`

- <a id="s-d3b0e8f04e"></a>`type`: `"object"`
- <a id="s-7ac57df965"></a>`additionalProperties`: `false`
- <a id="s-105a8215fd"></a>`required`: `["operation","target_registration_id","target_contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc63f10c5e"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-9f9e400c20"></a>`operation` | yes | [OperationRef](#s-99eb3814e2) |  |
| <a id="s-3052eec6fd"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-48807604a1"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-056ad80d93)) |  |
| <a id="s-b207bc52d1"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-e310f3d27f"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-a9efbbc9ed"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-cce2e09ebe"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-49014d0f4b"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [branch_set_documents](stove0-protocol-branchsetdecision-branch-set-documents.md)
- [canonical_branch_sets](stove0-protocol-branchsetdecision-canonical-branch-sets.md)
- [canonical_selections](stove0-protocol-branchsetdecision-canonical-selections.md)
- [complete_documents](stove0-protocol-branchsetdecision-complete-documents.md)
- [leaf_branches](stove0-protocol-branchsetdecision-leaf-branches.md)
- [selection_documents](stove0-protocol-branchsetdecision-selection-documents.md)

## Governing policies

- <a id="pa-74e8123523"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetDecision`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ad72cdc94651206687bc371523304bf6ad2c66702a01498b3c5f57fbe3170c8 -->

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
                "$ref": "#/$defs/ArtifactSubject"
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
              "minimum": 0,
              "type": "integer"
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
        "branch_sets": {
          "default": [],
          "items": {
            "$ref": "#/$defs/BranchSetPlan"
          },
          "type": "array"
        },
        "plan": {
          "$ref": "#/$defs/BranchSetPlan"
        },
        "selections": {
          "items": {
            "$ref": "#/$defs/ArtifactSelection"
          },
          "type": "array"
        }
      },
      "required": [
        "plan",
        "selections"
      ],
      "type": "object"
    },
    "signature": "'(*, plan: stove0_protocol.fork_join.BranchSetPlan, selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...], branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = ()) -> None'"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchSetDecision",
  "unit": "export"
}
```

</details>
