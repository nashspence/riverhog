# stove0_target_support.TargetJobDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobdeclaration:2537fcd64f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a994460853"></a>
- <a id="s-bc0b7ab172"></a>`distribution`: `stove0-target-support`
- <a id="s-19f1ca9bdd"></a>`module`: `stove0_target_support`
- <a id="s-48e4a253d5"></a>`name`: `TargetJobDeclaration`
- <a id="s-269780d130"></a>`unit`: `export`

### Declared structure

- <a id="s-51f10de20e"></a>`kind`: `"class"`
- <a id="s-ba569741ca"></a>`signature`: `"\"(*, job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], controller_evidence: stove0_protocol.models.ControllerEvidence, plan: stove0_target_protocol.protocol.TransformPlan \| stove0_target_protocol.protocol.EffectPlan, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None\""`

#### Validated model schema

<a id="s-0cce3f8154"></a>

- <a id="s-0aa82efd61"></a>`type`: `"object"`
- <a id="s-336d5b4dd7"></a>`additionalProperties`: `false`
- <a id="s-4b6278c416"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c4d38973ef"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-70837e4229"></a>`controller_evidence` | yes | [ControllerEvidence](#s-73dba491c8) |  |
| <a id="s-6318080f88"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-aff7c34a2e"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-71bea24626"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-070c476a93)); ([EffectPlan](#s-dec311e248))] |  |
| <a id="s-3138a1ddfe"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

##### Definitions

- [ArtifactSelectionRef](#s-30c3e47686)
- [ArtifactSubject](#s-f5d74bcd2a)
- [BranchWorkBinding](#s-033c6dc19a)
- [CollectionId](#s-a68e2629c1)
- [CollectionRootRef](#s-d33cf5c722)
- [ControllerEvidence](#s-73dba491c8)
- [EffectPlan](#s-dec311e248)
- [EvaluationBinding](#s-b7886c1732)
- [ExecutionEnvelope](#s-1ededad329)
- [JoinWorkBinding](#s-c45ec498f9)
- [JoinWorkMemberBinding](#s-804402c81b)
- [JsonSchemaValidationProfile](#s-5aefee9502)
- [JsonValue](#s-171969873a)
- [ObservationEvidence](#s-9526700216)
- [ObservationFailure](#s-0f0e4fdf4d)
- [ObservationInapplicable](#s-388b7d1295)
- [ObservationRequest](#s-39418d1c7c)
- [ObservationResult](#s-cc56272786)
- [ObserverImplementation](#s-2f95dafde9)
- [OperationRef](#s-bbc3e9aed2)
- [RecipeRef](#s-f4a9dc1600)
- [TargetInputAuthority](#s-9b71375628)
- [TargetInputRoleCount](#s-c830cdcc21)
- [TargetPlanBinding](#s-e709073479)
- [TransformPlan](#s-070c476a93)
- [WorkIdentity](#s-58046d6115)
- [WorkflowPlan](#s-bc2c7a33ce)

##### <a id="s-30c3e47686"></a>definition `ArtifactSelectionRef`

- <a id="s-45861aec71"></a>`type`: `"object"`
- <a id="s-4d8c4bb000"></a>`additionalProperties`: `false`
- <a id="s-a1c16e7c93"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-58d7f71161"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-c0a50966c3"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-922ee157e2"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-f5d74bcd2a"></a>definition `ArtifactSubject`

- <a id="s-11bc003999"></a>`type`: `"object"`
- <a id="s-60d429384c"></a>`additionalProperties`: `false`
- <a id="s-933c22129d"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05f8a816d2"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-5f898fd066"></a>`collection` | yes | [CollectionRootRef](#s-d33cf5c722) |  |
| <a id="s-f26ebd7082"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-15d84e387d"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-d8cf42589a"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-5cfb4b7e68"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9bd59b0dab"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-033c6dc19a"></a>definition `BranchWorkBinding`

- <a id="s-e15895ed27"></a>`type`: `"object"`
- <a id="s-ee0a1ce95f"></a>`additionalProperties`: `false`
- <a id="s-f93ee79e45"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a21de3cff8"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e583a6f34c"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cee23ca4c2"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a1594046d"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-04799b4646"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a68e2629c1"></a>definition `CollectionId`

- <a id="s-7ff3d44e50"></a>`type`: `"integer"`
- <a id="s-3fa0cb2ffb"></a>`minimum`: `1`

##### <a id="s-d33cf5c722"></a>definition `CollectionRootRef`

- <a id="s-f7bc65623d"></a>`type`: `"object"`
- <a id="s-07c883f1cd"></a>`additionalProperties`: `false`
- <a id="s-cab77576e5"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d7b81310ec"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3fe4f1edb3"></a>`collection_id` | yes | [CollectionId](#s-a68e2629c1) |  |
| <a id="s-279ce74498"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-73dba491c8"></a>definition `ControllerEvidence`

- <a id="s-4aa2000f66"></a>`type`: `"object"`
- <a id="s-5f6154ac21"></a>`additionalProperties`: `false`
- <a id="s-cbe5f67a07"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6f537db605"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2a4e0d5942"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-1ededad329) |  |
| <a id="s-29ceee19e6"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-dec311e248"></a>definition `EffectPlan`

- <a id="s-cb344248b2"></a>`type`: `"object"`
- <a id="s-8a8d3ff0b2"></a>`additionalProperties`: `false`
- <a id="s-7ed2eff047"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-916e88904b"></a>`inputs` | yes | [TargetInputAuthority](#s-9b71375628) |  |
| <a id="s-e7f69d0c1e"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-31c7e29ee4"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-32fe1439b1"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fddf15c4cb"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-24bd7d8321"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b43f857538"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-e4c067d3f8"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-235cae45e4"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1566b05d3b"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |

##### <a id="s-b7886c1732"></a>definition `EvaluationBinding`

- <a id="s-fe6e57feec"></a>`type`: `"object"`
- <a id="s-351c19b52d"></a>`additionalProperties`: `false`
- <a id="s-db8beedf91"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5b757c0c3d"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d782f2e6f4"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb46280321"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-2f0bc81309"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-1ededad329"></a>definition `ExecutionEnvelope`

- <a id="s-520569b0bb"></a>`type`: `"object"`
- <a id="s-fcac28d13b"></a>`additionalProperties`: `false`
- <a id="s-1b5154ebaa"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3f3f448163"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-37a2b4bb73"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fe01b4852e"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-a6ad2705b6"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-950ad61e9d"></a>`target_plan` | yes | [TargetPlanBinding](#s-e709073479) |  |
| <a id="s-255a3a5036"></a>`workflow_plan` | yes | [WorkflowPlan](#s-bc2c7a33ce) |  |

##### <a id="s-c45ec498f9"></a>definition `JoinWorkBinding`

- <a id="s-a07f4c21e4"></a>`type`: `"object"`
- <a id="s-c5bad5a1c4"></a>`additionalProperties`: `false`
- <a id="s-78c17c914e"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba88e08eff"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7671ef3f7a"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-a5ee76b9a4"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-804402c81b)); minItems=2 |  |
| <a id="s-9573524fc8"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-804402c81b"></a>definition `JoinWorkMemberBinding`

- <a id="s-439dce2db2"></a>`type`: `"object"`
- <a id="s-13e0fd99ab"></a>`additionalProperties`: `false`
- <a id="s-d6d1a4ad56"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed180852f8"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e7fe4a0506"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ef0d2180bb"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-7230b5b581"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5aefee9502"></a>definition `JsonSchemaValidationProfile`

- <a id="s-69f8b03da2"></a>`type`: `"object"`
- <a id="s-92b049315d"></a>`additionalProperties`: `false`
- <a id="s-5edc7292f1"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-55482aae3b"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-e2e42190c8"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-ba53773880"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-499a11d0e1"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0404c23e5c"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |

##### <a id="s-171969873a"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-9526700216"></a>definition `ObservationEvidence`

- <a id="s-96f6cc26c8"></a>`type`: `"object"`
- <a id="s-512cd14e5f"></a>`additionalProperties`: `false`
- <a id="s-487d9b9c58"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-137361100b"></a>`request` | yes | [ObservationRequest](#s-39418d1c7c) |  |
| <a id="s-b191d26efd"></a>`result` | yes | [ObservationResult](#s-cc56272786) |  |

##### <a id="s-0f0e4fdf4d"></a>definition `ObservationFailure`

- <a id="s-441f061a53"></a>`type`: `"object"`
- <a id="s-2e28f59081"></a>`additionalProperties`: `false`
- <a id="s-9db09594a2"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-91d3ded69a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a281018af6"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-a54dd8337e"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-388b7d1295"></a>definition `ObservationInapplicable`

- <a id="s-4d8f854e0f"></a>`type`: `"object"`
- <a id="s-890140419d"></a>`additionalProperties`: `false`
- <a id="s-af7bb43355"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f07014a92f"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-31f15c2281"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-39418d1c7c"></a>definition `ObservationRequest`

- <a id="s-31776e7389"></a>`type`: `"object"`
- <a id="s-4f419bb6fa"></a>`additionalProperties`: `false`
- <a id="s-1ccd38e6d6"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fc2ef37305"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-f5c9b4e65b"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-ab4b96c5a7"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cd54ff2a9f"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5ee6e72802"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d8b79c8f78"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-e2894ab119"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-4c73d5f8cb"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9e8f21f96c"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-7392ce040e"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-f5d74bcd2a)); minItems=1 |  |
| <a id="s-37fc7b38b5"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-5aceb8fa21"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-cc56272786"></a>definition `ObservationResult`

- <a id="s-db0d43299d"></a>`type`: `"object"`
- <a id="s-5d772a0e6c"></a>`additionalProperties`: `false`
- <a id="s-b03918eb61"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5bb8916249"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-8ac7558f37"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-171969873a))); (type="null")]; default=null |  |
| <a id="s-221e0440dc"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-5aefee9502)); (type="null")]; default=null |  |
| <a id="s-7c8a96c73b"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-8c205d1d44"></a>`failure` | no | anyOf=[([ObservationFailure](#s-0f0e4fdf4d)); (type="null")]; default=null |  |
| <a id="s-44ff78d120"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-228660044e"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-388b7d1295)); (type="null")]; default=null |  |
| <a id="s-4564b545a8"></a>`observer` | yes | [ObserverImplementation](#s-2f95dafde9) |  |
| <a id="s-bd818eb3e6"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-50baa28371"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c8b7c70871"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2e170524d0"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-466feefc15"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-733b65ef05"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-f5d74bcd2a)); minItems=1 |  |

##### <a id="s-2f95dafde9"></a>definition `ObserverImplementation`

- <a id="s-9e1a8cc7a1"></a>`type`: `"object"`
- <a id="s-5d00b38360"></a>`additionalProperties`: `false`
- <a id="s-7dfaf0efb3"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eab344dbb2"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-02714a4a3a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-10b229f981"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-2ffa67e340"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-da6e00abdc"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-bbc3e9aed2"></a>definition `OperationRef`

- <a id="s-45f2c0bee5"></a>`type`: `"object"`
- <a id="s-3f5081bf9b"></a>`additionalProperties`: `false`
- <a id="s-b1a66287ca"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66c216ec0b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a21367c12b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f4a9dc1600"></a>definition `RecipeRef`

- <a id="s-bbc30610cc"></a>`type`: `"object"`
- <a id="s-c55bc93862"></a>`additionalProperties`: `false`
- <a id="s-cad7431b20"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fd78dc4611"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-64dc5257e7"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-93ca5e8165"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9b71375628"></a>definition `TargetInputAuthority`

- <a id="s-a8f408f7d7"></a>`type`: `"object"`
- <a id="s-8a90a98197"></a>`additionalProperties`: `false`
- <a id="s-c9b359e428"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-478a31164e"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-c830cdcc21)); minItems=1 |  |
| <a id="s-b0d9790711"></a>`selection` | yes | [ArtifactSelectionRef](#s-30c3e47686) |  |

##### <a id="s-c830cdcc21"></a>definition `TargetInputRoleCount`

- <a id="s-69684ddc01"></a>`type`: `"object"`
- <a id="s-f8b5ac29fa"></a>`additionalProperties`: `false`
- <a id="s-5ab833377a"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7752b76e02"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-5b68d4c4b4"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-e709073479"></a>definition `TargetPlanBinding`

- <a id="s-99106a38f9"></a>`type`: `"object"`
- <a id="s-45008056f4"></a>`additionalProperties`: `false`
- <a id="s-6e8d048210"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-89e314fbb0"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c9df2e69a4"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-f6216268e6"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9e31ceccfc"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-16249abc81"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aab2b2cb5f"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-070c476a93"></a>definition `TransformPlan`

- <a id="s-9c9ad206af"></a>`type`: `"object"`
- <a id="s-8f63cc28ae"></a>`additionalProperties`: `false`
- <a id="s-ba793447fd"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-969c44272b"></a>`inputs` | yes | [TargetInputAuthority](#s-9b71375628) |  |
| <a id="s-38c0678ba9"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-5a0cee4dad"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-2788399cbb"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aed588f409"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8e769c2fac"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a2b6df9a56"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-a1a2eda071"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0bac4df22b"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-06bafdbf93"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |

##### <a id="s-58046d6115"></a>definition `WorkIdentity`

- <a id="s-43db562f95"></a>`type`: `"object"`
- <a id="s-6a83db3573"></a>`additionalProperties`: `false`
- <a id="s-0dad0722fd"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa95d9d725"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-aad1196ec6"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-b7886c1732)); (type="null")]; default=null |  |
| <a id="s-4118c9819a"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-033c6dc19a)); ([JoinWorkBinding](#s-c45ec498f9))]); (type="null")]; default=null |  |
| <a id="s-7bbee6a467"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-e1263ab545"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-d33cf5c722)); minItems=1 |  |
| <a id="s-f28ff63148"></a>`recipe` | yes | [RecipeRef](#s-f4a9dc1600) |  |
| <a id="s-53dc8cbb5f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bc2c7a33ce"></a>definition `WorkflowPlan`

- <a id="s-e383d1222f"></a>`type`: `"object"`
- <a id="s-1b65317d29"></a>`additionalProperties`: `false`
- <a id="s-9961c946d8"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fee30bfb60"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-cac5ca3a77"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-c1e0c004f8"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-9526700216)) |  |
| <a id="s-bad511730f"></a>`operation` | yes | [OperationRef](#s-bbc3e9aed2) |  |
| <a id="s-638dab7b3c"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-0d514087a2"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-171969873a)) |  |
| <a id="s-478bd3e1e1"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-a540bb147c"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-352830bbd0"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-92100fbbd7"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fce269e0fe"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-41fbf00bd5"></a>`work` | yes | [WorkIdentity](#s-58046d6115) |  |
| <a id="s-60be83563b"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [bind_execution](stove0-target-support-targetjobdeclaration-bind-execution.md)
- [canonical_claim_id](stove0-target-support-targetjobdeclaration-canonical-claim-id.md)

## Governing policies

- <a id="pa-43f6883e96"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobDeclaration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f1e8f688a7a1e140b3fe2dc53658462623f1dcf74b7e5ae5dd6fa862714b1668 -->

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
        }
      },
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
    "signature": "\"(*, job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], controller_evidence: stove0_protocol.models.ControllerEvidence, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetJobDeclaration",
  "unit": "export"
}
```

</details>
