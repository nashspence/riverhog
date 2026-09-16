# stove0_target_support.AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-acceptedtargetjob:f4887db677 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b852337104"></a>
- <a id="s-e9e2b14fd3"></a>`distribution`: `stove0-target-support`
- <a id="s-20411255f8"></a>`module`: `stove0_target_support`
- <a id="s-ebebba62f6"></a>`name`: `AcceptedTargetJob`
- <a id="s-68775f8cff"></a>`unit`: `export`

### Declared structure

- <a id="s-8edee87a2a"></a>`kind`: `"class"`
- <a id="s-e2d44fc143"></a>`signature`: `"\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-5bc900ceeb"></a>

- <a id="s-61157ff23a"></a>`type`: `"object"`
- <a id="s-63ddb4796d"></a>`additionalProperties`: `false`
- <a id="s-f6717d4ec0"></a>`required`: `["declaration","request_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25bea4191a"></a>`declaration` | yes | [TargetJobDeclaration](#s-56e270ccf3) |  |
| <a id="s-47268fbb6e"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSelectionRef](#s-7ad6a47971)
- [ArtifactSubject](#s-9c4214d418)
- [BranchWorkBinding](#s-0e753c1d24)
- [CollectionId](#s-7a021246a3)
- [CollectionRootRef](#s-7ab492e53b)
- [ControllerEvidence](#s-72053cc702)
- [EffectPlan](#s-133e2a7113)
- [EvaluationBinding](#s-9f76caaf2a)
- [ExecutionEnvelope](#s-038720f607)
- [JoinWorkBinding](#s-d0ae9252d9)
- [JoinWorkMemberBinding](#s-892541caca)
- [JsonSchemaDocument](#s-5cb95977fa)
- [JsonValue](#s-71f440f6f9)
- [ObservationEvidence](#s-f0e8f8ac07)
- [ObservationFailure](#s-bb4acf1a79)
- [ObservationInapplicable](#s-a91d9baade)
- [ObservationRequest](#s-d0e13fdfbd)
- [ObservationResult](#s-32b62c853b)
- [ObserverImplementation](#s-f29ba2e6fa)
- [OperationRef](#s-7e2ea6a762)
- [RecipeRef](#s-d7f2e769d4)
- [TargetInputAuthority](#s-9f6926eb08)
- [TargetInputRoleCount](#s-413658dec0)
- [TargetJobDeclaration](#s-56e270ccf3)
- [TargetPlanBinding](#s-bd9a2f7c37)
- [TransformPlan](#s-eddb58c797)
- [WorkIdentity](#s-8e0976835d)
- [WorkflowPlan](#s-45e6602b03)

##### <a id="s-7ad6a47971"></a>definition `ArtifactSelectionRef`

- <a id="s-9758ca43b6"></a>`type`: `"object"`
- <a id="s-8ca04de96c"></a>`additionalProperties`: `false`
- <a id="s-31f589202a"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b9fcd9fcc0"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-45d18edeb3"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-80a879df63"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-9c4214d418"></a>definition `ArtifactSubject`

- <a id="s-a2a0bf95bb"></a>`type`: `"object"`
- <a id="s-a4fcae7f60"></a>`additionalProperties`: `false`
- <a id="s-1468771591"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-29836051ad"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-98df4e82b0"></a>`collection` | yes | [CollectionRootRef](#s-7ab492e53b) |  |
| <a id="s-7b2f5581a1"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-4101b8da2d"></a>`media_type` | no | anyOf=(type="string"; maxLength=255; minLength=1) \| (type="null"); default=null |  |
| <a id="s-19d2631334"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-21cc7ee87b"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-30512497ec"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0e753c1d24"></a>definition `BranchWorkBinding`

- <a id="s-37db0dd885"></a>`type`: `"object"`
- <a id="s-6f5609eb36"></a>`additionalProperties`: `false`
- <a id="s-d41871a439"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72cf2b8ddf"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9dc4aea3aa"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-af89fa023f"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-68ba6064dc"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-c1fe2f5e9b"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-7a021246a3"></a>definition `CollectionId`

- <a id="s-cbbe1da1b2"></a>`type`: `"integer"`
- <a id="s-8be40063a7"></a>`minimum`: `1`

##### <a id="s-7ab492e53b"></a>definition `CollectionRootRef`

- <a id="s-ac6642cc39"></a>`type`: `"object"`
- <a id="s-c28749e5c8"></a>`additionalProperties`: `false`
- <a id="s-9708bd603b"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b508208581"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a8e2a37582"></a>`collection_id` | yes | [CollectionId](#s-7a021246a3) |  |
| <a id="s-441b903fa2"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-72053cc702"></a>definition `ControllerEvidence`

- <a id="s-f3b6cf6310"></a>`type`: `"object"`
- <a id="s-04fb01ec3d"></a>`additionalProperties`: `false`
- <a id="s-82b36ccef6"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a44846c92c"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-13d7482557"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-038720f607) |  |
| <a id="s-0f05d41a65"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-133e2a7113"></a>definition `EffectPlan`

- <a id="s-edd090f3b0"></a>`type`: `"object"`
- <a id="s-aa084f15fc"></a>`additionalProperties`: `false`
- <a id="s-0a2e3f7b86"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-44c38bc308"></a>`inputs` | yes | [TargetInputAuthority](#s-9f6926eb08) |  |
| <a id="s-e37e9977ea"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-a923ee230c"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-2de28ab1e3"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-da1ea5b128"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bfe5cbe003"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-96129f6825"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-301a907da5"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5e540976af"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-323859a2c1"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |

##### <a id="s-9f76caaf2a"></a>definition `EvaluationBinding`

- <a id="s-26d7b3565a"></a>`type`: `"object"`
- <a id="s-8f4d135a2e"></a>`additionalProperties`: `false`
- <a id="s-342ee34b3b"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dc5421af92"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ca7f5f72e6"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1b2e1c1be7"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-88ef23c921"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-038720f607"></a>definition `ExecutionEnvelope`

- <a id="s-4529fc1880"></a>`type`: `"object"`
- <a id="s-a85d186ca0"></a>`additionalProperties`: `false`
- <a id="s-1c39529c14"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-647971046d"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-0c8ea40958"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-81ad0d63dc"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-afcfdd70c6"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-35f93d2a82"></a>`target_plan` | yes | [TargetPlanBinding](#s-bd9a2f7c37) |  |
| <a id="s-cf1d3cc070"></a>`workflow_plan` | yes | [WorkflowPlan](#s-45e6602b03) |  |

##### <a id="s-d0ae9252d9"></a>definition `JoinWorkBinding`

- <a id="s-fd4ea8812d"></a>`type`: `"object"`
- <a id="s-90f59a6237"></a>`additionalProperties`: `false`
- <a id="s-d8f9fa4306"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0728f00d33"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5147b86b6c"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-8c04a53bed"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-892541caca)); minItems=2 |  |
| <a id="s-975ce880bb"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-892541caca"></a>definition `JoinWorkMemberBinding`

- <a id="s-220bce5eb4"></a>`type`: `"object"`
- <a id="s-c70fabad75"></a>`additionalProperties`: `false`
- <a id="s-4c6442ac27"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7a014e6444"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3f19ee0bfd"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-474ff8afd2"></a>`producer_settlement_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |
| <a id="s-b95ec55154"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5cb95977fa"></a>definition `JsonSchemaDocument`

- <a id="s-20a9e0b01e"></a>`type`: `"object"`
- <a id="s-b1b771252c"></a>`additionalProperties`: `false`
- <a id="s-0ccb203975"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83e268ec2c"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-5611283886"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-1d5009afc8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7a166cb658"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-c906e8ff9c"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-71f440f6f9"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-f0e8f8ac07"></a>definition `ObservationEvidence`

- <a id="s-7209691eba"></a>`type`: `"object"`
- <a id="s-0a0e94ae0b"></a>`additionalProperties`: `false`
- <a id="s-925306a9e7"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4510a838eb"></a>`request` | yes | [ObservationRequest](#s-d0e13fdfbd) |  |
| <a id="s-83294c0e87"></a>`result` | yes | [ObservationResult](#s-32b62c853b) |  |

##### <a id="s-bb4acf1a79"></a>definition `ObservationFailure`

- <a id="s-e1b81fcd5b"></a>`type`: `"object"`
- <a id="s-234895f6c2"></a>`additionalProperties`: `false`
- <a id="s-ec506d360d"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9b48b0cf04"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ca5940dd2c"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-17c20b31df"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-a91d9baade"></a>definition `ObservationInapplicable`

- <a id="s-88d9ef54ea"></a>`type`: `"object"`
- <a id="s-dd947c5599"></a>`additionalProperties`: `false`
- <a id="s-9d7a6defb2"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff75ac0d08"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9ca6c1068e"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-d0e13fdfbd"></a>definition `ObservationRequest`

- <a id="s-1dc97c5b95"></a>`type`: `"object"`
- <a id="s-e946dca56a"></a>`additionalProperties`: `false`
- <a id="s-bb00085499"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9b9ee18332"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-3b5443d255"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-ad72d342b1"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2ba779e453"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-54e3661e39"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9aae60e49c"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-3b3b59ae77"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-dd1aa4febe"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-55b625244f"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-bf5af25c73"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-9c4214d418)); minItems=1 |  |
| <a id="s-52999fcfdc"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-61c9921347"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-32b62c853b"></a>definition `ObservationResult`

- <a id="s-90dc59069d"></a>`type`: `"object"`
- <a id="s-1deb7ee0ff"></a>`additionalProperties`: `false`
- <a id="s-d5c88581bc"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b5687f5d24"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-2ac20a869d"></a>`facts` | no | anyOf=(type="object"; additionalProperties=([JsonValue](#s-71f440f6f9))) \| (type="null"); default=null |  |
| <a id="s-1ae173110f"></a>`facts_schema` | no | anyOf=([JsonSchemaDocument](#s-5cb95977fa)) \| (type="null"); default=null |  |
| <a id="s-332280dbdf"></a>`facts_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |
| <a id="s-dd30aae877"></a>`failure` | no | anyOf=([ObservationFailure](#s-bb4acf1a79)) \| (type="null"); default=null |  |
| <a id="s-91c267b018"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-eee7046b09"></a>`inapplicable` | no | anyOf=([ObservationInapplicable](#s-a91d9baade)) \| (type="null"); default=null |  |
| <a id="s-760d0bf6a0"></a>`observer` | yes | [ObserverImplementation](#s-f29ba2e6fa) |  |
| <a id="s-7b74d4aee1"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-49d82fe4c3"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-981d8fb242"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5bf2639a9d"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5840fbe084"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-291ac8023f"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-9c4214d418)); minItems=1 |  |

##### <a id="s-f29ba2e6fa"></a>definition `ObserverImplementation`

- <a id="s-65f2015c0a"></a>`type`: `"object"`
- <a id="s-b71cedb8a0"></a>`additionalProperties`: `false`
- <a id="s-9f9acfaae6"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ba188d383"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6ff28f0f02"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-39fe155bc1"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-b04811a601"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-5b3f01bf15"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-7e2ea6a762"></a>definition `OperationRef`

- <a id="s-710c6a6dde"></a>`type`: `"object"`
- <a id="s-c172d4bce4"></a>`additionalProperties`: `false`
- <a id="s-94d1852b2f"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0840d8ac94"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-04b9c9ddaf"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d7f2e769d4"></a>definition `RecipeRef`

- <a id="s-b8af82597b"></a>`type`: `"object"`
- <a id="s-b895f7e2a1"></a>`additionalProperties`: `false`
- <a id="s-2c9a783ab4"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-474ad7d72d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d428e8bc55"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-0de91dd07a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9f6926eb08"></a>definition `TargetInputAuthority`

- <a id="s-c2f8c35c10"></a>`type`: `"object"`
- <a id="s-551a07ca59"></a>`additionalProperties`: `false`
- <a id="s-b4fbc035a5"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-16e9ea0b75"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-413658dec0)); minItems=1 |  |
| <a id="s-025a536c71"></a>`selection` | yes | [ArtifactSelectionRef](#s-7ad6a47971) |  |

##### <a id="s-413658dec0"></a>definition `TargetInputRoleCount`

- <a id="s-a25c4658a8"></a>`type`: `"object"`
- <a id="s-0d118efa71"></a>`additionalProperties`: `false`
- <a id="s-d137b6b9e5"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95a210ce22"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-0cbf896064"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-56e270ccf3"></a>definition `TargetJobDeclaration`

- <a id="s-ac7bd37624"></a>`type`: `"object"`
- <a id="s-10a671e37c"></a>`additionalProperties`: `false`
- <a id="s-17de633408"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8b870a7f21"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-fed93a7bd3"></a>`controller_evidence` | yes | [ControllerEvidence](#s-72053cc702) |  |
| <a id="s-afb4f3218f"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-72365f2d30"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-798fd408c7"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"[EffectPlan](#s-133e2a7113)","stove0-transform-target/v1":"[TransformPlan](#s-eddb58c797)"},"propertyName":"protocol"}; oneOf=([TransformPlan](#s-eddb58c797)) \| ([EffectPlan](#s-133e2a7113)) |  |
| <a id="s-c1d0a5e58b"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

##### <a id="s-bd9a2f7c37"></a>definition `TargetPlanBinding`

- <a id="s-5b4d0b8a78"></a>`type`: `"object"`
- <a id="s-5c6d6c26f0"></a>`additionalProperties`: `false`
- <a id="s-5c7c85a6b9"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-97bc929ca8"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b042deec1e"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-0e48a89747"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-796f17b826"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fd9b34f4f6"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-842ee8f087"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-eddb58c797"></a>definition `TransformPlan`

- <a id="s-3c87e7ba78"></a>`type`: `"object"`
- <a id="s-f738ecaea2"></a>`additionalProperties`: `false`
- <a id="s-00025131ff"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ebdaf6a8f2"></a>`inputs` | yes | [TargetInputAuthority](#s-9f6926eb08) |  |
| <a id="s-a1360626e1"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-2d75247a48"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-4751bbf150"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8f0ec511f3"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-54c90ddf33"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1f8417c574"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-86315af3e6"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2a5fec1b49"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2f56400347"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |

##### <a id="s-8e0976835d"></a>definition `WorkIdentity`

- <a id="s-412c5e646c"></a>`type`: `"object"`
- <a id="s-ac4fea3b52"></a>`additionalProperties`: `false`
- <a id="s-1bac731c4d"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f91242d520"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-cdf2e23ef3"></a>`evaluation` | no | anyOf=([EvaluationBinding](#s-9f76caaf2a)) \| (type="null"); default=null |  |
| <a id="s-d85a713bbd"></a>`fork_join` | no | anyOf=(discriminator={"mapping":{"branch":"[BranchWorkBinding](#s-0e753c1d24)","join":"[JoinWorkBinding](#s-d0ae9252d9)"},"propertyName":"kind"}; oneOf=([BranchWorkBinding](#s-0e753c1d24)) \| ([JoinWorkBinding](#s-d0ae9252d9))) \| (type="null"); default=null |  |
| <a id="s-2f93b98021"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-127bb410b5"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-7ab492e53b)); minItems=1 |  |
| <a id="s-29422d31ae"></a>`recipe` | yes | [RecipeRef](#s-d7f2e769d4) |  |
| <a id="s-4d7ef6e3e5"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-45e6602b03"></a>definition `WorkflowPlan`

- <a id="s-38db981d7a"></a>`type`: `"object"`
- <a id="s-ca5b900780"></a>`additionalProperties`: `false`
- <a id="s-46f55c7e11"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4f6ff9753e"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-afb8e09d33"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-96dd10c5a0"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-f0e8f8ac07)) |  |
| <a id="s-72488f00e1"></a>`operation` | yes | [OperationRef](#s-7e2ea6a762) |  |
| <a id="s-a820ac3b17"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-5c7c1a75a7"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-71f440f6f9)) |  |
| <a id="s-b45e7d1e1a"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-550da6fb9a"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-fc4f26fe8c"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-3b982609e1"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d64ee25dbb"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-acedc0efa3"></a>`work` | yes | [WorkIdentity](#s-8e0976835d) |  |
| <a id="s-95603affef"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [verify_digest](stove0-target-support-acceptedtargetjob-verify-digest.md)

## Governing policies

- <a id="pa-39a0a37e45"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.AcceptedTargetJob`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4575c37254c08ef7bc5ec8bfb0eab3197aeb04d016dc0af607429977b0f51493 -->

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
    "signature": "\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "AcceptedTargetJob",
  "unit": "export"
}
```

</details>
