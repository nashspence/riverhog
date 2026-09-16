# stove0_target_support.TargetJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobrequest:8abb832635 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3cc758c929"></a>
- <a id="s-7f57d04aa1"></a>`distribution`: `stove0-target-support`
- <a id="s-5101141394"></a>`module`: `stove0_target_support`
- <a id="s-2990e7eb13"></a>`name`: `TargetJobRequest`
- <a id="s-4f01aa6e2f"></a>`unit`: `export`

### Declared structure

- <a id="s-c36eb04b5e"></a>`kind`: `"class"`
- <a id="s-c36742704e"></a>`signature`: `"\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, runtime: stove0_target_protocol.protocol.TargetRuntimeAuthority, callback_access: stove0_target_protocol.protocol.TargetCallbackAccess, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-98f668de06"></a>

- <a id="s-85103989ff"></a>`type`: `"object"`
- <a id="s-f8cb06a3dd"></a>`additionalProperties`: `false`
- <a id="s-6adc3b6336"></a>`required`: `["declaration","runtime","callback_access","request_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5e98c7e04c"></a>`callback_access` | yes | [TargetCallbackAccess](#s-86928cf8f7) |  |
| <a id="s-fd3b0ebbd9"></a>`declaration` | yes | [TargetJobDeclaration](#s-d41b74991c) |  |
| <a id="s-c8399e2ef2"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4d2398805a"></a>`runtime` | yes | [TargetRuntimeAuthority](#s-021bc7b293) |  |

##### Definitions

- [ArtifactSelectionRef](#s-f0e0f67912)
- [ArtifactSubject](#s-0a7a50d044)
- [BranchWorkBinding](#s-1cca51a6da)
- [CollectionId](#s-b5909be01a)
- [CollectionRootRef](#s-8e6db4b460)
- [ControllerEvidence](#s-ace24c0400)
- [EffectPlan](#s-cf3c6fe504)
- [EvaluationBinding](#s-16c39e0f8b)
- [ExecutionEnvelope](#s-4a0010d508)
- [JoinWorkBinding](#s-700df3002d)
- [JoinWorkMemberBinding](#s-beca41fd24)
- [JsonSchemaDocument](#s-97c5420f5c)
- [JsonValue](#s-e0da709161)
- [ObservationEvidence](#s-bf45f46b7a)
- [ObservationFailure](#s-51a56e7eef)
- [ObservationInapplicable](#s-133e7d890b)
- [ObservationRequest](#s-e0818bfe7c)
- [ObservationResult](#s-704af23928)
- [ObserverImplementation](#s-b38c162803)
- [OperationRef](#s-6fcd94a459)
- [RecipeRef](#s-d0648a228e)
- [TargetCallbackAccess](#s-86928cf8f7)
- [TargetInputAuthority](#s-a66744b1ee)
- [TargetInputRoleCount](#s-9e9860bbcd)
- [TargetJobDeclaration](#s-d41b74991c)
- [TargetPlanBinding](#s-cd772f8a9b)
- [TargetRuntimeAuthority](#s-021bc7b293)
- [TransformPlan](#s-8585693cdb)
- [WorkIdentity](#s-c0d27ef4d0)
- [WorkflowPlan](#s-44a17783ee)

##### <a id="s-f0e0f67912"></a>definition `ArtifactSelectionRef`

- <a id="s-ce71f0f4ac"></a>`type`: `"object"`
- <a id="s-c7414054fd"></a>`additionalProperties`: `false`
- <a id="s-dad198ee62"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c32941e8ae"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-f3ae5ea206"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f75450b6f5"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-0a7a50d044"></a>definition `ArtifactSubject`

- <a id="s-1d839339ca"></a>`type`: `"object"`
- <a id="s-0a7c8a0868"></a>`additionalProperties`: `false`
- <a id="s-69d79c512e"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-32f8023e6e"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-fe15fb074a"></a>`collection` | yes | [CollectionRootRef](#s-8e6db4b460) |  |
| <a id="s-5364a69fd8"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-9287a9b3df"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-6ccad6fbe6"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-dfc5ca21af"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-755f7a2d49"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1cca51a6da"></a>definition `BranchWorkBinding`

- <a id="s-adb5cfdaae"></a>`type`: `"object"`
- <a id="s-f91aa17e76"></a>`additionalProperties`: `false`
- <a id="s-cc9484a422"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c485fd1578"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-919f3aad52"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f47f9392d7"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6120014311"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-d10d0bb21b"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b5909be01a"></a>definition `CollectionId`

- <a id="s-68ed428c34"></a>`type`: `"integer"`
- <a id="s-681c2fc9db"></a>`minimum`: `1`

##### <a id="s-8e6db4b460"></a>definition `CollectionRootRef`

- <a id="s-eb8560665e"></a>`type`: `"object"`
- <a id="s-db7605916e"></a>`additionalProperties`: `false`
- <a id="s-1523cc3896"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f2a0e9318b"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-da0870e7a2"></a>`collection_id` | yes | [CollectionId](#s-b5909be01a) |  |
| <a id="s-af139bbf5e"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ace24c0400"></a>definition `ControllerEvidence`

- <a id="s-82ca980bb6"></a>`type`: `"object"`
- <a id="s-9b91c68f86"></a>`additionalProperties`: `false`
- <a id="s-1e29a397a8"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1704768065"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-60f84d4d5d"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-4a0010d508) |  |
| <a id="s-fcd2e5bbc5"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-cf3c6fe504"></a>definition `EffectPlan`

- <a id="s-e2519696de"></a>`type`: `"object"`
- <a id="s-7cdda6ec55"></a>`additionalProperties`: `false`
- <a id="s-f1e7e8bd95"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-548eb1f576"></a>`inputs` | yes | [TargetInputAuthority](#s-a66744b1ee) |  |
| <a id="s-a6355c6ff0"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-1af027e215"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-03d911f290"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b283c12870"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a37666ed5a"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b585da8c7"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-badbc342c8"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aaad1d7cef"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2b78bac14a"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |

##### <a id="s-16c39e0f8b"></a>definition `EvaluationBinding`

- <a id="s-ea52e43378"></a>`type`: `"object"`
- <a id="s-353493c76d"></a>`additionalProperties`: `false`
- <a id="s-fa0875d6e2"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a0b523ae0a"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-946efbfcad"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cc1f05fee1"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-7b0979ed3f"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-4a0010d508"></a>definition `ExecutionEnvelope`

- <a id="s-9f1258b0e4"></a>`type`: `"object"`
- <a id="s-73fe639bed"></a>`additionalProperties`: `false`
- <a id="s-a5ec936d38"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-484c917183"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-a8b0625d83"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c8365d4213"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-07b62bc150"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-107ca4a5d8"></a>`target_plan` | yes | [TargetPlanBinding](#s-cd772f8a9b) |  |
| <a id="s-ce243b09bb"></a>`workflow_plan` | yes | [WorkflowPlan](#s-44a17783ee) |  |

##### <a id="s-700df3002d"></a>definition `JoinWorkBinding`

- <a id="s-9e819d42ad"></a>`type`: `"object"`
- <a id="s-b5a90cbaed"></a>`additionalProperties`: `false`
- <a id="s-513dfc8e91"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d65fd8988"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-51ca773331"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-d4a2105a2d"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-beca41fd24)); minItems=2 |  |
| <a id="s-a6a6cc705b"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-beca41fd24"></a>definition `JoinWorkMemberBinding`

- <a id="s-133c54ab37"></a>`type`: `"object"`
- <a id="s-312001a56d"></a>`additionalProperties`: `false`
- <a id="s-fcac4118a3"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c0c753e49"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c2d2d724c6"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b8ca0ef378"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a3d0965696"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-97c5420f5c"></a>definition `JsonSchemaDocument`

- <a id="s-6f4291e048"></a>`type`: `"object"`
- <a id="s-7f42087b00"></a>`additionalProperties`: `false`
- <a id="s-c2154e908f"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f7359ad3f7"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-fb95eb8517"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-3c35e076a8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1a4c207feb"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-bd98e71e78"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e0da709161"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-bf45f46b7a"></a>definition `ObservationEvidence`

- <a id="s-e476b6ad07"></a>`type`: `"object"`
- <a id="s-294f7d7303"></a>`additionalProperties`: `false`
- <a id="s-0172b308e1"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ddc0f7915b"></a>`request` | yes | [ObservationRequest](#s-e0818bfe7c) |  |
| <a id="s-d87d986e4a"></a>`result` | yes | [ObservationResult](#s-704af23928) |  |

##### <a id="s-51a56e7eef"></a>definition `ObservationFailure`

- <a id="s-236124e7b0"></a>`type`: `"object"`
- <a id="s-737807cd6c"></a>`additionalProperties`: `false`
- <a id="s-967fde6437"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-98cfc311ad"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dff459b47c"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-bcfe7460ab"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-133e7d890b"></a>definition `ObservationInapplicable`

- <a id="s-d00cd4661b"></a>`type`: `"object"`
- <a id="s-4473c543d2"></a>`additionalProperties`: `false`
- <a id="s-00bdb6fab1"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e21340128d"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1855e20150"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-e0818bfe7c"></a>definition `ObservationRequest`

- <a id="s-de6bd95c1f"></a>`type`: `"object"`
- <a id="s-c3f6ad4d0d"></a>`additionalProperties`: `false`
- <a id="s-694e3dcdc5"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-42214d989b"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-a530737cbc"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-f5b43dafd0"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1b55539c3c"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f4eb81df93"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1e4c0b4609"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-f2123d9331"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-57f9dcce68"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4cc46694a2"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-9d471986d9"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-0a7a50d044)); minItems=1 |  |
| <a id="s-5effc1e970"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-10b642b5fb"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-704af23928"></a>definition `ObservationResult`

- <a id="s-ec62a4b18d"></a>`type`: `"object"`
- <a id="s-4e90a79caa"></a>`additionalProperties`: `false`
- <a id="s-728dc10962"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6fc5b92b4f"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-2b55442a34"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-e0da709161))); (type="null")]; default=null |  |
| <a id="s-91eebbc9de"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-97c5420f5c)); (type="null")]; default=null |  |
| <a id="s-be11c5abd7"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-aafec9e366"></a>`failure` | no | anyOf=[([ObservationFailure](#s-51a56e7eef)); (type="null")]; default=null |  |
| <a id="s-1030b62db0"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-a25c92f639"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-133e7d890b)); (type="null")]; default=null |  |
| <a id="s-8b1f99e07f"></a>`observer` | yes | [ObserverImplementation](#s-b38c162803) |  |
| <a id="s-febc52cf6a"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6135f637f8"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-666e832aed"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b3290b9d81"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2026efee6d"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-0e72c2271a"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-0a7a50d044)); minItems=1 |  |

##### <a id="s-b38c162803"></a>definition `ObserverImplementation`

- <a id="s-94e37f0174"></a>`type`: `"object"`
- <a id="s-fb9ab2aa26"></a>`additionalProperties`: `false`
- <a id="s-85ce43cb01"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dddadb42ce"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c15331bbcc"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-09b8fbd3c6"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-0e486f0f5d"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-606f49b0d7"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-6fcd94a459"></a>definition `OperationRef`

- <a id="s-85d9b1302c"></a>`type`: `"object"`
- <a id="s-8dc84bd626"></a>`additionalProperties`: `false`
- <a id="s-4a08b47fbb"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f242b31163"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-eb4a25074b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d0648a228e"></a>definition `RecipeRef`

- <a id="s-a85bd7e950"></a>`type`: `"object"`
- <a id="s-6d4395a892"></a>`additionalProperties`: `false`
- <a id="s-2025909caf"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db5cecc3d9"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-535f38e2a8"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-e42eaaf73f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-86928cf8f7"></a>definition `TargetCallbackAccess`

- <a id="s-cc2361ea8d"></a>`type`: `"object"`
- <a id="s-0811d96d28"></a>`additionalProperties`: `false`
- <a id="s-32b8ce44d2"></a>`required`: `["stove0_base_url","token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c5289e1634"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-7d74df1564"></a>`stove0_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-f12ec07107"></a>`token` | yes | type="string"; maxLength=4096; minLength=1 |  |

##### <a id="s-a66744b1ee"></a>definition `TargetInputAuthority`

- <a id="s-f6503df2a1"></a>`type`: `"object"`
- <a id="s-bf97fd998f"></a>`additionalProperties`: `false`
- <a id="s-083b6f41e7"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8d75b08830"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-9e9860bbcd)); minItems=1 |  |
| <a id="s-88c924f202"></a>`selection` | yes | [ArtifactSelectionRef](#s-f0e0f67912) |  |

##### <a id="s-9e9860bbcd"></a>definition `TargetInputRoleCount`

- <a id="s-d69b520b20"></a>`type`: `"object"`
- <a id="s-f231f2c70c"></a>`additionalProperties`: `false`
- <a id="s-0c6ca5562d"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38dfcd6d1d"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-d2be508153"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-d41b74991c"></a>definition `TargetJobDeclaration`

- <a id="s-0a696abb62"></a>`type`: `"object"`
- <a id="s-36c451dad3"></a>`additionalProperties`: `false`
- <a id="s-5573022d4c"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6368cada9e"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-ef2cf0df9d"></a>`controller_evidence` | yes | [ControllerEvidence](#s-ace24c0400) |  |
| <a id="s-b504a2f357"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-22d7648515"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c6e3fd2f09"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-8585693cdb)); ([EffectPlan](#s-cf3c6fe504))] |  |
| <a id="s-29341d7e3c"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

##### <a id="s-cd772f8a9b"></a>definition `TargetPlanBinding`

- <a id="s-ed76f33370"></a>`type`: `"object"`
- <a id="s-2e4cd59884"></a>`additionalProperties`: `false`
- <a id="s-a7c1c99ae8"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5aa15b4a3d"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-553fc28dc8"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-14206cbe42"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-953be369a3"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-960685753c"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-65de9da535"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-021bc7b293"></a>definition `TargetRuntimeAuthority`

- <a id="s-a35afdb8ab"></a>`type`: `"object"`
- <a id="s-7cd9335336"></a>`additionalProperties`: `false`
- <a id="s-a0e2caa0fc"></a>`required`: `["riverhog_base_url","capability_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5f4832e0b3"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-d3b50c2b65"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-25a802d16f"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-542846c409"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### <a id="s-8585693cdb"></a>definition `TransformPlan`

- <a id="s-a5c3fea4cc"></a>`type`: `"object"`
- <a id="s-5f874f25b6"></a>`additionalProperties`: `false`
- <a id="s-8a3af88aea"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-877fd7141a"></a>`inputs` | yes | [TargetInputAuthority](#s-a66744b1ee) |  |
| <a id="s-fccd10ea68"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-2f765d6f5f"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-7e12afd737"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f8598b1964"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b6bed607d0"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c6651769b7"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-8b9dc4ddbb"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-06b7987189"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6eda4a8f6e"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |

##### <a id="s-c0d27ef4d0"></a>definition `WorkIdentity`

- <a id="s-3e8ec58502"></a>`type`: `"object"`
- <a id="s-6b8ba52216"></a>`additionalProperties`: `false`
- <a id="s-8bc28c29a2"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-96e9310f4b"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-90b6cd4c1c"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-16c39e0f8b)); (type="null")]; default=null |  |
| <a id="s-0d912a78a4"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-1cca51a6da)); ([JoinWorkBinding](#s-700df3002d))]); (type="null")]; default=null |  |
| <a id="s-3d77a66871"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-f1a95a5b0f"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-8e6db4b460)); minItems=1 |  |
| <a id="s-a90e053a10"></a>`recipe` | yes | [RecipeRef](#s-d0648a228e) |  |
| <a id="s-624c749c7a"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-44a17783ee"></a>definition `WorkflowPlan`

- <a id="s-1ed42e62d3"></a>`type`: `"object"`
- <a id="s-ddd4d87492"></a>`additionalProperties`: `false`
- <a id="s-0e7870c10f"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c972deffd5"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-c70577efa3"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-93eb49250c"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-bf45f46b7a)) |  |
| <a id="s-6e5fe082e9"></a>`operation` | yes | [OperationRef](#s-6fcd94a459) |  |
| <a id="s-384fb6ffee"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-9c0fa74fda"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e0da709161)) |  |
| <a id="s-5e7688eac2"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-bd570632d5"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-b16477ce17"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-7e361f19f8"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3f7a8155dc"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-dedc8a3a12"></a>`work` | yes | [WorkIdentity](#s-c0d27ef4d0) |  |
| <a id="s-b517839206"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [accepted](stove0-target-support-targetjobrequest-accepted.md)
- [seal](stove0-target-support-targetjobrequest-seal.md)
- [verify_digest](stove0-target-support-targetjobrequest-verify-digest.md)

## Governing policies

- <a id="pa-1d82f531c1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60755934ee6f9fb821fbfedbaee303096c8988c0de15c4db81bbe975a5b2da4a -->

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
        "TargetCallbackAccess": {
          "additionalProperties": false,
          "properties": {
            "allow_insecure_http": {
              "default": false,
              "type": "boolean"
            },
            "stove0_base_url": {
              "maxLength": 2048,
              "minLength": 1,
              "type": "string"
            },
            "token": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "stove0_base_url",
            "token"
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
        "TargetRuntimeAuthority": {
          "additionalProperties": false,
          "properties": {
            "allow_insecure_http": {
              "default": false,
              "type": "boolean"
            },
            "capability_token": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "riverhog_base_url": {
              "maxLength": 2048,
              "minLength": 1,
              "type": "string"
            },
            "transport": {
              "const": "riverhog-capability/v1",
              "default": "riverhog-capability/v1",
              "type": "string"
            }
          },
          "required": [
            "riverhog_base_url",
            "capability_token"
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
        "callback_access": {
          "$ref": "#/$defs/TargetCallbackAccess"
        },
        "declaration": {
          "$ref": "#/$defs/TargetJobDeclaration"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
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
      "type": "object"
    },
    "signature": "\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, runtime: stove0_target_protocol.protocol.TargetRuntimeAuthority, callback_access: stove0_target_protocol.protocol.TargetCallbackAccess, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetJobRequest",
  "unit": "export"
}
```

</details>
