# stove0_target_support.TargetConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetconformanceresult:6a239f5efb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50258ec12f"></a>
- <a id="s-00ad0d6a6e"></a>`distribution`: `stove0-target-support`
- <a id="s-bbc1b58eb4"></a>`module`: `stove0_target_support`
- <a id="s-7fb0038d8f"></a>`name`: `TargetConformanceResult`
- <a id="s-85f5f66d61"></a>`unit`: `export`

### Declared structure

- <a id="s-9598a15050"></a>`kind`: `"class"`
- <a id="s-7fe1afaf12"></a>`signature`: `"\"(*, format: Literal['stove0-target-conformance-result/v1'] = 'stove0-target-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], target: stove0_target_protocol.protocol.TargetContract, coverage: stove0_target_support.conformance.TargetConformanceCoverage, operations: tuple[stove0_target_support.conformance.TargetOperationConformance, ...], operation_evidence: tuple[stove0_target_support.conformance.TargetOperationConformanceEvidence, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-6c31c89853"></a>

- <a id="s-fa9e1eabba"></a>`type`: `"object"`
- <a id="s-cf7b813aa2"></a>`additionalProperties`: `false`
- <a id="s-7e2d4f0738"></a>`required`: `["status","target","coverage","operations"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f31ab7371f"></a>`coverage` | yes | [TargetConformanceCoverage](#s-75f71bb00c) |  |
| <a id="s-a16e783941"></a>`format` | no | type="string"; const="stove0-target-conformance-result/v1"; default="stove0-target-conformance-result/v1" |  |
| <a id="s-758560c0a4"></a>`operation_evidence` | no | type="array"; default=[]; items=([TargetOperationConformanceEvidence](#s-b8e5a7e203)) |  |
| <a id="s-213049806b"></a>`operations` | yes | type="array"; items=([TargetOperationConformance](#s-4035eff85f)) |  |
| <a id="s-4431398fdb"></a>`status` | yes | type="string"; enum=["conformant","partially-exercised","inspected"] |  |
| <a id="s-9066cf1380"></a>`target` | yes | [TargetContract](#s-3c67c2c42b) |  |

##### Definitions

- [AcceptedTargetJob](#s-b81dfee6ee)
- [ArtifactDispositionSetIdentity](#s-9becce0ff4)
- [ArtifactSelectionRef](#s-3531919e69)
- [ArtifactSubject](#s-ff4e3408d2)
- [BranchWorkBinding](#s-a07879b74f)
- [CollectionId](#s-72a858e21c)
- [CollectionRootRef](#s-271fc90c9e)
- [ControllerEvidence](#s-d5dfdfd545)
- [EffectPlan](#s-c6acdd3519)
- [EvaluationBinding](#s-34c9753b7d)
- [ExecutionEnvelope](#s-34ed9fa3ee)
- [ExternalEffectReceipt](#s-b6a8e45a6b)
- [InputArtifactContract](#s-3d68dabffa)
- [JoinWorkBinding](#s-d521986268)
- [JoinWorkMemberBinding](#s-9424b624df)
- [JsonSchemaDocument](#s-cd5f353a8b)
- [JsonValue](#s-770b4fc958)
- [ObservationEvidence](#s-6cece00f01)
- [ObservationFailure](#s-f923916698)
- [ObservationInapplicable](#s-7e82c96732)
- [ObservationRequest](#s-4bb3ae3f4c)
- [ObservationResult](#s-919e0af467)
- [ObserverImplementation](#s-f0d48ddd05)
- [OperationContract](#s-4c0679d4d1)
- [OperationRef](#s-f742f4b080)
- [OutputArtifactContract](#s-1999c8b233)
- [OutputArtifactRoleCount](#s-71523c9823)
- [OutputArtifactSetIdentity](#s-4f3acd771d)
- [OutputCollectionRef](#s-ca421464d4)
- [RecipeRef](#s-ae620e4656)
- [SemanticIntentConformanceVector](#s-ad9607d3a8)
- [SemanticIntentConformanceVectors](#s-61324a766c)
- [SemanticValidationProfile](#s-de2c4c6a35)
- [TargetConformanceCoverage](#s-75f71bb00c)
- [TargetContract](#s-3c67c2c42b)
- [TargetExecutionEvidence](#s-b8dbfa9f81)
- [TargetFailure](#s-5516bc8a3d)
- [TargetInapplicable](#s-53ba199af7)
- [TargetInputAuthority](#s-f5a0f7cf00)
- [TargetInputRoleCount](#s-ae106f8ba9)
- [TargetJobDeclaration](#s-37eb2902e8)
- [TargetJobStatus](#s-d6605b4451)
- [TargetOperationConformance](#s-4035eff85f)
- [TargetOperationConformanceEvidence](#s-b8e5a7e203)
- [TargetOperationSupport](#s-e2738e9866)
- [TargetPlanBinding](#s-2e6e2ab53c)
- [TargetPreflightRequest](#s-e2c72ccf93)
- [TargetPreflightResponse](#s-7addb846c3)
- [TargetProductionAuthority](#s-3ada96cfa1)
- [TargetProgress](#s-41f70c9b28)
- [TargetSemanticConformance](#s-9219516899)
- [TransformPlan](#s-0e0545f239)
- [WorkIdentity](#s-52083e66b0)
- [WorkflowPlan](#s-58ebb24bfd)

##### <a id="s-b81dfee6ee"></a>definition `AcceptedTargetJob`

- <a id="s-369d901705"></a>`type`: `"object"`
- <a id="s-e3f5298fe8"></a>`additionalProperties`: `false`
- <a id="s-00e5d8237b"></a>`required`: `["declaration","request_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-73c768ea20"></a>`declaration` | yes | [TargetJobDeclaration](#s-37eb2902e8) |  |
| <a id="s-b384e3cfc9"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9becce0ff4"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-563ee81d25"></a>`type`: `"object"`
- <a id="s-30e881bd85"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-65d8861b0e"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-f1943f2990"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-2cc0e211f5"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-19a475205d"></a>`sha256` | yes | type="string" |  |

##### <a id="s-3531919e69"></a>definition `ArtifactSelectionRef`

- <a id="s-c40f02d0eb"></a>`type`: `"object"`
- <a id="s-2d11a46601"></a>`additionalProperties`: `false`
- <a id="s-498779ec7b"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a85826284a"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-8cf093e7c4"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e8344fc068"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-ff4e3408d2"></a>definition `ArtifactSubject`

- <a id="s-f5a2cb138f"></a>`type`: `"object"`
- <a id="s-b6c40e9e6a"></a>`additionalProperties`: `false`
- <a id="s-f11c699b91"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-85437563e7"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-32429f9a3d"></a>`collection` | yes | [CollectionRootRef](#s-271fc90c9e) |  |
| <a id="s-f3d5eeec77"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-a48163e3cc"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-bc7f53b966"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-14fdd21163"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5ad6a896d5"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a07879b74f"></a>definition `BranchWorkBinding`

- <a id="s-4c9c9a51b8"></a>`type`: `"object"`
- <a id="s-4c2fa52d6d"></a>`additionalProperties`: `false`
- <a id="s-55e5157b7b"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e382cecf5d"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9bb47fea2d"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7bfceb9a0e"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ee31250861"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-5aeb912d46"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-72a858e21c"></a>definition `CollectionId`

- <a id="s-af9bf41aa1"></a>`type`: `"integer"`
- <a id="s-f0aef837d2"></a>`minimum`: `1`

##### <a id="s-271fc90c9e"></a>definition `CollectionRootRef`

- <a id="s-28e2f415cc"></a>`type`: `"object"`
- <a id="s-c07a06d0d3"></a>`additionalProperties`: `false`
- <a id="s-8ac14f06f4"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8fffd76912"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-11d7bf5f74"></a>`collection_id` | yes | [CollectionId](#s-72a858e21c) |  |
| <a id="s-d6f27fc1d5"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d5dfdfd545"></a>definition `ControllerEvidence`

- <a id="s-167dbdba7e"></a>`type`: `"object"`
- <a id="s-a40cbf571e"></a>`additionalProperties`: `false`
- <a id="s-e0d002e6d1"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f69c13c629"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-341cb8f5bf"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-34ed9fa3ee) |  |
| <a id="s-f40b6383b0"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-c6acdd3519"></a>definition `EffectPlan`

- <a id="s-072ecf07b7"></a>`type`: `"object"`
- <a id="s-20de4414a8"></a>`additionalProperties`: `false`
- <a id="s-4210364006"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b02b234e5"></a>`inputs` | yes | [TargetInputAuthority](#s-f5a0f7cf00) |  |
| <a id="s-381451fb28"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-cc0b765f24"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-550c7fa92f"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-10bf890304"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6b069a787c"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b30f11e8e1"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-a7e12f67a1"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6e661a2687"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c80286d73d"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |

##### <a id="s-34c9753b7d"></a>definition `EvaluationBinding`

- <a id="s-1288a6e5c7"></a>`type`: `"object"`
- <a id="s-8364e6d706"></a>`additionalProperties`: `false`
- <a id="s-40d73c110e"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cd6f346660"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-211ed8a439"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb0452840d"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-9cad1816ae"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-34ed9fa3ee"></a>definition `ExecutionEnvelope`

- <a id="s-b6011295c6"></a>`type`: `"object"`
- <a id="s-6940803b24"></a>`additionalProperties`: `false`
- <a id="s-2cf3ea89eb"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d58a80cf35"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-493a999f75"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9ade84be03"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-cce920c9f6"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-528a6c35cb"></a>`target_plan` | yes | [TargetPlanBinding](#s-2e6e2ab53c) |  |
| <a id="s-0917fcba42"></a>`workflow_plan` | yes | [WorkflowPlan](#s-58ebb24bfd) |  |

##### <a id="s-b6a8e45a6b"></a>definition `ExternalEffectReceipt`

- <a id="s-6fedcd2c5c"></a>`type`: `"object"`
- <a id="s-7afee62d75"></a>`additionalProperties`: `false`
- <a id="s-1499942a35"></a>`required`: `["job_id","request_sha256","target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-594a65ee37"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7d12a6a5eb"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1" |  |
| <a id="s-3d3f37f338"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-805d24922e"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4d6f48c58a"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b597c5ba96"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5581b8f488"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-67b5bd7553"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-ea7d5a0419"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3d68dabffa"></a>definition `InputArtifactContract`

- <a id="s-626f270f8f"></a>`type`: `"object"`
- <a id="s-380cd91c0f"></a>`additionalProperties`: `false`
- <a id="s-d8790310f0"></a>`required`: `["role"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1b70652adc"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null |  |
| <a id="s-e6ca2ede14"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-174f07a978"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-73249a7dfa"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-d521986268"></a>definition `JoinWorkBinding`

- <a id="s-610738ab8a"></a>`type`: `"object"`
- <a id="s-9eebb47f96"></a>`additionalProperties`: `false`
- <a id="s-47418f10df"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e9511f7137"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2be9be17d6"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-48b68d0bdd"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-9424b624df)); minItems=2 |  |
| <a id="s-b6ad3efc37"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9424b624df"></a>definition `JoinWorkMemberBinding`

- <a id="s-8d1f9c91d9"></a>`type`: `"object"`
- <a id="s-1f506a6782"></a>`additionalProperties`: `false`
- <a id="s-e82d303a38"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6da4ccef8e"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1c9a4722c0"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f9fb39d5e0"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-98166becff"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-cd5f353a8b"></a>definition `JsonSchemaDocument`

- <a id="s-e9e0061652"></a>`type`: `"object"`
- <a id="s-4f0458ce52"></a>`additionalProperties`: `false`
- <a id="s-ebdace548f"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7a0ad7876e"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-0efa3dfbcd"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-887071b75f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5fc417d759"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-f320676183"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-770b4fc958"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-6cece00f01"></a>definition `ObservationEvidence`

- <a id="s-43582c7d46"></a>`type`: `"object"`
- <a id="s-4562d8e157"></a>`additionalProperties`: `false`
- <a id="s-52e33dfa27"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-210fd04738"></a>`request` | yes | [ObservationRequest](#s-4bb3ae3f4c) |  |
| <a id="s-65d390f63b"></a>`result` | yes | [ObservationResult](#s-919e0af467) |  |

##### <a id="s-f923916698"></a>definition `ObservationFailure`

- <a id="s-1c258a5e68"></a>`type`: `"object"`
- <a id="s-291e84cfac"></a>`additionalProperties`: `false`
- <a id="s-0ab635503f"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-633077f022"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-84d050d278"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-fa97fd2d4c"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-7e82c96732"></a>definition `ObservationInapplicable`

- <a id="s-1afc7e89b8"></a>`type`: `"object"`
- <a id="s-98d4727ccb"></a>`additionalProperties`: `false`
- <a id="s-954070b609"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c92855357e"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-91e429ec7f"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-4bb3ae3f4c"></a>definition `ObservationRequest`

- <a id="s-13144ac95d"></a>`type`: `"object"`
- <a id="s-f4cf6414f1"></a>`additionalProperties`: `false`
- <a id="s-7838cd89b1"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-273dbd093c"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-cd5cfa3e81"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-f0b31c3573"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8eb8528853"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-13d6c87f20"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-73f94913e8"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-57f117f284"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-8e352f521e"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3346dbabed"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-0b88b554ca"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ff4e3408d2)); minItems=1 |  |
| <a id="s-297e2405ad"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-ce7966f9fd"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-919e0af467"></a>definition `ObservationResult`

- <a id="s-1dc9ce13ce"></a>`type`: `"object"`
- <a id="s-cc773978fc"></a>`additionalProperties`: `false`
- <a id="s-102a28e3d3"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95a5a9fa87"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-9f90276722"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-770b4fc958))); (type="null")]; default=null |  |
| <a id="s-fa1a12401a"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-cd5f353a8b)); (type="null")]; default=null |  |
| <a id="s-4ebe8ea876"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-35538de894"></a>`failure` | no | anyOf=[([ObservationFailure](#s-f923916698)); (type="null")]; default=null |  |
| <a id="s-f390ce7d78"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-b3618cc9fa"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-7e82c96732)); (type="null")]; default=null |  |
| <a id="s-e05fbb0223"></a>`observer` | yes | [ObserverImplementation](#s-f0d48ddd05) |  |
| <a id="s-cd0ea27b5d"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a59a69372f"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0f3869e04b"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ab836c69c0"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-696f556167"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-d5014467f7"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ff4e3408d2)); minItems=1 |  |

##### <a id="s-f0d48ddd05"></a>definition `ObserverImplementation`

- <a id="s-b43ffb1af6"></a>`type`: `"object"`
- <a id="s-f671750ce5"></a>`additionalProperties`: `false`
- <a id="s-68ff276a2a"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6e967468c3"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d4871c778"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0cc50630d6"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-cdd2fac0f7"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-723aa9fe2c"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-4c0679d4d1"></a>definition `OperationContract`

- <a id="s-f626920852"></a>`type`: `"object"`
- <a id="s-85beb004ac"></a>`additionalProperties`: `false`
- <a id="s-dee5319a28"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b5ab42d85c"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-036b5412bd"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaDocument](#s-cd5f353a8b)); (type="null")]; default=null |  |
| <a id="s-11c2b49fbf"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ff5f3bd57d"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-3d68dabffa)); minItems=1 |  |
| <a id="s-8e18f5b695"></a>`intent_schema` | yes | [JsonSchemaDocument](#s-cd5f353a8b) |  |
| <a id="s-c4a94ba1a4"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-de2c4c6a35) |  |
| <a id="s-6f99d35aa6"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-1999c8b233)) |  |
| <a id="s-260b9bf26f"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-82d42f3311"></a>`source_retirement_permitted` | no | type="boolean"; default=false |  |

##### <a id="s-f742f4b080"></a>definition `OperationRef`

- <a id="s-e1c4446d8e"></a>`type`: `"object"`
- <a id="s-7d96104765"></a>`additionalProperties`: `false`
- <a id="s-691232acf6"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9a3ea14622"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-71d124d345"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1999c8b233"></a>definition `OutputArtifactContract`

- <a id="s-4d5ead5ad4"></a>`type`: `"object"`
- <a id="s-e5c3f7bea0"></a>`additionalProperties`: `false`
- <a id="s-710eb10f65"></a>`required`: `["role","derived_from_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-51537c7065"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-3a97e0f573"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-4c6cc32ca4"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-d15519c502"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-71523c9823"></a>definition `OutputArtifactRoleCount`

- <a id="s-6b623d42ef"></a>`type`: `"object"`
- <a id="s-5c362dd9ae"></a>`additionalProperties`: `false`
- <a id="s-f5641e9e89"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2c2bb7d330"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-e27563ec11"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-4f3acd771d"></a>definition `OutputArtifactSetIdentity`

- <a id="s-3707a496ec"></a>`type`: `"object"`
- <a id="s-1692dd27c2"></a>`additionalProperties`: `false`
- <a id="s-21a67d291e"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4c08f3a03b"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-fe642bea50"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-71523c9823)); minItems=1 |  |
| <a id="s-782ec0aa07"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7ef2a224e8"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-ca421464d4"></a>definition `OutputCollectionRef`

- <a id="s-49d7ab049b"></a>`type`: `"object"`
- <a id="s-32bad2c22d"></a>`additionalProperties`: `false`
- <a id="s-3b48ec0705"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6d900dcfec"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7676a07644"></a>`collection_id` | yes | [CollectionId](#s-72a858e21c) |  |
| <a id="s-e65217de83"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4335702a1c"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ae620e4656"></a>definition `RecipeRef`

- <a id="s-9c974af5f1"></a>`type`: `"object"`
- <a id="s-40ff00e332"></a>`additionalProperties`: `false`
- <a id="s-350c78ebb4"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d3dcecc5b3"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-238971ffc6"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-56ae9f5f68"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ad9607d3a8"></a>definition `SemanticIntentConformanceVector`

- <a id="s-870b93fc17"></a>`type`: `"object"`
- <a id="s-2f2bc3101a"></a>`additionalProperties`: `false`
- <a id="s-d919b6a7d2"></a>`required`: `["id","accepted","intent"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca787392c4"></a>`accepted` | yes | type="boolean" |  |
| <a id="s-61a3bb0828"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f5f77f0aaa"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |

##### <a id="s-61324a766c"></a>definition `SemanticIntentConformanceVectors`

- <a id="s-f168d6556e"></a>`type`: `"object"`
- <a id="s-73e25363c2"></a>`additionalProperties`: `false`
- <a id="s-37d45dbb87"></a>`required`: `["profile_id","vectors"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ad7c99830"></a>`format` | no | type="string"; const="stove0-semantic-intent-conformance/v1"; default="stove0-semantic-intent-conformance/v1" |  |
| <a id="s-3b0fd9b5d7"></a>`profile_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-967d6f0b33"></a>`vectors` | yes | type="array"; items=([SemanticIntentConformanceVector](#s-ad9607d3a8)); minItems=2 |  |

##### <a id="s-de2c4c6a35"></a>definition `SemanticValidationProfile`

- <a id="s-e18623ffed"></a>`type`: `"object"`
- <a id="s-23050ad859"></a>`additionalProperties`: `false`
- <a id="s-6076a35c1a"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-897354755d"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-84d7ec74a7"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-326d54cb6a"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-931608a6ef"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-75f71bb00c"></a>definition `TargetConformanceCoverage`

- <a id="s-5202f9a1ac"></a>`type`: `"object"`
- <a id="s-90965219c1"></a>`additionalProperties`: `false`
- <a id="s-1f145cbaf3"></a>`required`: `["advertised","exercised","complete"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-03d00803fd"></a>`advertised` | yes | type="integer"; minimum=0 |  |
| <a id="s-e02634f893"></a>`complete` | yes | type="boolean" |  |
| <a id="s-84aa57fcd5"></a>`exercised` | yes | type="integer"; minimum=0 |  |

##### <a id="s-3c67c2c42b"></a>definition `TargetContract`

- <a id="s-4cacc94231"></a>`type`: `"object"`
- <a id="s-3d88851369"></a>`additionalProperties`: `false`
- <a id="s-48381ced50"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6dfd9df2d3"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1c53df9988"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4f56b56aed"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dd9f7a672f"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-57dc21b0ab"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-e2738e9866)); minItems=1 |  |
| <a id="s-067e3c0ad3"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-778608a925"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-0e76288ab8"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### <a id="s-b8dbfa9f81"></a>definition `TargetExecutionEvidence`

- <a id="s-6d81b39d36"></a>`type`: `"object"`
- <a id="s-e6c91bf75e"></a>`additionalProperties`: `false`
- <a id="s-20ea6f10b8"></a>`required`: `["target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7eda4c1cb8"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2415defc07"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b0b881ac30"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c9e5d5e1f8"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-2ba828b36d"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5516bc8a3d"></a>definition `TargetFailure`

- <a id="s-c91b52891a"></a>`type`: `"object"`
- <a id="s-a980c3d121"></a>`additionalProperties`: `false`
- <a id="s-63f851324f"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c47820beee"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a4e5899b68"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-12ec5aa4d2"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-53ba199af7"></a>definition `TargetInapplicable`

- <a id="s-be91e5f7e3"></a>`type`: `"object"`
- <a id="s-d9a04839c5"></a>`additionalProperties`: `false`
- <a id="s-d8e118c4ba"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7142740e63"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-821eef9627"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-f5a0f7cf00"></a>definition `TargetInputAuthority`

- <a id="s-6990ce6c88"></a>`type`: `"object"`
- <a id="s-08667ceed4"></a>`additionalProperties`: `false`
- <a id="s-49d14e26ba"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca8e04b681"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-ae106f8ba9)); minItems=1 |  |
| <a id="s-056e5591df"></a>`selection` | yes | [ArtifactSelectionRef](#s-3531919e69) |  |

##### <a id="s-ae106f8ba9"></a>definition `TargetInputRoleCount`

- <a id="s-e3bed9825f"></a>`type`: `"object"`
- <a id="s-ff74cef486"></a>`additionalProperties`: `false`
- <a id="s-46ccaeeca9"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-15945fd519"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-26ad2ecfdb"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-37eb2902e8"></a>definition `TargetJobDeclaration`

- <a id="s-2499e810ff"></a>`type`: `"object"`
- <a id="s-9a71ed4615"></a>`additionalProperties`: `false`
- <a id="s-c04d0f3383"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f04d57291e"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-9557b91d78"></a>`controller_evidence` | yes | [ControllerEvidence](#s-d5dfdfd545) |  |
| <a id="s-f48674a7b4"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-481493ce7b"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6ad048a1c4"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-0e0545f239)); ([EffectPlan](#s-c6acdd3519))] |  |
| <a id="s-2a8833be6f"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

##### <a id="s-d6605b4451"></a>definition `TargetJobStatus`

- <a id="s-ad4f7a1a39"></a>`type`: `"object"`
- <a id="s-a7174190a0"></a>`additionalProperties`: `false`
- <a id="s-3356e3c6bb"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fd3e18ee2d"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-fe3f465207"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null |  |
| <a id="s-5603b02dbf"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-b6a8e45a6b)); (type="null")]; default=null |  |
| <a id="s-eb17518fce"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-b8dbfa9f81)); (type="null")]; default=null |  |
| <a id="s-c024e9a185"></a>`failure` | no | anyOf=[([TargetFailure](#s-5516bc8a3d)); (type="null")]; default=null |  |
| <a id="s-9d3bcd1181"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-53ba199af7)); (type="null")]; default=null |  |
| <a id="s-5d3de55b27"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2fe9b491bd"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-ca421464d4)); (type="null")]; default=null |  |
| <a id="s-f153dc293d"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e58201b5c2"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-3ada96cfa1)); (type="null")]; default=null |  |
| <a id="s-d6ad2d9666"></a>`progress` | yes | [TargetProgress](#s-41f70c9b28) |  |
| <a id="s-491e9ca814"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-bc76b9c412"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-91fba76d31"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

###### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-7d0bbb0675"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

##### <a id="s-4035eff85f"></a>definition `TargetOperationConformance`

- <a id="s-7faf3e2f4d"></a>`type`: `"object"`
- <a id="s-b7ff97ff62"></a>`additionalProperties`: `false`
- <a id="s-5fe754ea91"></a>`required`: `["operation_id","operation_contract_sha256","result_kind","options_schema_sha256","semantic_conformance"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-79beef85a7"></a>`intent_semantics_conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-2582f072f3"></a>`intent_semantics_id` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-8e4c1e3109"></a>`intent_semantics_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-1cc9ae3259"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d322f0b8d9"></a>`operation_id` | yes | type="string" |  |
| <a id="s-39bbe1ef26"></a>`options_schema_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a7eee653a3"></a>`result_kind` | yes | type="string"; enum=["collection","external-effect"] |  |
| <a id="s-3df5857bd7"></a>`semantic_conformance` | yes | type="string"; enum=["not-exercised","schema-only","exercised"] |  |

##### <a id="s-b8e5a7e203"></a>definition `TargetOperationConformanceEvidence`

- <a id="s-14e35ce61f"></a>`type`: `"object"`
- <a id="s-f9bea14de5"></a>`additionalProperties`: `false`
- <a id="s-fe443fc682"></a>`required`: `["operation_id","operation","semantic_conformance","preflight_request","preflight","accepted_job","submission","job_status"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-adbeacd17b"></a>`accepted_job` | yes | [AcceptedTargetJob](#s-b81dfee6ee) |  |
| <a id="s-b585e5dd9e"></a>`job_status` | yes | [TargetJobStatus](#s-d6605b4451) |  |
| <a id="s-e4ea052d40"></a>`operation` | yes | [OperationContract](#s-4c0679d4d1) |  |
| <a id="s-da3ba1cc7c"></a>`operation_id` | yes | type="string" |  |
| <a id="s-49bc2610f4"></a>`preflight` | yes | [TargetPreflightResponse](#s-7addb846c3) |  |
| <a id="s-1c59b6e292"></a>`preflight_request` | yes | [TargetPreflightRequest](#s-e2c72ccf93) |  |
| <a id="s-585235ef19"></a>`semantic_conformance` | yes | [TargetSemanticConformance](#s-9219516899) |  |
| <a id="s-994939d6d4"></a>`submission` | yes | [TargetJobStatus](#s-d6605b4451) |  |

##### <a id="s-e2738e9866"></a>definition `TargetOperationSupport`

- <a id="s-24f44b93f2"></a>`type`: `"object"`
- <a id="s-29208aaffe"></a>`additionalProperties`: `false`
- <a id="s-d7e2ccc240"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c9f6e3f546"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-731f2b5d7b"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a00bf6cde7"></a>`options_schema` | yes | [JsonSchemaDocument](#s-cd5f353a8b) |  |
| <a id="s-752c182fe2"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

##### <a id="s-2e6e2ab53c"></a>definition `TargetPlanBinding`

- <a id="s-dc0962343a"></a>`type`: `"object"`
- <a id="s-9f8ecde2b4"></a>`additionalProperties`: `false`
- <a id="s-4092ab608a"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6f7ed081a"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-92d427f5c5"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-c76776ff63"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-492b1bff74"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fa96b58ace"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4b50db2672"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-e2c72ccf93"></a>definition `TargetPreflightRequest`

- <a id="s-c0accff8e4"></a>`type`: `"object"`
- <a id="s-49c106a886"></a>`additionalProperties`: `false`
- <a id="s-18ec64a1d8"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-56f198c974"></a>`inputs` | yes | [TargetInputAuthority](#s-f5a0f7cf00) |  |
| <a id="s-842be5a796"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-a993344de7"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-6cece00f01)) |  |
| <a id="s-0d7a91ea78"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d1c61a2b39"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-11d16fb680"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-e66f23d512"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |

##### <a id="s-7addb846c3"></a>definition `TargetPreflightResponse`

- <a id="s-a0dfd0eb21"></a>`type`: `"object"`
- <a id="s-c515e3660c"></a>`additionalProperties`: `false`
- <a id="s-f03e785cc2"></a>`required`: `["target","plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c7022740f8"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-0e0545f239)); ([EffectPlan](#s-c6acdd3519))] |  |
| <a id="s-4a27722d79"></a>`target` | yes | [TargetContract](#s-3c67c2c42b) |  |

##### <a id="s-3ada96cfa1"></a>definition `TargetProductionAuthority`

- <a id="s-5ac4521649"></a>`type`: `"object"`
- <a id="s-f23cbdaf9a"></a>`additionalProperties`: `false`
- <a id="s-2fad931da1"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e2857ffc3"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-8c65a029d0"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-129ed36b98"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-1b0080b918"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-36081869a0"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-4f3acd771d) |  |
| <a id="s-f98a2013b2"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f00f390288"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b0e460148c"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-9becce0ff4) |  |
| <a id="s-2ae9eea1b7"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-3f1cf8c3a0"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-41f70c9b28"></a>definition `TargetProgress`

- <a id="s-cb990b71be"></a>`type`: `"object"`
- <a id="s-7b6138b075"></a>`additionalProperties`: `false`
- <a id="s-568aa359c9"></a>`required`: `["phase","completed"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e12c813b9"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-0b72685e50"></a>`phase` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-789045ddea"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null |  |
| <a id="s-09c0c36cd9"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null |  |

##### <a id="s-9219516899"></a>definition `TargetSemanticConformance`

- <a id="s-a012a04b3f"></a>`type`: `"object"`
- <a id="s-b015c2afeb"></a>`additionalProperties`: `false`
- <a id="s-624022af95"></a>`required`: `["profile_id","profile_sha256","status"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb6c0b4ce6"></a>`accepted_vector_ids` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-2a56c1f317"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-115882f904"></a>`profile_id` | yes | type="string" |  |
| <a id="s-25257021f3"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-150808611f"></a>`rejected_vector_ids` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-546f35c7f2"></a>`status` | yes | type="string"; enum=["schema-only","exercised"] |  |
| <a id="s-719386523e"></a>`vectors` | no | anyOf=[([SemanticIntentConformanceVectors](#s-61324a766c)); (type="null")]; default=null |  |

##### <a id="s-0e0545f239"></a>definition `TransformPlan`

- <a id="s-e51e370620"></a>`type`: `"object"`
- <a id="s-32c6f61340"></a>`additionalProperties`: `false`
- <a id="s-a4ddbe7482"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ae2086996a"></a>`inputs` | yes | [TargetInputAuthority](#s-f5a0f7cf00) |  |
| <a id="s-eddf335495"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-c85675a2b2"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-1c84df35aa"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-94b3a267fa"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fe370bad3a"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c435f1c61d"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-7edb0f6b9b"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4aaee76763"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c6e5b757f7"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |

##### <a id="s-52083e66b0"></a>definition `WorkIdentity`

- <a id="s-9fbdaed30b"></a>`type`: `"object"`
- <a id="s-a8c0163397"></a>`additionalProperties`: `false`
- <a id="s-8c9ff49420"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-18113e1414"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-5c5c10e682"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-34c9753b7d)); (type="null")]; default=null |  |
| <a id="s-a1653706cb"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-a07879b74f)); ([JoinWorkBinding](#s-d521986268))]); (type="null")]; default=null |  |
| <a id="s-07cb34bd9f"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-c28c11a742"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-271fc90c9e)); minItems=1 |  |
| <a id="s-5eb6689302"></a>`recipe` | yes | [RecipeRef](#s-ae620e4656) |  |
| <a id="s-e06f42d469"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-58ebb24bfd"></a>definition `WorkflowPlan`

- <a id="s-8634707005"></a>`type`: `"object"`
- <a id="s-87d1bfe8d8"></a>`additionalProperties`: `false`
- <a id="s-9cd90390be"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-154e919d4b"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-cc8c29d2ed"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-899d92fe58"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-6cece00f01)) |  |
| <a id="s-8440beafb9"></a>`operation` | yes | [OperationRef](#s-f742f4b080) |  |
| <a id="s-21ff23084c"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-5331dce695"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-770b4fc958)) |  |
| <a id="s-128e4a04eb"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-90a5234db2"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-6bf8672eaa"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-dd8fe0610e"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-318dce826f"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-8485d22dbf"></a>`work` | yes | [WorkIdentity](#s-52083e66b0) |  |
| <a id="s-4e868dfdb6"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [validate_result](stove0-target-support-targetconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-9ffbbb9306"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e130dc7132b93257cbb7c7b1811d0d3546a3e8e2dff3aedaad4b9dde4da3c18 -->

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
        "InputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "allowed_dispositions": {
              "anyOf": [
                {
                  "items": {
                    "enum": [
                      "transformed",
                      "preserved",
                      "omitted",
                      "rejected"
                    ],
                    "type": "string"
                  },
                  "type": "array"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role"
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
        "OperationContract": {
          "additionalProperties": false,
          "properties": {
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "effect_receipt_schema": {
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
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "inputs": {
              "items": {
                "$ref": "#/$defs/InputArtifactContract"
              },
              "minItems": 1,
              "type": "array"
            },
            "intent_schema": {
              "$ref": "#/$defs/JsonSchemaDocument"
            },
            "intent_semantics": {
              "$ref": "#/$defs/SemanticValidationProfile"
            },
            "outputs": {
              "default": [],
              "items": {
                "$ref": "#/$defs/OutputArtifactContract"
              },
              "type": "array"
            },
            "result_kind": {
              "default": "collection",
              "enum": [
                "collection",
                "external-effect"
              ],
              "type": "string"
            },
            "source_retirement_permitted": {
              "default": false,
              "type": "boolean"
            }
          },
          "required": [
            "id",
            "intent_schema",
            "intent_semantics",
            "inputs",
            "contract_sha256"
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
        "OutputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "derived_from_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "derived_from_roles"
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
        "SemanticIntentConformanceVector": {
          "additionalProperties": false,
          "properties": {
            "accepted": {
              "type": "boolean"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "accepted",
            "intent"
          ],
          "type": "object"
        },
        "SemanticIntentConformanceVectors": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-semantic-intent-conformance/v1",
              "default": "stove0-semantic-intent-conformance/v1",
              "type": "string"
            },
            "profile_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "vectors": {
              "items": {
                "$ref": "#/$defs/SemanticIntentConformanceVector"
              },
              "minItems": 2,
              "type": "array"
            }
          },
          "required": [
            "profile_id",
            "vectors"
          ],
          "type": "object"
        },
        "SemanticValidationProfile": {
          "additionalProperties": false,
          "properties": {
            "conformance_vectors_sha256": {
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
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "type": "object"
        },
        "TargetConformanceCoverage": {
          "additionalProperties": false,
          "properties": {
            "advertised": {
              "minimum": 0,
              "type": "integer"
            },
            "complete": {
              "type": "boolean"
            },
            "exercised": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "advertised",
            "exercised",
            "complete"
          ],
          "type": "object"
        },
        "TargetContract": {
          "additionalProperties": false,
          "properties": {
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "image_digest": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "implementation_version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            },
            "operations": {
              "items": {
                "$ref": "#/$defs/TargetOperationSupport"
              },
              "minItems": 1,
              "type": "array"
            },
            "protocol": {
              "default": "stove0-transform-target/v1",
              "enum": [
                "stove0-transform-target/v1",
                "stove0-effect-target/v1"
              ],
              "type": "string"
            },
            "source_revision": {
              "maxLength": 200,
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
            "implementation_id",
            "implementation_version",
            "source_revision",
            "image_digest",
            "operations",
            "contract_sha256"
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
        "TargetOperationConformance": {
          "additionalProperties": false,
          "properties": {
            "intent_semantics_conformance_vectors_sha256": {
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
            "intent_semantics_id": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "intent_semantics_sha256": {
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
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "type": "string"
            },
            "options_schema_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "result_kind": {
              "enum": [
                "collection",
                "external-effect"
              ],
              "type": "string"
            },
            "semantic_conformance": {
              "enum": [
                "not-exercised",
                "schema-only",
                "exercised"
              ],
              "type": "string"
            }
          },
          "required": [
            "operation_id",
            "operation_contract_sha256",
            "result_kind",
            "options_schema_sha256",
            "semantic_conformance"
          ],
          "type": "object"
        },
        "TargetOperationConformanceEvidence": {
          "additionalProperties": false,
          "properties": {
            "accepted_job": {
              "$ref": "#/$defs/AcceptedTargetJob"
            },
            "job_status": {
              "$ref": "#/$defs/TargetJobStatus"
            },
            "operation": {
              "$ref": "#/$defs/OperationContract"
            },
            "operation_id": {
              "type": "string"
            },
            "preflight": {
              "$ref": "#/$defs/TargetPreflightResponse"
            },
            "preflight_request": {
              "$ref": "#/$defs/TargetPreflightRequest"
            },
            "semantic_conformance": {
              "$ref": "#/$defs/TargetSemanticConformance"
            },
            "submission": {
              "$ref": "#/$defs/TargetJobStatus"
            }
          },
          "required": [
            "operation_id",
            "operation",
            "semantic_conformance",
            "preflight_request",
            "preflight",
            "accepted_job",
            "submission",
            "job_status"
          ],
          "type": "object"
        },
        "TargetOperationSupport": {
          "additionalProperties": false,
          "properties": {
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "options_schema": {
              "$ref": "#/$defs/JsonSchemaDocument"
            },
            "result_kind": {
              "default": "collection",
              "enum": [
                "collection",
                "external-effect"
              ],
              "type": "string"
            }
          },
          "required": [
            "operation_id",
            "operation_contract_sha256",
            "options_schema"
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
        "TargetPreflightRequest": {
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
            "observations": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ObservationEvidence"
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
            "protocol": {
              "default": "stove0-transform-target/v1",
              "enum": [
                "stove0-transform-target/v1",
                "stove0-effect-target/v1"
              ],
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
            "intent"
          ],
          "type": "object"
        },
        "TargetPreflightResponse": {
          "additionalProperties": false,
          "properties": {
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
            "target": {
              "$ref": "#/$defs/TargetContract"
            }
          },
          "required": [
            "target",
            "plan"
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
        "TargetSemanticConformance": {
          "additionalProperties": false,
          "properties": {
            "accepted_vector_ids": {
              "default": [],
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            "conformance_vectors_sha256": {
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
            "profile_id": {
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "rejected_vector_ids": {
              "default": [],
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            "status": {
              "enum": [
                "schema-only",
                "exercised"
              ],
              "type": "string"
            },
            "vectors": {
              "anyOf": [
                {
                  "$ref": "#/$defs/SemanticIntentConformanceVectors"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "profile_id",
            "profile_sha256",
            "status"
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
        "coverage": {
          "$ref": "#/$defs/TargetConformanceCoverage"
        },
        "format": {
          "const": "stove0-target-conformance-result/v1",
          "default": "stove0-target-conformance-result/v1",
          "type": "string"
        },
        "operation_evidence": {
          "default": [],
          "items": {
            "$ref": "#/$defs/TargetOperationConformanceEvidence"
          },
          "type": "array"
        },
        "operations": {
          "items": {
            "$ref": "#/$defs/TargetOperationConformance"
          },
          "type": "array"
        },
        "status": {
          "enum": [
            "conformant",
            "partially-exercised",
            "inspected"
          ],
          "type": "string"
        },
        "target": {
          "$ref": "#/$defs/TargetContract"
        }
      },
      "required": [
        "status",
        "target",
        "coverage",
        "operations"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-target-conformance-result/v1'] = 'stove0-target-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], target: stove0_target_protocol.protocol.TargetContract, coverage: stove0_target_support.conformance.TargetConformanceCoverage, operations: tuple[stove0_target_support.conformance.TargetOperationConformance, ...], operation_evidence: tuple[stove0_target_support.conformance.TargetOperationConformanceEvidence, ...] = ()) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetConformanceResult",
  "unit": "export"
}
```

</details>
