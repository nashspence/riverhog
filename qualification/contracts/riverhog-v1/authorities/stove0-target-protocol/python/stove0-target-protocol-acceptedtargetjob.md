# stove0_target_protocol.AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-acceptedtargetjob:624de7b1d1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c392c78ff8"></a>
- <a id="s-7607609177"></a>`distribution`: `stove0-target-protocol`
- <a id="s-d6419fb725"></a>`module`: `stove0_target_protocol`
- <a id="s-449903f6ce"></a>`name`: `AcceptedTargetJob`
- <a id="s-9ff0bccf11"></a>`unit`: `export`

### Declared structure

- <a id="s-c6a5e9c4f9"></a>`kind`: `"class"`
- <a id="s-d14127a6d4"></a>`signature`: `"\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-8f14119f71"></a>

- <a id="s-351604421a"></a>`type`: `"object"`
- <a id="s-d4af51f62a"></a>`additionalProperties`: `false`
- <a id="s-0944e2d0af"></a>`required`: `["declaration","request_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f9af5c784f"></a>`declaration` | yes | [TargetJobDeclaration](#s-77b308ad62) |  |
| <a id="s-2239eb7f78"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSelectionRef](#s-8d29998944)
- [ArtifactSubject](#s-37599065d0)
- [BranchWorkBinding](#s-c386399dfd)
- [CollectionId](#s-51c0a8da54)
- [CollectionRootRef](#s-1d1e6840f6)
- [ControllerEvidence](#s-5da467b0d4)
- [EffectPlan](#s-745b5b8fa8)
- [EvaluationBinding](#s-770268a072)
- [ExecutionEnvelope](#s-3bde0bf644)
- [JoinWorkBinding](#s-1b5cb48d41)
- [JoinWorkMemberBinding](#s-78f9d97d1f)
- [JsonSchemaValidationProfile](#s-2b849d9051)
- [JsonValue](#s-223b69a7b1)
- [ObservationEvidence](#s-23edc6aab1)
- [ObservationFailure](#s-2e70d451a3)
- [ObservationInapplicable](#s-3e9f0b5229)
- [ObservationRequest](#s-14d944036f)
- [ObservationResult](#s-6e7eaab09a)
- [ObserverImplementation](#s-32564876db)
- [OperationRef](#s-af8ff17674)
- [RecipeRef](#s-1d79145422)
- [TargetInputAuthority](#s-b03acb7057)
- [TargetInputRoleCount](#s-20b519275d)
- [TargetJobDeclaration](#s-77b308ad62)
- [TargetPlanBinding](#s-e66c6034c6)
- [TransformPlan](#s-c0ca1894ee)
- [WorkIdentity](#s-77b0ae1511)
- [WorkflowPlan](#s-1e85fec743)

##### <a id="s-8d29998944"></a>definition `ArtifactSelectionRef`

- <a id="s-d5e0366087"></a>`type`: `"object"`
- <a id="s-e68fa6c18a"></a>`additionalProperties`: `false`
- <a id="s-8fe1cc755e"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bc4f4559fa"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-4e2408fc59"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2a4dfcb6a0"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-37599065d0"></a>definition `ArtifactSubject`

- <a id="s-26f4a43dfc"></a>`type`: `"object"`
- <a id="s-01ba3903c6"></a>`additionalProperties`: `false`
- <a id="s-465902bfab"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8838b8edcd"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-9742a9989a"></a>`collection` | yes | [CollectionRootRef](#s-1d1e6840f6) |  |
| <a id="s-0c63ba89bd"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-77d09126aa"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-401676920c"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-3ab790a9d2"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-950684d5b2"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c386399dfd"></a>definition `BranchWorkBinding`

- <a id="s-db2b174275"></a>`type`: `"object"`
- <a id="s-018398ba73"></a>`additionalProperties`: `false`
- <a id="s-8d051ace96"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9598b0d74e"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa903286c4"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-60a9f76f9f"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb99f83443"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-b35708a9c1"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-51c0a8da54"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-5897e940e6"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-c51c543e0c"></a>2 | not=(const="0") |

##### <a id="s-1d1e6840f6"></a>definition `CollectionRootRef`

- <a id="s-8cb5171c53"></a>`type`: `"object"`
- <a id="s-c2b0c3f153"></a>`additionalProperties`: `false`
- <a id="s-c861ab4983"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a1087ff1fd"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-28ec2422d1"></a>`collection_id` | yes | [CollectionId](#s-51c0a8da54) |  |
| <a id="s-1a3eb7f072"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5da467b0d4"></a>definition `ControllerEvidence`

- <a id="s-5884801ac2"></a>`type`: `"object"`
- <a id="s-cdfb4e829f"></a>`additionalProperties`: `false`
- <a id="s-060dcb1021"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac04c1d3b2"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a7dbc3b3ee"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-3bde0bf644) |  |
| <a id="s-2ba8e2b33e"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-745b5b8fa8"></a>definition `EffectPlan`

- <a id="s-fa05e34231"></a>`type`: `"object"`
- <a id="s-8c0528e2d3"></a>`additionalProperties`: `false`
- <a id="s-706afbdea6"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a09054604b"></a>`inputs` | yes | [TargetInputAuthority](#s-b03acb7057) |  |
| <a id="s-6a3cdc8dfc"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-4b9c552d88"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-89cba508d8"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-48e5008152"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8623d0c5c3"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f8b038bf91"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-04b3921a26"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4189be10d4"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e2a88ed195"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |

##### <a id="s-770268a072"></a>definition `EvaluationBinding`

- <a id="s-837ddaf253"></a>`type`: `"object"`
- <a id="s-946a4c6a8f"></a>`additionalProperties`: `false`
- <a id="s-5854aa5c67"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19020a20c0"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-86d6e8a491"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b0294fc73c"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-23260c6198"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-3bde0bf644"></a>definition `ExecutionEnvelope`

- <a id="s-8ab934804d"></a>`type`: `"object"`
- <a id="s-7653535c91"></a>`additionalProperties`: `false`
- <a id="s-c8931672ea"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7817a9d491"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-90551e9738"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-475a134152"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-a2af760c99"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-49d28e3072"></a>`target_plan` | yes | [TargetPlanBinding](#s-e66c6034c6) |  |
| <a id="s-b6eb1b8e7b"></a>`workflow_plan` | yes | [WorkflowPlan](#s-1e85fec743) |  |

##### <a id="s-1b5cb48d41"></a>definition `JoinWorkBinding`

- <a id="s-9da78d5a04"></a>`type`: `"object"`
- <a id="s-357719dd22"></a>`additionalProperties`: `false`
- <a id="s-3dbedcce78"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-67630e478c"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d55840bcb1"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-3d1dc2d8e5"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-78f9d97d1f)); minItems=2 |  |
| <a id="s-d1a03f022d"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-78f9d97d1f"></a>definition `JoinWorkMemberBinding`

- <a id="s-f7a4d5e07b"></a>`type`: `"object"`
- <a id="s-c3e6a19f59"></a>`additionalProperties`: `false`
- <a id="s-f5903e7bba"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a1d810d07"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ca1932419d"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-997d138891"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-c6dd98f761"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2b849d9051"></a>definition `JsonSchemaValidationProfile`

- <a id="s-6b8fb0ff7a"></a>`type`: `"object"`
- <a id="s-754004c376"></a>`additionalProperties`: `false`
- <a id="s-dd5df1371c"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9f59d0d539"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-f8a05181c6"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-30b1e5b7a6"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0af8afc796"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-314e892fba"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |

##### <a id="s-223b69a7b1"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-23edc6aab1"></a>definition `ObservationEvidence`

- <a id="s-89e29e4f47"></a>`type`: `"object"`
- <a id="s-435d9436f4"></a>`additionalProperties`: `false`
- <a id="s-636bab7123"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fbce1885fc"></a>`request` | yes | [ObservationRequest](#s-14d944036f) |  |
| <a id="s-e4d43ec272"></a>`result` | yes | [ObservationResult](#s-6e7eaab09a) |  |

##### <a id="s-2e70d451a3"></a>definition `ObservationFailure`

- <a id="s-14495d5511"></a>`type`: `"object"`
- <a id="s-38d88401d9"></a>`additionalProperties`: `false`
- <a id="s-4836e060a9"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d9bb3d1dbd"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-612d38a4eb"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-8efb884ab0"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-3e9f0b5229"></a>definition `ObservationInapplicable`

- <a id="s-aa6236a007"></a>`type`: `"object"`
- <a id="s-b8802911a7"></a>`additionalProperties`: `false`
- <a id="s-05ed63364a"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-23211ecbd4"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f578daebff"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-14d944036f"></a>definition `ObservationRequest`

- <a id="s-9420cbf8e7"></a>`type`: `"object"`
- <a id="s-a34be639f2"></a>`additionalProperties`: `false`
- <a id="s-71906e07e8"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b425166983"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-b9f87d278b"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-9e9beddb60"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8fae741054"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-af24e9fb34"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-68758ab36a"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-acb966abfc"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-ba7d46b95c"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a245913692"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-e104df0405"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-37599065d0)); minItems=1 |  |
| <a id="s-2f2b317dc5"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-1f9a5d3cc9"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6e7eaab09a"></a>definition `ObservationResult`

- <a id="s-e734f16646"></a>`type`: `"object"`
- <a id="s-c61a3703ef"></a>`additionalProperties`: `false`
- <a id="s-afcb27ee31"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5918c2d207"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-22e0fc0fce"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-223b69a7b1))); (type="null")]; default=null |  |
| <a id="s-0ff375cdcc"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-2b849d9051)); (type="null")]; default=null |  |
| <a id="s-233b0ebf7f"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-c22145d1aa"></a>`failure` | no | anyOf=[([ObservationFailure](#s-2e70d451a3)); (type="null")]; default=null |  |
| <a id="s-263d1beba4"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-c5aa1c2ecd"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-3e9f0b5229)); (type="null")]; default=null |  |
| <a id="s-ce3334dc78"></a>`observer` | yes | [ObserverImplementation](#s-32564876db) |  |
| <a id="s-de0ebba37a"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bcb9c9d03e"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b37eb61560"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7de7544614"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4b71c925a7"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-29677eab55"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-37599065d0)); minItems=1 |  |

##### <a id="s-32564876db"></a>definition `ObserverImplementation`

- <a id="s-a3ab850b5e"></a>`type`: `"object"`
- <a id="s-86f6a6b95e"></a>`additionalProperties`: `false`
- <a id="s-fe4fcf4624"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72fa3333e6"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-61c734d552"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7d07ba5cbc"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-3cd199dd97"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-d8c86f2f70"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-af8ff17674"></a>definition `OperationRef`

- <a id="s-487001a410"></a>`type`: `"object"`
- <a id="s-bc2b94bbb9"></a>`additionalProperties`: `false`
- <a id="s-70880c5bbc"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fc3b1100e0"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-787895813e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1d79145422"></a>definition `RecipeRef`

- <a id="s-0c7aaf9e3d"></a>`type`: `"object"`
- <a id="s-b3d13f9c57"></a>`additionalProperties`: `false`
- <a id="s-1429e9363f"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-82206e653c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d35b1741ed"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-f3b92a3b84"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b03acb7057"></a>definition `TargetInputAuthority`

- <a id="s-e008409882"></a>`type`: `"object"`
- <a id="s-e4be40bcbd"></a>`additionalProperties`: `false`
- <a id="s-e53b440769"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88c327bfca"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-20b519275d)); minItems=1 |  |
| <a id="s-732e94b20f"></a>`selection` | yes | [ArtifactSelectionRef](#s-8d29998944) |  |

##### <a id="s-20b519275d"></a>definition `TargetInputRoleCount`

- <a id="s-934aea6540"></a>`type`: `"object"`
- <a id="s-d3e61e4f54"></a>`additionalProperties`: `false`
- <a id="s-b629629823"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b8f42cdd15"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-dfd3f075c1"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-77b308ad62"></a>definition `TargetJobDeclaration`

- <a id="s-3bf032b7d9"></a>`type`: `"object"`
- <a id="s-73e0833e83"></a>`additionalProperties`: `false`
- <a id="s-007830b842"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-355fc894a4"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-ddd5fe7e0b"></a>`controller_evidence` | yes | [ControllerEvidence](#s-5da467b0d4) |  |
| <a id="s-a53af36200"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-39f3d74caf"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6d0bb5fd0f"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-c0ca1894ee)); ([EffectPlan](#s-745b5b8fa8))] |  |
| <a id="s-d2892161a4"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

##### <a id="s-e66c6034c6"></a>definition `TargetPlanBinding`

- <a id="s-f684aadcd2"></a>`type`: `"object"`
- <a id="s-7cf399c48f"></a>`additionalProperties`: `false`
- <a id="s-851ce2fb6b"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a15cdab855"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-08f97b6fcd"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-15f9d4edff"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cd96b69aed"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fb98561305"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fb1d51c07c"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-c0ca1894ee"></a>definition `TransformPlan`

- <a id="s-4d3754d18d"></a>`type`: `"object"`
- <a id="s-ffe15f3c32"></a>`additionalProperties`: `false`
- <a id="s-1c00bd1dd5"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3491438be3"></a>`inputs` | yes | [TargetInputAuthority](#s-b03acb7057) |  |
| <a id="s-1ffc9c7f6c"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-27e28cb6c8"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-37d8579139"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b6ccd87c6d"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a7c7133519"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c46a32ca0a"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-2b8aa691dd"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b24e090075"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d89c84b6a1"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |

##### <a id="s-77b0ae1511"></a>definition `WorkIdentity`

- <a id="s-20d960e99a"></a>`type`: `"object"`
- <a id="s-923240e071"></a>`additionalProperties`: `false`
- <a id="s-8abc34e001"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ab0d2dbc7"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-8b3f0f3fcb"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-770268a072)); (type="null")]; default=null |  |
| <a id="s-30bd2a5d29"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-c386399dfd)); ([JoinWorkBinding](#s-1b5cb48d41))]); (type="null")]; default=null |  |
| <a id="s-49618377c7"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-cffb2318f2"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-1d1e6840f6)); minItems=1 |  |
| <a id="s-4cde6976a3"></a>`recipe` | yes | [RecipeRef](#s-1d79145422) |  |
| <a id="s-27e49dc38b"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1e85fec743"></a>definition `WorkflowPlan`

- <a id="s-70bfcebe5d"></a>`type`: `"object"`
- <a id="s-742d534491"></a>`additionalProperties`: `false`
- <a id="s-95ea44c2fe"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-84361baefd"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-796692f528"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-d3f9005374"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-23edc6aab1)) |  |
| <a id="s-81f55b3817"></a>`operation` | yes | [OperationRef](#s-af8ff17674) |  |
| <a id="s-071529a4ab"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-6ad4da7c6f"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-223b69a7b1)) |  |
| <a id="s-d532923e11"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-67844d30c6"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-334177f389"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-1e7d37bafe"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5f3055b9d5"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-2862a5239f"></a>`work` | yes | [WorkIdentity](#s-77b0ae1511) |  |
| <a id="s-a68c86d208"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [verify_digest](stove0-target-protocol-acceptedtargetjob-verify-digest.md)

## Governing policies

- <a id="pa-43e2eb2826"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.AcceptedTargetJob`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ae371aceff33c954dc08cb7144df712d0dffd94e582dddc3be382d4acd77041 -->

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
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "AcceptedTargetJob",
  "unit": "export"
}
```

</details>
