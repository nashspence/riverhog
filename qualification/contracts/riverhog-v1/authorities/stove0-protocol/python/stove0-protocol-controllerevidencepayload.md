# stove0_protocol.ControllerEvidencePayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-controllerevidencepayload:43a21c32f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bfcef07647"></a>
- <a id="s-c9085f3772"></a>`distribution`: `stove0-protocol`
- <a id="s-181aead80a"></a>`module`: `stove0_protocol`
- <a id="s-b5c8927554"></a>`name`: `ControllerEvidencePayload`
- <a id="s-a65312c23a"></a>`unit`: `export`

### Declared structure

- <a id="s-36fd12dfe6"></a>`kind`: `"class"`
- <a id="s-6a60792360"></a>`signature`: `"\"(*, format: Literal['stove0-controller-evidence/v1'] = 'stove0-controller-evidence/v1', execution_envelope: stove0_protocol.models.ExecutionEnvelope) -> None\""`

#### Validated model schema

<a id="s-0bba12d575"></a>

- <a id="s-1beab43755"></a>`type`: `"object"`
- <a id="s-73a595a490"></a>`additionalProperties`: `false`
- <a id="s-8562ad0e2e"></a>`required`: `["execution_envelope"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed5ee0f193"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-eb48b39f43) |  |
| <a id="s-6c7ecc3618"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### Definitions

- [ArtifactSubject](#s-3ed764cff8)
- [BranchWorkBinding](#s-207431a34a)
- [CollectionId](#s-6d25eb619c)
- [CollectionRootRef](#s-fb1f737f47)
- [EvaluationBinding](#s-93031e2ec3)
- [ExecutionEnvelope](#s-eb48b39f43)
- [JoinWorkBinding](#s-b0b0ee080c)
- [JoinWorkMemberBinding](#s-31f7b8988b)
- [JsonSchemaDocument](#s-3c6a152cca)
- [JsonValue](#s-34ca2b0810)
- [ObservationEvidence](#s-a5a2057f07)
- [ObservationFailure](#s-1d467c3f97)
- [ObservationInapplicable](#s-a8b7e75316)
- [ObservationRequest](#s-cfd730a0e8)
- [ObservationResult](#s-28134a93c8)
- [ObserverImplementation](#s-ab6b5827d4)
- [OperationRef](#s-da00c000b9)
- [RecipeRef](#s-57dece9540)
- [TargetPlanBinding](#s-ca7067305b)
- [WorkIdentity](#s-e4a1cd1f96)
- [WorkflowPlan](#s-fcaaa04d22)

##### <a id="s-3ed764cff8"></a>definition `ArtifactSubject`

- <a id="s-3f2a9adf12"></a>`type`: `"object"`
- <a id="s-3c677ffb9f"></a>`additionalProperties`: `false`
- <a id="s-08fa6ef69c"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-74c0823e84"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-30e45edd51"></a>`collection` | yes | [CollectionRootRef](#s-fb1f737f47) |  |
| <a id="s-5ade772cdf"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-adbe586bd8"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-e20a5b8246"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-9b9f992cc2"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-238b58c4e9"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-207431a34a"></a>definition `BranchWorkBinding`

- <a id="s-7d2708bfb1"></a>`type`: `"object"`
- <a id="s-cddff0c11e"></a>`additionalProperties`: `false`
- <a id="s-cf11e6fc14"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0005bfcff9"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2ebc8177a0"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-70584c79c8"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bed6db945e"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-b592ac4880"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6d25eb619c"></a>definition `CollectionId`

- <a id="s-55ddd3a985"></a>`type`: `"integer"`
- <a id="s-3bc2637780"></a>`minimum`: `1`

##### <a id="s-fb1f737f47"></a>definition `CollectionRootRef`

- <a id="s-7f8e5a803f"></a>`type`: `"object"`
- <a id="s-e21ac5cf6f"></a>`additionalProperties`: `false`
- <a id="s-fd97c21178"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e76221bde3"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a4a14c8631"></a>`collection_id` | yes | [CollectionId](#s-6d25eb619c) |  |
| <a id="s-7639d70bc6"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-93031e2ec3"></a>definition `EvaluationBinding`

- <a id="s-f04e55df8a"></a>`type`: `"object"`
- <a id="s-b39db79533"></a>`additionalProperties`: `false`
- <a id="s-5806cc1aac"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3ca82ba8d0"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-539a9f6dfc"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b042a2d613"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-34ca2b0810)) |  |
| <a id="s-746879ba9b"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-eb48b39f43"></a>definition `ExecutionEnvelope`

- <a id="s-a70201dd59"></a>`type`: `"object"`
- <a id="s-957b18042c"></a>`additionalProperties`: `false`
- <a id="s-cd80f2b8b9"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d743d38561"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-8ed6e849a0"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eece360888"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-60d69b6e7e"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-c71dd03d52"></a>`target_plan` | yes | [TargetPlanBinding](#s-ca7067305b) |  |
| <a id="s-0e12ab4e85"></a>`workflow_plan` | yes | [WorkflowPlan](#s-fcaaa04d22) |  |

##### <a id="s-b0b0ee080c"></a>definition `JoinWorkBinding`

- <a id="s-b48b86721f"></a>`type`: `"object"`
- <a id="s-a0a4ce6f96"></a>`additionalProperties`: `false`
- <a id="s-b23b2de5b5"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8d26df9e99"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-295cae7e18"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-070f36505a"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-31f7b8988b)); minItems=2 |  |
| <a id="s-aa200f5022"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-31f7b8988b"></a>definition `JoinWorkMemberBinding`

- <a id="s-170f610a18"></a>`type`: `"object"`
- <a id="s-83477c4d12"></a>`additionalProperties`: `false`
- <a id="s-caba5303b1"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4fa540aa58"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f3103c0cd7"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-38833c0e2d"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a93dae0baa"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3c6a152cca"></a>definition `JsonSchemaDocument`

- <a id="s-fb3c7687ee"></a>`type`: `"object"`
- <a id="s-879c226da6"></a>`additionalProperties`: `false`
- <a id="s-3e05e8bd52"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5dd276a1be"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-496a2f0dd6"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-450f561c46"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e883fb9e4e"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-34ca2b0810)) |  |
| <a id="s-7a1d49fac0"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-34ca2b0810"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-a5a2057f07"></a>definition `ObservationEvidence`

- <a id="s-88a5b3f9b8"></a>`type`: `"object"`
- <a id="s-8bcdb609a8"></a>`additionalProperties`: `false`
- <a id="s-a9cc3b1ee7"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2459008220"></a>`request` | yes | [ObservationRequest](#s-cfd730a0e8) |  |
| <a id="s-d18ed0cc0f"></a>`result` | yes | [ObservationResult](#s-28134a93c8) |  |

##### <a id="s-1d467c3f97"></a>definition `ObservationFailure`

- <a id="s-d8a1ebe3c0"></a>`type`: `"object"`
- <a id="s-35fb9e3d4b"></a>`additionalProperties`: `false`
- <a id="s-950ee61f6a"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff6e9e910a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f484964db3"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-1068ca03d4"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-a8b7e75316"></a>definition `ObservationInapplicable`

- <a id="s-226d91421f"></a>`type`: `"object"`
- <a id="s-f16da19515"></a>`additionalProperties`: `false`
- <a id="s-c0ca9e9f9d"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df383e8a46"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a8a76d59f9"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-cfd730a0e8"></a>definition `ObservationRequest`

- <a id="s-04361000a2"></a>`type`: `"object"`
- <a id="s-98cf40feab"></a>`additionalProperties`: `false`
- <a id="s-8fa87886fe"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1261220c98"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-472c86c2e4"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-0d0e750f48"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9f7589cc5e"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3ab0926619"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-150a070c9a"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-7c5035b6b8"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-34ca2b0810)) |  |
| <a id="s-516966af03"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-734bd320fd"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-807a78c209"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-3ed764cff8)); minItems=1 |  |
| <a id="s-be4a41d9d8"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-404616851e"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-28134a93c8"></a>definition `ObservationResult`

- <a id="s-eb2170e9e7"></a>`type`: `"object"`
- <a id="s-f964cc268c"></a>`additionalProperties`: `false`
- <a id="s-cf8a7eb23f"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e1fec1073"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-34ca2b0810)) |  |
| <a id="s-07e044d2c8"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-34ca2b0810))); (type="null")]; default=null |  |
| <a id="s-3d764cf1b7"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-3c6a152cca)); (type="null")]; default=null |  |
| <a id="s-936185aaa5"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-409ade6561"></a>`failure` | no | anyOf=[([ObservationFailure](#s-1d467c3f97)); (type="null")]; default=null |  |
| <a id="s-e83b4501e7"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-976d7c9dbe"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-a8b7e75316)); (type="null")]; default=null |  |
| <a id="s-ecef5fd25c"></a>`observer` | yes | [ObserverImplementation](#s-ab6b5827d4) |  |
| <a id="s-8262db1d6b"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-517907da05"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fc5fc5dd17"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3c22ace505"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f3c55376ae"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-d769161602"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-3ed764cff8)); minItems=1 |  |

##### <a id="s-ab6b5827d4"></a>definition `ObserverImplementation`

- <a id="s-959e01b199"></a>`type`: `"object"`
- <a id="s-be2a721dc9"></a>`additionalProperties`: `false`
- <a id="s-10a96784bd"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-12f6a20adb"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3565907bc7"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-03486c17ea"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-140bec8846"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-9c3f880485"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-da00c000b9"></a>definition `OperationRef`

- <a id="s-b1651a5bc0"></a>`type`: `"object"`
- <a id="s-89ac235622"></a>`additionalProperties`: `false`
- <a id="s-abf28b0cc5"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea9e9a0268"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fd8a521bb0"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-57dece9540"></a>definition `RecipeRef`

- <a id="s-cb4ccb3eb2"></a>`type`: `"object"`
- <a id="s-251edff2e8"></a>`additionalProperties`: `false`
- <a id="s-a68b217fba"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4b3a2fe2dc"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b71179cc5e"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-995e132f52"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ca7067305b"></a>definition `TargetPlanBinding`

- <a id="s-7f00e51b82"></a>`type`: `"object"`
- <a id="s-06f703594b"></a>`additionalProperties`: `false`
- <a id="s-d48408a702"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19ddd2d7ea"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f8d234a53e"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-34ca2b0810)) |  |
| <a id="s-e072eb1c91"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ebe2d83cdc"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f1d5d6ee00"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-31491eac27"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-e4a1cd1f96"></a>definition `WorkIdentity`

- <a id="s-2b98251a7f"></a>`type`: `"object"`
- <a id="s-30881aacca"></a>`additionalProperties`: `false`
- <a id="s-bb070c2363"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f09cb6ff80"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-34ca2b0810)) |  |
| <a id="s-915fa9b1c6"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-93031e2ec3)); (type="null")]; default=null |  |
| <a id="s-cd4492417b"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-207431a34a)); ([JoinWorkBinding](#s-b0b0ee080c))]); (type="null")]; default=null |  |
| <a id="s-5cec683b91"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-ce7d31a547"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-fb1f737f47)); minItems=1 |  |
| <a id="s-90525f539a"></a>`recipe` | yes | [RecipeRef](#s-57dece9540) |  |
| <a id="s-826b5fc4c9"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fcaaa04d22"></a>definition `WorkflowPlan`

- <a id="s-b9c11cd6ec"></a>`type`: `"object"`
- <a id="s-185eb89bc3"></a>`additionalProperties`: `false`
- <a id="s-ee471d099d"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8f5b6bf37d"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-28b890b99a"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-98b4fc7321"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-a5a2057f07)) |  |
| <a id="s-f02abf9837"></a>`operation` | yes | [OperationRef](#s-da00c000b9) |  |
| <a id="s-c241792973"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-34ca2b0810)) |  |
| <a id="s-8ecaa60047"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-34ca2b0810)) |  |
| <a id="s-3372e64b68"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-4868184a45"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-7f962258aa"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-9a9a32365b"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c77ab32c6e"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-f21e193560"></a>`work` | yes | [WorkIdentity](#s-e4a1cd1f96) |  |
| <a id="s-3c3a871a4c"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-1450a22284"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ControllerEvidencePayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2fd8156dfe54f1fd315b2b239496e37338636d22db21ccce439425a2f030012f -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
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
        "execution_envelope"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-controller-evidence/v1'] = 'stove0-controller-evidence/v1', execution_envelope: stove0_protocol.models.ExecutionEnvelope) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ControllerEvidencePayload",
  "unit": "export"
}
```

</details>
