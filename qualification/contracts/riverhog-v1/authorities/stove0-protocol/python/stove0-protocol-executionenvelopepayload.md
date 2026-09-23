# stove0_protocol.ExecutionEnvelopePayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelopepayload:0215c4d5d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc1e484385"></a>
- <a id="s-701586d587"></a>`distribution`: `stove0-protocol`
- <a id="s-d6af3672c6"></a>`module`: `stove0_protocol`
- <a id="s-7700ad1d17"></a>`name`: `ExecutionEnvelopePayload`
- <a id="s-917fe1f086"></a>`unit`: `export`

### Declared structure

- <a id="s-e77889dbb2"></a>`kind`: `"class"`
- <a id="s-0520f25634"></a>`signature`: `"\"(*, format: Literal['stove0-execution-envelope/v1'] = 'stove0-execution-envelope/v1', claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], workflow_plan: stove0_protocol.models.WorkflowPlan, target_plan: stove0_protocol.models.TargetPlanBinding) -> None\""`

#### Validated model schema

<a id="s-ad8e47c173"></a>

- <a id="s-8ea2689d31"></a>`type`: `"object"`
- <a id="s-37a9638336"></a>`additionalProperties`: `false`
- <a id="s-c1fc77be74"></a>`required`: `["claim_id","fence","workflow_plan","target_plan"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ccdf44a83f"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-c617134b10"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-5732613e99"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-95c278fb49"></a>`target_plan` | yes | [TargetPlanBinding](#s-d98e92a230) |  |
| <a id="s-16addec42c"></a>`workflow_plan` | yes | [WorkflowPlan](#s-531e395e15) |  |

##### Definitions

- [ArtifactSubject](#s-ce324b621a)
- [BranchWorkBinding](#s-3d07a39952)
- [CollectionId](#s-4f3e1607df)
- [CollectionRootRef](#s-b5cb505da7)
- [EvaluationBinding](#s-25931f9640)
- [JoinWorkBinding](#s-9389509f75)
- [JoinWorkMemberBinding](#s-3f2562c1dd)
- [JsonSchemaValidationProfile](#s-ee6c191c25)
- [JsonValue](#s-e3a2bfbcb8)
- [ObservationEvidence](#s-f07b36cf86)
- [ObservationFailure](#s-a7393c8a22)
- [ObservationInapplicable](#s-c8da04a722)
- [ObservationRequest](#s-39ea17964d)
- [ObservationResult](#s-11d84afa5d)
- [ObserverImplementation](#s-b2177165a4)
- [OperationRef](#s-0239fcd917)
- [RecipeRef](#s-d9ddbeb901)
- [TargetPlanBinding](#s-d98e92a230)
- [WorkIdentity](#s-7f2412f5fb)
- [WorkflowPlan](#s-531e395e15)

##### <a id="s-ce324b621a"></a>definition `ArtifactSubject`

- <a id="s-5a0362dd3c"></a>`type`: `"object"`
- <a id="s-fb93329204"></a>`additionalProperties`: `false`
- <a id="s-405ba41b73"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f2e1a9805b"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-37f092a8b7"></a>`collection` | yes | [CollectionRootRef](#s-b5cb505da7) |  |
| <a id="s-48926a57b1"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-2641c14b5e"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-2de72c43a2"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-971fd787bb"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-de5e88b249"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3d07a39952"></a>definition `BranchWorkBinding`

- <a id="s-1d8ac941d2"></a>`type`: `"object"`
- <a id="s-b0c9b79a14"></a>`additionalProperties`: `false`
- <a id="s-881d3648bf"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-91795eb8fe"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa742715ec"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4a95e68cc8"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5e7f0ffbd3"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-d9e56549e6"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4f3e1607df"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-4a80360c2f"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-15090a3bbd"></a>2 | not=(const="0") |

##### <a id="s-b5cb505da7"></a>definition `CollectionRootRef`

- <a id="s-209ca20828"></a>`type`: `"object"`
- <a id="s-ffca388e4e"></a>`additionalProperties`: `false`
- <a id="s-fbbfe2548d"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-13ec2fd2ef"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b4c0bd80c"></a>`collection_id` | yes | [CollectionId](#s-4f3e1607df) |  |
| <a id="s-029854551a"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-25931f9640"></a>definition `EvaluationBinding`

- <a id="s-9a31cb8b52"></a>`type`: `"object"`
- <a id="s-da64a6a3d6"></a>`additionalProperties`: `false`
- <a id="s-93afce6941"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-006c8a872c"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-270d08b4d0"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-53f8fc56ac"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8)) |  |
| <a id="s-daab038f34"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-9389509f75"></a>definition `JoinWorkBinding`

- <a id="s-eecb9b7295"></a>`type`: `"object"`
- <a id="s-6d95d4018d"></a>`additionalProperties`: `false`
- <a id="s-28d08b194f"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fdfe689704"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b19051664d"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-7069c59807"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-3f2562c1dd)); minItems=2 |  |
| <a id="s-649dad203c"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3f2562c1dd"></a>definition `JoinWorkMemberBinding`

- <a id="s-9cf367905e"></a>`type`: `"object"`
- <a id="s-f6360a618b"></a>`additionalProperties`: `false`
- <a id="s-8d9e7f3c1b"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6d5707e8eb"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1c264a70b6"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e9d4834504"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-879ab352dc"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ee6c191c25"></a>definition `JsonSchemaValidationProfile`

- <a id="s-0af11d51e0"></a>`type`: `"object"`
- <a id="s-22931b3dce"></a>`additionalProperties`: `false`
- <a id="s-a7df24536a"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3e89361563"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-b79b95b263"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-93c34f58b1"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dbacb8bb1d"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-656a779581"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8)) |  |

##### <a id="s-e3a2bfbcb8"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-f07b36cf86"></a>definition `ObservationEvidence`

- <a id="s-00e3808934"></a>`type`: `"object"`
- <a id="s-ee7227bbca"></a>`additionalProperties`: `false`
- <a id="s-22c52224fe"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bd2e1aa3a1"></a>`request` | yes | [ObservationRequest](#s-39ea17964d) |  |
| <a id="s-a937cce503"></a>`result` | yes | [ObservationResult](#s-11d84afa5d) |  |

##### <a id="s-a7393c8a22"></a>definition `ObservationFailure`

- <a id="s-98fde8b4a5"></a>`type`: `"object"`
- <a id="s-3e405764be"></a>`additionalProperties`: `false`
- <a id="s-63d263cc7e"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1a70e6d007"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-15dbde2ab7"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-596168b4b1"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-c8da04a722"></a>definition `ObservationInapplicable`

- <a id="s-f37418fc5f"></a>`type`: `"object"`
- <a id="s-06f32ecb4a"></a>`additionalProperties`: `false`
- <a id="s-b6ac216fa7"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c258688449"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d811c68f3f"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-39ea17964d"></a>definition `ObservationRequest`

- <a id="s-aaff92ab38"></a>`type`: `"object"`
- <a id="s-f6a175d34a"></a>`additionalProperties`: `false`
- <a id="s-cdbf113deb"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2562a2534a"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-48c209a099"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-e2426be529"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7cf8c4d20f"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0393b43d6c"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7e85435c30"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-198ede2d3f"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8)) |  |
| <a id="s-7c5bf2694b"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b29d85c7aa"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-2815691d5f"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ce324b621a)); minItems=1 |  |
| <a id="s-5aa733efdb"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-e23e49eb20"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-11d84afa5d"></a>definition `ObservationResult`

- <a id="s-c7caa1e580"></a>`type`: `"object"`
- <a id="s-965d38053c"></a>`additionalProperties`: `false`
- <a id="s-e725c0bc33"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c93e5a8340"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8)) |  |
| <a id="s-846c624445"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8))); (type="null")]; default=null |  |
| <a id="s-93382679c0"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-ee6c191c25)); (type="null")]; default=null |  |
| <a id="s-6873dd43ab"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-c27a77cf60"></a>`failure` | no | anyOf=[([ObservationFailure](#s-a7393c8a22)); (type="null")]; default=null |  |
| <a id="s-59f9b793e1"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-c344977860"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-c8da04a722)); (type="null")]; default=null |  |
| <a id="s-474ea38789"></a>`observer` | yes | [ObserverImplementation](#s-b2177165a4) |  |
| <a id="s-88f2acee72"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0b495fc43a"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-77c2671b73"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4e7b4a2f0a"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-89eb1ce6cc"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-ee6ea103b3"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ce324b621a)); minItems=1 |  |

##### <a id="s-b2177165a4"></a>definition `ObserverImplementation`

- <a id="s-86685d45f1"></a>`type`: `"object"`
- <a id="s-8fbd482bc6"></a>`additionalProperties`: `false`
- <a id="s-09f69b6302"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bfda4b8958"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f2fb575aab"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0fdc4a1cd6"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-f059ef0290"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-e54e5cb853"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-0239fcd917"></a>definition `OperationRef`

- <a id="s-8feed62caf"></a>`type`: `"object"`
- <a id="s-0fc92918f7"></a>`additionalProperties`: `false`
- <a id="s-25998c95e7"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-099b8f1d83"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9c23a0f47f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d9ddbeb901"></a>definition `RecipeRef`

- <a id="s-26355ace8e"></a>`type`: `"object"`
- <a id="s-e9cfee532b"></a>`additionalProperties`: `false`
- <a id="s-9ac0993c07"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-445011e6b5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4dff5b1b87"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-2584febae0"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d98e92a230"></a>definition `TargetPlanBinding`

- <a id="s-55cccf1e26"></a>`type`: `"object"`
- <a id="s-4d44155e49"></a>`additionalProperties`: `false`
- <a id="s-e93ccad61b"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5e6d840d14"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c9585480a2"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8)) |  |
| <a id="s-4c5f09bc6c"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cb51985563"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2396452453"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d4779b987b"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-7f2412f5fb"></a>definition `WorkIdentity`

- <a id="s-93e888b84f"></a>`type`: `"object"`
- <a id="s-8eb976b040"></a>`additionalProperties`: `false`
- <a id="s-2d7309a884"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e5702fa85"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8)) |  |
| <a id="s-8bfe53e778"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-25931f9640)); (type="null")]; default=null |  |
| <a id="s-d18710db85"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-3d07a39952)); ([JoinWorkBinding](#s-9389509f75))]); (type="null")]; default=null |  |
| <a id="s-746d9d7233"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-3f735ac636"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-b5cb505da7)); minItems=1 |  |
| <a id="s-4d8852c2f7"></a>`recipe` | yes | [RecipeRef](#s-d9ddbeb901) |  |
| <a id="s-d6749911a6"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-531e395e15"></a>definition `WorkflowPlan`

- <a id="s-8b1efac8a7"></a>`type`: `"object"`
- <a id="s-7a35ef5fe5"></a>`additionalProperties`: `false`
- <a id="s-8c49d85086"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bbc33a66ed"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-41c52b86c4"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-a1405e1d60"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-f07b36cf86)) |  |
| <a id="s-f7030df1f0"></a>`operation` | yes | [OperationRef](#s-0239fcd917) |  |
| <a id="s-debb1f29e5"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8)) |  |
| <a id="s-783d9d7a76"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e3a2bfbcb8)) |  |
| <a id="s-f46ca59bb5"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-c3f87877fc"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-e01096e585"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-37963b0805"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c39a64d5e2"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-a412cd82f3"></a>`work` | yes | [WorkIdentity](#s-7f2412f5fb) |  |
| <a id="s-a00bfb028a"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_claim_id](stove0-protocol-executionenvelopepayload-canonical-claim-id.md)
- [bind_target](stove0-protocol-executionenvelopepayload-bind-target.md)

## Governing policies

- <a id="pa-add3ae9004"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelopePayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e3d2135e3c161d8fa3970cedd79c1c749818ff264dac99a59863ac5b13339ab -->

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
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
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
        "target_plan"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-execution-envelope/v1'] = 'stove0-execution-envelope/v1', claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], workflow_plan: stove0_protocol.models.WorkflowPlan, target_plan: stove0_protocol.models.TargetPlanBinding) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ExecutionEnvelopePayload",
  "unit": "export"
}
```

</details>
