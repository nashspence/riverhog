# stove0_protocol.BranchPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchplan:03d66abe2c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7eaf409fc2"></a>
- <a id="s-1c53b5d912"></a>`distribution`: `stove0-protocol`
- <a id="s-c8cb5e6e65"></a>`module`: `stove0_protocol`
- <a id="s-c5641b2544"></a>`name`: `BranchPlan`
- <a id="s-d5c26121bf"></a>`unit`: `export`

### Declared structure

- <a id="s-99472d7a8d"></a>`kind`: `"class"`
- <a id="s-9dd8338568"></a>`signature`: `"\"(*, kind: Literal['leaf'] = 'leaf', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef, workflow_plan: stove0_protocol.models.WorkflowPlan) -> None\""`

#### Validated model schema

<a id="s-84d2f1b734"></a>

- <a id="s-2c07c39eaa"></a>`type`: `"object"`
- <a id="s-0b0f9cdc62"></a>`additionalProperties`: `false`
- <a id="s-386ed5cd68"></a>`required`: `["branch_id","artifact_selection","workflow_plan"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-57ca839a2c"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-f9d4646841) |  |
| <a id="s-24d7fcc523"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-534772afc8"></a>`kind` | no | type="string"; const="leaf"; default="leaf" |  |
| <a id="s-c9cd117423"></a>`workflow_plan` | yes | [WorkflowPlan](#s-20b2e6825f) |  |

##### Definitions

- [ArtifactSelectionRef](#s-f9d4646841)
- [ArtifactSubject](#s-587a6b6747)
- [BranchWorkBinding](#s-2f7c9d11af)
- [CollectionId](#s-20328cebdb)
- [CollectionRootRef](#s-cc696fc58f)
- [EvaluationBinding](#s-1d6708d93b)
- [JoinWorkBinding](#s-e616a9e832)
- [JoinWorkMemberBinding](#s-240ad0ae62)
- [JsonSchemaDocument](#s-a1730a9e72)
- [JsonValue](#s-027a198060)
- [ObservationEvidence](#s-86bb712be5)
- [ObservationFailure](#s-7bc4262e6f)
- [ObservationInapplicable](#s-bea240a5e2)
- [ObservationRequest](#s-ecf4c53a60)
- [ObservationResult](#s-9125ffeb64)
- [ObserverImplementation](#s-c6108de4dd)
- [OperationRef](#s-e8bd89d36b)
- [RecipeRef](#s-fdaeca286b)
- [WorkIdentity](#s-55cfd70fdd)
- [WorkflowPlan](#s-20b2e6825f)

##### <a id="s-f9d4646841"></a>definition `ArtifactSelectionRef`

- <a id="s-f1c04a330e"></a>`type`: `"object"`
- <a id="s-fd1e75b697"></a>`additionalProperties`: `false`
- <a id="s-e3aedf52fe"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ff7e3c70f"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-9d54c95ec9"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2336b552c6"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-587a6b6747"></a>definition `ArtifactSubject`

- <a id="s-630d207398"></a>`type`: `"object"`
- <a id="s-6a4b0672ad"></a>`additionalProperties`: `false`
- <a id="s-84af95e084"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4bdf33b664"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-80c8458628"></a>`collection` | yes | [CollectionRootRef](#s-cc696fc58f) |  |
| <a id="s-c16b68b0f3"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-b4373897c0"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-1fd7c18b21"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-665ce7aa90"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-791d887b28"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2f7c9d11af"></a>definition `BranchWorkBinding`

- <a id="s-7e1449c40c"></a>`type`: `"object"`
- <a id="s-91dc951e40"></a>`additionalProperties`: `false`
- <a id="s-62907d8fec"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-79de3477dd"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ea9387792b"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-27fa961a34"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-44f3fbd54f"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-d3d62c1aa6"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-20328cebdb"></a>definition `CollectionId`

- <a id="s-9ee06961d6"></a>`type`: `"integer"`
- <a id="s-aac52340a7"></a>`minimum`: `1`

##### <a id="s-cc696fc58f"></a>definition `CollectionRootRef`

- <a id="s-6fb8362f1c"></a>`type`: `"object"`
- <a id="s-28d8b17861"></a>`additionalProperties`: `false`
- <a id="s-84da9c6418"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e424889e96"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b33542f794"></a>`collection_id` | yes | [CollectionId](#s-20328cebdb) |  |
| <a id="s-1fe38fa156"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1d6708d93b"></a>definition `EvaluationBinding`

- <a id="s-281e27b03e"></a>`type`: `"object"`
- <a id="s-160127469f"></a>`additionalProperties`: `false`
- <a id="s-e81a76a579"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3f76f2f816"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e661dfdc28"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa8f8f7b4b"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-3e64fc3740"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-e616a9e832"></a>definition `JoinWorkBinding`

- <a id="s-f9e31ed445"></a>`type`: `"object"`
- <a id="s-31c77f5325"></a>`additionalProperties`: `false`
- <a id="s-1169927e08"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7efc4333c1"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5f8c3efc01"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-151e117fa3"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-240ad0ae62)); minItems=2 |  |
| <a id="s-62a4923aaa"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-240ad0ae62"></a>definition `JoinWorkMemberBinding`

- <a id="s-9b24af074a"></a>`type`: `"object"`
- <a id="s-a8c8549d0b"></a>`additionalProperties`: `false`
- <a id="s-63bc82700a"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66750544d1"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8555186b38"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4806fffc9c"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-bcc2de4e38"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a1730a9e72"></a>definition `JsonSchemaDocument`

- <a id="s-fe05de9391"></a>`type`: `"object"`
- <a id="s-448d46abbd"></a>`additionalProperties`: `false`
- <a id="s-7beb641bae"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bf31c8c69c"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-55a87207cc"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-c9d0931e02"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-68cf6183ae"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-af86b9a6eb"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-027a198060"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-86bb712be5"></a>definition `ObservationEvidence`

- <a id="s-28c74ec85f"></a>`type`: `"object"`
- <a id="s-e12ae8c617"></a>`additionalProperties`: `false`
- <a id="s-e04ad71dfe"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a199f9dd3"></a>`request` | yes | [ObservationRequest](#s-ecf4c53a60) |  |
| <a id="s-c8e06172de"></a>`result` | yes | [ObservationResult](#s-9125ffeb64) |  |

##### <a id="s-7bc4262e6f"></a>definition `ObservationFailure`

- <a id="s-9fd625d0e7"></a>`type`: `"object"`
- <a id="s-7195a5b8f9"></a>`additionalProperties`: `false`
- <a id="s-358a122a27"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c88ae4aa14"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bed383d9a0"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-c219056690"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-bea240a5e2"></a>definition `ObservationInapplicable`

- <a id="s-96dfa88927"></a>`type`: `"object"`
- <a id="s-db0cc1c06e"></a>`additionalProperties`: `false`
- <a id="s-6299e9ee1f"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7f2420affb"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9123a247ac"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-ecf4c53a60"></a>definition `ObservationRequest`

- <a id="s-2a0bfe041a"></a>`type`: `"object"`
- <a id="s-39485da9fa"></a>`additionalProperties`: `false`
- <a id="s-5d96009867"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b319382651"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-afce912b44"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-426609bff3"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-64c151a9c9"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-07bf870a46"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-13ea6bd81f"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-ccf1813412"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-d17a83821c"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5597e6ecf6"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-275f96b5b9"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-587a6b6747)); minItems=1 |  |
| <a id="s-0118c16034"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-defa860729"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9125ffeb64"></a>definition `ObservationResult`

- <a id="s-2390c589d2"></a>`type`: `"object"`
- <a id="s-b71f73b54b"></a>`additionalProperties`: `false`
- <a id="s-e34db68986"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cbd0b9f718"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-dd63f98ee6"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-027a198060))); (type="null")]; default=null |  |
| <a id="s-a4e75b0480"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-a1730a9e72)); (type="null")]; default=null |  |
| <a id="s-2eb17f2ba0"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a5e3315584"></a>`failure` | no | anyOf=[([ObservationFailure](#s-7bc4262e6f)); (type="null")]; default=null |  |
| <a id="s-6c31846f84"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-7f085991db"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-bea240a5e2)); (type="null")]; default=null |  |
| <a id="s-9a7196c9ca"></a>`observer` | yes | [ObserverImplementation](#s-c6108de4dd) |  |
| <a id="s-c090a02223"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-06fd98f69b"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a7f567bb55"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c21188c2a3"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-05d4f2318e"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-ef55835f3a"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-587a6b6747)); minItems=1 |  |

##### <a id="s-c6108de4dd"></a>definition `ObserverImplementation`

- <a id="s-2392b75823"></a>`type`: `"object"`
- <a id="s-4150b3a73f"></a>`additionalProperties`: `false`
- <a id="s-49e1abb706"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d2b53e5acf"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-65a97c1fc9"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e3ae4fd3ad"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-299ad737e9"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-275fa8406b"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-e8bd89d36b"></a>definition `OperationRef`

- <a id="s-9e36320740"></a>`type`: `"object"`
- <a id="s-4cf078288b"></a>`additionalProperties`: `false`
- <a id="s-2c8183d8ef"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-49d86f20d6"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-396ee7a98a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fdaeca286b"></a>definition `RecipeRef`

- <a id="s-cd91cdab20"></a>`type`: `"object"`
- <a id="s-66b8e59ead"></a>`additionalProperties`: `false`
- <a id="s-0822ae55dd"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0c6494e1c3"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-332ce97e47"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-866916f80e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-55cfd70fdd"></a>definition `WorkIdentity`

- <a id="s-bbe8e0b4ae"></a>`type`: `"object"`
- <a id="s-54da837c38"></a>`additionalProperties`: `false`
- <a id="s-8a1328e66a"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-117a2a2b58"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-c27d6aa34b"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-1d6708d93b)); (type="null")]; default=null |  |
| <a id="s-6600bc01cf"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-2f7c9d11af)); ([JoinWorkBinding](#s-e616a9e832))]); (type="null")]; default=null |  |
| <a id="s-2027772824"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-ad64c5baaf"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-cc696fc58f)); minItems=1 |  |
| <a id="s-5f1e6f28af"></a>`recipe` | yes | [RecipeRef](#s-fdaeca286b) |  |
| <a id="s-2c957295cd"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-20b2e6825f"></a>definition `WorkflowPlan`

- <a id="s-442aa6f472"></a>`type`: `"object"`
- <a id="s-baf3431c32"></a>`additionalProperties`: `false`
- <a id="s-debe9fb3e5"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df163da59d"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-759bac93ce"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-0de64c8ae4"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-86bb712be5)) |  |
| <a id="s-3b8b248bf5"></a>`operation` | yes | [OperationRef](#s-e8bd89d36b) |  |
| <a id="s-ba0ed19f52"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-fedfddb994"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-6edfe95abf"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-c0160ea2e8"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-d91a43c3d3"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-55fe995444"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b1f75ff0b"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-7479cb7635"></a>`work` | yes | [WorkIdentity](#s-55cfd70fdd) |  |
| <a id="s-c42dbed097"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [bind_child_work](stove0-protocol-branchplan-bind-child-work.md)
- [build](stove0-protocol-branchplan-build.md)

## Governing policies

- <a id="pa-639ccb615e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50e9213c819d0ce79fc40802db05848be840a24e4bcaac63115b39e17975fea6 -->

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
    "signature": "\"(*, kind: Literal['leaf'] = 'leaf', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef, workflow_plan: stove0_protocol.models.WorkflowPlan) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchPlan",
  "unit": "export"
}
```

</details>
