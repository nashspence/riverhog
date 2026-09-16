# stove0_protocol.WorkflowPlanPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanpayload:7192923642 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-512741deed"></a>
- <a id="s-70e3f912d6"></a>`distribution`: `stove0-protocol`
- <a id="s-de20089f24"></a>`module`: `stove0_protocol`
- <a id="s-fc2c30d8f6"></a>`name`: `WorkflowPlanPayload`
- <a id="s-d31a97a13d"></a>`unit`: `export`

### Declared structure

- <a id="s-5f75cd08c3"></a>`kind`: `"class"`
- <a id="s-aa1ce26261"></a>`signature`: `"\"(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>) -> None\""`

#### Validated model schema

<a id="s-ad7f837e9e"></a>

- <a id="s-238a1f36a9"></a>`type`: `"object"`
- <a id="s-44fdd80eac"></a>`additionalProperties`: `false`
- <a id="s-8b5a6182b2"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c856c331e1"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-190384559d"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-b74363585b"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-2a7da7c353)) |  |
| <a id="s-68f64d6a4f"></a>`operation` | yes | [OperationRef](#s-2bd403557f) |  |
| <a id="s-2c9c60567b"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-9bdf06b46b)) |  |
| <a id="s-7a04c32c27"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-9bdf06b46b)) |  |
| <a id="s-e0af81e983"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-22be6b1907"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-12bc8c9a39"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-9d52b63ae3"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6917abc58f"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-435008fca6"></a>`work` | yes | [WorkIdentity](#s-507634b90c) |  |

##### Definitions

- [ArtifactSubject](#s-614368d109)
- [BranchWorkBinding](#s-f4d8f3a99b)
- [CollectionId](#s-433cf11bca)
- [CollectionRootRef](#s-61d7090181)
- [EvaluationBinding](#s-1f1edd9ca0)
- [JoinWorkBinding](#s-7f2cb39d9f)
- [JoinWorkMemberBinding](#s-2ef2e02ebe)
- [JsonSchemaDocument](#s-52bdb910f4)
- [JsonValue](#s-9bdf06b46b)
- [ObservationEvidence](#s-2a7da7c353)
- [ObservationFailure](#s-5ad0a958d8)
- [ObservationInapplicable](#s-e67ccec75f)
- [ObservationRequest](#s-6438de4aea)
- [ObservationResult](#s-1853b6d107)
- [ObserverImplementation](#s-769a673957)
- [OperationRef](#s-2bd403557f)
- [RecipeRef](#s-00db1b40e0)
- [WorkIdentity](#s-507634b90c)

##### <a id="s-614368d109"></a>definition `ArtifactSubject`

- <a id="s-136ea2b651"></a>`type`: `"object"`
- <a id="s-759d211395"></a>`additionalProperties`: `false`
- <a id="s-ac79104b64"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b4b2381d75"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-6ad60c03cf"></a>`collection` | yes | [CollectionRootRef](#s-61d7090181) |  |
| <a id="s-d37d2f780c"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-224b7853d2"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-fbc4c9d9b9"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-fd20481d32"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-00ed3a084b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f4d8f3a99b"></a>definition `BranchWorkBinding`

- <a id="s-5866abc70c"></a>`type`: `"object"`
- <a id="s-2aeb7155b6"></a>`additionalProperties`: `false`
- <a id="s-b7f5dab5ac"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c678e0e3f6"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0df1dd3d20"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e49a723d0d"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f68b221be1"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-18ca0f33a0"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-433cf11bca"></a>definition `CollectionId`

- <a id="s-12486e29fd"></a>`type`: `"integer"`
- <a id="s-b733356c48"></a>`minimum`: `1`

##### <a id="s-61d7090181"></a>definition `CollectionRootRef`

- <a id="s-2bb9c0d501"></a>`type`: `"object"`
- <a id="s-c8c4a3263f"></a>`additionalProperties`: `false`
- <a id="s-7ac93d5cf6"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dc863d0aca"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-41e9d13824"></a>`collection_id` | yes | [CollectionId](#s-433cf11bca) |  |
| <a id="s-a08153894c"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1f1edd9ca0"></a>definition `EvaluationBinding`

- <a id="s-7c03c7b2e0"></a>`type`: `"object"`
- <a id="s-2a8553e606"></a>`additionalProperties`: `false`
- <a id="s-38ababfc16"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2c074d863a"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-89f6cd488a"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d4c4371c72"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-9bdf06b46b)) |  |
| <a id="s-416f8286e3"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-7f2cb39d9f"></a>definition `JoinWorkBinding`

- <a id="s-2365ca1f1f"></a>`type`: `"object"`
- <a id="s-ac0994acf0"></a>`additionalProperties`: `false`
- <a id="s-308ae499cd"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab5246c68b"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cac163f686"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-e1e55a4451"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-2ef2e02ebe)); minItems=2 |  |
| <a id="s-f049f88d03"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2ef2e02ebe"></a>definition `JoinWorkMemberBinding`

- <a id="s-b533d3134d"></a>`type`: `"object"`
- <a id="s-34dfb1bd5e"></a>`additionalProperties`: `false`
- <a id="s-f7fa656743"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92f61244e4"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2e95bd0b82"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cb69befea5"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-9e9a5d10b3"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-52bdb910f4"></a>definition `JsonSchemaDocument`

- <a id="s-780a79a81a"></a>`type`: `"object"`
- <a id="s-7669986740"></a>`additionalProperties`: `false`
- <a id="s-6044b699b3"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-03f483cdf3"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-dc54f5451c"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-b642b8d1f9"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d1fdefb963"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-9bdf06b46b)) |  |
| <a id="s-1f1cab14da"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9bdf06b46b"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-2a7da7c353"></a>definition `ObservationEvidence`

- <a id="s-7bfc665064"></a>`type`: `"object"`
- <a id="s-c06787df7f"></a>`additionalProperties`: `false`
- <a id="s-4f9e144174"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd50421393"></a>`request` | yes | [ObservationRequest](#s-6438de4aea) |  |
| <a id="s-6cf4d2d852"></a>`result` | yes | [ObservationResult](#s-1853b6d107) |  |

##### <a id="s-5ad0a958d8"></a>definition `ObservationFailure`

- <a id="s-61fde3cd08"></a>`type`: `"object"`
- <a id="s-7e40634799"></a>`additionalProperties`: `false`
- <a id="s-378767a80a"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-496fca2681"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ad6b340973"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-af3849574d"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-e67ccec75f"></a>definition `ObservationInapplicable`

- <a id="s-3b2a6337aa"></a>`type`: `"object"`
- <a id="s-5059ddc7dc"></a>`additionalProperties`: `false`
- <a id="s-65503fa205"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee05c43892"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cde8b8fa6c"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-6438de4aea"></a>definition `ObservationRequest`

- <a id="s-fa905b8f7c"></a>`type`: `"object"`
- <a id="s-f65e3a1f90"></a>`additionalProperties`: `false`
- <a id="s-60f67f4a24"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88b876fefc"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-f6d2cc7033"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-f5e6641e0a"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-66d8c48b0a"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a3462bed9c"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-997714f5da"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-35e1307b83"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-9bdf06b46b)) |  |
| <a id="s-0bf13af004"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2433e387bb"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-c89ba172cb"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-614368d109)); minItems=1 |  |
| <a id="s-b85564c4f2"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-892037ff4d"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1853b6d107"></a>definition `ObservationResult`

- <a id="s-b7bebfdf9e"></a>`type`: `"object"`
- <a id="s-aaa14c7c5f"></a>`additionalProperties`: `false`
- <a id="s-d195b01244"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c8bd3d0ab0"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-9bdf06b46b)) |  |
| <a id="s-0b977a5067"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-9bdf06b46b))); (type="null")]; default=null |  |
| <a id="s-cd117d8e45"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-52bdb910f4)); (type="null")]; default=null |  |
| <a id="s-6f128cb999"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-2dc410f39e"></a>`failure` | no | anyOf=[([ObservationFailure](#s-5ad0a958d8)); (type="null")]; default=null |  |
| <a id="s-4a9eaf693d"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-ed5d4af023"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-e67ccec75f)); (type="null")]; default=null |  |
| <a id="s-f224094650"></a>`observer` | yes | [ObserverImplementation](#s-769a673957) |  |
| <a id="s-b765a7f99b"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e425346b3d"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-64a27ffe62"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a7f9c33dec"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2cc8eeeeff"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-95a48ce699"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-614368d109)); minItems=1 |  |

##### <a id="s-769a673957"></a>definition `ObserverImplementation`

- <a id="s-c4c0be7871"></a>`type`: `"object"`
- <a id="s-1b912dfc36"></a>`additionalProperties`: `false`
- <a id="s-1551efae1d"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e66bc2074"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-61e587da8d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-75526b833f"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-a76b4d2a41"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-dad2f33f22"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-2bd403557f"></a>definition `OperationRef`

- <a id="s-4f1b45f64a"></a>`type`: `"object"`
- <a id="s-6fe12277d3"></a>`additionalProperties`: `false`
- <a id="s-9ab2b3f087"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-696aa3d7ba"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7f0e600e35"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-00db1b40e0"></a>definition `RecipeRef`

- <a id="s-53fe7bdead"></a>`type`: `"object"`
- <a id="s-9207aa8c9c"></a>`additionalProperties`: `false`
- <a id="s-27fada42b5"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9bb0dd32df"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-20c581c26b"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-af14ad7746"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-507634b90c"></a>definition `WorkIdentity`

- <a id="s-7881b59094"></a>`type`: `"object"`
- <a id="s-72d9a8e3e8"></a>`additionalProperties`: `false`
- <a id="s-300aed7d4a"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-96d4393987"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-9bdf06b46b)) |  |
| <a id="s-cba85822cb"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-1f1edd9ca0)); (type="null")]; default=null |  |
| <a id="s-fd30fedbf9"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-f4d8f3a99b)); ([JoinWorkBinding](#s-7f2cb39d9f))]); (type="null")]; default=null |  |
| <a id="s-0129a69852"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-f17aed9aad"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-61d7090181)); minItems=1 |  |
| <a id="s-e7f53f65d4"></a>`recipe` | yes | [RecipeRef](#s-00db1b40e0) |  |
| <a id="s-5df298b83e"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_observations](stove0-protocol-workflowplanpayload-canonical-observations.md)
- [protect_evaluation_sources](stove0-protocol-workflowplanpayload-protect-evaluation-sources.md)

## Governing policies

- <a id="pa-fed3628ef6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e7a42163280954261f14a8a6be964fce7a76386d1cf7d2c8411b7f09d937b56 -->

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
        }
      },
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
        }
      },
      "required": [
        "work",
        "operation",
        "target_registration_id",
        "target_contract_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPlanPayload",
  "unit": "export"
}
```

</details>
