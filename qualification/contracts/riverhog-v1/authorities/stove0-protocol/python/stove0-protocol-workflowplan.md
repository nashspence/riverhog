# stove0_protocol.WorkflowPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplan:1051bd4de8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-16449ced4b"></a>
- <a id="s-ce3685dc1d"></a>`distribution`: `stove0-protocol`
- <a id="s-db14ca4d38"></a>`module`: `stove0_protocol`
- <a id="s-aa36e507f9"></a>`name`: `WorkflowPlan`
- <a id="s-4de37a1b2e"></a>`unit`: `export`

### Declared structure

- <a id="s-43193476da"></a>`kind`: `"class"`
- <a id="s-f761ab25e0"></a>`signature`: `"\"(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>, workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-e5ad9a5d0a"></a>

- <a id="s-1cc007c411"></a>`type`: `"object"`
- <a id="s-a92ebdb0ee"></a>`additionalProperties`: `false`
- <a id="s-4355e9b429"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4998011bdb"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-db8ee37eec"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-e36ced0ec4"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-e27ba5e565)) |  |
| <a id="s-3b48301872"></a>`operation` | yes | [OperationRef](#s-b18790f58d) |  |
| <a id="s-8a4e38ece3"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-800d18517c"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-4d361b5e5c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-92c92d7079"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-718eac81c4"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-3c6878b5cf"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8ba9ed12fe"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-ea56bd7420"></a>`work` | yes | [WorkIdentity](#s-f185cccf70) |  |
| <a id="s-de45a84b7a"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSubject](#s-998ebaf184)
- [BranchWorkBinding](#s-a783a0b6d9)
- [CollectionId](#s-05b21cd0dc)
- [CollectionRootRef](#s-0435619c5e)
- [EvaluationBinding](#s-d85a23e820)
- [JoinWorkBinding](#s-5d9b2b6117)
- [JoinWorkMemberBinding](#s-3e6462b479)
- [JsonSchemaDocument](#s-268660e515)
- [JsonValue](#s-4fc3efab64)
- [ObservationEvidence](#s-e27ba5e565)
- [ObservationFailure](#s-1e3302875e)
- [ObservationInapplicable](#s-9336c38702)
- [ObservationRequest](#s-0f8396d6c4)
- [ObservationResult](#s-de9583a242)
- [ObserverImplementation](#s-982e1d84b8)
- [OperationRef](#s-b18790f58d)
- [RecipeRef](#s-42877ab73f)
- [WorkIdentity](#s-f185cccf70)

##### <a id="s-998ebaf184"></a>definition `ArtifactSubject`

- <a id="s-91224a8714"></a>`type`: `"object"`
- <a id="s-9e4bb7354b"></a>`additionalProperties`: `false`
- <a id="s-2a6d2f2116"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-63e219e27e"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-9f3891ccd0"></a>`collection` | yes | [CollectionRootRef](#s-0435619c5e) |  |
| <a id="s-f4ffa01a39"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-ac25df9aab"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-0d74b1bec6"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-00480f186a"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-235fadbd0d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a783a0b6d9"></a>definition `BranchWorkBinding`

- <a id="s-9c467ac641"></a>`type`: `"object"`
- <a id="s-019424f71d"></a>`additionalProperties`: `false`
- <a id="s-8ee03d432b"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e15ebcae1a"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ae282d3b7d"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5cac6f6960"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-49b83d9bde"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-4b9befc02b"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-05b21cd0dc"></a>definition `CollectionId`

- <a id="s-39e12ae93f"></a>`type`: `"integer"`
- <a id="s-5f5f1efdc6"></a>`minimum`: `1`

##### <a id="s-0435619c5e"></a>definition `CollectionRootRef`

- <a id="s-206b4db78a"></a>`type`: `"object"`
- <a id="s-7d91a6610f"></a>`additionalProperties`: `false`
- <a id="s-97713054a2"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-613e89d9b0"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eabd363e38"></a>`collection_id` | yes | [CollectionId](#s-05b21cd0dc) |  |
| <a id="s-649a8049db"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d85a23e820"></a>definition `EvaluationBinding`

- <a id="s-754e66abca"></a>`type`: `"object"`
- <a id="s-9a653f47dd"></a>`additionalProperties`: `false`
- <a id="s-1b13fca791"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7ecc9dc135"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0ac00e6b2d"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d5e576a38"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-a693810ff9"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-5d9b2b6117"></a>definition `JoinWorkBinding`

- <a id="s-f21d4b2d53"></a>`type`: `"object"`
- <a id="s-e279d9e32c"></a>`additionalProperties`: `false`
- <a id="s-457d0a8161"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b12c78ad9"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9840a3dbea"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-1f6e485da0"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-3e6462b479)); minItems=2 |  |
| <a id="s-82b9f19c21"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3e6462b479"></a>definition `JoinWorkMemberBinding`

- <a id="s-96f526d674"></a>`type`: `"object"`
- <a id="s-4ed95f65ed"></a>`additionalProperties`: `false`
- <a id="s-71023bfdf8"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba74a3cc27"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e41a49b45a"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5061049d1c"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-09de199bbb"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-268660e515"></a>definition `JsonSchemaDocument`

- <a id="s-8dcb5ad23d"></a>`type`: `"object"`
- <a id="s-77cd021f82"></a>`additionalProperties`: `false`
- <a id="s-5fd29a0956"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1b691d934"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-77642692f2"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-569e9f26c4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a386c404a5"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-ee0715edd0"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4fc3efab64"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-e27ba5e565"></a>definition `ObservationEvidence`

- <a id="s-e35cc8b94d"></a>`type`: `"object"`
- <a id="s-fa2344792c"></a>`additionalProperties`: `false`
- <a id="s-82ec5ed959"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-18a71f6422"></a>`request` | yes | [ObservationRequest](#s-0f8396d6c4) |  |
| <a id="s-80a04a8fd6"></a>`result` | yes | [ObservationResult](#s-de9583a242) |  |

##### <a id="s-1e3302875e"></a>definition `ObservationFailure`

- <a id="s-913f05aff0"></a>`type`: `"object"`
- <a id="s-518119234c"></a>`additionalProperties`: `false`
- <a id="s-338f8d0861"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60fafa7605"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d36763a92f"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-74b08ee603"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-9336c38702"></a>definition `ObservationInapplicable`

- <a id="s-3ed871dd3e"></a>`type`: `"object"`
- <a id="s-a6e5ac0d03"></a>`additionalProperties`: `false`
- <a id="s-e95e0d7fb4"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8b5b716256"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b3cb78aa9d"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-0f8396d6c4"></a>definition `ObservationRequest`

- <a id="s-a2819cdbf1"></a>`type`: `"object"`
- <a id="s-233fb789d7"></a>`additionalProperties`: `false`
- <a id="s-a38dc3c8a9"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c6dde0fd1"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-e0c546377a"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-1aa5818c03"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-294745b87f"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-18b71d3a8f"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ba12776aae"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-bbcedaf0b7"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-1e68f53f0c"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-21087daca0"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-c2108b152c"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-998ebaf184)); minItems=1 |  |
| <a id="s-07f33c518a"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-d76ae63e35"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-de9583a242"></a>definition `ObservationResult`

- <a id="s-483a10cd3e"></a>`type`: `"object"`
- <a id="s-f9bc457185"></a>`additionalProperties`: `false`
- <a id="s-be1b409900"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d244c3bb0"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-6c205a3fd2"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-4fc3efab64))); (type="null")]; default=null |  |
| <a id="s-6f55240053"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-268660e515)); (type="null")]; default=null |  |
| <a id="s-a5e905d55d"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-8f38591e19"></a>`failure` | no | anyOf=[([ObservationFailure](#s-1e3302875e)); (type="null")]; default=null |  |
| <a id="s-680b7f3372"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-b3d605770c"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-9336c38702)); (type="null")]; default=null |  |
| <a id="s-f5bc499d84"></a>`observer` | yes | [ObserverImplementation](#s-982e1d84b8) |  |
| <a id="s-3c2359c39b"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a58cbb1158"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f96975ee65"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c484b300bf"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d81a7ee6cd"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-329dc21de6"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-998ebaf184)); minItems=1 |  |

##### <a id="s-982e1d84b8"></a>definition `ObserverImplementation`

- <a id="s-2a1c910931"></a>`type`: `"object"`
- <a id="s-a3dc5d8e87"></a>`additionalProperties`: `false`
- <a id="s-0e721a3f4d"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1b9f8cabec"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8fa08caeda"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-867a0bf8b7"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-3aa227c2d7"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-7eb1f0cf99"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-b18790f58d"></a>definition `OperationRef`

- <a id="s-974964778f"></a>`type`: `"object"`
- <a id="s-16b5c8a23d"></a>`additionalProperties`: `false`
- <a id="s-93f884f885"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e9eabe683a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-eb9a6fc400"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-42877ab73f"></a>definition `RecipeRef`

- <a id="s-176a12836a"></a>`type`: `"object"`
- <a id="s-00098868fd"></a>`additionalProperties`: `false`
- <a id="s-6fd6674d46"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ced9762e2b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dce83f41cd"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-b59344c170"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f185cccf70"></a>definition `WorkIdentity`

- <a id="s-4f68a62b15"></a>`type`: `"object"`
- <a id="s-f160af578a"></a>`additionalProperties`: `false`
- <a id="s-e0289ac13c"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e4557f20fd"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-95f73ff9fc"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-d85a23e820)); (type="null")]; default=null |  |
| <a id="s-c6df4f3de7"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-a783a0b6d9)); ([JoinWorkBinding](#s-5d9b2b6117))]); (type="null")]; default=null |  |
| <a id="s-b1ebc97341"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-6158bde4b6"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-0435619c5e)); minItems=1 |  |
| <a id="s-39606a0959"></a>`recipe` | yes | [RecipeRef](#s-42877ab73f) |  |
| <a id="s-e946b20d62"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_observations](stove0-protocol-workflowplan-canonical-observations.md)
- [protect_evaluation_sources](stove0-protocol-workflowplan-protect-evaluation-sources.md)
- [seal](stove0-protocol-workflowplan-seal.md)
- [verify_digest](stove0-protocol-workflowplan-verify-digest.md)

## Governing policies

- <a id="pa-35e57ffe6b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6e64a97e1b72810ffbccfbce46a962e9736c67281599c060a7d8efb121e282a -->

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
    "signature": "\"(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>, workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPlan",
  "unit": "export"
}
```

</details>
