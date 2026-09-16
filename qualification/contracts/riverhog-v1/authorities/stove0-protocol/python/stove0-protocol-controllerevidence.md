# stove0_protocol.ControllerEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-controllerevidence:2b8e594eda -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3811541496"></a>
- <a id="s-352d403811"></a>`distribution`: `stove0-protocol`
- <a id="s-4f1920641f"></a>`module`: `stove0_protocol`
- <a id="s-4de31b5966"></a>`name`: `ControllerEvidence`
- <a id="s-2f2f608996"></a>`unit`: `export`

### Declared structure

- <a id="s-be8f0fe0f4"></a>`kind`: `"class"`
- <a id="s-3c9373ee6d"></a>`signature`: `"\"(*, format: Literal['stove0-controller-evidence/v1'] = 'stove0-controller-evidence/v1', execution_envelope: stove0_protocol.models.ExecutionEnvelope, controller_evidence_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-bcd420934d"></a>

- <a id="s-0a39ac9dcc"></a>`type`: `"object"`
- <a id="s-db5897709b"></a>`additionalProperties`: `false`
- <a id="s-9b5bdaa275"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc17e0f4c2"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3b527b4b2a"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-262f53bb3c) |  |
| <a id="s-628e4c6245"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### Definitions

- [ArtifactSubject](#s-f8675c3ccc)
- [BranchWorkBinding](#s-11e18814aa)
- [CollectionId](#s-a5d2848801)
- [CollectionRootRef](#s-81d94b131e)
- [EvaluationBinding](#s-9267c46b0f)
- [ExecutionEnvelope](#s-262f53bb3c)
- [JoinWorkBinding](#s-7f87535079)
- [JoinWorkMemberBinding](#s-740e0259c4)
- [JsonSchemaDocument](#s-a4bf1a8686)
- [JsonValue](#s-712fb83f77)
- [ObservationEvidence](#s-48b8056b8b)
- [ObservationFailure](#s-e6e1e17120)
- [ObservationInapplicable](#s-18c738bff2)
- [ObservationRequest](#s-21590873a2)
- [ObservationResult](#s-ce5a4e7e9e)
- [ObserverImplementation](#s-2fdb49b7e9)
- [OperationRef](#s-62244fbc0b)
- [RecipeRef](#s-f0b24656de)
- [TargetPlanBinding](#s-3eae18a492)
- [WorkIdentity](#s-b89bbc020f)
- [WorkflowPlan](#s-1b718a16db)

##### <a id="s-f8675c3ccc"></a>definition `ArtifactSubject`

- <a id="s-1e3321ea87"></a>`type`: `"object"`
- <a id="s-7f1fbd5bbd"></a>`additionalProperties`: `false`
- <a id="s-8a2563ead8"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b86df91d47"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-f5f23b563d"></a>`collection` | yes | [CollectionRootRef](#s-81d94b131e) |  |
| <a id="s-c10de6cd84"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-2c9d3e38f3"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-4d88fb776f"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-a09599afd8"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-32a6486249"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-11e18814aa"></a>definition `BranchWorkBinding`

- <a id="s-a948024e9d"></a>`type`: `"object"`
- <a id="s-4e88e74e59"></a>`additionalProperties`: `false`
- <a id="s-b74a024b35"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-99a3fb830d"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5e1322b8fe"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-75ddaa8a08"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-145898b76a"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-4341005e40"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a5d2848801"></a>definition `CollectionId`

- <a id="s-5b4f9c7c68"></a>`type`: `"integer"`
- <a id="s-af19790e00"></a>`minimum`: `1`

##### <a id="s-81d94b131e"></a>definition `CollectionRootRef`

- <a id="s-8731467fea"></a>`type`: `"object"`
- <a id="s-e7d8acf882"></a>`additionalProperties`: `false`
- <a id="s-6eac7a68bb"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b5d5e0dc54"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-14aa5220db"></a>`collection_id` | yes | [CollectionId](#s-a5d2848801) |  |
| <a id="s-7411801b97"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9267c46b0f"></a>definition `EvaluationBinding`

- <a id="s-7a04e5ddb3"></a>`type`: `"object"`
- <a id="s-7f74c2ba0d"></a>`additionalProperties`: `false`
- <a id="s-41f80b3a67"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f7ca9653f1"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-43c775102e"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b13618b3da"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-712fb83f77)) |  |
| <a id="s-6e0a4daa52"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-262f53bb3c"></a>definition `ExecutionEnvelope`

- <a id="s-8083432900"></a>`type`: `"object"`
- <a id="s-e26dcd3c8a"></a>`additionalProperties`: `false`
- <a id="s-9eb270e622"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4ce66515cd"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-77c1416e1c"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-456da999cb"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-693f82dcca"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-9eae8015de"></a>`target_plan` | yes | [TargetPlanBinding](#s-3eae18a492) |  |
| <a id="s-4261d0b2ee"></a>`workflow_plan` | yes | [WorkflowPlan](#s-1b718a16db) |  |

##### <a id="s-7f87535079"></a>definition `JoinWorkBinding`

- <a id="s-f8d918e352"></a>`type`: `"object"`
- <a id="s-c7a493a3dd"></a>`additionalProperties`: `false`
- <a id="s-1bf0f434c9"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac84ed0041"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ebbe4ffeff"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-b5c6a7edb0"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-740e0259c4)); minItems=2 |  |
| <a id="s-b17a5c1d66"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-740e0259c4"></a>definition `JoinWorkMemberBinding`

- <a id="s-d17d6ec7cd"></a>`type`: `"object"`
- <a id="s-c941e33f58"></a>`additionalProperties`: `false`
- <a id="s-a3b8f296c9"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a5ed243d8a"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e3eab4adaa"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1f4d6c73e8"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-c04fbc2d2a"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a4bf1a8686"></a>definition `JsonSchemaDocument`

- <a id="s-a9da1c9999"></a>`type`: `"object"`
- <a id="s-71e31c1f91"></a>`additionalProperties`: `false`
- <a id="s-d6660fe499"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-543653f16f"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-bc84fe4db4"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-edaf0cc7ec"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-540357d61b"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-712fb83f77)) |  |
| <a id="s-430e14b491"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-712fb83f77"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-48b8056b8b"></a>definition `ObservationEvidence`

- <a id="s-f32cf65518"></a>`type`: `"object"`
- <a id="s-46b598cc68"></a>`additionalProperties`: `false`
- <a id="s-faa3c63e9e"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ef16b9e7ac"></a>`request` | yes | [ObservationRequest](#s-21590873a2) |  |
| <a id="s-e9e28c3c57"></a>`result` | yes | [ObservationResult](#s-ce5a4e7e9e) |  |

##### <a id="s-e6e1e17120"></a>definition `ObservationFailure`

- <a id="s-e454561b25"></a>`type`: `"object"`
- <a id="s-ccfdb10657"></a>`additionalProperties`: `false`
- <a id="s-63ac6ebb68"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f2dd3a7222"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0f3ed4babc"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-4486d997fb"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-18c738bff2"></a>definition `ObservationInapplicable`

- <a id="s-ee6eb5492d"></a>`type`: `"object"`
- <a id="s-6ab44995a7"></a>`additionalProperties`: `false`
- <a id="s-f5ff51d9f3"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-53de27db20"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-91587fcd39"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-21590873a2"></a>definition `ObservationRequest`

- <a id="s-db774c5d15"></a>`type`: `"object"`
- <a id="s-c744977248"></a>`additionalProperties`: `false`
- <a id="s-e28f29a2f5"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-39ffc758ed"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-10a814c0ad"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-3936028e86"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-737b5bc8cf"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f1c0b36b20"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8af5e13e2c"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-9488e68919"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-712fb83f77)) |  |
| <a id="s-13c708d603"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9984c6ee85"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-f146eca966"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-f8675c3ccc)); minItems=1 |  |
| <a id="s-aba86cf8d5"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-27f2441faa"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ce5a4e7e9e"></a>definition `ObservationResult`

- <a id="s-36a7339918"></a>`type`: `"object"`
- <a id="s-7fc1947ce5"></a>`additionalProperties`: `false`
- <a id="s-e568ab9c05"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-32fd8d96a7"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-712fb83f77)) |  |
| <a id="s-af3a09ac45"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-712fb83f77))); (type="null")]; default=null |  |
| <a id="s-534895b915"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-a4bf1a8686)); (type="null")]; default=null |  |
| <a id="s-7c18f22ae6"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-73b4573afc"></a>`failure` | no | anyOf=[([ObservationFailure](#s-e6e1e17120)); (type="null")]; default=null |  |
| <a id="s-6ab2b02740"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-a1360899fb"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-18c738bff2)); (type="null")]; default=null |  |
| <a id="s-e9822351fc"></a>`observer` | yes | [ObserverImplementation](#s-2fdb49b7e9) |  |
| <a id="s-42bd27cc0a"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ae9d585c09"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-abae3f1f76"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-55e5e8552f"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-be35e6d36d"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-d025453621"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-f8675c3ccc)); minItems=1 |  |

##### <a id="s-2fdb49b7e9"></a>definition `ObserverImplementation`

- <a id="s-257cea55af"></a>`type`: `"object"`
- <a id="s-8315729654"></a>`additionalProperties`: `false`
- <a id="s-ff092cf061"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b673ec8292"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b12710036"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a002c7f128"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-537f78747e"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-7ec67102f8"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-62244fbc0b"></a>definition `OperationRef`

- <a id="s-296276b7e0"></a>`type`: `"object"`
- <a id="s-c10382ee03"></a>`additionalProperties`: `false`
- <a id="s-96dbd60ef2"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-31bfd059f5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fd7e1c48f5"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f0b24656de"></a>definition `RecipeRef`

- <a id="s-536c6aaef3"></a>`type`: `"object"`
- <a id="s-9b9fdafe82"></a>`additionalProperties`: `false`
- <a id="s-f7eeb28f47"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e0974af12a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0020bed2ab"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-3bd91cbff7"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3eae18a492"></a>definition `TargetPlanBinding`

- <a id="s-fe706422be"></a>`type`: `"object"`
- <a id="s-02d6f62e8b"></a>`additionalProperties`: `false`
- <a id="s-e00f99c172"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-092100ea20"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e2ebb69c13"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-712fb83f77)) |  |
| <a id="s-6c7bd10eda"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-12c70abcdb"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d9cd1d62a6"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-52e955725e"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-b89bbc020f"></a>definition `WorkIdentity`

- <a id="s-2e8c8a20eb"></a>`type`: `"object"`
- <a id="s-ad3681ddc9"></a>`additionalProperties`: `false`
- <a id="s-4d26b78036"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25d7c61bd5"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-712fb83f77)) |  |
| <a id="s-0a627fd974"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-9267c46b0f)); (type="null")]; default=null |  |
| <a id="s-f7b36a8063"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-11e18814aa)); ([JoinWorkBinding](#s-7f87535079))]); (type="null")]; default=null |  |
| <a id="s-54c782eb3d"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-9c429bc06c"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-81d94b131e)); minItems=1 |  |
| <a id="s-00584e50fb"></a>`recipe` | yes | [RecipeRef](#s-f0b24656de) |  |
| <a id="s-bc6ef5160b"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1b718a16db"></a>definition `WorkflowPlan`

- <a id="s-9f3e258181"></a>`type`: `"object"`
- <a id="s-a0243885d1"></a>`additionalProperties`: `false`
- <a id="s-b6f255a2b7"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dbafa9b336"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-a438be1f27"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-aa23e6c256"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-48b8056b8b)) |  |
| <a id="s-ebb5e9104f"></a>`operation` | yes | [OperationRef](#s-62244fbc0b) |  |
| <a id="s-68d4cf33ad"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-712fb83f77)) |  |
| <a id="s-f405495ef8"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-712fb83f77)) |  |
| <a id="s-39b7dc5137"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-a6844263dd"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-8bdfa7af02"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-9e6f74af3d"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d5951a0158"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-30d625d862"></a>`work` | yes | [WorkIdentity](#s-b89bbc020f) |  |
| <a id="s-922f5a5e26"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [seal](stove0-protocol-controllerevidence-seal.md)
- [verify_digest](stove0-protocol-controllerevidence-verify-digest.md)

## Governing policies

- <a id="pa-3dce469917"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ControllerEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9140b8ff10d6b44470984711be8e5d30e9106f7765508ae21409b3ec2d7d96d6 -->

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
    "signature": "\"(*, format: Literal['stove0-controller-evidence/v1'] = 'stove0-controller-evidence/v1', execution_envelope: stove0_protocol.models.ExecutionEnvelope, controller_evidence_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ControllerEvidence",
  "unit": "export"
}
```

</details>
