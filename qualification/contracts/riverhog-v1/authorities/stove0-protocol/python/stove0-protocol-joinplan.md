# stove0_protocol.JoinPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinplan:43f4428e78 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ccfe9b231e"></a>
- <a id="s-1010761575"></a>`distribution`: `stove0-protocol`
- <a id="s-9c4531c6aa"></a>`module`: `stove0_protocol`
- <a id="s-45387df416"></a>`name`: `JoinPlan`
- <a id="s-5bea08eb82"></a>`unit`: `export`

### Declared structure

- <a id="s-a860a9a650"></a>`kind`: `"class"`
- <a id="s-58f5b89177"></a>`signature`: `"\"(*, format: Literal['stove0-join-plan/v1'] = 'stove0-join-plan/v1', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], declaration: stove0_protocol.fork_join.JoinDeclaration, inputs: Annotated[tuple[stove0_protocol.fork_join.JoinInputPlan, ...], MinLen(min_length=2)], work: stove0_protocol.models.WorkIdentity, workflow_plan: stove0_protocol.models.WorkflowPlan, join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-fed1ae8d50"></a>

- <a id="s-3fa1b1de8d"></a>`type`: `"object"`
- <a id="s-7c206f25ff"></a>`additionalProperties`: `false`
- <a id="s-6fa8180554"></a>`required`: `["parent_work_id","branch_set_sha256","declaration","inputs","work","workflow_plan","join_plan_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9b4bd1c759"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7e3d58cd7b"></a>`declaration` | yes | [JoinDeclaration](#s-750d631d8e) |  |
| <a id="s-846ca4277d"></a>`format` | no | type="string"; const="stove0-join-plan/v1"; default="stove0-join-plan/v1" |  |
| <a id="s-ea8267efc9"></a>`inputs` | yes | type="array"; items=([JoinInputPlan](#s-7930b1c493)); minItems=2 |  |
| <a id="s-30c7b7484f"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b431393aa4"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d88e597f18"></a>`work` | yes | [WorkIdentity](#s-0c4f9121c1) |  |
| <a id="s-16337ea292"></a>`workflow_plan` | yes | [WorkflowPlan](#s-82a90c1db6) |  |

##### Definitions

- [ArtifactSelectionRef](#s-e88eb968a9)
- [ArtifactSubject](#s-18dd54a05c)
- [BranchWorkBinding](#s-f65d3d2519)
- [CollectionId](#s-5bc5dea0a3)
- [CollectionRootRef](#s-f54eb01cea)
- [EvaluationBinding](#s-647d4be573)
- [JoinDeclaration](#s-750d631d8e)
- [JoinInputPlan](#s-7930b1c493)
- [JoinMemberDeclaration](#s-0795829b87)
- [JoinWorkBinding](#s-f5cf573058)
- [JoinWorkMemberBinding](#s-22ed86339a)
- [JsonSchemaDocument](#s-33397eb4f6)
- [JsonValue](#s-12dea0e478)
- [ObservationEvidence](#s-705074dc13)
- [ObservationFailure](#s-8f5d285b2e)
- [ObservationInapplicable](#s-784201be08)
- [ObservationRequest](#s-d9d9f1b17f)
- [ObservationResult](#s-dbafedf098)
- [ObserverImplementation](#s-a8a2e1a315)
- [OperationRef](#s-89846baf14)
- [RecipeRef](#s-a1e58b9bde)
- [WorkIdentity](#s-0c4f9121c1)
- [WorkflowPlan](#s-82a90c1db6)
- [WorkflowPlanIntent](#s-40b25d54ea)

##### <a id="s-e88eb968a9"></a>definition `ArtifactSelectionRef`

- <a id="s-0f8952de6a"></a>`type`: `"object"`
- <a id="s-f9c8f9adbb"></a>`additionalProperties`: `false`
- <a id="s-0932b692bd"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-28979c03f9"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-91722de88b"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a3e55ea16e"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-18dd54a05c"></a>definition `ArtifactSubject`

- <a id="s-55cf4c9833"></a>`type`: `"object"`
- <a id="s-65dfc7af22"></a>`additionalProperties`: `false`
- <a id="s-4f401a497e"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d8aaab28ae"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-3874c8a3f3"></a>`collection` | yes | [CollectionRootRef](#s-f54eb01cea) |  |
| <a id="s-1aeccb52c1"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-de008069b3"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-1b48f69c21"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-85492be311"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-de34542169"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f65d3d2519"></a>definition `BranchWorkBinding`

- <a id="s-91ab883cef"></a>`type`: `"object"`
- <a id="s-34cf814ef2"></a>`additionalProperties`: `false`
- <a id="s-8e0e79aa99"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9e604b6a5f"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e53cfa6829"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dbbb99ef1d"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-954697e5c0"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-5aa8d7fa02"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5bc5dea0a3"></a>definition `CollectionId`

- <a id="s-ffc735ac91"></a>`type`: `"integer"`
- <a id="s-4521d41216"></a>`minimum`: `1`

##### <a id="s-f54eb01cea"></a>definition `CollectionRootRef`

- <a id="s-9895c57247"></a>`type`: `"object"`
- <a id="s-25d41b6969"></a>`additionalProperties`: `false`
- <a id="s-74ab879c5f"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eddc603eee"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5a4abd4093"></a>`collection_id` | yes | [CollectionId](#s-5bc5dea0a3) |  |
| <a id="s-03bb383b42"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-647d4be573"></a>definition `EvaluationBinding`

- <a id="s-ed66cbb50a"></a>`type`: `"object"`
- <a id="s-cf7b930b83"></a>`additionalProperties`: `false`
- <a id="s-b860121ba8"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d101319861"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c08e005517"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f025e51861"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-e91b8463ff"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-750d631d8e"></a>definition `JoinDeclaration`

- <a id="s-7cb78483c9"></a>`type`: `"object"`
- <a id="s-94a5eee799"></a>`additionalProperties`: `false`
- <a id="s-94cc11151e"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0197cf1bc2"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-8880904291"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-982fda7e11"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e19355924e"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-0795829b87)); minItems=2 |  |
| <a id="s-3dd57bc522"></a>`recipe` | yes | [RecipeRef](#s-a1e58b9bde) |  |
| <a id="s-044fe92418"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-40b25d54ea) |  |

##### <a id="s-7930b1c493"></a>definition `JoinInputPlan`

- <a id="s-4b2f527bc1"></a>`type`: `"object"`
- <a id="s-06ffe8b827"></a>`additionalProperties`: `false`
- <a id="s-834dd139a5"></a>`required`: `["branch_id","settlement_sha256","derivation_sha256","output_collection","artifact_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f88441ba66"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-e88eb968a9) |  |
| <a id="s-e01893ac83"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-18b322478f"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9639c99d46"></a>`output_collection` | yes | [CollectionRootRef](#s-f54eb01cea) |  |
| <a id="s-33f8164959"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-375ab17f23"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0795829b87"></a>definition `JoinMemberDeclaration`

- <a id="s-adc5273deb"></a>`type`: `"object"`
- <a id="s-29e4a623fd"></a>`additionalProperties`: `false`
- <a id="s-9620a25caf"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-301dd0b543"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-14e43f9bb6"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-f5cf573058"></a>definition `JoinWorkBinding`

- <a id="s-4827a7a643"></a>`type`: `"object"`
- <a id="s-0aa3b2fc79"></a>`additionalProperties`: `false`
- <a id="s-8769a5563a"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e8d9fcb437"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fbb5dbe6c3"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-fdb13c6f22"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-22ed86339a)); minItems=2 |  |
| <a id="s-e3f8b2cd37"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-22ed86339a"></a>definition `JoinWorkMemberBinding`

- <a id="s-001fe6730a"></a>`type`: `"object"`
- <a id="s-a94eb341bb"></a>`additionalProperties`: `false`
- <a id="s-89ac47e5d6"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2fd0791dcc"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-48ab9f6a9e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-28bae61591"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a510e66c26"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-33397eb4f6"></a>definition `JsonSchemaDocument`

- <a id="s-00de9e8083"></a>`type`: `"object"`
- <a id="s-642eecc3f7"></a>`additionalProperties`: `false`
- <a id="s-8658e284f9"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eaf06515cf"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-6d86dfb833"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-b42cffd1d5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fca45b8766"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-42ad3a0015"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-12dea0e478"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-705074dc13"></a>definition `ObservationEvidence`

- <a id="s-e1eb64cd30"></a>`type`: `"object"`
- <a id="s-247e5a4a49"></a>`additionalProperties`: `false`
- <a id="s-a872303d5e"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a75e989d1e"></a>`request` | yes | [ObservationRequest](#s-d9d9f1b17f) |  |
| <a id="s-a3d75cf2d5"></a>`result` | yes | [ObservationResult](#s-dbafedf098) |  |

##### <a id="s-8f5d285b2e"></a>definition `ObservationFailure`

- <a id="s-144e903050"></a>`type`: `"object"`
- <a id="s-3a91db89a2"></a>`additionalProperties`: `false`
- <a id="s-fc7f9fdcbb"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-caedb3dc70"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-563b1fd978"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-14b6e61035"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-784201be08"></a>definition `ObservationInapplicable`

- <a id="s-6fe0132e9e"></a>`type`: `"object"`
- <a id="s-743bc8381b"></a>`additionalProperties`: `false`
- <a id="s-a131b8c7dc"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-631b8ec3ed"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-14ae5da292"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-d9d9f1b17f"></a>definition `ObservationRequest`

- <a id="s-6bffebf9ec"></a>`type`: `"object"`
- <a id="s-15c1dfbb56"></a>`additionalProperties`: `false`
- <a id="s-562bc017d2"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5cc6f0e1a4"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-7ee05b1852"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-2d47cc1aeb"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9dc3f1304e"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-57332db647"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e67452c8c2"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-809ddb321b"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-fd598099b2"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3dfa314899"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-389b588d26"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-18dd54a05c)); minItems=1 |  |
| <a id="s-062e96013b"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-dfbfc9647d"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-dbafedf098"></a>definition `ObservationResult`

- <a id="s-7b2663146a"></a>`type`: `"object"`
- <a id="s-0dba23dded"></a>`additionalProperties`: `false`
- <a id="s-f6eed3f571"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-33a4b65d8b"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-6025219151"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-12dea0e478))); (type="null")]; default=null |  |
| <a id="s-c6c5affadf"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-33397eb4f6)); (type="null")]; default=null |  |
| <a id="s-e94d371fa1"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-1260cecc2b"></a>`failure` | no | anyOf=[([ObservationFailure](#s-8f5d285b2e)); (type="null")]; default=null |  |
| <a id="s-53508d72ea"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-363c9a78a4"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-784201be08)); (type="null")]; default=null |  |
| <a id="s-6b81b22709"></a>`observer` | yes | [ObserverImplementation](#s-a8a2e1a315) |  |
| <a id="s-9a9d18a5de"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-db11b95ef2"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-43f273082f"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f74ccdee19"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-27b1709786"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-db8888c222"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-18dd54a05c)); minItems=1 |  |

##### <a id="s-a8a2e1a315"></a>definition `ObserverImplementation`

- <a id="s-96443cba50"></a>`type`: `"object"`
- <a id="s-8ce8753b48"></a>`additionalProperties`: `false`
- <a id="s-bb9d087c67"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-394615d964"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0e0ae27855"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8a5e6ab28d"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-786d3ce756"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-ff3b1bbc2f"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-89846baf14"></a>definition `OperationRef`

- <a id="s-360f72b4ae"></a>`type`: `"object"`
- <a id="s-7178e652e2"></a>`additionalProperties`: `false`
- <a id="s-4d00c286ac"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab42edbb69"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0c3cbbe6da"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a1e58b9bde"></a>definition `RecipeRef`

- <a id="s-82700e925d"></a>`type`: `"object"`
- <a id="s-978d28c910"></a>`additionalProperties`: `false`
- <a id="s-8afe774a4e"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-58865ee801"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-201c4c8751"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-62e3343acb"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0c4f9121c1"></a>definition `WorkIdentity`

- <a id="s-8c11b7c37a"></a>`type`: `"object"`
- <a id="s-45ce993414"></a>`additionalProperties`: `false`
- <a id="s-8b689177a3"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-062e6c2274"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-d3cb8c7772"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-647d4be573)); (type="null")]; default=null |  |
| <a id="s-c0dc17e4a9"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-f65d3d2519)); ([JoinWorkBinding](#s-f5cf573058))]); (type="null")]; default=null |  |
| <a id="s-cdf74ab79e"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-897c85f1c0"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-f54eb01cea)); minItems=1 |  |
| <a id="s-d71184d611"></a>`recipe` | yes | [RecipeRef](#s-a1e58b9bde) |  |
| <a id="s-e29bc07850"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-82a90c1db6"></a>definition `WorkflowPlan`

- <a id="s-c287d1b5c1"></a>`type`: `"object"`
- <a id="s-48d09dd71d"></a>`additionalProperties`: `false`
- <a id="s-14965b8493"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-317eccc9e6"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-bac791df06"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-0b84cbdb98"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-705074dc13)) |  |
| <a id="s-f47528421c"></a>`operation` | yes | [OperationRef](#s-89846baf14) |  |
| <a id="s-15cafa5e19"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-5848a470fd"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-46e92964fe"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-29a6704a8f"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-7c762937b5"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-92d03db18e"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-95507f1167"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-c2742ba485"></a>`work` | yes | [WorkIdentity](#s-0c4f9121c1) |  |
| <a id="s-95f3185729"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-40b25d54ea"></a>definition `WorkflowPlanIntent`

- <a id="s-aa8e737d2c"></a>`type`: `"object"`
- <a id="s-104d52a179"></a>`additionalProperties`: `false`
- <a id="s-0319b5fcdb"></a>`required`: `["operation","target_registration_id","target_contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3599ba9663"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-d18558b187"></a>`operation` | yes | [OperationRef](#s-89846baf14) |  |
| <a id="s-3cd7608f7b"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-c383295dc4"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-12dea0e478)) |  |
| <a id="s-924e14784e"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-3a63e21fca"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-8aa4153aba"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-5ff80dcc52"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a81785504"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-protocol-joinplan-canonical-inputs.md)
- [seal](stove0-protocol-joinplan-seal.md)
- [verify_contract](stove0-protocol-joinplan-verify-contract.md)

## Governing policies

- <a id="pa-3c113d6aab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 684c546fc2c885ec3b2346fb2b1c0da32f5db62cb457b43c45d0f72ced70d192 -->

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
        "JoinDeclaration": {
          "additionalProperties": false,
          "properties": {
            "effective_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "format": {
              "const": "stove0-join-declaration/v1",
              "default": "stove0-join-declaration/v1",
              "type": "string"
            },
            "join_declaration_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "members": {
              "items": {
                "$ref": "#/$defs/JoinMemberDeclaration"
              },
              "minItems": 2,
              "type": "array"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeRef"
            },
            "workflow_intent": {
              "$ref": "#/$defs/WorkflowPlanIntent"
            }
          },
          "required": [
            "members",
            "recipe",
            "workflow_intent",
            "join_declaration_sha256"
          ],
          "type": "object"
        },
        "JoinInputPlan": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "output_collection": {
              "$ref": "#/$defs/CollectionRootRef"
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
            "derivation_sha256",
            "output_collection",
            "artifact_selection"
          ],
          "type": "object"
        },
        "JoinMemberDeclaration": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "output_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "branch_id",
            "output_roles"
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
        },
        "WorkflowPlanIntent": {
          "additionalProperties": false,
          "properties": {
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
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
            }
          },
          "required": [
            "operation",
            "target_registration_id",
            "target_contract_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "declaration": {
          "$ref": "#/$defs/JoinDeclaration"
        },
        "format": {
          "const": "stove0-join-plan/v1",
          "default": "stove0-join-plan/v1",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/JoinInputPlan"
          },
          "minItems": 2,
          "type": "array"
        },
        "join_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "parent_work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        },
        "workflow_plan": {
          "$ref": "#/$defs/WorkflowPlan"
        }
      },
      "required": [
        "parent_work_id",
        "branch_set_sha256",
        "declaration",
        "inputs",
        "work",
        "workflow_plan",
        "join_plan_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-join-plan/v1'] = 'stove0-join-plan/v1', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], declaration: stove0_protocol.fork_join.JoinDeclaration, inputs: Annotated[tuple[stove0_protocol.fork_join.JoinInputPlan, ...], MinLen(min_length=2)], work: stove0_protocol.models.WorkIdentity, workflow_plan: stove0_protocol.models.WorkflowPlan, join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinPlan",
  "unit": "export"
}
```

</details>
