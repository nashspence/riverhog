# stove0_protocol.WorkflowPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplan:1051bd4de8 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-f761ab25e0"></a>`signature`: `"\"(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ContentObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationIdentityRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', source_collection_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', source_collection_retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>, workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-e5ad9a5d0a"></a>

- <a id="s-1cc007c411"></a>`type`: `"object"`
- <a id="s-a92ebdb0ee"></a>`additionalProperties`: `false`
- <a id="s-4355e9b429"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4998011bdb"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-db8ee37eec"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-e36ced0ec4"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-873ca68bee)) |  |
| <a id="s-3b48301872"></a>`operation` | yes | [OperationIdentityRef](#s-7ebc39d405) |  |
| <a id="s-8a4e38ece3"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-800d18517c"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-4d361b5e5c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-cd2bcdb8e4"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-79cafcd3b1"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-a6523fe8de"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8ba9ed12fe"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-ea56bd7420"></a>`work` | yes | [WorkIdentity](#s-f185cccf70) |  |
| <a id="s-de45a84b7a"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [BranchWorkBinding](#s-a783a0b6d9)
- [CollectionId](#s-05b21cd0dc)
- [CollectionRootIdentityRef](#s-359a6c443e)
- [ContentObservationEvidence](#s-873ca68bee)
- [ContentObservationFailure](#s-e48a3ec77c)
- [ContentObservationInapplicable](#s-fde745304c)
- [ContentObservationRequest](#s-b7fdf3a20e)
- [ContentObservationResult](#s-652d6972d2)
- [EvaluationBinding](#s-d85a23e820)
- [JoinWorkBinding](#s-5d9b2b6117)
- [JoinWorkMemberBinding](#s-3e6462b479)
- [JsonSchemaValidationProfile](#s-fb32774868)
- [JsonValue](#s-4fc3efab64)
- [NonnegativeDecimal](#s-a7da740e27)
- [ObserverImplementation](#s-982e1d84b8)
- [OperationIdentityRef](#s-7ebc39d405)
- [RecipeIdentityRef](#s-142ab36f35)
- [WorkArtifactSubject](#s-ef32f31489)
- [WorkIdentity](#s-f185cccf70)

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


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-09c6330151"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-69f6558d4c"></a>2 | not=(const="0") |

##### <a id="s-359a6c443e"></a>definition `CollectionRootIdentityRef`

- <a id="s-7900446957"></a>`type`: `"object"`
- <a id="s-d2855f8c30"></a>`additionalProperties`: `false`
- <a id="s-56637562cf"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9f5a3cdb74"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b5b91d3308"></a>`collection_id` | yes | [CollectionId](#s-05b21cd0dc) |  |
| <a id="s-ad53972367"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-873ca68bee"></a>definition `ContentObservationEvidence`

- <a id="s-c205e689b9"></a>`type`: `"object"`
- <a id="s-cffdd41516"></a>`additionalProperties`: `false`
- <a id="s-b779d6977c"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-33bd52ea94"></a>`request` | yes | [ContentObservationRequest](#s-b7fdf3a20e) |  |
| <a id="s-66b1a60139"></a>`result` | yes | [ContentObservationResult](#s-652d6972d2) |  |

##### <a id="s-e48a3ec77c"></a>definition `ContentObservationFailure`

- <a id="s-bbce8ca10c"></a>`type`: `"object"`
- <a id="s-24eac3e2d0"></a>`additionalProperties`: `false`
- <a id="s-78e0720d7e"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b1d250c491"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-286df6031d"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-4232c7e605"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-fde745304c"></a>definition `ContentObservationInapplicable`

- <a id="s-f37631534b"></a>`type`: `"object"`
- <a id="s-dd6da84411"></a>`additionalProperties`: `false`
- <a id="s-8cb5c4a857"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d379f7ec28"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-22ce544eb6"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-b7fdf3a20e"></a>definition `ContentObservationRequest`

- <a id="s-8846e9090d"></a>`type`: `"object"`
- <a id="s-5329627bfb"></a>`additionalProperties`: `false`
- <a id="s-7ac8a195a9"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f5e139e279"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-a35c9d8b63"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-c093b8a871"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-16938306fe"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3a8af355be"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4a49e492a6"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-4b0a4b8e86"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-cbb874fa89"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a9d549bea6"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-6850236460"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-ef32f31489)); minItems=1 |  |
| <a id="s-4047509bc3"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-ce7b59b9eb"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-652d6972d2"></a>definition `ContentObservationResult`

- <a id="s-ce01238700"></a>`type`: `"object"`
- <a id="s-d79cd0427f"></a>`additionalProperties`: `false`
- <a id="s-5e3afb0461"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fd3328734c"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |
| <a id="s-362e825732"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-4fc3efab64))); (type="null")]; default=null |  |
| <a id="s-068a7e864b"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-fb32774868)); (type="null")]; default=null |  |
| <a id="s-691be02056"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-d7a4506b65"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-e48a3ec77c)); (type="null")]; default=null |  |
| <a id="s-ae4e5bc7cf"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-60ba3bb512"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-fde745304c)); (type="null")]; default=null |  |
| <a id="s-4a38713495"></a>`observer` | yes | [ObserverImplementation](#s-982e1d84b8) |  |
| <a id="s-9ac7347e28"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dd840bd6ee"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cc33fad3c9"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4b59f2241b"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-87a9964bad"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-0ad803beef"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-ef32f31489)); minItems=1 |  |

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

##### <a id="s-fb32774868"></a>definition `JsonSchemaValidationProfile`

- <a id="s-5cd2a8ee4e"></a>`type`: `"object"`
- <a id="s-6a7af95f73"></a>`additionalProperties`: `false`
- <a id="s-176c3daf06"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-caeaad50d5"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-a83f117c5f"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-058346ea89"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2a8616037a"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-60113e77a7"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-4fc3efab64)) |  |

##### <a id="s-4fc3efab64"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-a7da740e27"></a>definition `NonnegativeDecimal`

- <a id="s-cddf38e714"></a>`type`: `"string"`
- <a id="s-b22a12b252"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

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

##### <a id="s-7ebc39d405"></a>definition `OperationIdentityRef`

- <a id="s-b8c1ad9abe"></a>`type`: `"object"`
- <a id="s-c1de588ef2"></a>`additionalProperties`: `false`
- <a id="s-99518e45b2"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-232dd6b8a3"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-138e40fedf"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-142ab36f35"></a>definition `RecipeIdentityRef`

- <a id="s-578aa0ef7f"></a>`type`: `"object"`
- <a id="s-1e83befd26"></a>`additionalProperties`: `false`
- <a id="s-cda54799b6"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c3001f83f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e923cc92c7"></a>`revision` | yes | [NonnegativeDecimal](#s-a7da740e27); ge=1 |  |
| <a id="s-edd4f3c148"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ef32f31489"></a>definition `WorkArtifactSubject`

- <a id="s-99406cdbea"></a>`type`: `"object"`
- <a id="s-0a23b0200b"></a>`additionalProperties`: `false`
- <a id="s-a5b1b011b6"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-042b60087c"></a>`bytes` | yes | [NonnegativeDecimal](#s-a7da740e27); ge=0 |  |
| <a id="s-851868ff45"></a>`collection` | yes | [CollectionRootIdentityRef](#s-359a6c443e) |  |
| <a id="s-73fd97f98b"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-1f7392b879"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-161f26638b"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-46668553f9"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7a62af0d16"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

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
| <a id="s-6158bde4b6"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-359a6c443e)); minItems=1 |  |
| <a id="s-39606a0959"></a>`recipe` | yes | [RecipeIdentityRef](#s-142ab36f35) |  |
| <a id="s-e946b20d62"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_observations](stove0-protocol-workflowplan-canonical-observations.md)
- [protect_evaluation_sources](stove0-protocol-workflowplan-protect-evaluation-sources.md)
- [seal](stove0-protocol-workflowplan-seal.md)
- [verify_digest](stove0-protocol-workflowplan-verify-digest.md)

## Governing policies

- <a id="pa-35e57ffe6b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f162e024f78e6775697f5bfd7557060cc77809b5683a129232ef86d92a15307 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
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
        "CollectionRootIdentityRef": {
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
        "ContentObservationEvidence": {
          "additionalProperties": false,
          "properties": {
            "request": {
              "$ref": "#/$defs/ContentObservationRequest"
            },
            "result": {
              "$ref": "#/$defs/ContentObservationResult"
            }
          },
          "required": [
            "request",
            "result"
          ],
          "type": "object"
        },
        "ContentObservationFailure": {
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
        "ContentObservationInapplicable": {
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
        "ContentObservationRequest": {
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
                "$ref": "#/$defs/WorkArtifactSubject"
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
        "ContentObservationResult": {
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
                  "$ref": "#/$defs/ContentObservationFailure"
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
                  "$ref": "#/$defs/ContentObservationInapplicable"
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
                "$ref": "#/$defs/WorkArtifactSubject"
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
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
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
        "OperationIdentityRef": {
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
        "RecipeIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
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
        "WorkArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootIdentityRef"
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
                "$ref": "#/$defs/CollectionRootIdentityRef"
              },
              "minItems": 1,
              "type": "array"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeIdentityRef"
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
            "$ref": "#/$defs/ContentObservationEvidence"
          },
          "type": "array"
        },
        "operation": {
          "$ref": "#/$defs/OperationIdentityRef"
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
        "source_collection_retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "type": "integer"
        },
        "source_collection_retirement_policy": {
          "default": "retain",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "type": "string"
        },
        "target_descriptor_sha256": {
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
        "target_descriptor_sha256",
        "workflow_plan_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ContentObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationIdentityRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', source_collection_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', source_collection_retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>, workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPlan",
  "unit": "export"
}
```

</details>
