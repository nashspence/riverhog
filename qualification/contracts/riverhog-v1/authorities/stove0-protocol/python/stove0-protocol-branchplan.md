# stove0_protocol.BranchPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchplan:03d66abe2c -->

Exact externally visible contract owned by this contract element.

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
- [BranchWorkBinding](#s-2f7c9d11af)
- [CollectionId](#s-20328cebdb)
- [CollectionRootIdentityRef](#s-400aad3919)
- [ContentObservationEvidence](#s-0fc48af330)
- [ContentObservationFailure](#s-c60cb9934d)
- [ContentObservationInapplicable](#s-19316af211)
- [ContentObservationRequest](#s-9552c34d74)
- [ContentObservationResult](#s-40c6035e8e)
- [EvaluationBinding](#s-1d6708d93b)
- [JoinWorkBinding](#s-e616a9e832)
- [JoinWorkMemberBinding](#s-240ad0ae62)
- [JsonSchemaValidationProfile](#s-af4d0fc55a)
- [JsonValue](#s-027a198060)
- [NonnegativeDecimal](#s-9840c3273b)
- [ObserverImplementation](#s-c6108de4dd)
- [OperationIdentityRef](#s-b654632b68)
- [RecipeIdentityRef](#s-5a60206445)
- [WorkArtifactSubject](#s-85ccab22f9)
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


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-6d4b1974e8"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-c91cd41de3"></a>2 | not=(const="0") |

##### <a id="s-400aad3919"></a>definition `CollectionRootIdentityRef`

- <a id="s-ca525d97de"></a>`type`: `"object"`
- <a id="s-2cd881ebfc"></a>`additionalProperties`: `false`
- <a id="s-8b1f2b4fc9"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b0a1221ae"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3183103db2"></a>`collection_id` | yes | [CollectionId](#s-20328cebdb) |  |
| <a id="s-7b94f0b986"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0fc48af330"></a>definition `ContentObservationEvidence`

- <a id="s-ad7adf12f9"></a>`type`: `"object"`
- <a id="s-53ff59a662"></a>`additionalProperties`: `false`
- <a id="s-f71b32bbb9"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-738c30f792"></a>`request` | yes | [ContentObservationRequest](#s-9552c34d74) |  |
| <a id="s-da3464dabc"></a>`result` | yes | [ContentObservationResult](#s-40c6035e8e) |  |

##### <a id="s-c60cb9934d"></a>definition `ContentObservationFailure`

- <a id="s-6839b7ece3"></a>`type`: `"object"`
- <a id="s-50bb0e227a"></a>`additionalProperties`: `false`
- <a id="s-948220c822"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-668aaba5ed"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7bb8ed7fc2"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-17ee6f2d18"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-19316af211"></a>definition `ContentObservationInapplicable`

- <a id="s-577417251d"></a>`type`: `"object"`
- <a id="s-d34f4a16c7"></a>`additionalProperties`: `false`
- <a id="s-055469c5f9"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5e918d1b7c"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-17affba42d"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-9552c34d74"></a>definition `ContentObservationRequest`

- <a id="s-c6688fec63"></a>`type`: `"object"`
- <a id="s-a0e61a4c82"></a>`additionalProperties`: `false`
- <a id="s-0214c76414"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0c7c0de4c4"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-21a294a320"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-0acd590dca"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-21a0c40d88"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-04c777d697"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f47c98cbd3"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-00fc3e2334"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-de6c48c693"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cf09ff46da"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-2e0b66b734"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-85ccab22f9)); minItems=1 |  |
| <a id="s-5f5176bd72"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-e55e5994c0"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-40c6035e8e"></a>definition `ContentObservationResult`

- <a id="s-e9c74ebb22"></a>`type`: `"object"`
- <a id="s-303a4bbfe5"></a>`additionalProperties`: `false`
- <a id="s-27e6de2d1d"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-444571829c"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-f29a9529b4"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-027a198060))); (type="null")]; default=null |  |
| <a id="s-11a1cc0703"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-af4d0fc55a)); (type="null")]; default=null |  |
| <a id="s-d4edf5527a"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-2efd57fa74"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-c60cb9934d)); (type="null")]; default=null |  |
| <a id="s-b78328ac79"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-42fd7c0b7b"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-19316af211)); (type="null")]; default=null |  |
| <a id="s-1cdbaea491"></a>`observer` | yes | [ObserverImplementation](#s-c6108de4dd) |  |
| <a id="s-70ab497466"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a4f76764ef"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1ff5ef5f3e"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-efc049a8ce"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-751e4eecd5"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-f5e6d32e27"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-85ccab22f9)); minItems=1 |  |

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

##### <a id="s-af4d0fc55a"></a>definition `JsonSchemaValidationProfile`

- <a id="s-b902522628"></a>`type`: `"object"`
- <a id="s-058379ca79"></a>`additionalProperties`: `false`
- <a id="s-70cc5152ed"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7486823204"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-8eefeb6181"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-7c35a93c52"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0a918082c5"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-105d11055c"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |

##### <a id="s-027a198060"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-9840c3273b"></a>definition `NonnegativeDecimal`

- <a id="s-1ff786550b"></a>`type`: `"string"`
- <a id="s-1817a3ad85"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

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

##### <a id="s-b654632b68"></a>definition `OperationIdentityRef`

- <a id="s-86258316a4"></a>`type`: `"object"`
- <a id="s-7c58925930"></a>`additionalProperties`: `false`
- <a id="s-79703972f8"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba00e46d15"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-780d31e716"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5a60206445"></a>definition `RecipeIdentityRef`

- <a id="s-c35c6e9c4c"></a>`type`: `"object"`
- <a id="s-a076de9f67"></a>`additionalProperties`: `false`
- <a id="s-15f80fce77"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bd9a5e64bd"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-92afe8c522"></a>`revision` | yes | [NonnegativeDecimal](#s-9840c3273b); ge=1 |  |
| <a id="s-77b1d1c101"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-85ccab22f9"></a>definition `WorkArtifactSubject`

- <a id="s-7320bd74d7"></a>`type`: `"object"`
- <a id="s-259116be17"></a>`additionalProperties`: `false`
- <a id="s-be761f8e05"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d0eac8c30f"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-100c489586"></a>`collection` | yes | [CollectionRootIdentityRef](#s-400aad3919) |  |
| <a id="s-6243b5be4a"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-bacc9ef9f2"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-91bce2f8bc"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-ff4bf1539e"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2c9d3b1347"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

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
| <a id="s-ad64c5baaf"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-400aad3919)); minItems=1 |  |
| <a id="s-5f1e6f28af"></a>`recipe` | yes | [RecipeIdentityRef](#s-5a60206445) |  |
| <a id="s-2c957295cd"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-20b2e6825f"></a>definition `WorkflowPlan`

- <a id="s-442aa6f472"></a>`type`: `"object"`
- <a id="s-baf3431c32"></a>`additionalProperties`: `false`
- <a id="s-debe9fb3e5"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df163da59d"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-759bac93ce"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-0de64c8ae4"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-0fc48af330)) |  |
| <a id="s-3b8b248bf5"></a>`operation` | yes | [OperationIdentityRef](#s-b654632b68) |  |
| <a id="s-ba0ed19f52"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-fedfddb994"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-027a198060)) |  |
| <a id="s-6edfe95abf"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-1412d96eb6"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-7004b1e5cb"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-7f5e83d556"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b1f75ff0b"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-7479cb7635"></a>`work` | yes | [WorkIdentity](#s-55cfd70fdd) |  |
| <a id="s-c42dbed097"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [bind_child_work](stove0-protocol-branchplan-bind-child-work.md)
- [build](stove0-protocol-branchplan-build.md)

## Governing policies

- <a id="pa-639ccb615e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c606d02ef57ceb68438355ca036124c7c2ce1aa2a7d08574dd9d28b60201c97d -->

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
              "minimum": 0,
              "type": "integer"
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
