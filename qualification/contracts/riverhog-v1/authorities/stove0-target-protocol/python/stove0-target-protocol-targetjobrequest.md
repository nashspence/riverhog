# stove0_target_protocol.TargetJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobrequest:dc2711e32c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b03dff792f"></a>
- <a id="s-eef13811c0"></a>`distribution`: `stove0-target-protocol`
- <a id="s-c98e03ec89"></a>`module`: `stove0_target_protocol`
- <a id="s-d9131e81fc"></a>`name`: `TargetJobRequest`
- <a id="s-6f74b4fc74"></a>`unit`: `export`

### Declared structure

- <a id="s-0ecdcd19a1"></a>`kind`: `"class"`
- <a id="s-fe605ce528"></a>`signature`: `"\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, runtime: stove0_target_protocol.protocol.TargetRuntimeAuthority, callback_access: stove0_target_protocol.protocol.TargetCallbackAccess, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-46ffd95148"></a>

- <a id="s-751c2df10a"></a>`type`: `"object"`
- <a id="s-df97f3f995"></a>`additionalProperties`: `false`
- <a id="s-8341dee031"></a>`required`: `["declaration","runtime","callback_access","request_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-57904fec25"></a>`callback_access` | yes | [TargetCallbackAccess](#s-1a5d2ca569) |  |
| <a id="s-2adf0a7017"></a>`declaration` | yes | [TargetJobDeclaration](#s-d6f049cbe9) |  |
| <a id="s-caa3b06c68"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ef267214a4"></a>`runtime` | yes | [TargetRuntimeAuthority](#s-dde20fa4dd) |  |

##### Definitions

- [ArtifactSelectionRef](#s-9f11eec211)
- [ArtifactSubject](#s-cc05686e36)
- [BranchWorkBinding](#s-b9521d8afb)
- [CollectionId](#s-0169766eb5)
- [CollectionRootRef](#s-0387afc75e)
- [ControllerEvidence](#s-bb50fa8e70)
- [DeclaredWorkspaceProtection](#s-591bf70cd2)
- [EffectPlan](#s-8fae60e2bc)
- [EvaluationBinding](#s-55a8419207)
- [ExecutionEnvelope](#s-4714cdc7b2)
- [JoinWorkBinding](#s-d7263f7f9f)
- [JoinWorkMemberBinding](#s-5edd536aa3)
- [JsonSchemaValidationProfile](#s-867d63df4b)
- [JsonValue](#s-92d5fcb8ed)
- [ObservationEvidence](#s-50312fffcf)
- [ObservationFailure](#s-cb4793f4c2)
- [ObservationInapplicable](#s-ec85f770f3)
- [ObservationRequest](#s-32ffb4626d)
- [ObservationResult](#s-a3880d35c0)
- [ObserverImplementation](#s-f910fcdf9a)
- [OperationRef](#s-ba57e7eb2d)
- [RecipeRef](#s-e90110279e)
- [TargetCallbackAccess](#s-1a5d2ca569)
- [TargetInputAuthority](#s-0021e3ee94)
- [TargetInputRoleCount](#s-2cfb6e2637)
- [TargetJobDeclaration](#s-d6f049cbe9)
- [TargetPlanBinding](#s-91f9dc0c92)
- [TargetRuntimeAuthority](#s-dde20fa4dd)
- [TransformPlan](#s-2ec128e55a)
- [WorkIdentity](#s-0fdf700e51)
- [WorkflowPlan](#s-6505acb36a)

##### <a id="s-9f11eec211"></a>definition `ArtifactSelectionRef`

- <a id="s-85fbeef0c2"></a>`type`: `"object"`
- <a id="s-94a97834a5"></a>`additionalProperties`: `false`
- <a id="s-c04c9b7256"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1926aa2a14"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-b118f26b9f"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a14dd1813b"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-cc05686e36"></a>definition `ArtifactSubject`

- <a id="s-c292b1f37d"></a>`type`: `"object"`
- <a id="s-643c234e84"></a>`additionalProperties`: `false`
- <a id="s-4be5a35178"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a72e34faa"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-8798856920"></a>`collection` | yes | [CollectionRootRef](#s-0387afc75e) |  |
| <a id="s-b59bdc1b1a"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-a5f4d3477a"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-8f367f0bc1"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-a366782aae"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-699c92abea"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b9521d8afb"></a>definition `BranchWorkBinding`

- <a id="s-7599a16b51"></a>`type`: `"object"`
- <a id="s-4ab7faea44"></a>`additionalProperties`: `false`
- <a id="s-2a7bdfa92a"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ccf883d84a"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6d556928ef"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3aa47badb1"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1e448a05ef"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-fc9828bc69"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0169766eb5"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-3455869450"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-0a2e42fff5"></a>2 | not=(const="0") |

##### <a id="s-0387afc75e"></a>definition `CollectionRootRef`

- <a id="s-51e4adcd35"></a>`type`: `"object"`
- <a id="s-936cf2caa0"></a>`additionalProperties`: `false`
- <a id="s-957c5a5b6a"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1303b89624"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7e0dfd8518"></a>`collection_id` | yes | [CollectionId](#s-0169766eb5) |  |
| <a id="s-a5067474e7"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bb50fa8e70"></a>definition `ControllerEvidence`

- <a id="s-357eed1d75"></a>`type`: `"object"`
- <a id="s-1a37574423"></a>`additionalProperties`: `false`
- <a id="s-72f78070bb"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b9be8080a9"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bc318a8153"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-4714cdc7b2) |  |
| <a id="s-bdbf3c1ae5"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1" |  |

##### <a id="s-591bf70cd2"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-8924cbb825"></a>`type`: `"string"`
- <a id="s-0096acb6c4"></a>`enum`: `["encrypted-at-rest","memory-backed"]`

##### <a id="s-8fae60e2bc"></a>definition `EffectPlan`

- <a id="s-2657982217"></a>`type`: `"object"`
- <a id="s-3febd42b89"></a>`additionalProperties`: `false`
- <a id="s-5d747b300a"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-665e151e16"></a>`inputs` | yes | [TargetInputAuthority](#s-0021e3ee94) |  |
| <a id="s-9e4f7301d2"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-020d9d4440"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-8838148d97"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-169cc3ea5e"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1b84301a59"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bd2c99bef3"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-7f8b7bba29"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f1a3416ca0"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b23c0a0136"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |

##### <a id="s-55a8419207"></a>definition `EvaluationBinding`

- <a id="s-6d51a4d26a"></a>`type`: `"object"`
- <a id="s-8e091f76e1"></a>`additionalProperties`: `false`
- <a id="s-83a667cfef"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5fbe1827b2"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-55b8d7ecf3"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1428508626"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-e160153d79"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-4714cdc7b2"></a>definition `ExecutionEnvelope`

- <a id="s-6a0458f588"></a>`type`: `"object"`
- <a id="s-84a485c5de"></a>`additionalProperties`: `false`
- <a id="s-e92cec407f"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-887d3a5fe8"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-8d6d941d4d"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-725b78631e"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-84ba690fe0"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1" |  |
| <a id="s-b851b2742e"></a>`target_plan` | yes | [TargetPlanBinding](#s-91f9dc0c92) |  |
| <a id="s-5f49df6bdb"></a>`workflow_plan` | yes | [WorkflowPlan](#s-6505acb36a) |  |

##### <a id="s-d7263f7f9f"></a>definition `JoinWorkBinding`

- <a id="s-2c13dee920"></a>`type`: `"object"`
- <a id="s-16627a0156"></a>`additionalProperties`: `false`
- <a id="s-370b3059f8"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-40060004cd"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb51f71fc0"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-5d2e277527"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-5edd536aa3)); minItems=2 |  |
| <a id="s-81eb11847b"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5edd536aa3"></a>definition `JoinWorkMemberBinding`

- <a id="s-c6d6e61581"></a>`type`: `"object"`
- <a id="s-ae7343e5f5"></a>`additionalProperties`: `false`
- <a id="s-3b235d75ea"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6bef8e9797"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6f47e4cb7f"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dfa2011f24"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-469f9f38c0"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-867d63df4b"></a>definition `JsonSchemaValidationProfile`

- <a id="s-f70f396b3b"></a>`type`: `"object"`
- <a id="s-5440c18718"></a>`additionalProperties`: `false`
- <a id="s-a8df15b343"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e0335deb32"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-3fc1ce1817"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-86ac8c1f28"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-43af30f22c"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3ad195403f"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |

##### <a id="s-92d5fcb8ed"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-50312fffcf"></a>definition `ObservationEvidence`

- <a id="s-4e5848f066"></a>`type`: `"object"`
- <a id="s-bccb3d8f71"></a>`additionalProperties`: `false`
- <a id="s-46275c3535"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d313a75bdb"></a>`request` | yes | [ObservationRequest](#s-32ffb4626d) |  |
| <a id="s-f23b13f0ee"></a>`result` | yes | [ObservationResult](#s-a3880d35c0) |  |

##### <a id="s-cb4793f4c2"></a>definition `ObservationFailure`

- <a id="s-c94a72777a"></a>`type`: `"object"`
- <a id="s-35316cab42"></a>`additionalProperties`: `false`
- <a id="s-cb96cca5ac"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f8a455a1d0"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a376e1b1b1"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-d38d43bb15"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-ec85f770f3"></a>definition `ObservationInapplicable`

- <a id="s-4fe89e5abc"></a>`type`: `"object"`
- <a id="s-587c008927"></a>`additionalProperties`: `false`
- <a id="s-d19b407e42"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c16f9cc9d"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0428b4971c"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-32ffb4626d"></a>definition `ObservationRequest`

- <a id="s-a8d1586e6f"></a>`type`: `"object"`
- <a id="s-f11d50f33f"></a>`additionalProperties`: `false`
- <a id="s-3b3fb1507d"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0510a275bb"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-59f8992d3f"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-31cab2c3c1"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-be00fb49d7"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-12bc1444a9"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-af69a0da6d"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-ec386f0d61"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-5b7e52e11a"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-31ecac1353"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-574039ea20"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-cc05686e36)); minItems=1 |  |
| <a id="s-192175b3e9"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-8f64a88b0e"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a3880d35c0"></a>definition `ObservationResult`

- <a id="s-bad135e637"></a>`type`: `"object"`
- <a id="s-f95a9b9b70"></a>`additionalProperties`: `false`
- <a id="s-a1517252dc"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-597ad1cde7"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-9cf79aeeae"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed))); (type="null")]; default=null |  |
| <a id="s-139d1e174a"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-867d63df4b)); (type="null")]; default=null |  |
| <a id="s-058cb99101"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-6e09ead990"></a>`failure` | no | anyOf=[([ObservationFailure](#s-cb4793f4c2)); (type="null")]; default=null |  |
| <a id="s-de618ed557"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-2d19942fda"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-ec85f770f3)); (type="null")]; default=null |  |
| <a id="s-96e61190a5"></a>`observer` | yes | [ObserverImplementation](#s-f910fcdf9a) |  |
| <a id="s-19437f9207"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dc8d5454f1"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9364d15639"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4ec05594cf"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-602446b5b9"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-3d5d661d03"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-cc05686e36)); minItems=1 |  |

##### <a id="s-f910fcdf9a"></a>definition `ObserverImplementation`

- <a id="s-4193417a56"></a>`type`: `"object"`
- <a id="s-540c69d69c"></a>`additionalProperties`: `false`
- <a id="s-b03e522af7"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa48ac797f"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-42fa0f7083"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-661d3808a3"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-b6f496f1aa"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-8af5e82f7a"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-ba57e7eb2d"></a>definition `OperationRef`

- <a id="s-dda4f395a4"></a>`type`: `"object"`
- <a id="s-efc02f6cca"></a>`additionalProperties`: `false`
- <a id="s-a39a50691e"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5f7d7622e9"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6c5ebd1b2d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e90110279e"></a>definition `RecipeRef`

- <a id="s-96249d37b5"></a>`type`: `"object"`
- <a id="s-254e7d40a0"></a>`additionalProperties`: `false`
- <a id="s-39100e8c37"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3f460b763e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-552ab78293"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-f1dfa23a8b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1a5d2ca569"></a>definition `TargetCallbackAccess`

- <a id="s-7ba1dc4434"></a>`type`: `"object"`
- <a id="s-3bff2a4f7b"></a>`additionalProperties`: `false`
- <a id="s-c51302c2be"></a>`required`: `["stove0_base_url","token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88cc1a1c19"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-9af89f1e25"></a>`stove0_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-f68769c8c2"></a>`token` | yes | type="string"; maxLength=4096; minLength=1 |  |

##### <a id="s-0021e3ee94"></a>definition `TargetInputAuthority`

- <a id="s-2eecf42df6"></a>`type`: `"object"`
- <a id="s-56db9896a6"></a>`additionalProperties`: `false`
- <a id="s-e505372b6b"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cbc3c5d34"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-2cfb6e2637)); minItems=1 |  |
| <a id="s-b3dbc975aa"></a>`selection` | yes | [ArtifactSelectionRef](#s-9f11eec211) |  |

##### <a id="s-2cfb6e2637"></a>definition `TargetInputRoleCount`

- <a id="s-69f1853f96"></a>`type`: `"object"`
- <a id="s-8c15ffbff7"></a>`additionalProperties`: `false`
- <a id="s-e2daafef96"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d0438ea8ed"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-f3a9a277ae"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-d6f049cbe9"></a>definition `TargetJobDeclaration`

- <a id="s-6d8f01ae74"></a>`type`: `"object"`
- <a id="s-3caa9ed4bc"></a>`additionalProperties`: `false`
- <a id="s-25e9c84225"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","declared_workspace_protection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-07de2790a2"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-38fdb23d53"></a>`controller_evidence` | yes | [ControllerEvidence](#s-bb50fa8e70) |  |
| <a id="s-70b0fdc9f3"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-591bf70cd2) |  |
| <a id="s-05b1ada5a9"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-d634501f3e"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2ae0a091ab"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-2ec128e55a)); ([EffectPlan](#s-8fae60e2bc))] |  |

##### <a id="s-91f9dc0c92"></a>definition `TargetPlanBinding`

- <a id="s-44f57179e0"></a>`type`: `"object"`
- <a id="s-a856199058"></a>`additionalProperties`: `false`
- <a id="s-6d1eab9286"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2fff2f80b4"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-739aac417f"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-5f89cbca30"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2ccaa760a4"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0466850dc6"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7f894de910"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-dde20fa4dd"></a>definition `TargetRuntimeAuthority`

- <a id="s-281bcf4d7e"></a>`type`: `"object"`
- <a id="s-cb2a97b9b7"></a>`additionalProperties`: `false`
- <a id="s-6c54297ede"></a>`required`: `["riverhog_base_url","capability_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fdbddeaccc"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-549a68975e"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-a022ef137c"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-576f12f6cc"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### <a id="s-2ec128e55a"></a>definition `TransformPlan`

- <a id="s-3e9f713611"></a>`type`: `"object"`
- <a id="s-3bfada5090"></a>`additionalProperties`: `false`
- <a id="s-d657d6d0ff"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-823e86e181"></a>`inputs` | yes | [TargetInputAuthority](#s-0021e3ee94) |  |
| <a id="s-d00835294a"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-4059f72f4f"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-862da0300e"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-07b80ce35c"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7d7e33fff5"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dc5dac76c6"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-3054000d53"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e09e365057"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a6a78c9ef7"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |

##### <a id="s-0fdf700e51"></a>definition `WorkIdentity`

- <a id="s-becdabff02"></a>`type`: `"object"`
- <a id="s-3754ad31c0"></a>`additionalProperties`: `false`
- <a id="s-2b504252cc"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ef8b8d1dd"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-1dbb2ce3ef"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-55a8419207)); (type="null")]; default=null |  |
| <a id="s-990b7ce2b4"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-b9521d8afb)); ([JoinWorkBinding](#s-d7263f7f9f))]); (type="null")]; default=null |  |
| <a id="s-9b2e2d88cb"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-ad5a586ea3"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-0387afc75e)); minItems=1 |  |
| <a id="s-d69e265635"></a>`recipe` | yes | [RecipeRef](#s-e90110279e) |  |
| <a id="s-000ed1683b"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6505acb36a"></a>definition `WorkflowPlan`

- <a id="s-c3d3cb60ff"></a>`type`: `"object"`
- <a id="s-922b813c77"></a>`additionalProperties`: `false`
- <a id="s-17555f4633"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a15db0fc6f"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-74ea6dbc05"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-76a99f3989"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-50312fffcf)) |  |
| <a id="s-676cbc582f"></a>`operation` | yes | [OperationRef](#s-ba57e7eb2d) |  |
| <a id="s-3547041b3d"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-ebe6e9c068"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-92d5fcb8ed)) |  |
| <a id="s-6dd9f85519"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-47ce6655f1"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-0027cde7eb"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-e1cc8e8c88"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2af5346e2a"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-afa5d79e8d"></a>`work` | yes | [WorkIdentity](#s-0fdf700e51) |  |
| <a id="s-3043586e6a"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [accepted](stove0-target-protocol-targetjobrequest-accepted.md)
- [seal](stove0-target-protocol-targetjobrequest-seal.md)
- [verify_digest](stove0-target-protocol-targetjobrequest-verify-digest.md)

## Governing policies

- <a id="pa-db8081f8df"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59154bbdfb5a73785a6998675cde6ab15cd94cec0cfc3ef991516465b76ddde8 -->

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
        "DeclaredWorkspaceProtection": {
          "enum": [
            "encrypted-at-rest",
            "memory-backed"
          ],
          "type": "string"
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
        "TargetCallbackAccess": {
          "additionalProperties": false,
          "properties": {
            "allow_insecure_http": {
              "default": false,
              "type": "boolean"
            },
            "stove0_base_url": {
              "maxLength": 2048,
              "minLength": 1,
              "type": "string"
            },
            "token": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "stove0_base_url",
            "token"
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
            "declared_workspace_protection": {
              "$ref": "#/$defs/DeclaredWorkspaceProtection"
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
            }
          },
          "required": [
            "job_id",
            "claim_id",
            "fence",
            "controller_evidence",
            "plan",
            "declared_workspace_protection"
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
        "TargetRuntimeAuthority": {
          "additionalProperties": false,
          "properties": {
            "allow_insecure_http": {
              "default": false,
              "type": "boolean"
            },
            "capability_token": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "riverhog_base_url": {
              "maxLength": 2048,
              "minLength": 1,
              "type": "string"
            },
            "transport": {
              "const": "riverhog-capability/v1",
              "default": "riverhog-capability/v1",
              "type": "string"
            }
          },
          "required": [
            "riverhog_base_url",
            "capability_token"
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
        "callback_access": {
          "$ref": "#/$defs/TargetCallbackAccess"
        },
        "declaration": {
          "$ref": "#/$defs/TargetJobDeclaration"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "runtime": {
          "$ref": "#/$defs/TargetRuntimeAuthority"
        }
      },
      "required": [
        "declaration",
        "runtime",
        "callback_access",
        "request_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, runtime: stove0_target_protocol.protocol.TargetRuntimeAuthority, callback_access: stove0_target_protocol.protocol.TargetCallbackAccess, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetJobRequest",
  "unit": "export"
}
```

</details>
