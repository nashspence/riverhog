# stove0_protocol.WorkflowPreview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview:bbf475d7e1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-651702325b"></a>
- <a id="s-812cee1ddd"></a>`distribution`: `stove0-protocol`
- <a id="s-a0917f42b4"></a>`module`: `stove0_protocol`
- <a id="s-e79104d0ec"></a>`name`: `WorkflowPreview`
- <a id="s-d96e0e3c3f"></a>`unit`: `export`

### Declared structure

- <a id="s-3bd595da1e"></a>`kind`: `"class"`
- <a id="s-8250f12a24"></a>`signature`: `"\"(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ContentObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan \| None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome \| None = None, warnings: tuple[str, ...] = (), preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-81a1ea6aa0"></a>

- <a id="s-8a04fe0c55"></a>`type`: `"object"`
- <a id="s-79f9d371f0"></a>`additionalProperties`: `false`
- <a id="s-dea15b1441"></a>`required`: `["preview_id","state","work","preview_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0e42aa9676"></a>`branch_set_plan` | no | anyOf=[([BranchSetPlan](#s-21ac64d411)); (type="null")]; default=null |  |
| <a id="s-a76b72dca1"></a>`branch_sets` | no | type="array"; default=[]; items=([BranchSetPlan](#s-21ac64d411)) |  |
| <a id="s-3c88f9e333"></a>`format` | no | type="string"; const="stove0-workflow-preview/v1"; default="stove0-workflow-preview/v1" |  |
| <a id="s-3d2b0ee24f"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-abecead756)) |  |
| <a id="s-47ed167091"></a>`outcome` | no | anyOf=[([PreviewOutcome](#s-b22537e605)); (type="null")]; default=null |  |
| <a id="s-87bb34d9ad"></a>`preview_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-451df9d341"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bdb06537dd"></a>`selections` | no | type="array"; default=[]; items=([ArtifactSelection](#s-a714f2944c)) |  |
| <a id="s-ed517e1872"></a>`state` | yes | type="string"; enum=["ready","inapplicable","failed","canceled"] |  |
| <a id="s-fc8e8c6b96"></a>`target_plans` | no | type="array"; default=[]; items=([BranchTargetPreview](#s-3c67d4e666)) |  |
| <a id="s-d776a4e46e"></a>`warnings` | no | type="array"; default=[]; items=(type="string") |  |
| <a id="s-878e671f1a"></a>`work` | yes | [WorkIdentity](#s-3869a9b08e) |  |

##### Definitions

- [ArtifactSelection](#s-a714f2944c)
- [ArtifactSelectionRef](#s-c80df5a893)
- [ArtifactSubject](#s-ac676d0abd)
- [BranchPlan](#s-3f7e0f08b1)
- [BranchSetPlan](#s-21ac64d411)
- [BranchTargetPreview](#s-3c67d4e666)
- [BranchWorkBinding](#s-b2eb508a56)
- [CollectionId](#s-cb9942ab76)
- [CollectionRootRef](#s-79d50387f6)
- [ContentObservationEvidence](#s-abecead756)
- [ContentObservationFailure](#s-3926dee94c)
- [ContentObservationInapplicable](#s-9f7e5a9dd4)
- [ContentObservationRequest](#s-c6892c5dc2)
- [ContentObservationResult](#s-e424feead0)
- [CoordinationBranchPlan](#s-cbec4e220b)
- [EvaluationBinding](#s-01ce021abb)
- [JoinDeclaration](#s-f8578c1e81)
- [JoinMemberDeclaration](#s-6e947c9694)
- [JoinWorkBinding](#s-d88819dfa6)
- [JoinWorkMemberBinding](#s-018eaaf578)
- [JsonSchemaValidationProfile](#s-67d8e3da93)
- [JsonValue](#s-8664a811cd)
- [ObserverImplementation](#s-7d26ff83dc)
- [OperationRef](#s-536e9e8cac)
- [PreviewOutcome](#s-b22537e605)
- [RecipeRef](#s-d26d08db0c)
- [TargetPlanBinding](#s-58822f76be)
- [WorkIdentity](#s-3869a9b08e)
- [WorkflowPlan](#s-3bf71a6673)
- [WorkflowPlanIntent](#s-8849589028)

##### <a id="s-a714f2944c"></a>definition `ArtifactSelection`

- <a id="s-520de12991"></a>`type`: `"object"`
- <a id="s-f374266355"></a>`additionalProperties`: `false`
- <a id="s-112bc2d158"></a>`required`: `["artifacts","artifact_count","total_bytes","selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6f4fcb6396"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-bf9e7088e1"></a>`artifacts` | yes | type="array"; items=([ArtifactSubject](#s-ac676d0abd)); minItems=1 |  |
| <a id="s-736fda6e0e"></a>`format` | no | type="string"; const="stove0-artifact-selection/v1"; default="stove0-artifact-selection/v1" |  |
| <a id="s-c3e0005c8f"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c6b5e8a668"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-c80df5a893"></a>definition `ArtifactSelectionRef`

- <a id="s-dbab0d71e9"></a>`type`: `"object"`
- <a id="s-dace9d0a46"></a>`additionalProperties`: `false`
- <a id="s-025f95cd2f"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ece57bc175"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-c33cb7ae2b"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7e2e7eff89"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-ac676d0abd"></a>definition `ArtifactSubject`

- <a id="s-ce9525b12f"></a>`type`: `"object"`
- <a id="s-6b9a4f0a7d"></a>`additionalProperties`: `false`
- <a id="s-b671fdfb4e"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d9310d2afa"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-26c613f241"></a>`collection` | yes | [CollectionRootRef](#s-79d50387f6) |  |
| <a id="s-ce4c320708"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-76227d5910"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-77aa374f83"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-f0839bee50"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-10abd7845e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3f7e0f08b1"></a>definition `BranchPlan`

- <a id="s-a9db8923e3"></a>`type`: `"object"`
- <a id="s-d27ca31d32"></a>`additionalProperties`: `false`
- <a id="s-4128540c97"></a>`required`: `["branch_id","artifact_selection","workflow_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-90a300c58f"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-c80df5a893) |  |
| <a id="s-f2755e78e0"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-50125c7613"></a>`kind` | no | type="string"; const="leaf"; default="leaf" |  |
| <a id="s-37a627cc23"></a>`workflow_plan` | yes | [WorkflowPlan](#s-3bf71a6673) |  |

##### <a id="s-21ac64d411"></a>definition `BranchSetPlan`

- <a id="s-f92402a928"></a>`type`: `"object"`
- <a id="s-2164eea1aa"></a>`additionalProperties`: `false`
- <a id="s-81cf0f164b"></a>`required`: `["parent_work","decision_sha256","branches","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7adf8e72fa"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dc568cb6f6"></a>`branches` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/CoordinationBranchPlan","leaf":"#/$defs/BranchPlan"},"propertyName":"kind"}; oneOf=[([BranchPlan](#s-3f7e0f08b1)); ([CoordinationBranchPlan](#s-cbec4e220b))]); minItems=1 |  |
| <a id="s-f625cb16a8"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ee620402ff"></a>`evidence_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-de1b0928aa"></a>`format` | no | type="string"; const="stove0-branch-set/v1"; default="stove0-branch-set/v1" |  |
| <a id="s-9f8fdaa2fb"></a>`join` | no | anyOf=[([JoinDeclaration](#s-f8578c1e81)); (type="null")]; default=null |  |
| <a id="s-a39eace417"></a>`parent_work` | yes | [WorkIdentity](#s-3869a9b08e) |  |
| <a id="s-9b3ade8e56"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-9836a31577"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### <a id="s-3c67d4e666"></a>definition `BranchTargetPreview`

- <a id="s-6b684d2363"></a>`type`: `"object"`
- <a id="s-c8bf2cb2f1"></a>`additionalProperties`: `false`
- <a id="s-fe57b7fbe5"></a>`required`: `["branch_id","work_id","workflow_plan_sha256","target_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9f28abdb68"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ec4f38377d"></a>`target_plan` | yes | [TargetPlanBinding](#s-58822f76be) |  |
| <a id="s-8c2f3fe054"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4bf90c1664"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b2eb508a56"></a>definition `BranchWorkBinding`

- <a id="s-da0ee9fbb6"></a>`type`: `"object"`
- <a id="s-d5ce641032"></a>`additionalProperties`: `false`
- <a id="s-c7e6d0e7ca"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba02c5c7c2"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-200b3ed244"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7a7fd8c989"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1bd547e450"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-3b5b011ac4"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-cb9942ab76"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-1709eb1134"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-fc759d35b5"></a>2 | not=(const="0") |

##### <a id="s-79d50387f6"></a>definition `CollectionRootRef`

- <a id="s-a89ba0b6bc"></a>`type`: `"object"`
- <a id="s-d9b524ed4b"></a>`additionalProperties`: `false`
- <a id="s-db3d5f3542"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7bb33ff7ff"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b41660ee38"></a>`collection_id` | yes | [CollectionId](#s-cb9942ab76) |  |
| <a id="s-9781bc46d9"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-abecead756"></a>definition `ContentObservationEvidence`

- <a id="s-04283146f1"></a>`type`: `"object"`
- <a id="s-5db7176ed0"></a>`additionalProperties`: `false`
- <a id="s-82e1ec41f5"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a28d96292c"></a>`request` | yes | [ContentObservationRequest](#s-c6892c5dc2) |  |
| <a id="s-5c2f219199"></a>`result` | yes | [ContentObservationResult](#s-e424feead0) |  |

##### <a id="s-3926dee94c"></a>definition `ContentObservationFailure`

- <a id="s-85facef9d6"></a>`type`: `"object"`
- <a id="s-9eb25c86e2"></a>`additionalProperties`: `false`
- <a id="s-5bef623ac6"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b059af92f"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c3d196ab50"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-3835344e47"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-9f7e5a9dd4"></a>definition `ContentObservationInapplicable`

- <a id="s-890a3cb31c"></a>`type`: `"object"`
- <a id="s-0a7da1c30a"></a>`additionalProperties`: `false`
- <a id="s-62a006a86a"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-653e74c206"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ea83f3328d"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-c6892c5dc2"></a>definition `ContentObservationRequest`

- <a id="s-f0268d53f5"></a>`type`: `"object"`
- <a id="s-f276d288cb"></a>`additionalProperties`: `false`
- <a id="s-7168bdbfe8"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-941e99b727"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-5d74b65623"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-690c1c9474"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-96c49b4450"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1a7de2fcc4"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-34112edb9c"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-bc2cb4fe33"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-92bf566e41"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f0c057f970"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-0d28df7b44"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ac676d0abd)); minItems=1 |  |
| <a id="s-3211e07a77"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-b2873e1418"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e424feead0"></a>definition `ContentObservationResult`

- <a id="s-030e9c8520"></a>`type`: `"object"`
- <a id="s-66bce888f6"></a>`additionalProperties`: `false`
- <a id="s-e310986e0c"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-144831c2ce"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-43e2a3361a"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-8664a811cd))); (type="null")]; default=null |  |
| <a id="s-350e81c46d"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-67d8e3da93)); (type="null")]; default=null |  |
| <a id="s-fdcab17ded"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a9eea3bc22"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-3926dee94c)); (type="null")]; default=null |  |
| <a id="s-8248fac814"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-1bb0ffac4a"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-9f7e5a9dd4)); (type="null")]; default=null |  |
| <a id="s-4c7be0f0db"></a>`observer` | yes | [ObserverImplementation](#s-7d26ff83dc) |  |
| <a id="s-399131d305"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9519a5d04e"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7ff3aeb1f7"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-783a64fcaa"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9e67620977"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-0d263b210a"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-ac676d0abd)); minItems=1 |  |

##### <a id="s-cbec4e220b"></a>definition `CoordinationBranchPlan`

- <a id="s-10935e4532"></a>`type`: `"object"`
- <a id="s-3de77169e6"></a>`additionalProperties`: `false`
- <a id="s-eac8c35310"></a>`required`: `["branch_id","artifact_selection","work","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7780d4ca2c"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-c80df5a893) |  |
| <a id="s-472959ef8f"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f16fdb0ee5"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1da54f3c53"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-e1bb2d7988"></a>`work` | yes | [WorkIdentity](#s-3869a9b08e) |  |

##### <a id="s-01ce021abb"></a>definition `EvaluationBinding`

- <a id="s-0fafbc64b0"></a>`type`: `"object"`
- <a id="s-ae39ba968b"></a>`additionalProperties`: `false`
- <a id="s-7d4dc097b3"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0559e5f608"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8b0cbe7fdd"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fee19befa5"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-844e5b569f"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-f8578c1e81"></a>definition `JoinDeclaration`

- <a id="s-71bb717b0c"></a>`type`: `"object"`
- <a id="s-1595059cac"></a>`additionalProperties`: `false`
- <a id="s-4b9e031787"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fbeca3f21c"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-c96584bcf4"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-71c8e4fd5c"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bbd58fff52"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-6e947c9694)); minItems=2 |  |
| <a id="s-4c24b4a150"></a>`recipe` | yes | [RecipeRef](#s-d26d08db0c) |  |
| <a id="s-0cc6cdaf72"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-8849589028) |  |

##### <a id="s-6e947c9694"></a>definition `JoinMemberDeclaration`

- <a id="s-75c49d45a3"></a>`type`: `"object"`
- <a id="s-78f62466af"></a>`additionalProperties`: `false`
- <a id="s-9cb69edda7"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1af354d3f2"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cf14b3c661"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-d88819dfa6"></a>definition `JoinWorkBinding`

- <a id="s-622bee89d7"></a>`type`: `"object"`
- <a id="s-b235929776"></a>`additionalProperties`: `false`
- <a id="s-bdfa925623"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-30808feb1f"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3f812b42c2"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-c4d698c5fe"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-018eaaf578)); minItems=2 |  |
| <a id="s-a2449e3691"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-018eaaf578"></a>definition `JoinWorkMemberBinding`

- <a id="s-9e7eea98d0"></a>`type`: `"object"`
- <a id="s-f27a63f4ec"></a>`additionalProperties`: `false`
- <a id="s-01fcd075e8"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac8f684c93"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-50b8de21b6"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4d09fa465e"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-40a62d44c7"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-67d8e3da93"></a>definition `JsonSchemaValidationProfile`

- <a id="s-99dc0b36cc"></a>`type`: `"object"`
- <a id="s-69455dd195"></a>`additionalProperties`: `false`
- <a id="s-30a6b69576"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdb9156318"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-4e7e467401"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-3f8ed7a07f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3bb4541a7c"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9878462de2"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |

##### <a id="s-8664a811cd"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-7d26ff83dc"></a>definition `ObserverImplementation`

- <a id="s-25b0162436"></a>`type`: `"object"`
- <a id="s-25e53c2405"></a>`additionalProperties`: `false`
- <a id="s-eda685305f"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-035576481c"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c8c2c2c304"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-45c2555e7e"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-d1f76aa4ca"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-b77543e2c4"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-536e9e8cac"></a>definition `OperationRef`

- <a id="s-7a4004908e"></a>`type`: `"object"`
- <a id="s-65e70511ce"></a>`additionalProperties`: `false`
- <a id="s-e31672f978"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7da3ed5e2a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c1c38ce734"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b22537e605"></a>definition `PreviewOutcome`

- <a id="s-e97a8300fe"></a>`type`: `"object"`
- <a id="s-1edae74287"></a>`additionalProperties`: `false`
- <a id="s-106c9ea7a2"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-abc9747caa"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8b21dbcdb7"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-05b2fc3a56"></a>`retryable` | no | anyOf=[(type="boolean"); (type="null")]; default=null |  |

##### <a id="s-d26d08db0c"></a>definition `RecipeRef`

- <a id="s-c078ae5bba"></a>`type`: `"object"`
- <a id="s-9f1e0a03ed"></a>`additionalProperties`: `false`
- <a id="s-76a28e117c"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cbf4fe296"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-495256e645"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-eb65582adc"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-58822f76be"></a>definition `TargetPlanBinding`

- <a id="s-393c61b692"></a>`type`: `"object"`
- <a id="s-9794b18070"></a>`additionalProperties`: `false`
- <a id="s-78af5cd4e4"></a>`required`: `["protocol","target_implementation_id","target_contract_sha256","operation_contract_sha256","plan","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a508b69a38"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bdff1198a2"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-1db2ed2ad2"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1fb414e3b2"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f11667e891"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-86a04e9311"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-3869a9b08e"></a>definition `WorkIdentity`

- <a id="s-eed73043f1"></a>`type`: `"object"`
- <a id="s-7dcf23ff07"></a>`additionalProperties`: `false`
- <a id="s-edef6756f5"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ce3f7fa23"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-34d640d200"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-01ce021abb)); (type="null")]; default=null |  |
| <a id="s-a3d52d0c28"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-b2eb508a56)); ([JoinWorkBinding](#s-d88819dfa6))]); (type="null")]; default=null |  |
| <a id="s-c76ad3e4f9"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-d8f6e0e687"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-79d50387f6)); minItems=1 |  |
| <a id="s-cc9c0fae9b"></a>`recipe` | yes | [RecipeRef](#s-d26d08db0c) |  |
| <a id="s-ed3b58b8d5"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3bf71a6673"></a>definition `WorkflowPlan`

- <a id="s-6d339dc7ab"></a>`type`: `"object"`
- <a id="s-5e5ba2efdb"></a>`additionalProperties`: `false`
- <a id="s-1ca28af7c6"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b8f38b2bcc"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-09a0caf5dd"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-02806d3613"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-abecead756)) |  |
| <a id="s-993f8f2bea"></a>`operation` | yes | [OperationRef](#s-536e9e8cac) |  |
| <a id="s-cb552379d5"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-0b5987af4a"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-fd89bbb07d"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-4498780b6d"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-612cc4ce06"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-039a5e4018"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8612bcebb9"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-7b281c6641"></a>`work` | yes | [WorkIdentity](#s-3869a9b08e) |  |
| <a id="s-575647c50d"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8849589028"></a>definition `WorkflowPlanIntent`

- <a id="s-8f0fd727d8"></a>`type`: `"object"`
- <a id="s-5e59e281ed"></a>`additionalProperties`: `false`
- <a id="s-dbd8bafad7"></a>`required`: `["operation","target_registration_id","target_contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83e48ff1d9"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-3955b9c8e1"></a>`operation` | yes | [OperationRef](#s-536e9e8cac) |  |
| <a id="s-0711d09284"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-0ac9e643ec"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-8664a811cd)) |  |
| <a id="s-c5dc37f0ff"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-c607c88a88"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-9df9ff211f"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-d51726b8da"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-503a6d55ba"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_observations](stove0-protocol-workflowpreview-canonical-observations.md)
- [canonical_target_plans](stove0-protocol-workflowpreview-canonical-target-plans.md)
- [canonical_child_branch_sets](stove0-protocol-workflowpreview-canonical-child-branch-sets.md)
- [canonical_selections](stove0-protocol-workflowpreview-canonical-selections.md)
- [canonical_warnings](stove0-protocol-workflowpreview-canonical-warnings.md)
- [seal](stove0-protocol-workflowpreview-seal.md)
- [validate_state](stove0-protocol-workflowpreview-validate-state.md)
- [verify_digest](stove0-protocol-workflowpreview-verify-digest.md)

## Governing policies

- <a id="pa-006a1cbc2a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0ff9d51c6b4a476d6f9b3db8a0d16d7c0e3812e77c2923b0ec5d0ea40c03af5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelection": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "artifacts": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
              "type": "array"
            },
            "format": {
              "const": "stove0-artifact-selection/v1",
              "default": "stove0-artifact-selection/v1",
              "type": "string"
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
            "artifacts",
            "artifact_count",
            "total_bytes",
            "selection_sha256"
          ],
          "type": "object"
        },
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
        "BranchPlan": {
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
        "BranchSetPlan": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "branches": {
              "items": {
                "discriminator": {
                  "mapping": {
                    "coordination": "#/$defs/CoordinationBranchPlan",
                    "leaf": "#/$defs/BranchPlan"
                  },
                  "propertyName": "kind"
                },
                "oneOf": [
                  {
                    "$ref": "#/$defs/BranchPlan"
                  },
                  {
                    "$ref": "#/$defs/CoordinationBranchPlan"
                  }
                ]
              },
              "minItems": 1,
              "type": "array"
            },
            "decision_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "evidence_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "format": {
              "const": "stove0-branch-set/v1",
              "default": "stove0-branch-set/v1",
              "type": "string"
            },
            "join": {
              "anyOf": [
                {
                  "$ref": "#/$defs/JoinDeclaration"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "parent_work": {
              "$ref": "#/$defs/WorkIdentity"
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
            }
          },
          "required": [
            "parent_work",
            "decision_sha256",
            "branches",
            "branch_set_sha256"
          ],
          "type": "object"
        },
        "BranchTargetPreview": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_plan": {
              "$ref": "#/$defs/TargetPlanBinding"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "workflow_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "work_id",
            "workflow_plan_sha256",
            "target_plan"
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
        "CoordinationBranchPlan": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "kind": {
              "const": "coordination",
              "default": "coordination",
              "type": "string"
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            }
          },
          "required": [
            "branch_id",
            "artifact_selection",
            "work",
            "branch_set_sha256"
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
        "PreviewOutcome": {
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
              "anyOf": [
                {
                  "type": "boolean"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "code",
            "message"
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
                "$ref": "#/$defs/ContentObservationEvidence"
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
        "branch_set_plan": {
          "anyOf": [
            {
              "$ref": "#/$defs/BranchSetPlan"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "branch_sets": {
          "default": [],
          "items": {
            "$ref": "#/$defs/BranchSetPlan"
          },
          "type": "array"
        },
        "format": {
          "const": "stove0-workflow-preview/v1",
          "default": "stove0-workflow-preview/v1",
          "type": "string"
        },
        "observations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ContentObservationEvidence"
          },
          "type": "array"
        },
        "outcome": {
          "anyOf": [
            {
              "$ref": "#/$defs/PreviewOutcome"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "preview_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "preview_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "selections": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ArtifactSelection"
          },
          "type": "array"
        },
        "state": {
          "enum": [
            "ready",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "type": "string"
        },
        "target_plans": {
          "default": [],
          "items": {
            "$ref": "#/$defs/BranchTargetPreview"
          },
          "type": "array"
        },
        "warnings": {
          "default": [],
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        }
      },
      "required": [
        "preview_id",
        "state",
        "work",
        "preview_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ContentObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome | None = None, warnings: tuple[str, ...] = (), preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreview",
  "unit": "export"
}
```

</details>
