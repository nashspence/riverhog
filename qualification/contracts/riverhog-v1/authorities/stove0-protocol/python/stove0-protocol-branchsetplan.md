# stove0_protocol.BranchSetPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetplan:a61ab9e1a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c9b65240a"></a>
- <a id="s-bf0eeb1c6c"></a>`distribution`: `stove0-protocol`
- <a id="s-2be05d98be"></a>`module`: `stove0_protocol`
- <a id="s-35a648cc6b"></a>`name`: `BranchSetPlan`
- <a id="s-ca6c6f17aa"></a>`unit`: `export`

### Declared structure

- <a id="s-3e71bdce5a"></a>`kind`: `"class"`
- <a id="s-e9b9db92b9"></a>`signature`: `"\"(*, format: Literal['stove0-branch-set/v1'] = 'stove0-branch-set/v1', parent_work: stove0_protocol.models.WorkIdentity, decision_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], evidence_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = (), branches: Annotated[tuple[Annotated[stove0_protocol.fork_join.BranchPlan \| stove0_protocol.fork_join.CoordinationBranchPlan, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], join: stove0_protocol.fork_join.JoinDeclaration \| None = None, retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-30e73dc260"></a>

- <a id="s-03dc7fc455"></a>`type`: `"object"`
- <a id="s-334a39a1f3"></a>`additionalProperties`: `false`
- <a id="s-33263a59d1"></a>`required`: `["parent_work","decision_sha256","branches","branch_set_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b42e803f88"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5b2da67281"></a>`branches` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/CoordinationBranchPlan","leaf":"#/$defs/BranchPlan"},"propertyName":"kind"}; oneOf=[([BranchPlan](#s-68ec3d00c6)); ([CoordinationBranchPlan](#s-adb13afaef))]); minItems=1 |  |
| <a id="s-5c7a202702"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3ac54f6c0a"></a>`evidence_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-b9dcc56ae9"></a>`format` | no | type="string"; const="stove0-branch-set/v1"; default="stove0-branch-set/v1" |  |
| <a id="s-f863b5c986"></a>`join` | no | anyOf=[([JoinDeclaration](#s-cab5858423)); (type="null")]; default=null |  |
| <a id="s-efde35856b"></a>`parent_work` | yes | [WorkIdentity](#s-003bfc6007) |  |
| <a id="s-55ef77822a"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-f04ec5d290"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### Definitions

- [ArtifactSelectionRef](#s-e4fae573b4)
- [ArtifactSubject](#s-e3e47d640e)
- [BranchPlan](#s-68ec3d00c6)
- [BranchWorkBinding](#s-e314459c48)
- [CollectionId](#s-af3f4a04f5)
- [CollectionRootRef](#s-5bca169a71)
- [ContentObservationEvidence](#s-9fd5d07578)
- [ContentObservationFailure](#s-31e608f465)
- [ContentObservationInapplicable](#s-8c36698bad)
- [ContentObservationRequest](#s-5e3092c1c3)
- [ContentObservationResult](#s-4ca196691e)
- [CoordinationBranchPlan](#s-adb13afaef)
- [EvaluationBinding](#s-be6f19bdae)
- [JoinDeclaration](#s-cab5858423)
- [JoinMemberDeclaration](#s-6f8d016751)
- [JoinWorkBinding](#s-4f8c5a3e2c)
- [JoinWorkMemberBinding](#s-f42153e70b)
- [JsonSchemaValidationProfile](#s-a9252eadae)
- [JsonValue](#s-fb6fe79cb8)
- [ObserverImplementation](#s-f38f25ca17)
- [OperationRef](#s-30bcceecd4)
- [RecipeRef](#s-1e7455a09b)
- [WorkIdentity](#s-003bfc6007)
- [WorkflowPlan](#s-bcca79cd0a)
- [WorkflowPlanIntent](#s-16c6144f78)

##### <a id="s-e4fae573b4"></a>definition `ArtifactSelectionRef`

- <a id="s-b58e43e533"></a>`type`: `"object"`
- <a id="s-f4435d4148"></a>`additionalProperties`: `false`
- <a id="s-e71bfc1032"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61fbb4cc8c"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-7e5096e701"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ced5b89af5"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-e3e47d640e"></a>definition `ArtifactSubject`

- <a id="s-220cc31e51"></a>`type`: `"object"`
- <a id="s-bd84dfe4a5"></a>`additionalProperties`: `false`
- <a id="s-37636bec47"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0de522aca9"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-1ae1d867ac"></a>`collection` | yes | [CollectionRootRef](#s-5bca169a71) |  |
| <a id="s-da1e86e6c9"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-86b3619b02"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-109b1ee38f"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-9f50446ad7"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6aa105b601"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-68ec3d00c6"></a>definition `BranchPlan`

- <a id="s-2ec9570e9e"></a>`type`: `"object"`
- <a id="s-121ef1278f"></a>`additionalProperties`: `false`
- <a id="s-9276773e60"></a>`required`: `["branch_id","artifact_selection","workflow_plan"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1b15b5ed7b"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-e4fae573b4) |  |
| <a id="s-42a812b726"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e32f3cde38"></a>`kind` | no | type="string"; const="leaf"; default="leaf" |  |
| <a id="s-ad67bf99f0"></a>`workflow_plan` | yes | [WorkflowPlan](#s-bcca79cd0a) |  |

##### <a id="s-e314459c48"></a>definition `BranchWorkBinding`

- <a id="s-9b385e222c"></a>`type`: `"object"`
- <a id="s-72e94a0a3e"></a>`additionalProperties`: `false`
- <a id="s-62d88fc83b"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a6c1539b4"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6388c19f21"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c08f81237c"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa61b63105"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-d1be8e3068"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-af3f4a04f5"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-064985ff11"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-321cf27e62"></a>2 | not=(const="0") |

##### <a id="s-5bca169a71"></a>definition `CollectionRootRef`

- <a id="s-ba8be0eaca"></a>`type`: `"object"`
- <a id="s-9fc815d221"></a>`additionalProperties`: `false`
- <a id="s-8645c92027"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa4ee7dd99"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3381059551"></a>`collection_id` | yes | [CollectionId](#s-af3f4a04f5) |  |
| <a id="s-65ac477aba"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9fd5d07578"></a>definition `ContentObservationEvidence`

- <a id="s-f9960e3d98"></a>`type`: `"object"`
- <a id="s-d85fd66629"></a>`additionalProperties`: `false`
- <a id="s-0ac0c63ea3"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ecefd4544a"></a>`request` | yes | [ContentObservationRequest](#s-5e3092c1c3) |  |
| <a id="s-8efd9fc15d"></a>`result` | yes | [ContentObservationResult](#s-4ca196691e) |  |

##### <a id="s-31e608f465"></a>definition `ContentObservationFailure`

- <a id="s-191eae5af6"></a>`type`: `"object"`
- <a id="s-d22e529831"></a>`additionalProperties`: `false`
- <a id="s-cc72dd2742"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ecef59cca"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-73f97a587e"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-b6f5679d10"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-8c36698bad"></a>definition `ContentObservationInapplicable`

- <a id="s-d8cc540e80"></a>`type`: `"object"`
- <a id="s-6c7578fc72"></a>`additionalProperties`: `false`
- <a id="s-6483872f5a"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a9bfe21d3d"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ef5636d4ff"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-5e3092c1c3"></a>definition `ContentObservationRequest`

- <a id="s-b44827cb33"></a>`type`: `"object"`
- <a id="s-761c4cf53a"></a>`additionalProperties`: `false`
- <a id="s-703067858f"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22ef75b328"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-f137ee12e6"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-98a53676a6"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b3d9f7b920"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7c4a4f2634"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d6820b9726"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-80827cb834"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-1c2c7b2484"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b956923cc8"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-3166038457"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-e3e47d640e)); minItems=1 |  |
| <a id="s-88366366fa"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-bd925a76c3"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4ca196691e"></a>definition `ContentObservationResult`

- <a id="s-114a75dc93"></a>`type`: `"object"`
- <a id="s-a3652e4adf"></a>`additionalProperties`: `false`
- <a id="s-ba6f85aa49"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d482d1d938"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-8c9a7b41fb"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8))); (type="null")]; default=null |  |
| <a id="s-923baa860a"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-a9252eadae)); (type="null")]; default=null |  |
| <a id="s-2945bc52d2"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-4da6cc0d98"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-31e608f465)); (type="null")]; default=null |  |
| <a id="s-58de8ab16f"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-d1af1b1b00"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-8c36698bad)); (type="null")]; default=null |  |
| <a id="s-4c4e59e38b"></a>`observer` | yes | [ObserverImplementation](#s-f38f25ca17) |  |
| <a id="s-76030978e4"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4b3a55a51d"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d100c1daa"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e4eb40a204"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d4163bfa2b"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-ca0fd2ff13"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-e3e47d640e)); minItems=1 |  |

##### <a id="s-adb13afaef"></a>definition `CoordinationBranchPlan`

- <a id="s-dda5c25ac4"></a>`type`: `"object"`
- <a id="s-111811c247"></a>`additionalProperties`: `false`
- <a id="s-c7ab3a7da7"></a>`required`: `["branch_id","artifact_selection","work","branch_set_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-614085ae61"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-e4fae573b4) |  |
| <a id="s-143122de6e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9d309985d9"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3e790a3fe0"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-17182c43c1"></a>`work` | yes | [WorkIdentity](#s-003bfc6007) |  |

##### <a id="s-be6f19bdae"></a>definition `EvaluationBinding`

- <a id="s-d0ff6bdcb1"></a>`type`: `"object"`
- <a id="s-51ff72e9a4"></a>`additionalProperties`: `false`
- <a id="s-94aa448594"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5e234c5ead"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9120bd91ec"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6b6f75d2e4"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-05a08298c7"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-cab5858423"></a>definition `JoinDeclaration`

- <a id="s-9a520a2f27"></a>`type`: `"object"`
- <a id="s-69553e5874"></a>`additionalProperties`: `false`
- <a id="s-8bc56960f5"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-952b3e33c4"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-7c70719b6b"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-cb43f2cc3a"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-870e31e278"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-6f8d016751)); minItems=2 |  |
| <a id="s-b5c2081ca3"></a>`recipe` | yes | [RecipeRef](#s-1e7455a09b) |  |
| <a id="s-c64ae9644d"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-16c6144f78) |  |

##### <a id="s-6f8d016751"></a>definition `JoinMemberDeclaration`

- <a id="s-0dfee8f9a7"></a>`type`: `"object"`
- <a id="s-39fe21cc74"></a>`additionalProperties`: `false`
- <a id="s-fd594f8e05"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f798dc65ac"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e6899807ee"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-4f8c5a3e2c"></a>definition `JoinWorkBinding`

- <a id="s-a725738d99"></a>`type`: `"object"`
- <a id="s-2848a2e00d"></a>`additionalProperties`: `false`
- <a id="s-5560d8423c"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b2f1676546"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e8fb4828fe"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-5c85c17e47"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-f42153e70b)); minItems=2 |  |
| <a id="s-af744d45f7"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f42153e70b"></a>definition `JoinWorkMemberBinding`

- <a id="s-0a7b6528fd"></a>`type`: `"object"`
- <a id="s-01943d2884"></a>`additionalProperties`: `false`
- <a id="s-0ea5df90a0"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1c388d6bf"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-330f35b491"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-22280de5ce"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-466ee59516"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a9252eadae"></a>definition `JsonSchemaValidationProfile`

- <a id="s-fdf20203d2"></a>`type`: `"object"`
- <a id="s-951804b1ac"></a>`additionalProperties`: `false`
- <a id="s-a2342913d3"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea384c0357"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-3d014879fa"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-469b717aee"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5cdd81b909"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d40a0496f2"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |

##### <a id="s-fb6fe79cb8"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-f38f25ca17"></a>definition `ObserverImplementation`

- <a id="s-9d742ed3dc"></a>`type`: `"object"`
- <a id="s-345a4c12a1"></a>`additionalProperties`: `false`
- <a id="s-f04e4e4f7c"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ee8eaf93a"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5527f79ffe"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-51a8d2a7f4"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-0f4f6ee9df"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-f14a76e250"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-30bcceecd4"></a>definition `OperationRef`

- <a id="s-6b79c6918f"></a>`type`: `"object"`
- <a id="s-8bd38b8609"></a>`additionalProperties`: `false`
- <a id="s-83c20562aa"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aef1eedf0e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8854fa6fc0"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1e7455a09b"></a>definition `RecipeRef`

- <a id="s-a5a47d6a57"></a>`type`: `"object"`
- <a id="s-bd9b4b6b7b"></a>`additionalProperties`: `false`
- <a id="s-1917ba9328"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bf57bccf71"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0977b9b327"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-0dfd293f89"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-003bfc6007"></a>definition `WorkIdentity`

- <a id="s-306a6da975"></a>`type`: `"object"`
- <a id="s-b1503ea593"></a>`additionalProperties`: `false`
- <a id="s-53ad7b5486"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60a91e7948"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-a4cd07514d"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-be6f19bdae)); (type="null")]; default=null |  |
| <a id="s-02532cc7f3"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-e314459c48)); ([JoinWorkBinding](#s-4f8c5a3e2c))]); (type="null")]; default=null |  |
| <a id="s-347fbc9099"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-9e55534874"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-5bca169a71)); minItems=1 |  |
| <a id="s-f8d3d05f09"></a>`recipe` | yes | [RecipeRef](#s-1e7455a09b) |  |
| <a id="s-b4935ab020"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bcca79cd0a"></a>definition `WorkflowPlan`

- <a id="s-b3c279caa6"></a>`type`: `"object"`
- <a id="s-9f04b93434"></a>`additionalProperties`: `false`
- <a id="s-df5bb2a8f2"></a>`required`: `["work","operation","target_registration_id","target_contract_sha256","workflow_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fca81f02cc"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1" |  |
| <a id="s-6deff9b21d"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-d6820ecc6d"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-9fd5d07578)) |  |
| <a id="s-d2acecf469"></a>`operation` | yes | [OperationRef](#s-30bcceecd4) |  |
| <a id="s-cd027ca484"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-8dbdc2da51"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-a5917756ef"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-6addcaf94c"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-609b7fe242"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-322d9efcd3"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b4b6f189c0"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-7eae345fe6"></a>`work` | yes | [WorkIdentity](#s-003bfc6007) |  |
| <a id="s-676758c48c"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-16c6144f78"></a>definition `WorkflowPlanIntent`

- <a id="s-71590379e1"></a>`type`: `"object"`
- <a id="s-4a09e3bd3d"></a>`additionalProperties`: `false`
- <a id="s-5c8b2db9df"></a>`required`: `["operation","target_registration_id","target_contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8c14827202"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-ac50388ef9"></a>`operation` | yes | [OperationRef](#s-30bcceecd4) |  |
| <a id="s-4fd20ffca6"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-0061d371a9"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-fb6fe79cb8)) |  |
| <a id="s-16502fe46d"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-98146cf19f"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-5414e7a738"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-62e817a855"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ae04f16ba6"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_branches](stove0-protocol-branchsetplan-canonical-branches.md)
- [canonical_bytes](stove0-protocol-branchsetplan-canonical-bytes.md)
- [canonical_evidence](stove0-protocol-branchsetplan-canonical-evidence.md)
- [seal](stove0-protocol-branchsetplan-seal.md)
- [verify_contract](stove0-protocol-branchsetplan-verify-contract.md)

## Governing policies

- <a id="pa-72bff8eb17"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 358584e9acba182fa9f4639b56935ec239b3686f87d61ff8e067bf599907ac57 -->

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
    "signature": "\"(*, format: Literal['stove0-branch-set/v1'] = 'stove0-branch-set/v1', parent_work: stove0_protocol.models.WorkIdentity, decision_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], evidence_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = (), branches: Annotated[tuple[Annotated[stove0_protocol.fork_join.BranchPlan | stove0_protocol.fork_join.CoordinationBranchPlan, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], join: stove0_protocol.fork_join.JoinDeclaration | None = None, retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchSetPlan",
  "unit": "export"
}
```

</details>
