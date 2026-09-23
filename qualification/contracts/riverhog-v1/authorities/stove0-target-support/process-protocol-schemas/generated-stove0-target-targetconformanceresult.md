# generated:stove0-target: TargetConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-targetconformanceresult:63e54b6084 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-8cf797b25e"></a>

- <a id="s-83a0e7889f"></a>`type`: `"object"`
- <a id="s-49b0e32acd"></a>`additionalProperties`: `false`
- <a id="s-bd5c1a3619"></a>`required`: `["status","descriptor","coverage","operations"]`
- <a id="s-166034031e"></a>`title`: `"TargetConformanceResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54849ef8ea"></a>`coverage` | yes | [TargetConformanceCoverage](#s-3af4423fcb) |  |
| <a id="s-828d809b16"></a>`descriptor` | yes | [TargetDescriptor](#s-c5f049f824) |  |
| <a id="s-073c47c5d8"></a>`format` | no | type="string"; const="stove0-target-conformance-result/v1"; default="stove0-target-conformance-result/v1"; title="Format" |  |
| <a id="s-cdbf79ac63"></a>`operation_evidence` | no | type="array"; default=[]; items=([TargetOperationConformanceEvidence](#s-fd16c1183e)); title="Operation Evidence" |  |
| <a id="s-b7d24726ce"></a>`operations` | yes | type="array"; items=([TargetOperationConformance](#s-d56ff4bc3b)); title="Operations" |  |
| <a id="s-65f3100125"></a>`status` | yes | type="string"; enum=["conformant","partially-exercised","inspected"]; title="Status" |  |

### Definitions

- [AcceptedTargetJob](#s-0993620c2a)
- [ArtifactDispositionSetIdentity](#s-64c14db0d1)
- [ArtifactSelectionRef](#s-4348e1d25c)
- [ArtifactSubject](#s-9650f4fcc0)
- [BranchWorkBinding](#s-5d02bd6d33)
- [CollectionId](#s-40a59539e2)
- [CollectionRootRef](#s-3b0dd8e93b)
- [ContentObservationEvidence](#s-eb0ebf9844)
- [ContentObservationFailure](#s-c812e9c1db)
- [ContentObservationInapplicable](#s-47d3ea9222)
- [ContentObservationRequest](#s-d39e50e273)
- [ContentObservationResult](#s-860cf42eef)
- [ControllerEvidence](#s-b707894e8c)
- [DeclaredWorkspaceProtection](#s-7c36dc1beb)
- [EffectPlan](#s-ef86be7809)
- [EvaluationBinding](#s-06c04695a5)
- [ExecutionEnvelope](#s-3c944b74fe)
- [ExternalEffectReceipt](#s-91999efbe2)
- [InputArtifactContract](#s-ae1b42ecf6)
- [JoinWorkBinding](#s-13aa1fbe2e)
- [JoinWorkMemberBinding](#s-b0603b6193)
- [JsonSchemaValidationProfile](#s-5dd81cb02e)
- [JsonValue](#s-40b30ec575)
- [ObserverImplementation](#s-df8b23cd98)
- [OperationContract](#s-0d00de964e)
- [OperationRef](#s-46180fa7c3)
- [OutputArtifactContract](#s-d338c851cc)
- [OutputArtifactRoleCount](#s-deff193b8e)
- [OutputArtifactSetIdentity](#s-fc2841cacc)
- [OutputCollectionRef](#s-7a55ca8250)
- [RecipeRef](#s-175e50de18)
- [SemanticIntentConformanceVector](#s-7bceecb5d9)
- [SemanticIntentConformanceVectors](#s-f235361316)
- [SemanticValidationProfile](#s-ee0cfd3dd1)
- [TargetConformanceCoverage](#s-3af4423fcb)
- [TargetDescriptor](#s-c5f049f824)
- [TargetExecutionEvidence](#s-633d457279)
- [TargetFailure](#s-c0bd76e74f)
- [TargetInapplicable](#s-d1d84486e0)
- [TargetInputAuthority](#s-d47194a8ed)
- [TargetInputRoleCount](#s-a279c59e72)
- [TargetJobDeclaration](#s-cd86ac44a9)
- [TargetJobStatus](#s-ef307f8c14)
- [TargetOperationConformance](#s-d56ff4bc3b)
- [TargetOperationConformanceEvidence](#s-fd16c1183e)
- [TargetOperationSupport](#s-bf993b5ad1)
- [TargetPlanBinding](#s-51289b901e)
- [TargetPreflightRequest](#s-53a4ad89c5)
- [TargetPreflightResponse](#s-e0e566c7c9)
- [TargetProductionAuthority](#s-1c35901519)
- [TargetProgress](#s-d3a358b363)
- [TargetSemanticConformance](#s-b21311b603)
- [TransformPlan](#s-db60c65572)
- [WorkIdentity](#s-cf67f00f31)
- [WorkflowPlan](#s-3c14667828)

### <a id="s-0993620c2a"></a>definition `AcceptedTargetJob`

- <a id="s-d13ca152d4"></a>`type`: `"object"`
- <a id="s-00a5a03173"></a>`additionalProperties`: `false`
- <a id="s-730cd09c9d"></a>`description`: `"Durable, non-secret identity of one accepted target job request."`
- <a id="s-d3867e02ae"></a>`required`: `["declaration","request_sha256"]`
- <a id="s-2a443163ca"></a>`title`: `"AcceptedTargetJob"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4afee9175b"></a>`declaration` | yes | [TargetJobDeclaration](#s-cd86ac44a9) |  |
| <a id="s-af9b296ddb"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |

### <a id="s-64c14db0d1"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-2c68b8e9f2"></a>`type`: `"object"`
- <a id="s-09d7ae0d22"></a>`description`: `"Small identity for one sealed claim-scoped relational disposition set."`
- <a id="s-59abe5326f"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`
- <a id="s-cf2ee2716c"></a>`title`: `"ArtifactDispositionSetIdentity"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-13146aa90c"></a>`disposition_count` | yes | type="integer"; title="Disposition Count" |  |
| <a id="s-4a105a01bc"></a>`output_artifact_count` | yes | type="integer"; title="Output Artifact Count" |  |
| <a id="s-cbc15a51c0"></a>`output_edge_count` | yes | type="integer"; title="Output Edge Count" |  |
| <a id="s-b4c26ea362"></a>`sha256` | yes | type="string"; title="Sha256" |  |

### <a id="s-4348e1d25c"></a>definition `ArtifactSelectionRef`

- <a id="s-33618c09b6"></a>`type`: `"object"`
- <a id="s-2c96992d7f"></a>`additionalProperties`: `false`
- <a id="s-51046aba85"></a>`description`: `"Closed reference to a separately retained selection document."`
- <a id="s-70230968f7"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`
- <a id="s-82e75bd03f"></a>`title`: `"ArtifactSelectionRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-692be7a667"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-f99ecb544f"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Selection Sha256" |  |
| <a id="s-ffbd1f2350"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

### <a id="s-9650f4fcc0"></a>definition `ArtifactSubject`

- <a id="s-425fc6ddf4"></a>`type`: `"object"`
- <a id="s-9581509b0d"></a>`additionalProperties`: `false`
- <a id="s-673f9ceb41"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-1510e4b6b5"></a>`title`: `"ArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0b7b61ad78"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-bb492425d6"></a>`collection` | yes | [CollectionRootRef](#s-3b0dd8e93b) |  |
| <a id="s-b111813df7"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-e41ea6b19f"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-d319e35eb4"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-e3be6916ed"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-12d6b41438"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-5d02bd6d33"></a>definition `BranchWorkBinding`

- <a id="s-2471e42bdb"></a>`type`: `"object"`
- <a id="s-5f2e7d9ef7"></a>`additionalProperties`: `false`
- <a id="s-aaa203ba27"></a>`description`: `"Stable parent/branch lineage for one ordinary child work identity."`
- <a id="s-dff3a442d7"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`
- <a id="s-e407cbc7f8"></a>`title`: `"BranchWorkBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6bd334a851"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-fa2ae3882e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-4b84162470"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Decision Sha256" |  |
| <a id="s-319062191a"></a>`kind` | no | type="string"; const="branch"; default="branch"; title="Kind" |  |
| <a id="s-a0e4f9b315"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

### <a id="s-40a59539e2"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-d7ccf76e4d"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-83db1174b0"></a>2 | not=(const="0") |

### <a id="s-3b0dd8e93b"></a>definition `CollectionRootRef`

- <a id="s-48e53f286f"></a>`type`: `"object"`
- <a id="s-e54a911e82"></a>`additionalProperties`: `false`
- <a id="s-507dc5dbde"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-3c812099bf"></a>`title`: `"CollectionRootRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d73966d812"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-e7379e3f4f"></a>`collection_id` | yes | [CollectionId](#s-40a59539e2) |  |
| <a id="s-35151e0ed6"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-eb0ebf9844"></a>definition `ContentObservationEvidence`

- <a id="s-e100d1c94f"></a>`type`: `"object"`
- <a id="s-ed030643db"></a>`additionalProperties`: `false`
- <a id="s-c2b0c50481"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-41736d72fd"></a>`required`: `["request","result"]`
- <a id="s-d37ac90068"></a>`title`: `"ContentObservationEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6f732b143"></a>`request` | yes | [ContentObservationRequest](#s-d39e50e273) |  |
| <a id="s-a2058e660a"></a>`result` | yes | [ContentObservationResult](#s-860cf42eef) |  |

### <a id="s-c812e9c1db"></a>definition `ContentObservationFailure`

- <a id="s-2f207a75e8"></a>`type`: `"object"`
- <a id="s-5077a7bccb"></a>`additionalProperties`: `false`
- <a id="s-786cbcb5d8"></a>`required`: `["code","message","retryable"]`
- <a id="s-728ac13f91"></a>`title`: `"ContentObservationFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee0fa33ad1"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-dc4417d8d0"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-dae1bf2430"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-47d3ea9222"></a>definition `ContentObservationInapplicable`

- <a id="s-f001e6f36f"></a>`type`: `"object"`
- <a id="s-ce47ac4f00"></a>`additionalProperties`: `false`
- <a id="s-0173aac952"></a>`required`: `["code","message"]`
- <a id="s-6dcc062224"></a>`title`: `"ContentObservationInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df66fcaa4a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-53c580944b"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-d39e50e273"></a>definition `ContentObservationRequest`

- <a id="s-34b2fc173d"></a>`type`: `"object"`
- <a id="s-a784bd474e"></a>`additionalProperties`: `false`
- <a id="s-a17fa1996d"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-d54023abaa"></a>`title`: `"ContentObservationRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-813403548d"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-a6e6f6a7be"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-3ebae14289"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-ac648465f0"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-edfe063c42"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-c8d94a41d9"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-12db35356b"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Options" |  |
| <a id="s-cb42053871"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-22d9fed609"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-c6ce7a46ce"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-9650f4fcc0)); minItems=1; title="Subjects" |  |
| <a id="s-5dda00f402"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-841fbcf26e"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-860cf42eef"></a>definition `ContentObservationResult`

- <a id="s-65f856318e"></a>`type`: `"object"`
- <a id="s-ea2cb1e1e4"></a>`additionalProperties`: `false`
- <a id="s-ac7da6283f"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-09c5cc6206"></a>`title`: `"ContentObservationResult"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eb9154317a"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Execution Evidence" |  |
| <a id="s-0ce0edfe66"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-40b30ec575))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-590defbb6a"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-5dd81cb02e)); (type="null")]; default=null |  |
| <a id="s-da288cdefd"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-17390dccb4"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-c812e9c1db)); (type="null")]; default=null |  |
| <a id="s-8ddb6d2186"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-05a563d55f"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-47d3ea9222)); (type="null")]; default=null |  |
| <a id="s-a28e09dab8"></a>`observer` | yes | [ObserverImplementation](#s-df8b23cd98) |  |
| <a id="s-9fb7815cc0"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-74353b6293"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-65f391e6ed"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-188a200af8"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-18efbf1404"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-a7b19b12ae"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-9650f4fcc0)); minItems=1; title="Subjects" |  |

### <a id="s-b707894e8c"></a>definition `ControllerEvidence`

- <a id="s-7faf3675b1"></a>`type`: `"object"`
- <a id="s-b3010f6256"></a>`additionalProperties`: `false`
- <a id="s-bc68917582"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`
- <a id="s-b049cc3ef6"></a>`title`: `"ControllerEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bd2dec1b4b"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Controller Evidence Sha256" |  |
| <a id="s-719a9ae4ab"></a>`execution_envelope` | yes | [ExecutionEnvelope](#s-3c944b74fe) |  |
| <a id="s-b887d63886"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1"; title="Format" |  |

### <a id="s-7c36dc1beb"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-2b63659e11"></a>`type`: `"string"`
- <a id="s-66fe9829ef"></a>`enum`: `["encrypted-at-rest","memory-backed"]`
- <a id="s-c13d59f09e"></a>`description`: `"Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy."`

### <a id="s-ef86be7809"></a>definition `EffectPlan`

- <a id="s-f764676ba4"></a>`type`: `"object"`
- <a id="s-b5c3843eea"></a>`additionalProperties`: `false`
- <a id="s-d001fac781"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`
- <a id="s-826861545d"></a>`title`: `"EffectPlan"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b5d226888e"></a>`inputs` | yes | [TargetInputAuthority](#s-d47194a8ed) |  |
| <a id="s-9ca967882c"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Intent" |  |
| <a id="s-5b72f76c58"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-7de1128789"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-40aa47ce6d"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-80f86a6953"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-bfc362d369"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1"; title="Protocol" |  |
| <a id="s-6c8b524415"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-ffc298b86a"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-43205d72a8"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Target Options" |  |

### <a id="s-06c04695a5"></a>definition `EvaluationBinding`

- <a id="s-4db45e4719"></a>`type`: `"object"`
- <a id="s-67179bbce6"></a>`additionalProperties`: `false`
- <a id="s-5eee28e5fc"></a>`description`: `"Immutable membership of one work item in a trial/evaluation matrix."`
- <a id="s-53229b9220"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`
- <a id="s-f9df081fcb"></a>`title`: `"EvaluationBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-431d09d7ae"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Evaluation Id" |  |
| <a id="s-17b04f0a6c"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Matrix Sha256" |  |
| <a id="s-c87e02bb0d"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Parameters" |  |
| <a id="s-0df445b346"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Variant Id" |  |

### <a id="s-3c944b74fe"></a>definition `ExecutionEnvelope`

- <a id="s-dd4c3e46e8"></a>`type`: `"object"`
- <a id="s-4d819c40ed"></a>`additionalProperties`: `false`
- <a id="s-64dee2cc1d"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`
- <a id="s-eb5dca153d"></a>`title`: `"ExecutionEnvelope"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d75b747567"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-7aa4348aed"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Envelope Sha256" |  |
| <a id="s-cf20339c64"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-9318c82a7a"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1"; title="Format" |  |
| <a id="s-9157e18efe"></a>`target_plan` | yes | [TargetPlanBinding](#s-51289b901e) |  |
| <a id="s-7a9266250c"></a>`workflow_plan` | yes | [WorkflowPlan](#s-3c14667828) |  |

### <a id="s-91999efbe2"></a>definition `ExternalEffectReceipt`

- <a id="s-bbf720b525"></a>`type`: `"object"`
- <a id="s-0aa2d8c7e2"></a>`additionalProperties`: `false`
- <a id="s-a42ff90ab1"></a>`required`: `["job_id","request_sha256","target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`
- <a id="s-9da40c3127"></a>`title`: `"ExternalEffectReceipt"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db819eede7"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-b21fbd949b"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1"; title="Format" |  |
| <a id="s-83346264ee"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-da963ca073"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-c03b774088"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-e8b63a7207"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Receipt Sha256" |  |
| <a id="s-f5b5db1c49"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-9b803bf4d5"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Result"; x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-63e5c65774"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |

### <a id="s-ae1b42ecf6"></a>definition `InputArtifactContract`

- <a id="s-100b5cd056"></a>`type`: `"object"`
- <a id="s-d485a0f5a6"></a>`additionalProperties`: `false`
- <a id="s-c11779bd80"></a>`required`: `["role"]`
- <a id="s-7bcd3a78de"></a>`title`: `"InputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-17ca92afbc"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null; title="Allowed Dispositions" |  |
| <a id="s-6fd8493bf9"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-60f958249d"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-e27f5a35ee"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-13aa1fbe2e"></a>definition `JoinWorkBinding`

- <a id="s-321acf7747"></a>`type`: `"object"`
- <a id="s-2bcab329be"></a>`additionalProperties`: `false`
- <a id="s-9f1606ca2d"></a>`description`: `"Stable branch-set lineage for one ordinary join work identity."`
- <a id="s-02ab429f59"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`
- <a id="s-2f6c10070b"></a>`title`: `"JoinWorkBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3fd72a2def"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Branch Set Sha256" |  |
| <a id="s-2a1dea6590"></a>`kind` | no | type="string"; const="join"; default="join"; title="Kind" |  |
| <a id="s-5084ea7e19"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-b0603b6193)); minItems=2; title="Members" |  |
| <a id="s-48e0669585"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

### <a id="s-b0603b6193"></a>definition `JoinWorkMemberBinding`

- <a id="s-4e1870b2e0"></a>`type`: `"object"`
- <a id="s-cff315a1d7"></a>`additionalProperties`: `false`
- <a id="s-3e6d5a0da9"></a>`description`: `"Exact successful branch result used to derive one join work identity."`
- <a id="s-d2062aaeb6"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`
- <a id="s-c8a9f59be6"></a>`title`: `"JoinWorkMemberBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88bc1ecb63"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-4986f869c1"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-a363cba30d"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Producer Settlement Sha256" |  |
| <a id="s-8165c6e0af"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |

### <a id="s-5dd81cb02e"></a>definition `JsonSchemaValidationProfile`

- <a id="s-d72c319f59"></a>`type`: `"object"`
- <a id="s-f382aa5614"></a>`additionalProperties`: `false`
- <a id="s-4088420ba4"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-b4e84f8c31"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-afd67f1fbc"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-770c55c896"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-0fdd302c02"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-5456747373"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-a6285f089f"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Schema" |  |

### <a id="s-40b30ec575"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-df8b23cd98"></a>definition `ObserverImplementation`

- <a id="s-36c7a557db"></a>`type`: `"object"`
- <a id="s-afc2eab04a"></a>`additionalProperties`: `false`
- <a id="s-aaf63726dd"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-a983bf6ec9"></a>`title`: `"ObserverImplementation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dae02c3529"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-75d1d0df0d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-34d793a4a1"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-c65c6661a9"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-cbd5c59a79"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

### <a id="s-0d00de964e"></a>definition `OperationContract`

- <a id="s-6009e3f6c4"></a>`type`: `"object"`
- <a id="s-ff1c84f906"></a>`additionalProperties`: `false`
- <a id="s-d4eeefb8d5"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`
- <a id="s-5a128582f3"></a>`title`: `"OperationContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db12b8f130"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-c27f24027e"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-5dd81cb02e)); (type="null")]; default=null |  |
| <a id="s-b8f718ea39"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-864b2f62ff"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-ae1b42ecf6)); minItems=1; title="Inputs" |  |
| <a id="s-4cdcd181ad"></a>`intent_schema` | yes | [JsonSchemaValidationProfile](#s-5dd81cb02e) |  |
| <a id="s-825b6cb6a3"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-ee0cfd3dd1) |  |
| <a id="s-9fda8d7f17"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-d338c851cc)); title="Outputs" |  |
| <a id="s-0fae452d35"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-5d3b745cad"></a>`source_retirement_permitted` | no | type="boolean"; default=false; title="Source Retirement Permitted" |  |

### <a id="s-46180fa7c3"></a>definition `OperationRef`

- <a id="s-61776bf279"></a>`type`: `"object"`
- <a id="s-5dba25e7d8"></a>`additionalProperties`: `false`
- <a id="s-ad71c4a269"></a>`required`: `["id","sha256"]`
- <a id="s-d0aee7c9ac"></a>`title`: `"OperationRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-172d686e84"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-6debe79b3a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-d338c851cc"></a>definition `OutputArtifactContract`

- <a id="s-ba55f84a17"></a>`type`: `"object"`
- <a id="s-37c36de06d"></a>`additionalProperties`: `false`
- <a id="s-1bb10b5cf1"></a>`required`: `["role","derived_from_roles"]`
- <a id="s-1af9378be1"></a>`title`: `"OutputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c486ecc58a"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Derived From Roles" |  |
| <a id="s-29cbad14ab"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-a34752d5da"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-774044fa3e"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-deff193b8e"></a>definition `OutputArtifactRoleCount`

- <a id="s-1d03876b46"></a>`type`: `"object"`
- <a id="s-5b5bde0755"></a>`additionalProperties`: `false`
- <a id="s-f7f9c837cd"></a>`required`: `["role","count"]`
- <a id="s-9a1fa98104"></a>`title`: `"OutputArtifactRoleCount"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e3fe95a0f"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-eb59bf7e8b"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-fc2841cacc"></a>definition `OutputArtifactSetIdentity`

- <a id="s-075f91c3f7"></a>`type`: `"object"`
- <a id="s-7d88aef65e"></a>`additionalProperties`: `false`
- <a id="s-7d93ba5b69"></a>`description`: `"Small identity for target outputs already registered with Riverhog."`
- <a id="s-61a272c753"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`
- <a id="s-3f4bfbfc90"></a>`title`: `"OutputArtifactSetIdentity"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d9d7a413dd"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-220278cd96"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-deff193b8e)); minItems=1; title="Roles" |  |
| <a id="s-a8be724a77"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-dcca5f1430"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

### <a id="s-7a55ca8250"></a>definition `OutputCollectionRef`

- <a id="s-634b382757"></a>`type`: `"object"`
- <a id="s-51ac424532"></a>`additionalProperties`: `false`
- <a id="s-17f3559d69"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`
- <a id="s-4ba176ff46"></a>`title`: `"OutputCollectionRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69bab181a1"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-8eaa3fe5cd"></a>`collection_id` | yes | [CollectionId](#s-40a59539e2) |  |
| <a id="s-7bc373dcee"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-c536e7cde3"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Derivation Sha256" |  |

### <a id="s-175e50de18"></a>definition `RecipeRef`

- <a id="s-ecb253f3b9"></a>`type`: `"object"`
- <a id="s-98d2341750"></a>`additionalProperties`: `false`
- <a id="s-312e639529"></a>`required`: `["id","revision","sha256"]`
- <a id="s-f2e71bd1d3"></a>`title`: `"RecipeRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e275db8a46"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-21d92cfeb7"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-21ab0ccfd9"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-7bceecb5d9"></a>definition `SemanticIntentConformanceVector`

- <a id="s-2454f2c251"></a>`type`: `"object"`
- <a id="s-7d954d30b9"></a>`additionalProperties`: `false`
- <a id="s-7a120c195d"></a>`required`: `["id","accepted","intent"]`
- <a id="s-3e522061b4"></a>`title`: `"SemanticIntentConformanceVector"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ef882ce0a4"></a>`accepted` | yes | type="boolean"; title="Accepted" |  |
| <a id="s-5156616d33"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-0158a31d7a"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Intent" |  |

### <a id="s-f235361316"></a>definition `SemanticIntentConformanceVectors`

- <a id="s-a0b69f2d15"></a>`type`: `"object"`
- <a id="s-88c2214518"></a>`additionalProperties`: `false`
- <a id="s-3d855607ff"></a>`required`: `["profile_id","vectors"]`
- <a id="s-aecff33113"></a>`title`: `"SemanticIntentConformanceVectors"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06cfa3076b"></a>`format` | no | type="string"; const="stove0-semantic-intent-conformance/v1"; default="stove0-semantic-intent-conformance/v1"; title="Format" |  |
| <a id="s-1f17a1d803"></a>`profile_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Profile Id" |  |
| <a id="s-91e52e6fb9"></a>`vectors` | yes | type="array"; items=([SemanticIntentConformanceVector](#s-7bceecb5d9)); minItems=2; title="Vectors" |  |

### <a id="s-ee0cfd3dd1"></a>definition `SemanticValidationProfile`

- <a id="s-f333b10d51"></a>`type`: `"object"`
- <a id="s-4fb111f07b"></a>`additionalProperties`: `false`
- <a id="s-5ca583c95f"></a>`required`: `["id","rules","profile_sha256"]`
- <a id="s-52d200c2e6"></a>`title`: `"SemanticValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b782684072"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-aee255489c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-b9e00d4926"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-d8c0eb321f"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Rules" |  |

### <a id="s-3af4423fcb"></a>definition `TargetConformanceCoverage`

- <a id="s-afe744ca6f"></a>`type`: `"object"`
- <a id="s-f014462efd"></a>`additionalProperties`: `false`
- <a id="s-ff9715ab45"></a>`required`: `["advertised","exercised","complete"]`
- <a id="s-f63e60c002"></a>`title`: `"TargetConformanceCoverage"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6f0cae1f5f"></a>`advertised` | yes | type="integer"; minimum=0; title="Advertised" |  |
| <a id="s-3f42e5321a"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-9e32bccbb8"></a>`exercised` | yes | type="integer"; minimum=0; title="Exercised" |  |

### <a id="s-c5f049f824"></a>definition `TargetDescriptor`

- <a id="s-061d2681c8"></a>`type`: `"object"`
- <a id="s-49465b7d99"></a>`additionalProperties`: `false`
- <a id="s-252bdc217f"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","operations","descriptor_sha256"]`
- <a id="s-6885eec81a"></a>`title`: `"TargetDescriptor"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3a02edaf53"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-205ed5b733"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$"; title="Image Id" |  |
| <a id="s-d34dda900b"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-d7907d80b6"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-6689dacd70"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-bf993b5ad1)); minItems=1; title="Operations" |  |
| <a id="s-4a431669d1"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-99e61d674f"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-32470def49"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1"; title="Transport" |  |

### <a id="s-633d457279"></a>definition `TargetExecutionEvidence`

- <a id="s-99a8aa4cb3"></a>`type`: `"object"`
- <a id="s-1f4efc8beb"></a>`additionalProperties`: `false`
- <a id="s-0af54bc7ef"></a>`required`: `["target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`
- <a id="s-1c8460f241"></a>`title`: `"TargetExecutionEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2cc5345db5"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-6262284788"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-1e30ff802c"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-6112224187"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Runtime" |  |
| <a id="s-6259a60776"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |

### <a id="s-c0bd76e74f"></a>definition `TargetFailure`

- <a id="s-68c6687bab"></a>`type`: `"object"`
- <a id="s-63075d8298"></a>`additionalProperties`: `false`
- <a id="s-8ca1b1c0a1"></a>`required`: `["code","message","retryable"]`
- <a id="s-065dde32e9"></a>`title`: `"TargetFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca07a92988"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-07583e604a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-7a6f3c0aff"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-d1d84486e0"></a>definition `TargetInapplicable`

- <a id="s-c32116a5b4"></a>`type`: `"object"`
- <a id="s-c0000b80b6"></a>`additionalProperties`: `false`
- <a id="s-393edaaacc"></a>`required`: `["code","message"]`
- <a id="s-bd0ea89469"></a>`title`: `"TargetInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6fe16dc91c"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-d9508e393e"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-d47194a8ed"></a>definition `TargetInputAuthority`

- <a id="s-2ffce7488d"></a>`type`: `"object"`
- <a id="s-2ff668c5be"></a>`additionalProperties`: `false`
- <a id="s-05c494d410"></a>`description`: `"Small exact input authority retained by Stove0 and traversed in bounded pages."`
- <a id="s-c527b96f4b"></a>`required`: `["selection","roles"]`
- <a id="s-ebcd86eac8"></a>`title`: `"TargetInputAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea2d4c93fb"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-a279c59e72)); minItems=1; title="Roles" |  |
| <a id="s-6c6de32710"></a>`selection` | yes | [ArtifactSelectionRef](#s-4348e1d25c) |  |

### <a id="s-a279c59e72"></a>definition `TargetInputRoleCount`

- <a id="s-b7d416b348"></a>`type`: `"object"`
- <a id="s-de4acd746a"></a>`additionalProperties`: `false`
- <a id="s-e272769a6f"></a>`required`: `["role","count"]`
- <a id="s-b87514f24e"></a>`title`: `"TargetInputRoleCount"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-209e85a56d"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-f386031155"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-cd86ac44a9"></a>definition `TargetJobDeclaration`

- <a id="s-221470735b"></a>`type`: `"object"`
- <a id="s-d9ff4ea7a3"></a>`additionalProperties`: `false`
- <a id="s-59b95c727b"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","declared_workspace_protection"]`
- <a id="s-6ed85100a6"></a>`title`: `"TargetJobDeclaration"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d518182a9c"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-b457950183"></a>`controller_evidence` | yes | [ControllerEvidence](#s-b707894e8c) |  |
| <a id="s-f46eff8d33"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-7c36dc1beb) |  |
| <a id="s-ed1ee3dac8"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-09a9d64cdc"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-10ef2039bb"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-db60c65572)); ([EffectPlan](#s-ef86be7809))]; title="Plan" |  |

### <a id="s-ef307f8c14"></a>definition `TargetJobStatus`

- <a id="s-9c4ba1ae36"></a>`type`: `"object"`
- <a id="s-f83ef22c0c"></a>`additionalProperties`: `false`
- <a id="s-37960b3e21"></a>`required`: `["job_id","state","attempt","request_sha256","plan_sha256","progress"]`
- <a id="s-bf9ac61d0f"></a>`title`: `"TargetJobStatus"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b57a7917be"></a>`attempt` | yes | type="integer"; minimum=1; title="Attempt" |  |
| <a id="s-fa08fdb7e1"></a>`derivation` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Derivation" |  |
| <a id="s-52f5246fd5"></a>`effect_receipt` | no | anyOf=[([ExternalEffectReceipt](#s-91999efbe2)); (type="null")]; default=null |  |
| <a id="s-efc92afe8a"></a>`execution_evidence` | no | anyOf=[([TargetExecutionEvidence](#s-633d457279)); (type="null")]; default=null |  |
| <a id="s-2b120f5730"></a>`failure` | no | anyOf=[([TargetFailure](#s-c0bd76e74f)); (type="null")]; default=null |  |
| <a id="s-70d3cfa045"></a>`inapplicable` | no | anyOf=[([TargetInapplicable](#s-d1d84486e0)); (type="null")]; default=null |  |
| <a id="s-ef8ef1fd20"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-2d17f042e4"></a>`output_collection` | no | anyOf=[([OutputCollectionRef](#s-7a55ca8250)); (type="null")]; default=null |  |
| <a id="s-6f47f4e26a"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-3dd39b723d"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-1c35901519)); (type="null")]; default=null |  |
| <a id="s-7f3e7a7ac4"></a>`progress` | yes | [TargetProgress](#s-d3a358b363) |  |
| <a id="s-1fb061bafa"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-9070a362f7"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-ce10749702"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"]; title="State" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-8040f235d0"></a>1 | properties={state: (const="failed")} | properties={failure: (type="object")}; required=["failure"] | properties={failure: (type="null")} |

### <a id="s-d56ff4bc3b"></a>definition `TargetOperationConformance`

- <a id="s-ff44c3b672"></a>`type`: `"object"`
- <a id="s-2f4848a8da"></a>`additionalProperties`: `false`
- <a id="s-cbc31c976c"></a>`required`: `["operation_id","operation_contract_sha256","result_kind","options_schema_profile_sha256","semantic_conformance"]`
- <a id="s-681049eccd"></a>`title`: `"TargetOperationConformance"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a1c96a1ec7"></a>`intent_semantics_conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Intent Semantics Conformance Vectors Sha256" |  |
| <a id="s-eb27955d1d"></a>`intent_semantics_id` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Intent Semantics Id" |  |
| <a id="s-d6b1f77509"></a>`intent_semantics_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Intent Semantics Sha256" |  |
| <a id="s-3bf21a25eb"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-1a08274365"></a>`operation_id` | yes | type="string"; title="Operation Id" |  |
| <a id="s-fc4735320f"></a>`options_schema_profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Options Schema Profile Sha256" |  |
| <a id="s-90a6acab7d"></a>`result_kind` | yes | type="string"; enum=["collection","external-effect"]; title="Result Kind" |  |
| <a id="s-8588a67811"></a>`semantic_conformance` | yes | type="string"; enum=["not-exercised","schema-only","exercised"]; title="Semantic Conformance" |  |

### <a id="s-fd16c1183e"></a>definition `TargetOperationConformanceEvidence`

- <a id="s-716c741873"></a>`type`: `"object"`
- <a id="s-f7a662b90e"></a>`additionalProperties`: `false`
- <a id="s-06403ad186"></a>`required`: `["operation_id","operation","semantic_conformance","preflight_request","preflight","accepted_job","submission","job_status"]`
- <a id="s-e85a4f32ae"></a>`title`: `"TargetOperationConformanceEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a9aef8b72"></a>`accepted_job` | yes | [AcceptedTargetJob](#s-0993620c2a) |  |
| <a id="s-8658777d76"></a>`job_status` | yes | [TargetJobStatus](#s-ef307f8c14) |  |
| <a id="s-b3832d9ef4"></a>`operation` | yes | [OperationContract](#s-0d00de964e) |  |
| <a id="s-1c556f9eed"></a>`operation_id` | yes | type="string"; title="Operation Id" |  |
| <a id="s-7743e20613"></a>`preflight` | yes | [TargetPreflightResponse](#s-e0e566c7c9) |  |
| <a id="s-3f071241aa"></a>`preflight_request` | yes | [TargetPreflightRequest](#s-53a4ad89c5) |  |
| <a id="s-34c2eae861"></a>`semantic_conformance` | yes | [TargetSemanticConformance](#s-b21311b603) |  |
| <a id="s-d7d5955b55"></a>`submission` | yes | [TargetJobStatus](#s-ef307f8c14) |  |

### <a id="s-bf993b5ad1"></a>definition `TargetOperationSupport`

- <a id="s-0d9c8360f1"></a>`type`: `"object"`
- <a id="s-7fe50412d5"></a>`additionalProperties`: `false`
- <a id="s-915af6f5bb"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`
- <a id="s-1c7f34724d"></a>`title`: `"TargetOperationSupport"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-933c5a1e37"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-ec006056a2"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-a890454c27"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-5dd81cb02e) |  |
| <a id="s-17bd19a14f"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |

### <a id="s-51289b901e"></a>definition `TargetPlanBinding`

- <a id="s-1d8285e3d2"></a>`type`: `"object"`
- <a id="s-fea44286a7"></a>`additionalProperties`: `false`
- <a id="s-9d768cb4ff"></a>`description`: `"Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope."`
- <a id="s-967c893a9d"></a>`required`: `["protocol","target_implementation_id","target_descriptor_sha256","operation_contract_sha256","plan","plan_sha256"]`
- <a id="s-bcbbac114b"></a>`title`: `"TargetPlanBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-891e764597"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-483659871f"></a>`plan` | yes | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Plan" |  |
| <a id="s-41e0eec4b4"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-1b132b92ba"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Protocol" |  |
| <a id="s-d39cf64354"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-576498355a"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |

### <a id="s-53a4ad89c5"></a>definition `TargetPreflightRequest`

- <a id="s-d1843ea690"></a>`type`: `"object"`
- <a id="s-287789370a"></a>`additionalProperties`: `false`
- <a id="s-543cb7d37a"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent"]`
- <a id="s-9cb8444082"></a>`title`: `"TargetPreflightRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a1d8b1e8e7"></a>`inputs` | yes | [TargetInputAuthority](#s-d47194a8ed) |  |
| <a id="s-1ec262eca2"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Intent" |  |
| <a id="s-1c0ca85fa0"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-eb0ebf9844)); title="Observations" |  |
| <a id="s-58b6a5935b"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-4d92a74499"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-af5e266515"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-7ca71e93a2"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Target Options" |  |

### <a id="s-e0e566c7c9"></a>definition `TargetPreflightResponse`

- <a id="s-f38e303c8c"></a>`type`: `"object"`
- <a id="s-340415011a"></a>`additionalProperties`: `false`
- <a id="s-b654b1db09"></a>`required`: `["descriptor","plan"]`
- <a id="s-b87d448d36"></a>`title`: `"TargetPreflightResponse"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c57c60684"></a>`descriptor` | yes | [TargetDescriptor](#s-c5f049f824) |  |
| <a id="s-9bdc263ee9"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-db60c65572)); ([EffectPlan](#s-ef86be7809))]; title="Plan" |  |

### <a id="s-1c35901519"></a>definition `TargetProductionAuthority`

- <a id="s-7f49f93983"></a>`type`: `"object"`
- <a id="s-144295fd94"></a>`additionalProperties`: `false`
- <a id="s-17d13065b7"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`
- <a id="s-0a7c206837"></a>`title`: `"TargetProductionAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bd51b1793"></a>`disposition_count` | yes | type="integer"; minimum=1; title="Disposition Count" |  |
| <a id="s-6cbdb7e5a7"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Disposition Sha256" |  |
| <a id="s-da2988aec9"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1"; title="Format" |  |
| <a id="s-173d7b1276"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-70ebf0c773"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-fc2841cacc) |  |
| <a id="s-fe8c9ec074"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-0e3f79af37"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Production Sha256" |  |
| <a id="s-105185c3a5"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-64c14db0d1) |  |
| <a id="s-3a5e66d5be"></a>`source_edge_count` | yes | type="integer"; minimum=1; title="Source Edge Count" |  |
| <a id="s-54785aae65"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Source Edge Sha256" |  |

### <a id="s-d3a358b363"></a>definition `TargetProgress`

- <a id="s-96aa59a393"></a>`type`: `"object"`
- <a id="s-7ec9361818"></a>`additionalProperties`: `false`
- <a id="s-320347b349"></a>`required`: `["phase","completed"]`
- <a id="s-1dc10e6f9f"></a>`title`: `"TargetProgress"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0fa17c3036"></a>`completed` | yes | type="integer"; minimum=0; title="Completed" |  |
| <a id="s-3a92e3ebef"></a>`phase` | yes | type="string"; maxLength=120; minLength=1; title="Phase" |  |
| <a id="s-7946feb855"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null; title="Total" |  |
| <a id="s-0cafe57af4"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; default=null; title="Unit" |  |

### <a id="s-b21311b603"></a>definition `TargetSemanticConformance`

- <a id="s-dcf8f6ea55"></a>`type`: `"object"`
- <a id="s-262a51f35f"></a>`additionalProperties`: `false`
- <a id="s-38f205ee76"></a>`required`: `["profile_id","profile_sha256","status"]`
- <a id="s-8d8530fa5a"></a>`title`: `"TargetSemanticConformance"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-875d411fa0"></a>`accepted_vector_ids` | no | type="array"; default=[]; items=(type="string"); title="Accepted Vector Ids" |  |
| <a id="s-2e78a5d72f"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-d12542b44e"></a>`profile_id` | yes | type="string"; title="Profile Id" |  |
| <a id="s-143df259a1"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-fa13b84a6b"></a>`rejected_vector_ids` | no | type="array"; default=[]; items=(type="string"); title="Rejected Vector Ids" |  |
| <a id="s-913b59d0c6"></a>`status` | yes | type="string"; enum=["schema-only","exercised"]; title="Status" |  |
| <a id="s-cf3a59226b"></a>`vectors` | no | anyOf=[([SemanticIntentConformanceVectors](#s-f235361316)); (type="null")]; default=null |  |

### <a id="s-db60c65572"></a>definition `TransformPlan`

- <a id="s-835d5416c0"></a>`type`: `"object"`
- <a id="s-0fcb7df76c"></a>`additionalProperties`: `false`
- <a id="s-ad13e87ece"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`
- <a id="s-91be2ed73d"></a>`title`: `"TransformPlan"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59b2da3b0a"></a>`inputs` | yes | [TargetInputAuthority](#s-d47194a8ed) |  |
| <a id="s-4a16960d1f"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Intent" |  |
| <a id="s-be4f54821b"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-53d17c8c75"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-954b1f8c76"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-d9e38e912f"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-eae24a9f72"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-d96810acce"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-d9f1d4ec30"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-64403125d5"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Target Options" |  |

### <a id="s-cf67f00f31"></a>definition `WorkIdentity`

- <a id="s-e72b6d08b7"></a>`type`: `"object"`
- <a id="s-89f774ea05"></a>`additionalProperties`: `false`
- <a id="s-c5d379ec82"></a>`required`: `["recipe","inputs","work_id"]`
- <a id="s-8fc0341c22"></a>`title`: `"WorkIdentity"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fdafcd6296"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Effective Intent" |  |
| <a id="s-52828781f8"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-06c04695a5)); (type="null")]; default=null |  |
| <a id="s-5d45035b0c"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-5d02bd6d33)); ([JoinWorkBinding](#s-13aa1fbe2e))]); (type="null")]; default=null; title="Fork Join" |  |
| <a id="s-7d568f8efd"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1"; title="Format" |  |
| <a id="s-c17fdffda5"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-3b0dd8e93b)); minItems=1; title="Inputs" |  |
| <a id="s-f863629033"></a>`recipe` | yes | [RecipeRef](#s-175e50de18) |  |
| <a id="s-3498a29c05"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-3c14667828"></a>definition `WorkflowPlan`

- <a id="s-657454fe06"></a>`type`: `"object"`
- <a id="s-79b34ebbe8"></a>`additionalProperties`: `false`
- <a id="s-db02dc8da8"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`
- <a id="s-5074620442"></a>`title`: `"WorkflowPlan"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e72d3cb718"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1"; title="Format" |  |
| <a id="s-9aa27a2da7"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-cfc3faa0e2"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-eb0ebf9844)); title="Observations" |  |
| <a id="s-8456e8ce76"></a>`operation` | yes | [OperationRef](#s-46180fa7c3) |  |
| <a id="s-8379dee316"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Output Policy" |  |
| <a id="s-765ca1361e"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-40b30ec575)); title="Requested Target Options" |  |
| <a id="s-f92f3d622e"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-ebefece060"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Retirement Grace Seconds" |  |
| <a id="s-4e517edfac"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Retirement Policy" |  |
| <a id="s-a1f389aea5"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-b1e2a706b7"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Target Registration Id" |  |
| <a id="s-2a1d55f30e"></a>`work` | yes | [WorkIdentity](#s-cf67f00f31) |  |
| <a id="s-aef415faea"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field operation_evidence](#s-cdbf79ac63) | `cardinality · items · operational_policy` | shared above |
| [field operations](#s-b7d24726ce) | `cardinality · items · operational_policy` | shared above |
| [definition ArtifactSubject · field bytes](#s-0b7b61ad78) | `value · schema-value · operational_policy` | shared above |
| [definition ContentObservationRequest · field options](#s-12db35356b) | `cardinality · entries · operational_policy` | shared above |
| [definition ContentObservationRequest · field subjects](#s-c6ce7a46ce) | `cardinality · items · operational_policy` | shared above |
| [definition ContentObservationResult · field execution_evidence](#s-eb9154317a) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-61c09086ce"></a>[definition ContentObservationResult · field facts · object value](#s-0ce0edfe66) | `cardinality · entries · operational_policy` | shared above |
| [definition ContentObservationResult · field subjects](#s-a7b19b12ae) | `cardinality · items · operational_policy` | shared above |
| [definition EffectPlan · field intent](#s-9ca967882c) | `cardinality · entries · operational_policy` | shared above |
| [definition EffectPlan · field observation_result_sha256s](#s-5b72f76c58) | `cardinality · items · operational_policy` | shared above |
| [definition EffectPlan · field target_options](#s-43205d72a8) | `cardinality · entries · operational_policy` | shared above |
| [definition EvaluationBinding · field parameters](#s-c87e02bb0d) | `cardinality · entries · operational_policy` | shared above |
| [definition ExternalEffectReceipt · field result](#s-9b803bf4d5) | `cardinality · entries · operational_policy` | shared above |
| [definition JoinWorkBinding · field members](#s-5084ea7e19) | `cardinality · items · operational_policy` | shared above |
| [definition OperationContract · field inputs](#s-864b2f62ff) | `cardinality · items · operational_policy` | shared above |
| [definition OperationContract · field outputs](#s-9fda8d7f17) | `cardinality · items · operational_policy` | shared above |
| [definition OutputArtifactRoleCount · field count](#s-8e3fe95a0f) | `value · schema-value · operational_policy` | shared above |
| [definition OutputArtifactSetIdentity · field roles](#s-220278cd96) | `cardinality · items · operational_policy` | shared above |
| [definition SemanticIntentConformanceVector · field intent](#s-0158a31d7a) | `cardinality · entries · operational_policy` | shared above |
| [definition SemanticIntentConformanceVectors · field vectors](#s-91e52e6fb9) | `cardinality · items · operational_policy` | shared above |
| [definition TargetDescriptor · field operations](#s-6689dacd70) | `cardinality · items · operational_policy` | shared above |
| [definition TargetExecutionEvidence · field runtime](#s-6112224187) | `cardinality · entries · operational_policy` | shared above |
| [definition TargetInputAuthority · field roles](#s-ea2d4c93fb) | `cardinality · items · operational_policy` | shared above |
| [definition TargetInputRoleCount · field count](#s-209e85a56d) | `value · schema-value · operational_policy` | shared above |
| <a id="s-548bd052fb"></a>[definition TargetJobStatus · field derivation · object value](#s-fa08fdb7e1) | `cardinality · entries · operational_policy` | shared above |
| [definition TargetPlanBinding · field plan](#s-483659871f) | `cardinality · entries · operational_policy` | shared above |
| [definition TargetPreflightRequest · field intent](#s-1ec262eca2) | `cardinality · entries · operational_policy` | shared above |
| [definition TargetPreflightRequest · field observations](#s-1c0ca85fa0) | `cardinality · items · operational_policy` | shared above |
| [definition TargetPreflightRequest · field target_options](#s-7ca71e93a2) | `cardinality · entries · operational_policy` | shared above |
| [definition TargetSemanticConformance · field accepted_vector_ids](#s-875d411fa0) | `cardinality · items · operational_policy` | shared above |
| [definition TargetSemanticConformance · field rejected_vector_ids](#s-fa13b84a6b) | `cardinality · items · operational_policy` | shared above |
| [definition TransformPlan · field intent](#s-4a16960d1f) | `cardinality · entries · operational_policy` | shared above |
| [definition TransformPlan · field observation_result_sha256s](#s-be4f54821b) | `cardinality · items · operational_policy` | shared above |
| [definition TransformPlan · field target_options](#s-64403125d5) | `cardinality · entries · operational_policy` | shared above |
| [definition WorkIdentity · field effective_intent](#s-fdafcd6296) | `cardinality · entries · operational_policy` | shared above |
| [definition WorkIdentity · field inputs](#s-c17fdffda5) | `cardinality · items · operational_policy` | shared above |
| [definition WorkflowPlan · field observations](#s-cfc3faa0e2) | `cardinality · items · operational_policy` | shared above |
| [definition WorkflowPlan · field output_policy](#s-8379dee316) | `cardinality · entries · operational_policy` | shared above |
| [definition WorkflowPlan · field requested_target_options](#s-765ca1361e) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition AcceptedTargetJob · field request_sha256](#s-af9b296ddb) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ArtifactSelectionRef · field selection_sha256](#s-f99ecb544f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-aa209e506a"></a>[definition ArtifactSubject · field media_type · string value](#s-e41ea6b19f) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition ArtifactSubject · field path](#s-d319e35eb4) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition ArtifactSubject · field sha256](#s-12d6b41438) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition BranchWorkBinding · field artifact_selection_sha256](#s-6bd334a851) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition BranchWorkBinding · field decision_sha256](#s-4b84162470) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition BranchWorkBinding · field parent_work_id](#s-a0e4f9b315) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition CollectionRootRef · field archive_root_sha256](#s-d73966d812) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition CollectionRootRef · field content_identity](#s-35151e0ed6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationFailure · field message](#s-dc4417d8d0) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition ContentObservationInapplicable · field message](#s-53c580944b) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition ContentObservationRequest · field maximum_result_bytes](#s-a6e6f6a7be) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [definition ContentObservationRequest · field observer_contract_sha256](#s-ac648465f0) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationRequest · field observer_descriptor_sha256](#s-edfe063c42) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationRequest · field request_id](#s-cb42053871) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationRequest · field timeout_seconds](#s-5dda00f402) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [definition ContentObservationRequest · field work_id](#s-841fbcf26e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f2d559dc11"></a>[definition ContentObservationResult · field facts_sha256 · string value](#s-da288cdefd) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationResult · field observer_contract_sha256](#s-74353b6293) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationResult · field request_id](#s-65f391e6ed) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationResult · field result_sha256](#s-188a200af8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ControllerEvidence · field controller_evidence_sha256](#s-bd2dec1b4b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-e2ed80f398"></a>[definition EffectPlan · field observation_result_sha256s · items](#s-5b72f76c58) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition EffectPlan · field operation_contract_sha256](#s-7de1128789) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition EffectPlan · field plan_sha256](#s-80f86a6953) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition EffectPlan · field target_descriptor_sha256](#s-6c8b524415) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition EvaluationBinding · field evaluation_id](#s-431d09d7ae) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition EvaluationBinding · field matrix_sha256](#s-17b04f0a6c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExecutionEnvelope · field claim_id](#s-d75b747567) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition ExecutionEnvelope · field execution_envelope_sha256](#s-7aa4348aed) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExternalEffectReceipt · field execution_sha256](#s-db819eede7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExternalEffectReceipt · field job_id](#s-83346264ee) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExternalEffectReceipt · field operation_contract_sha256](#s-da963ca073) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExternalEffectReceipt · field plan_sha256](#s-c03b774088) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExternalEffectReceipt · field receipt_sha256](#s-e8b63a7207) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExternalEffectReceipt · field request_sha256](#s-f5b5db1c49) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExternalEffectReceipt · field result](#s-9b803bf4d5) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-external-effect-receipt"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition ExternalEffectReceipt · field target_descriptor_sha256](#s-63e5c65774) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition JoinWorkBinding · field branch_set_sha256](#s-3fd72a2def) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition JoinWorkBinding · field parent_work_id](#s-48e0669585) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition JoinWorkMemberBinding · field artifact_selection_sha256](#s-88bc1ecb63) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-31332e38ef"></a>[definition JoinWorkMemberBinding · field producer_settlement_sha256 · string value](#s-a363cba30d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition JoinWorkMemberBinding · field settlement_sha256](#s-8165c6e0af) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverImplementation · field descriptor_sha256](#s-dae02c3529) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverImplementation · field source_revision](#s-c65c6661a9) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [definition ObserverImplementation · field version](#s-cbd5c59a79) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [definition OperationContract · field contract_sha256](#s-db12b8f130) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition OperationRef · field sha256](#s-6debe79b3a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition OutputArtifactSetIdentity · field sha256](#s-a8be724a77) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition OutputCollectionRef · field archive_root_sha256](#s-69bab181a1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition OutputCollectionRef · field content_identity](#s-7bc373dcee) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition OutputCollectionRef · field derivation_sha256](#s-c536e7cde3) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition RecipeRef · field sha256](#s-21ab0ccfd9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetDescriptor · field descriptor_sha256](#s-3a02edaf53) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetDescriptor · field implementation_version](#s-d7907d80b6) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [definition TargetDescriptor · field source_revision](#s-99e61d674f) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [definition TargetExecutionEvidence · field execution_sha256](#s-2cc5345db5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetExecutionEvidence · field operation_contract_sha256](#s-6262284788) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetExecutionEvidence · field plan_sha256](#s-1e30ff802c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetExecutionEvidence · field target_descriptor_sha256](#s-6259a60776) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetFailure · field message](#s-07583e604a) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition TargetInapplicable · field message](#s-d9508e393e) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition TargetJobDeclaration · field claim_id](#s-d518182a9c) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition TargetJobDeclaration · field job_id](#s-09a9d64cdc) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetJobStatus · field job_id](#s-ef8ef1fd20) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetJobStatus · field plan_sha256](#s-6f47f4e26a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetJobStatus · field request_sha256](#s-9070a362f7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-4298a4626e"></a>[definition TargetOperationConformance · field intent_semantics_conformance_vectors_sha256 · string value](#s-a1c96a1ec7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-730db5906a"></a>[definition TargetOperationConformance · field intent_semantics_sha256 · string value](#s-d6b1f77509) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetOperationConformance · field operation_contract_sha256](#s-3bf21a25eb) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetOperationConformance · field options_schema_profile_sha256](#s-fc4735320f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetOperationSupport · field operation_contract_sha256](#s-933c5a1e37) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetPlanBinding · field operation_contract_sha256](#s-891e764597) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetPlanBinding · field plan_sha256](#s-41e0eec4b4) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetPlanBinding · field target_descriptor_sha256](#s-d39cf64354) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetPreflightRequest · field operation_contract_sha256](#s-58b6a5935b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetProductionAuthority · field disposition_sha256](#s-6cbdb7e5a7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetProductionAuthority · field job_id](#s-173d7b1276) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetProductionAuthority · field plan_sha256](#s-fe8c9ec074) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetProductionAuthority · field production_sha256](#s-0e3f79af37) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetProductionAuthority · field source_edge_sha256](#s-54785aae65) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetProgress · field phase](#s-3a92e3ebef) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| <a id="s-7c59ae0b68"></a>[definition TargetProgress · field unit · string value](#s-0cafe57af4) | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |
| <a id="s-6abdaeda79"></a>[definition TargetSemanticConformance · field conformance_vectors_sha256 · string value](#s-2e78a5d72f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TargetSemanticConformance · field profile_sha256](#s-143df259a1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-b92ace78d4"></a>[definition TransformPlan · field observation_result_sha256s · items](#s-be4f54821b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TransformPlan · field operation_contract_sha256](#s-53d17c8c75) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TransformPlan · field plan_sha256](#s-d9e38e912f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TransformPlan · field target_descriptor_sha256](#s-d96810acce) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition WorkIdentity · field work_id](#s-3498a29c05) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition WorkflowPlan · field target_descriptor_sha256](#s-a1f389aea5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition WorkflowPlan · field workflow_plan_sha256](#s-aef415faea) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8088daece5"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-f18a8edd46"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-2fd4e8911b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73656e8ca918b7a68c780060bf357c88f7743622cb38e33fc720884124274153 -->

```json
{
  "$defs": {
    "AcceptedTargetJob": {
      "additionalProperties": false,
      "description": "Durable, non-secret identity of one accepted target job request.",
      "properties": {
        "declaration": {
          "$ref": "#/$defs/TargetJobDeclaration"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        }
      },
      "required": [
        "declaration",
        "request_sha256"
      ],
      "title": "AcceptedTargetJob",
      "type": "object"
    },
    "ArtifactDispositionSetIdentity": {
      "description": "Small identity for one sealed claim-scoped relational disposition set.",
      "properties": {
        "disposition_count": {
          "title": "Disposition Count",
          "type": "integer"
        },
        "output_artifact_count": {
          "title": "Output Artifact Count",
          "type": "integer"
        },
        "output_edge_count": {
          "title": "Output Edge Count",
          "type": "integer"
        },
        "sha256": {
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "disposition_count",
        "output_edge_count",
        "output_artifact_count",
        "sha256"
      ],
      "title": "ArtifactDispositionSetIdentity",
      "type": "object"
    },
    "ArtifactSelectionRef": {
      "additionalProperties": false,
      "description": "Closed reference to a separately retained selection document.",
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "title": "Artifact Count",
          "type": "integer"
        },
        "selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Selection Sha256",
          "type": "string"
        },
        "total_bytes": {
          "minimum": 0,
          "title": "Total Bytes",
          "type": "integer"
        }
      },
      "required": [
        "selection_sha256",
        "artifact_count",
        "total_bytes"
      ],
      "title": "ArtifactSelectionRef",
      "type": "object"
    },
    "ArtifactSubject": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootRef"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
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
          "default": null,
          "title": "Media Type"
        },
        "path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Path",
          "type": "string"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
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
      "title": "ArtifactSubject",
      "type": "object"
    },
    "BranchWorkBinding": {
      "additionalProperties": false,
      "description": "Stable parent/branch lineage for one ordinary child work identity.",
      "properties": {
        "artifact_selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Artifact Selection Sha256",
          "type": "string"
        },
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Branch Id",
          "type": "string"
        },
        "decision_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Decision Sha256",
          "type": "string"
        },
        "kind": {
          "const": "branch",
          "default": "branch",
          "title": "Kind",
          "type": "string"
        },
        "parent_work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Parent Work Id",
          "type": "string"
        }
      },
      "required": [
        "parent_work_id",
        "branch_id",
        "decision_sha256",
        "artifact_selection_sha256"
      ],
      "title": "BranchWorkBinding",
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
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Content Identity",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity"
      ],
      "title": "CollectionRootRef",
      "type": "object"
    },
    "ContentObservationEvidence": {
      "additionalProperties": false,
      "description": "Complete routing evidence: immutable request plus accepted result.",
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
      "title": "ContentObservationEvidence",
      "type": "object"
    },
    "ContentObservationFailure": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        },
        "retryable": {
          "title": "Retryable",
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "title": "ContentObservationFailure",
      "type": "object"
    },
    "ContentObservationInapplicable": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "ContentObservationInapplicable",
      "type": "object"
    },
    "ContentObservationRequest": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-observation-request/v1",
          "default": "stove0-observation-request/v1",
          "title": "Format",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "observer_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Observer Contract Id",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Contract Sha256",
          "type": "string"
        },
        "observer_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Descriptor Sha256",
          "type": "string"
        },
        "observer_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "title": "Observer Registration Id",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Id",
          "type": "string"
        },
        "retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "title": "Retrieval Policy",
          "type": "string"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        },
        "timeout_seconds": {
          "default": 300,
          "maximum": 86400,
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
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
      "title": "ContentObservationRequest",
      "type": "object"
    },
    "ContentObservationResult": {
      "additionalProperties": false,
      "properties": {
        "execution_evidence": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Execution Evidence",
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
          "default": null,
          "title": "Facts"
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
          "default": null,
          "title": "Facts Sha256"
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
          "title": "Format",
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
          "title": "Observer Contract Id",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Contract Sha256",
          "type": "string"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Id",
          "type": "string"
        },
        "result_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Result Sha256",
          "type": "string"
        },
        "state": {
          "enum": [
            "observed",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
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
      "title": "ContentObservationResult",
      "type": "object"
    },
    "ControllerEvidence": {
      "additionalProperties": false,
      "properties": {
        "controller_evidence_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Controller Evidence Sha256",
          "type": "string"
        },
        "execution_envelope": {
          "$ref": "#/$defs/ExecutionEnvelope"
        },
        "format": {
          "const": "stove0-controller-evidence/v1",
          "default": "stove0-controller-evidence/v1",
          "title": "Format",
          "type": "string"
        }
      },
      "required": [
        "execution_envelope",
        "controller_evidence_sha256"
      ],
      "title": "ControllerEvidence",
      "type": "object"
    },
    "DeclaredWorkspaceProtection": {
      "description": "Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy.",
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
          "title": "Intent",
          "type": "object"
        },
        "observation_result_sha256s": {
          "default": [],
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "title": "Observation Result Sha256S",
          "type": "array"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-effect-target/v1",
          "default": "stove0-effect-target/v1",
          "title": "Protocol",
          "type": "string"
        },
        "target_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Descriptor Sha256",
          "type": "string"
        },
        "target_implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Target Implementation Id",
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent",
        "target_implementation_id",
        "target_descriptor_sha256",
        "plan_sha256"
      ],
      "title": "EffectPlan",
      "type": "object"
    },
    "EvaluationBinding": {
      "additionalProperties": false,
      "description": "Immutable membership of one work item in a trial/evaluation matrix.",
      "properties": {
        "evaluation_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Evaluation Id",
          "type": "string"
        },
        "matrix_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Matrix Sha256",
          "type": "string"
        },
        "parameters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Parameters",
          "type": "object"
        },
        "variant_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Variant Id",
          "type": "string"
        }
      },
      "required": [
        "evaluation_id",
        "matrix_sha256",
        "variant_id"
      ],
      "title": "EvaluationBinding",
      "type": "object"
    },
    "ExecutionEnvelope": {
      "additionalProperties": false,
      "properties": {
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Claim Id",
          "type": "string"
        },
        "execution_envelope_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Execution Envelope Sha256",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "title": "Fence",
          "type": "integer"
        },
        "format": {
          "const": "stove0-execution-envelope/v1",
          "default": "stove0-execution-envelope/v1",
          "title": "Format",
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
      "title": "ExecutionEnvelope",
      "type": "object"
    },
    "ExternalEffectReceipt": {
      "additionalProperties": false,
      "properties": {
        "execution_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Execution Sha256",
          "type": "string"
        },
        "format": {
          "const": "stove0-external-effect-receipt/v1",
          "default": "stove0-external-effect-receipt/v1",
          "title": "Format",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "receipt_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Receipt Sha256",
          "type": "string"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        },
        "result": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Result",
          "type": "object",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-external-effect-receipt"
          }
        },
        "target_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Descriptor Sha256",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "request_sha256",
        "target_descriptor_sha256",
        "operation_contract_sha256",
        "plan_sha256",
        "execution_sha256",
        "result",
        "receipt_sha256"
      ],
      "title": "ExternalEffectReceipt",
      "type": "object"
    },
    "InputArtifactContract": {
      "additionalProperties": false,
      "properties": {
        "allowed_dispositions": {
          "anyOf": [
            {
              "items": {
                "enum": [
                  "transformed",
                  "preserved",
                  "omitted",
                  "rejected"
                ],
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Allowed Dispositions"
        },
        "maximum": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum"
        },
        "minimum": {
          "default": 1,
          "minimum": 0,
          "title": "Minimum",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role"
      ],
      "title": "InputArtifactContract",
      "type": "object"
    },
    "JoinWorkBinding": {
      "additionalProperties": false,
      "description": "Stable branch-set lineage for one ordinary join work identity.",
      "properties": {
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Branch Set Sha256",
          "type": "string"
        },
        "kind": {
          "const": "join",
          "default": "join",
          "title": "Kind",
          "type": "string"
        },
        "members": {
          "items": {
            "$ref": "#/$defs/JoinWorkMemberBinding"
          },
          "minItems": 2,
          "title": "Members",
          "type": "array"
        },
        "parent_work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Parent Work Id",
          "type": "string"
        }
      },
      "required": [
        "parent_work_id",
        "branch_set_sha256",
        "members"
      ],
      "title": "JoinWorkBinding",
      "type": "object"
    },
    "JoinWorkMemberBinding": {
      "additionalProperties": false,
      "description": "Exact successful branch result used to derive one join work identity.",
      "properties": {
        "artifact_selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Artifact Selection Sha256",
          "type": "string"
        },
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Branch Id",
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
          "default": null,
          "title": "Producer Settlement Sha256"
        },
        "settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Settlement Sha256",
          "type": "string"
        }
      },
      "required": [
        "branch_id",
        "settlement_sha256",
        "artifact_selection_sha256"
      ],
      "title": "JoinWorkMemberBinding",
      "type": "object"
    },
    "JsonSchemaValidationProfile": {
      "additionalProperties": false,
      "properties": {
        "dialect": {
          "const": "https://json-schema.org/draft/2020-12/schema",
          "default": "https://json-schema.org/draft/2020-12/schema",
          "title": "Dialect",
          "type": "string"
        },
        "format_policy": {
          "const": "annotation-only",
          "default": "annotation-only",
          "title": "Format Policy",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        }
      },
      "required": [
        "id",
        "profile_sha256",
        "schema"
      ],
      "title": "JsonSchemaValidationProfile",
      "type": "object"
    },
    "JsonValue": {},
    "ObserverImplementation": {
      "additionalProperties": false,
      "properties": {
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-content-observer/v1",
          "default": "stove0-content-observer/v1",
          "title": "Protocol",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
          "type": "string"
        },
        "version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Version",
          "type": "string"
        }
      },
      "required": [
        "id",
        "version",
        "source_revision",
        "descriptor_sha256"
      ],
      "title": "ObserverImplementation",
      "type": "object"
    },
    "OperationContract": {
      "additionalProperties": false,
      "properties": {
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "effect_receipt_schema": {
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
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/InputArtifactContract"
          },
          "minItems": 1,
          "title": "Inputs",
          "type": "array"
        },
        "intent_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "intent_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "outputs": {
          "default": [],
          "items": {
            "$ref": "#/$defs/OutputArtifactContract"
          },
          "title": "Outputs",
          "type": "array"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        },
        "source_retirement_permitted": {
          "default": false,
          "title": "Source Retirement Permitted",
          "type": "boolean"
        }
      },
      "required": [
        "id",
        "intent_schema",
        "intent_semantics",
        "inputs",
        "contract_sha256"
      ],
      "title": "OperationContract",
      "type": "object"
    },
    "OperationRef": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "sha256"
      ],
      "title": "OperationRef",
      "type": "object"
    },
    "OutputArtifactContract": {
      "additionalProperties": false,
      "properties": {
        "derived_from_roles": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Derived From Roles",
          "type": "array"
        },
        "maximum": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum"
        },
        "minimum": {
          "default": 1,
          "minimum": 0,
          "title": "Minimum",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role",
        "derived_from_roles"
      ],
      "title": "OutputArtifactContract",
      "type": "object"
    },
    "OutputArtifactRoleCount": {
      "additionalProperties": false,
      "properties": {
        "count": {
          "minimum": 1,
          "title": "Count",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role",
        "count"
      ],
      "title": "OutputArtifactRoleCount",
      "type": "object"
    },
    "OutputArtifactSetIdentity": {
      "additionalProperties": false,
      "description": "Small identity for target outputs already registered with Riverhog.",
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "title": "Artifact Count",
          "type": "integer"
        },
        "roles": {
          "items": {
            "$ref": "#/$defs/OutputArtifactRoleCount"
          },
          "minItems": 1,
          "title": "Roles",
          "type": "array"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        },
        "total_bytes": {
          "minimum": 0,
          "title": "Total Bytes",
          "type": "integer"
        }
      },
      "required": [
        "artifact_count",
        "total_bytes",
        "roles",
        "sha256"
      ],
      "title": "OutputArtifactSetIdentity",
      "type": "object"
    },
    "OutputCollectionRef": {
      "additionalProperties": false,
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Content Identity",
          "type": "string"
        },
        "derivation_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Derivation Sha256",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity",
        "derivation_sha256"
      ],
      "title": "OutputCollectionRef",
      "type": "object"
    },
    "RecipeRef": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
          "type": "integer"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "sha256"
      ],
      "title": "RecipeRef",
      "type": "object"
    },
    "SemanticIntentConformanceVector": {
      "additionalProperties": false,
      "properties": {
        "accepted": {
          "title": "Accepted",
          "type": "boolean"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        }
      },
      "required": [
        "id",
        "accepted",
        "intent"
      ],
      "title": "SemanticIntentConformanceVector",
      "type": "object"
    },
    "SemanticIntentConformanceVectors": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-semantic-intent-conformance/v1",
          "default": "stove0-semantic-intent-conformance/v1",
          "title": "Format",
          "type": "string"
        },
        "profile_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Profile Id",
          "type": "string"
        },
        "vectors": {
          "items": {
            "$ref": "#/$defs/SemanticIntentConformanceVector"
          },
          "minItems": 2,
          "title": "Vectors",
          "type": "array"
        }
      },
      "required": [
        "profile_id",
        "vectors"
      ],
      "title": "SemanticIntentConformanceVectors",
      "type": "object"
    },
    "SemanticValidationProfile": {
      "additionalProperties": false,
      "properties": {
        "conformance_vectors_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Conformance Vectors Sha256"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "rules": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Rules",
          "type": "array"
        }
      },
      "required": [
        "id",
        "rules",
        "profile_sha256"
      ],
      "title": "SemanticValidationProfile",
      "type": "object"
    },
    "TargetConformanceCoverage": {
      "additionalProperties": false,
      "properties": {
        "advertised": {
          "minimum": 0,
          "title": "Advertised",
          "type": "integer"
        },
        "complete": {
          "title": "Complete",
          "type": "boolean"
        },
        "exercised": {
          "minimum": 0,
          "title": "Exercised",
          "type": "integer"
        }
      },
      "required": [
        "advertised",
        "exercised",
        "complete"
      ],
      "title": "TargetConformanceCoverage",
      "type": "object"
    },
    "TargetDescriptor": {
      "additionalProperties": false,
      "properties": {
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
          "title": "Image Id",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Implementation Id",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Implementation Version",
          "type": "string"
        },
        "operations": {
          "items": {
            "$ref": "#/$defs/TargetOperationSupport"
          },
          "minItems": 1,
          "title": "Operations",
          "type": "array"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "title": "Protocol",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "title": "Transport",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_id",
        "operations",
        "descriptor_sha256"
      ],
      "title": "TargetDescriptor",
      "type": "object"
    },
    "TargetExecutionEvidence": {
      "additionalProperties": false,
      "properties": {
        "execution_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Execution Sha256",
          "type": "string"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "runtime": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Runtime",
          "type": "object"
        },
        "target_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Descriptor Sha256",
          "type": "string"
        }
      },
      "required": [
        "target_descriptor_sha256",
        "operation_contract_sha256",
        "plan_sha256",
        "execution_sha256"
      ],
      "title": "TargetExecutionEvidence",
      "type": "object"
    },
    "TargetFailure": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        },
        "retryable": {
          "title": "Retryable",
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "title": "TargetFailure",
      "type": "object"
    },
    "TargetInapplicable": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "TargetInapplicable",
      "type": "object"
    },
    "TargetInputAuthority": {
      "additionalProperties": false,
      "description": "Small exact input authority retained by Stove0 and traversed in bounded pages.",
      "properties": {
        "roles": {
          "items": {
            "$ref": "#/$defs/TargetInputRoleCount"
          },
          "minItems": 1,
          "title": "Roles",
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
      "title": "TargetInputAuthority",
      "type": "object"
    },
    "TargetInputRoleCount": {
      "additionalProperties": false,
      "properties": {
        "count": {
          "minimum": 1,
          "title": "Count",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role",
        "count"
      ],
      "title": "TargetInputRoleCount",
      "type": "object"
    },
    "TargetJobDeclaration": {
      "additionalProperties": false,
      "properties": {
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Claim Id",
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
          "title": "Fence",
          "type": "integer"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
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
          ],
          "title": "Plan"
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
      "title": "TargetJobDeclaration",
      "type": "object"
    },
    "TargetJobStatus": {
      "additionalProperties": false,
      "allOf": [
        {
          "else": {
            "properties": {
              "failure": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "const": "failed"
              }
            }
          },
          "then": {
            "properties": {
              "failure": {
                "type": "object"
              }
            },
            "required": [
              "failure"
            ]
          }
        }
      ],
      "properties": {
        "attempt": {
          "minimum": 1,
          "title": "Attempt",
          "type": "integer"
        },
        "derivation": {
          "anyOf": [
            {
              "additionalProperties": true,
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Derivation"
        },
        "effect_receipt": {
          "anyOf": [
            {
              "$ref": "#/$defs/ExternalEffectReceipt"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "execution_evidence": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetExecutionEvidence"
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
              "$ref": "#/$defs/TargetFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetInapplicable"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "output_collection": {
          "anyOf": [
            {
              "$ref": "#/$defs/OutputCollectionRef"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "production": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetProductionAuthority"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "progress": {
          "$ref": "#/$defs/TargetProgress"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "title": "Protocol",
          "type": "string"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        },
        "state": {
          "enum": [
            "queued",
            "running",
            "canceling",
            "interrupted",
            "inapplicable",
            "succeeded",
            "failed",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "state",
        "attempt",
        "request_sha256",
        "plan_sha256",
        "progress"
      ],
      "title": "TargetJobStatus",
      "type": "object"
    },
    "TargetOperationConformance": {
      "additionalProperties": false,
      "properties": {
        "intent_semantics_conformance_vectors_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Intent Semantics Conformance Vectors Sha256"
        },
        "intent_semantics_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Intent Semantics Id"
        },
        "intent_semantics_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Intent Semantics Sha256"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "title": "Operation Id",
          "type": "string"
        },
        "options_schema_profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Options Schema Profile Sha256",
          "type": "string"
        },
        "result_kind": {
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        },
        "semantic_conformance": {
          "enum": [
            "not-exercised",
            "schema-only",
            "exercised"
          ],
          "title": "Semantic Conformance",
          "type": "string"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "result_kind",
        "options_schema_profile_sha256",
        "semantic_conformance"
      ],
      "title": "TargetOperationConformance",
      "type": "object"
    },
    "TargetOperationConformanceEvidence": {
      "additionalProperties": false,
      "properties": {
        "accepted_job": {
          "$ref": "#/$defs/AcceptedTargetJob"
        },
        "job_status": {
          "$ref": "#/$defs/TargetJobStatus"
        },
        "operation": {
          "$ref": "#/$defs/OperationContract"
        },
        "operation_id": {
          "title": "Operation Id",
          "type": "string"
        },
        "preflight": {
          "$ref": "#/$defs/TargetPreflightResponse"
        },
        "preflight_request": {
          "$ref": "#/$defs/TargetPreflightRequest"
        },
        "semantic_conformance": {
          "$ref": "#/$defs/TargetSemanticConformance"
        },
        "submission": {
          "$ref": "#/$defs/TargetJobStatus"
        }
      },
      "required": [
        "operation_id",
        "operation",
        "semantic_conformance",
        "preflight_request",
        "preflight",
        "accepted_job",
        "submission",
        "job_status"
      ],
      "title": "TargetOperationConformanceEvidence",
      "type": "object"
    },
    "TargetOperationSupport": {
      "additionalProperties": false,
      "properties": {
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "options_schema"
      ],
      "title": "TargetOperationSupport",
      "type": "object"
    },
    "TargetPlanBinding": {
      "additionalProperties": false,
      "description": "Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope.",
      "properties": {
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "plan": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Plan",
          "type": "object"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "protocol": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Protocol",
          "type": "string"
        },
        "target_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Descriptor Sha256",
          "type": "string"
        },
        "target_implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Target Implementation Id",
          "type": "string"
        }
      },
      "required": [
        "protocol",
        "target_implementation_id",
        "target_descriptor_sha256",
        "operation_contract_sha256",
        "plan",
        "plan_sha256"
      ],
      "title": "TargetPlanBinding",
      "type": "object"
    },
    "TargetPreflightRequest": {
      "additionalProperties": false,
      "properties": {
        "inputs": {
          "$ref": "#/$defs/TargetInputAuthority"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "observations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ContentObservationEvidence"
          },
          "title": "Observations",
          "type": "array"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "title": "Protocol",
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent"
      ],
      "title": "TargetPreflightRequest",
      "type": "object"
    },
    "TargetPreflightResponse": {
      "additionalProperties": false,
      "properties": {
        "descriptor": {
          "$ref": "#/$defs/TargetDescriptor"
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
          ],
          "title": "Plan"
        }
      },
      "required": [
        "descriptor",
        "plan"
      ],
      "title": "TargetPreflightResponse",
      "type": "object"
    },
    "TargetProductionAuthority": {
      "additionalProperties": false,
      "properties": {
        "disposition_count": {
          "minimum": 1,
          "title": "Disposition Count",
          "type": "integer"
        },
        "disposition_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Disposition Sha256",
          "type": "string"
        },
        "format": {
          "const": "stove0-target-production/v1",
          "default": "stove0-target-production/v1",
          "title": "Format",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "outputs": {
          "$ref": "#/$defs/OutputArtifactSetIdentity"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "production_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Production Sha256",
          "type": "string"
        },
        "riverhog_disposition_set": {
          "$ref": "#/$defs/ArtifactDispositionSetIdentity"
        },
        "source_edge_count": {
          "minimum": 1,
          "title": "Source Edge Count",
          "type": "integer"
        },
        "source_edge_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Source Edge Sha256",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "plan_sha256",
        "outputs",
        "disposition_count",
        "disposition_sha256",
        "source_edge_count",
        "source_edge_sha256",
        "riverhog_disposition_set",
        "production_sha256"
      ],
      "title": "TargetProductionAuthority",
      "type": "object"
    },
    "TargetProgress": {
      "additionalProperties": false,
      "properties": {
        "completed": {
          "minimum": 0,
          "title": "Completed",
          "type": "integer"
        },
        "phase": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Phase",
          "type": "string"
        },
        "total": {
          "anyOf": [
            {
              "minimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Total"
        },
        "unit": {
          "anyOf": [
            {
              "maxLength": 40,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Unit"
        }
      },
      "required": [
        "phase",
        "completed"
      ],
      "title": "TargetProgress",
      "type": "object"
    },
    "TargetSemanticConformance": {
      "additionalProperties": false,
      "properties": {
        "accepted_vector_ids": {
          "default": [],
          "items": {
            "type": "string"
          },
          "title": "Accepted Vector Ids",
          "type": "array"
        },
        "conformance_vectors_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Conformance Vectors Sha256"
        },
        "profile_id": {
          "title": "Profile Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "rejected_vector_ids": {
          "default": [],
          "items": {
            "type": "string"
          },
          "title": "Rejected Vector Ids",
          "type": "array"
        },
        "status": {
          "enum": [
            "schema-only",
            "exercised"
          ],
          "title": "Status",
          "type": "string"
        },
        "vectors": {
          "anyOf": [
            {
              "$ref": "#/$defs/SemanticIntentConformanceVectors"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "profile_id",
        "profile_sha256",
        "status"
      ],
      "title": "TargetSemanticConformance",
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
          "title": "Intent",
          "type": "object"
        },
        "observation_result_sha256s": {
          "default": [],
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "title": "Observation Result Sha256S",
          "type": "array"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-transform-target/v1",
          "default": "stove0-transform-target/v1",
          "title": "Protocol",
          "type": "string"
        },
        "target_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Descriptor Sha256",
          "type": "string"
        },
        "target_implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Target Implementation Id",
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent",
        "target_implementation_id",
        "target_descriptor_sha256",
        "plan_sha256"
      ],
      "title": "TransformPlan",
      "type": "object"
    },
    "WorkIdentity": {
      "additionalProperties": false,
      "properties": {
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Effective Intent",
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
          "default": null,
          "title": "Fork Join"
        },
        "format": {
          "const": "stove0-work/v1",
          "default": "stove0-work/v1",
          "title": "Format",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/CollectionRootRef"
          },
          "minItems": 1,
          "title": "Inputs",
          "type": "array"
        },
        "recipe": {
          "$ref": "#/$defs/RecipeRef"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        }
      },
      "required": [
        "recipe",
        "inputs",
        "work_id"
      ],
      "title": "WorkIdentity",
      "type": "object"
    },
    "WorkflowPlan": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-workflow-plan/v1",
          "default": "stove0-workflow-plan/v1",
          "title": "Format",
          "type": "string"
        },
        "input_retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "title": "Input Retrieval Policy",
          "type": "string"
        },
        "observations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ContentObservationEvidence"
          },
          "title": "Observations",
          "type": "array"
        },
        "operation": {
          "$ref": "#/$defs/OperationRef"
        },
        "output_policy": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Output Policy",
          "type": "object"
        },
        "requested_target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Requested Target Options",
          "type": "object"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        },
        "retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "title": "Retirement Grace Seconds",
          "type": "integer"
        },
        "retirement_policy": {
          "default": "retain",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "title": "Retirement Policy",
          "type": "string"
        },
        "target_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Descriptor Sha256",
          "type": "string"
        },
        "target_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "title": "Target Registration Id",
          "type": "string"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        },
        "workflow_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Workflow Plan Sha256",
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
      "title": "WorkflowPlan",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "coverage": {
      "$ref": "#/$defs/TargetConformanceCoverage"
    },
    "descriptor": {
      "$ref": "#/$defs/TargetDescriptor"
    },
    "format": {
      "const": "stove0-target-conformance-result/v1",
      "default": "stove0-target-conformance-result/v1",
      "title": "Format",
      "type": "string"
    },
    "operation_evidence": {
      "default": [],
      "items": {
        "$ref": "#/$defs/TargetOperationConformanceEvidence"
      },
      "title": "Operation Evidence",
      "type": "array"
    },
    "operations": {
      "items": {
        "$ref": "#/$defs/TargetOperationConformance"
      },
      "title": "Operations",
      "type": "array"
    },
    "status": {
      "enum": [
        "conformant",
        "partially-exercised",
        "inspected"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "descriptor",
    "coverage",
    "operations"
  ],
  "title": "TargetConformanceResult",
  "type": "object"
}
```

</details>
