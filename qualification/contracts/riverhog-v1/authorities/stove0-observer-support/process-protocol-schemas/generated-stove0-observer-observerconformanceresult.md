# generated:stove0-observer: ObserverConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-observerconformanceresult:c9aa4067bf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-fc5a6c3bef"></a>

- <a id="s-79c5c6f594"></a>`type`: `"object"`
- <a id="s-6295f094f9"></a>`additionalProperties`: `false`
- <a id="s-3e67ce95d3"></a>`required`: `["status","descriptor","coverage","contracts"]`
- <a id="s-c607c4203b"></a>`title`: `"ObserverConformanceResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11cb6cb7b0"></a>`contracts` | yes | type="array"; items=([ObserverContractConformance](#s-3429ae0b0c)); title="Contracts" |  |
| <a id="s-6e150e7a08"></a>`coverage` | yes | [ObserverConformanceCoverage](#s-a0b35db38e) |  |
| <a id="s-503986d75d"></a>`descriptor` | yes | [ObserverDescriptor](#s-c720233b4b) |  |
| <a id="s-f07392d541"></a>`format` | no | type="string"; const="stove0-observer-conformance-result/v1"; default="stove0-observer-conformance-result/v1"; title="Format" |  |
| <a id="s-86e322f70a"></a>`status` | yes | type="string"; enum=["conformant","partially-exercised","inspected"]; title="Status" |  |

### Definitions

- [ArtifactSubject](#s-6da8dc866c)
- [CollectionId](#s-31ea8cf25b)
- [CollectionRootRef](#s-44b34c0b44)
- [JsonSchemaValidationProfile](#s-da9fd7c093)
- [JsonValue](#s-ff4601229e)
- [ObservationFailure](#s-50f0629b74)
- [ObservationInapplicable](#s-93a287d09a)
- [ObservationRequest](#s-257aa2bfb3)
- [ObservationResult](#s-c17307bef8)
- [ObserverConformanceCoverage](#s-a0b35db38e)
- [ObserverContractConformance](#s-3429ae0b0c)
- [ObserverContractConformanceEvidence](#s-6014cd6960)
- [ObserverContractSupport](#s-c7de29acfc)
- [ObserverDescriptor](#s-c720233b4b)
- [ObserverImplementation](#s-ff3c4a891a)
- [ObserverSemanticAcceptance](#s-cb936e6e5e)
- [ObserverSemanticVectorEvidence](#s-cadb8352a7)
- [SemanticFactsConformanceVector](#s-c3aa3f5051)
- [SemanticFactsConformanceVectors](#s-02d07e8151)
- [SemanticValidationProfile](#s-5fcce07d86)

### <a id="s-6da8dc866c"></a>definition `ArtifactSubject`

- <a id="s-c053f60f84"></a>`type`: `"object"`
- <a id="s-153be89ef6"></a>`additionalProperties`: `false`
- <a id="s-f83229bb6a"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-5ded488401"></a>`title`: `"ArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7f4e336ffd"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-11d80d9212"></a>`collection` | yes | [CollectionRootRef](#s-44b34c0b44) |  |
| <a id="s-32af5dddc0"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-3ae226d409"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-8001657182"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-ad00442c9f"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-a66068345d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-31ea8cf25b"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-f2d2c9b0bc"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-c6321b43dc"></a>2 | not=(const="0") |

### <a id="s-44b34c0b44"></a>definition `CollectionRootRef`

- <a id="s-e80972cfa4"></a>`type`: `"object"`
- <a id="s-aea7b372b4"></a>`additionalProperties`: `false`
- <a id="s-d2bebcc355"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-ec9124659f"></a>`title`: `"CollectionRootRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6abd9b8502"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-228e1fc6b5"></a>`collection_id` | yes | [CollectionId](#s-31ea8cf25b) |  |
| <a id="s-c60a496dd7"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-da9fd7c093"></a>definition `JsonSchemaValidationProfile`

- <a id="s-5089e41aa4"></a>`type`: `"object"`
- <a id="s-ca6d991013"></a>`additionalProperties`: `false`
- <a id="s-20fa6b5c4d"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-b1a47da76f"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d2c1aceff1"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-c217e66265"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-153311da26"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-fb9e72256b"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-5319309a2f"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-ff4601229e)); title="Schema" |  |

### <a id="s-ff4601229e"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-50f0629b74"></a>definition `ObservationFailure`

- <a id="s-06ae15fbac"></a>`type`: `"object"`
- <a id="s-5d25dd52fa"></a>`additionalProperties`: `false`
- <a id="s-18b6ce6f67"></a>`required`: `["code","message","retryable"]`
- <a id="s-08f5cd3f8f"></a>`title`: `"ObservationFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1b9ec049ca"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-32f334ea59"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-2e0bc17e07"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-93a287d09a"></a>definition `ObservationInapplicable`

- <a id="s-af82c28587"></a>`type`: `"object"`
- <a id="s-d240f2bbe0"></a>`additionalProperties`: `false`
- <a id="s-9f95a76019"></a>`required`: `["code","message"]`
- <a id="s-69227920bd"></a>`title`: `"ObservationInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d16c78a08b"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-124c7a2555"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-257aa2bfb3"></a>definition `ObservationRequest`

- <a id="s-8329a6333d"></a>`type`: `"object"`
- <a id="s-690386a7d4"></a>`additionalProperties`: `false`
- <a id="s-f7b8ef2e6b"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-deee355643"></a>`title`: `"ObservationRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-063dbbbac8"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-85e5d57c88"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-9505a95bd6"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-b60c589e12"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-a8076c4360"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-3703138164"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-a6b34a213f"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-ff4601229e)); title="Options" |  |
| <a id="s-add37a784d"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-6d8c6061d2"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-71c6f314e8"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6da8dc866c)); minItems=1; title="Subjects" |  |
| <a id="s-9c0a94b110"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-8f110b30b1"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-c17307bef8"></a>definition `ObservationResult`

- <a id="s-988702c2ff"></a>`type`: `"object"`
- <a id="s-9f996697ca"></a>`additionalProperties`: `false`
- <a id="s-5802f343d8"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-f816936276"></a>`title`: `"ObservationResult"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-18fec8dde8"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-ff4601229e)); title="Execution Evidence" |  |
| <a id="s-772d9bd642"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-ff4601229e))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-f25ed3d92e"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-da9fd7c093)); (type="null")]; default=null |  |
| <a id="s-0e80e5d651"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-0022c7d805"></a>`failure` | no | anyOf=[([ObservationFailure](#s-50f0629b74)); (type="null")]; default=null |  |
| <a id="s-28498be470"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-3cf498ecf9"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-93a287d09a)); (type="null")]; default=null |  |
| <a id="s-771d96d8ec"></a>`observer` | yes | [ObserverImplementation](#s-ff3c4a891a) |  |
| <a id="s-90052cd0c4"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-0cc5d9dcbc"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-8e01b8f9e5"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-0d1cf88363"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-30158d8a6d"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-f5c3f9cb96"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6da8dc866c)); minItems=1; title="Subjects" |  |

### <a id="s-a0b35db38e"></a>definition `ObserverConformanceCoverage`

- <a id="s-0e74268425"></a>`type`: `"object"`
- <a id="s-76d2675a68"></a>`additionalProperties`: `false`
- <a id="s-47347322b0"></a>`required`: `["advertised","exercised","complete"]`
- <a id="s-f45fe65b1b"></a>`title`: `"ObserverConformanceCoverage"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cfffbb449b"></a>`advertised` | yes | type="integer"; minimum=0; title="Advertised" |  |
| <a id="s-9035439578"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-8b641bcfe9"></a>`exercised` | yes | type="integer"; minimum=0; title="Exercised" |  |

### <a id="s-3429ae0b0c"></a>definition `ObserverContractConformance`

- <a id="s-3896e12241"></a>`type`: `"object"`
- <a id="s-eae004cf6e"></a>`additionalProperties`: `false`
- <a id="s-c2ba9a8263"></a>`required`: `["contract_id","contract_sha256","options_schema_profile_sha256","facts_schema_profile_sha256","facts_semantics_id","facts_semantics_sha256","preferred_subject_batch_size","maximum_result_bytes","execution"]`
- <a id="s-0d0c324fb2"></a>`title`: `"ObserverContractConformance"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92337f811b"></a>`contract_id` | yes | type="string"; title="Contract Id" |  |
| <a id="s-61309d58ef"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-fe63aa2b7a"></a>`evidence` | no | anyOf=[([ObserverContractConformanceEvidence](#s-6014cd6960)); (type="null")]; default=null |  |
| <a id="s-427ec692af"></a>`execution` | yes | type="string"; enum=["not-exercised","exercised"]; title="Execution" |  |
| <a id="s-11b95ce1ff"></a>`facts_schema_profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Facts Schema Profile Sha256" |  |
| <a id="s-23b7f66d33"></a>`facts_semantics_conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Semantics Conformance Vectors Sha256" |  |
| <a id="s-e9835fc00b"></a>`facts_semantics_id` | yes | type="string"; title="Facts Semantics Id" |  |
| <a id="s-4d9c57cb7a"></a>`facts_semantics_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Facts Semantics Sha256" |  |
| <a id="s-b355fd545b"></a>`maximum_result_bytes` | yes | type="integer"; minimum=1; title="Maximum Result Bytes" |  |
| <a id="s-8b12d42e1b"></a>`options_schema_profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Options Schema Profile Sha256" |  |
| <a id="s-ac70fd6b4e"></a>`preferred_subject_batch_size` | yes | type="integer"; minimum=1; title="Preferred Subject Batch Size" |  |
| <a id="s-f952afe858"></a>`semantic_acceptance` | no | anyOf=[([ObserverSemanticAcceptance](#s-cb936e6e5e)); (type="null")]; default=null |  |

### <a id="s-6014cd6960"></a>definition `ObserverContractConformanceEvidence`

- <a id="s-f721167944"></a>`type`: `"object"`
- <a id="s-ae2085c8f4"></a>`additionalProperties`: `false`
- <a id="s-8b39db5175"></a>`required`: `["request","observation"]`
- <a id="s-80573b26d1"></a>`title`: `"ObserverContractConformanceEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f1a0bb00f5"></a>`observation` | yes | [ObservationResult](#s-c17307bef8) |  |
| <a id="s-8f6bf3330e"></a>`request` | yes | [ObservationRequest](#s-257aa2bfb3) |  |

### <a id="s-c7de29acfc"></a>definition `ObserverContractSupport`

- <a id="s-1f6881786f"></a>`type`: `"object"`
- <a id="s-b1fa78b566"></a>`additionalProperties`: `false`
- <a id="s-bd1db9508b"></a>`required`: `["contract_id","contract_sha256","options_schema","facts_schema","facts_semantics","maximum_result_bytes"]`
- <a id="s-7fbc291fdf"></a>`title`: `"ObserverContractSupport"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9737520dfb"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Contract Id" |  |
| <a id="s-0bf8abada4"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-be69f7b573"></a>`facts_schema` | yes | [JsonSchemaValidationProfile](#s-da9fd7c093) |  |
| <a id="s-2954664f39"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-5fcce07d86) |  |
| <a id="s-6c87e3c09e"></a>`maximum_result_bytes` | yes | type="integer"; minimum=1; maximum=67108864; title="Maximum Result Bytes" |  |
| <a id="s-71e00e102e"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-da9fd7c093) |  |
| <a id="s-01e13b0bc0"></a>`preferred_subject_batch_size` | no | type="integer"; minimum=1; default=128; title="Preferred Subject Batch Size" |  |

### <a id="s-c720233b4b"></a>definition `ObserverDescriptor`

- <a id="s-cbe0f513a3"></a>`type`: `"object"`
- <a id="s-2a692d102d"></a>`additionalProperties`: `false`
- <a id="s-09d1e56d14"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","contracts","descriptor_sha256"]`
- <a id="s-e261655d00"></a>`title`: `"ObserverDescriptor"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ef1f8a487"></a>`contracts` | yes | type="array"; items=([ObserverContractSupport](#s-c7de29acfc)); minItems=1; title="Contracts" |  |
| <a id="s-42cfeceeff"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-f9bf8ce9e8"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-10c44bb963"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-dceba6dd81"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-1f3d23eb37"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-065ca78d50"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |

### <a id="s-ff3c4a891a"></a>definition `ObserverImplementation`

- <a id="s-9cab9b4336"></a>`type`: `"object"`
- <a id="s-4feae3e9de"></a>`additionalProperties`: `false`
- <a id="s-824e8935ba"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-0eb25e140b"></a>`title`: `"ObserverImplementation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a2e6069b06"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-e98b7bb56c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-edfa24d35e"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-bc58e8e12b"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-5360514868"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

### <a id="s-cb936e6e5e"></a>definition `ObserverSemanticAcceptance`

- <a id="s-5beef27254"></a>`type`: `"object"`
- <a id="s-9474c35511"></a>`additionalProperties`: `false`
- <a id="s-b748102b8d"></a>`description`: `"Exact schema revalidation or explicit conformance-runner attestation."`
- <a id="s-6165fa5509"></a>`required`: `["kind","profile_id","profile_sha256"]`
- <a id="s-984aa249a1"></a>`title`: `"ObserverSemanticAcceptance"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-acdca8b0f6"></a>`kind` | yes | type="string"; enum=["schema-only-revalidated","conformance-runner-attestation"]; title="Kind" |  |
| <a id="s-58eb835086"></a>`profile_id` | yes | type="string"; title="Profile Id" |  |
| <a id="s-4696fbe59a"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-4f684adacc"></a>`vectors` | no | anyOf=[([ObserverSemanticVectorEvidence](#s-cadb8352a7)); (type="null")]; default=null |  |

### <a id="s-cadb8352a7"></a>definition `ObserverSemanticVectorEvidence`

- <a id="s-19749c2c8f"></a>`type`: `"object"`
- <a id="s-9e5b86f642"></a>`additionalProperties`: `false`
- <a id="s-4e07f437f6"></a>`required`: `["vectors","accepted_vector_ids","rejected_vector_ids"]`
- <a id="s-2da3cd282a"></a>`title`: `"ObserverSemanticVectorEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-84a0c09846"></a>`accepted_vector_ids` | yes | type="array"; items=(type="string"); title="Accepted Vector Ids" |  |
| <a id="s-a7a7a2ea7c"></a>`rejected_vector_ids` | yes | type="array"; items=(type="string"); title="Rejected Vector Ids" |  |
| <a id="s-0a099f7dfd"></a>`vectors` | yes | [SemanticFactsConformanceVectors](#s-02d07e8151) |  |

### <a id="s-c3aa3f5051"></a>definition `SemanticFactsConformanceVector`

- <a id="s-e76b9ebc9a"></a>`type`: `"object"`
- <a id="s-aff262e3b2"></a>`additionalProperties`: `false`
- <a id="s-11fb15ec9d"></a>`required`: `["id","accepted","subjects","facts"]`
- <a id="s-1f963619c7"></a>`title`: `"SemanticFactsConformanceVector"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4128764b82"></a>`accepted` | yes | type="boolean"; title="Accepted" |  |
| <a id="s-b56714fb4e"></a>`facts` | yes | type="object"; additionalProperties=([JsonValue](#s-ff4601229e)); title="Facts" |  |
| <a id="s-8fd47d1fad"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-14bd5c07e0"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-ff4601229e)); title="Options" |  |
| <a id="s-a0524302e6"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6da8dc866c)); minItems=1; title="Subjects" |  |

### <a id="s-02d07e8151"></a>definition `SemanticFactsConformanceVectors`

- <a id="s-7d4901e424"></a>`type`: `"object"`
- <a id="s-513f29aedd"></a>`additionalProperties`: `false`
- <a id="s-58339d082c"></a>`required`: `["profile_id","vectors"]`
- <a id="s-f41b7f920b"></a>`title`: `"SemanticFactsConformanceVectors"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6d2354da8"></a>`format` | no | type="string"; const="stove0-semantic-facts-conformance/v1"; default="stove0-semantic-facts-conformance/v1"; title="Format" |  |
| <a id="s-10d5c10d22"></a>`profile_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Profile Id" |  |
| <a id="s-34b15007e4"></a>`vectors` | yes | type="array"; items=([SemanticFactsConformanceVector](#s-c3aa3f5051)); minItems=2; title="Vectors" |  |

### <a id="s-5fcce07d86"></a>definition `SemanticValidationProfile`

- <a id="s-0dff62d189"></a>`type`: `"object"`
- <a id="s-d13e661354"></a>`additionalProperties`: `false`
- <a id="s-74aaefa112"></a>`required`: `["id","rules","profile_sha256"]`
- <a id="s-d660fc9b23"></a>`title`: `"SemanticValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a5fa440820"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-20d7dcf1ac"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-45433a22b1"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-90fed6464f"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Rules" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contracts](#s-11cb6cb7b0) | `cardinality · items · operational_policy` | shared above |
| [definition ObservationResult · field execution_evidence](#s-18fec8dde8) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-454b889102"></a>[definition ObservationResult · field facts · object value](#s-772d9bd642) | `cardinality · entries · operational_policy` | shared above |
| [definition ObservationResult · field subjects](#s-f5c3f9cb96) | `cardinality · items · operational_policy` | shared above |
| [definition ObserverDescriptor · field contracts](#s-2ef1f8a487) | `cardinality · items · operational_policy` | shared above |
| [definition ObserverSemanticVectorEvidence · field accepted_vector_ids](#s-84a0c09846) | `cardinality · items · operational_policy` | shared above |
| [definition ObserverSemanticVectorEvidence · field rejected_vector_ids](#s-a7a7a2ea7c) | `cardinality · items · operational_policy` | shared above |
| [definition SemanticFactsConformanceVector · field facts](#s-b56714fb4e) | `cardinality · entries · operational_policy` | shared above |
| [definition SemanticFactsConformanceVector · field options](#s-14bd5c07e0) | `cardinality · entries · operational_policy` | shared above |
| [definition SemanticFactsConformanceVector · field subjects](#s-a0524302e6) | `cardinality · items · operational_policy` | shared above |
| [definition SemanticFactsConformanceVectors · field vectors](#s-34b15007e4) | `cardinality · items · operational_policy` | shared above |
| [definition SemanticValidationProfile · field rules](#s-90fed6464f) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-689a9a00f0"></a>[definition ObservationResult · field facts_sha256 · string value](#s-0e80e5d651) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObservationResult · field observer_contract_sha256](#s-0cc5d9dcbc) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObservationResult · field request_id](#s-8e01b8f9e5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObservationResult · field result_sha256](#s-0d1cf88363) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverContractConformance · field contract_sha256](#s-61309d58ef) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverContractConformance · field facts_schema_profile_sha256](#s-11b95ce1ff) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-24b459cc32"></a>[definition ObserverContractConformance · field facts_semantics_conformance_vectors_sha256 · string value](#s-23b7f66d33) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverContractConformance · field facts_semantics_sha256](#s-4d9c57cb7a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverContractConformance · field options_schema_profile_sha256](#s-8b12d42e1b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverContractSupport · field contract_sha256](#s-0bf8abada4) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverContractSupport · field maximum_result_bytes](#s-6c87e3c09e) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [definition ObserverDescriptor · field descriptor_sha256](#s-42cfeceeff) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverDescriptor · field image_digest](#s-f9bf8ce9e8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverDescriptor · field implementation_version](#s-dceba6dd81) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [definition ObserverDescriptor · field source_revision](#s-065ca78d50) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [definition ObserverSemanticAcceptance · field profile_sha256](#s-4696fbe59a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-2852262eab"></a>[definition SemanticValidationProfile · field conformance_vectors_sha256 · string value](#s-a5fa440820) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SemanticValidationProfile · field profile_sha256](#s-45433a22b1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1b9cb8a9bb"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-95bb6c7bd0"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-15628f904f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObserverConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 722ec3e8b343fad370ffdbd250c12ed693f4064f8af27e2b1c4ca7111d103516 -->

```json
{
  "$defs": {
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
    "ObservationFailure": {
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
      "title": "ObservationFailure",
      "type": "object"
    },
    "ObservationInapplicable": {
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
      "title": "ObservationInapplicable",
      "type": "object"
    },
    "ObservationRequest": {
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
      "title": "ObservationRequest",
      "type": "object"
    },
    "ObservationResult": {
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
          "title": "Format",
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
      "title": "ObservationResult",
      "type": "object"
    },
    "ObserverConformanceCoverage": {
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
      "title": "ObserverConformanceCoverage",
      "type": "object"
    },
    "ObserverContractConformance": {
      "additionalProperties": false,
      "properties": {
        "contract_id": {
          "title": "Contract Id",
          "type": "string"
        },
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "evidence": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObserverContractConformanceEvidence"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "execution": {
          "enum": [
            "not-exercised",
            "exercised"
          ],
          "title": "Execution",
          "type": "string"
        },
        "facts_schema_profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Facts Schema Profile Sha256",
          "type": "string"
        },
        "facts_semantics_conformance_vectors_sha256": {
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
          "title": "Facts Semantics Conformance Vectors Sha256"
        },
        "facts_semantics_id": {
          "title": "Facts Semantics Id",
          "type": "string"
        },
        "facts_semantics_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Facts Semantics Sha256",
          "type": "string"
        },
        "maximum_result_bytes": {
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options_schema_profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Options Schema Profile Sha256",
          "type": "string"
        },
        "preferred_subject_batch_size": {
          "minimum": 1,
          "title": "Preferred Subject Batch Size",
          "type": "integer"
        },
        "semantic_acceptance": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObserverSemanticAcceptance"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "contract_id",
        "contract_sha256",
        "options_schema_profile_sha256",
        "facts_schema_profile_sha256",
        "facts_semantics_id",
        "facts_semantics_sha256",
        "preferred_subject_batch_size",
        "maximum_result_bytes",
        "execution"
      ],
      "title": "ObserverContractConformance",
      "type": "object"
    },
    "ObserverContractConformanceEvidence": {
      "additionalProperties": false,
      "properties": {
        "observation": {
          "$ref": "#/$defs/ObservationResult"
        },
        "request": {
          "$ref": "#/$defs/ObservationRequest"
        }
      },
      "required": [
        "request",
        "observation"
      ],
      "title": "ObserverContractConformanceEvidence",
      "type": "object"
    },
    "ObserverContractSupport": {
      "additionalProperties": false,
      "properties": {
        "contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Contract Id",
          "type": "string"
        },
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "facts_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "facts_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "maximum_result_bytes": {
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "preferred_subject_batch_size": {
          "default": 128,
          "minimum": 1,
          "title": "Preferred Subject Batch Size",
          "type": "integer"
        }
      },
      "required": [
        "contract_id",
        "contract_sha256",
        "options_schema",
        "facts_schema",
        "facts_semantics",
        "maximum_result_bytes"
      ],
      "title": "ObserverContractSupport",
      "type": "object"
    },
    "ObserverDescriptor": {
      "additionalProperties": false,
      "properties": {
        "contracts": {
          "items": {
            "$ref": "#/$defs/ObserverContractSupport"
          },
          "minItems": 1,
          "title": "Contracts",
          "type": "array"
        },
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Image Digest",
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
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_digest",
        "contracts",
        "descriptor_sha256"
      ],
      "title": "ObserverDescriptor",
      "type": "object"
    },
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
    "ObserverSemanticAcceptance": {
      "additionalProperties": false,
      "description": "Exact schema revalidation or explicit conformance-runner attestation.",
      "properties": {
        "kind": {
          "enum": [
            "schema-only-revalidated",
            "conformance-runner-attestation"
          ],
          "title": "Kind",
          "type": "string"
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
        "vectors": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObserverSemanticVectorEvidence"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "kind",
        "profile_id",
        "profile_sha256"
      ],
      "title": "ObserverSemanticAcceptance",
      "type": "object"
    },
    "ObserverSemanticVectorEvidence": {
      "additionalProperties": false,
      "properties": {
        "accepted_vector_ids": {
          "items": {
            "type": "string"
          },
          "title": "Accepted Vector Ids",
          "type": "array"
        },
        "rejected_vector_ids": {
          "items": {
            "type": "string"
          },
          "title": "Rejected Vector Ids",
          "type": "array"
        },
        "vectors": {
          "$ref": "#/$defs/SemanticFactsConformanceVectors"
        }
      },
      "required": [
        "vectors",
        "accepted_vector_ids",
        "rejected_vector_ids"
      ],
      "title": "ObserverSemanticVectorEvidence",
      "type": "object"
    },
    "SemanticFactsConformanceVector": {
      "additionalProperties": false,
      "properties": {
        "accepted": {
          "title": "Accepted",
          "type": "boolean"
        },
        "facts": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Facts",
          "type": "object"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
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
        "id",
        "accepted",
        "subjects",
        "facts"
      ],
      "title": "SemanticFactsConformanceVector",
      "type": "object"
    },
    "SemanticFactsConformanceVectors": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-semantic-facts-conformance/v1",
          "default": "stove0-semantic-facts-conformance/v1",
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
            "$ref": "#/$defs/SemanticFactsConformanceVector"
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
      "title": "SemanticFactsConformanceVectors",
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
    }
  },
  "additionalProperties": false,
  "properties": {
    "contracts": {
      "items": {
        "$ref": "#/$defs/ObserverContractConformance"
      },
      "title": "Contracts",
      "type": "array"
    },
    "coverage": {
      "$ref": "#/$defs/ObserverConformanceCoverage"
    },
    "descriptor": {
      "$ref": "#/$defs/ObserverDescriptor"
    },
    "format": {
      "const": "stove0-observer-conformance-result/v1",
      "default": "stove0-observer-conformance-result/v1",
      "title": "Format",
      "type": "string"
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
    "contracts"
  ],
  "title": "ObserverConformanceResult",
  "type": "object"
}
```

</details>
