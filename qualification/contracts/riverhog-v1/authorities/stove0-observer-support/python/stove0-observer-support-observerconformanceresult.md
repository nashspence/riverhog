# stove0_observer_support.ObserverConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerconformanceresult:82b6a495cb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc4e720fe4"></a>
- <a id="s-a04371e892"></a>`distribution`: `stove0-observer-support`
- <a id="s-0b2a786915"></a>`module`: `stove0_observer_support`
- <a id="s-ef0f9faad0"></a>`name`: `ObserverConformanceResult`
- <a id="s-268fd9a495"></a>`unit`: `export`

### Declared structure

- <a id="s-8f0566d785"></a>`kind`: `"class"`
- <a id="s-0992fdf9a7"></a>`signature`: `"\"(*, format: Literal['stove0-observer-conformance-result/v1'] = 'stove0-observer-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], descriptor: stove0_protocol.models.ObserverDescriptor, coverage: stove0_observer_support.conformance.ObserverConformanceCoverage, contracts: tuple[stove0_observer_support.conformance.ObserverContractConformance, ...]) -> None\""`

#### Validated model schema

<a id="s-837129e687"></a>

- <a id="s-9aabec6c04"></a>`type`: `"object"`
- <a id="s-3697f8c18b"></a>`additionalProperties`: `false`
- <a id="s-11bd863828"></a>`required`: `["status","descriptor","coverage","contracts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e8467985db"></a>`contracts` | yes | type="array"; items=([ObserverContractConformance](#s-1810706536)) |  |
| <a id="s-4239338330"></a>`coverage` | yes | [ObserverConformanceCoverage](#s-ed0b4a623e) |  |
| <a id="s-f6874849e5"></a>`descriptor` | yes | [ObserverDescriptor](#s-21208e2a60) |  |
| <a id="s-3796d93708"></a>`format` | no | type="string"; const="stove0-observer-conformance-result/v1"; default="stove0-observer-conformance-result/v1" |  |
| <a id="s-07cec605ce"></a>`status` | yes | type="string"; enum=["conformant","partially-exercised","inspected"] |  |

##### Definitions

- [ArtifactSubject](#s-3ce92b5f73)
- [CollectionId](#s-d02ffcac30)
- [CollectionRootRef](#s-fe9cd9564b)
- [ContentObservationFailure](#s-8af03bdd0e)
- [ContentObservationInapplicable](#s-fc5abaf45b)
- [ContentObservationRequest](#s-92cb7320b2)
- [ContentObservationResult](#s-c77d4633aa)
- [JsonSchemaValidationProfile](#s-b68f9b01be)
- [JsonValue](#s-5225835bde)
- [ObserverConformanceCoverage](#s-ed0b4a623e)
- [ObserverContractConformance](#s-1810706536)
- [ObserverContractConformanceEvidence](#s-e9d1642adb)
- [ObserverContractSupport](#s-e95a44f82c)
- [ObserverDescriptor](#s-21208e2a60)
- [ObserverImplementation](#s-9addfdd17e)
- [ObserverSemanticAcceptance](#s-4af9735373)
- [ObserverSemanticVectorEvidence](#s-e03614fd2e)
- [SemanticFactsConformanceVector](#s-e3a938c4ae)
- [SemanticFactsConformanceVectors](#s-54f57b5506)
- [SemanticValidationProfile](#s-94f2656e94)

##### <a id="s-3ce92b5f73"></a>definition `ArtifactSubject`

- <a id="s-956a707527"></a>`type`: `"object"`
- <a id="s-9bdeb52218"></a>`additionalProperties`: `false`
- <a id="s-13e1e9ed0b"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a11785c7cc"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-c604767bbe"></a>`collection` | yes | [CollectionRootRef](#s-fe9cd9564b) |  |
| <a id="s-69609ace18"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-3bd1cb9986"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-c68f44a017"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-3a9cdf8f7b"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4e0ffefc82"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d02ffcac30"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-911212f6d9"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-63a56a1927"></a>2 | not=(const="0") |

##### <a id="s-fe9cd9564b"></a>definition `CollectionRootRef`

- <a id="s-aa2ece7971"></a>`type`: `"object"`
- <a id="s-258d652d91"></a>`additionalProperties`: `false`
- <a id="s-e883c57a94"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-548b39268d"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-52e6f136b8"></a>`collection_id` | yes | [CollectionId](#s-d02ffcac30) |  |
| <a id="s-189c8a7e89"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8af03bdd0e"></a>definition `ContentObservationFailure`

- <a id="s-dab862ee6d"></a>`type`: `"object"`
- <a id="s-4214247b1a"></a>`additionalProperties`: `false`
- <a id="s-34649ca6cd"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6cf79268cf"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fb9932b3e0"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-a2abf0429f"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-fc5abaf45b"></a>definition `ContentObservationInapplicable`

- <a id="s-77f53508d6"></a>`type`: `"object"`
- <a id="s-7aad9006e5"></a>`additionalProperties`: `false`
- <a id="s-ae5f080107"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5d858257c1"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-522f028bc4"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-92cb7320b2"></a>definition `ContentObservationRequest`

- <a id="s-b2e1b19f11"></a>`type`: `"object"`
- <a id="s-bbe83f0469"></a>`additionalProperties`: `false`
- <a id="s-6812820ebf"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a136b51d73"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-444c9410bc"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-63f41aeda1"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4d05a967dd"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-04616aa09f"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a092307f00"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-8a4c2da487"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-5225835bde)) |  |
| <a id="s-88671a1493"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1b5378a297"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-b3f9a4819c"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-3ce92b5f73)); minItems=1 |  |
| <a id="s-0c68b6acf5"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-49dec5d1e7"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c77d4633aa"></a>definition `ContentObservationResult`

- <a id="s-aae75d357c"></a>`type`: `"object"`
- <a id="s-c624b9cb2b"></a>`additionalProperties`: `false`
- <a id="s-bc6c11eada"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-62ac4e510d"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-5225835bde)) |  |
| <a id="s-7f82111708"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-5225835bde))); (type="null")]; default=null |  |
| <a id="s-cab4da2bbc"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-b68f9b01be)); (type="null")]; default=null |  |
| <a id="s-8d8bb322da"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-967be264f3"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-8af03bdd0e)); (type="null")]; default=null |  |
| <a id="s-dfc9bfade6"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-f123ef8209"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-fc5abaf45b)); (type="null")]; default=null |  |
| <a id="s-5eb398dc5a"></a>`observer` | yes | [ObserverImplementation](#s-9addfdd17e) |  |
| <a id="s-9817376f7c"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0e5883fe0e"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-07eb387cac"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3e4e6e5a02"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d06b4a7135"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-93724d7386"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-3ce92b5f73)); minItems=1 |  |

##### <a id="s-b68f9b01be"></a>definition `JsonSchemaValidationProfile`

- <a id="s-3081ae7b35"></a>`type`: `"object"`
- <a id="s-39a7195ea2"></a>`additionalProperties`: `false`
- <a id="s-7d7e6e698a"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-55d0c143bc"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-f757fc31d4"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-1182be91b4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-20c6deb5bf"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6ca6f56a37"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-5225835bde)) |  |

##### <a id="s-5225835bde"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-ed0b4a623e"></a>definition `ObserverConformanceCoverage`

- <a id="s-5b17542e35"></a>`type`: `"object"`
- <a id="s-e45906370b"></a>`additionalProperties`: `false`
- <a id="s-9beb71294e"></a>`required`: `["advertised","exercised","complete"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-663df1af01"></a>`advertised` | yes | type="integer"; minimum=0 |  |
| <a id="s-4ee685478c"></a>`complete` | yes | type="boolean" |  |
| <a id="s-4e74994230"></a>`exercised` | yes | type="integer"; minimum=0 |  |

##### <a id="s-1810706536"></a>definition `ObserverContractConformance`

- <a id="s-9299f4ee36"></a>`type`: `"object"`
- <a id="s-6c08c6c162"></a>`additionalProperties`: `false`
- <a id="s-e334dd4700"></a>`required`: `["contract_id","contract_sha256","options_schema_profile_sha256","facts_schema_profile_sha256","facts_semantics_id","facts_semantics_sha256","preferred_subject_batch_size","maximum_result_bytes","execution"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa8bd00094"></a>`contract_id` | yes | type="string" |  |
| <a id="s-d6af6f81cf"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6fd9decde4"></a>`evidence` | no | anyOf=[([ObserverContractConformanceEvidence](#s-e9d1642adb)); (type="null")]; default=null |  |
| <a id="s-606bf765fd"></a>`execution` | yes | type="string"; enum=["not-exercised","exercised"] |  |
| <a id="s-7911563582"></a>`facts_schema_profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0ab33e5189"></a>`facts_semantics_conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-05e9c398a1"></a>`facts_semantics_id` | yes | type="string" |  |
| <a id="s-3584b7368d"></a>`facts_semantics_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-79dce4f59c"></a>`maximum_result_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-27e3754b86"></a>`options_schema_profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cee660bca3"></a>`preferred_subject_batch_size` | yes | type="integer"; minimum=1 |  |
| <a id="s-e9398e8346"></a>`semantic_acceptance` | no | anyOf=[([ObserverSemanticAcceptance](#s-4af9735373)); (type="null")]; default=null |  |

##### <a id="s-e9d1642adb"></a>definition `ObserverContractConformanceEvidence`

- <a id="s-750f9fddee"></a>`type`: `"object"`
- <a id="s-20d048964d"></a>`additionalProperties`: `false`
- <a id="s-eb79af4853"></a>`required`: `["request","observation"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c370cc8bbd"></a>`observation` | yes | [ContentObservationResult](#s-c77d4633aa) |  |
| <a id="s-7f609ce739"></a>`request` | yes | [ContentObservationRequest](#s-92cb7320b2) |  |

##### <a id="s-e95a44f82c"></a>definition `ObserverContractSupport`

- <a id="s-3f4351640e"></a>`type`: `"object"`
- <a id="s-1c5d32e2a7"></a>`additionalProperties`: `false`
- <a id="s-4209f054bf"></a>`required`: `["contract_id","contract_sha256","options_schema","facts_schema","facts_semantics","maximum_result_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d1b3319aee"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3a5db5fa72"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-880ab728a9"></a>`facts_schema` | yes | [JsonSchemaValidationProfile](#s-b68f9b01be) |  |
| <a id="s-9d0319a58c"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-94f2656e94) |  |
| <a id="s-82a576e9fd"></a>`maximum_result_bytes` | yes | type="integer"; minimum=1; maximum=67108864 |  |
| <a id="s-6cd5ad300e"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-b68f9b01be) |  |
| <a id="s-602c876564"></a>`preferred_subject_batch_size` | no | type="integer"; minimum=1; default=128 |  |

##### <a id="s-21208e2a60"></a>definition `ObserverDescriptor`

- <a id="s-a00a2624f8"></a>`type`: `"object"`
- <a id="s-cfc3e346ea"></a>`additionalProperties`: `false`
- <a id="s-581a92fcd9"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","contracts","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d7e740b29e"></a>`contracts` | yes | type="array"; items=([ObserverContractSupport](#s-e95a44f82c)); minItems=1 |  |
| <a id="s-9f0f2da653"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fe3ef4394c"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$" |  |
| <a id="s-d7dbf159b4"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c599f2d816"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-4718eea680"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-46e60956e8"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |

##### <a id="s-9addfdd17e"></a>definition `ObserverImplementation`

- <a id="s-8e75bdc58d"></a>`type`: `"object"`
- <a id="s-c57d37aa05"></a>`additionalProperties`: `false`
- <a id="s-4b2ba19555"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-650b5b8555"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8150a3cc12"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f6123d6a55"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-15d6ec05f9"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-dcbf65727b"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-4af9735373"></a>definition `ObserverSemanticAcceptance`

- <a id="s-8574f0290d"></a>`type`: `"object"`
- <a id="s-b6f067fbba"></a>`additionalProperties`: `false`
- <a id="s-d2f31b5a31"></a>`required`: `["kind","profile_id","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0111f4828c"></a>`kind` | yes | type="string"; enum=["schema-only-revalidated","conformance-runner-attestation"] |  |
| <a id="s-04c9ba9a69"></a>`profile_id` | yes | type="string" |  |
| <a id="s-ad30e9a72d"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-63c6e7c369"></a>`vectors` | no | anyOf=[([ObserverSemanticVectorEvidence](#s-e03614fd2e)); (type="null")]; default=null |  |

##### <a id="s-e03614fd2e"></a>definition `ObserverSemanticVectorEvidence`

- <a id="s-b6e0c52004"></a>`type`: `"object"`
- <a id="s-66d39c7534"></a>`additionalProperties`: `false`
- <a id="s-fae0e1b220"></a>`required`: `["vectors","accepted_vector_ids","rejected_vector_ids"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3448bbeb41"></a>`accepted_vector_ids` | yes | type="array"; items=(type="string") |  |
| <a id="s-454528acd1"></a>`rejected_vector_ids` | yes | type="array"; items=(type="string") |  |
| <a id="s-f10f31d41c"></a>`vectors` | yes | [SemanticFactsConformanceVectors](#s-54f57b5506) |  |

##### <a id="s-e3a938c4ae"></a>definition `SemanticFactsConformanceVector`

- <a id="s-7f62d485b8"></a>`type`: `"object"`
- <a id="s-db462ee7f7"></a>`additionalProperties`: `false`
- <a id="s-e9c2c671fd"></a>`required`: `["id","accepted","subjects","facts"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3cac7a59c0"></a>`accepted` | yes | type="boolean" |  |
| <a id="s-90fb2424af"></a>`facts` | yes | type="object"; additionalProperties=([JsonValue](#s-5225835bde)) |  |
| <a id="s-95c0a1d98e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fc6c68608e"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-5225835bde)) |  |
| <a id="s-ece27ac198"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-3ce92b5f73)); minItems=1 |  |

##### <a id="s-54f57b5506"></a>definition `SemanticFactsConformanceVectors`

- <a id="s-7d9529f4b3"></a>`type`: `"object"`
- <a id="s-4cc51f8c61"></a>`additionalProperties`: `false`
- <a id="s-049f13cdc6"></a>`required`: `["profile_id","vectors"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a2264c738f"></a>`format` | no | type="string"; const="stove0-semantic-facts-conformance/v1"; default="stove0-semantic-facts-conformance/v1" |  |
| <a id="s-fbb27f6c6e"></a>`profile_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-46e6f6adf6"></a>`vectors` | yes | type="array"; items=([SemanticFactsConformanceVector](#s-e3a938c4ae)); minItems=2 |  |

##### <a id="s-94f2656e94"></a>definition `SemanticValidationProfile`

- <a id="s-07fbf60655"></a>`type`: `"object"`
- <a id="s-023690041d"></a>`additionalProperties`: `false`
- <a id="s-99fe7715f4"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4cfd0a7dc4"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-7a9e4a646d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-866ed6246e"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-56beb51396"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [validate_result](stove0-observer-support-observerconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-545f5f77df"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8bb6fdfee36e9ab2a4aaca59d38d03ccd200bce638b840f9ce154ea3692e70b7 -->

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
        "ObserverConformanceCoverage": {
          "additionalProperties": false,
          "properties": {
            "advertised": {
              "minimum": 0,
              "type": "integer"
            },
            "complete": {
              "type": "boolean"
            },
            "exercised": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "advertised",
            "exercised",
            "complete"
          ],
          "type": "object"
        },
        "ObserverContractConformance": {
          "additionalProperties": false,
          "properties": {
            "contract_id": {
              "type": "string"
            },
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "type": "string"
            },
            "facts_schema_profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "default": null
            },
            "facts_semantics_id": {
              "type": "string"
            },
            "facts_semantics_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "maximum_result_bytes": {
              "minimum": 1,
              "type": "integer"
            },
            "options_schema_profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "preferred_subject_batch_size": {
              "minimum": 1,
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
          "type": "object"
        },
        "ObserverContractConformanceEvidence": {
          "additionalProperties": false,
          "properties": {
            "observation": {
              "$ref": "#/$defs/ContentObservationResult"
            },
            "request": {
              "$ref": "#/$defs/ContentObservationRequest"
            }
          },
          "required": [
            "request",
            "observation"
          ],
          "type": "object"
        },
        "ObserverContractSupport": {
          "additionalProperties": false,
          "properties": {
            "contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "type": "integer"
            },
            "options_schema": {
              "$ref": "#/$defs/JsonSchemaValidationProfile"
            },
            "preferred_subject_batch_size": {
              "default": 128,
              "minimum": 1,
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
              "type": "array"
            },
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "image_id": {
              "pattern": "^sha256:[0-9a-f]{64}$",
              "type": "string"
            },
            "implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "implementation_version": {
              "maxLength": 120,
              "minLength": 1,
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
            }
          },
          "required": [
            "implementation_id",
            "implementation_version",
            "source_revision",
            "image_id",
            "contracts",
            "descriptor_sha256"
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
        "ObserverSemanticAcceptance": {
          "additionalProperties": false,
          "properties": {
            "kind": {
              "enum": [
                "schema-only-revalidated",
                "conformance-runner-attestation"
              ],
              "type": "string"
            },
            "profile_id": {
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
          "type": "object"
        },
        "ObserverSemanticVectorEvidence": {
          "additionalProperties": false,
          "properties": {
            "accepted_vector_ids": {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            "rejected_vector_ids": {
              "items": {
                "type": "string"
              },
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
          "type": "object"
        },
        "SemanticFactsConformanceVector": {
          "additionalProperties": false,
          "properties": {
            "accepted": {
              "type": "boolean"
            },
            "facts": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
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
            "id",
            "accepted",
            "subjects",
            "facts"
          ],
          "type": "object"
        },
        "SemanticFactsConformanceVectors": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-semantic-facts-conformance/v1",
              "default": "stove0-semantic-facts-conformance/v1",
              "type": "string"
            },
            "profile_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "vectors": {
              "items": {
                "$ref": "#/$defs/SemanticFactsConformanceVector"
              },
              "minItems": 2,
              "type": "array"
            }
          },
          "required": [
            "profile_id",
            "vectors"
          ],
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
              "default": null
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "contracts": {
          "items": {
            "$ref": "#/$defs/ObserverContractConformance"
          },
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
          "type": "string"
        },
        "status": {
          "enum": [
            "conformant",
            "partially-exercised",
            "inspected"
          ],
          "type": "string"
        }
      },
      "required": [
        "status",
        "descriptor",
        "coverage",
        "contracts"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-observer-conformance-result/v1'] = 'stove0-observer-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], descriptor: stove0_protocol.models.ObserverDescriptor, coverage: stove0_observer_support.conformance.ObserverConformanceCoverage, contracts: tuple[stove0_observer_support.conformance.ObserverContractConformance, ...]) -> None\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ObserverConformanceResult",
  "unit": "export"
}
```

</details>
