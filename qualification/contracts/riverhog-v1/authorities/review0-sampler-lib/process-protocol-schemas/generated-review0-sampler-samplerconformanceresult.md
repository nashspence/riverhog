# generated:review0-sampler: SamplerConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:review0-sampler-lib:generated-review0-sampler-samplerconformanceresult:914893d122 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-002e19f795"></a>

- <a id="s-104e3b9de3"></a>`type`: `"object"`
- <a id="s-6830253dfd"></a>`additionalProperties`: `false`
- <a id="s-69def4a3c5"></a>`required`: `["status","sampler","coverage","sampling"]`
- <a id="s-c008da38a0"></a>`title`: `"SamplerConformanceResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca5a823754"></a>`coverage` | yes | [SamplerConformanceCoverage](#s-e527608c59) |  |
| <a id="s-f05d967776"></a>`format` | no | type="string"; const="review0-sampler-conformance-result/v1"; default="review0-sampler-conformance-result/v1"; title="Format" |  |
| <a id="s-215a859ad1"></a>`request` | no | anyOf=[([SamplerRequest](#s-55189b34ad)); (type="null")]; default=null |  |
| <a id="s-0b6c8bcc1a"></a>`sample` | no | anyOf=[([SamplerResult](#s-92916a533d)); (type="null")]; default=null |  |
| <a id="s-97926ca93e"></a>`sampler` | yes | [SamplerDescriptor](#s-b65d05a8f9) |  |
| <a id="s-ee9f284923"></a>`sampling` | yes | type="string"; enum=["exercised","not-exercised"]; title="Sampling" |  |
| <a id="s-c317be12e2"></a>`status` | yes | type="string"; enum=["conformant","inspected"]; title="Status" |  |

### Definitions

- [JsonSchemaValidationProfile](#s-18fb039d31)
- [JsonValue](#s-812832d2c0)
- [SamplerConformanceCoverage](#s-e527608c59)
- [SamplerDescriptor](#s-b65d05a8f9)
- [SamplerFailure](#s-74f3a8271f)
- [SamplerInapplicable](#s-20ccc83941)
- [SamplerInput](#s-304df22058)
- [SamplerOutput](#s-a52322f394)
- [SamplerRequest](#s-55189b34ad)
- [SamplerResult](#s-92916a533d)
- [SamplerWindow](#s-96a52547a6)

### <a id="s-18fb039d31"></a>definition `JsonSchemaValidationProfile`

- <a id="s-32d6890473"></a>`type`: `"object"`
- <a id="s-9bb61638a6"></a>`additionalProperties`: `false`
- <a id="s-0d131af782"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-805c6b9df1"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d15b4cf993"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-a8486948f0"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-8f05bf3093"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-0495b32f2a"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-e8e4d90bb2"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-812832d2c0)); title="Schema" |  |

### <a id="s-812832d2c0"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-e527608c59"></a>definition `SamplerConformanceCoverage`

- <a id="s-06eddccb84"></a>`type`: `"object"`
- <a id="s-a6dc882f86"></a>`additionalProperties`: `false`
- <a id="s-836198c5ff"></a>`required`: `["exercised","complete"]`
- <a id="s-88c5a3e09a"></a>`title`: `"SamplerConformanceCoverage"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7a31b97a38"></a>`advertised` | no | type="integer"; const=1; default=1; title="Advertised" |  |
| <a id="s-70a5b4d8d8"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-4a54aaf338"></a>`exercised` | yes | type="integer"; minimum=0; maximum=1; title="Exercised" |  |

### <a id="s-b65d05a8f9"></a>definition `SamplerDescriptor`

- <a id="s-338e321dad"></a>`type`: `"object"`
- <a id="s-ff9fd38032"></a>`additionalProperties`: `false`
- <a id="s-40fc35c892"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","primary_operation_id","primary_operation_contract_sha256","portable_intent_schema","output_role","descriptor_sha256"]`
- <a id="s-157d9e6c78"></a>`title`: `"SamplerDescriptor"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff4eb4b0b1"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-259057a15e"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$"; title="Image Id" |  |
| <a id="s-835c8f3452"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-c484d1f4a1"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-f08c26ff8b"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Output Role" |  |
| <a id="s-0771a4adcd"></a>`portable_intent_schema` | yes | [JsonSchemaValidationProfile](#s-18fb039d31) |  |
| <a id="s-467429d518"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Primary Operation Contract Sha256" |  |
| <a id="s-9e48c9e86e"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Primary Operation Id" |  |
| <a id="s-e2e5376e89"></a>`protocol` | no | type="string"; const="review0-sampler/v1"; default="review0-sampler/v1"; title="Protocol" |  |
| <a id="s-cd700aa32e"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |

### <a id="s-74f3a8271f"></a>definition `SamplerFailure`

- <a id="s-af16ee40e6"></a>`type`: `"object"`
- <a id="s-0c0fe14c4e"></a>`additionalProperties`: `false`
- <a id="s-b5aa8bd3d5"></a>`required`: `["code","message","retryable"]`
- <a id="s-486134f1e7"></a>`title`: `"SamplerFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa78c44c03"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-4b48454fba"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-792a7a23d2"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-20ccc83941"></a>definition `SamplerInapplicable`

- <a id="s-82461731bc"></a>`type`: `"object"`
- <a id="s-aa8dc10bde"></a>`additionalProperties`: `false`
- <a id="s-516ab83481"></a>`required`: `["code","message"]`
- <a id="s-c022904b2d"></a>`title`: `"SamplerInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d89ab04db5"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-4c512bb07a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-304df22058"></a>definition `SamplerInput`

- <a id="s-032e6e0225"></a>`type`: `"object"`
- <a id="s-bb7c2675ff"></a>`additionalProperties`: `false`
- <a id="s-e599e93f27"></a>`required`: `["id","path","bytes","sha256"]`
- <a id="s-fd180be549"></a>`title`: `"SamplerInput"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd248c0974"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-8abfd5b018"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-56bbb4b8da"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-3718ec0136"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-12cc25b961"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-a52322f394"></a>definition `SamplerOutput`

- <a id="s-ee7ac5aef1"></a>`type`: `"object"`
- <a id="s-f65a7f0c47"></a>`additionalProperties`: `false`
- <a id="s-e9bf80638e"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`
- <a id="s-42e6a95766"></a>`title`: `"SamplerOutput"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-18e330f614"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-7963722ea1"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1; title="Derived From" |  |
| <a id="s-080820c544"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-a6a3c9cfb0"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1; title="Media Type" |  |
| <a id="s-1b222cec5e"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-7955ea17e0"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-55189b34ad"></a>definition `SamplerRequest`

- <a id="s-6774527590"></a>`type`: `"object"`
- <a id="s-d4ef3e89e4"></a>`additionalProperties`: `false`
- <a id="s-d246755bfb"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path","request_sha256"]`
- <a id="s-12c6d8a678"></a>`title`: `"SamplerRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3092ec1fc5"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1; title="Cancellation Path" |  |
| <a id="s-0464f47118"></a>`format` | no | type="string"; const="review0-sampler-request/v1"; default="review0-sampler-request/v1"; title="Format" |  |
| <a id="s-9d6a449140"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-304df22058)); minItems=1; title="Inputs" |  |
| <a id="s-0fad7ced73"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776; title="Maximum Output Bytes" |  |
| <a id="s-d9308f0246"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-812832d2c0)); title="Portable Intent" |  |
| <a id="s-1badfd6b0b"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-43dba02bd9"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sampler Descriptor Sha256" |  |
| <a id="s-dd1fde3346"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400; title="Timeout Seconds" |  |
| <a id="s-d854581f1b"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-96a52547a6)); minItems=1; title="Windows" |  |
| <a id="s-1c78930163"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workspace Id" |  |

### <a id="s-92916a533d"></a>definition `SamplerResult`

- <a id="s-27b026418b"></a>`type`: `"object"`
- <a id="s-19cd82901d"></a>`additionalProperties`: `false`
- <a id="s-64aff9ff46"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state","result_sha256"]`
- <a id="s-c9790fec94"></a>`title`: `"SamplerResult"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0bfd5625c5"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-812832d2c0)); title="Execution Evidence" |  |
| <a id="s-c1c6a5502d"></a>`failure` | no | anyOf=[([SamplerFailure](#s-74f3a8271f)); (type="null")]; default=null |  |
| <a id="s-efb51c2412"></a>`format` | no | type="string"; const="review0-sampler-result/v1"; default="review0-sampler-result/v1"; title="Format" |  |
| <a id="s-f5b6959834"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-20ccc83941)); (type="null")]; default=null |  |
| <a id="s-08d90f8356"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-a52322f394)); title="Outputs" |  |
| <a id="s-29bead92ad"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-e35926411b"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-bd5aa895f4"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sampler Descriptor Sha256" |  |
| <a id="s-e0495de381"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"]; title="State" |  |

### <a id="s-96a52547a6"></a>definition `SamplerWindow`

- <a id="s-e431dff956"></a>`type`: `"object"`
- <a id="s-9ad1e2e22f"></a>`additionalProperties`: `false`
- <a id="s-ecec62d899"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`
- <a id="s-4f8b069a86"></a>`title`: `"SamplerWindow"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b8cd5d611"></a>`duration_ms` | yes | type="integer"; minimum=1; title="Duration Ms" |  |
| <a id="s-68a7472b7c"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-c6d14ed40f"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Input Id" |  |
| <a id="s-742f768e03"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1; title="Output Path" |  |
| <a id="s-a55d0c3584"></a>`start_ms` | yes | type="integer"; minimum=0; title="Start Ms" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"review0-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition JsonSchemaValidationProfile · field schema](#s-e8e4d90bb2) | `cardinality · entries · operational_policy` | shared above |
| [definition SamplerInput · field bytes](#s-dd248c0974) | `value · schema-value · operational_policy` | shared above |
| [definition SamplerOutput · field bytes](#s-18e330f614) | `value · schema-value · operational_policy` | shared above |
| [definition SamplerOutput · field derived_from](#s-7963722ea1) | `cardinality · items · operational_policy` | shared above |
| [definition SamplerRequest · field inputs](#s-9d6a449140) | `cardinality · items · operational_policy` | shared above |
| [definition SamplerRequest · field portable_intent](#s-d9308f0246) | `cardinality · entries · operational_policy` | shared above |
| [definition SamplerRequest · field windows](#s-d854581f1b) | `cardinality · items · operational_policy` | shared above |
| [definition SamplerResult · field execution_evidence](#s-0bfd5625c5) | `cardinality · entries · operational_policy` | shared above |
| [definition SamplerResult · field outputs](#s-08d90f8356) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition JsonSchemaValidationProfile · field profile_sha256](#s-0495b32f2a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerConformanceCoverage · field exercised](#s-4a54aaf338) | `value · schema-value · contract_max` | maximum=1; minimum=0; reason="schema-maximum" |
| [definition SamplerDescriptor · field descriptor_sha256](#s-ff4eb4b0b1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerDescriptor · field implementation_version](#s-c484d1f4a1) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [definition SamplerDescriptor · field primary_operation_contract_sha256](#s-467429d518) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerDescriptor · field source_revision](#s-cd700aa32e) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [definition SamplerFailure · field message](#s-4b48454fba) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition SamplerInapplicable · field message](#s-4c512bb07a) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-aa8459ef60"></a>[definition SamplerInput · field media_type · string value](#s-56bbb4b8da) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition SamplerInput · field path](#s-3718ec0136) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition SamplerInput · field sha256](#s-12cc25b961) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerOutput · field media_type](#s-a6a3c9cfb0) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition SamplerOutput · field path](#s-1b222cec5e) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition SamplerOutput · field sha256](#s-7955ea17e0) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerRequest · field cancellation_path](#s-3092ec1fc5) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition SamplerRequest · field maximum_output_bytes](#s-0fad7ced73) | `value · schema-value · contract_max` | maximum=1099511627776; minimum=1; reason="schema-maximum" |
| [definition SamplerRequest · field request_sha256](#s-1badfd6b0b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerRequest · field sampler_descriptor_sha256](#s-43dba02bd9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerRequest · field timeout_seconds](#s-dd1fde3346) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [definition SamplerRequest · field workspace_id](#s-1c78930163) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerResult · field request_sha256](#s-29bead92ad) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerResult · field result_sha256](#s-e35926411b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerResult · field sampler_descriptor_sha256](#s-bd5aa895f4) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerWindow · field output_path](#s-742f768e03) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-932bea4afa"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-16c85e3bd6"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-3d450227a1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/schemas/SamplerConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3320130741b29567197aa60f3d20c993b55bb1a0a8ac4c1efc00cf858f66c52c -->

```json
{
  "$defs": {
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
    "SamplerConformanceCoverage": {
      "additionalProperties": false,
      "properties": {
        "advertised": {
          "const": 1,
          "default": 1,
          "title": "Advertised",
          "type": "integer"
        },
        "complete": {
          "title": "Complete",
          "type": "boolean"
        },
        "exercised": {
          "maximum": 1,
          "minimum": 0,
          "title": "Exercised",
          "type": "integer"
        }
      },
      "required": [
        "exercised",
        "complete"
      ],
      "title": "SamplerConformanceCoverage",
      "type": "object"
    },
    "SamplerDescriptor": {
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
        "output_role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Output Role",
          "type": "string"
        },
        "portable_intent_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "primary_operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Primary Operation Contract Sha256",
          "type": "string"
        },
        "primary_operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Primary Operation Id",
          "type": "string"
        },
        "protocol": {
          "const": "review0-sampler/v1",
          "default": "review0-sampler/v1",
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
        "image_id",
        "primary_operation_id",
        "primary_operation_contract_sha256",
        "portable_intent_schema",
        "output_role",
        "descriptor_sha256"
      ],
      "title": "SamplerDescriptor",
      "type": "object"
    },
    "SamplerFailure": {
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
      "title": "SamplerFailure",
      "type": "object"
    },
    "SamplerInapplicable": {
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
      "title": "SamplerInapplicable",
      "type": "object"
    },
    "SamplerInput": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
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
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "path",
        "bytes",
        "sha256"
      ],
      "title": "SamplerInput",
      "type": "object"
    },
    "SamplerOutput": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "derived_from": {
          "items": {
            "type": "string"
          },
          "minItems": 1,
          "title": "Derived From",
          "type": "array"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "media_type": {
          "maxLength": 255,
          "minLength": 1,
          "title": "Media Type",
          "type": "string"
        },
        "path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Path",
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
        "path",
        "bytes",
        "sha256",
        "media_type",
        "derived_from"
      ],
      "title": "SamplerOutput",
      "type": "object"
    },
    "SamplerRequest": {
      "additionalProperties": false,
      "properties": {
        "cancellation_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Cancellation Path",
          "type": "string"
        },
        "format": {
          "const": "review0-sampler-request/v1",
          "default": "review0-sampler-request/v1",
          "title": "Format",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/SamplerInput"
          },
          "minItems": 1,
          "title": "Inputs",
          "type": "array"
        },
        "maximum_output_bytes": {
          "maximum": 1099511627776,
          "minimum": 1,
          "title": "Maximum Output Bytes",
          "type": "integer"
        },
        "portable_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Portable Intent",
          "type": "object"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        },
        "sampler_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sampler Descriptor Sha256",
          "type": "string"
        },
        "timeout_seconds": {
          "maximum": 86400,
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "windows": {
          "items": {
            "$ref": "#/$defs/SamplerWindow"
          },
          "minItems": 1,
          "title": "Windows",
          "type": "array"
        },
        "workspace_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Workspace Id",
          "type": "string"
        }
      },
      "required": [
        "sampler_descriptor_sha256",
        "workspace_id",
        "inputs",
        "windows",
        "portable_intent",
        "maximum_output_bytes",
        "timeout_seconds",
        "cancellation_path",
        "request_sha256"
      ],
      "title": "SamplerRequest",
      "type": "object"
    },
    "SamplerResult": {
      "additionalProperties": false,
      "properties": {
        "execution_evidence": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Execution Evidence",
          "type": "object"
        },
        "failure": {
          "anyOf": [
            {
              "$ref": "#/$defs/SamplerFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "format": {
          "const": "review0-sampler-result/v1",
          "default": "review0-sampler-result/v1",
          "title": "Format",
          "type": "string"
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/SamplerInapplicable"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "outputs": {
          "default": [],
          "items": {
            "$ref": "#/$defs/SamplerOutput"
          },
          "title": "Outputs",
          "type": "array"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        },
        "result_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Result Sha256",
          "type": "string"
        },
        "sampler_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sampler Descriptor Sha256",
          "type": "string"
        },
        "state": {
          "enum": [
            "succeeded",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        }
      },
      "required": [
        "request_sha256",
        "sampler_descriptor_sha256",
        "state",
        "result_sha256"
      ],
      "title": "SamplerResult",
      "type": "object"
    },
    "SamplerWindow": {
      "additionalProperties": false,
      "properties": {
        "duration_ms": {
          "minimum": 1,
          "title": "Duration Ms",
          "type": "integer"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "input_id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Input Id",
          "type": "string"
        },
        "output_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Output Path",
          "type": "string"
        },
        "start_ms": {
          "minimum": 0,
          "title": "Start Ms",
          "type": "integer"
        }
      },
      "required": [
        "id",
        "input_id",
        "start_ms",
        "duration_ms",
        "output_path"
      ],
      "title": "SamplerWindow",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "coverage": {
      "$ref": "#/$defs/SamplerConformanceCoverage"
    },
    "format": {
      "const": "review0-sampler-conformance-result/v1",
      "default": "review0-sampler-conformance-result/v1",
      "title": "Format",
      "type": "string"
    },
    "request": {
      "anyOf": [
        {
          "$ref": "#/$defs/SamplerRequest"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "sample": {
      "anyOf": [
        {
          "$ref": "#/$defs/SamplerResult"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "sampler": {
      "$ref": "#/$defs/SamplerDescriptor"
    },
    "sampling": {
      "enum": [
        "exercised",
        "not-exercised"
      ],
      "title": "Sampling",
      "type": "string"
    },
    "status": {
      "enum": [
        "conformant",
        "inspected"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "sampler",
    "coverage",
    "sampling"
  ],
  "title": "SamplerConformanceResult",
  "type": "object"
}
```

</details>
