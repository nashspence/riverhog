# generated:stove0-review-sampler: SamplerConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-review-sampler-support:generated-stove0-review-sampler-samplerco-d72f170b7c:bd1c88689f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-91458a90af"></a>

- <a id="s-3824ed0f76"></a>`type`: `"object"`
- <a id="s-960228e395"></a>`additionalProperties`: `false`
- <a id="s-e3566e1b33"></a>`required`: `["status","sampler","coverage","sampling"]`
- <a id="s-155f99f675"></a>`title`: `"SamplerConformanceResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f5926c5386"></a>`coverage` | yes | [SamplerConformanceCoverage](#s-94518c8e12) |  |
| <a id="s-0c55a65141"></a>`format` | no | type="string"; const="stove0-review-sampler-conformance-result/v1"; default="stove0-review-sampler-conformance-result/v1"; title="Format" |  |
| <a id="s-ccd482d298"></a>`request` | no | anyOf=[([SamplerRequest](#s-31a33a19b5)); (type="null")]; default=null |  |
| <a id="s-d731a26fe4"></a>`sample` | no | anyOf=[([SamplerResult](#s-36154d3941)); (type="null")]; default=null |  |
| <a id="s-27f28a3590"></a>`sampler` | yes | [SamplerDescriptor](#s-3cbd950f9c) |  |
| <a id="s-e308775b66"></a>`sampling` | yes | type="string"; enum=["exercised","not-exercised"]; title="Sampling" |  |
| <a id="s-3a0ae02679"></a>`status` | yes | type="string"; enum=["conformant","inspected"]; title="Status" |  |

### Definitions

- [JsonSchemaDocument](#s-70c0e8bf4a)
- [JsonValue](#s-859721ca5f)
- [SamplerConformanceCoverage](#s-94518c8e12)
- [SamplerDescriptor](#s-3cbd950f9c)
- [SamplerFailure](#s-86fc0a8ec0)
- [SamplerInapplicable](#s-9293e5a127)
- [SamplerInput](#s-657bf3ad58)
- [SamplerOutput](#s-3bdca52814)
- [SamplerRequest](#s-31a33a19b5)
- [SamplerResult](#s-36154d3941)
- [SamplerWindow](#s-0f82a2deb8)

### <a id="s-70c0e8bf4a"></a>definition `JsonSchemaDocument`

- <a id="s-f7e146fb8a"></a>`type`: `"object"`
- <a id="s-f748b5a706"></a>`additionalProperties`: `false`
- <a id="s-4ca89fe391"></a>`required`: `["id","sha256","schema"]`
- <a id="s-1e5ad4ce6f"></a>`title`: `"JsonSchemaDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a5da406dd8"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-61dba31662"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-dbdd3ef851"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-3ec7a155f9"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-859721ca5f)); title="Schema" |  |
| <a id="s-bbb1edec18"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-859721ca5f"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-94518c8e12"></a>definition `SamplerConformanceCoverage`

- <a id="s-eb30fbe273"></a>`type`: `"object"`
- <a id="s-c98b0532c3"></a>`additionalProperties`: `false`
- <a id="s-376e51f96d"></a>`required`: `["exercised","complete"]`
- <a id="s-4f11d1eaa0"></a>`title`: `"SamplerConformanceCoverage"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-58137fa3d2"></a>`advertised` | no | type="integer"; const=1; default=1; title="Advertised" |  |
| <a id="s-1498f3f440"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-52bf1095a1"></a>`exercised` | yes | type="integer"; minimum=0; maximum=1; title="Exercised" |  |

### <a id="s-3cbd950f9c"></a>definition `SamplerDescriptor`

- <a id="s-02c88e13ed"></a>`type`: `"object"`
- <a id="s-44f5989caa"></a>`additionalProperties`: `false`
- <a id="s-256273580e"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","primary_operation_id","primary_operation_contract_sha256","portable_intent_schema","output_role","descriptor_sha256"]`
- <a id="s-6a795d2729"></a>`title`: `"SamplerDescriptor"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d4183e9ecd"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-3555292979"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-73ffea13b0"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-1dfc403081"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-0f1741a749"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Output Role" |  |
| <a id="s-3d36acd0c1"></a>`portable_intent_schema` | yes | [JsonSchemaDocument](#s-70c0e8bf4a) |  |
| <a id="s-20b5af8d04"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Primary Operation Contract Sha256" |  |
| <a id="s-4cb03d8b2c"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Primary Operation Id" |  |
| <a id="s-116bbaa07b"></a>`protocol` | no | type="string"; const="stove0-review-sampler/v1"; default="stove0-review-sampler/v1"; title="Protocol" |  |
| <a id="s-f8b6cb1c7a"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |

### <a id="s-86fc0a8ec0"></a>definition `SamplerFailure`

- <a id="s-08af4c8424"></a>`type`: `"object"`
- <a id="s-b995dfe554"></a>`additionalProperties`: `false`
- <a id="s-d1e734adde"></a>`required`: `["code","message","retryable"]`
- <a id="s-d671715dbf"></a>`title`: `"SamplerFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce33e4109c"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-3614157752"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-804edefd2e"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-9293e5a127"></a>definition `SamplerInapplicable`

- <a id="s-30694e3e63"></a>`type`: `"object"`
- <a id="s-22feedf8a6"></a>`additionalProperties`: `false`
- <a id="s-320ce3ab2a"></a>`required`: `["code","message"]`
- <a id="s-579dcd5115"></a>`title`: `"SamplerInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-42cd785837"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-7450a8b3c0"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-657bf3ad58"></a>definition `SamplerInput`

- <a id="s-f137254ca0"></a>`type`: `"object"`
- <a id="s-c1775ddb68"></a>`additionalProperties`: `false`
- <a id="s-9707ce14fa"></a>`required`: `["id","path","bytes","sha256"]`
- <a id="s-4d8e856bc3"></a>`title`: `"SamplerInput"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06a13f5f97"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-6de7508f99"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-d504bcc1a5"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-37db911b0a"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-59e0c99219"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-3bdca52814"></a>definition `SamplerOutput`

- <a id="s-2bc26a5696"></a>`type`: `"object"`
- <a id="s-7932bf1158"></a>`additionalProperties`: `false`
- <a id="s-020b4fa113"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`
- <a id="s-97b11c1b3f"></a>`title`: `"SamplerOutput"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6432d25c7a"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-ad304a6152"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1; title="Derived From" |  |
| <a id="s-de0e2b3d47"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-5e84a610c9"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1; title="Media Type" |  |
| <a id="s-fb0c811393"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-e595347167"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-31a33a19b5"></a>definition `SamplerRequest`

- <a id="s-8706cd87fd"></a>`type`: `"object"`
- <a id="s-9a1285c093"></a>`additionalProperties`: `false`
- <a id="s-ed6bac3cbd"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path","request_sha256"]`
- <a id="s-c68d90d925"></a>`title`: `"SamplerRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e973289435"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1; title="Cancellation Path" |  |
| <a id="s-1a5043d87d"></a>`format` | no | type="string"; const="stove0-review-sampler-request/v1"; default="stove0-review-sampler-request/v1"; title="Format" |  |
| <a id="s-8e9bf2a4dc"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-657bf3ad58)); minItems=1; title="Inputs" |  |
| <a id="s-5cc372c030"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776; title="Maximum Output Bytes" |  |
| <a id="s-0f61fa8233"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-859721ca5f)); title="Portable Intent" |  |
| <a id="s-ce4e43ab77"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-e75aab889c"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sampler Descriptor Sha256" |  |
| <a id="s-5f20545494"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400; title="Timeout Seconds" |  |
| <a id="s-ba92fcfa2d"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-0f82a2deb8)); minItems=1; title="Windows" |  |
| <a id="s-8e4c8d92b1"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workspace Id" |  |

### <a id="s-36154d3941"></a>definition `SamplerResult`

- <a id="s-084bcac5c0"></a>`type`: `"object"`
- <a id="s-4b690c6cdb"></a>`additionalProperties`: `false`
- <a id="s-c1ec15556a"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state","result_sha256"]`
- <a id="s-49f31bbd7d"></a>`title`: `"SamplerResult"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba880d4448"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-859721ca5f)); title="Execution Evidence" |  |
| <a id="s-b674c53edf"></a>`failure` | no | anyOf=[([SamplerFailure](#s-86fc0a8ec0)); (type="null")]; default=null |  |
| <a id="s-9bb36df30c"></a>`format` | no | type="string"; const="stove0-review-sampler-result/v1"; default="stove0-review-sampler-result/v1"; title="Format" |  |
| <a id="s-e77252b417"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-9293e5a127)); (type="null")]; default=null |  |
| <a id="s-8b6287177a"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-3bdca52814)); title="Outputs" |  |
| <a id="s-fad6aae132"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-7e3a4328e7"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-c913fa2c60"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sampler Descriptor Sha256" |  |
| <a id="s-1c356d279e"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"]; title="State" |  |

### <a id="s-0f82a2deb8"></a>definition `SamplerWindow`

- <a id="s-25e47652cc"></a>`type`: `"object"`
- <a id="s-3d26d5101f"></a>`additionalProperties`: `false`
- <a id="s-d57a6331e4"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`
- <a id="s-ffbc0b9d11"></a>`title`: `"SamplerWindow"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5251028025"></a>`duration_ms` | yes | type="integer"; minimum=1; title="Duration Ms" |  |
| <a id="s-00934f1415"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-d6d4abb04b"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Input Id" |  |
| <a id="s-97704c5295"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1; title="Output Path" |  |
| <a id="s-b24ae7ee3b"></a>`start_ms` | yes | type="integer"; minimum=0; title="Start Ms" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-review-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition JsonSchemaDocument · field schema](#s-3ec7a155f9) | `cardinality · entries · operational_policy` | shared above |
| [definition SamplerInput · field bytes](#s-06a13f5f97) | `value · schema-value · operational_policy` | shared above |
| [definition SamplerOutput · field bytes](#s-6432d25c7a) | `value · schema-value · operational_policy` | shared above |
| [definition SamplerOutput · field derived_from](#s-ad304a6152) | `cardinality · items · operational_policy` | shared above |
| [definition SamplerRequest · field inputs](#s-8e9bf2a4dc) | `cardinality · items · operational_policy` | shared above |
| [definition SamplerRequest · field portable_intent](#s-0f61fa8233) | `cardinality · entries · operational_policy` | shared above |
| [definition SamplerRequest · field windows](#s-ba92fcfa2d) | `cardinality · items · operational_policy` | shared above |
| [definition SamplerResult · field execution_evidence](#s-ba880d4448) | `cardinality · entries · operational_policy` | shared above |
| [definition SamplerResult · field outputs](#s-8b6287177a) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition JsonSchemaDocument · field sha256](#s-bbb1edec18) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerConformanceCoverage · field exercised](#s-52bf1095a1) | `value · schema-value · contract_max` | maximum=1; minimum=0; reason="schema-maximum" |
| [definition SamplerDescriptor · field descriptor_sha256](#s-d4183e9ecd) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerDescriptor · field image_digest](#s-3555292979) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerDescriptor · field implementation_version](#s-1dfc403081) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [definition SamplerDescriptor · field primary_operation_contract_sha256](#s-20b5af8d04) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerDescriptor · field source_revision](#s-f8b6cb1c7a) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [definition SamplerFailure · field message](#s-3614157752) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition SamplerInapplicable · field message](#s-7450a8b3c0) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-e7e4f045ed"></a>[definition SamplerInput · field media_type · string value](#s-d504bcc1a5) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition SamplerInput · field path](#s-37db911b0a) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition SamplerInput · field sha256](#s-59e0c99219) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerOutput · field media_type](#s-5e84a610c9) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition SamplerOutput · field path](#s-fb0c811393) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition SamplerOutput · field sha256](#s-e595347167) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerRequest · field cancellation_path](#s-e973289435) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition SamplerRequest · field maximum_output_bytes](#s-5cc372c030) | `value · schema-value · contract_max` | maximum=1099511627776; minimum=1; reason="schema-maximum" |
| [definition SamplerRequest · field request_sha256](#s-ce4e43ab77) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerRequest · field sampler_descriptor_sha256](#s-e75aab889c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerRequest · field timeout_seconds](#s-5f20545494) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [definition SamplerRequest · field workspace_id](#s-8e4c8d92b1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerResult · field request_sha256](#s-fad6aae132) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerResult · field result_sha256](#s-7e3a4328e7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerResult · field sampler_descriptor_sha256](#s-c913fa2c60) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerWindow · field output_path](#s-97704c5295) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-review-sampler protocol](../process-protocol/generated-stove0-review-sampler-protocol.md)

## Governing policies

- <a id="pa-3f37b845c7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ee2b23b4b8"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-4400b8b76c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/schemas.py::sampler\_schema\_bundle](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04925f6f55b8bdb122d1cfdcba5aa12da892ebae3bdc0f0f0f6955364773c80f -->

```json
{
  "$defs": {
    "JsonSchemaDocument": {
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
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "sha256",
        "schema"
      ],
      "title": "JsonSchemaDocument",
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
        "output_role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Output Role",
          "type": "string"
        },
        "portable_intent_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
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
          "const": "stove0-review-sampler/v1",
          "default": "stove0-review-sampler/v1",
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
          "const": "stove0-review-sampler-request/v1",
          "default": "stove0-review-sampler-request/v1",
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
          "const": "stove0-review-sampler-result/v1",
          "default": "stove0-review-sampler-result/v1",
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
      "const": "stove0-review-sampler-conformance-result/v1",
      "default": "stove0-review-sampler-conformance-result/v1",
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
