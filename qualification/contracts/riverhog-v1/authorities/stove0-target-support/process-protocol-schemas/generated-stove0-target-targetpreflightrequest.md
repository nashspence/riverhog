# generated:stove0-target: TargetPreflightRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-targetpreflightrequest:039a969058 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-d5b96e238f"></a>

- <a id="s-6b81c388f5"></a>`type`: `"object"`
- <a id="s-f11906cbf1"></a>`additionalProperties`: `false`
- <a id="s-9817043f81"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent"]`
- <a id="s-dd26b16f8a"></a>`title`: `"TargetPreflightRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1f7d55f071"></a>`inputs` | yes | [TargetInputAuthority](#s-0bf40c6b84) |  |
| <a id="s-c1ea16c2da"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-b0637b7a98)); title="Intent" |  |
| <a id="s-60c5a34834"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-2dc2fc885a)); title="Observations" |  |
| <a id="s-07a5946a90"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-142277f62c"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-6069a512bd"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-342fd382c1"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b0637b7a98)); title="Target Options" |  |

### Definitions

- [ArtifactSelectionRef](#s-3c4967cccf)
- [ArtifactSubject](#s-56a22d7fcb)
- [CollectionId](#s-a5154ea9fe)
- [CollectionRootRef](#s-fc381411d0)
- [JsonSchemaValidationProfile](#s-f58ad8865b)
- [JsonValue](#s-b0637b7a98)
- [ObservationEvidence](#s-2dc2fc885a)
- [ObservationFailure](#s-75fd5a78a3)
- [ObservationInapplicable](#s-03dc31d0d4)
- [ObservationRequest](#s-0774706ca1)
- [ObservationResult](#s-62d1c54be3)
- [ObserverImplementation](#s-b3d0b4be58)
- [TargetInputAuthority](#s-0bf40c6b84)
- [TargetInputRoleCount](#s-a0d64f4573)

### <a id="s-3c4967cccf"></a>definition `ArtifactSelectionRef`

- <a id="s-209c4ee4ab"></a>`type`: `"object"`
- <a id="s-0d4bb45228"></a>`additionalProperties`: `false`
- <a id="s-77189fed8f"></a>`description`: `"Closed reference to a separately retained selection document."`
- <a id="s-e6e6023a08"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`
- <a id="s-5f98c26f71"></a>`title`: `"ArtifactSelectionRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ae7101efd"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-d29b4f03dd"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Selection Sha256" |  |
| <a id="s-406e23a91d"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

### <a id="s-56a22d7fcb"></a>definition `ArtifactSubject`

- <a id="s-edd1ac3073"></a>`type`: `"object"`
- <a id="s-7ab99e4d71"></a>`additionalProperties`: `false`
- <a id="s-ca8ad64bec"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-908fda200e"></a>`title`: `"ArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2595b85632"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-32cb3a173b"></a>`collection` | yes | [CollectionRootRef](#s-fc381411d0) |  |
| <a id="s-285f24a4e5"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-f68e650b14"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-60fd345f49"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-ea8bccc8ba"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-3e418c8478"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-a5154ea9fe"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e3cfccb451"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-20f9e9cdd0"></a>2 | not=(const="0") |

### <a id="s-fc381411d0"></a>definition `CollectionRootRef`

- <a id="s-1ec946d130"></a>`type`: `"object"`
- <a id="s-f1940d5f99"></a>`additionalProperties`: `false`
- <a id="s-320048aadc"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-1886b79e2a"></a>`title`: `"CollectionRootRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-005cf9f0ca"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-4ea1b1e4b8"></a>`collection_id` | yes | [CollectionId](#s-a5154ea9fe) |  |
| <a id="s-e2c297efe4"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-f58ad8865b"></a>definition `JsonSchemaValidationProfile`

- <a id="s-0cb119608e"></a>`type`: `"object"`
- <a id="s-0ef6eedd71"></a>`additionalProperties`: `false`
- <a id="s-8b16ede07b"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-200e6aa2b0"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54afde4c5e"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-dfc2d4594c"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-747d2d6140"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-05b989459b"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-6f7fde9b4a"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-b0637b7a98)); title="Schema" |  |

### <a id="s-b0637b7a98"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-2dc2fc885a"></a>definition `ObservationEvidence`

- <a id="s-904b1922fa"></a>`type`: `"object"`
- <a id="s-2fbdab8e9e"></a>`additionalProperties`: `false`
- <a id="s-9845b803ed"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-6756331e56"></a>`required`: `["request","result"]`
- <a id="s-52b96da477"></a>`title`: `"ObservationEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-852b4d0897"></a>`request` | yes | [ObservationRequest](#s-0774706ca1) |  |
| <a id="s-f1903c9349"></a>`result` | yes | [ObservationResult](#s-62d1c54be3) |  |

### <a id="s-75fd5a78a3"></a>definition `ObservationFailure`

- <a id="s-dc769b4afc"></a>`type`: `"object"`
- <a id="s-df44c54979"></a>`additionalProperties`: `false`
- <a id="s-84681e0bd8"></a>`required`: `["code","message","retryable"]`
- <a id="s-2d160d667c"></a>`title`: `"ObservationFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c2f6441d7"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-fbdfc6b895"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-3611956b2a"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-03dc31d0d4"></a>definition `ObservationInapplicable`

- <a id="s-5d5f4218d0"></a>`type`: `"object"`
- <a id="s-90bb87cdf2"></a>`additionalProperties`: `false`
- <a id="s-b8b8d3d8d6"></a>`required`: `["code","message"]`
- <a id="s-8ad961e25e"></a>`title`: `"ObservationInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3ef0315560"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-aabccd7962"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-0774706ca1"></a>definition `ObservationRequest`

- <a id="s-6ef5d0388f"></a>`type`: `"object"`
- <a id="s-c896c93c4d"></a>`additionalProperties`: `false`
- <a id="s-0578592b46"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-a698a536df"></a>`title`: `"ObservationRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-618652f0f3"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-514caa4e89"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-9627389317"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-ea22d51c77"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-920e080fcc"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-45f7fd22f3"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-d614e2f80d"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-b0637b7a98)); title="Options" |  |
| <a id="s-bdc2f7598b"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-cae634579e"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-7efcc21acf"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-56a22d7fcb)); minItems=1; title="Subjects" |  |
| <a id="s-e97f6388df"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-c47504c389"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-62d1c54be3"></a>definition `ObservationResult`

- <a id="s-3d4ffe7d71"></a>`type`: `"object"`
- <a id="s-a0e3acb2f6"></a>`additionalProperties`: `false`
- <a id="s-9e9dc5ab5a"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-368a628021"></a>`title`: `"ObservationResult"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8625d8f9e8"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-b0637b7a98)); title="Execution Evidence" |  |
| <a id="s-51cddbb33e"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-b0637b7a98))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-828e39dc72"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-f58ad8865b)); (type="null")]; default=null |  |
| <a id="s-9eea10e91d"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-5c6ef9b55d"></a>`failure` | no | anyOf=[([ObservationFailure](#s-75fd5a78a3)); (type="null")]; default=null |  |
| <a id="s-a01fed9c53"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-20383931d2"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-03dc31d0d4)); (type="null")]; default=null |  |
| <a id="s-f431e812ef"></a>`observer` | yes | [ObserverImplementation](#s-b3d0b4be58) |  |
| <a id="s-1d27135646"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-4419214758"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-84137ea3e0"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-7e872e1e47"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-3da2417cd3"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-fb7d5c7312"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-56a22d7fcb)); minItems=1; title="Subjects" |  |

### <a id="s-b3d0b4be58"></a>definition `ObserverImplementation`

- <a id="s-0cd669866f"></a>`type`: `"object"`
- <a id="s-fdaf1aa01d"></a>`additionalProperties`: `false`
- <a id="s-869b63bf9a"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-6194898a26"></a>`title`: `"ObserverImplementation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d92b39cd23"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-1c0f6c1d4f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-01619d6c2a"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-7291e4c881"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-203ca69f7f"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

### <a id="s-0bf40c6b84"></a>definition `TargetInputAuthority`

- <a id="s-c7d4a4568d"></a>`type`: `"object"`
- <a id="s-e3b805a4a4"></a>`additionalProperties`: `false`
- <a id="s-fff8140d53"></a>`description`: `"Small exact input authority retained by Stove0 and traversed in bounded pages."`
- <a id="s-10f2e3bf9e"></a>`required`: `["selection","roles"]`
- <a id="s-5cdc976cb9"></a>`title`: `"TargetInputAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eddf99919c"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-a0d64f4573)); minItems=1; title="Roles" |  |
| <a id="s-b19333068c"></a>`selection` | yes | [ArtifactSelectionRef](#s-3c4967cccf) |  |

### <a id="s-a0d64f4573"></a>definition `TargetInputRoleCount`

- <a id="s-893c66754c"></a>`type`: `"object"`
- <a id="s-d5d1668e65"></a>`additionalProperties`: `false`
- <a id="s-f141bd587a"></a>`required`: `["role","count"]`
- <a id="s-160dc2e6e7"></a>`title`: `"TargetInputRoleCount"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b6c378a52c"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-1bc7a898ba"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field intent](#s-c1ea16c2da) | `cardinality · entries · operational_policy` | shared above |
| [field observations](#s-60c5a34834) | `cardinality · items · operational_policy` | shared above |
| [field target_options](#s-342fd382c1) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field operation_contract_sha256](#s-07a5946a90) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b66d02a587"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-5a0f9fb2c7"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-cbe2539aba"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetPreflightRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3db57455540342112a651c7ce511a2e3ed58472731687d2ec5cf87d5dc145c99 -->

```json
{
  "$defs": {
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
    "ObservationEvidence": {
      "additionalProperties": false,
      "description": "Complete routing evidence: immutable request plus accepted result.",
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
      "title": "ObservationEvidence",
      "type": "object"
    },
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
    }
  },
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
        "$ref": "#/$defs/ObservationEvidence"
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
}
```

</details>
