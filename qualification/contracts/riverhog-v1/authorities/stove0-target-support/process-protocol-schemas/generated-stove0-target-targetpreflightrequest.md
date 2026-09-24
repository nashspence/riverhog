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
| <a id="s-60c5a34834"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](#s-85672ac4d2)); title="Observations" |  |
| <a id="s-07a5946a90"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-142277f62c"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-6069a512bd"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-342fd382c1"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b0637b7a98)); title="Target Options" |  |

### Definitions

- [ArtifactSelectionRef](#s-3c4967cccf)
- [CollectionId](#s-a5154ea9fe)
- [CollectionRootIdentityRef](#s-1385076d93)
- [ContentObservationEvidence](#s-85672ac4d2)
- [ContentObservationFailure](#s-d7998e1949)
- [ContentObservationInapplicable](#s-e1a61ed7c1)
- [ContentObservationRequest](#s-e49da09261)
- [ContentObservationResult](#s-02e76bc583)
- [JsonSchemaValidationProfile](#s-f58ad8865b)
- [JsonValue](#s-b0637b7a98)
- [ObserverImplementation](#s-b3d0b4be58)
- [TargetInputAuthority](#s-0bf40c6b84)
- [TargetInputRoleCount](#s-a0d64f4573)
- [WorkArtifactSubject](#s-9d18d8578a)

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

### <a id="s-a5154ea9fe"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e3cfccb451"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-20f9e9cdd0"></a>2 | not=(const="0") |

### <a id="s-1385076d93"></a>definition `CollectionRootIdentityRef`

- <a id="s-15da2c46cd"></a>`type`: `"object"`
- <a id="s-b1061cee20"></a>`additionalProperties`: `false`
- <a id="s-9296be3b91"></a>`description`: `"Embedded Stove0 reference to the Riverhog collection-root identity."`
- <a id="s-29fcd01634"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-365cc3eca2"></a>`title`: `"CollectionRootIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aed004140e"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-2eb320f7fd"></a>`collection_id` | yes | [CollectionId](#s-a5154ea9fe) |  |
| <a id="s-6dae36674a"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-85672ac4d2"></a>definition `ContentObservationEvidence`

- <a id="s-81b73a28e3"></a>`type`: `"object"`
- <a id="s-c60422c5a9"></a>`additionalProperties`: `false`
- <a id="s-3a17ee0868"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-212907c24b"></a>`required`: `["request","result"]`
- <a id="s-7a066c07a2"></a>`title`: `"ContentObservationEvidence"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee39a2a1ce"></a>`request` | yes | [ContentObservationRequest](#s-e49da09261) |  |
| <a id="s-fd2a95747e"></a>`result` | yes | [ContentObservationResult](#s-02e76bc583) |  |

### <a id="s-d7998e1949"></a>definition `ContentObservationFailure`

- <a id="s-8642716753"></a>`type`: `"object"`
- <a id="s-cd56939d31"></a>`additionalProperties`: `false`
- <a id="s-0eb26fbb3f"></a>`required`: `["code","message","retryable"]`
- <a id="s-60833c340f"></a>`title`: `"ContentObservationFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-40fcaded8a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-44d7ad929a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-d06e5323a3"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-e1a61ed7c1"></a>definition `ContentObservationInapplicable`

- <a id="s-f339f9ae49"></a>`type`: `"object"`
- <a id="s-e462f41f4d"></a>`additionalProperties`: `false`
- <a id="s-048ff9b100"></a>`required`: `["code","message"]`
- <a id="s-3f2e6c4da8"></a>`title`: `"ContentObservationInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e3ac50a1a3"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-2c5ab624ce"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-e49da09261"></a>definition `ContentObservationRequest`

- <a id="s-f2fb934335"></a>`type`: `"object"`
- <a id="s-8d42d0935a"></a>`additionalProperties`: `false`
- <a id="s-004b67de24"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-9a10c03dab"></a>`title`: `"ContentObservationRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cba056367d"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-d85b35cf90"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-fb2c83f38b"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-cb4e67e542"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-c5e181ddd6"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-4a0282ce5c"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-e99ac77336"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-b0637b7a98)); title="Options" |  |
| <a id="s-41d25e7e91"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-78842a3bb8"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-8f49c952b8"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-9d18d8578a)); minItems=1; title="Subjects" |  |
| <a id="s-0ce7b68236"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-ff44a9e644"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-02e76bc583"></a>definition `ContentObservationResult`

- <a id="s-3725d18d46"></a>`type`: `"object"`
- <a id="s-837667728b"></a>`additionalProperties`: `false`
- <a id="s-9520c277ce"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-e33aec20d3"></a>`title`: `"ContentObservationResult"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b3fdb698c9"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-b0637b7a98)); title="Execution Evidence" |  |
| <a id="s-c9340a8d52"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-b0637b7a98))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-30bc5d3810"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-f58ad8865b)); (type="null")]; default=null |  |
| <a id="s-3933b56b15"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-e7dbc00ae7"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-d7998e1949)); (type="null")]; default=null |  |
| <a id="s-17019f5eab"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-354f81228d"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-e1a61ed7c1)); (type="null")]; default=null |  |
| <a id="s-0ff5f90c2b"></a>`observer` | yes | [ObserverImplementation](#s-b3d0b4be58) |  |
| <a id="s-46067fcd87"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-4b1bf24746"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-9a361deba7"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-71e3ecc478"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-a57cd3f608"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-99990d6156"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-9d18d8578a)); minItems=1; title="Subjects" |  |

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

### <a id="s-9d18d8578a"></a>definition `WorkArtifactSubject`

- <a id="s-4ef60f1e3e"></a>`type`: `"object"`
- <a id="s-0d005fc0c1"></a>`additionalProperties`: `false`
- <a id="s-f591b272e1"></a>`description`: `"A collection logical file assigned an ID and role within one Stove0 work."`
- <a id="s-ce0f7a7263"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-25042ce7c9"></a>`title`: `"WorkArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b73be5916b"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-e65e3e8491"></a>`collection` | yes | [CollectionRootIdentityRef](#s-1385076d93) |  |
| <a id="s-d0536c6ca2"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-1262cb66a3"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-a3a5108b66"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-311140494e"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-a1be27d94e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

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

<!-- exact-contract-value: 6f351c2e7bfed42411fbc4960e3a2d5f71388fd102b7838680fdf9dba09bde0d -->

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
      "description": "Embedded Stove0 reference to the Riverhog collection-root identity.",
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
      "title": "CollectionRootIdentityRef",
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
            "$ref": "#/$defs/WorkArtifactSubject"
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
            "$ref": "#/$defs/WorkArtifactSubject"
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
    "WorkArtifactSubject": {
      "additionalProperties": false,
      "description": "A collection logical file assigned an ID and role within one Stove0 work.",
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootIdentityRef"
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
      "title": "WorkArtifactSubject",
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
}
```

</details>
