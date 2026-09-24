# schemas: ContentObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-contentobservationrequest:1527394f9a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e8a64064b5"></a>

- <a id="s-0686648e5b"></a>`type`: `"object"`
- <a id="s-05b6b9496a"></a>`additionalProperties`: `false`
- <a id="s-27973bccc4"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-2420537f80"></a>`title`: `"ContentObservationRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7791311f0c"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-a921e3751b"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-6982286703"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-27e5521cc7"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-8c4ce807c7"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-41dc4db6f5"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-68cfe88ada"></a>`options` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Options" |  |
| <a id="s-eb33288024"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-fff4a6273c"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-c3a47947f3"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](schemas-workartifactsubject.md)); minItems=1; title="Subjects" |  |
| <a id="s-494b2e79ce"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-4e1bd7f51d"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field options](#s-68cfe88ada) | `cardinality · entries · operational_policy` | shared above |
| [field subjects](#s-c3a47947f3) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field maximum_result_bytes](#s-a921e3751b) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [field observer_contract_sha256](#s-27e5521cc7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field observer_descriptor_sha256](#s-8c4ce807c7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field request_id](#s-eb33288024) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field timeout_seconds](#s-494b2e79ce) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [field work_id](#s-4e1bd7f51d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [JsonValue](schemas-jsonvalue.md)
- [WorkArtifactSubject](schemas-workartifactsubject.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-af3dfeaed8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3dcdb6c33e"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-76515b7717"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ContentObservationRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e2810ce782cae122fa2267104906a468724b221a8fe34c0fba937262a4c2845 -->

```json
{
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
        "$ref": "#/components/schemas/JsonValue"
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
        "$ref": "#/components/schemas/WorkArtifactSubject"
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
}
```

</details>
