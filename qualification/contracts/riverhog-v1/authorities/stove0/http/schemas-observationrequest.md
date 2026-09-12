# schemas: ObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationrequest:a5956711b0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

<a id="s-62031e66a6"></a>
- <a id="s-a1984b91f9"></a>`title`: ObservationRequest
- <a id="s-944051be41"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6e8803262"></a>`format` | no | type="string"; const="stove0-observation-request/v1" |  |
| <a id="s-b2704269ad"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864 |  |
| <a id="s-dcf378d6a2"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-59c784d543"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dfb26a8874"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3cd9f0ed9d"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-6d486c894d"></a>`options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-602b52c8a1"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b16393f0df"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| <a id="s-1b6c6673ea"></a>`subjects` | yes | type="array"; minItems=1; items=(#/components/schemas/ArtifactSubject) |  |
| <a id="s-3cc30d6b48"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400 |  |
| <a id="s-44cda49790"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field options](#s-6d486c894d) | `cardinality · entries · operational_policy` | shared above |
| [field subjects](#s-1b6c6673ea) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field maximum_result_bytes](#s-b2704269ad) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [field observer_contract_sha256](#s-59c784d543) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field observer_descriptor_sha256](#s-dfb26a8874) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field request_id](#s-602b52c8a1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field timeout_seconds](#s-3cc30d6b48) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [field work_id](#s-44cda49790) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSubject](schemas-artifactsubject.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-c9090f7cba"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4926ea093c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-e92a32a1e1"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95c32706c6c4a78abdfe9a9750731d3341a8d656706f85010ca470253be19d2d -->

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
        "$ref": "#/components/schemas/ArtifactSubject"
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
}
```
