# schemas: EffectPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-effectplan:3fb7b4b541 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8485814f8e"></a>

- <a id="s-ee7da793c3"></a>`type`: `"object"`
- <a id="s-fb28e95d06"></a>`additionalProperties`: `false`
- <a id="s-6d01dc9d18"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`
- <a id="s-47fb8e647e"></a>`title`: `"EffectPlan"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db34175e92"></a>`inputs` | yes | [TargetInputAuthority](schemas-targetinputauthority.md) |  |
| <a id="s-a50283aacb"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Intent" |  |
| <a id="s-73e7c4241f"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-32963f2a0f"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-e510f4e7d6"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-1a7e01efe5"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-4b9d796104"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1"; title="Protocol" |  |
| <a id="s-3801ab0d31"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Contract Sha256" |  |
| <a id="s-5265da247b"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-b4e9848bc5"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Target Options" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field intent](#s-a50283aacb) | `cardinality · entries · operational_policy` | shared above |
| [field observation_result_sha256s](#s-73e7c4241f) | `cardinality · items · operational_policy` | shared above |
| [field target_options](#s-b4e9848bc5) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-e3020cc442"></a>[field observation_result_sha256s · items](#s-73e7c4241f) | `length · characters · fixed` | shared above |
| [field operation_contract_sha256](#s-32963f2a0f) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-1a7e01efe5) | `length · characters · fixed` | shared above |
| [field target_contract_sha256](#s-3801ab0d31) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [JsonValue](schemas-jsonvalue.md)
- [TargetInputAuthority](schemas-targetinputauthority.md)

## Governing policies

- <a id="pa-876162ce67"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-2fb994db2e"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-3195262832"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EffectPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff477804d70b8b67a6b1410e7f173bcfc7bf1a7f62febcb6afb86dd51a88f0a1 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "inputs": {
      "$ref": "#/components/schemas/TargetInputAuthority"
    },
    "intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
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
    "target_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Contract Sha256",
      "type": "string"
    },
    "target_implementation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Target Implementation Id",
      "type": "string"
    },
    "target_options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
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
    "target_contract_sha256",
    "plan_sha256"
  ],
  "title": "EffectPlan",
  "type": "object"
}
```

</details>
