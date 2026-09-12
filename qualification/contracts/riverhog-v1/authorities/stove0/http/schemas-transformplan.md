# schemas: TransformPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-transformplan:f901bc9f58 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-4939cc6f45"></a>
- <a id="s-dbe908045a"></a>`title`: TransformPlan
- <a id="s-272f25ed69"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bbd1f6cd2e"></a>`inputs` | yes | #/components/schemas/TargetInputAuthority |  |
| <a id="s-a758235a3e"></a>`intent` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-ec8e360ca1"></a>`observation_result_sha256s` | no | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-f1d0b2c715"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-856c116b98"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c0cb979840"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fd41b9e89f"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1" |  |
| <a id="s-8c06f8c241"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-863feda82e"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-190f6a3902"></a>`target_options` | no | type="object"; additional keys=`additionalProperties` |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field intent](#s-a758235a3e) | `cardinality · entries · operational_policy` | shared above |
| [field observation_result_sha256s](#s-ec8e360ca1) | `cardinality · items · operational_policy` | shared above |
| [field target_options](#s-190f6a3902) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-9b86f26746"></a>[field observation_result_sha256s · items](#s-ec8e360ca1) | `length · characters · fixed` | shared above |
| [field operation_contract_sha256](#s-f1d0b2c715) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-c0cb979840) | `length · characters · fixed` | shared above |
| [field target_contract_sha256](#s-8c06f8c241) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: TargetInputAuthority](schemas-targetinputauthority.md)

## Governing policies

- <a id="pa-37289c804b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f708bf07cc"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-0b53ee2d06"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TransformPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c8787f6bfeccc4a726d18bf765bf3296a50ab27367c6040ab9b711e1ac9ec48 -->

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
      "const": "stove0-transform-target/v1",
      "default": "stove0-transform-target/v1",
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
  "title": "TransformPlan",
  "type": "object"
}
```
