# schemas: TargetPlanBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetplanbinding:97956d1dde -->

Opaque binding to a target-owned preflight plan.

The target protocol owns the plan schema and canonicalization algorithm. stove0
retains the complete validated plan document and its target-issued digest, but
deliberately does not reinterpret or re-hash the plan with stove0's canonical
JSON rules. This prevents two authorities from disagreeing about target plan
identity while preserving the full document in the execution envelope.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-b6eb0a2955"></a>
- <a id="s-0ca7c36062"></a>`title`: TargetPlanBinding
- <a id="s-f30d751b24"></a>`description`: Opaque binding to a target-owned preflight plan.  The target protocol owns the plan schema and canonicalization algorithm. stove0 retains the complete validated plan document and its target-issued digest, but deliberately does not reinterpret or re-hash the plan with stove0's canonical JSON rules. This prevents two authorities from disagreeing about target plan identity while preserving the full document in the execution envelope.
- <a id="s-346dc2109d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-413e96b2fe"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-993b6f21cd"></a>`plan` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-8280a47826"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9dd294d2a7"></a>`protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d9f811c470"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f996140a29"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field plan](#s-993b6f21cd) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field operation_contract_sha256](#s-413e96b2fe) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-8280a47826) | `length · characters · fixed` | shared above |
| [field target_contract_sha256](#s-d9f811c470) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-6a7ee453b3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f2c168be8b"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-f896f0c331"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetPlanBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e347041141bb80657f5487a5324d1f55b68bcaee8feaa37c3c34760ba20f5045 -->

```json
{
  "additionalProperties": false,
  "description": "Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope.",
  "properties": {
    "operation_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Operation Contract Sha256",
      "type": "string"
    },
    "plan": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Plan",
      "type": "object"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "protocol": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
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
    }
  },
  "required": [
    "protocol",
    "target_implementation_id",
    "target_contract_sha256",
    "operation_contract_sha256",
    "plan",
    "plan_sha256"
  ],
  "title": "TargetPlanBinding",
  "type": "object"
}
```
