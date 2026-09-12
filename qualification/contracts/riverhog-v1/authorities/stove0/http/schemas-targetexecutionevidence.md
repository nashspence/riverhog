# schemas: TargetExecutionEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetexecutionevidence:ab920f1220 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-d1cc74145f"></a>
- <a id="s-1b9cbf8489"></a>`title`: TargetExecutionEvidence
- <a id="s-a1556263da"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0702eb4e69"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-190d671a90"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-427d486dae"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a67d9da8a7"></a>`runtime` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-2e35457826"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field runtime](#s-a67d9da8a7) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_sha256](#s-0702eb4e69) | `length · characters · fixed` | shared above |
| [field operation_contract_sha256](#s-190d671a90) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-427d486dae) | `length · characters · fixed` | shared above |
| [field target_contract_sha256](#s-2e35457826) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-d782018306"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-d48206f3d8"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-3cef3ec95c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetExecutionEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49ee579a04f4aeb23dc0b1a4c6638cde055c1e5198c38a2b31b859c65580563f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "execution_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Execution Sha256",
      "type": "string"
    },
    "operation_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Operation Contract Sha256",
      "type": "string"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "runtime": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Runtime",
      "type": "object"
    },
    "target_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Contract Sha256",
      "type": "string"
    }
  },
  "required": [
    "target_contract_sha256",
    "operation_contract_sha256",
    "plan_sha256",
    "execution_sha256"
  ],
  "title": "TargetExecutionEvidence",
  "type": "object"
}
```
