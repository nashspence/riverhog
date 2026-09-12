# schemas: JoinAdmittedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joinadmittedeventdata:2251d2dede -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-43623f3a7b"></a>
- <a id="s-0bba2fe1fc"></a>`title`: JoinAdmittedEventData
- <a id="s-781830739f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a11a283b4"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0c6b187d5f"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0380a5b474"></a>`join_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eb49618452"></a>`phase` | yes | type="string"; const="coordinating" |  |
| <a id="s-e3abe001ac"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-b42337d8ba"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-2a11a283b4) | `length · characters · fixed` | shared above |
| [field join_plan_sha256](#s-0c6b187d5f) | `length · characters · fixed` | shared above |
| [field join_work_id](#s-0380a5b474) | `length · characters · fixed` | shared above |
| [field work_id](#s-b42337d8ba) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-1236e603a6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-c522a94005"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinAdmittedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dab536a055b1029ef8bc9d5c83589286aaeab4d73dbf9d5fc9a1f6b951f16e8d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "join_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Join Plan Sha256",
      "type": "string"
    },
    "join_work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Join Work Id",
      "type": "string"
    },
    "phase": {
      "const": "coordinating",
      "title": "Phase",
      "type": "string"
    },
    "revision": {
      "minimum": 2,
      "title": "Revision",
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
    "phase",
    "revision",
    "branch_set_sha256",
    "join_plan_sha256",
    "join_work_id"
  ],
  "title": "JoinAdmittedEventData",
  "type": "object"
}
```
