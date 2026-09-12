# schemas: BranchSetAdmittedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetadmittedeventdata:96986e89f7 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetAdmittedEventData`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: BranchSetAdmittedEventData
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `admitted_work_count` | yes | integer |  |
| `branch_count` | yes | integer |  |
| `branch_set_sha256` | yes | string |  |
| `phase` | yes | string |  |
| `revision` | yes | integer |  |
| `work_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8915aca1dade95650ccfb03c5ac47cf18fb3cd693a2c2becad387ba7624688f6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admitted_work_count": {
      "minimum": 1,
      "title": "Admitted Work Count",
      "type": "integer"
    },
    "branch_count": {
      "minimum": 1,
      "title": "Branch Count",
      "type": "integer"
    },
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
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
    "branch_count",
    "admitted_work_count"
  ],
  "title": "BranchSetAdmittedEventData",
  "type": "object"
}
```
