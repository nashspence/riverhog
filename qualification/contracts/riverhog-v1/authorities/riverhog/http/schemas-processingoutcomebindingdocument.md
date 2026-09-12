# schemas: ProcessingOutcomeBindingDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingoutcomebindingdocument:4bdea3f0e6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingOutcomeBindingDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ProcessingOutcomeBindingDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `claim_id` | yes | string |  |
| `fence` | yes | integer |  |
| `outcome_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 53b400e47c4282f39ca9413347f8cd65db26d06e70dab9ef036d89feaa731635 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "outcome_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Outcome Id",
      "type": "string"
    }
  },
  "required": [
    "claim_id",
    "fence",
    "outcome_id"
  ],
  "title": "ProcessingOutcomeBindingDocument",
  "type": "object"
}
```
