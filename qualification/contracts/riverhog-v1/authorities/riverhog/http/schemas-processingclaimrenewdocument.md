# schemas: ProcessingClaimRenewDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimrenewdocument:b83325245d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimRenewDocument`

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
| value | schema-value | `contract_max` | maximum=86400, minimum=30, reason=schema-maximum |

## Contract summary

- `title`: ProcessingClaimRenewDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `fence` | yes | integer |  |
| `lease_seconds` | no | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 653180bef69a08ae3179fd94db145a2e362fe71ddfe0e1bbfd4e65e8ced1f146 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "lease_seconds": {
      "default": 1800,
      "maximum": 86400,
      "minimum": 30,
      "title": "Lease Seconds",
      "type": "integer"
    }
  },
  "required": [
    "fence"
  ],
  "title": "ProcessingClaimRenewDocument",
  "type": "object"
}
```
