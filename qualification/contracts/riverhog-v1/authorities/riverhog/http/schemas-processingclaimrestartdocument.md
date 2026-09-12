# schemas: ProcessingClaimRestartDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimrestartdocument:e264ed84b7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimRestartDocument`

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

- `title`: ProcessingClaimRestartDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `fence` | yes | integer |  |
| `lease_seconds` | no | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1567d5ffa38a300b1cbcb6ae7cf2fc435dc07e4f6a6b18d2578795929f17619 -->

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
  "title": "ProcessingClaimRestartDocument",
  "type": "object"
}
```
