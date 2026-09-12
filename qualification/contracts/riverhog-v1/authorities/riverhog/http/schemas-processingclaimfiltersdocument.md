# schemas: ProcessingClaimFiltersDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimfiltersdocument:347988e701 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimFiltersDocument`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: ProcessingClaimFiltersDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `state` | no | object (2 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59b4d8bb229a8c43d763c982eebd067a71e6bccd1c8851ee169948435abc1158 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "anyOf": [
        {
          "enum": [
            "active",
            "settled",
            "retiring",
            "abandoned",
            "released"
          ],
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "State"
    }
  },
  "title": "ProcessingClaimFiltersDocument",
  "type": "object"
}
```
