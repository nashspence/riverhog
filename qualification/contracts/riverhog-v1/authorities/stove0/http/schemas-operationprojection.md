# schemas: OperationProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-operationprojection:ef6a8f5504 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OperationProjection`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: OperationProjection
- `description`: One declarative JSON-pointer copy into an operation request.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `destination` | yes | string |  |
| `destination_pointer` | yes | string |  |
| `source` | yes | string |  |
| `source_pointer` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 412cc6fe371170daf347f525d51353e79834de6222531002457e205e033c56fa -->

```json
{
  "additionalProperties": false,
  "description": "One declarative JSON-pointer copy into an operation request.",
  "properties": {
    "destination": {
      "enum": [
        "intent",
        "target-options"
      ],
      "title": "Destination",
      "type": "string"
    },
    "destination_pointer": {
      "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
      "title": "Destination Pointer",
      "type": "string"
    },
    "source": {
      "enum": [
        "work-effective-intent",
        "work-evaluation"
      ],
      "title": "Source",
      "type": "string"
    },
    "source_pointer": {
      "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
      "title": "Source Pointer",
      "type": "string"
    }
  },
  "required": [
    "source",
    "source_pointer",
    "destination",
    "destination_pointer"
  ],
  "title": "OperationProjection",
  "type": "object"
}
```
