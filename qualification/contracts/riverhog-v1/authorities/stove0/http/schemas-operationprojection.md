# schemas: OperationProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-operationprojection:ef6a8f5504 -->

One declarative JSON-pointer copy into an operation request.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: OperationProjection
- `description`: One declarative JSON-pointer copy into an operation request.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `destination` | yes | type="string"; enum=["intent","target-options"] |  |
| `destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| `source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| `source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OperationProjection`

### Exact owned JSON

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
