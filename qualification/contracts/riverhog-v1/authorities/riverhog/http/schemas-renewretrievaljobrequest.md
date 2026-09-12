# schemas: RenewRetrievalJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-renewretrievaljobrequest:9184554874 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: RenewRetrievalJobRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `lease_seconds` | yes | type="integer"; minimum=1 |  |

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RenewRetrievalJobRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c340f9b632598fe59aae3fc711a245ed0211b36c94b1b056c43b0a5bc95e419b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "lease_seconds": {
      "minimum": 1,
      "title": "Lease Seconds",
      "type": "integer"
    }
  },
  "required": [
    "lease_seconds"
  ],
  "title": "RenewRetrievalJobRequest",
  "type": "object"
}
```
