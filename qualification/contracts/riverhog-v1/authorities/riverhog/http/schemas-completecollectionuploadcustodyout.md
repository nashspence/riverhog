# schemas: CompleteCollectionUploadCustodyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-completecollectionuploadcustodyout:05ed40534c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: CompleteCollectionUploadCustodyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `state` | yes | type="string"; const="complete" |  |

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

- `/external_contract/http_openapi/riverhog/components/schemas/CompleteCollectionUploadCustodyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2710a21f095aac00ba34d113f099bded56cbe528cf70ce4f2f25eb490f5b758 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "const": "complete",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state"
  ],
  "title": "CompleteCollectionUploadCustodyOut",
  "type": "object"
}
```
