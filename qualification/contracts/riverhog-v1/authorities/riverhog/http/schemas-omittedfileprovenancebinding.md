# schemas: OmittedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-omittedfileprovenancebinding:25a1bef479 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: OmittedFileProvenanceBinding
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `omission_reason` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| `status` | yes | type="string"; const="omitted" |  |

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

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedFileProvenanceBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5eba6adbe60f961048cf4ff31b645ca43fb22f9f4ff95b3476666b75a05b8a3c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "omission_reason": {
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Omission Reason",
      "type": "string"
    },
    "status": {
      "const": "omitted",
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "omission_reason"
  ],
  "title": "OmittedFileProvenanceBinding",
  "type": "object"
}
```
