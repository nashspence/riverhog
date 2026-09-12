# schemas: RiverhogEventCause

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-riverhogeventcause:59b3f4a517 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: RiverhogEventCause
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `id` | yes | type="string"; minLength=1; maxLength=300 |  |
| `source` | yes | type="string"; minLength=1; maxLength=1000 |  |
| `subject` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| `type` | yes | type="string"; minLength=1; maxLength=300 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogEventCause`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac29075e27ddb589a896c56213e369861c512dd5f6d747bf10c49fe01431fcb2 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "source": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Source",
      "type": "string"
    },
    "subject": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "type": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type"
  ],
  "title": "RiverhogEventCause",
  "type": "object"
}
```
