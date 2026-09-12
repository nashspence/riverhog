# schemas: AdmissionRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionrun:c0eebc7608 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: AdmissionRun
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `failures` | no | type="array"; items=(#/components/schemas/SchedulerFailure) |  |
| `progressed` | yes | type="array"; items=(type="string") |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: SchedulerFailure](schemas-schedulerfailure.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionRun`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0bbd87e19c427d12cb899be4bfee0310f6a65555bc5c9dd350752e31f181080e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "failures": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/SchedulerFailure"
      },
      "title": "Failures",
      "type": "array"
    },
    "progressed": {
      "items": {
        "type": "string"
      },
      "title": "Progressed",
      "type": "array"
    }
  },
  "required": [
    "progressed"
  ],
  "title": "AdmissionRun",
  "type": "object"
}
```
