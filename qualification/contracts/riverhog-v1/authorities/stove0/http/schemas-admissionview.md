# schemas: AdmissionView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionview:e44d6c6b0c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

- `title`: AdmissionView
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempt_count` | yes | type="integer"; minimum=0 |  |
| `created_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| `failure` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| `intent` | yes | #/components/schemas/AdmissionIntent |  |
| `next_attempt_at` | no | anyOf=type="string"; minLength=1; maxLength=40 \| type="null" |  |
| `preview_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| `state` | yes | type="string"; enum=["intent","previewed","work_bound"] |  |
| `updated_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| `work_id` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=40, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=40, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=40, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AdmissionIntent](schemas-admissionintent.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cba7e1704e5373153f3f9f290e884c6d67733a32baed9f56bec9097eb280ffe -->

```json
{
  "additionalProperties": false,
  "properties": {
    "attempt_count": {
      "minimum": 0,
      "title": "Attempt Count",
      "type": "integer"
    },
    "created_at": {
      "maxLength": 40,
      "minLength": 1,
      "title": "Created At",
      "type": "string"
    },
    "failure": {
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
      "title": "Failure"
    },
    "intent": {
      "$ref": "#/components/schemas/AdmissionIntent"
    },
    "next_attempt_at": {
      "anyOf": [
        {
          "maxLength": 40,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Attempt At"
    },
    "preview_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Preview Sha256"
    },
    "state": {
      "enum": [
        "intent",
        "previewed",
        "work_bound"
      ],
      "title": "State",
      "type": "string"
    },
    "updated_at": {
      "maxLength": 40,
      "minLength": 1,
      "title": "Updated At",
      "type": "string"
    },
    "work_id": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Work Id"
    }
  },
  "required": [
    "intent",
    "state",
    "attempt_count",
    "created_at",
    "updated_at"
  ],
  "title": "AdmissionView",
  "type": "object"
}
```
