# schemas: RetrievalPlanRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanrequest:e6b84ec030 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanRequest`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: RetrievalFileReferenceDocument](schemas-retrievalfilereferencedocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=10000, minimum=1, reason=bounded-retrieval-work-request |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: RetrievalPlanRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `files` | yes | array |  |
| `idempotency_key` | yes | string |  |
| `lease_seconds` | no | object (2 fields) |  |
| `restore_policy` | no | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0713d37a8808e79acb34c07987edcd1d422626671a7dcf49e3a452cbbaf30d0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "files": {
      "items": {
        "$ref": "#/components/schemas/RetrievalFileReferenceDocument"
      },
      "maxItems": 10000,
      "minItems": 1,
      "title": "Files",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "multiple-retrieval-jobs",
        "reason": "bounded-retrieval-work-request"
      }
    },
    "idempotency_key": {
      "maxLength": 200,
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Idempotency Key",
      "type": "string"
    },
    "lease_seconds": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Lease Seconds"
    },
    "restore_policy": {
      "default": "allow",
      "enum": [
        "allow",
        "never"
      ],
      "title": "Restore Policy",
      "type": "string"
    }
  },
  "required": [
    "files",
    "idempotency_key"
  ],
  "title": "RetrievalPlanRequest",
  "type": "object"
}
```
