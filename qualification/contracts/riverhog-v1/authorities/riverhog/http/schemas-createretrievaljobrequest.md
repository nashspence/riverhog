# schemas: CreateRetrievalJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-createretrievaljobrequest:e4b1c621d2 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateRetrievalJobRequest`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| encoded-size | bytes | `contract_max` | maximum=4096, reason=bounded-lifecycle-event-context |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: CreateRetrievalJobRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `event_context` | no | object (2 fields) |  |
| `plan_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8be23595a98af525a48d70ccd78c62c429aef2a8b7e0676da7c4ab1a2ddca0b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "event_context": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4096,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-lifecycle-event-context"
          }
        },
        {
          "type": "null"
        }
      ],
      "title": "Event Context"
    },
    "plan_id": {
      "title": "Plan Id",
      "type": "string"
    }
  },
  "required": [
    "plan_id"
  ],
  "title": "CreateRetrievalJobRequest",
  "type": "object"
}
```
