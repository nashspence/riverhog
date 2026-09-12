# schemas: RetrievalCacheStoreStatusOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachestorestatusout:593a25b981 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStoreStatusOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

## Contract summary

- `title`: RetrievalCacheStoreStatusOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `admission_budget_bytes` | no | object (2 fields) |  |
| `admission_enabled` | yes | boolean |  |
| `cache_store` | yes | #/components/schemas/RetrievalCacheStoreName |  |
| `committed_bytes` | yes | integer |  |
| `priority` | yes | integer |  |
| `reserved_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47a05eaf9ad5742e01f81bcc666f488950151a064fc3351783353cb032473daf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admission_budget_bytes": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Admission Budget Bytes"
    },
    "admission_enabled": {
      "title": "Admission Enabled",
      "type": "boolean"
    },
    "cache_store": {
      "$ref": "#/components/schemas/RetrievalCacheStoreName"
    },
    "committed_bytes": {
      "minimum": 0,
      "title": "Committed Bytes",
      "type": "integer"
    },
    "priority": {
      "minimum": 1,
      "title": "Priority",
      "type": "integer"
    },
    "reserved_bytes": {
      "minimum": 0,
      "title": "Reserved Bytes",
      "type": "integer"
    }
  },
  "required": [
    "cache_store",
    "priority",
    "admission_enabled",
    "reserved_bytes",
    "committed_bytes"
  ],
  "title": "RetrievalCacheStoreStatusOut",
  "type": "object"
}
```
