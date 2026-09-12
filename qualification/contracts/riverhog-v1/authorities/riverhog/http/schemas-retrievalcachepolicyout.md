# schemas: RetrievalCachePolicyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachepolicyout:a4be5f615e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: RetrievalCachePolicyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `new_archive_lease_seconds` | yes | type="integer" |  |
| `pending_timeout_seconds` | yes | type="integer" |  |
| `restore_poll_interval_seconds` | yes | type="integer" |  |
| `retrieval_default_lease_seconds` | yes | type="integer" |  |
| `retrieval_max_lease_seconds` | yes | type="integer" |  |
| `sweep_interval_seconds` | yes | type="integer" |  |

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

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCachePolicyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 740de456c0f1474303689e5327befe189dd96d45b7fb36c67e1bc6d49fb71815 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "new_archive_lease_seconds": {
      "title": "New Archive Lease Seconds",
      "type": "integer"
    },
    "pending_timeout_seconds": {
      "title": "Pending Timeout Seconds",
      "type": "integer"
    },
    "restore_poll_interval_seconds": {
      "title": "Restore Poll Interval Seconds",
      "type": "integer"
    },
    "retrieval_default_lease_seconds": {
      "title": "Retrieval Default Lease Seconds",
      "type": "integer"
    },
    "retrieval_max_lease_seconds": {
      "title": "Retrieval Max Lease Seconds",
      "type": "integer"
    },
    "sweep_interval_seconds": {
      "title": "Sweep Interval Seconds",
      "type": "integer"
    }
  },
  "required": [
    "new_archive_lease_seconds",
    "retrieval_default_lease_seconds",
    "retrieval_max_lease_seconds",
    "pending_timeout_seconds",
    "sweep_interval_seconds",
    "restore_poll_interval_seconds"
  ],
  "title": "RetrievalCachePolicyOut",
  "type": "object"
}
```
