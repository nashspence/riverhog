# schemas: RetrievalCachePolicyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachepolicyout:a4be5f615e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5c2812ec04"></a>
- <a id="s-1e99e2ed4c"></a>`title`: RetrievalCachePolicyOut
- <a id="s-d6408a19a0"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ae8a5ac6fa"></a>`new_archive_lease_seconds` | yes | type="integer" |  |
| <a id="s-6835d4cfa2"></a>`pending_timeout_seconds` | yes | type="integer" |  |
| <a id="s-8e1b64b618"></a>`restore_poll_interval_seconds` | yes | type="integer" |  |
| <a id="s-33aaca2d4d"></a>`retrieval_default_lease_seconds` | yes | type="integer" |  |
| <a id="s-3712d11b36"></a>`retrieval_max_lease_seconds` | yes | type="integer" |  |
| <a id="s-1ca2f61629"></a>`sweep_interval_seconds` | yes | type="integer" |  |

## Governing policies

- <a id="pa-0c03bab3ed"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
