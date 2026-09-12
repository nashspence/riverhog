# schemas: RetrievalCacheStatusOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachestatusout:5e6012674f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: RetrievalCacheStatusOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `configured` | yes | type="boolean" |  |
| `new_archive_enabled` | yes | type="boolean" |  |
| `objects` | yes | type="integer" |  |
| `policy` | yes | #/components/schemas/RetrievalCachePolicyOut |  |
| `protected_objects` | yes | type="integer" |  |
| `stored_bytes` | yes | type="integer" |  |
| `stores` | yes | type="array"; items=(#/components/schemas/RetrievalCacheStoreStatusOut) |  |
| `unleased_objects` | yes | type="integer" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalCachePolicyOut](schemas-retrievalcachepolicyout.md)
- [schemas: RetrievalCacheStoreStatusOut](schemas-retrievalcachestorestatusout.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStatusOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61092d75a6bc2c2885c00ee6210558fcbf09b31f3414181c97fc286a499c4073 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "configured": {
      "title": "Configured",
      "type": "boolean"
    },
    "new_archive_enabled": {
      "title": "New Archive Enabled",
      "type": "boolean"
    },
    "objects": {
      "title": "Objects",
      "type": "integer"
    },
    "policy": {
      "$ref": "#/components/schemas/RetrievalCachePolicyOut"
    },
    "protected_objects": {
      "title": "Protected Objects",
      "type": "integer"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stores": {
      "items": {
        "$ref": "#/components/schemas/RetrievalCacheStoreStatusOut"
      },
      "title": "Stores",
      "type": "array"
    },
    "unleased_objects": {
      "title": "Unleased Objects",
      "type": "integer"
    }
  },
  "required": [
    "configured",
    "new_archive_enabled",
    "objects",
    "stored_bytes",
    "protected_objects",
    "unleased_objects",
    "stores",
    "policy"
  ],
  "title": "RetrievalCacheStatusOut",
  "type": "object"
}
```
