# schemas: RetrievalCacheStatusOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachestatusout:5e6012674f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStatusOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: RetrievalCachePolicyOut](schemas-retrievalcachepolicyout.md)
- [schemas: RetrievalCacheStoreStatusOut](schemas-retrievalcachestorestatusout.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: RetrievalCacheStatusOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `configured` | yes | boolean |  |
| `new_archive_enabled` | yes | boolean |  |
| `objects` | yes | integer |  |
| `policy` | yes | #/components/schemas/RetrievalCachePolicyOut |  |
| `protected_objects` | yes | integer |  |
| `stored_bytes` | yes | integer |  |
| `stores` | yes | array |  |
| `unleased_objects` | yes | integer |  |

## Complete owned contract

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
