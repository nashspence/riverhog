# schemas: RetrievalCacheSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachesort:4b9e045daa -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheSort`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `type`: string

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 916239d4b23b93de2df0d96c1e53030beb898fe22f943cb8f8fc21fc53540c68 -->

```json
{
  "enum": [
    "collection_id",
    "source_store",
    "object_id",
    "stored_bytes",
    "cached_at",
    "verified_at",
    "protected_until"
  ],
  "type": "string"
}
```
