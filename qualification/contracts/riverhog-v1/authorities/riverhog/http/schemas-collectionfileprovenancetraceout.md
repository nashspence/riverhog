# schemas: CollectionFileProvenanceTraceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionfileprovenancetraceout:119a156dc0 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionFileProvenanceTraceOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CapturedCollectionFileProvenanceTraceOut](schemas-capturedcollectionfileprovenancetraceout.md)
- [schemas: OmittedCollectionFileProvenanceTraceOut](schemas-omittedcollectionfileprovenancetraceout.md)

## Contract summary

- `title`: CollectionFileProvenanceTraceOut

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffae236bd5ff4a1cb5d80e6bba10a6dda77ccd4a580c2fdee5bd6bee98d651a4 -->

```json
{
  "anyOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionFileProvenanceTraceOut"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionFileProvenanceTraceOut"
    }
  ],
  "title": "CollectionFileProvenanceTraceOut"
}
```
