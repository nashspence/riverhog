# schemas: CollectionFileProvenanceDetailOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionfileprovenancedetailout:427371a8fe -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionFileProvenanceDetailOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CapturedCollectionFileProvenanceDetailOut](schemas-capturedcollectionfileprovenancedetailout.md)
- [schemas: OmittedCollectionFileProvenanceDetailOut](schemas-omittedcollectionfileprovenancedetailout.md)

## Contract summary

- `title`: CollectionFileProvenanceDetailOut

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc667b2d14c7947a359412ea4369dec8d1087c49fa4611ff251846ef53bffe76 -->

```json
{
  "anyOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionFileProvenanceDetailOut"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionFileProvenanceDetailOut"
    }
  ],
  "title": "CollectionFileProvenanceDetailOut"
}
```
