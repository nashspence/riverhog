# schemas: CollectionFileProvenanceDetailOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionfileprovenancedetailout:427371a8fe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: CollectionFileProvenanceDetailOut

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CapturedCollectionFileProvenanceDetailOut](schemas-capturedcollectionfileprovenancedetailout.md)
- [schemas: OmittedCollectionFileProvenanceDetailOut](schemas-omittedcollectionfileprovenancedetailout.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionFileProvenanceDetailOut`

### Exact owned JSON

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
