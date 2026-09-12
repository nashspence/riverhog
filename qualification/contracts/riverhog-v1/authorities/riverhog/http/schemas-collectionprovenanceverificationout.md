# schemas: CollectionProvenanceVerificationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionprovenanceverificationout:d332a86c17 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionProvenanceVerificationOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CapturedCollectionProvenanceVerification](schemas-capturedcollectionprovenanceverification.md)
- [schemas: OmittedCollectionProvenanceVerification](schemas-omittedcollectionprovenanceverification.md)

## Contract summary

- `title`: CollectionProvenanceVerificationOut

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bab7fea3f2032a486a9745607f077373aefca57e6ce2b7f13b6581e01c85a662 -->

```json
{
  "discriminator": {
    "mapping": {
      "captured": "#/components/schemas/CapturedCollectionProvenanceVerification",
      "mixed": "#/components/schemas/CapturedCollectionProvenanceVerification",
      "omitted": "#/components/schemas/OmittedCollectionProvenanceVerification"
    },
    "propertyName": "provenance_mode"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionProvenanceVerification"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionProvenanceVerification"
    }
  ],
  "title": "CollectionProvenanceVerificationOut"
}
```
