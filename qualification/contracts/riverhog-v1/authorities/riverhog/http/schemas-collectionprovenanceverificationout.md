# schemas: CollectionProvenanceVerificationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionprovenanceverificationout:d332a86c17 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-04d0c8d1c1c0"></a>
- <a id="s-24a6ab608d08"></a>`title`: CollectionProvenanceVerificationOut

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CapturedCollectionProvenanceVerification](schemas-capturedcollectionprovenanceverification.md)
- [schemas: OmittedCollectionProvenanceVerification](schemas-omittedcollectionprovenanceverification.md)

## Governing policies

- <a id="pa-b80803cab09b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionProvenanceVerificationOut`

### Exact owned JSON

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
