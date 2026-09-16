# schemas: CollectionProvenanceVerificationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionprovenanceverificationout:e06aecc6a3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-04d0c8d1c1"></a>

- <a id="s-6fe2d962e6"></a>`discriminator`: `{"mapping":{"captured":"#/components/schemas/CapturedCollectionProvenanceVerification","mixed":"#/components/schemas/CapturedCollectionProvenanceVerification","omitted":"#/components/schemas/OmittedCollectionProvenanceVerification"},"propertyName":"provenance_mode"}`
- <a id="s-24a6ab608d"></a>`title`: `"CollectionProvenanceVerificationOut"`

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-d00336862d"></a>1 | #/components/schemas/CapturedCollectionProvenanceVerification |
| <a id="s-1415e7914a"></a>2 | #/components/schemas/OmittedCollectionProvenanceVerification |

## Maintained corroboration

### Referenced contract dossiers

- [CapturedCollectionProvenanceVerification](schemas-capturedcollectionprovenanceverification.md)
- [OmittedCollectionProvenanceVerification](schemas-omittedcollectionprovenanceverification.md)

## Governing policies

- <a id="pa-a75eced052"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionProvenanceVerificationOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
