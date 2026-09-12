# schemas: CollectionFileProvenanceDetailOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionfileprovenancedetailout:427371a8fe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d8098d0602"></a>
- <a id="s-799ff2dcb9"></a>`title`: CollectionFileProvenanceDetailOut

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CapturedCollectionFileProvenanceDetailOut](schemas-capturedcollectionfileprovenancedetailout.md)
- [schemas: OmittedCollectionFileProvenanceDetailOut](schemas-omittedcollectionfileprovenancedetailout.md)

## Governing policies

- <a id="pa-59bf6fba56"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
