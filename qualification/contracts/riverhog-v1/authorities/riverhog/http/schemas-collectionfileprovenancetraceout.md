# schemas: CollectionFileProvenanceTraceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionfileprovenancetraceout:119a156dc0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-160da0fc8a"></a>
- <a id="s-b6f5784b06"></a>`title`: CollectionFileProvenanceTraceOut

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CapturedCollectionFileProvenanceTraceOut](schemas-capturedcollectionfileprovenancetraceout.md)
- [schemas: OmittedCollectionFileProvenanceTraceOut](schemas-omittedcollectionfileprovenancetraceout.md)

## Governing policies

- <a id="pa-ce06e387d5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionFileProvenanceTraceOut`

### Exact owned JSON

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
