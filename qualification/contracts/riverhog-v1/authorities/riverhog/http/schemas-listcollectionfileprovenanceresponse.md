# schemas: ListCollectionFileProvenanceResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-listcollectionfileprovenanceresponse:dd5a297a31 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-62bcebadbd00"></a>
- <a id="s-bf7392250b98"></a>`title`: ListCollectionFileProvenanceResponse

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CapturedCollectionFileProvenancePage](schemas-capturedcollectionfileprovenancepage.md)
- [schemas: MixedCollectionFileProvenancePage](schemas-mixedcollectionfileprovenancepage.md)
- [schemas: OmittedCollectionFileProvenancePage](schemas-omittedcollectionfileprovenancepage.md)

## Governing policies

- <a id="pa-ab098c2d2f24"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionFileProvenanceResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1958a6ac97dfb941a4083e304b6d96461650baf008d19010a45960c7aa31ed38 -->

```json
{
  "discriminator": {
    "mapping": {
      "captured": "#/components/schemas/CapturedCollectionFileProvenancePage",
      "mixed": "#/components/schemas/MixedCollectionFileProvenancePage",
      "omitted": "#/components/schemas/OmittedCollectionFileProvenancePage"
    },
    "propertyName": "provenance_mode"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionFileProvenancePage"
    },
    {
      "$ref": "#/components/schemas/MixedCollectionFileProvenancePage"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionFileProvenancePage"
    }
  ],
  "title": "ListCollectionFileProvenanceResponse"
}
```
